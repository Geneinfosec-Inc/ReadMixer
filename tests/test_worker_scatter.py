"""Child complete-set scatter capabilities, acknowledgments and ownership cleanup."""
from dataclasses import replace
import os
import struct

import pytest

from readmixer_py.crypto import OperationKeys, pairs_identity
from readmixer_py.direct_scatter import DirectScatter
from readmixer_py.errors import IntegrityError, ResourceError
from readmixer_py.resources import Budget
from readmixer_py.storage import SegmentStore
from readmixer_py.worker_scatter import WorkerScatterGeneration
from readmixer_py.worker_storage import WorkerStorage


def keys():
    return OperationKeys._from_test(bytes(range(32)), bytes(range(16)))


def frame(sample, ordinal, size=13):
    lines = (b'@same/1', b'A' * size, b'+', b'!' * size,
             b'@same/2', b'T' * size, b'+', b'!' * size)
    return struct.pack('<IIQ8I', 48 + sum(map(len, lines)), sample, ordinal,
                       *map(len, lines)) + b''.join(lines)


def grants_for(manager, *, producer=0, batch=0, batch_bytes=4096, start=1, task=0):
    return tuple(manager.issue_write(task, start + bucket, 1, pairs_identity((bucket,), producer, batch),
                                     batch_bytes, batch_bytes // 48) for bucket in range(64))


def collect_frames(payload):
    offset = 0
    while offset < len(payload):
        size, = struct.unpack_from('<I', payload, offset)
        yield payload[offset:offset + size]
        offset += size
    assert offset == len(payload)


@pytest.mark.parametrize('mode', ['memory', 'external', 'auto'])
def test_worker_generations_import_to_one_paged_canonical_inventory(tmp_path, mode):
    budget = Budget(512 << 20)
    source = [frame(sample, ordinal) for sample in range(2) for ordinal in range(13)]
    observed = []
    with SegmentStore(mode, budget, keys(), temp_dir=tmp_path) as store:
        manager = None if mode == 'memory' else WorkerStorage(store, max_pending=64, max_job_bytes=4096)
        try:
            with DirectScatter(store, max_record_bytes=64, batch_bytes=4096, sort_run_bytes=4096,
                               inventory_only=True, producer_count=2) as inventory:
                for producer in range(2):
                    grants = None if manager is None else grants_for(manager, producer=producer, start=64 * producer + 1)
                    receipts = []
                    def returned(bucket, maximum, result, count):
                        receipts.append(bucket)
                        if not count:
                            assert result is None and maximum == 0
                            if manager is not None:
                                manager.discard_empty_write(grants[bucket])
                            return
                        if manager is None:
                            with store.new_segment(1, pairs_identity((bucket,), producer, 0)) as writer:
                                writer.append(result, item_count=count)
                                ref = writer.seal()
                        else:
                            assert result.item_count == count
                            ref = manager.accept_write(grants[bucket], result)
                        inventory.import_batch_reference(bucket, producer, 0, maximum, ref)
                        if manager is not None:
                            assert manager.take_write(grants[bucket]) == ref
                    kwargs = dict(producer=producer, batch=0, bucket_key=keys().bucket_key(()),
                                  max_record_bytes=64, batch_bytes=4096)
                    if manager is not None:
                        kwargs.update(credit=lambda bucket, offset, amount: manager.authorize_write(grants[bucket], offset, amount),
                                      commit=lambda bucket, offset, amount: manager.commit_write(grants[bucket], offset, amount))
                    generation = WorkerScatterGeneration(grants, **kwargs)
                    with generation:
                        for record in reversed(source[producer * 13:(producer + 1) * 13]):
                            generation.append(record)
                        count, size, maximum = generation.finish(returned)
                        assert (count, size) == (13, sum(map(len, source[:13])))
                        assert maximum == len(source[0])
                        assert receipts == list(range(64))
                        assert all(writer.delivered for writer in generation._buckets)
                    assert generation._budget.active_leases == 0
                if manager is not None:
                    assert not manager._writes
                    manager.cleanup(workers_reaped=True)
                with inventory.iter_sorted() as records:
                    observed = list(records)
                assert sorted(observed) == sorted(source)
                assert inventory.records == 26
        finally:
            if manager is not None:
                manager.cleanup(workers_reaped=True)
    assert budget.active_leases == 0
    assert list(tmp_path.iterdir()) == []


def test_worker_source_bytes_remain_owned_until_acknowledgment():
    generation = WorkerScatterGeneration(producer=0, batch=0, bucket_key=keys().bucket_key(()),
                                          max_record_bytes=64, batch_bytes=4096)
    generation.append(frame(0, 0))
    retained = []
    def no_ack(bucket, maximum, result, count):
        if count:
            assert isinstance(result, bytes)
            retained.append(generation._budget.used_bytes)
            raise ResourceError('receiver did not acknowledge')
    with pytest.raises(ResourceError, match='acknowledge'):
        generation.finish(no_ack)
    assert retained and any(writer.result is not None for writer in generation._buckets)
    assert generation._budget.active_leases > 0
    generation.close()
    assert generation._budget.active_leases == 0


@pytest.mark.parametrize('fault', ['wrong-producer', 'duplicate', 'foreign-directory', 'wrong-key'])
def test_worker_rejects_or_authenticates_forged_generation_capabilities(tmp_path, fault):
    budget = Budget(256 << 20)
    with SegmentStore('external', budget, keys(), temp_dir=tmp_path) as store:
        manager = WorkerStorage(store, max_pending=64, max_job_bytes=4096)
        grants = grants_for(manager)
        options = dict(producer=0, batch=0, bucket_key=keys().bucket_key(()), max_record_bytes=64,
                       batch_bytes=4096,
                       credit=lambda bucket, offset, amount: manager.authorize_write(grants[bucket], offset, amount),
                       commit=lambda bucket, offset, amount: manager.commit_write(grants[bucket], offset, amount))
        forged = list(grants)
        if fault == 'wrong-producer':
            options['producer'] = 1
        elif fault == 'duplicate':
            forged[1] = forged[0]
        elif fault == 'foreign-directory':
            forged[1] = replace(forged[1], directory_inode=forged[1].directory_inode + 1)
        else:
            forged = [replace(grant, key=bytes(32)) for grant in forged]
        try:
            with pytest.raises(IntegrityError):
                with WorkerScatterGeneration(tuple(forged), **options) as generation:
                    generation.append(frame(0, 0))
                    def returned(bucket, maximum, result, count):
                        if count:
                            manager.accept_write(grants[bucket], result)
                    generation.finish(returned)
        finally:
            manager.cleanup(workers_reaped=True)
    assert budget.active_leases == 0
    assert list(tmp_path.iterdir()) == []


def test_empty_grant_retirement_survives_journal_commit_interruption(tmp_path, monkeypatch):
    budget = Budget(128 << 20)
    with SegmentStore('external', budget, keys(), temp_dir=tmp_path) as store:
        manager = WorkerStorage(store, max_pending=1)
        grant = manager.issue_write(0, 1, 1, pairs_identity((0,), 0, 0), 4096, 85)
        original = manager.ledger._commit_owned_boundary
        def interrupted(boundary):
            original(boundary)
            raise KeyboardInterrupt()
        monkeypatch.setattr(manager.ledger, '_commit_owned_boundary', interrupted)
        with pytest.raises(KeyboardInterrupt):
            manager.discard_empty_write(grant)
        assert not (store.scratch_directory / store._name(grant.ref)).exists()
        assert manager._writes
        manager.cleanup(workers_reaped=True)
    assert budget.active_leases == 0


def test_empty_grant_retirement_preserves_a_foreign_replacement(tmp_path):
    budget = Budget(128 << 20)
    with SegmentStore('external', budget, keys(), temp_dir=tmp_path) as store:
        manager = WorkerStorage(store, max_pending=1)
        grant = manager.issue_write(0, 1, 1, pairs_identity((0,), 0, 0), 4096, 85)
        path = store.scratch_directory / store._name(grant.ref)
        held = os.open(path, os.O_RDONLY)
        path.unlink()
        path.write_bytes(b'foreign')
        try:
            with pytest.raises(IntegrityError, match='replaced'):
                manager.discard_empty_write(grant)
            assert path.read_bytes() == b'foreign'
        finally:
            path.unlink()
            os.close(held)
            manager.cleanup(workers_reaped=True)
    assert budget.active_leases == 0


def test_exact_whole_pair_batch_limit_and_one_over():
    record = frame(0, 0)
    generation = WorkerScatterGeneration(producer=0, batch=0, bucket_key=keys().bucket_key(()),
                                          max_record_bytes=64, batch_bytes=2 * len(record))
    with generation:
        generation.append(record)
        generation.append(frame(0, 1))
        with pytest.raises(ResourceError, match='whole-pair'):
            generation.append(frame(0, 2))
        assert generation.pairs == 2
        outputs = []
        generation.finish(lambda bucket, maximum, data, count: outputs.extend(collect_frames(data)) if data else None)
        assert sorted(outputs) == sorted([record, frame(0, 1)])
    assert generation._budget.active_leases == 0


@pytest.mark.parametrize('external', [False, True])
def test_generation_crosses_two_full_codec_blocks_and_retains_partial_tail(tmp_path, external):
    record = frame(0, 0, size=600_000)
    budget = Budget(256 << 20)
    with SegmentStore('external' if external else 'memory', budget, keys(), temp_dir=tmp_path) as store:
        manager = WorkerStorage(store, max_pending=64, max_job_bytes=3 << 20) if external else None
        grants = grants_for(manager, batch_bytes=3 << 20) if external else None
        credits = []
        result_refs = []
        def credit(bucket, offset, amount):
            credits.append((bucket, offset, amount))
            manager.authorize_write(grants[bucket], offset, amount)
        def returned(bucket, maximum, result, count):
            if not count:
                if external:
                    manager.discard_empty_write(grants[bucket])
                return
            assert maximum == len(record) and count == 1
            if external:
                ref = manager.accept_write(grants[bucket], result)
                result_refs.append(ref)
                manager.take_write(grants[bucket])
                assert ref.data_block_count == 3
                with store.open_verified(ref) as stream:
                    decoded = bytearray()
                    while payload := stream.read(1 << 20):
                        decoded.extend(payload)
                assert bytes(decoded) == record
            else:
                assert result == record
        generation = WorkerScatterGeneration(grants, producer=0, batch=0, bucket_key=keys().bucket_key(()),
            max_record_bytes=1_300_000, batch_bytes=3 << 20,
            credit=credit if external else None,
            commit=(lambda bucket, offset, amount: manager.commit_write(grants[bucket], offset, amount)) if external else None)
        try:
            with generation:
                generation.append(record)
                generation.finish(returned)
            assert generation._budget.active_leases == 0
            if external:
                # Header + three encrypted blocks + complete authenticated seal.
                assert len(credits) == 5
                assert all(0 < amount <= (1 << 20) + 36 for _bucket, _offset, amount in credits)
        finally:
            for ref in result_refs:
                store.drop(ref, missing_ok=True)
            if manager is not None:
                manager.cleanup(workers_reaped=True)
    assert budget.active_leases == 0
    assert list(tmp_path.iterdir()) == []
