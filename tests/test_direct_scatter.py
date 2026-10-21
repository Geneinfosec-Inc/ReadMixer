"""Plan 14 whole-set scatter admission, canonical bytes and ownership faults."""
import hashlib
import random
import struct

import pytest

from readmixer_py.crypto import OperationKeys, observations_identity
from readmixer_py.direct_scatter import DirectScatter
from readmixer_py.errors import ConfigurationError, IntegrityError, ResourceError
from readmixer_py.resources import Budget
from readmixer_py.storage import BLOCK_BYTES, SegmentStore, SharedFlushWorkspace


def keys():
    return OperationKeys._from_test(bytes(range(32)), bytes(range(16)))


def frame(sample, ordinal, size=5):
    lines = (b'@same/1', b'A' * size, b'+', b'!' * size,
             b'@same/2', b'T' * size, b'+', b'!' * size)
    return struct.pack('<IIQ8I', 48 + sum(map(len, lines)), sample, ordinal,
                       *map(len, lines)) + b''.join(lines)


def oracle_key(record):
    sample, ordinal = struct.unpack_from('<IQ', record, 4)
    digest = hashlib.blake2b(struct.pack('>IQ', sample, ordinal),
                            key=keys().bucket_key(()), digest_size=8).digest()
    return -(int.from_bytes(digest, 'little') & 63), sample, ordinal


@pytest.mark.parametrize('mode', ['memory', 'external', 'auto'])
@pytest.mark.parametrize('batch_bytes,producer', [(168, 0), (1024, 3), (65536, 9)])
def test_scatter_strategies_and_boundaries_match_canonical_bytes(tmp_path, mode, batch_bytes, producer):
    budget = Budget(256 << 20)
    source = [frame(sample, ordinal) for sample in range(3) for ordinal in range(17)]
    random.Random(414).shuffle(source)
    expected = sorted(source, key=oracle_key)
    with SegmentStore(mode, budget, keys(), temp_dir=tmp_path) as store:
        assert DirectScatter.can_admit(store, max_record_bytes=64, batch_bytes=batch_bytes,
                                       producer=producer, sort_run_bytes=4096)
        with DirectScatter(store, max_record_bytes=64, batch_bytes=batch_bytes,
                           producer=producer, sort_run_bytes=4096) as scatter:
            assert sum(writer is not None for writer in scatter._writers) == 64
            assert scatter._workspace._writers == 64
            for record in source:
                scatter.append(record)
            with scatter.iter_sorted() as records:
                assert list(records) == expected
            assert scatter.records == len(source)
            assert scatter.frame_bytes == sum(map(len, source))
            assert scatter.batches == ((len(source) + batch_bytes // len(source[0]) - 1)
                                       // (batch_bytes // len(source[0])))
        assert store.resident_segments == 0
        if mode == 'memory':
            assert store.scratch_directory is None
    assert budget.used_bytes == budget.used_descriptors == budget.used_metadata_bytes == 0
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize('mode', ['memory', 'external', 'auto'])
def test_empty_set_creates_no_data_segments(tmp_path, mode):
    budget = Budget(128 << 20)
    with SegmentStore(mode, budget, keys(), temp_dir=tmp_path) as store:
        with DirectScatter(store, max_record_bytes=64, batch_bytes=1024, sort_run_bytes=4096) as scatter:
            with scatter.iter_sorted() as records:
                assert list(records) == []
            assert scatter.batches == scatter._published == 0
        assert store.scratch_directory is None
    assert budget.active_leases == 0


@pytest.mark.parametrize('mode', ['external', 'auto'])
def test_low_descriptor_refusal_does_not_consume_input_or_create_scratch(tmp_path, mode):
    budget = Budget(256 << 20, descriptors=50)
    with SegmentStore(mode, budget, keys(), temp_dir=tmp_path) as store:
        before = budget.used_bytes, budget.used_descriptors, budget.used_metadata_bytes, store._next_id
        assert not DirectScatter.can_admit(store, max_record_bytes=64, batch_bytes=1024, sort_run_bytes=4096)
        assert before == (budget.used_bytes, budget.used_descriptors, budget.used_metadata_bytes, store._next_id)
        with pytest.raises(ResourceError, match='descriptors'):
            DirectScatter(store, max_record_bytes=64, batch_bytes=1024, sort_run_bytes=4096)
        assert store.resident_segments == 0
        assert store.scratch_directory is None
    assert budget.active_leases == 0


def test_shared_workspace_has_one_transient_charge_and_rejects_overlap(tmp_path):
    budget = Budget(128 << 20)
    with SegmentStore('external', budget, keys(), temp_dir=tmp_path) as store:
        with DirectScatter(store, max_record_bytes=64, batch_bytes=1024, sort_run_bytes=4096) as scatter:
            assert [lease.owner for lease in budget._leases].count('shared-segment-flush') == 1
            assert not any(lease.owner == 'segment-output' for lease in budget._leases)
            assert len([lease for lease in budget._leases if lease.owner == 'shared-segment-output']) == 64
            assert budget.used_bytes < 90 << 20
            with scatter._workspace.borrow():
                with pytest.raises(ResourceError, match='already borrowed'):
                    with scatter._workspace.borrow():
                        pass
                with pytest.raises(ResourceError, match='live borrowers'):
                    scatter._workspace.close()
            scatter.append(frame(0, 0))
    assert budget.active_leases == 0


def test_independent_shared_writers_keep_distinct_seals_and_keys(tmp_path):
    budget = Budget(48 << 20)
    data = b'pair bytes' * (BLOCK_BYTES // 10 + 13)
    with SegmentStore('external', budget, keys(), temp_dir=tmp_path) as store:
        with SharedFlushWorkspace(budget) as workspace:
            refs = []
            for ordinal in range(2):
                with store.new_segment(5, observations_identity('shared', ordinal), flush_workspace=workspace) as writer:
                    writer.append(data, item_count=11)
                    refs.append(writer.seal())
            assert refs[0].digest == refs[1].digest == hashlib.sha256(data).digest()
            assert refs[0].segment_id != refs[1].segment_id
            first, second = [(store.scratch_directory / store._name(ref)).read_bytes() for ref in refs]
            assert first != second
            for ref in refs:
                with store.open_verified(ref) as reader:
                    assert reader.read(len(data)) == data
                store.drop(ref)
    assert budget.active_leases == 0


def test_auto_seals_and_spills_every_full_set_before_large_source_continues(tmp_path):
    budget = Budget(128 << 20)
    with SegmentStore('auto', budget, keys(), temp_dir=tmp_path) as store:
        with DirectScatter(store, max_record_bytes=64, batch_bytes=1024, sort_run_bytes=4096) as scatter:
            for ordinal in range(1000):
                scatter.append(frame(0, ordinal))
                assert scatter._batch_used <= 1024
                # Complete batches leave resident ownership in authenticated
                # pages; only the fresh 64-writer set remains in the registry.
                assert store.resident_segments == 64
                assert budget.used_bytes < 110 << 20
            assert scatter.batches > 70
            with scatter.iter_sorted() as records:
                assert list(records) == sorted([frame(0, ordinal) for ordinal in range(1000)], key=oracle_key)
    assert budget.active_leases == 0
    assert list(tmp_path.iterdir()) == []


def test_duplicate_identity_fails_canonical_merge_without_leaking(tmp_path):
    budget = Budget(128 << 20)
    with SegmentStore('external', budget, keys(), temp_dir=tmp_path) as store:
        with DirectScatter(store, max_record_bytes=64, batch_bytes=168, sort_run_bytes=4096) as scatter:
            scatter.append(frame(1, 7))
            scatter.append(frame(1, 7))
            with pytest.raises(IntegrityError, match='repeated'):
                with scatter.iter_sorted() as records:
                    list(records)
    assert budget.active_leases == 0


def test_owned_inventory_append_interruption_keeps_committed_handoff(tmp_path, monkeypatch):
    budget = Budget(128 << 20)
    with SegmentStore('external', budget, keys(), temp_dir=tmp_path) as store:
        scatter = DirectScatter(store, max_record_bytes=64, batch_bytes=168, sort_run_bytes=4096)
        scatter.append(frame(1, 7))
        original = scatter._inventory._commit_owned_boundary
        def interrupted(boundary):
            original(boundary)
            raise KeyboardInterrupt()
        monkeypatch.setattr(scatter._inventory, '_commit_owned_boundary', interrupted)
        with pytest.raises(KeyboardInterrupt):
            scatter.append(frame(1, 8))
            scatter.append(frame(1, 9))
        assert scatter._inventory.owned_count == scatter._published == 1
        assert scatter._pending is None
        scatter.close()
        assert store.resident_segments == 0
    assert budget.active_leases == 0


def test_cleanup_retry_retains_ownership_until_success(tmp_path, monkeypatch):
    budget = Budget(128 << 20)
    with SegmentStore('external', budget, keys(), temp_dir=tmp_path) as store:
        scatter = DirectScatter(store, max_record_bytes=64, batch_bytes=1024, sort_run_bytes=4096)
        for ordinal in range(3):
            scatter.append(frame(0, ordinal))
        scatter._seal_set()
        original = store.drop
        calls = 0
        def fail_once(ref, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise ResourceError('injected owned deletion failure')
            return original(ref, **kwargs)
        monkeypatch.setattr(store, 'drop', fail_once)
        with pytest.raises(ResourceError, match='injected'):
            scatter.close()
        assert not scatter._closed
        assert scatter._inventory.owned_remaining
        scatter.close()
    assert budget.active_leases == 0
    assert list(tmp_path.iterdir()) == []


def test_memory_exhaustion_keeps_original_writer_set_owned_and_never_spills(tmp_path):
    budget = Budget(128 << 20)
    with SegmentStore('memory', budget, keys(), temp_dir=tmp_path) as store:
        with DirectScatter(store, max_record_bytes=64, batch_bytes=168, sort_run_bytes=4096) as scatter:
            scatter.append(frame(0, 0))
            # Reduce the available envelope only after complete-set admission.
            pressure = budget.acquire('test-pressure', budget.remaining_bytes - (1 << 20))
            try:
                with pytest.raises(ResourceError):
                    for ordinal in range(1, 1000):
                        scatter.append(frame(0, ordinal))
                assert store.scratch_directory is None
            finally:
                pressure.release()
    assert budget.active_leases == 0
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize('batch', [0, 167, (64 << 20) + 1, True])
def test_invalid_batch_target_is_rejected_before_resource_mutation(tmp_path, batch):
    budget = Budget(128 << 20)
    with SegmentStore('memory', budget, keys(), temp_dir=tmp_path) as store:
        before = budget.used_bytes, budget.used_metadata_bytes
        with pytest.raises(ConfigurationError):
            DirectScatter(store, max_record_bytes=64, batch_bytes=batch, sort_run_bytes=4096)
        assert before == (budget.used_bytes, budget.used_metadata_bytes)


@pytest.mark.parametrize('mode', ['memory', 'external', 'auto'])
def test_shuffler_direct_and_sorter_preserve_exact_pair_order(tmp_path, mode):
    from readmixer_py.shuffle import SerialShuffler
    results = []
    for strategy, batch in [('direct', 168), ('direct', 4096), ('sorter', 4096)]:
        budget = Budget(256 << 20)
        with SegmentStore(mode, budget, keys(), temp_dir=tmp_path) as store:
            with SerialShuffler(store, terminal_bytes=64 << 20, max_record_bytes=64,
                                sort_run_bytes=4096, scatter_strategy=strategy,
                                scatter_batch_bytes=batch) as shuffler:
                for sample in (2, 0, 1):
                    for ordinal in range(9, -1, -1):
                        shuffler.append_frame(frame(sample, ordinal))
                with shuffler.iter_ordered((10, 10, 10)) as pairs:
                    results.append([(pair.secure_pair_index, pair.sample_index,
                                     pair.original_pair_index, bytes(pair.frame)) for pair in pairs])
        assert budget.active_leases == 0
    assert results[0] == results[1] == results[2]
    assert list(tmp_path.iterdir()) == []


def test_shuffler_low_descriptor_fallback_is_chosen_before_input(tmp_path):
    from readmixer_py.shuffle import SerialShuffler
    budget = Budget(128 << 20, descriptors=32)
    with SegmentStore('external', budget, keys(), temp_dir=tmp_path) as store:
        with SerialShuffler(store, terminal_bytes=64 << 20, max_record_bytes=64,
                            sort_run_bytes=4096) as shuffler:
            assert shuffler.scatter_strategy == 'sorter'
            assert shuffler.pairs == 0
            assert store.scratch_directory is None
        before = budget.used_bytes, budget.used_descriptors, store.resident_segments
        with pytest.raises(ResourceError, match='complete direct scatter'):
            SerialShuffler(store, terminal_bytes=64 << 20, max_record_bytes=64,
                           sort_run_bytes=4096, scatter_strategy='direct')
        assert before == (budget.used_bytes, budget.used_descriptors, store.resident_segments)
    assert budget.active_leases == 0


def test_shuffler_auto_reduces_complete_batch_reservation_before_fallback(tmp_path):
    from readmixer_py.shuffle import SerialShuffler
    budget = Budget(128 << 20)
    with SegmentStore('memory', budget, keys(), temp_dir=tmp_path) as store:
        with SerialShuffler(store, terminal_bytes=64 << 20, max_record_bytes=64,
                            sort_run_bytes=4096) as shuffler:
            assert shuffler.scatter_strategy == 'direct'
            assert 168 <= shuffler.scatter_batch_bytes < 64 << 20
            assert sum(writer is not None for writer in shuffler._scatter._writers) == 64
    assert budget.active_leases == 0


def test_corrupted_direct_segment_fails_authentication_and_cleans_owned_files(tmp_path):
    budget = Budget(128 << 20)
    with SegmentStore('external', budget, keys(), temp_dir=tmp_path) as store:
        with DirectScatter(store, max_record_bytes=64, batch_bytes=1024, sort_run_bytes=4096) as scatter:
            scatter.append(frame(0, 0))
            scatter._seal_set()
            with scatter._inventory.open_reverse() as records:
                _bucket, _maximum, ref = scatter._decode_inventory(next(records))
            path = store.scratch_directory / store._name(ref)
            payload = bytearray(path.read_bytes())
            payload[-1] ^= 1
            path.write_bytes(payload)
            with pytest.raises(IntegrityError, match='authentication'):
                with scatter.iter_sorted() as records:
                    list(records)
    assert budget.active_leases == 0
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize('mode', ['memory', 'external', 'auto'])
def test_finish_ingress_retires_writer_work_before_emitter_admission(tmp_path, mode):
    budget = Budget(128 << 20)
    with SegmentStore(mode, budget, keys(), temp_dir=tmp_path) as store:
        with DirectScatter(store, max_record_bytes=64, batch_bytes=1024, sort_run_bytes=4096) as scatter:
            scatter.append(frame(0, 0))
            before = budget.used_bytes
            scatter.finish_ingress()
            assert budget.used_bytes < before - (64 << 20)
            assert scatter._workspace._closed
            assert scatter._retention.released
            scatter.finish_ingress()
            with pytest.raises(ResourceError):
                scatter.append(frame(0, 1))
            with scatter.iter_sorted() as records:
                assert list(records) == [frame(0, 0)]
    assert budget.active_leases == 0
