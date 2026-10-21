"""Closed production worker policies, stable run merging and shared ownership."""
import hashlib
import random
import struct

import pytest

from readmixer_py.crypto import OperationKeys
from readmixer_py.errors import ConfigurationError, IntegrityError
from readmixer_py.execution import CpuObservations, resolve_cpu_budget
from readmixer_py.resources import Budget
from readmixer_py.sorting import ExternalSorter
from readmixer_py.stage_policy import (StagePolicy, SortPolicy, decode_policy,
                                      encode_policy, execute_policy)
from readmixer_py.stage_scheduler import StageScheduler
from readmixer_py.storage import SegmentStore
from readmixer_py.worker_protocol import (WorkerJob, PolicyJob, encode_message,
                                         decode_message, MAX_CONTROL_BYTES)
from test_stage_scheduler import frame


def cpu(count):
    return resolve_cpu_budget(observations=CpuObservations(count))


def packed(records):
    return b''.join(struct.pack('>I', len(record)) + record for record in records)


def cases():
    frames = [frame(2, 1), frame(0, 4), frame(0, 1), frame(1, 2), frame(0, 1)]
    original = StagePolicy(SortPolicy.ORIGINAL_FRAMES, 1024)
    tags = [bytes([value]) * 16 + ordinal.to_bytes(8, 'big')
            for ordinal, value in enumerate((2, 1, 1, 3, 0, 1))]
    identities = [struct.pack('>IQ', sample, ordinal) for sample, ordinal in ((1, 2), (0, 7), (0, 3), (1, 2))]
    key = bytes(range(32))
    def bucket(record):
        sample, ordinal = struct.unpack_from('<IQ', record, 4)
        digest = hashlib.blake2b(struct.pack('>IQ', sample, ordinal), key=key, digest_size=8).digest()
        return -(int.from_bytes(digest, 'little') & 7), sample, ordinal
    return [
        (original, frames, lambda value: struct.unpack_from('<IQ', value, 4)),
        (StagePolicy(SortPolicy.TAG_OBSERVATIONS), tags, lambda value: value[:16]),
        (StagePolicy(SortPolicy.COVERAGE_IDENTITIES), identities, lambda value: value),
        (StagePolicy.bucket((3, 4), 8, key, 1024), frames, bucket),
    ]


@pytest.mark.parametrize('policy,records,key', cases(), ids=lambda value: value.kind.name if isinstance(value, StagePolicy) else None)
def test_P14_T07_closed_policy_independent_keys_and_exact_bytes(policy, records, key):
    assert decode_policy(encode_policy(policy)) == policy
    result, count = execute_policy(packed(records), policy)
    assert result == packed(sorted(records, key=key))
    assert count == len(records)
    job = WorkerJob(bytes(16), 1, 2, len(result), 100000, hashlib.sha256(packed(records)).digest())
    wrapped = PolicyJob(job, encode_policy(policy))
    wire = encode_message(wrapped)
    assert len(wire) < MAX_CONTROL_BYTES
    assert decode_message(wire) == wrapped
    assert 'keys=' not in repr(policy)
    for corrupt in (wire[:-1], wire + b'x'):
        with pytest.raises(IntegrityError):
            decode_message(corrupt)


@pytest.mark.parametrize('policy,records,key', cases(), ids=lambda value: value.kind.name if isinstance(value, StagePolicy) else None)
@pytest.mark.parametrize('mode', ['memory', 'external', 'auto'])
def test_P14_T07_spawned_policies_all_storage_modes(tmp_path, policy, records, key, mode):
    budget = Budget(768 << 20, descriptors=256)
    keys = OperationKeys._from_test(bytes(32), bytes(16))
    with SegmentStore(mode, budget, keys, temp_dir=tmp_path) as store:
        with StageScheduler(budget, cpu(2), keys.run_id, store=store, production=True) as scheduler:
            assert scheduler.process_slots == 1
            with ExternalSorter(store, policy=policy, run_bytes=1000, max_record_bytes=1024, fan_in=2) as sorter:
                for record in records:
                    sorter.append(record)
                with sorter.iter_sorted() as result:
                    assert list(result) == sorted(records, key=key)
                assert scheduler.peak_pending > 0
            assert not scheduler.pending_jobs
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize('mode', ['memory', 'external', 'auto'])
@pytest.mark.parametrize('cpus', [1, 3])
def test_P14_T07_stable_ties_multipass_and_interleaved_sorter_owners(tmp_path, mode, cpus):
    budget = Budget(1024 << 20, descriptors=256)
    keys = OperationKeys._from_test(bytes(32), bytes(16))
    records = [bytes([number % 3]) * 16 + number.to_bytes(8, 'big') for number in range(80)]
    random.Random(441).shuffle(records)
    with SegmentStore(mode, budget, keys, temp_dir=tmp_path) as store:
        with StageScheduler(budget, cpu(cpus), keys.run_id, store=store, production=True,
                            task_count=8, max_job_bytes=512) as scheduler:
            policy = StagePolicy(SortPolicy.TAG_OBSERVATIONS)
            with ExternalSorter(store, policy=policy, run_bytes=1200, max_record_bytes=24, fan_in=2) as left, \
                 ExternalSorter(store, policy=policy, run_bytes=1300, max_record_bytes=24, fan_in=2) as right:
                for record in records:
                    left.append(record)
                    right.append(record)
                for sorter in (left, right):
                    with sorter.iter_sorted() as result:
                        assert list(result) == sorted(records, key=lambda value: value[:16])
                    assert sorter.merge_passes >= 3
                if cpus > 1:
                    assert scheduler.peak_pending == scheduler.capacity
            assert not scheduler.pending_jobs
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0


def test_P14_T07_foreign_stage_results_are_not_consumed_by_sorter(tmp_path):
    budget = Budget(768 << 20, descriptors=256)
    keys = OperationKeys._from_test(bytes(32), bytes(16))
    with SegmentStore('memory', budget, keys, temp_dir=tmp_path) as store:
        with StageScheduler(budget, cpu(2), keys.run_id, store=store, production=True) as scheduler:
            data = frame(2, 1)
            scheduler.submit(data, budget.acquire('test-foreign-input', len(data)))
            with ExternalSorter(store, policy=SortPolicy.COVERAGE_IDENTITIES, run_bytes=400,
                                max_record_bytes=12, fan_in=2) as sorter:
                for number in range(8, -1, -1):
                    sorter.append(struct.pack('>IQ', 0, number))
                with sorter.iter_sorted() as result:
                    assert list(result) == [struct.pack('>IQ', 0, number) for number in range(9)]
            assert scheduler.pending_jobs == 1
            with scheduler.take_next() as result:
                assert result.data == data
    assert budget.used_bytes == 0


@pytest.mark.parametrize('payload,policy', [(b'\0\0\0\xffx', StagePolicy(SortPolicy.TAG_OBSERVATIONS)),
    (packed([bytes(23)]), StagePolicy(SortPolicy.TAG_OBSERVATIONS)),
    (packed([bytes(13)]), StagePolicy(SortPolicy.COVERAGE_IDENTITIES)),
    (packed([bytes(48)]), StagePolicy(SortPolicy.ORIGINAL_FRAMES))])
def test_P14_T07_invalid_production_records_are_rejected(payload, policy):
    with pytest.raises(IntegrityError):
        execute_policy(payload, policy)


def test_P14_T07_policy_codec_rejects_extra_keys_and_unknown_policies():
    valid = encode_policy(StagePolicy(SortPolicy.ORIGINAL_FRAMES))
    for invalid in (b'', valid + bytes(32), bytes([2]) + valid[1:], valid[:1] + bytes([255]) + valid[2:]):
        with pytest.raises(IntegrityError):
            decode_policy(invalid)
    with pytest.raises(ConfigurationError):
        StagePolicy(SortPolicy.ORIGINAL_FRAMES, keys=(bytes(32),))


def test_P14_T07_complete_verification_uses_both_spawned_policies(tmp_path, monkeypatch):
    from readmixer_py import mixing, verification
    from readmixer_py.config import MixConfig, VerifyConfig, StorageMode
    from test_pipeline import fixtures
    inputs = tmp_path / 'inputs'
    fixtures(inputs, counts=(55, 31, 0))
    report = mixing._mix(MixConfig(input_dir=inputs, output=tmp_path / 'pool', workers=1,
        max_ram=1 << 30, storage=StorageMode.EXTERNAL),
        _keys=OperationKeys._from_test(bytes(32), bytes(16)))
    monkeypatch.setattr(verification, 'resolve_cpu_budget', lambda workers: cpu(workers))
    seen, pids = [], set()
    original = StageScheduler.submit_segment
    def submitted(self, ref, *, policy=None):
        seen.append(policy.kind)
        pids.update(self.child_pids)
        return original(self, ref, policy=policy)
    monkeypatch.setattr(StageScheduler, 'submit_segment', submitted)
    verified = verification.verify(VerifyConfig(manifest=tmp_path / 'pool' / report.manifest,
        workers=3, max_ram=1 << 30, storage=StorageMode.EXTERNAL))
    assert verified.completed and verified.pairs == 86 and verified.workers_effective > 1
    assert SortPolicy.ORIGINAL_FRAMES in seen and SortPolicy.TAG_OBSERVATIONS in seen
    import psutil
    assert pids and all(not psutil.pid_exists(pid) for pid in pids)


@pytest.mark.parametrize('mode', ['indexed', 'sequential'])
def test_P14_T07_spawned_recovery_preserves_one_union_gather(tmp_path, monkeypatch, mode):
    from readmixer_py import mixing, recovery
    from readmixer_py.config import MixConfig, RecoverConfig, RecoveryMode, StorageMode
    from readmixer_py.scan import PoolScanner
    from test_pipeline import fixtures
    from test_recovery import restored_files
    inputs = tmp_path / 'inputs'
    fixtures(inputs, counts=(35, 21, 0))
    report = mixing._mix(MixConfig(input_dir=inputs, output=tmp_path / 'pool', workers=1,
        max_ram=1 << 30, storage=StorageMode.EXTERNAL),
        _keys=OperationKeys._from_test(bytes(32), bytes(16)))
    manifest = tmp_path / 'pool' / report.manifest
    import json
    names = tuple(sample['sample_id'] for sample in json.loads(manifest.read_bytes())['samples'])
    monkeypatch.setattr(recovery, 'resolve_cpu_budget', lambda workers: cpu(workers))
    calls, pids = [], set()
    original_scan = getattr(PoolScanner, mode)
    def scanned(self, *args, **kwargs):
        calls.append(mode)
        return original_scan(self, *args, **kwargs)
    monkeypatch.setattr(PoolScanner, mode, scanned)
    original_submit = StageScheduler.submit_segment
    def submitted(self, ref, *, policy=None):
        assert policy.kind is SortPolicy.ORIGINAL_FRAMES
        pids.update(self.child_pids)
        return original_submit(self, ref, policy=policy)
    monkeypatch.setattr(StageScheduler, 'submit_segment', submitted)
    results = []
    for count in (1, 3):
        destination = tmp_path / f'restored-{count}'
        result = recovery.recover(RecoverConfig(manifest=manifest, output=destination,
            sample=names, io=RecoveryMode(mode), workers=count, max_ram=1 << 30,
            storage=StorageMode.EXTERNAL))
        assert result.completed and result.pairs == 56
        assert result.workers_effective == (1 if count == 1 else 2)
        results.append(restored_files(destination, result)[1])
    assert calls == [mode, mode] and results[0] == results[1]
    import psutil
    assert pids and all(not psutil.pid_exists(pid) for pid in pids)


def test_P14_T08_spawned_sort_failure_blocks_recovery_publication(tmp_path, monkeypatch):
    import psutil
    from readmixer_py import mixing, recovery
    from readmixer_py.config import MixConfig, RecoverConfig, StorageMode
    from test_pipeline import fixtures
    inputs = tmp_path / 'inputs'
    fixtures(inputs, counts=(12, 7, 0))
    report = mixing._mix(MixConfig(input_dir=inputs, output=tmp_path / 'pool', workers=1,
        max_ram=1 << 30, storage=StorageMode.EXTERNAL),
        _keys=OperationKeys._from_test(bytes(32), bytes(16)))
    manifest = tmp_path / 'pool' / report.manifest
    import json
    names = tuple(sample['sample_id'] for sample in json.loads(manifest.read_bytes())['samples'])
    source = {path.name: path.read_bytes() for path in manifest.parent.iterdir()}
    target = tmp_path / 'failed'
    target.mkdir(mode=0o700)
    (target / 'foreign').write_bytes(b'untouched')
    pids = set()
    monkeypatch.setattr(recovery, 'resolve_cpu_budget', lambda workers: cpu(workers))
    original = StageScheduler.submit_segment
    def bad_policy(self, ref, *, policy=None):
        pids.update(self.child_pids)
        return original(self, ref, policy=StagePolicy(SortPolicy.TAG_OBSERVATIONS))
    monkeypatch.setattr(StageScheduler, 'submit_segment', bad_policy)
    with pytest.raises(IntegrityError, match='worker stage failed'):
        recovery.recover(RecoverConfig(manifest=manifest, output=target, sample=names,
            workers=3, max_ram=1 << 30, storage=StorageMode.EXTERNAL))
    assert pids and all(not psutil.pid_exists(pid) for pid in pids)
    assert {path.name: path.read_bytes() for path in manifest.parent.iterdir()} == source
    assert {path.name: path.read_bytes() for path in target.iterdir()} == {'foreign': b'untouched'}


def test_P14_T07_large_legal_frames_use_bounded_serial_sort_fallback(tmp_path):
    budget = Budget(768 << 20, descriptors=128)
    keys = OperationKeys._from_test(bytes(32), bytes(16))
    records = [frame(1, 0, 140000), frame(0, 0, 140000)]
    assert all(len(value) > 512 << 10 for value in records)
    with SegmentStore('external', budget, keys, temp_dir=tmp_path) as store:
        with StageScheduler(budget, cpu(2), keys.run_id, store=store, production=True) as scheduler:
            with ExternalSorter(store, policy=StagePolicy(SortPolicy.ORIGINAL_FRAMES, 500000),
                                run_bytes=2 << 20, max_record_bytes=1000040, fan_in=2) as sorter:
                for record in records:
                    sorter.append(record)
                with sorter.iter_sorted() as result:
                    assert list(result) == list(reversed(records))
                assert scheduler.peak_pending == 0
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
