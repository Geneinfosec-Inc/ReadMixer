"""Plan 13 explicit sender ownership through real scheduler handoffs."""
import struct

import psutil
import pytest

from readmixer_py.crypto import OperationKeys, observations_identity
from readmixer_py.errors import CancelledError, ConfigurationError, IntegrityError, ResourceError
from readmixer_py.execution import CpuObservations, resolve_cpu_budget
from readmixer_py.resources import Budget
from readmixer_py.stage_scheduler import StageScheduler
from readmixer_py.storage import SegmentStore
from readmixer_py.worker_memory import MemoryRef


RUN = bytes(range(16))


def cpu(count):
    return resolve_cpu_budget(observations=CpuObservations(count))


def frame(sample, ordinal):
    lines = (b'@private/1', b'ACGT', b'+opaque', b'!!!!',
             b'@private/2', b'TGCA', b'+opaque', b'####')
    return struct.pack('<IIQ8I', 48 + sum(map(len, lines)), sample, ordinal,
                       *map(len, lines)) + b''.join(lines)


def memory(budget, payload, owner=1, run=RUN):
    lease = budget.acquire('test-memory', max(1, len(payload)))
    ref = MemoryRef(payload, lease, run, owner, generation=13, budget=budget)
    assert lease.released  # handoff removed the old alias's lease authority
    return ref


def store_segment(store, payload):
    with store.new_segment(5, observations_identity('verify', 0)) as writer:
        writer.append(payload, item_count=2)
        return writer.seal()


@pytest.mark.parametrize('cpus', [1, 2])
def test_P13_T05_explicit_memory_ref_preserves_result_ownership_after_scheduler(cpus):
    budget = Budget(768 << 20)
    payload = frame(4, 0) + frame(1, 0)
    ref = memory(budget, payload)
    with StageScheduler(budget, cpu(cpus), RUN) as scheduler:
        pids = scheduler.child_pids
        assert scheduler.submit_memory(ref) == 0
        assert ref.bound
        if cpus > 1:
            assert not ref.released  # initial dispatch has not accepted input yet
        with pytest.raises(ConfigurationError):
            scheduler.submit_memory(ref)
        result = scheduler.take_next()
        assert ref.released
        assert result.data == frame(1, 0) + frame(4, 0)
        assert result.records == 2
    assert all(not psutil.pid_exists(pid) for pid in pids)
    assert result.data == frame(1, 0) + frame(4, 0)
    assert budget.used_bytes > 0
    result.close()
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0


@pytest.mark.parametrize('cpus', [1, 2])
def test_P13_T05_full_window_keeps_rejected_reference_unbound_and_owned(cpus):
    budget = Budget(768 << 20)
    with StageScheduler(budget, cpu(cpus), RUN, task_count=4) as scheduler:
        accepted = []
        for identity in range(scheduler.capacity):
            ref = memory(budget, frame(0, identity), identity)
            scheduler.submit_memory(ref)
            accepted.append(ref)
        rejected = memory(budget, frame(9, 0), 99)
        with pytest.raises(ResourceError, match='window is full'):
            scheduler.submit_memory(rejected)
        assert not rejected.bound and not rejected.released
        assert rejected.payload == frame(9, 0)
        rejected.cancel_after_reap()  # never issued to a worker
        for ref in accepted:
            with scheduler.take_next():
                pass
            assert ref.released
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0


@pytest.mark.parametrize('cpus', [1, 2])
def test_P13_T05_failed_admission_keeps_unbound_memory_with_caller(monkeypatch, cpus):
    budget = Budget(768 << 20)
    scheduler = StageScheduler(budget, cpu(cpus), RUN)
    pids = scheduler.child_pids
    ref = memory(budget, frame(0, 0))
    def pressure():
        raise ResourceError('injected aggregate pressure')
    monkeypatch.setattr(scheduler.admission, 'observe_pressure', pressure)
    with pytest.raises(ResourceError, match='aggregate pressure'):
        scheduler.submit_memory(ref)
    assert not ref.bound and not ref.released
    assert ref.payload == frame(0, 0)
    assert all(not psutil.pid_exists(pid) for pid in pids)
    assert budget.active_leases == 1
    ref.cancel_after_reap()
    assert budget.used_bytes == 0


@pytest.mark.parametrize('wrong', ['run', 'budget', 'released', 'oversized'])
def test_P13_T03_rejected_memory_identity_does_not_consume_ownership(wrong):
    budget = Budget(768 << 20)
    foreign = Budget(8 << 20)
    with StageScheduler(budget, cpu(1), RUN, max_job_bytes=128) as scheduler:
        ref = memory(foreign if wrong == 'budget' else budget,
                     b'x' * 129 if wrong == 'oversized' else frame(0, 0),
                     run=b'x' * 16 if wrong == 'run' else RUN)
        if wrong == 'released':
            ref.cancel_after_reap()
        with pytest.raises((ConfigurationError, ResourceError)):
            scheduler.submit_memory(ref)
        assert not ref.bound
        if wrong != 'released':
            assert not ref.released
        assert scheduler.pending_jobs == 0
        ref.cancel_after_reap()
    assert budget.used_bytes == foreign.used_bytes == 0


@pytest.mark.parametrize('cpus', [1, 2])
def test_P13_T03_failed_stage_reaps_before_releasing_bound_memory(cpus):
    budget = Budget(768 << 20)
    scheduler = StageScheduler(budget, cpu(cpus), RUN)
    pids = scheduler.child_pids
    ref = memory(budget, b'PRIVATE MALFORMED INPUT')
    with pytest.raises(IntegrityError) as caught:
        scheduler.submit_memory(ref)
        scheduler.take_next()
    assert 'PRIVATE' not in str(caught.value)
    assert ref.bound and ref.released
    assert all(not psutil.pid_exists(pid) for pid in pids)
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0


def test_P13_T05_unreaped_worker_keeps_bound_memory_for_cleanup_retry(monkeypatch):
    budget = Budget(768 << 20)
    scheduler = StageScheduler(budget, cpu(2), RUN)
    pids = scheduler.child_pids
    ref = memory(budget, frame(0, 0))
    stop = scheduler._stop_workers
    def fail_service():
        raise CancelledError('injected cancellation')
    def fail_reap():
        raise ResourceError('injected reap failure')
    monkeypatch.setattr(scheduler, '_service', fail_service)
    monkeypatch.setattr(scheduler, '_stop_workers', fail_reap)
    try:
        with pytest.raises(CancelledError, match='injected cancellation'):
            scheduler.submit_memory(ref)
        assert ref.bound and not ref.released
        assert ref.payload == frame(0, 0)
        assert any(psutil.pid_exists(pid) for pid in pids)
        assert budget.used_bytes > 0
    finally:
        monkeypatch.setattr(scheduler, '_stop_workers', stop)
        scheduler.abort()
    assert ref.released
    assert all(not psutil.pid_exists(pid) for pid in pids)
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0


@pytest.mark.parametrize('cpus', [1, 2])
def test_P13_T05_memory_segment_uses_raw_transfer_without_spill(tmp_path, cpus):
    budget = Budget(768 << 20)
    keys = OperationKeys._from_test(bytes(range(32)), RUN)
    payload = frame(4, 0) + frame(1, 0)
    with SegmentStore('memory', budget, keys, temp_dir=tmp_path) as store:
        source = store_segment(store, payload)
        with StageScheduler(budget, cpu(cpus), RUN, store=store) as scheduler:
            pids = scheduler.child_pids
            assert scheduler.storage is None
            assert scheduler.submit_segment(source) == 0
            with scheduler.take_next() as result:
                assert result.data == frame(1, 0) + frame(4, 0)
                assert result.records == 2
            assert store.scratch_directory is None
            assert list(tmp_path.iterdir()) == []
        assert all(not psutil.pid_exists(pid) for pid in pids)
        with store.open_verified(source) as reader:
            assert reader.read() == payload
        store.drop(source)
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize('cpus', [1, 2])
def test_P13_T03_failed_memory_segment_preserves_original_store_input(tmp_path, cpus):
    budget = Budget(768 << 20)
    keys = OperationKeys._from_test(bytes(range(32)), RUN)
    payload = b'PRIVATE MALFORMED PAIR STREAM'
    with SegmentStore('memory', budget, keys, temp_dir=tmp_path) as store:
        source = store_segment(store, payload)
        scheduler = StageScheduler(budget, cpu(cpus), RUN, store=store)
        pids = scheduler.child_pids
        with pytest.raises(IntegrityError) as caught:
            scheduler.submit_segment(source)
            scheduler.take_next()
        assert 'PRIVATE' not in str(caught.value)
        assert all(not psutil.pid_exists(pid) for pid in pids)
        assert store.scratch_directory is None
        with store.open_verified(source) as reader:
            assert reader.read() == payload
        store.drop(source)
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
    assert list(tmp_path.iterdir()) == []
