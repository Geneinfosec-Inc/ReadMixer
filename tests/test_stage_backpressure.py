"""P13-T06: real private workers with a deterministically gated first result."""
from contextlib import nullcontext
from functools import partial
import multiprocessing
import struct
import time

import psutil
import pytest

from readmixer_py import stage_scheduler
from readmixer_py.errors import CancelledError, ResourceError
from readmixer_py.execution import CpuObservations, resolve_cpu_budget
from readmixer_py.resources import Budget
from readmixer_py.worker_protocol import WorkerResult


def _gated_worker(sock, run_id, max_job_bytes, workspace_bytes, *, gate):
    # Test-only spawn target; production decoding, sorting, ACKs and STOP handling
    # are unchanged. Only worker zero's first sort waits for the parent gate.
    from readmixer_py import worker_bootstrap
    original = worker_bootstrap.sort_identity_frames
    first = True

    def sort(payload, *, checkpoint):
        nonlocal first
        if first and multiprocessing.current_process().name.endswith('-0'):
            deadline = time.monotonic() + 10
            while not gate.wait(0.01):
                checkpoint()  # cooperative STOP must work under backpressure
                if time.monotonic() >= deadline:
                    raise ResourceError('test result gate exceeded watchdog')
        first = False
        return original(payload, checkpoint=checkpoint)

    worker_bootstrap.sort_identity_frames = sort
    worker_bootstrap.worker_main(sock, run_id, max_job_bytes, workspace_bytes)


def _frame(ordinal):
    fields = (b'@synthetic/1', b'A', b'+', b'!',
              b'@synthetic/2', b'C', b'+', b'#')
    return struct.pack('<IIQ8I', 48 + sum(map(len, fields)), 0, ordinal,
                       *map(len, fields)) + b''.join(fields)


@pytest.mark.parametrize('cancel_blocked', [False, True], ids=['ordered-drain', 'cancel'])
def test_P13_T06_delayed_first_result_bounds_window_and_services_stop(monkeypatch, cancel_blocked):
    gate = multiprocessing.get_context('spawn').Event()
    monkeypatch.setattr(stage_scheduler, 'worker_main', partial(_gated_worker, gate=gate))
    budget = Budget(768 << 20, descriptors=256)
    cpu = resolve_cpu_budget(observations=CpuObservations(3))
    deadline = time.monotonic() + 15
    cancelled = False
    owners = []
    completed = []
    results = []
    scheduler = None
    pids = ()
    original = stage_scheduler.StageScheduler._message

    def observe(self, index, worker, message):
        original(self, index, worker, message)
        if isinstance(message, WorkerResult):
            completed.append(message.task_id)

    monkeypatch.setattr(stage_scheduler.StageScheduler, '_message', observe)
    expected = pytest.raises(CancelledError) if cancel_blocked else nullcontext()
    try:
        with expected as caught:
            with stage_scheduler.StageScheduler(
                    budget, cpu, b'b' * 16, task_count=4, max_job_bytes=1024,
                    cancel=lambda: cancelled or time.monotonic() >= deadline) as scheduler:
                pids = scheduler.child_pids
                assert scheduler.process_slots == 2 and scheduler.capacity == 4
                for ordinal in range(4):
                    payload = _frame(ordinal)
                    owner = budget.acquire('backpressure-input', len(payload))
                    owners.append(owner)
                    scheduler.submit(payload, owner)
                while not all(scheduler._jobs[n].result_ack for n in (2, 3)):
                    scheduler._service()
                # The checkpoint proves actual out-of-order completions, not a
                # sleep-based assumption about relative worker speed.
                assert completed == [2, 3]
                assert not scheduler._jobs[0].complete
                assert scheduler.pending_jobs == scheduler.peak_pending == 4
                assert scheduler.pending_bytes == sum(len(_frame(n)) for n in range(4))
                assert scheduler.pending_bytes <= scheduler.byte_ceiling
                assert all(not lease.released for lease in scheduler._slots)
                assert all(scheduler._jobs[n].output == _frame(n) for n in (2, 3))
                assert [owner.released for owner in owners] == [False, False, True, True]
                extra = budget.acquire('backpressure-extra', len(_frame(4)))
                try:
                    before = (budget.used_bytes, budget.used_metadata_bytes, budget.used_descriptors)
                    with pytest.raises(ResourceError, match='window is full'):
                        scheduler.submit(_frame(4), extra)
                    assert not extra.released
                    assert before == (budget.used_bytes, budget.used_metadata_bytes, budget.used_descriptors)
                finally:
                    extra.release()
                if cancel_blocked:
                    cancelled = True  # leave the gate closed during STOP/reaping
                else:
                    gate.set()
                for ordinal in range(4):
                    result = scheduler.take_next()
                    results.append(result)
                    assert result.task_id == ordinal and result.data == _frame(ordinal)
                    result.close()
        assert scheduler._closed and all(owner.released for owner in owners)
        assert all(not psutil.pid_exists(pid) for pid in pids)
        assert budget.active_leases == 0
        assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
        if cancel_blocked:
            assert not gate.is_set()
            assert scheduler.cancellation.error is caught.value
        else:
            assert completed == [2, 3, 0, 1]
    finally:
        gate.set()
        if scheduler is not None:
            scheduler.abort()
        for result in results:
            result.close()
        for owner in owners:
            owner.release()
