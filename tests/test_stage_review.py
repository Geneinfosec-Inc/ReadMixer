"""Independent lifecycle regressions for the private memory worker increment."""

import inspect
import sys

import pytest

from readmixer_py.errors import ResourceError
from readmixer_py.execution import CpuPlan
from readmixer_py.resources import Budget
from readmixer_py import stage_scheduler as module


def cpu(count):
    return CpuPlan(count, count, count, None, count, ('test',))


def test_P13_T01_channel_constructor_failure_closes_both_unregistered_sockets(monkeypatch):
    budget = Budget(512 << 20)
    captured = []
    original_pair = module.socket_pair
    def pair():
        result = original_pair()
        captured.extend(result)
        return result
    def fail_channel(sock):
        raise MemoryError('injected channel allocation failure')
    monkeypatch.setattr(module, 'socket_pair', pair)
    monkeypatch.setattr(module, 'FramedSocket', fail_channel)
    try:
        with pytest.raises((MemoryError, ResourceError)) as caught:
            module.StageScheduler(budget, cpu(2), bytes(16), max_job_bytes=1024)
        assert caught.value.__traceback__ is not None
        assert captured and all(sock.fileno() == -1 for sock in captured)
        assert budget.used_bytes == budget.used_descriptors == budget.used_metadata_bytes == 0
    finally:
        # Keep an initially failing regression from leaking descriptors in pytest.
        for sock in captured:
            sock.close()


def test_P13_T05_interrupted_result_handoff_releases_unreturned_result_owner():
    budget = Budget(512 << 20)
    scheduler = module.StageScheduler(budget, cpu(1), bytes(16), max_job_bytes=1024)
    source = budget.acquire('test-source', 1)
    scheduler.submit(b'', source)
    lines, first = inspect.getsourcelines(module.StageScheduler.take_next)
    target = next(first + index for index, text in enumerate(lines) if text.strip() == 'job.output = None')
    interruption = KeyboardInterrupt()
    reached = []
    def trace(frame, event, arg):
        if frame.f_code is module.StageScheduler.take_next.__code__ and event == 'line' and frame.f_lineno == target:
            reached.append(target)
            raise interruption
        return trace
    previous = sys.gettrace()
    try:
        sys.settrace(trace)
        with pytest.raises(KeyboardInterrupt) as caught:
            scheduler.take_next()
    finally:
        sys.settrace(previous)
    try:
        assert reached == [target] and caught.value is interruption
        assert scheduler._closed
        assert budget.used_bytes == budget.used_descriptors == budget.used_metadata_bytes == 0
        assert budget.active_leases == 0
    finally:
        scheduler.abort()
        # Avoid retaining the deliberately exposed missing owner if not fixed yet.
        for lease in tuple(budget._state.leases):
            lease.release()


def test_P13_T07_secondary_interrupt_during_shutdown_still_reaps_live_child(monkeypatch):
    import psutil

    budget = Budget(512 << 20)
    scheduler = module.StageScheduler(budget, cpu(2), bytes(16), max_job_bytes=1024)
    pids = scheduler.child_pids
    channel = scheduler._workers[0].channel
    original = channel.pump
    triggered = []
    def interrupted(*args, **kwargs):
        if not triggered:
            triggered.append(True)
            raise KeyboardInterrupt()
        return original(*args, **kwargs)
    monkeypatch.setattr(channel, 'pump', interrupted)
    # Short watchdog bounds are test-only. A healthy idle worker cooperatively
    # stops; the original error must remain first even if cleanup is interrupted.
    monkeypatch.setattr(module, '_GRACE_SECONDS', 0.3)
    monkeypatch.setattr(module, '_TERMINATE_SECONDS', 0.3)
    monkeypatch.setattr(module, '_REAP_SECONDS', 0.3)
    primary = ResourceError('injected causal failure')
    try:
        assert scheduler._fail(primary) is primary
        assert triggered
        assert scheduler._closed
        assert all(not psutil.pid_exists(pid) for pid in pids)
        assert budget.active_leases == budget.used_descriptors == budget.used_metadata_bytes == 0
        assert budget.used_bytes == 0
    finally:
        monkeypatch.setattr(channel, 'pump', original)
        scheduler.abort()


def _identity_frame():
    import struct

    lines = (b'@symbolic/1', b'XY', b'+', b'!!',
             b'@symbolic/2', b'YX', b'+', b'##')
    return struct.pack('<IIQ8I', 48 + sum(map(len, lines)), 0, 0,
                       *map(len, lines)) + b''.join(lines)


def test_P13_T05_one_sender_lease_cannot_back_two_live_jobs():
    from readmixer_py.errors import ConfigurationError

    budget = Budget(512 << 20)
    source = budget.acquire('test-exclusive-sender', 1024)
    payload = _identity_frame()
    try:
        with module.StageScheduler(budget, cpu(2), bytes(16), max_job_bytes=1024) as scheduler:
            assert scheduler.submit(payload, source) == 0
            assert not source.released
            with pytest.raises(ConfigurationError, match='already borrowed'):
                scheduler.submit(payload, source)
            assert scheduler.pending_jobs == 1
            with scheduler.take_next() as result:
                assert result.task_id == 0 and result.records == 1
                assert result.data == payload
            assert source.released
        assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
    finally:
        source.release()


def _ignores_cooperative_stop(sock, run_id, max_job_bytes, workspace_bytes):
    _shutdown_test_worker(sock, ignore_terminate=False)


def _ignores_cooperative_and_terminate(sock, run_id, max_job_bytes, workspace_bytes):
    _shutdown_test_worker(sock, ignore_terminate=True)


def _shutdown_test_worker(sock, *, ignore_terminate):
    """Real spawned, READY-reporting worker that deliberately ignores STOP."""
    import signal
    import time
    import psutil

    from readmixer_py.worker_protocol import Ready
    from readmixer_py.worker_transport import FramedSocket

    signal.signal(signal.SIGINT, signal.SIG_IGN)
    if ignore_terminate:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    process = psutil.Process()
    channel = FramedSocket(sock)
    channel.send(Ready(process.memory_info().rss, process.num_fds(), process.num_threads()))
    while True:
        channel.pump(0.01)
        channel.receive()  # Intentionally discard STOP and all application frames.
        time.sleep(0.002)


def test_P13_T07_production_shutdown_grace_bounds():
    assert module._GRACE_SECONDS == 10.0
    assert module._TERMINATE_SECONDS == 5.0
    assert module._REAP_SECONDS == 5.0


def _short_shutdown_bounds(monkeypatch):
    monkeypatch.setattr(module, '_GRACE_SECONDS', 0.1)
    monkeypatch.setattr(module, '_TERMINATE_SECONDS', 0.1)
    monkeypatch.setattr(module, '_REAP_SECONDS', 0.5)


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX signal escalation semantics')
@pytest.mark.parametrize('ignore_terminate', [False, True], ids=['terminate', 'kill'])
def test_P13_T07_real_uncooperative_child_is_escalated_and_reaped(monkeypatch, ignore_terminate):
    import signal
    import psutil

    target = _ignores_cooperative_and_terminate if ignore_terminate else _ignores_cooperative_stop
    monkeypatch.setattr(module, 'worker_main', target)
    _short_shutdown_bounds(monkeypatch)
    budget = Budget(512 << 20)
    scheduler = module.StageScheduler(budget, cpu(2), bytes(16), max_job_bytes=1024)
    worker = scheduler._workers[0]
    process, pid = worker.process, worker.process.pid
    called = []
    exitcodes = []
    original_terminate, original_kill, original_close = process.terminate, process.kill, process.close
    def terminate():
        called.append('terminate')
        original_terminate()
    def kill():
        called.append('kill')
        original_kill()
    def close():
        exitcodes.append(process.exitcode)
        original_close()
    monkeypatch.setattr(process, 'terminate', terminate)
    monkeypatch.setattr(process, 'kill', kill)
    monkeypatch.setattr(process, 'close', close)
    primary = ResourceError('injected stage failure')
    try:
        assert scheduler._fail(primary) is primary
        assert scheduler.cancellation.error is primary
        assert worker.stop_sent
        assert called == (['terminate', 'kill'] if ignore_terminate else ['terminate'])
        assert exitcodes == [-int(signal.SIGKILL if ignore_terminate else signal.SIGTERM)]
        assert scheduler._closed and not psutil.pid_exists(pid)
        assert budget.active_leases == budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
    finally:
        scheduler.abort()


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX signal escalation semantics')
@pytest.mark.parametrize('failure', ['kill', 'reap'])
def test_P13_T07_unreaped_child_retains_all_admission_until_successful_retry(monkeypatch, failure):
    import psutil

    monkeypatch.setattr(module, 'worker_main', _ignores_cooperative_and_terminate)
    _short_shutdown_bounds(monkeypatch)
    budget = Budget(512 << 20)
    scheduler = module.StageScheduler(budget, cpu(2), bytes(16), max_job_bytes=1024)
    process = scheduler._workers[0].process
    pid = process.pid
    source = budget.acquire('test-sender-retained-until-reap', 1024)
    original_kill, original_alive = process.kill, process.is_alive
    attempted = []
    def cannot_kill():
        attempted.append('kill')
        raise OSError('injected kill refusal')
    def kill_then_hide_reap():
        attempted.append('kill')
        original_kill()
        monkeypatch.setattr(process, 'is_alive', lambda: True)
    monkeypatch.setattr(process, 'kill', cannot_kill if failure == 'kill' else kill_then_hide_reap)
    primary = ResourceError('injected first cause')
    try:
        scheduler.submit(_identity_frame(), source)
        retained = (budget.used_bytes, budget.used_metadata_bytes, budget.used_descriptors, budget.active_leases)
        assert scheduler._fail(primary) is primary
        assert scheduler.cancellation.error is primary
        assert attempted == ['kill']
        assert not scheduler._closed and scheduler._closing
        assert budget._stage_scheduler is scheduler
        assert scheduler._workers[0].process is process
        assert not source.released
        assert (budget.used_bytes, budget.used_metadata_bytes, budget.used_descriptors, budget.active_leases) == retained
        if failure == 'kill':
            assert psutil.pid_exists(pid) and original_alive()
    finally:
        # A failed reap deliberately keeps the real process handle and its grants.
        # Restore reliable OS operations so no child escapes this bounded test.
        monkeypatch.setattr(process, 'kill', original_kill)
        monkeypatch.setattr(process, 'is_alive', original_alive)
        scheduler.abort(primary)
        source.release()
    assert scheduler._closed and not psutil.pid_exists(pid)
    assert budget.active_leases == budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
