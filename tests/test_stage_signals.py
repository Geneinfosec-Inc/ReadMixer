"""P13-T01/T07 fresh-module signal harness; not production CLI parallelism."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import textwrap
import time

import psutil
import pytest
import readmixer_py


HARNESS = r'''
import json
import os
from pathlib import Path
import signal
import sys

if __name__ == '__mp_main__':
    # Spawn reimports this guarded module before it reaches the test worker.
    for name in ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS',
                 'VECLIB_MAXIMUM_THREADS', 'NUMEXPR_NUM_THREADS'):
        assert os.environ[name] == '1'


def blocked_worker(sock, run_id, max_job_bytes, workspace_bytes, *extra):
    import psutil
    from readmixer_py.worker_protocol import InputDone, Ready, Stop
    from readmixer_py.worker_transport import FramedSocket
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    channel = FramedSocket(sock)
    try:
        process = psutil.Process()
        channel.send(Ready(process.memory_info().rss, process.num_fds(), process.num_threads()))
        while True:
            channel.pump(0.01)
            message = channel.receive()
            if isinstance(message, InputDone):
                # Prove the coordinator reached the result wait with a live,
                # fully transferred input owner before its signal is injected.
                Path(os.environ['TEST_STAGE_CHECKPOINT']).write_text('input-done')
            if isinstance(message, Stop):
                return
    finally:
        channel.close()


def main():
    import psutil
    from multiprocessing import resource_tracker
    from readmixer_py import stage_scheduler as module
    from readmixer_py.cli import _signals
    from readmixer_py.errors import CancelledError
    from readmixer_py.execution import CpuPlan
    from readmixer_py.resources import Budget

    # The interpreter-owned helper can persist after invocation close. Start it
    # before the descriptor baseline so this test measures scheduler-owned FDs.
    resource_tracker.ensure_running()
    before_fds = psutil.Process().num_fds()
    before_handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    environment = {name: os.environ.get(name) for name in module._NATIVE_LIMITS}
    module.worker_main = blocked_worker
    budget = Budget(512 << 20)
    plan = CpuPlan(2, 2, 2, None, 2, ('signal-harness',))
    scheduler = None
    pids = ()
    caught = None
    try:
        with _signals():
            with module.StageScheduler(budget, plan, bytes(16), max_job_bytes=1024) as scheduler:
                pids = scheduler.child_pids
                Path(os.environ['TEST_STAGE_PIDS']).write_text(json.dumps(list(pids)))
                source = budget.acquire('test-signal-source', 1)
                scheduler.submit(b'', source)
                scheduler.take_next()
                raise AssertionError('blocked worker returned before coordinator signal')
    except CancelledError as error:
        caught = error
    assert caught is not None and caught.signal in (signal.SIGINT, signal.SIGTERM)
    assert scheduler is not None and scheduler._closed
    assert scheduler.cancellation.error is caught
    assert source.released
    assert not any(psutil.pid_exists(pid) for pid in pids)
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
    assert budget.active_leases == 0
    assert psutil.Process().num_fds() == before_fds
    assert all(signal.getsignal(sig) == previous for sig, previous in before_handlers.items())
    assert {name: os.environ.get(name) for name in module._NATIVE_LIMITS} == environment
    print(json.dumps({'signal': caught.signal, 'workers': list(pids), 'reaped': True,
                      'source_released': True, 'leases': 0, 'descriptors_restored': True,
                      'signal_handlers_restored': True, 'first_cause_preserved': True}), flush=True)
    return 128 + caught.signal


if __name__ == '__main__':
    raise SystemExit(main())
'''


@pytest.mark.skipif(os.name != 'posix', reason='POSIX signal and process-group acceptance')
@pytest.mark.parametrize('signum', [signal.SIGINT, signal.SIGTERM], ids=['SIGINT', 'SIGTERM'])
def test_P13_T07_module_harness_real_coordinator_signals_reap_pending_worker(tmp_path, signum):
    module = tmp_path / 'stage_signal_harness.py'
    module.write_text(textwrap.dedent(HARNESS))
    checkpoint = tmp_path / 'checkpoint'
    pids_path = tmp_path / 'worker-pids.json'
    env = os.environ.copy()
    env['PYTHONPATH'] = str(Path(readmixer_py.__file__).resolve().parent.parent)
    env['TEST_STAGE_CHECKPOINT'] = str(checkpoint)
    env['TEST_STAGE_PIDS'] = str(pids_path)
    # The launch lock must restore exact caller values after child startup.
    env['OMP_NUM_THREADS'] = '3'
    env['OPENBLAS_NUM_THREADS'] = '7'
    process = subprocess.Popen([sys.executable, '-m', 'stage_signal_harness'], cwd=tmp_path,
                               env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               start_new_session=True)
    reached = False
    try:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if checkpoint.exists():
                reached = checkpoint.read_text() == 'input-done'
                if reached:
                    break
            if process.poll() is not None:
                break
            time.sleep(0.01)
        if not reached:
            process.kill()
            stdout, stderr = process.communicate(timeout=5)
            pytest.fail(f'signal harness did not reach input acceptance: {stderr[-2000:]!r}')
        pids = json.loads(pids_path.read_text())
        assert len(pids) == 1 and all(psutil.pid_exists(pid) for pid in pids)
        process.send_signal(signum)
        stdout, stderr = process.communicate(timeout=25)
        assert process.returncode == 128 + signum, stderr[-2000:]
        assert stderr == b''
        result = json.loads(stdout)
        assert result == {'signal': signum, 'workers': pids, 'reaped': True,
                          'source_released': True, 'leases': 0, 'descriptors_restored': True,
                          'signal_handlers_restored': True, 'first_cause_preserved': True}
        assert all(not psutil.pid_exists(pid) for pid in pids)
    finally:
        # This session belongs solely to this temporary test harness. Reap even
        # a broken coordinator and its helper/worker descendants on timeout.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=5)


CONCURRENT_HARNESS = r'''
import json
import os
import threading

if __name__ == '__mp_main__':
    for name in ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS',
                 'VECLIB_MAXIMUM_THREADS', 'NUMEXPR_NUM_THREADS'):
        assert os.environ[name] == '1'


def main():
    import psutil
    from multiprocessing import resource_tracker
    from readmixer_py import stage_scheduler as module
    from readmixer_py.execution import CpuPlan
    from readmixer_py.resources import Budget

    resource_tracker.ensure_running()
    before_fds = psutil.Process().num_fds()
    previous = {name: os.environ.get(name) for name in module._NATIVE_LIMITS}
    launch = threading.Barrier(2, timeout=10)
    ready = threading.Barrier(2, timeout=10)
    inspected = threading.Barrier(2, timeout=10)
    failures = []
    pids = []
    finished = []
    def run(index):
        try:
            budget = Budget(512 << 20)
            plan = CpuPlan(2, 2, 2, None, 2, ('concurrent-harness',))
            launch.wait()
            with module.StageScheduler(budget, plan, bytes([index]) * 16,
                                       max_job_bytes=1024) as scheduler:
                assert scheduler.process_slots == 1
                pids.extend(scheduler.child_pids)
                ready.wait()  # Both real worker processes must coexist.
                assert len(pids) == 2
                assert all(psutil.pid_exists(pid) for pid in pids)
                inspected.wait()  # Neither child can close before both checks.
                source = budget.acquire('test-concurrent-source', 1)
                scheduler.submit(b'', source)
                with scheduler.take_next() as result:
                    assert result.data == b'' and result.records == 0
                assert source.released
            assert budget.used_bytes == budget.used_descriptors == budget.used_metadata_bytes == 0
            assert budget.active_leases == 0
            finished.append(index)
        except BaseException as error:
            failures.append(type(error).__name__)
    threads = [threading.Thread(target=run, args=(index,)) for index in (1, 2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=20)
    assert not any(thread.is_alive() for thread in threads)
    assert not failures, failures
    assert sorted(finished) == [1, 2]
    assert len(set(pids)) == 2
    assert all(not psutil.pid_exists(pid) for pid in pids)
    assert {name: os.environ.get(name) for name in module._NATIVE_LIMITS} == previous
    assert psutil.Process().num_fds() == before_fds
    print(json.dumps({'workers': pids, 'concurrent': True, 'reaped': True,
                      'environment_restored': True, 'descriptors_restored': True}), flush=True)


if __name__ == '__main__':
    main()
'''


@pytest.mark.skipif(os.name != 'posix', reason='POSIX spawn descriptor and watchdog acceptance')
def test_P13_T01_guarded_module_concurrent_real_scheduler_launches_restore_environment(tmp_path):
    module = tmp_path / 'stage_concurrent_harness.py'
    module.write_text(textwrap.dedent(CONCURRENT_HARNESS))
    env = os.environ.copy()
    env['PYTHONPATH'] = str(Path(readmixer_py.__file__).resolve().parent.parent)
    env['OMP_NUM_THREADS'] = '3'
    env['OPENBLAS_NUM_THREADS'] = '7'
    process = subprocess.Popen([sys.executable, '-m', 'stage_concurrent_harness'], cwd=tmp_path,
                               env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               start_new_session=True)
    try:
        stdout, stderr = process.communicate(timeout=35)
        assert process.returncode == 0, stderr[-3000:]
        assert stderr == b''
        result = json.loads(stdout)
        assert result['concurrent'] is result['reaped'] is result['environment_restored'] is True
        assert result['descriptors_restored'] is True
        assert len(result['workers']) == 2
        assert all(not psutil.pid_exists(pid) for pid in result['workers'])
    finally:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=5)


def test_P13_T02_idle_and_pending_service_poll_aggregate_pressure_at_bounded_interval(monkeypatch):
    from readmixer_py import stage_scheduler as module
    from readmixer_py.execution import CpuPlan
    from readmixer_py.resources import Budget

    budget = Budget(512 << 20)
    plan = CpuPlan(2, 2, 2, None, 2, ('pressure-poll',))
    with module.StageScheduler(budget, plan, bytes(16), max_job_bytes=1024) as scheduler:
        original = scheduler.admission.observe_pressure
        observations = []
        def observe():
            facts = original()
            observations.append(facts)
            return facts
        monkeypatch.setattr(scheduler.admission, 'observe_pressure', observe)
        clock = [100.0]
        # A local clock scope ends before shutdown; real watchdog deadlines retain
        # their monotonic implementation even if an assertion fails.
        with monkeypatch.context() as scoped:
            scoped.setattr(module.time, 'monotonic', lambda: clock[0])
            scheduler._last_pressure = 0
            scheduler._service()
            assert len(observations) == 1
            scheduler._service()
            assert len(observations) == 1
            clock[0] += 0.11
            scheduler._service()
            assert len(observations) == 2
        assert all(not lease.released for lease in scheduler.admission.baseline_leases)
        source = budget.acquire('test-pressure-source', 1)
        scheduler.submit(b'', source)
        before = len(observations)
        scheduler._last_pressure = 0
        scheduler._service()
        assert len(observations) == before + 1 and scheduler.pending_jobs == 1
        for facts in observations:
            assert set(scheduler.child_pids).issubset({item.identity.pid for item in facts.workers})
            assert all(item.threads == 1 for item in facts.workers)
        with scheduler.take_next() as result:
            assert result.data == b''
    assert budget.active_leases == budget.used_bytes == budget.used_descriptors == 0
