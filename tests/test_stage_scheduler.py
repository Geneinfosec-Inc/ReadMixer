"""Real spawned memory workers; no production parallel-pipeline claim."""
import hashlib
import os
import struct
import subprocess
import sys
import textwrap

import pytest

from readmixer_py.errors import ConfigurationError, IntegrityError, ResourceError
from readmixer_py.execution import CpuObservations, resolve_cpu_budget, CompressionService
from readmixer_py.resources import Budget
from readmixer_py.stage_scheduler import StageScheduler
from readmixer_py import stage_scheduler
from readmixer_py.worker_transport import FramedSocket, socket_pair
from readmixer_py.worker_protocol import Chunk, HEADER_BYTES, Stop


def cpu(count):
    return resolve_cpu_budget(observations=CpuObservations(count))


def frame(sample, ordinal, bases=3):
    lines = (b'@private/1', b'A' * bases, b'+opaque', b'!' * bases,
             b'@private/2', b'C' * bases, b'+opaque', b'#' * bases)
    n = 48 + sum(map(len, lines))
    return struct.pack('<IIQ8I', n, sample, ordinal, *map(len, lines)) + b''.join(lines)


def submit(scheduler, data):
    owner = scheduler.budget.acquire('test-input', max(1, len(data)))
    try:
        return scheduler.submit(data, owner), owner
    except BaseException:
        owner.release()
        raise


@pytest.mark.parametrize('cpus', [1, 2, 4])
def test_P13_T01_T05_real_sort_jobs_and_caller_result_lifetime(cpus):
    budget = Budget(768 << 20, descriptors=256)
    before = {name: os.environ.get(name) for name in stage_scheduler._NATIVE_LIMITS}
    with StageScheduler(budget, cpu(cpus), b'r' * 16, task_count=8) as scheduler:
        payloads = []
        owners = []
        for ordinal in range(scheduler.capacity):
            data = frame(1, ordinal) + frame(0, ordinal)
            _, owner = submit(scheduler, data)
            payloads.append(frame(0, ordinal) + frame(1, ordinal))
            owners.append(owner)
        assert scheduler.pending_jobs == scheduler.capacity
        assert scheduler.pending_bytes <= scheduler.byte_ceiling
        with pytest.raises(ResourceError, match='window is full'):
            submit(scheduler, frame(2, 0))
        results = [scheduler.take_next() for _ in payloads]
        assert [result.data for result in results] == payloads
        assert all(result.records == 2 for result in results)
        assert all(owner.released for owner in owners)
        assert scheduler.workers_effective == (1 if cpus == 1 else cpus)
        pids = scheduler.child_pids
    assert {name: os.environ.get(name) for name in stage_scheduler._NATIVE_LIMITS} == before
    assert budget.used_bytes > 0  # returned bytes remain caller-owned after reaping
    import psutil
    assert all(not psutil.pid_exists(pid) for pid in pids)
    for result in results:
        result.close()
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0


def test_P13_T05_multichunk_transfer_and_empty_job():
    budget = Budget(1024 << 20, descriptors=128)
    with StageScheduler(budget, cpu(2), b'r' * 16, max_job_bytes=3 << 20) as scheduler:
        first, second = frame(9, 4, 200000), frame(3, 2, 200000)
        assert len(first + second) > 1 << 20
        submit(scheduler, first + second)
        submit(scheduler, b'')
        with scheduler.take_next() as result:
            assert result.data == second + first
        with scheduler.take_next() as result:
            assert result.data == b'' and result.records == 0
    assert budget.used_bytes == budget.used_descriptors == 0


@pytest.mark.parametrize('payload', [b'PRIVATE READ BYTES', frame(1, 0) * 2])
def test_P13_T03_bad_frames_fail_privately_and_reap(payload):
    # This scenario targets worker payload validation. Leave enough room for
    # the full-suite coordinator RSS so pressure admission cannot preempt it.
    budget = Budget(512 << 20, descriptors=128)
    with pytest.raises(IntegrityError) as caught:
        with StageScheduler(budget, cpu(2), b'r' * 16) as scheduler:
            pids = scheduler.child_pids
            submit(scheduler, payload)
            scheduler.take_next()
    assert 'PRIVATE' not in str(caught.value) and 'private' not in str(caught.value)
    import psutil
    assert all(not psutil.pid_exists(pid) for pid in pids)
    assert budget.used_bytes == budget.used_descriptors == 0


def test_P13_T01_failed_start_restores_environment_and_releases(monkeypatch):
    budget = Budget(256 << 20, descriptors=128)
    before = {name: os.environ.get(name) for name in stage_scheduler._NATIVE_LIMITS}
    from multiprocessing.process import BaseProcess
    def fail(_self):
        assert all(os.environ[name] == '1' for name in stage_scheduler._NATIVE_LIMITS)
        raise OSError('private failure detail')
    monkeypatch.setattr(BaseProcess, 'start', fail)
    with pytest.raises(ResourceError):
        StageScheduler(budget, cpu(2), b'r' * 16)
    assert {name: os.environ.get(name) for name in stage_scheduler._NATIVE_LIMITS} == before
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0


def test_P13_T01_cpu_one_never_launches(monkeypatch):
    monkeypatch.setattr(stage_scheduler.multiprocessing, 'get_context',
                        lambda *_: pytest.fail('CPU one requested a process context'))
    # The full pytest process retains imports from other workflow tests.
    # This case proves serial execution, not a minimum-RSS qualification.
    budget = Budget(512 << 20)
    with StageScheduler(budget, cpu(1), b'r' * 16) as scheduler:
        submit(scheduler, frame(0, 0))
        with scheduler.take_next() as result:
            assert result.records == 1
    assert budget.used_bytes == 0


def test_P13_T02_fixed_mix_compression_partition():
    budget = Budget(512 << 20, descriptors=128)
    plan = cpu(4)
    with StageScheduler(budget, plan, b'r' * 16, operation='mix', task_count=4) as scheduler:
        assert scheduler.process_slots == 2
        with CompressionService(budget, plan, encoder=lambda payload, level: payload) as compression:
            assert compression.slots == 1
            assert compression.actual_cpu_slots == scheduler.workers_effective + compression.slots == 4
    assert budget.used_bytes == 0


def test_P13_T07_unexpected_exit_never_becomes_success():
    budget = Budget(256 << 20, descriptors=128)
    with pytest.raises(ResourceError):
        with StageScheduler(budget, cpu(2), b'r' * 16) as scheduler:
            submit(scheduler, frame(0, 0))
            scheduler._workers[0].process.kill()
            scheduler._workers[0].process.join(timeout=3)
            scheduler.take_next()
    assert budget.used_bytes == budget.used_descriptors == 0


def test_P13_T01_guarded_library_launch_in_fresh_interpreter(tmp_path):
    import readmixer_py
    script = tmp_path / 'guarded.py'
    script.write_text(textwrap.dedent('''
        import os
        # This executes while spawn reimports main, BEFORE worker target imports.
        if __name__ == '__mp_main__':
            for key in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','VECLIB_MAXIMUM_THREADS','NUMEXPR_NUM_THREADS'):
                assert os.environ[key] == '1'
        if __name__ == '__main__':
            from readmixer_py.resources import Budget
            from readmixer_py.execution import CpuObservations,resolve_cpu_budget
            from readmixer_py.stage_scheduler import StageScheduler
            budget=Budget(256<<20)
            with StageScheduler(budget,resolve_cpu_budget(observations=CpuObservations(2)),b'x'*16) as scheduler:
                assert scheduler.process_slots == 1
            assert budget.used_bytes == budget.used_descriptors == 0
    '''))
    env = os.environ.copy()
    env['PYTHONPATH'] = str(__import__('pathlib').Path(readmixer_py.__file__).resolve().parent.parent)
    result = subprocess.run([sys.executable, str(script)], env=env, capture_output=True, timeout=30)
    assert result.returncode == 0, result.stderr[-3000:]


def test_P13_T03_transport_checks_header_before_body_and_limits_pending_frame():
    left, right = socket_pair()
    channel = FramedSocket(left)
    try:
        with pytest.raises(ResourceError):
            channel.send(Stop())
            channel.send(Stop())
        # A valid header with an impossible body size must fail without waiting
        # for the attacker to send that body or allocating the declared amount.
        from readmixer_py.worker_protocol import encode_message
        wire = bytearray(encode_message(Stop()))
        wire[-4:] = (0xFFFFFFFF).to_bytes(4, 'big')
        right.sendall(wire[:HEADER_BYTES])
        with pytest.raises(IntegrityError):
            channel.pump(0.1)
        assert channel._body is None
    finally:
        channel.close();right.close()
