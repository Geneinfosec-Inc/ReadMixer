"""Real spawned peers forge closed messages after a measured READY handshake.

These tests exercise coordinator transfer states, rather than bypassing the
socket with calls to ``_message``. The fake peer waits for STOP after its bad
message so an incidental child exit cannot masquerade as protocol rejection.
"""

import hashlib
import struct
import time
import traceback

import psutil
import pytest

from readmixer_py import stage_scheduler as module
from readmixer_py.errors import IntegrityError
from readmixer_py.execution import CpuObservations, resolve_cpu_budget
from readmixer_py.resources import Budget
from readmixer_py.worker_protocol import (
    Chunk, ChunkAck, HEADER_BYTES, InputDone, Ready, Stop, WorkerJob,
    WorkerResult, decode_message, encode_message, inspect_header,
)


_PRIVATE = b'OPAQUE-PRIVATE-READ-AND-HEADER'


def _read_exact(sock, size):
    parts = bytearray()
    while len(parts) < size:
        piece = sock.recv(size - len(parts))
        if not piece:
            raise EOFError
        parts.extend(piece)
    return bytes(parts)


def _receive(sock):
    header = _read_exact(sock, HEADER_BYTES)
    _, size = inspect_header(header)
    message = decode_message(header + _read_exact(sock, size))
    if isinstance(message, Stop):
        raise StopIteration
    return message


def _send(sock, message):
    sock.sendall(encode_message(message))


def _forged_worker(sock, mode):
    """Bounded peer; the timeout also makes a missing rejection fail promptly."""
    sock.settimeout(8)
    process = psutil.Process()
    try:
        _send(sock, Ready(process.memory_info().rss, process.num_fds(), process.num_threads()))
        job = _receive(sock)
        assert isinstance(job, WorkerJob)
        payload = bytearray()
        if mode != 'early-result':
            while len(payload) < job.input_bytes:
                chunk = _receive(sock)
                assert isinstance(chunk, Chunk) and chunk.offset == len(payload)
                payload.extend(chunk.payload)
                end = len(payload)
                if mode == 'wrong-ack':
                    _send(sock, ChunkAck(job.task_id, job.grant_id, end - 1))
                    break
                _send(sock, ChunkAck(job.task_id, job.grant_id, end))
                if mode == 'replayed-ack':
                    _send(sock, ChunkAck(job.task_id, job.grant_id, end))
                    break
            else:
                assert isinstance(_receive(sock), InputDone)

        if mode not in ('wrong-ack', 'replayed-ack'):
            task = job.task_id + (mode == 'unknown-task')
            grant = job.grant_id + (mode == 'unknown-grant')
            length = job.input_bytes + (mode == 'oversized-result')
            count = job.input_bytes // 48 + 1 if mode == 'oversized-count' else 1
            digest = hashlib.sha256(payload).digest()
            _send(sock, WorkerResult(task, grant, length, count, digest))
            if mode in ('wrong-offset', 'oversized-chunk', 'wrong-digest'):
                data = bytes(payload)
                offset = int(mode == 'wrong-offset')
                if mode == 'oversized-chunk':
                    data += b'!'
                if mode == 'wrong-digest':
                    data = bytes([data[0] ^ 1]) + data[1:]
                _send(sock, Chunk(task, grant, offset, data))

        # Stay alive to let the coordinator observe the malformed message first.
        # No worker exceptions, input bytes, or socket details are printed.
        while True:
            _receive(sock)
    except (StopIteration, EOFError, OSError):
        pass
    finally:
        sock.close()


# Top-level targets stay importable under multiprocessing's spawn context.
def _unknown_task(sock, run_id, max_job_bytes, workspace_bytes):
    _forged_worker(sock, 'unknown-task')


def _unknown_grant(sock, run_id, max_job_bytes, workspace_bytes):
    _forged_worker(sock, 'unknown-grant')


def _early_result(sock, run_id, max_job_bytes, workspace_bytes):
    _forged_worker(sock, 'early-result')


def _wrong_ack(sock, run_id, max_job_bytes, workspace_bytes):
    _forged_worker(sock, 'wrong-ack')


def _replayed_ack(sock, run_id, max_job_bytes, workspace_bytes):
    _forged_worker(sock, 'replayed-ack')


def _oversized_result(sock, run_id, max_job_bytes, workspace_bytes):
    _forged_worker(sock, 'oversized-result')


def _oversized_count(sock, run_id, max_job_bytes, workspace_bytes):
    _forged_worker(sock, 'oversized-count')


def _wrong_offset(sock, run_id, max_job_bytes, workspace_bytes):
    _forged_worker(sock, 'wrong-offset')


def _oversized_chunk(sock, run_id, max_job_bytes, workspace_bytes):
    _forged_worker(sock, 'oversized-chunk')


def _wrong_digest(sock, run_id, max_job_bytes, workspace_bytes):
    _forged_worker(sock, 'wrong-digest')


def _frame():
    lines = (b'@' + _PRIVATE + b'/1', b'ACG', b'+opaque', b'!!!',
             b'@' + _PRIVATE + b'/2', b'TGC', b'+opaque', b'###')
    return struct.pack('<IIQ8I', 48 + sum(map(len, lines)), 0, 0,
                       *map(len, lines)) + b''.join(lines)


@pytest.mark.parametrize(('target', 'expected', 'message_type', 'input_done'), [
    (_unknown_task, 'worker message refers to an unknown task or grant', WorkerResult, True),
    (_unknown_grant, 'worker message refers to an unknown task or grant', WorkerResult, True),
    (_early_result, 'worker result exceeds its admitted job', WorkerResult, False),
    (_wrong_ack, 'worker input acknowledgment is out of order', ChunkAck, False),
    (_replayed_ack, 'worker input acknowledgment is out of order', ChunkAck, True),
    (_oversized_result, 'worker result exceeds its admitted job', WorkerResult, True),
    (_oversized_count, 'worker result exceeds its admitted job', WorkerResult, True),
    (_wrong_offset, 'worker result chunk is outside its admitted window', Chunk, True),
    (_oversized_chunk, 'worker result chunk is outside its admitted window', Chunk, True),
    (_wrong_digest, 'worker result digest did not authenticate', Chunk, True),
], ids=lambda value: value.__name__.lstrip('_') if callable(value) else None)
def test_P13_T03_T05_forged_worker_state_rejects_privately_and_reaps(
        monkeypatch, capfd, target, expected, message_type, input_done):
    budget = Budget(512 << 20, descriptors=128)
    cpu = resolve_cpu_budget(observations=CpuObservations(2))
    payload = _frame()
    source = budget.acquire('state-test-input', len(payload))
    monkeypatch.setattr(module, 'worker_main', target)
    # Every scheduler loop polls this deadline; the fake peer also has an I/O
    # timeout. A protocol that accidentally waits forever cannot pass the case.
    deadline = time.monotonic() + 12
    scheduler = None
    pids = ()
    witnessed = []
    original = module.StageScheduler._message

    def observe(self, index, worker, message):
        job = worker.active
        try:
            return original(self, index, worker, message)
        except IntegrityError:
            witnessed.append((type(message), None if job is None else job.input_done))
            raise

    monkeypatch.setattr(module.StageScheduler, '_message', observe)
    try:
        with pytest.raises(IntegrityError) as caught:
            with module.StageScheduler(budget, cpu, b's' * 16, max_job_bytes=1024,
                                       cancel=lambda: time.monotonic() >= deadline) as scheduler:
                pids = scheduler.child_pids
                assert len(pids) == scheduler.process_slots == 1
                scheduler.submit(payload, source)
                scheduler.take_next()
        assert str(caught.value) == expected
        assert witnessed == [(message_type, input_done)]
        assert scheduler.cancellation.error is caught.value
        assert scheduler._closed and source.released
        assert all(not psutil.pid_exists(pid) for pid in pids)
        assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
        assert budget.active_leases == 0
        diagnostic = ''.join(traceback.TracebackException.from_exception(caught.value).format())
        assert _PRIVATE.decode() not in diagnostic
        stdout, stderr = capfd.readouterr()
        assert not stdout and not stderr
    finally:
        if scheduler is not None:
            scheduler.abort()
        source.release()
