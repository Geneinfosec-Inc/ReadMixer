"""Hostile real parser peers must not advance ownership or publish completions."""
from dataclasses import replace
import hashlib
import struct

import psutil
import pytest

from readmixer_py import stage_scheduler as module
from readmixer_py.catalog import build_catalog
from readmixer_py.errors import IntegrityError
from readmixer_py.execution import CpuObservations, resolve_cpu_budget
from readmixer_py.parser_stage import (encode_source, decode_source, SourceReceiver,
                                      IngressCredits, encode_report)
from readmixer_py.resources import Budget
from readmixer_py.worker_protocol import (ParserJob, ParserBatch, ParserDone, Chunk,
    ChunkAck, ResultAck, Ready, encode_message, decode_message)
from test_parallel_parser import inputs
from test_stage_scheduler import frame
from test_worker_state import _send, _receive


def _hostile_parser(sock, run_id, max_job_bytes, workspace_bytes, **kwargs):
    sock.settimeout(8)
    process = psutil.Process()
    try:
        _send(sock, Ready(process.memory_info().rss, process.num_fds(), process.num_threads()))
        job = _receive(sock)
        assert isinstance(job, ParserJob)
        task, grant = job.task_id, job.grant_id
        mode = run_id[0]
        payload = frame(0, 0)
        header = ParserBatch(task, grant, 0, len(payload), 1, len(payload) - 40,
                             hashlib.sha256(payload).digest())
        if mode == 1:
            _send(sock, replace(header, grant_id=grant + 1))
        elif mode == 2:
            _send(sock, Chunk(task, grant, 0, payload))
        elif mode == 3:
            _send(sock, replace(header, batch=1))
        elif mode == 4:
            _send(sock, replace(header, normalized_bytes=header.normalized_bytes + 1))
        else:
            _send(sock, header)
            assert _receive(sock) == ChunkAck(task, grant, 0)
            if mode == 5:
                payload = payload[:-1] + bytes([payload[-1] ^ 1])
                _send(sock, Chunk(task, grant, 0, payload))
            elif mode == 6:
                _send(sock, Chunk(task, grant, 1, payload))
            elif mode == 7:
                _send(sock, header)
            elif mode == 8:
                _send(sock, Chunk(task, grant, 0, payload))
                assert _receive(sock) == ChunkAck(task, grant, len(payload))
                assert isinstance(_receive(sock), ResultAck)
                _send(sock, header)  # replay an already accepted batch
            else:
                raise AssertionError('unknown test scenario')
        while True:
            _receive(sock)
    except (StopIteration, EOFError, OSError):
        pass
    finally:
        sock.close()


@pytest.mark.parametrize('scenario', range(1, 9), ids=[
    'unknown-grant', 'chunk-before-batch', 'skipped-batch', 'wrong-normalized-credit',
    'changed-payload', 'wrong-offset', 'overlapping-batch', 'replayed-batch'])
def test_P14_T08_parser_state_rejects_forged_peer_and_reaps(tmp_path, monkeypatch, scenario):
    inputs(tmp_path / 'inputs', (1, 1))
    originals = {path.name: path.read_bytes() for path in (tmp_path / 'inputs').iterdir()}
    accepted = []
    budget = Budget(768 << 20, descriptors=128)
    monkeypatch.setattr(module, 'worker_main', _hostile_parser)
    with build_catalog(input_dir=tmp_path / 'inputs', budget=budget) as catalog:
        with module.StageScheduler(budget, resolve_cpu_budget(observations=CpuObservations(2)),
                bytes([scenario]) + bytes(15), parser_record_bytes=128, production=True) as stage:
            pids = stage.child_pids
            encoded = encode_source(catalog, catalog.samples[0], 128, stage.parser_batch_bytes)
            receiver = SourceReceiver(decode_source(encoded), lambda value: accepted.append(bytes(value)), IngressCredits())
            stage.submit_parser(encoded, receiver, producer=0)
            with pytest.raises(IntegrityError) as caught:
                stage.take_next()
            assert '@private' not in str(caught.value) and str(tmp_path) not in str(caught.value)
            assert len(accepted) == int(scenario == 8)
    assert pids and all(not psutil.pid_exists(pid) for pid in pids)
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
    assert {path.name: path.read_bytes() for path in (tmp_path / 'inputs').iterdir()} == originals


def test_P14_T08_parser_fixed_messages_reject_truncation_and_trailing_bytes():
    payload = frame(0, 0)
    message = ParserBatch(1, 2, 0, len(payload), 1, len(payload) - 40, hashlib.sha256(payload).digest())
    encoded = encode_message(message)
    assert decode_message(encoded) == message
    for offset in range(len(encoded)):
        with pytest.raises(IntegrityError):
            decode_message(encoded[:offset])
    with pytest.raises(IntegrityError):
        decode_message(encoded + b'private')
