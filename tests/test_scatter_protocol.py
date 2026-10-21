"""Real malformed scatter peers, exact phases, and post-reap ownership cleanup."""
from dataclasses import replace
import hashlib

import psutil
import pytest

from readmixer_py import stage_scheduler as module
from readmixer_py.catalog import build_catalog
from readmixer_py.crypto import OperationKeys
from readmixer_py.direct_scatter import DirectScatter
from readmixer_py.errors import IntegrityError
from readmixer_py.execution import CpuObservations, resolve_cpu_budget
from readmixer_py.fastq import SampleReadReport
from readmixer_py.parser_stage import (encode_source, decode_source, SourceReceiver,
                                      IngressCredits, encode_report)
from readmixer_py.resources import Budget
from readmixer_py.shuffle import bucket_index
from readmixer_py.storage import SegmentStore, serialize_segment_ref
from readmixer_py.worker_protocol import (ParserJob, ParserBatch, ParserDone, Chunk,
    ChunkAck, ResultAck, Ready, ScatterMessage, ScatterPhase as Phase,
    encode_message, decode_message)
from readmixer_py.worker_storage import decode_write_grant
from test_parallel_parser import inputs
from test_stage_scheduler import frame
from test_worker_state import _send, _receive


def _hostile_scatter(sock, run_id, max_job_bytes, workspace_bytes, **kwargs):
    sock.settimeout(8)
    process = psutil.Process()
    try:
        _send(sock, Ready(process.memory_info().rss, process.num_fds(), process.num_threads()))
        job = _receive(sock)
        assert isinstance(job, ParserJob)
        cap = decode_source(job.capability)
        task, grant, case = job.task_id, job.grant_id, run_id[0]
        payload = frame(cap.sample, 0)
        digest = hashlib.sha256(payload).digest()
        header = ParserBatch(task, grant, 0, len(payload), 1, len(payload) - 40, bytes(32))
        def message(phase, bucket=0, **values):
            return ScatterMessage(task, grant, 0, phase, bucket, **values)
        def empty_until(bucket):
            for previous in range(bucket):
                _send(sock, message(Phase.RESULT, previous))
                assert _receive(sock) == ResultAck(task, grant)
        if case == 1:
            _send(sock, message(Phase.DONE))
        else:
            _send(sock, header)
            assert _receive(sock).phase is Phase.BEGIN
            if case == 2:
                _send(sock, message(Phase.GRANT_REQUEST))
            elif case == 3:
                _send(sock, message(Phase.RESULT, 1))
            elif case == 4:
                _send(sock, message(Phase.DONE))
            elif case == 5:
                _send(sock, message(Phase.DISK_REQUEST, amount=1))
            elif case == 6:
                _send(sock, message(Phase.RESULT, amount=len(payload) + 1, records=1,
                                    maximum=len(payload), digest=digest))
            elif case == 8:
                report = SampleReadReport(cap.sample, 0, 0, 0, 0, 0, 0, 0,
                    cap.r1.snapshot.size, cap.r2.snapshot.size, 0,
                    hashlib.sha256(b'').hexdigest(), hashlib.sha256(b'').hexdigest(), True)
                _send(sock, ParserDone(task, grant, encode_report(report)))
            elif case == 9:
                _send(sock, message(Phase.GRANT_REQUEST, 1))
            elif case == 10:
                _send(sock, message(Phase.RESULT))
            elif case == 11:
                for bucket in (0, 1):
                    _send(sock, message(Phase.GRANT_REQUEST, bucket))
                    assert _receive(sock).phase is Phase.GRANT
                _send(sock, message(Phase.GRANT_REQUEST, 1))
            elif case == 12:
                issued = []
                for bucket in range(64):
                    _send(sock, message(Phase.GRANT_REQUEST, bucket))
                    issued.append(decode_write_grant(_receive(sock).payload))
                forged = replace(issued[0].ref, decoded_bytes=len(payload), item_count=1,
                    physical_bytes=4096, data_block_count=1, digest=digest)
                _send(sock, message(Phase.RESULT, amount=len(payload), records=1,
                    maximum=len(payload), digest=digest, payload=serialize_segment_ref(forged)))
            elif case == 15:
                _send(sock, message(Phase.RESULT))
                assert _receive(sock) == ResultAck(task, grant)
                _send(sock, message(Phase.RESULT))
            elif case == 16:
                _send(sock, replace(message(Phase.DONE), task_id=task + 1))
            elif case == 17:
                _send(sock, message(Phase.BEGIN))
            else:
                bucket = bucket_index(cap.bucket_key, cap.sample, 0, 64)
                if case == 14:
                    bucket = (bucket + 1) % 64
                empty_until(bucket)
                _send(sock, message(Phase.RESULT, bucket, amount=len(payload), records=1,
                    maximum=len(payload) - int(case == 18), digest=digest))
                assert _receive(sock) == ChunkAck(task, grant, 0)
                if case == 7:
                    _send(sock, Chunk(task, grant, 0, payload[:10]))
                    assert _receive(sock) == ChunkAck(task, grant, 10)
                    _send(sock, Chunk(task, grant, 11, payload[10:]))
                else:
                    if case == 13:
                        payload = payload[:-1] + bytes([payload[-1] ^ 1])
                    _send(sock, Chunk(task, grant, 0, payload))
        while True:
            _receive(sock)
    except (StopIteration, EOFError, OSError):
        pass
    finally:
        sock.close()


CASES = [(1, 'memory', 'unadmitted-generation'), (2, 'memory', 'disk-grant-in-memory'),
    (3, 'memory', 'skipped-bucket'), (4, 'memory', 'early-generation-done'),
    (5, 'external', 'write-before-complete-grants'), (6, 'memory', 'over-batch-result'),
    (7, 'memory', 'interrupted-raw-result'), (8, 'memory', 'late-source-done'),
    (9, 'external', 'skipped-grant'), (10, 'external', 'result-before-writer-set'),
    (11, 'external', 'replayed-grant'), (12, 'external', 'forged-seal'),
    (13, 'memory', 'wrong-digest'), (14, 'memory', 'wrong-bucket'),
    (15, 'memory', 'replayed-bucket'), (16, 'memory', 'unknown-task'),
    (17, 'memory', 'coordinator-phase-from-child'), (18, 'memory', 'false-frame-maximum')]


@pytest.mark.parametrize('case,mode,label', CASES, ids=[case[2] for case in CASES])
def test_P14_T08_scatter_phases_fail_closed_and_retained_receivers_drop_buffers(tmp_path, monkeypatch, case, mode, label):
    inputs(tmp_path / 'inputs', (1, 1))
    scratch = tmp_path / 'scratch'
    scratch.mkdir()
    originals = {path.name: path.read_bytes() for path in (tmp_path / 'inputs').iterdir()}
    budget = Budget(768 << 20, descriptors=256)
    keys = OperationKeys._from_test(bytes(32), bytes([case]) + bytes(15))
    monkeypatch.setattr(module, 'worker_main', _hostile_scatter)
    with build_catalog(input_dir=tmp_path / 'inputs', budget=budget) as catalog:
        with SegmentStore(mode, budget, keys, temp_dir=scratch) as store:
            with module.StageScheduler(budget, resolve_cpu_budget(observations=CpuObservations(2)),
                    keys.run_id, store=store, parser_record_bytes=128, parser_scatter=True,
                    production=True, parser_batch_bytes=4096) as stage:
                assert stage.process_slots == 1
                assert stage.job_descriptors == (80 if mode == 'external' else 8)
                pids = stage.child_pids
                with DirectScatter(store, max_record_bytes=128, batch_bytes=4096,
                        sort_run_bytes=4096, inventory_only=True) as inventory:
                    encoded = encode_source(catalog, catalog.samples[0], 128, stage.parser_batch_bytes,
                        scatter_mode=mode, producer=0, bucket_key=keys.bucket_key(()))
                    receiver = SourceReceiver(decode_source(encoded), lambda value: pytest.fail('raw source callback'), IngressCredits())
                    receiver.scatter_inventory = inventory
                    receiver.scatter_completed = lambda count, size: pytest.fail('completed invalid generation')
                    stage.submit_parser(encoded, receiver, producer=0)
                    assert stage.pending_bytes == stage.parser_batch_bytes
                    assert stage.peak_payload_bytes == stage.parser_batch_bytes
                    assert stage.pending_bytes <= stage.byte_ceiling
                    reasons = {
                        1: 'no admitted generation', 2: 'grant is outside',
                        3: 'unexpected bucket', 4: 'without complete statistics',
                        5: 'write is outside', 6: 'unexpected bucket',
                        7: 'raw chunk exceeds', 8: 'completion report differs',
                        9: 'grant is outside', 10: 'unexpected bucket',
                        11: 'grant is outside', 12: 'exceeds its storage grant',
                        13: 'raw payload did not authenticate', 14: 'source or bucket identity',
                        15: 'unexpected bucket', 16: 'unknown task or grant',
                        17: 'invalid on a worker channel', 18: 'exceeds its admitted maximum',
                    }
                    with pytest.raises(IntegrityError, match=reasons[case]) as caught:
                        stage.take_next()
                    assert str(tmp_path) not in str(caught.value) and '@private' not in str(caught.value)
                    assert not inventory.records
                    # The private caller deliberately retains the receiver after
                    # abort. It must not retain raw result buffers or key grants
                    # whose scheduler windows have already been released.
                    assert receiver._seen is None
                    assert receiver.scatter is None
            assert all(not psutil.pid_exists(pid) for pid in pids)
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
    assert not list(scratch.iterdir())
    assert {path.name: path.read_bytes() for path in (tmp_path / 'inputs').iterdir()} == originals


def test_P14_T08_scatter_wire_boundaries_and_private_payloads():
    message = ScatterMessage(1, 2, 3, Phase.GRANT, 4, payload=b'private-grant')
    encoded = encode_message(message)
    assert decode_message(encoded) == message and 'private-grant' not in repr(message)
    for offset in range(len(encoded)):
        with pytest.raises(IntegrityError):
            decode_message(encoded[:offset])
    with pytest.raises(IntegrityError):
        decode_message(encoded + b'x')


@pytest.mark.parametrize('scatter,mode', [(False, 'memory'), (True, 'memory'), (True, 'external')])
def test_P14_T02_small_sort_windows_still_admit_complete_parser_metadata(tmp_path, scatter, mode):
    budget = Budget(768 << 20, descriptors=256)
    keys = OperationKeys._from_test(bytes(32), bytes(16))
    with SegmentStore(mode, budget, keys, temp_dir=tmp_path) as store:
        with module.StageScheduler(budget, resolve_cpu_budget(observations=CpuObservations(2)),
                keys.run_id, store=store, max_job_bytes=1024, parser_record_bytes=128,
                parser_scatter=scatter, production=True, parser_batch_bytes=4096) as stage:
            assert stage.process_slots == 1
            assert stage.byte_ceiling >= stage.capacity * stage.parser_batch_bytes
            minimum = (4 << 20) if scatter else 65536
            assert stage.job_metadata_bytes >= minimum
            assert all(lease.metadata_bytes >= minimum for lease in stage._slots)
            if scatter:
                from readmixer_py.worker_scatter import WorkerScatterGeneration
                parser = 3 * stage.parser_batch_bytes + 4 * stage.parser_record_bytes + (4 << 20)
                required = WorkerScatterGeneration.required_bytes(stage.parser_batch_bytes,
                    stage.parser_record_bytes, mode != 'memory') + parser
                assert stage.workspace_bytes >= required + 16384
            if mode == 'external':
                assert stage.scatter_storage is not None
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize('foreign', ['task', 'run'])
def test_P14_T08_worker_scatter_rejects_foreign_grants_before_opening_writers(tmp_path, foreign, monkeypatch):
    from collections import deque
    from readmixer_py.scatter_stage import send_generation
    from readmixer_py.worker_storage import WorkerStorage, encode_grant
    from readmixer_py.worker_scatter import WorkerScatterGeneration
    from readmixer_py.crypto import pairs_identity
    inputs(tmp_path / 'inputs', (1, 1))
    scratch = tmp_path / 'scratch'
    scratch.mkdir()
    budget = Budget(512 << 20, descriptors=256)
    keys = OperationKeys._from_test(bytes(32), bytes(16))
    queue, received = deque(), []
    with build_catalog(input_dir=tmp_path / 'inputs', budget=budget) as catalog:
        encoded = encode_source(catalog, catalog.samples[0], 128, 4096,
            scatter_mode='external', producer=0, bucket_key=keys.bucket_key(()))
        cap = decode_source(encoded)
        with SegmentStore('external', budget, keys, temp_dir=scratch) as store:
            manager = WorkerStorage(store, max_pending=64, max_job_bytes=4096, scatter=True)
            try:
                grants = []
                for bucket in range(64):
                    issued = manager.issue_write(7 if foreign == 'task' else 0, bucket + 1, 1,
                        pairs_identity((bucket,), 0, 0), 4096, 4096 // 48)
                    grants.append(issued)
                    encoded_grant = encode_grant(issued)
                    queue.append(ScatterMessage(0, 1, 0, Phase.GRANT, bucket, payload=encoded_grant))
                def send(message):
                    assert message.phase is Phase.GRANT_REQUEST
                    received.append(message.bucket)
                def forbidden(*args, **kwargs):
                    pytest.fail('foreign capability reached writer construction')
                monkeypatch.setattr(WorkerScatterGeneration, '__init__', forbidden)
                with pytest.raises(IntegrityError, match='another parser task or invocation'):
                    send_generation(cap, 0, 1, 0, memoryview(frame(0, 0)),
                        ScatterMessage(0, 1, 0, Phase.BEGIN), send, queue.popleft, lambda: None,
                        run_id=b'x' * 16 if foreign == 'run' else keys.run_id)
                assert received == list(range(64)) and not queue
                assert all(manager._info(grant.ref).st_size == 0 for grant in grants)
            finally:
                manager.cleanup(workers_reaped=True)
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
    assert not list(scratch.iterdir())


def test_P14_T08_early_job_failure_survives_full_raw_chunk_and_reaps_sibling(monkeypatch):
    from readmixer_py.errors import ResourceError
    from readmixer_py.resources import TERMINAL_OVERHEAD
    from readmixer_py.stage_policy import StagePolicy, SortPolicy
    from readmixer_py.worker_transport import FramedSocket
    budget = Budget(1024 << 20, descriptors=256)
    keys = OperationKeys._from_test(bytes(32), bytes(16))
    policy = StagePolicy.terminal((1,), 1, 128, len(frame(0, 0)),
        tuple(keys.rank_key((1,), 1, attempt) for attempt in range(8)))
    original_limit = module.StageScheduler._policy_limit
    def limit(stage, value):
        if value is not None and value.kind is SortPolicy.TERMINAL_RANK:
            return stage.terminal_payload_bytes
        return original_limit(stage, value)
    monkeypatch.setattr(module.StageScheduler, '_policy_limit', limit)
    chunks = []
    original_send = FramedSocket.send
    def send(channel, message):
        if isinstance(message, Chunk):
            chunks.append(len(message.payload))
        return original_send(channel, message)
    monkeypatch.setattr(FramedSocket, 'send', send)
    payload = bytes(1 << 20)
    owner = budget.acquire('test-raw-overlap', len(payload))
    pids = ()
    try:
        with pytest.raises(IntegrityError, match='worker stage failed'):
            with module.StageScheduler(budget, resolve_cpu_budget(observations=CpuObservations(3)), keys.run_id,
                    task_count=2, production=True, terminal_bytes=TERMINAL_OVERHEAD + 512,
                    max_job_bytes=8192) as stage:
                pids = stage.child_pids
                assert len(pids) == 2
                stage.submit(payload, owner, policy=policy)
                stage.take_next()
    finally:
        owner.release()
    assert (1 << 20) in chunks
    assert all(not psutil.pid_exists(pid) for pid in pids)
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0


def test_P14_T08_parser_zero_input_digest_is_bound_before_source_reads(tmp_path, monkeypatch):
    from readmixer_py.worker_transport import FramedSocket
    inputs(tmp_path / 'inputs', (1, 1))
    budget = Budget(768 << 20, descriptors=128)
    original_send = FramedSocket.send
    submitted = []
    def send(channel, message):
        if isinstance(message, ParserJob):
            submitted.append(message.task_id)
            message = replace(message, job=replace(message.job, input_sha256=bytes(32)))
        return original_send(channel, message)
    monkeypatch.setattr(FramedSocket, 'send', send)
    with build_catalog(input_dir=tmp_path / 'inputs', budget=budget) as catalog:
        with module.StageScheduler(budget, resolve_cpu_budget(observations=CpuObservations(2)), bytes(16),
                production=True, parser_record_bytes=128) as stage:
            pids = stage.child_pids
            encoded = encode_source(catalog, catalog.samples[0], 128, stage.parser_batch_bytes)
            receiver = SourceReceiver(decode_source(encoded), lambda value: pytest.fail('input accepted'), IngressCredits())
            stage.submit_parser(encoded, receiver, producer=0)
            with pytest.raises(IntegrityError, match='worker stage failed'):
                stage.take_next()
    assert submitted == [0] and all(not psutil.pid_exists(pid) for pid in pids)
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0


@pytest.mark.parametrize('mode,expected', [('memory', 1), ('external', 0)])
def test_P14_T02_memory_workers_reserve_actual_handles_at_low_descriptor_limits(tmp_path, mode, expected):
    budget = Budget(768 << 20, descriptors=96)
    keys = OperationKeys._from_test(bytes(32), bytes(16))
    with SegmentStore(mode, budget, keys, temp_dir=tmp_path) as store:
        with module.StageScheduler(budget, resolve_cpu_budget(observations=CpuObservations(2)),
                keys.run_id, store=store, parser_record_bytes=128, parser_scatter=True,
                production=True, parser_batch_bytes=4096) as stage:
            assert stage.process_slots == expected
            assert stage.job_descriptors == (8 if expected else 4)
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
    assert not list(tmp_path.iterdir())
