"""Lightweight spawn target. Native limits are inherited before main reimport.

Only a closed, bounded identity-sort stage is supported by this increment.
External jobs carry exact segment capabilities; root keys and publication
authority remain coordinator-owned.
"""
import hashlib
import signal
import struct
import time

from .errors import CancelledError, IntegrityError, ResourceError, InputError, RankCollisionError
from .worker_protocol import (Ready, WorkerJob, WorkerResult, WorkerFailure, Chunk,
                              ChunkAck, InputDone, ResultAck, Stop, FailureCode,
                              MAX_CHUNK_BYTES, ExternalJob, SegmentResult,
                              DiskRequest, DiskCredit, DiskCommit, DiskCommitted, PolicyJob, ParserJob)
from .stage_policy import decode_policy, execute_policy, SortPolicy
from .worker_transport import FramedSocket


def sort_identity_frames(payload, *, checkpoint=lambda: None):
    from .paircodec import decode_pair_view
    view = memoryview(payload)
    rows = []
    offset = 0
    while offset < len(view):
        if len(rows) % 256 == 0:
            checkpoint()
        if len(view) - offset < 48:
            raise IntegrityError('worker pair stream is truncated')
        length = struct.unpack_from('<I', view, offset)[0]
        if length < 48 or length > len(view) - offset:
            raise IntegrityError('worker pair stream length is invalid')
        pair = decode_pair_view(view[offset:offset + length],
                                max_record_bytes=max(1, min(len(view), (0xFFFFFFFF - 40) // 2)))
        rows.append((pair.sample_index, pair.original_pair_index, offset, length))
        del pair
        offset += length
    checkpoint()
    rows.sort(key=lambda row: row[:2])
    checkpoint()
    previous = None
    output = bytearray(len(payload))
    offset = 0
    for index, (sample, ordinal, start, length) in enumerate(rows):
        if index % 256 == 0:
            checkpoint()
        identity = (sample, ordinal)
        if identity == previous:
            raise IntegrityError('worker identity-sort input contains duplicate identities')
        previous = identity
        output[offset:offset + length] = view[start:start + length]
        offset += length
    return bytes(output), len(rows)


def worker_main(sock, run_id, max_job_bytes, workspace_bytes, *, terminal_bytes=0, production=False, parser_record_bytes=0, parser_scatter=False, parser_batch_bytes=0, parser_external=False):
    # The coordinator sets environment limits around Process.start, before
    # multiprocessing imports the caller's main module. Do not set them late.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    channel = FramedSocket(sock)
    task = grant = 0
    def receive():
        while True:
            channel.pump(0.05)
            message = channel.receive()
            if isinstance(message, Stop):
                raise CancelledError()
            if message is not None:
                return message
    def send(message):
        channel.send(message)
        while channel.sending:
            channel.pump(0.05)
            incoming = channel.receive()
            if isinstance(incoming, Stop):
                raise CancelledError()
            if incoming is not None:
                if channel.sending:
                    raise IntegrityError('worker received a message before its send completed')
                channel._message = incoming
    def check_stop():
        channel.pump(0.0)
        incoming = channel.receive()
        if isinstance(incoming, Stop):
            raise CancelledError()
        if incoming is not None:
            raise IntegrityError('worker received an unexpected message during work')
    try:
        import psutil
        from . import paircodec  # required stage imports precede READY observation
        from .worker_storage import (decode_read_grant, decode_write_grant,
                                     read_granted, write_granted)
        from .storage import serialize_segment_ref
        from .worker_memory import LocalWorkspace
        if terminal_bytes:
            from . import shuffle  # NumPy baseline must be measured before READY
        process = psutil.Process()
        send(Ready(process.memory_info().rss, process.num_fds(), process.num_threads()))
        last_task = -1
        last_grant = 0
        while True:
            message = receive()
            if isinstance(message, ParserJob):
                from .parser_stage import decode_source, run_source
                cap = decode_source(message.capability)
                job = message.job
                if (not parser_record_bytes or cap.max_record_bytes != parser_record_bytes or
                        job.run_id != run_id or job.task_id <= last_task or job.grant_id <= last_grant or
                        job.workspace_bytes != workspace_bytes or job.input_bytes or
                        job.input_sha256 != hashlib.sha256(b'').digest() or
                        cap.batch_bytes != parser_batch_bytes or cap.scatter_mode != 'stream' and
                        (not parser_scatter or (cap.scatter_mode == 'external') != parser_external)):
                    raise IntegrityError('parser source is outside its issued grant')
                task, grant = job.task_id, job.grant_id
                last_task, last_grant = task, grant
                required = 3 * cap.batch_bytes + 4 * cap.max_record_bytes + (4 << 20)
                if cap.scatter_mode != 'stream':
                    from .worker_scatter import WorkerScatterGeneration
                    required += WorkerScatterGeneration.required_bytes(cap.batch_bytes, cap.max_record_bytes, cap.scatter_mode == 'external')
                if required > workspace_bytes:
                    raise ResourceError('parser workspace exceeds its admitted grant')
                with LocalWorkspace(workspace_bytes, (4 << 20) if parser_scatter else 65536, descriptors=80 if parser_external else 8) as local:
                    with local.acquire('parser-stream', required, metadata_bytes=(3 << 20) if parser_scatter else 8192, descriptors=80 if parser_external else 8):
                        run_source(message, send, receive, check_stop)
                message = cap = job = None
                continue
            policy = decode_policy(message.policy) if isinstance(message, PolicyJob) else None
            if policy is not None:
                message = message.job
            terminal = policy is not None and policy.kind is SortPolicy.TERMINAL_RANK
            payload_limit = min(64 << 20, terminal_bytes) if terminal else max_job_bytes
            external = message if isinstance(message, ExternalJob) else None
            job = external.job if external is not None else message
            if (not isinstance(job, WorkerJob) or job.run_id != run_id or
                    job.task_id <= last_task or job.grant_id <= last_grant or
                    job.input_bytes > payload_limit or job.workspace_bytes != workspace_bytes):
                raise IntegrityError('worker job does not match its issued grant')
            if terminal:
                from .resources import terminal_fits
                if not terminal_fits(terminal_bytes, policy.count, job.input_bytes):
                    raise IntegrityError('terminal job exceeds its canonical allowance')
            task, grant = job.task_id, job.grant_id
            last_task, last_grant = task, grant
            metadata_bytes = (16 if production else 8) * max_job_bytes + 16384
            local = LocalWorkspace(workspace_bytes, metadata_bytes, descriptors=4)
            with local:
                with local.acquire('input', max(1, job.input_bytes)), \
                     local.acquire('sort-and-output',
                                   (2 * payload_limit + (8 << 20) if terminal else (20 if production else 12) * max_job_bytes + 8192),
                                   metadata_bytes=(4096 if terminal else metadata_bytes - 8192)), \
                     local.acquire('protected-codecs', 12 << 20, descriptors=4):
                    if external is not None:
                        read = decode_read_grant(external.input_grant)
                        write = decode_write_grant(external.output_grant)
                        if (read.ref.run_id != run_id or write.ref.run_id != run_id
                                or (write.task_id, write.grant_id) != (task, grant)
                                or read.ref.decoded_bytes != job.input_bytes
                                or read.ref.digest != job.input_sha256):
                            raise IntegrityError('worker external capabilities disagree with the job')
                        payload = read_granted(read, checkpoint=check_stop)
                    else:
                        payload = bytearray(job.input_bytes)
                        offset = 0
                        digest = hashlib.sha256()
                        while offset < job.input_bytes:
                            chunk = receive()
                            if (not isinstance(chunk, Chunk) or (chunk.task_id, chunk.grant_id) != (task, grant)
                                    or chunk.offset != offset or len(chunk.payload) > job.input_bytes - offset):
                                raise IntegrityError('worker input chunk violates its grant')
                            payload[offset:offset + len(chunk.payload)] = chunk.payload
                            digest.update(chunk.payload)
                            offset += len(chunk.payload)
                            send(ChunkAck(task, grant, offset))
                        done = receive()
                        if (not isinstance(done, InputDone) or (done.task_id, done.grant_id) != (task, grant)
                                or digest.digest() != job.input_sha256):
                            raise IntegrityError('worker input transfer did not authenticate')
                    output, count = execute_policy(payload, policy, checkpoint=check_stop)
                    payload = None
                    if external is not None:
                        def credit(offset, amount):
                            send(DiskRequest(task, grant, offset, amount))
                            response = receive()
                            if type(response) is not DiskCredit or (response.task_id, response.grant_id,
                                    response.offset, response.amount) != (task, grant, offset, amount):
                                raise IntegrityError('worker disk credit does not match its request')
                        def committed(offset, amount):
                            send(DiskCommit(task, grant, offset, amount))
                            response = receive()
                            if type(response) is not DiskCommitted or (response.task_id, response.grant_id,
                                    response.offset, response.amount) != (task, grant, offset, amount):
                                raise IntegrityError('worker disk reconciliation was not acknowledged')
                        ref = write_granted(write, output, count, credit, committed, checkpoint=check_stop)
                        send(SegmentResult(task, grant, serialize_segment_ref(ref)))
                        ack = receive()
                        if not isinstance(ack, ResultAck) or (ack.task_id, ack.grant_id) != (task, grant):
                            raise IntegrityError('worker external ownership was not acknowledged')
                        output = read = write = ref = external = message = job = None
                        continue
                    send(WorkerResult(task, grant, len(output), count, hashlib.sha256(output).digest()))
                    for offset in range(0, len(output), MAX_CHUNK_BYTES):
                        part = output[offset:offset + MAX_CHUNK_BYTES]
                        send(Chunk(task, grant, offset, part))
                        ack = receive()
                        if (not isinstance(ack, ChunkAck) or (ack.task_id, ack.grant_id, ack.end)
                                != (task, grant, offset + len(part))):
                            raise IntegrityError('worker output acknowledgment is invalid')
                    ack = receive()
                    if not isinstance(ack, ResultAck) or (ack.task_id, ack.grant_id) != (task, grant):
                        raise IntegrityError('worker result ownership was not acknowledged')
                    output = part = chunk = done = job = message = None

    except CancelledError:
        pass
    except BaseException as error:
        code = FailureCode.INPUT if isinstance(error, InputError) else FailureCode.RANK_COLLISION if isinstance(error, RankCollisionError) else (FailureCode.INVALID_INPUT if isinstance(error, (IntegrityError, InputError)) else (
            FailureCode.RESOURCE if isinstance(error, (MemoryError, ResourceError)) else FailureCode.INTERNAL))
        try:
            # A rejected job can race a coordinator's already-admitted input
            # chunk. Keep the duplex channel alive until STOP so an unread
            # chunk cannot turn the causal failure into a connection reset.
            # Never run another job or serialize exception text during this
            # bounded drain. The normal send helper intentionally rejects such
            # concurrent messages and is not appropriate for failure shutdown.
            deadline = time.monotonic() + 10.0
            failure_queued = False
            while time.monotonic() < deadline:
                if not channel.sending and not failure_queued:
                    channel.send(WorkerFailure(task, max(1, grant), code))
                    failure_queued = True
                channel.pump(min(0.05, max(0.0, deadline - time.monotonic())))
                incoming = channel.receive()
                if isinstance(incoming, Stop):
                    break
                incoming = None
        except BaseException:
            pass
    finally:
        channel.close()
