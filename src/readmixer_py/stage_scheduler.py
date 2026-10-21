"""Private spawn scheduler for admitted identity-sort batches and storage handoffs.

The coordinator owns memory/segment grants, disk credits, and ordered results.
Production pipeline stages integrate this private foundation under Plan 14.
"""
from collections import deque
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
import hashlib
import multiprocessing
import os
import time
from threading import Lock, get_ident

from .errors import CancelledError, ConfigurationError, IntegrityError, ResourceError, InputError, RankCollisionError
from .execution import CancellationState
from .resources import Lease, TERMINAL_OVERHEAD
from .stage_admission import StageAdmission, ReadyAdmissionError, CHILD_BASELINE_BYTES
from .worker_bootstrap import worker_main, sort_identity_frames
from .worker_protocol import (Ready, WorkerJob, WorkerResult, WorkerFailure, Chunk,
                              ChunkAck, InputDone, ResultAck, Stop, MAX_CHUNK_BYTES,
                              ExternalJob, SegmentResult, DiskRequest, DiskCredit,
                              DiskCommit, DiskCommitted, PolicyJob, ParserJob, ParserBatch, ParserDone, FailureCode, ScatterMessage, ScatterPhase)
from .stage_policy import StagePolicy, SortPolicy, encode_policy, execute_policy, record_count_limit
from .worker_transport import FramedSocket, socket_pair

_NATIVE_LIMITS = ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS',
                  'VECLIB_MAXIMUM_THREADS', 'NUMEXPR_NUM_THREADS')
_LAUNCH_LOCK = Lock()
_GRACE_SECONDS = 10.0
_TERMINATE_SECONDS = 5.0
_REAP_SECONDS = 5.0


def _start(process):
    # Spawn reimports __main__ before the target. The inherited environment must
    # already limit native pools, and every original parent value is restored.
    with _LAUNCH_LOCK:
        previous = {name: os.environ.get(name) for name in _NATIVE_LIMITS}
        try:
            for name in _NATIVE_LIMITS:
                os.environ[name] = '1'
            process.start()
        finally:
            for name, value in previous.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value


@dataclass(slots=True)
class _Task:
    task: int
    grant: int
    slot: int
    payload: bytes = field(repr=False)
    source: Lease = field(repr=False)
    length: int
    digest: bytes = field(repr=False)
    policy: object = field(default=None, repr=False)
    parser: object = field(default=None, repr=False)
    parser_ack: bool = False
    sent: int = 0
    awaiting: int | None = None
    input_done: bool = False
    result: object = None
    output: object = field(default=None, repr=False)
    received: int = 0
    complete: bool = False
    result_ack: bool = False
    memory: object = field(default=None, repr=False)
    read_grant: object = field(default=None, repr=False)
    write_grant: object = field(default=None, repr=False)


@dataclass(slots=True)
class _Worker:
    process: object
    channel: object
    child_socket: object
    ready: bool = False
    active: object = None
    queue: object = field(default_factory=deque)
    stop_sent: bool = False


class StageResult(AbstractContextManager):
    """Caller-owned bytes and their lease survive scheduler shutdown."""
    __slots__ = ('task_id', 'records', '_data', '_lease')

    def __init__(self, task, records, data, lease):
        self.task_id, self.records, self._data, self._lease = task, records, data, lease

    @property
    def data(self):
        if self._lease.released:
            raise ResourceError('worker result is closed')
        return self._data

    def close(self):
        self._data = None
        self._lease.release()

    def __exit__(self, *_exc):
        self.close()

    def __repr__(self):
        return f'StageResult(task_id={self.task_id}, records={self.records})'


class StageSegmentResult(AbstractContextManager):
    """Caller-owned authenticated output; close it before closing its store."""
    def __init__(self, task, ref, store, lease):
        self.task_id, self.records = task, ref.item_count
        self.ref, self._store, self._lease = ref, store, lease
        self._closed = False

    def close(self):
        if not self._closed:
            self._store.drop(self.ref, missing_ok=True)
            self._lease.release()
            self._closed = True

    def take_ref(self):
        if self._closed:
            raise ResourceError('worker segment result is closed')
        self._lease.release()
        self._closed = True
        return self.ref

    def __exit__(self, *_exc):
        self.close()

    def __repr__(self):
        return f'StageSegmentResult(task_id={self.task_id}, records={self.records})'


class StageScheduler(AbstractContextManager):
    def __init__(self, budget, cpu_plan, run_id, *, operation='verify', task_count=1,
                 max_job_bytes=512 << 10, cancel=None, store=None, production=False, terminal_bytes=0, parser_record_bytes=0, parser_scatter=False, parser_batch_bytes=None):
        if type(run_id) is not bytes or len(run_id) != 16:
            raise ConfigurationError('worker operation identity must have 16 bytes')
        if type(max_job_bytes) is not int or not 1 <= max_job_bytes <= 8 << 20:
            raise ConfigurationError('worker batch limit must be in 1..8388608')
        if type(terminal_bytes) is not int or not 0 <= terminal_bytes <= 128 << 20:
            raise ConfigurationError('terminal allowance is outside its canonical bound')
        from .paircodec import maximum_frame_bytes
        if type(parser_record_bytes) is not int or parser_record_bytes < 0:
            raise ConfigurationError('parser record bound is invalid')
        if type(parser_scatter) is not bool or parser_scatter and (not parser_record_bytes or store is None):
            raise ConfigurationError('worker scatter needs an admitted parser')
        self.parser_scatter = parser_scatter
        self.parser_record_bytes = parser_record_bytes
        self.parser_batch_bytes = max(1 << 20, maximum_frame_bytes(parser_record_bytes, single_frame_limit=64 << 20)) if parser_record_bytes else 0
        if parser_batch_bytes is not None:
            if (not parser_record_bytes or type(parser_batch_bytes) is not int or
                    not maximum_frame_bytes(parser_record_bytes) <= parser_batch_bytes <= 64 << 20):
                raise ConfigurationError('parser batch is outside its pair boundary limits')
            self.parser_batch_bytes = parser_batch_bytes
        self.parser_external = parser_scatter and store.mode != 'memory'
        self.job_descriptors = 80 if self.parser_external else 8 if parser_record_bytes else 4
        self.production, self.terminal_bytes = production, terminal_bytes
        self.terminal_payload_bytes = min(64 << 20, terminal_bytes)
        previous = getattr(budget, '_stage_scheduler', None)
        if previous is not None and not previous._closed:
            raise ConfigurationError('an invocation may own only one stage scheduler')
        compression = getattr(budget, '_compression_service', None)
        if compression is not None and not compression._closed:
            raise ConfigurationError('stage partition must be fixed before compression starts')
        if store is not None and (store.budget is not budget or store.keys.run_id != run_id):
            raise ConfigurationError('worker store must belong to this invocation')
        self.store, self.storage, self.scatter_storage = store, None, None
        self._scatter_generations = [0] * 64
        self.budget, self.run_id, self.max_job_bytes = budget, run_id, max_job_bytes
        self.cancellation = CancellationState(cancel)
        self._owner = get_ident()
        self._workers, self._jobs = [], {}
        self._next_task = self._drain_task = 0
        self._closed = False
        self._closing = False
        self._serial_lease = self._metadata = self._helper_lease = self._manager_preflight = self.admission = None
        self._slots = ()
        self._pending_sockets = None
        self._pending_return = None
        self._ordinary_workspace = (28 if production else 20) * max_job_bytes + (16 << 20) + 16384
        self.workspace_bytes = max(self._ordinary_workspace,
            3 * self.terminal_payload_bytes + (20 << 20) if terminal_bytes else 0,
            3 * self.parser_batch_bytes + 4 * parser_record_bytes + (4 << 20) + 16384 if parser_record_bytes else 0)
        if parser_scatter:
            from .worker_scatter import WorkerScatterGeneration
            self.workspace_bytes = max(self.workspace_bytes,
                WorkerScatterGeneration.required_bytes(self.parser_batch_bytes, parser_record_bytes,
                    store is not None and store.mode != 'memory') + 3 * self.parser_batch_bytes + 4 * parser_record_bytes + (4 << 20) + 16384)
        self.job_metadata_bytes = max((16 if production else 8) * max_job_bytes + 16384,
            (4 << 20) if parser_scatter else 65536 if parser_record_bytes else 0)
        self._manager_per_job = ((1 << 20) if parser_scatter else 0) + 32768 if store is not None and store.mode != 'memory' else 0
        self.peak_pending = self.peak_payload_bytes = 0
        self._last_pressure = time.monotonic()
        try:
            import psutil
            parent_fds = psutil.Process().num_fds()
            self._metadata = budget.acquire('stage-scheduler', 32768, metadata_bytes=32768)
            if cpu_plan.effective_limit > 1 and task_count:
                self._helper_lease = budget.acquire('stage-spawn-helper', (64 << 20) + 4096,
                                                    descriptors=8, metadata_bytes=4096)
            if self._manager_per_job:
                self._manager_preflight = budget.acquire('stage-manager-preflight', 2 << 20, metadata_bytes=2 << 20)
            budget._stage_scheduler = self
            self._start_partition(cpu_plan, operation, task_count, parent_fds)
            if self._manager_preflight is not None:
                for slot in self._slots:
                    slot.resize(self.workspace_bytes, descriptors=self.job_descriptors, metadata_bytes=self.job_metadata_bytes)
                self._manager_preflight.release()
                self._manager_preflight = None
            if store is not None and store.mode != 'memory':
                from .worker_storage import WorkerStorage
                self.storage = WorkerStorage(store, max_pending=self.capacity,
                    max_job_bytes=max(max_job_bytes, self.terminal_payload_bytes))
                if parser_scatter and self.process_slots:
                    self.scatter_storage = WorkerStorage(store, max_pending=64 * self.process_slots,
                        max_job_bytes=self.parser_batch_bytes, scatter=True)
        except BaseException as error:
            primary = self._fail(error)
            raise primary from None

    def _start_partition(self, cpu_plan, operation, task_count, parent_fds):
        """Fix the complete partition before admitting jobs or compression.

        A valid READY shortage alone permits retry. Every failed attempt must be
        fully reaped before its leases are released or a smaller one is started.
        Keep only scalar high-water observations, never a growing attempt log.
        """
        ceiling = None
        self.startup_retries = 0
        self.startup_baseline_bytes = CHILD_BASELINE_BYTES
        self.startup_worker_descriptors = 8
        while True:
            self.cancellation.check()
            self.admission = StageAdmission(self.budget, cpu_plan, operation=operation,
                task_count=task_count, per_job_bytes=self.workspace_bytes + self._manager_per_job,
                per_job_metadata_bytes=self.job_metadata_bytes + self._manager_per_job, per_job_descriptors=self.job_descriptors,
                coordinator_descriptors=parent_fds, max_process_slots=ceiling,
                baseline_bytes=self.startup_baseline_bytes,
                worker_descriptors=self.startup_worker_descriptors)
            self.process_slots = self.admission.process_slots
            if not self.process_slots:
                if self._helper_lease is not None:
                    self._helper_lease.release()
                    self._helper_lease = None
                # Low-resource production keeps the canonical serial terminal
                # path; do not reserve two native terminal arenas for no child.
                if self.terminal_bytes or self.parser_scatter:
                    self.workspace_bytes = self._ordinary_workspace
                    self.job_descriptors = 4
                self._serial_lease = self.budget.acquire('stage-serial-job', self.workspace_bytes, descriptors=self.job_descriptors,
                                                        metadata_bytes=self.job_metadata_bytes)
                self._slots = (self._serial_lease,)
            else:
                self._slots = self.admission.job_leases
            self.capacity = len(self._slots)
            self._metadata.resize(32768 + 8192 * self.capacity,
                                  metadata_bytes=32768 + 8192 * self.capacity)
            self.byte_ceiling = self.capacity * max(self.max_job_bytes,
                self.terminal_payload_bytes if self.process_slots else 0,
                self.parser_batch_bytes if self.process_slots else 0)
            try:
                self._launch_partition()
            except ReadyAdmissionError as error:
                # A smaller attempt must keep every valid observation, including
                # larger successful READY baselines from earlier children.
                ceiling = self.process_slots - 1
                self.startup_baseline_bytes = max(self.startup_baseline_bytes, error.baseline_bytes)
                self.startup_worker_descriptors = max(self.startup_worker_descriptors, error.descriptors)
                self._discard_startup_attempt()
                self.startup_retries += 1
                continue
            self.admission.seal_ready()
            self.admission.observe_pressure()
            return

    def _launch_partition(self):
        context = multiprocessing.get_context('spawn') if self.process_slots else None
        for index in range(self.process_slots):
            self._pending_sockets = socket_pair()
            left, right = self._pending_sockets
            worker = _Worker(None, FramedSocket(left), right)
            self._workers.append(worker)
            self._pending_sockets = None
            worker.process = context.Process(target=worker_main,
                args=(right, self.run_id, self.max_job_bytes, self.workspace_bytes),
                kwargs=({'terminal_bytes': self.terminal_bytes, 'production': self.production,
                         'parser_record_bytes': self.parser_record_bytes, 'parser_scatter': self.parser_scatter,
                         'parser_batch_bytes': self.parser_batch_bytes, 'parser_external': self.parser_external}
                        if self.terminal_bytes or self.production or self.parser_record_bytes else {}),
                name=f'readmixer-stage-{index}')
            _start(worker.process)
            if index == 0:
                from multiprocessing import resource_tracker
                helper_pid = resource_tracker._resource_tracker._pid
                if helper_pid is not None:
                    self.admission.register_helper(helper_pid)
            right.close()
            worker.child_socket = None
            self.admission.register_process(index, worker.process.pid)
            while not worker.ready:
                self._service()

    def _discard_startup_attempt(self):
        """Reap first; a failed cleanup is fatal and never starts another attempt."""
        self.cancellation.check()
        cleanup_error = self._stop_workers()
        if cleanup_error is not None:
            raise cleanup_error
        self.cancellation.check()
        self._workers.clear()
        self.admission.unregister_helper()
        self.admission.close()
        self.admission = None
        self._slots = ()
        self._metadata.resize(32768, metadata_bytes=32768)

    @property
    def workers_effective(self):
        return 1 + self.process_slots

    @property
    def pending_jobs(self):
        return len(self._jobs)

    @property
    def pending_bytes(self):
        return sum(self.parser_batch_bytes if job.parser is not None else job.length
                   for job in self._jobs.values())

    @property
    def child_pids(self):
        return tuple(worker.process.pid for worker in self._workers if worker.process is not None)

    def _check(self):
        if self._owner != get_ident():
            raise ResourceError('stage scheduler must remain on its coordinator thread')
        if self._closed or self._closing:
            raise ResourceError('stage scheduler is closed or stopping')
        self.cancellation.check()

    def _fail(self, error):
        primary = self.cancellation.fail(error, stage='stage-worker')
        try:
            self.abort(primary)
        except BaseException as cleanup:
            self.cancellation.fail(cleanup, stage='stage-cleanup')
        return primary

    def _advance(self, worker):
        channel = worker.channel
        if channel.sending:
            return
        if worker.active is not None and worker.active.result_ack:
            worker.active = None
        if worker.active is None:
            if not worker.queue:
                return
            worker.active = worker.queue.popleft()
            job = worker.active
            message = WorkerJob(self.run_id, job.task, job.grant, job.length,
                                self.workspace_bytes, job.digest)
            if job.write_grant is not None:
                from .worker_storage import encode_grant
                message = ExternalJob(message, encode_grant(job.read_grant), encode_grant(job.write_grant))
                job.input_done = True
            if job.policy is not None:
                message = PolicyJob(message, encode_policy(job.policy))
            if job.parser is not None:
                message = ParserJob(message, job.parser[0])
                job.input_done = True
            channel.send(message)
            return
        job = worker.active
        if job.parser is not None:
            if job.parser_ack:
                channel.send(ResultAck(job.task, job.grant))
                job.parser_ack = False
                if job.complete:
                    job.result_ack = True
            return
        if job.payload is not None and job.source.released:
            raise ResourceError('sender released its lease before input acknowledgment')
        if job.result is not None:
            if job.complete:
                channel.send(ResultAck(job.task, job.grant))
                job.result_ack = True
            return
        if job.awaiting is not None or job.input_done:
            return
        if job.sent < job.length:
            end = min(job.length, job.sent + MAX_CHUNK_BYTES)
            channel.send(Chunk(job.task, job.grant, job.sent, job.payload[job.sent:end]))
            job.awaiting = end
        else:
            channel.send(InputDone(job.task, job.grant))
            job.input_done = True

    def _message(self, index, worker, message):
        if not worker.ready:
            if not isinstance(message, Ready):
                raise IntegrityError('spawned worker did not provide READY')
            self.admission.observe_ready(index, message.rss_bytes, message.descriptors, message.threads)
            worker.ready = True
            return
        job = worker.active
        if isinstance(message, WorkerFailure):
            if job is not None and (message.task_id, message.grant_id) != (job.task, job.grant):
                raise IntegrityError('worker failure refers to an unknown grant')
            if message.code is FailureCode.RANK_COLLISION:
                raise RankCollisionError('all eight terminal rank batches contain a collision')
            if message.code is FailureCode.RESOURCE:
                raise ResourceError('worker exceeded its admitted resources')
            if message.code is FailureCode.CANCELLED:
                raise CancelledError()
            if message.code is FailureCode.INPUT:
                raise InputError(f'input worker failed with code {int(message.code)}')
            raise IntegrityError(f'worker stage failed with code {int(message.code)}')
        if job is None or (getattr(message, 'task_id', None), getattr(message, 'grant_id', None)) != (job.task, job.grant):
            raise IntegrityError('worker message refers to an unknown task or grant')
        if job.parser is not None:
            self._parser_message(worker, job, message)
            return
        if type(message) is DiskRequest:
            if job.write_grant is None or job.result is not None or worker.channel.sending:
                raise IntegrityError('worker disk request is outside its task state')
            self.admission.observe_pressure()
            self.storage.authorize_write(job.write_grant, message.offset, message.amount)
            worker.channel.send(DiskCredit(job.task, job.grant, message.offset, message.amount))
        elif type(message) is DiskCommit:
            if job.write_grant is None or job.result is not None or worker.channel.sending:
                raise IntegrityError('worker disk commit is outside its task state')
            self.storage.commit_write(job.write_grant, message.offset, message.amount)
            worker.channel.send(DiskCommitted(job.task, job.grant, message.offset, message.amount))
        elif isinstance(message, SegmentResult):
            if job.write_grant is None or job.result is not None or not job.input_done:
                raise IntegrityError('worker segment result is outside its task state')
            from .storage import deserialize_segment_ref
            ref = deserialize_segment_ref(message.reference)
            if (ref.decoded_bytes != job.length or ref.item_count > record_count_limit(job.policy, job.length)
                    or job.policy is not None and job.policy.kind is SortPolicy.TERMINAL_RANK
                    and ref.item_count != job.policy.count):
                raise IntegrityError('worker segment result exceeds its job bounds')
            ref = self.storage.accept_write(job.write_grant, ref)
            job.output = ref
            job.result = WorkerResult(job.task, job.grant, ref.decoded_bytes, ref.item_count, ref.digest)
            self.storage.release_read(job.read_grant)
            job.read_grant = None
            job.complete = True
        elif isinstance(message, ChunkAck):
            if job.input_done or job.awaiting is None or message.end != job.awaiting:
                raise IntegrityError('worker input acknowledgment is out of order')
            job.sent, job.awaiting = message.end, None
        elif isinstance(message, WorkerResult):
            if (job.write_grant is not None or not job.input_done or job.result is not None or message.output_bytes != job.length
                    or message.records > record_count_limit(job.policy, job.length)
                    or job.policy is not None and job.policy.kind is SortPolicy.TERMINAL_RANK
                    and message.records != job.policy.count):
                raise IntegrityError('worker result exceeds its admitted job')
            job.result = message
            job.output = bytearray(message.output_bytes)
            # A RESULT is the receiver's authenticated final input acceptance.
            job.payload = None
            if job.memory is not None:
                job.memory.accept(job.memory.receipt())
            else:
                job.source.release()
            if not message.output_bytes:
                if message.digest != hashlib.sha256(b'').digest() or message.records:
                    raise IntegrityError('empty worker result is inconsistent')
                job.output = b''
                job.complete = True
        elif isinstance(message, Chunk):
            if (job.result is None or job.complete or message.offset != job.received
                    or len(message.payload) > job.length - job.received or worker.channel.sending):
                raise IntegrityError('worker result chunk is outside its admitted window')
            job.output[job.received:job.received + len(message.payload)] = message.payload
            job.received += len(message.payload)
            if job.received == job.length:
                if hashlib.sha256(job.output).digest() != job.result.digest:
                    raise IntegrityError('worker result digest did not authenticate')
                job.output = bytes(job.output)
                job.complete = True
            worker.channel.send(ChunkAck(job.task, job.grant, job.received))
        else:
            raise IntegrityError('worker message is invalid for the current task state')

    def _parser_message(self, worker, job, message):
        receiver = job.parser[1]
        if worker.channel.sending or job.complete or job.parser_ack:
            raise IntegrityError('parser message arrived outside its admitted turn')
        if isinstance(message, ParserBatch):
            if job.result is not None:
                raise IntegrityError('parser has overlapping batches')
            receiver.begin(message)
            if receiver.capability.scatter_mode != 'stream':
                from .scatter_stage import ScatterReceiver
                producer = receiver.capability.producer
                generation = self._scatter_generations[producer]
                if generation >= (1 << 64) - 1:
                    raise ResourceError('scatter generation namespace is exhausted')
                self._scatter_generations[producer] += 1
                receiver.scatter = ScatterReceiver(receiver, self.store, self.scatter_storage,
                    receiver.scatter_inventory, receiver.scatter_completed, generation)
                worker.channel.send(ScatterMessage(job.task, job.grant, message.batch,
                    ScatterPhase.BEGIN, offset=generation))
                return
            job.result = message
            job.received = 0
            job.output = bytearray(message.output_bytes)
            worker.channel.send(ChunkAck(job.task, job.grant, 0))
        elif isinstance(message, ScatterMessage):
            if receiver.scatter is None:
                raise IntegrityError('scatter message has no admitted generation')
            response = receiver.scatter.message(message, job.task, job.grant)
            worker.channel.send(response)
            if message.phase is ScatterPhase.DONE:
                receiver.scatter = None
        elif isinstance(message, Chunk):
            if receiver.scatter is not None:
                complete = receiver.scatter.chunk(message)
                worker.channel.send(ChunkAck(job.task, job.grant, receiver.scatter.received))
                if complete:
                    job.parser_ack = True
                return
            if (job.result is None or message.offset != job.received or
                    len(message.payload) > job.result.output_bytes - job.received):
                raise IntegrityError('parser chunk is outside its granted batch')
            job.output[job.received:job.received + len(message.payload)] = message.payload
            job.received += len(message.payload)
            if job.received == job.result.output_bytes:
                receiver.accept(job.output)
                job.output = job.result = None
                job.parser_ack = True
            worker.channel.send(ChunkAck(job.task, job.grant, job.received))
        elif isinstance(message, ParserDone):
            if job.result is not None:
                raise IntegrityError('parser completed with an unfinished batch')
            report = receiver.finish(message.report)
            receiver.discard()
            job.result = WorkerResult(job.task, job.grant, len(message.report), report.pair_count,
                                      hashlib.sha256(message.report).digest())
            job.output = message.report
            job.complete = job.parser_ack = True
        else:
            raise IntegrityError('parser returned an unsupported message')

    def submit_parser(self, capability, receiver, *, producer):
        """Bind one source to its deterministic producer; no arbitrary callback crosses IPC."""
        self._check()
        from .parser_stage import decode_source, SourceReceiver
        cap = decode_source(capability)
        if (not self.process_slots or not self.parser_record_bytes or
                cap.max_record_bytes != self.parser_record_bytes or cap.batch_bytes != self.parser_batch_bytes or
                type(producer) is not int or not 0 <= producer < self.process_slots or
                not isinstance(receiver, SourceReceiver) or receiver.capability != cap or
                (cap.scatter_mode != 'stream' and (not self.parser_scatter or cap.producer != producer or
                    cap.scatter_mode != ('memory' if self.store.mode == 'memory' else 'external')))):
            raise ConfigurationError('parser source does not belong to its admitted producer')
        occupied = {job.slot for job in self._jobs.values()}
        slots = [slot for slot in (2 * producer, 2 * producer + 1) if slot not in occupied]
        if not slots:
            raise ResourceError('parser producer has two submitted sources; drain one')
        slot = slots[0]
        try:
            self._slots[slot].resize(self.workspace_bytes, descriptors=self.job_descriptors,
                                     metadata_bytes=self.job_metadata_bytes)
            if self._next_task >= (1 << 64) - 1:
                raise ResourceError('stage task identity namespace is exhausted')
            task = self._next_task
            job = _Task(task, task + 1, slot, None, None, 0, hashlib.sha256(b'').digest())
            job.parser = (capability, receiver)
            self._jobs[task] = job
            self._next_task += 1
            self._workers[producer].queue.append(job)
            self._service()
            self.peak_pending = max(self.peak_pending, len(self._jobs))
            self.peak_payload_bytes = max(self.peak_payload_bytes, self.pending_bytes)
            return task
        except BaseException as error:
            primary = self._fail(error)
            raise primary from None

    def _service(self):
        self._check()
        now = time.monotonic()
        if self.admission._sealed and now - self._last_pressure >= 0.1:
            self.admission.observe_pressure()
            self._last_pressure = now
        progressed = False
        for index, worker in enumerate(self._workers):
            worker.channel.pump(0.0)
            message = worker.channel.receive()
            if message is not None:
                self._message(index, worker, message)
                progressed = True
            if worker.process is not None and worker.process.pid is not None and not worker.process.is_alive():
                raise ResourceError('registered stage worker exited unexpectedly')
            if worker.ready:
                self._advance(worker)
        if not progressed:
            time.sleep(0.002)

    def _policy_limit(self, policy):
        if policy is not None:
            encode_policy(policy)
            if policy.kind is SortPolicy.TERMINAL_RANK:
                if not self.process_slots or not self.terminal_bytes:
                    raise ResourceError('native terminal workers were not admitted')
                return min(self.terminal_payload_bytes, self.terminal_bytes - TERMINAL_OVERHEAD - 128 * policy.count)
            if not self.production:
                raise ConfigurationError('production policies require a production workspace')
        return self.max_job_bytes

    def submit_memory(self, ref, *, policy=None):
        from .worker_memory import MemoryRef
        if not isinstance(ref, MemoryRef) or ref.run_id != self.run_id or ref.bound:
            raise ConfigurationError('memory reference does not belong to this invocation')
        return self.submit(ref.payload, ref.lease, _memory=ref, policy=policy)

    def submit(self, payload, lease, *, _memory=None, policy=None):
        self._check()
        if (type(payload) is not bytes or len(payload) > self._policy_limit(policy) or not isinstance(lease, Lease)
                or lease._budget is not self.budget or lease.released or lease.bytes < len(payload)):
            raise ConfigurationError('stage input requires immutable bytes and its admitted owner')
        if any(job.source is lease for job in self._jobs.values()):
            raise ConfigurationError('sender lease is already borrowed by another job')
        occupied = {job.slot for job in self._jobs.values()}
        if len(occupied) >= self.capacity:
            raise ResourceError('stage submitted/completed window is full; drain the earliest result')
        slot = next(index for index in range(self.capacity) if index not in occupied)
        try:
            self.admission.observe_pressure()
            self._slots[slot].resize(self.workspace_bytes, descriptors=self.job_descriptors, metadata_bytes=self.job_metadata_bytes)
            if self._next_task >= (1 << 64) - 1:
                raise ResourceError('stage task identity namespace is exhausted')
            task = self._next_task
            job = _Task(task, task + 1, slot, payload, lease, len(payload), hashlib.sha256(payload).digest())
            job.policy = policy
            job.memory = _memory
            self._jobs[task] = job  # retain the owner before any fallible dispatch
            if _memory is not None:
                _memory.bind(job.task, job.grant)
            self._next_task += 1
            if self.process_slots:
                self._workers[slot // 2].queue.append(job)
                self._service()
            else:
                output, count = execute_policy(payload, policy, checkpoint=self.cancellation.check)
                job.result = WorkerResult(task, job.grant, len(output), count, hashlib.sha256(output).digest())
                job.output, job.complete, job.result_ack = output, True, True
                job.payload = None
                if job.memory is not None:
                    job.memory.accept(job.memory.receipt())
                else:
                    lease.release()
            self.peak_pending = max(self.peak_pending, len(self._jobs))
            self.peak_payload_bytes = max(self.peak_payload_bytes, self.pending_bytes)
            return task
        except BaseException as error:
            primary = self._fail(error)
            raise primary from None

    def submit_segment(self, ref, *, policy=None):
        """Borrow one protected input. Auto seals/spills before external grants.

        Memory mode uses admitted raw transfer and never creates disk staging.
        External results remain owned by the task ledger until ordered delivery.
        """
        self._check()
        if self.store is None or ref.run_id != self.run_id or ref.decoded_bytes > self._policy_limit(policy):
            raise ConfigurationError('worker input segment is outside its invocation or batch bound')
        if len(self._jobs) >= self.capacity:
            raise ResourceError('stage submitted/completed window is full; drain the earliest result')
        if self.storage is None:
            from .worker_memory import MemoryRef
            owner = self.budget.acquire('worker-segment-transfer', 2 * ref.decoded_bytes + 4096, metadata_bytes=4096)
            memory = None
            try:
                with self.store.open_verified(ref) as reader:
                    data = bytearray()
                    while len(data) < ref.decoded_bytes:
                        data.extend(reader.read(min(1 << 20, ref.decoded_bytes - len(data))))
                payload = bytes(data)
                del data
                owner.resize(ref.decoded_bytes + 4096, metadata_bytes=4096)
                memory = MemoryRef(payload, owner, self.run_id, self._next_task, budget=self.budget)
                del payload
                return self.submit_memory(memory, policy=policy)
            except BaseException:
                if memory is not None and not memory.bound:
                    memory.cancel_after_reap()
                owner.release()
                raise
        occupied = {job.slot for job in self._jobs.values()}
        slot = next(index for index in range(self.capacity) if index not in occupied)
        try:
            self.admission.observe_pressure()
            self._slots[slot].resize(self.workspace_bytes, descriptors=self.job_descriptors, metadata_bytes=self.job_metadata_bytes)
            if self._next_task >= (1 << 64) - 1:
                raise ResourceError('stage task identity namespace is exhausted')
            task = self._next_task
            job = _Task(task, task + 1, slot, None, None, ref.decoded_bytes, ref.digest)
            job.policy = policy
            self._jobs[task] = job
            self._next_task += 1
            job.read_grant = self.storage.issue_read(ref)
            from .crypto import sort_identity
            job.write_grant = self.storage.issue_write(task, job.grant, 3,
                sort_identity('worker-identity', 0, task), ref.decoded_bytes, record_count_limit(policy, ref.decoded_bytes))
            if self.process_slots:
                self._workers[slot // 2].queue.append(job)
                self._service()
            else:
                from .worker_storage import read_granted, write_granted
                payload = read_granted(job.read_grant, checkpoint=self.cancellation.check)
                output, count = execute_policy(payload, policy, checkpoint=self.cancellation.check)
                sealed = write_granted(job.write_grant, output, count,
                    lambda offset, amount: self.storage.authorize_write(job.write_grant, offset, amount),
                    lambda offset, amount: self.storage.commit_write(job.write_grant, offset, amount),
                    checkpoint=self.cancellation.check)
                job.output = self.storage.accept_write(job.write_grant, sealed)
                job.result = WorkerResult(task, job.grant, sealed.decoded_bytes, count, sealed.digest)
                self.storage.release_read(job.read_grant)
                job.read_grant = None
                job.complete = job.result_ack = True
            self.peak_pending = max(self.peak_pending, len(self._jobs))
            self.peak_payload_bytes = max(self.peak_payload_bytes, self.pending_bytes)
            return task
        except BaseException as error:
            primary = self._fail(error)
            raise primary from None

    def take_next(self):
        self._check()
        if self._drain_task not in self._jobs:
            raise ResourceError('no stage result is pending')
        job = self._jobs[self._drain_task]
        owner = result = None
        try:
            while not job.complete or not job.result_ack:
                self._service()
            self.admission.observe_pressure()
            owner = self._slots[job.slot].split('stage-returned-result',
                (4096 if job.write_grant is not None else len(job.output) + 4096), metadata_bytes=4096)
            if job.write_grant is not None:
                result = StageSegmentResult(job.task, job.output, self.store, owner)
                self.storage.take_write(job.write_grant)
            else:
                result = StageResult(job.task, job.result.records, job.output, owner)
            # The caller now owns result RAM independently of scheduler lifetime.
            job.output = None
            del self._jobs[job.task]
            self._drain_task += 1
            return result
        except BaseException as error:
            # The receiver exists before ownership transfer. Retain it before
            # any fallible cleanup so a second error cannot orphan its lease or
            # replace the first cause. abort reaps writers before retrying close.
            self._pending_return = result if result is not None else owner
            primary = self._fail(error)
            raise primary from None

    def _stop_workers(self):
        first = None
        def attempt(action):
            nonlocal first
            for retry in range(2):
                try:
                    return action()
                except BaseException as error:
                    if first is None:
                        first = error
                    if retry or not isinstance(error, (KeyboardInterrupt, CancelledError, SystemExit)):
                        return None

        def alive():
            result = []
            for worker in self._workers:
                if worker.process is not None and worker.process.pid is not None:
                    observed = attempt(worker.process.is_alive)
                    if observed is not False:  # unknown liveness never authorizes release
                        result.append(worker)
            return result

        deadline = time.monotonic() + _GRACE_SECONDS
        while alive() and time.monotonic() < deadline:
            for worker in alive():
                def service():
                    try:
                        worker.channel.pump(0.0)
                        worker.channel.receive()
                        if not worker.stop_sent and not worker.channel.sending:
                            worker.channel.send(Stop())
                            worker.stop_sent = True
                    except (OSError, ResourceError, IntegrityError):
                        worker.channel.close()
                attempt(service)
            attempt(lambda: time.sleep(0.005))
        for worker in alive():
            attempt(worker.process.terminate)
        deadline = time.monotonic() + _TERMINATE_SECONDS
        while alive() and time.monotonic() < deadline:
            attempt(lambda: time.sleep(0.005))
        for worker in alive():
            attempt(worker.process.kill)
        deadline = time.monotonic() + _REAP_SECONDS
        while alive() and time.monotonic() < deadline:
            attempt(lambda: time.sleep(0.005))
        if alive():
            error = first or ResourceError('stage workers could not be reaped; ownership retained')
            error.add_note('unreaped worker ownership and reservations remain retained')
            raise error
        for index, worker in enumerate(self._workers):
            if worker.process is not None:
                if worker.process.pid is not None:
                    attempt(lambda: worker.process.join(timeout=0))
                attempt(worker.process.close)
                if worker.process._closed:
                    worker.process = None
            attempt(worker.channel.close)
            if worker.child_socket is not None:
                attempt(worker.child_socket.close)
                if worker.child_socket.fileno() < 0:
                    worker.child_socket = None
            if self.admission is not None:
                attempt(lambda: self.admission.unregister_process(index))
        if self._pending_sockets is not None:
            for sock in self._pending_sockets:
                attempt(sock.close)
            if all(sock.fileno() < 0 for sock in self._pending_sockets):
                self._pending_sockets = None
        if (self._pending_sockets is not None or any(worker.process is not None or
                not worker.channel.closed or worker.child_socket is not None for worker in self._workers)):
            error = first or ResourceError('stage descriptor cleanup incomplete; ownership retained')
            error.add_note('stage descriptor reservations remain retained for cleanup retry')
            raise error
        return first

    def abort(self, primary=None):
        if self._closed:
            return
        self._closing = True
        if primary is not None:
            self.cancellation.fail(primary, stage='stage-shutdown')
        cleanup_error = self._stop_workers()  # never release live workers' grants or file ownership
        if self._pending_return is not None:
            pending = self._pending_return
            if isinstance(pending, Lease):
                pending.release()
            else:
                pending.close()
            self._pending_return = None
        for job in self._jobs.values():
            job.payload = job.output = None
            if job.parser is not None:
                job.parser[1].discard()
                job.parser = None
            if job.memory is not None:
                job.memory.cancel_after_reap()
            elif job.source is not None:
                job.source.release()
        if self.scatter_storage is not None:
            self.scatter_storage.cleanup(workers_reaped=True)
        if self.storage is not None:
            self.storage.cleanup(workers_reaped=True)
        self._jobs.clear()
        self._workers.clear()
        if self._serial_lease is not None:
            self._serial_lease.release()
        if self.admission is not None:
            self.admission.unregister_helper()
            self.admission.close()
        if self._helper_lease is not None:
            self._helper_lease.release()
        if self._manager_preflight is not None:
            self._manager_preflight.release()
            self._manager_preflight = None
        if self._metadata is not None:
            self._metadata.release()
        if getattr(self.budget, '_stage_scheduler', None) is self:
            self.budget._stage_scheduler = None
        self._closed = True
        if cleanup_error is not None:
            raise cleanup_error

    def close(self):
        if self._closed:
            return
        if self._jobs:
            error = ResourceError('stage scheduler has undrained results')
            self._fail(error)
            raise error
        self.abort()

    def __exit__(self, exc_type, exc, traceback):
        if exc is None:
            self.close()
        else:
            self._fail(exc)
