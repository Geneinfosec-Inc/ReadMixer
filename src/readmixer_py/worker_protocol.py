"""Closed binary messages for the initial private SORT_IDENTITY worker stage.

The transport reads exactly HEADER_BYTES and calls inspect_header before
allocating a body. Control-frame and raw-chunk bounds are independent. This
codec establishes structural validity only: task/grant freshness, byte grants,
stream offsets, final digests and legal state transitions belong to the owner
of the channel. The format is ephemeral and has no persistence/resume contract.
"""

from dataclasses import dataclass, field
from enum import IntEnum
import struct

from .errors import ConfigurationError, IntegrityError


MAGIC = b'RMPYWRK1'
VERSION = 1
MAX_CONTROL_BYTES = 64 << 10
MAX_CHUNK_BYTES = 1 << 20
_HEADER = struct.Struct('>8sBBI')
HEADER_BYTES = _HEADER.size
_U64_MAX = (1 << 64) - 1


class MessageKind(IntEnum):
    READY = 1
    JOB = 2
    CHUNK = 3
    CHUNK_ACK = 4
    INPUT_DONE = 5
    RESULT = 6
    RESULT_ACK = 7
    STOP = 8
    FAILURE = 9
    EXTERNAL_JOB = 10
    SEGMENT_RESULT = 11
    DISK_REQUEST = 12
    DISK_CREDIT = 13
    DISK_COMMIT = 14
    DISK_COMMITTED = 15
    POLICY_JOB = 16
    PARSER_JOB = 17
    PARSER_BATCH = 18
    PARSER_DONE = 19
    SCATTER = 20


class ScatterPhase(IntEnum):
    BEGIN = 1
    GRANT_REQUEST = 2
    GRANT = 3
    DISK_REQUEST = 4
    DISK_CREDIT = 5
    DISK_COMMIT = 6
    DISK_COMMITTED = 7
    RESULT = 8
    DONE = 9


class Stage(IntEnum):
    SORT_IDENTITY = 1


class FailureCode(IntEnum):
    INVALID_INPUT = 1
    RESOURCE = 2
    CANCELLED = 3
    INTERNAL = 4
    RANK_COLLISION = 5
    INPUT = 6


class _Message:
    __slots__ = ()

    def __post_init__(self):
        _validate(self)


@dataclass(frozen=True, slots=True)
class Ready(_Message):
    rss_bytes: int
    descriptors: int
    threads: int


@dataclass(frozen=True, slots=True)
class WorkerJob(_Message):
    run_id: bytes = field(repr=False)
    task_id: int
    grant_id: int
    input_bytes: int
    workspace_bytes: int
    input_sha256: bytes = field(repr=False)
    stage: Stage = Stage.SORT_IDENTITY


@dataclass(frozen=True, slots=True)
class Chunk(_Message):
    task_id: int
    grant_id: int
    offset: int
    payload: bytes = field(repr=False)


@dataclass(frozen=True, slots=True)
class ChunkAck(_Message):
    task_id: int
    grant_id: int
    end: int


@dataclass(frozen=True, slots=True)
class InputDone(_Message):
    task_id: int
    grant_id: int


@dataclass(frozen=True, slots=True)
class WorkerResult(_Message):
    task_id: int
    grant_id: int
    output_bytes: int
    records: int
    digest: bytes = field(repr=False)


@dataclass(frozen=True, slots=True)
class ResultAck(_Message):
    task_id: int
    grant_id: int


@dataclass(frozen=True, slots=True)
class Stop(_Message):
    pass


@dataclass(frozen=True, slots=True)
class WorkerFailure(_Message):
    task_id: int
    grant_id: int
    code: FailureCode


@dataclass(frozen=True, slots=True)
class ExternalJob(_Message):
    job: WorkerJob = field(repr=False)
    input_grant: bytes = field(repr=False)
    output_grant: bytes = field(repr=False)

    @property
    def task_id(self):
        return self.job.task_id

    @property
    def grant_id(self):
        return self.job.grant_id


@dataclass(frozen=True, slots=True)
class PolicyJob(_Message):
    job: object = field(repr=False)
    policy: bytes = field(repr=False)

    @property
    def task_id(self):
        return self.job.task_id

    @property
    def grant_id(self):
        return self.job.grant_id


@dataclass(frozen=True, slots=True)
class ParserJob(_Message):
    job: WorkerJob = field(repr=False)
    capability: bytes = field(repr=False)

    @property
    def task_id(self):
        return self.job.task_id

    @property
    def grant_id(self):
        return self.job.grant_id


@dataclass(frozen=True, slots=True)
class ParserBatch(_Message):
    task_id: int
    grant_id: int
    batch: int
    output_bytes: int
    records: int
    normalized_bytes: int
    digest: bytes = field(repr=False)


@dataclass(frozen=True, slots=True)
class ParserDone(_Message):
    task_id: int
    grant_id: int
    report: bytes = field(repr=False)


@dataclass(frozen=True, slots=True)
class ScatterMessage(_Message):
    task_id: int
    grant_id: int
    batch: int
    phase: ScatterPhase
    bucket: int = 0
    offset: int = 0
    amount: int = 0
    records: int = 0
    maximum: int = 0
    digest: bytes = field(default=bytes(32), repr=False)
    payload: bytes = field(default=b'', repr=False)


@dataclass(frozen=True, slots=True)
class SegmentResult(_Message):
    task_id: int
    grant_id: int
    reference: bytes = field(repr=False)


@dataclass(frozen=True, slots=True)
class DiskRequest(_Message):
    task_id: int
    grant_id: int
    offset: int
    amount: int


@dataclass(frozen=True, slots=True)
class DiskCredit(DiskRequest):
    pass


@dataclass(frozen=True, slots=True)
class DiskCommit(DiskRequest):
    pass


@dataclass(frozen=True, slots=True)
class DiskCommitted(DiskRequest):
    pass


MAX_GRANT_BYTES = 16 << 10
MAX_REFERENCE_BYTES = 8192
MAX_DISK_INCREMENT = (1 << 20) + 128


_TYPES = {
    Ready: MessageKind.READY, WorkerJob: MessageKind.JOB, Chunk: MessageKind.CHUNK,
    ChunkAck: MessageKind.CHUNK_ACK, InputDone: MessageKind.INPUT_DONE,
    WorkerResult: MessageKind.RESULT, ResultAck: MessageKind.RESULT_ACK,
    Stop: MessageKind.STOP, WorkerFailure: MessageKind.FAILURE,
    PolicyJob: MessageKind.POLICY_JOB, ParserJob: MessageKind.PARSER_JOB,
    ParserBatch: MessageKind.PARSER_BATCH, ParserDone: MessageKind.PARSER_DONE,
    ScatterMessage: MessageKind.SCATTER,
    ExternalJob: MessageKind.EXTERNAL_JOB, SegmentResult: MessageKind.SEGMENT_RESULT,
    DiskRequest: MessageKind.DISK_REQUEST, DiskCredit: MessageKind.DISK_CREDIT,
    DiskCommit: MessageKind.DISK_COMMIT, DiskCommitted: MessageKind.DISK_COMMITTED,
}
_FORMATS = {
    MessageKind.READY: struct.Struct('>QQQ'),
    MessageKind.JOB: struct.Struct('>16sBQQQQ32s'),
    MessageKind.CHUNK: struct.Struct('>QQQ'),
    MessageKind.CHUNK_ACK: struct.Struct('>QQQ'),
    MessageKind.INPUT_DONE: struct.Struct('>QQ'),
    MessageKind.RESULT: struct.Struct('>QQQQ32s'),
    MessageKind.RESULT_ACK: struct.Struct('>QQ'),
    MessageKind.STOP: struct.Struct('>'),
    MessageKind.FAILURE: struct.Struct('>QQB'),
    MessageKind.EXTERNAL_JOB: struct.Struct('>HHH'),
    MessageKind.POLICY_JOB: struct.Struct('>HH'),
    MessageKind.PARSER_JOB: struct.Struct('>HH'),
    MessageKind.PARSER_BATCH: struct.Struct('>6Q32s'),
    MessageKind.PARSER_DONE: struct.Struct('>QQ'),
    MessageKind.SCATTER: struct.Struct('>QQQBBQQQI32sH'),
    MessageKind.SEGMENT_RESULT: struct.Struct('>QQH'),
    **{kind: struct.Struct('>QQQQ') for kind in (MessageKind.DISK_REQUEST,
       MessageKind.DISK_CREDIT, MessageKind.DISK_COMMIT, MessageKind.DISK_COMMITTED)},
}
MAX_FRAME_BYTES = HEADER_BYTES + _FORMATS[MessageKind.CHUNK].size + MAX_CHUNK_BYTES


def _uint(value, label, *, positive=False):
    if type(value) is not int or not int(positive) <= value <= _U64_MAX:
        raise ConfigurationError(f'worker protocol {label} must be a bounded unsigned integer')


def _bytes(value, size, label):
    if type(value) is not bytes or len(value) != size:
        raise ConfigurationError(f'worker protocol {label} has an invalid byte width')


def _validate(message):
    kind = _TYPES.get(type(message))
    if kind is None:
        raise ConfigurationError('worker protocol message type is unsupported')
    if kind == MessageKind.SCATTER:
        if not any(message.phase is value for value in ScatterPhase):
            raise ConfigurationError('scatter phase is invalid')
        for name in ('task_id', 'batch', 'offset', 'amount', 'records', 'maximum'):
            _uint(getattr(message, name), name)
        _uint(message.grant_id, 'grant identity', positive=True)
        if type(message.bucket) is not int or not 0 <= message.bucket < 64 or message.maximum > (1 << 32) - 1:
            raise ConfigurationError('scatter bucket or frame maximum is invalid')
        _bytes(message.digest, 32, 'scatter digest')
        if type(message.payload) is not bytes or len(message.payload) > MAX_GRANT_BYTES:
            raise ConfigurationError('scatter control payload exceeds its bound')
        phase = message.phase
        if phase is ScatterPhase.RESULT:
            if (message.offset or message.amount > 64 << 20 or len(message.payload) > MAX_REFERENCE_BYTES
                    or message.records > message.amount // 48 or message.maximum > message.amount
                    or bool(message.records) != bool(message.amount) or bool(message.maximum) != bool(message.amount)):
                raise ConfigurationError('scatter result statistics are inconsistent')
        elif message.records or message.maximum or message.digest != bytes(32):
            raise ConfigurationError('scatter control contains unrelated result fields')
        elif phase is ScatterPhase.GRANT:
            if not message.payload or message.offset or message.amount:
                raise ConfigurationError('scatter grant response is invalid')
        elif message.payload:
            raise ConfigurationError('scatter control contains unrelated capability bytes')
        elif phase in (ScatterPhase.DISK_REQUEST, ScatterPhase.DISK_CREDIT, ScatterPhase.DISK_COMMIT, ScatterPhase.DISK_COMMITTED):
            if message.amount > MAX_DISK_INCREMENT or message.offset > _U64_MAX - message.amount:
                raise ConfigurationError('scatter disk increment exceeds its bound')
            if phase in (ScatterPhase.DISK_REQUEST, ScatterPhase.DISK_CREDIT) and not message.amount:
                raise ConfigurationError('scatter disk request must be positive')
        elif phase is ScatterPhase.BEGIN:
            if message.bucket or message.amount:
                raise ConfigurationError('scatter generation grant is invalid')
        elif message.offset or message.amount or phase is ScatterPhase.DONE and message.bucket:
            raise ConfigurationError('scatter control contains unrelated counters')
    elif kind == MessageKind.PARSER_JOB:
        from .parser_stage import MAX_SOURCE_BYTES, decode_source
        if type(message.job) is not WorkerJob:
            raise ConfigurationError('parser needs a closed job schema')
        _validate(message.job)
        if type(message.capability) is not bytes or len(message.capability) > MAX_SOURCE_BYTES:
            raise ConfigurationError('parser source capability exceeds its bound')
        try:
            decode_source(message.capability)
        except IntegrityError:
            raise ConfigurationError('parser source capability is malformed') from None
    elif kind == MessageKind.PARSER_DONE:
        _uint(message.task_id, 'task identity')
        _uint(message.grant_id, 'grant identity', positive=True)
        from .parser_stage import decode_report
        try:
            decode_report(message.report)
        except IntegrityError:
            raise ConfigurationError('parser report is malformed') from None
    elif kind == MessageKind.PARSER_BATCH:
        for name in ('task_id', 'batch', 'output_bytes', 'records', 'normalized_bytes'):
            _uint(getattr(message, name), name)
        _uint(message.grant_id, 'grant identity', positive=True)
        _bytes(message.digest, 32, 'batch digest')
        if not 48 <= message.output_bytes <= 64 << 20 or not 1 <= message.records <= message.output_bytes // 48:
            raise ConfigurationError('parser batch exceeds its bounded envelope')
    elif kind == MessageKind.POLICY_JOB:
        from .stage_policy import MAX_POLICY_BYTES, decode_policy
        if type(message.job) not in (WorkerJob, ExternalJob):
            raise ConfigurationError('policy job needs a closed input schema')
        _validate(message.job)
        if type(message.policy) is not bytes or not 1 <= len(message.policy) <= MAX_POLICY_BYTES:
            raise ConfigurationError('worker policy exceeds its control bound')
        try:
            decode_policy(message.policy)
        except IntegrityError:
            raise ConfigurationError('worker policy is malformed') from None
    elif kind == MessageKind.EXTERNAL_JOB:
        if type(message.job) is not WorkerJob:
            raise ConfigurationError('external worker job needs a closed job schema')
        _validate(message.job)
        for grant in (message.input_grant, message.output_grant):
            if type(grant) is not bytes or not 1 <= len(grant) <= MAX_GRANT_BYTES:
                raise ConfigurationError('worker storage capability exceeds its envelope')
    elif kind == MessageKind.READY:
        for name in ('rss_bytes', 'descriptors', 'threads'):
            _uint(getattr(message, name), name)
    elif kind != MessageKind.STOP:
        _uint(message.task_id, 'task identity')
        _uint(message.grant_id, 'grant identity', positive=True)
        if kind == MessageKind.JOB:
            _bytes(message.run_id, 16, 'run identity')
            _bytes(message.input_sha256, 32, 'input digest')
            _uint(message.input_bytes, 'input byte count')
            _uint(message.workspace_bytes, 'workspace byte count')
            if message.stage is not Stage.SORT_IDENTITY:
                raise ConfigurationError('worker protocol stage is unsupported')
        elif kind == MessageKind.CHUNK:
            _uint(message.offset, 'chunk offset')
            if type(message.payload) is not bytes or not 1 <= len(message.payload) <= MAX_CHUNK_BYTES:
                raise ConfigurationError('worker protocol chunk must contain 1..1048576 raw bytes')
            if len(message.payload) > _U64_MAX - message.offset:
                raise ConfigurationError('worker protocol chunk end exceeds its unsigned width')
        elif kind == MessageKind.CHUNK_ACK:
            _uint(message.end, 'acknowledged chunk end')
        elif kind == MessageKind.RESULT:
            _uint(message.output_bytes, 'output byte count')
            _uint(message.records, 'record count')
            _bytes(message.digest, 32, 'output digest')
        elif kind == MessageKind.SEGMENT_RESULT:
            if type(message.reference) is not bytes or not 1 <= len(message.reference) <= MAX_REFERENCE_BYTES:
                raise ConfigurationError('worker segment reference exceeds its envelope')
        elif kind in (MessageKind.DISK_REQUEST, MessageKind.DISK_CREDIT,
                      MessageKind.DISK_COMMIT, MessageKind.DISK_COMMITTED):
            _uint(message.offset, 'disk offset')
            _uint(message.amount, 'disk increment')
            if message.amount > MAX_DISK_INCREMENT or message.offset > _U64_MAX - message.amount:
                raise ConfigurationError('worker disk increment exceeds its envelope')
            if not message.amount and kind in (MessageKind.DISK_REQUEST, MessageKind.DISK_CREDIT):
                raise ConfigurationError('worker disk request must be positive')
        elif kind == MessageKind.FAILURE:
            if not any(message.code is code for code in FailureCode):
                raise ConfigurationError('worker protocol failure code is unsupported')
    return kind


def inspect_header(header: bytes) -> tuple[MessageKind, int]:
    """Check an exact fixed header before the transport allocates its body."""
    if type(header) is not bytes or len(header) != HEADER_BYTES:
        raise IntegrityError('worker protocol header has an invalid length or type')
    magic, version, number, body_length = _HEADER.unpack(header)
    if magic != MAGIC or version != VERSION:
        raise IntegrityError('worker protocol magic or version is unsupported')
    try:
        kind = MessageKind(number)
    except ValueError:
        raise IntegrityError('worker protocol message kind is unsupported') from None
    fixed = _FORMATS[kind].size
    if kind == MessageKind.CHUNK:
        if not fixed < body_length <= fixed + MAX_CHUNK_BYTES:
            raise IntegrityError('worker protocol raw chunk length exceeds its bound')
    else:
        if body_length > MAX_CONTROL_BYTES - HEADER_BYTES:
            raise IntegrityError('worker protocol control envelope exceeds its bound')
        if kind == MessageKind.SCATTER:
            if not fixed <= body_length <= fixed + MAX_GRANT_BYTES:
                raise IntegrityError('scatter message exceeds its control bound')
        elif kind == MessageKind.PARSER_JOB:
            from .parser_stage import MAX_SOURCE_BYTES
            if not fixed + HEADER_BYTES + _FORMATS[MessageKind.JOB].size < body_length <= fixed + HEADER_BYTES + _FORMATS[MessageKind.JOB].size + MAX_SOURCE_BYTES:
                raise IntegrityError('parser job exceeds its control envelope')
        elif kind == MessageKind.PARSER_DONE:
            from .parser_stage import _REPORT
            if body_length != fixed + _REPORT.size:
                raise IntegrityError('parser report has an invalid width')
        elif kind == MessageKind.POLICY_JOB:
            if not fixed + HEADER_BYTES + _FORMATS[MessageKind.JOB].size < body_length:
                raise IntegrityError('worker policy job length is invalid')
        elif kind == MessageKind.EXTERNAL_JOB:
            if not fixed + HEADER_BYTES + _FORMATS[MessageKind.JOB].size + 2 <= body_length <= fixed + HEADER_BYTES + _FORMATS[MessageKind.JOB].size + 2 * MAX_GRANT_BYTES:
                raise IntegrityError('worker external job length exceeds its bound')
        elif kind == MessageKind.SEGMENT_RESULT:
            if not fixed < body_length <= fixed + MAX_REFERENCE_BYTES:
                raise IntegrityError('worker segment result length exceeds its bound')
        elif body_length != fixed:
            raise IntegrityError('worker protocol control message has an invalid length')
    return kind, body_length


def encode_message(message) -> bytes:
    """Encode exactly one validated message; no generic object serialization."""
    kind = _validate(message)
    if kind == MessageKind.SCATTER:
        body = _FORMATS[kind].pack(message.task_id, message.grant_id, message.batch, message.phase,
            message.bucket, message.offset, message.amount, message.records, message.maximum,
            message.digest, len(message.payload)) + message.payload
        return _HEADER.pack(MAGIC, VERSION, kind, len(body)) + body
    if kind == MessageKind.PARSER_JOB:
        nested = encode_message(message.job)
        body = _FORMATS[kind].pack(len(nested), len(message.capability)) + nested + message.capability
        return _HEADER.pack(MAGIC, VERSION, kind, len(body)) + body
    if kind == MessageKind.PARSER_DONE:
        body = _FORMATS[kind].pack(message.task_id, message.grant_id) + message.report
        return _HEADER.pack(MAGIC, VERSION, kind, len(body)) + body
    if kind == MessageKind.POLICY_JOB:
        nested = encode_message(message.job)
        body = _FORMATS[kind].pack(len(nested), len(message.policy)) + nested + message.policy
        if len(body) > MAX_CONTROL_BYTES - HEADER_BYTES:
            raise ConfigurationError('worker policy job exceeds its control envelope')
        return _HEADER.pack(MAGIC, VERSION, kind, len(body)) + body
    if kind == MessageKind.EXTERNAL_JOB:
        job = encode_message(message.job)
        body = _FORMATS[kind].pack(len(job), len(message.input_grant), len(message.output_grant))
        body = b''.join((body, job, message.input_grant, message.output_grant))
        return _HEADER.pack(MAGIC, VERSION, kind, len(body)) + body
    if kind == MessageKind.SEGMENT_RESULT:
        body = _FORMATS[kind].pack(message.task_id, message.grant_id, len(message.reference)) + message.reference
        return _HEADER.pack(MAGIC, VERSION, kind, len(body)) + body
    if kind == MessageKind.READY:
        values = message.rss_bytes, message.descriptors, message.threads
    elif kind == MessageKind.JOB:
        values = (message.run_id, int(message.stage), message.task_id, message.grant_id,
                  message.input_bytes, message.workspace_bytes, message.input_sha256)
    elif kind == MessageKind.CHUNK:
        values = message.task_id, message.grant_id, message.offset
    elif kind == MessageKind.CHUNK_ACK:
        values = message.task_id, message.grant_id, message.end
    elif kind in (MessageKind.INPUT_DONE, MessageKind.RESULT_ACK):
        values = message.task_id, message.grant_id
    elif kind == MessageKind.PARSER_BATCH:
        values = message.task_id, message.grant_id, message.batch, message.output_bytes, message.records, message.normalized_bytes, message.digest
    elif kind == MessageKind.RESULT:
        values = message.task_id, message.grant_id, message.output_bytes, message.records, message.digest
    elif kind in (MessageKind.DISK_REQUEST, MessageKind.DISK_CREDIT,
                  MessageKind.DISK_COMMIT, MessageKind.DISK_COMMITTED):
        values = message.task_id, message.grant_id, message.offset, message.amount
    elif kind == MessageKind.FAILURE:
        values = message.task_id, message.grant_id, int(message.code)
    else:
        values = ()
    body = _FORMATS[kind].pack(*values)
    payload = message.payload if kind == MessageKind.CHUNK else b''
    header = _HEADER.pack(MAGIC, VERSION, kind, len(body) + len(payload))
    # Joining once avoids an extra complete raw-body copy for large chunks.
    return b''.join((header, body, payload))


def decode_message(data: bytes):
    """Decode one whole immutable frame, rejecting truncation and trailing data."""
    if type(data) is not bytes or len(data) < HEADER_BYTES:
        raise IntegrityError('worker protocol frame has an invalid length or type')
    kind, body_length = inspect_header(data[:HEADER_BYTES])
    if len(data) != HEADER_BYTES + body_length:
        raise IntegrityError('worker protocol frame length differs from its prefix')
    values = _FORMATS[kind].unpack_from(data, HEADER_BYTES)
    try:
        if kind == MessageKind.SCATTER:
            task, grant, batch, phase, bucket, offset, amount, records, maximum, digest, size = values
            payload = data[HEADER_BYTES + _FORMATS[kind].size:]
            if len(payload) != size:
                raise IntegrityError('scatter message length differs from its prefix')
            return ScatterMessage(task, grant, batch, ScatterPhase(phase), bucket, offset, amount, records, maximum, digest, payload)
        if kind == MessageKind.PARSER_JOB:
            a, b = values
            offset = HEADER_BYTES + _FORMATS[kind].size
            if a != HEADER_BYTES + _FORMATS[MessageKind.JOB].size or offset + a + b != len(data):
                raise IntegrityError('parser job nested lengths are invalid')
            nested = data[offset:offset + a]
            if inspect_header(nested[:HEADER_BYTES])[0] != MessageKind.JOB:
                raise IntegrityError('parser job contains an invalid nested schema')
            return ParserJob(decode_message(nested), data[offset + a:])
        if kind == MessageKind.PARSER_DONE:
            return ParserDone(*values, data[HEADER_BYTES + _FORMATS[kind].size:])
        if kind == MessageKind.PARSER_BATCH:
            return ParserBatch(*values)
        if kind == MessageKind.POLICY_JOB:
            a, b = values
            offset = HEADER_BYTES + _FORMATS[kind].size
            from .stage_policy import MAX_POLICY_BYTES
            if not 1 <= b <= MAX_POLICY_BYTES or a < HEADER_BYTES or offset + a + b != len(data):
                raise IntegrityError('policy job has invalid nested lengths')
            nested = data[offset:offset + a]
            if inspect_header(nested[:HEADER_BYTES])[0] not in (MessageKind.JOB, MessageKind.EXTERNAL_JOB):
                raise IntegrityError('policy job contains an invalid nested schema')
            return PolicyJob(decode_message(nested), data[offset + a:])
        if kind == MessageKind.EXTERNAL_JOB:
            a, b, c = values
            offset = HEADER_BYTES + _FORMATS[kind].size
            if a != HEADER_BYTES + _FORMATS[MessageKind.JOB].size or not 1 <= b <= MAX_GRANT_BYTES or not 1 <= c <= MAX_GRANT_BYTES or offset + a + b + c != len(data):
                raise IntegrityError('external job has invalid capability lengths')
            nested = data[offset:offset + a]
            if inspect_header(nested[:HEADER_BYTES])[0] != MessageKind.JOB:
                raise IntegrityError('external job contains an invalid nested schema')
            return ExternalJob(decode_message(nested), data[offset+a:offset+a+b], data[offset+a+b:])
        if kind == MessageKind.SEGMENT_RESULT:
            task, grant, size = values
            reference = data[HEADER_BYTES + _FORMATS[kind].size:]
            if size != len(reference):
                raise IntegrityError('segment result has invalid reference length')
            return SegmentResult(task, grant, reference)
        if kind in (MessageKind.DISK_REQUEST, MessageKind.DISK_CREDIT,
                    MessageKind.DISK_COMMIT, MessageKind.DISK_COMMITTED):
            cls = {MessageKind.DISK_REQUEST: DiskRequest, MessageKind.DISK_CREDIT: DiskCredit,
                   MessageKind.DISK_COMMIT: DiskCommit, MessageKind.DISK_COMMITTED: DiskCommitted}[kind]
            return cls(*values)
        if kind == MessageKind.READY:
            return Ready(*values)
        if kind == MessageKind.JOB:
            run_id, stage, task, grant, input_bytes, workspace, digest = values
            return WorkerJob(run_id, task, grant, input_bytes, workspace, digest, Stage(stage))
        if kind == MessageKind.CHUNK:
            return Chunk(*values, data[HEADER_BYTES + _FORMATS[kind].size:])
        if kind == MessageKind.CHUNK_ACK:
            return ChunkAck(*values)
        if kind == MessageKind.INPUT_DONE:
            return InputDone(*values)
        if kind == MessageKind.RESULT:
            return WorkerResult(*values)
        if kind == MessageKind.RESULT_ACK:
            return ResultAck(*values)
        if kind == MessageKind.FAILURE:
            task, grant, code = values
            return WorkerFailure(task, grant, FailureCode(code))
        return Stop()
    except (ConfigurationError, ValueError):
        raise IntegrityError('worker protocol message has invalid scalar fields') from None
