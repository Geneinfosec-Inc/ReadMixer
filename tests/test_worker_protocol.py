"""Independent wire vectors, bounded headers, and redacted worker messages."""

from dataclasses import FrozenInstanceError, replace
from enum import IntEnum
import struct

import pytest

from readmixer_py.errors import ConfigurationError, IntegrityError
from readmixer_py.worker_protocol import (
    HEADER_BYTES, MAX_CHUNK_BYTES, MAX_CONTROL_BYTES, MAX_FRAME_BYTES,
    Chunk, ChunkAck, FailureCode, InputDone, MessageKind, Ready, ResultAck,
    Stage, Stop, WorkerFailure, WorkerJob, WorkerResult,
    decode_message, encode_message, inspect_header,
)


U64 = (1 << 64) - 1
RUN = bytes(range(16))
DIGEST = bytes(range(32, 64))


def frame(kind, body, *, declared=None, version=1, magic=b'RMPYWRK1'):
    # Construct the prefix independently: the length counts only the body.
    return (magic + bytes((version, kind))
            + (len(body) if declared is None else declared).to_bytes(4, 'big') + body)


VECTORS = [
    (Ready(0x01020304, 8, 1), 1, struct.pack('>QQQ', 0x01020304, 8, 1)),
    (WorkerJob(RUN, 0x0102030405060708, 0x1112131415161718, 19, 20, DIGEST), 2,
     RUN + b'\x01' + struct.pack('>QQQQ', 0x0102030405060708, 0x1112131415161718, 19, 20) + DIGEST),
    (Chunk(2, 3, 4, b'\0opaque\xff\r\n'), 3, struct.pack('>QQQ', 2, 3, 4) + b'\0opaque\xff\r\n'),
    (ChunkAck(2, 3, 14), 4, struct.pack('>QQQ', 2, 3, 14)),
    (InputDone(2, 3), 5, struct.pack('>QQ', 2, 3)),
    (WorkerResult(2, 3, 100, 4, DIGEST), 6, struct.pack('>QQQQ', 2, 3, 100, 4) + DIGEST),
    (ResultAck(2, 3), 7, struct.pack('>QQ', 2, 3)),
    (Stop(), 8, b''),
    (WorkerFailure(2, 3, FailureCode.INVALID_INPUT), 9, struct.pack('>QQB', 2, 3, 1)),
]


@pytest.mark.parametrize('message,kind,body', VECTORS, ids=lambda value: type(value).__name__ if hasattr(value, '__dataclass_fields__') else None)
def test_P13_T03_exact_independent_wire_vectors(message, kind, body):
    expected = frame(kind, body)
    assert HEADER_BYTES == 14
    assert encode_message(message) == expected
    assert inspect_header(expected[:14]) == (MessageKind(kind), len(body))
    assert decode_message(expected) == message
    assert len(expected) <= MAX_CONTROL_BYTES or kind == 3


@pytest.mark.parametrize('message,kind,body', VECTORS)
def test_P13_T03_truncation_trailing_and_concatenation_are_never_messages(message, kind, body):
    complete = frame(kind, body)
    for bad in (complete[:-1], complete + b'\0', complete + complete):
        with pytest.raises(IntegrityError):
            decode_message(bad)


def test_P13_T03_all_job_prefix_truncations_are_checked_before_unpack():
    complete = frame(2, VECTORS[1][2])
    for length in range(len(complete)):
        with pytest.raises(IntegrityError):
            decode_message(complete[:length])


@pytest.mark.parametrize('version,kind,magic', [
    (0, 1, b'RMPYWRK1'), (2, 1, b'RMPYWRK1'), (255, 1, b'RMPYWRK1'),
    (1, 0, b'RMPYWRK1'), (1, 16, b'RMPYWRK1'), (1, 255, b'RMPYWRK1'),
    (1, 1, b'RMPYWRK2'), (1, 1, b'\0' * 8),
])
def test_P13_T03_unknown_prefix_values_fail_in_header_only_inspection(version, kind, magic):
    header = frame(kind, b'', declared=24, version=version, magic=magic)
    with pytest.raises(IntegrityError):
        inspect_header(header)
    with pytest.raises(IntegrityError):
        decode_message(header)


@pytest.mark.parametrize('kind', [1, 2, 4, 5, 6, 7, 8, 9])
def test_P13_T03_control_lengths_reject_before_body_allocation(kind):
    for length in (65536 - HEADER_BYTES, 65537 - HEADER_BYTES, (1 << 32) - 1):
        with pytest.raises(IntegrityError):
            inspect_header(frame(kind, b'', declared=length))
    exact = next(len(body) for _, number, body in VECTORS if number == kind)
    with pytest.raises(IntegrityError):
        inspect_header(frame(kind, b'', declared=exact + 1))


@pytest.mark.parametrize('length', [0, 23, 24, 24 + (1 << 20) + 1, (1 << 32) - 1])
def test_P13_T05_raw_chunk_header_rejects_empty_and_excess_length(length):
    with pytest.raises(IntegrityError):
        inspect_header(frame(3, b'', declared=length))


def test_P13_T05_exact_raw_chunk_limit_is_separate_from_control_envelope():
    payload = b'\0\xff' * (MAX_CHUNK_BYTES // 2)
    message = Chunk(0, U64, 1, payload)
    encoded = encode_message(message)
    assert MAX_CONTROL_BYTES == 65536 and MAX_CHUNK_BYTES == 1048576
    assert len(encoded) == MAX_FRAME_BYTES == 1048614
    assert len(encoded) > MAX_CONTROL_BYTES
    assert inspect_header(encoded[:HEADER_BYTES]) == (MessageKind.CHUNK, 24 + MAX_CHUNK_BYTES)
    assert decode_message(encoded) == message
    with pytest.raises(ConfigurationError):
        Chunk(0, 1, 0, payload + b'x')
    with pytest.raises(ConfigurationError):
        Chunk(0, 1, 0, b'')


COUNTER_CASES = [
    (Ready(0, 0, 0), 'rss_bytes'), (Ready(0, 0, 0), 'descriptors'), (Ready(0, 0, 0), 'threads'),
    (WorkerJob(RUN, 0, 1, 0, 0, DIGEST), 'task_id'),
    (WorkerJob(RUN, 0, 1, 0, 0, DIGEST), 'grant_id'),
    (WorkerJob(RUN, 0, 1, 0, 0, DIGEST), 'input_bytes'),
    (WorkerJob(RUN, 0, 1, 0, 0, DIGEST), 'workspace_bytes'),
    (Chunk(0, 1, 0, b'x'), 'offset'), (ChunkAck(0, 1, 0), 'end'),
    (WorkerResult(0, 1, 0, 0, DIGEST), 'output_bytes'),
    (WorkerResult(0, 1, 0, 0, DIGEST), 'records'),
]


@pytest.mark.parametrize('message,name', COUNTER_CASES)
@pytest.mark.parametrize('value', [True, -1, 1 << 64, 1.5])
def test_P13_T03_counters_have_strict_unsigned_types_and_widths(message, name, value):
    with pytest.raises(ConfigurationError):
        replace(message, **{name: value})
    # Revalidate at encode even if a caller deliberately bypasses frozen fields.
    altered = replace(message)
    object.__setattr__(altered, name, value)
    with pytest.raises(ConfigurationError):
        encode_message(altered)


def test_P13_T03_unsigned_maxima_and_chunk_end_boundary():
    messages = [Ready(U64, U64, U64), WorkerJob(RUN, U64, U64, U64, U64, DIGEST),
                Chunk(U64, U64, U64 - 1, b'x'), ChunkAck(U64, U64, U64),
                InputDone(U64, U64), WorkerResult(U64, U64, U64, U64, DIGEST),
                ResultAck(U64, U64), WorkerFailure(U64, U64, FailureCode.INTERNAL)]
    for message in messages:
        assert decode_message(encode_message(message)) == message
    with pytest.raises(ConfigurationError):
        Chunk(0, 1, U64, b'x')
    with pytest.raises(IntegrityError):
        decode_message(frame(3, struct.pack('>QQQ', 0, 1, U64) + b'x'))


# STOP is not task-bound; its wire format is covered by the vector tests.
@pytest.mark.parametrize('message,kind,body', [vector for vector in VECTORS[1:] if vector[1] != 8])
def test_P13_T03_every_task_bound_message_requires_positive_grant(message, kind, body):
    with pytest.raises(ConfigurationError):
        replace(message, grant_id=0)
    changed = bytearray(body)
    start = 25 if kind == 2 else 8  # JOB starts with run16/stage1/task8.
    changed[start:start + 8] = bytes(8)
    with pytest.raises(IntegrityError):
        decode_message(frame(kind, bytes(changed)))


@pytest.mark.parametrize('name,value', [('run_id', b'x' * 15), ('run_id', b'x' * 17),
    ('run_id', bytearray(16)), ('input_sha256', b'x' * 31), ('input_sha256', b'x' * 33),
    ('input_sha256', memoryview(bytes(32)))])
def test_P13_T03_job_byte_fields_are_exact_immutable_widths(name, value):
    with pytest.raises(ConfigurationError):
        replace(WorkerJob(RUN, 0, 1, 0, 0, DIGEST), **{name: value})


@pytest.mark.parametrize('value', [b'x' * 31, b'x' * 33, bytearray(32), memoryview(bytes(32)), 'private'])
def test_P13_T03_result_digest_is_exact_immutable_width(value):
    with pytest.raises(ConfigurationError):
        WorkerResult(0, 1, 0, 0, value)


class ForeignCode(IntEnum):
    FIRST = 1


@pytest.mark.parametrize('value', [1, True, 'SORT_IDENTITY', ForeignCode.FIRST])
def test_P13_T03_stage_and_failure_enums_do_not_accept_coercions(value):
    with pytest.raises(ConfigurationError):
        WorkerJob(RUN, 0, 1, 0, 0, DIGEST, stage=value)
    with pytest.raises(ConfigurationError):
        WorkerFailure(0, 1, value)


@pytest.mark.parametrize('number', [0, 2, 255])
def test_P13_T03_unknown_stage_values_are_not_jobs(number):
    body = bytearray(VECTORS[1][2])
    body[16] = number
    with pytest.raises(IntegrityError):
        decode_message(frame(2, bytes(body)))


@pytest.mark.parametrize('number', [0, 254, 255])
def test_P13_T03_unknown_failure_values_are_not_diagnostics(number):
    with pytest.raises(IntegrityError):
        decode_message(frame(9, struct.pack('>QQB', 0, 1, number)))


@pytest.mark.parametrize('code', list(FailureCode))
def test_P13_T03_all_fixed_failure_codes_have_exact_binary_values(code):
    message = WorkerFailure(0, 1, code)
    assert encode_message(message) == frame(9, struct.pack('>QQB', 0, 1, int(code)))
    assert decode_message(encode_message(message)) == message


@pytest.mark.parametrize('value', [bytearray(14), memoryview(bytes(14)), 'private diagnostics', None, 14])
def test_P13_T03_decoder_and_header_reject_nonimmutable_types(value):
    for decoder in (inspect_header, decode_message):
        with pytest.raises(IntegrityError) as caught:
            decoder(value)
        assert 'private diagnostics' not in str(caught.value)


def test_P13_T03_closed_objects_never_invoke_pickle_or_accept_arbitrary_fields():
    class Unknown:
        def __reduce__(self):
            pytest.fail('the protocol must never attempt generic serialization')
        def __repr__(self):
            return 'private diagnostics'
    for value in (Unknown(), {'path': 'private diagnostics'}, lambda: None):
        with pytest.raises(ConfigurationError) as caught:
            encode_message(value)
        assert 'private diagnostics' not in str(caught.value)
    with pytest.raises(TypeError):
        WorkerFailure(0, 1, FailureCode.INTERNAL, diagnostics='private diagnostics')
    with pytest.raises((FrozenInstanceError, TypeError, AttributeError)):
        Stop().path = 'private diagnostics'
    with pytest.raises(FrozenInstanceError):
        Ready(0, 0, 0).rss_bytes = 1


def test_P13_T03_printable_messages_never_include_payload_run_or_digest_bytes():
    secret = b'private opaque marker'
    job = WorkerJob(secret[:16], 0, 1, 30, 40, secret.ljust(32, b'!'))
    result = WorkerResult(0, 1, 30, 2, secret.ljust(32, b'?'))
    chunk = Chunk(0, 1, 0, secret)
    for message in (job, result, chunk):
        rendered = str(message) + repr(message)
        assert 'private' not in rendered and 'opaque' not in rendered
        assert secret.hex() not in rendered
        assert not hasattr(message, '__dict__')
    assert repr(WorkerFailure(0, 1, FailureCode.INTERNAL)) == (
        'WorkerFailure(task_id=0, grant_id=1, code=<FailureCode.INTERNAL: 4>)')
