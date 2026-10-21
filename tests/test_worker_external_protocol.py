"""P13-T03/T04 independent bounds for protected-storage control messages."""
from dataclasses import replace
import struct

import pytest

from readmixer_py import worker_protocol as protocol
from readmixer_py.errors import ConfigurationError, IntegrityError
from readmixer_py.worker_protocol import (
    DiskCommit, DiskCommitted, DiskCredit, DiskRequest, ExternalJob, SegmentResult,
    WorkerJob, decode_message, encode_message, inspect_header,
)

U64 = (1 << 64) - 1
RUN = bytes(range(16))
DIGEST = bytes(range(32))
JOB = WorkerJob(RUN, 7, 11, 13, 17, DIGEST)
JOB_BODY = RUN + b'\x01' + struct.pack('>QQQQ', 7, 11, 13, 17) + DIGEST


def wire(kind, body, *, declared=None):
    return b'RMPYWRK1' + bytes((1, kind)) + struct.pack('>I', len(body) if declared is None else declared) + body


JOB_WIRE = wire(2, JOB_BODY)
VECTORS = [
    (ExternalJob(JOB, b'\x00private-input\xff', b'\xfeprivate-output\x00'), 10,
     struct.pack('>HHH', len(JOB_WIRE), 15, 16) + JOB_WIRE + b'\x00private-input\xff' + b'\xfeprivate-output\x00'),
    (SegmentResult(7, 11, b'\x00private-reference\xff'), 11,
     struct.pack('>QQH', 7, 11, 19) + b'\x00private-reference\xff'),
    (DiskRequest(7, 11, 23, 29), 12, struct.pack('>QQQQ', 7, 11, 23, 29)),
    (DiskCredit(7, 11, 23, 29), 13, struct.pack('>QQQQ', 7, 11, 23, 29)),
    (DiskCommit(7, 11, 23, 29), 14, struct.pack('>QQQQ', 7, 11, 23, 29)),
    (DiskCommitted(7, 11, 23, 29), 15, struct.pack('>QQQQ', 7, 11, 23, 29)),
]


@pytest.mark.parametrize('message,kind,body', VECTORS)
def test_P13_T03_external_control_exact_independent_wire_vectors(message, kind, body):
    expected = wire(kind, body)
    assert encode_message(message) == expected
    assert decode_message(expected) == message
    assert inspect_header(expected[:14]) == (kind, len(body))
    assert len(expected) < 65536
    for invalid in (expected[:-1], expected + b'x', expected + expected):
        with pytest.raises(IntegrityError):
            decode_message(invalid)


def test_P13_T03_external_grant_envelope_exact_and_one_over():
    grant = b'\x00' * 16384
    message = ExternalJob(JOB, grant, grant)
    encoded = encode_message(message)
    assert protocol.MAX_GRANT_BYTES == 16384
    assert len(encoded) == 14 + 6 + 95 + 2 * 16384 == 32883
    assert decode_message(encoded) == message
    # The per-capability bounds dominate the common 64 KiB envelope.
    for name in ('input_grant', 'output_grant'):
        for invalid in (b'', grant + b'x', bytearray(grant), memoryview(grant), 'private-key'):
            with pytest.raises(ConfigurationError) as caught:
                replace(message, **{name: invalid})
            assert 'private-key' not in str(caught.value)
    with pytest.raises(IntegrityError):
        inspect_header(wire(10, b'', declared=len(encoded) - 14 + 1))
    for length in (65536 - 14, 65537 - 14, (1 << 32) - 1):
        with pytest.raises(IntegrityError):
            inspect_header(wire(10, b'', declared=length))


def test_P13_T03_segment_reference_exact_and_one_over():
    reference = b'x' * 8192
    encoded = encode_message(SegmentResult(7, 11, reference))
    assert protocol.MAX_REFERENCE_BYTES == 8192
    assert len(encoded) == 14 + 18 + 8192
    assert decode_message(encoded).reference == reference
    for invalid in (b'', reference + b'x', bytearray(reference), memoryview(reference), 'private-ref'):
        with pytest.raises(ConfigurationError) as caught:
            SegmentResult(7, 11, invalid)
        assert 'private-ref' not in str(caught.value)
    for size in (18, 18 + 8193, 65536, (1 << 32) - 1):
        with pytest.raises(IntegrityError):
            inspect_header(wire(11, b'', declared=size))


@pytest.mark.parametrize('lengths', [(94, 1, 1), (96, 1, 1), (95, 0, 2), (95, 2, 0),
                                    (95, 1, 2), (95, 16385, 1), (95, 1, 16385)])
def test_P13_T03_external_declared_lengths_cannot_repartition_capabilities(lengths):
    payload = struct.pack('>HHH', *lengths) + JOB_WIRE + b'xy'
    with pytest.raises(IntegrityError):
        decode_message(wire(10, payload))


@pytest.mark.parametrize('kind', [1, 3, 8, 10, 11, 12, 15, 255])
def test_P13_T03_nested_foreign_schema_rejects_without_recursive_decode(monkeypatch, kind):
    nested = wire(kind, JOB_BODY)
    payload = struct.pack('>HHH', len(nested), 1, 1) + nested + b'xy'
    original = protocol.decode_message
    def forbidden_recursive_call(data):
        pytest.fail('untrusted nested kind reached recursive decoding')
    monkeypatch.setattr(protocol, 'decode_message', forbidden_recursive_call)
    with pytest.raises(IntegrityError):
        original(wire(10, payload))


@pytest.mark.parametrize('declared', [0, 1, 3, 8192, 65535])
def test_P13_T03_reference_declared_size_must_equal_available_bytes(declared):
    payload = struct.pack('>QQH', 7, 11, declared) + b'xy'
    with pytest.raises(IntegrityError):
        decode_message(wire(11, payload))


@pytest.mark.parametrize('cls,kind', [(DiskRequest, 12), (DiskCredit, 13),
                                     (DiskCommit, 14), (DiskCommitted, 15)])
def test_P13_T04_disk_credit_increment_and_unsigned_end_boundaries(cls, kind):
    limit = (1 << 20) + 128
    assert protocol.MAX_DISK_INCREMENT == limit
    message = cls(U64, U64, U64 - limit, limit)
    assert decode_message(encode_message(message)) == message
    for offset, amount in ((0, limit + 1), (U64, 1), (U64 - limit + 1, limit)):
        with pytest.raises(ConfigurationError):
            cls(0, 1, offset, amount)
        with pytest.raises(IntegrityError):
            decode_message(wire(kind, struct.pack('>QQQQ', 0, 1, offset, amount)))
    if kind in (12, 13):
        with pytest.raises(ConfigurationError):
            cls(0, 1, 0, 0)
        with pytest.raises(IntegrityError):
            decode_message(wire(kind, struct.pack('>QQQQ', 0, 1, 0, 0)))
    else:
        # An attempted write may have committed zero bytes before failing.
        assert decode_message(encode_message(cls(0, 1, U64, 0))) == cls(0, 1, U64, 0)
    for size in (31, 33, 65536, (1 << 32) - 1):
        with pytest.raises(IntegrityError):
            inspect_header(wire(kind, b'', declared=size))


@pytest.mark.parametrize('cls', [DiskRequest, DiskCredit, DiskCommit, DiskCommitted])
@pytest.mark.parametrize('field', ['task_id', 'grant_id', 'offset', 'amount'])
def test_P13_T04_disk_message_strict_integer_fields(cls, field):
    message = cls(0, 1, 0, 1)
    for invalid in (True, -1, 1 << 64, 1.5):
        with pytest.raises(ConfigurationError):
            replace(message, **{field: invalid})
    with pytest.raises(ConfigurationError):
        replace(message, grant_id=0)


def test_P13_T03_external_nested_job_is_closed_and_privately_printable():
    private = b'private-secret-marker'
    job = WorkerJob(private[:16], 7, 11, 13, 17, private.ljust(32, b'?'))
    nested = ExternalJob(job, private, private)
    reference = SegmentResult(7, 11, private)
    for message in (nested, reference):
        assert 'private' not in repr(message)
        assert 'private' not in str(message)
    for invalid in (nested, {'private': private}, b'private', None):
        with pytest.raises(ConfigurationError) as caught:
            ExternalJob(invalid, private, private)
        assert 'private-secret-marker' not in str(caught.value)
    # Encoders must reject mutable bypasses of the frozen dataclass as well.
    altered = replace(nested)
    object.__setattr__(altered, 'job', nested)
    with pytest.raises(ConfigurationError):
        encode_message(altered)
