"""Independent struct fixtures for the private PYPAIR1 frame wire format."""
from __future__ import annotations

import io
import struct
from types import SimpleNamespace

import pytest

from readmixer_py.errors import ConfigurationError, InputError, IntegrityError, ResourceError
from readmixer_py.fastq import PairedReader, RecordFramer
from readmixer_py.paircodec import decode_pair_view, encode_pair_into, maximum_frame_bytes

LINES = (b'@id/1 opaque\xff', b'A\x00\x81', b'+meta\xfe', b'!~\r',
         b'@id/2 metadata', b'G\x00\x81', b'+other\xff', b'#!\r')


def fixture(lines=LINES, sample=7, ordinal=0x0102030405060708):
    # Independent golden assembly, rather than a codec roundtrip as oracle.
    return (struct.pack('<IIQ8I', 48 + sum(map(len, lines)), sample, ordinal,
                        *map(len, lines)) + b''.join(lines))


def input_pair(lines=LINES, sample=7, ordinal=0x0102030405060708):
    return SimpleNamespace(sample_index=sample, original_pair_index=ordinal,
                           r1=SimpleNamespace(lines=tuple(memoryview(line) for line in lines[:4])),
                           r2=SimpleNamespace(lines=tuple(memoryview(line) for line in lines[4:])))


def test_exact_binary_golden_and_borrowed_opaque_lines():
    frame = bytearray(fixture())
    pair = decode_pair_view(frame)
    assert pair.sample_index == 7
    assert pair.original_pair_index == 0x0102030405060708
    assert tuple(bytes(line) for line in (*pair.r1.lines, *pair.r2.lines)) == LINES
    assert pair.frame.readonly
    assert pair.frame.obj is frame
    assert all(line.obj is frame and line.readonly for line in pair.r1.lines)
    assert pair.frame_bytes == len(frame)
    assert pair.normalized_bytes == len(frame) - 40
    assert pair.r1.normalized_bytes == 4 + sum(map(len, LINES[:4]))
    assert bytes(pair.r2.header) == LINES[4]
    assert bytes(pair.r1.sequence) == LINES[1]
    assert bytes(pair.r1.plus) == LINES[2]
    assert bytes(pair.r1.quality) == LINES[3]
    destination = bytearray(len(frame) + 10)
    destination[-10:] = b'untouched!'
    assert encode_pair_into(input_pair(), destination) == len(frame)
    assert destination[:len(frame)] == frame
    assert destination[-10:] == b'untouched!'


def test_parser_to_frame_preserves_cr_and_non_utf8_metadata():
    framers = [RecordFramer(io.BytesIO(b'\n'.join(LINES[start:start+4])+b'\n'),
                           line_mode='canonical_lf') for start in (0, 4)]
    pair = PairedReader(*framers, sample_index=7).next_pair()
    target = bytearray(pair.frame_bytes)
    assert encode_pair_into(pair, target) == len(target)
    assert target == fixture(ordinal=0)


def test_empty_sequence_and_quality_and_identity_maxima():
    lines = (b'@x/1', b'', b'+', b'', b'@x/2', b'', b'+', b'')
    frame = fixture(lines, 0xFFFFFFFF, 0xFFFFFFFFFFFFFFFF)
    view = decode_pair_view(frame, max_record_bytes=9)
    assert view.r1.sequence == view.r1.quality == b''
    assert len(frame) == 40 + 2 * 9
    target = bytearray(len(frame))
    encode_pair_into(input_pair(lines, 0xFFFFFFFF, 0xFFFFFFFFFFFFFFFF), target,
                     max_record_bytes=9)
    assert bytes(target) == frame


@pytest.mark.parametrize('size', [0, 1, 4, 15, 16, 47])
def test_truncated_fixed_header(size):
    with pytest.raises(IntegrityError):
        decode_pair_view(fixture()[:size])


@pytest.mark.parametrize('kind', ['declared-short', 'declared-long', 'body-truncated',
                                  'body-trailing', 'length-too-large', 'lengths-too-small'])
def test_malformed_lengths_rejected_without_allocating_declared_sizes(kind):
    frame = bytearray(fixture())
    if kind == 'declared-short':
        struct.pack_into('<I', frame, 0, len(frame)-1)
    elif kind == 'declared-long':
        struct.pack_into('<I', frame, 0, 0xFFFFFFFF)
    elif kind == 'body-truncated':
        frame.pop()
        struct.pack_into('<I', frame, 0, len(frame))
    elif kind == 'body-trailing':
        frame.append(0)
        struct.pack_into('<I', frame, 0, len(frame))
    elif kind == 'length-too-large':
        struct.pack_into('<I', frame, 16, 0xFFFFFFFF)
    else:
        struct.pack_into('<I', frame, 16, 0)
    with pytest.raises(IntegrityError):
        decode_pair_view(frame)


@pytest.mark.parametrize('index,replacement', [
    (0, b'id/1'), (0, b'@'), (0, b'@id/2'), (0, b'@id/1 2:N:0:1'),
    (0, b'@id/1 trailing/2'), (0, b'@different/1'), (0, b'@id/1\nmetadata'),
    (1, b'AAAA'), (1, b'\n\x00\x81'), (2, b'missing-marker'),
    (3, b'!!'), (4, b'@id/1'), (4, b'@different/2'), (6, b''),
])
def test_fastq_and_pair_grammar_revalidated_in_private_frame(index, replacement):
    lines = list(LINES)
    lines[index] = replacement
    with pytest.raises(IntegrityError):
        decode_pair_view(fixture(lines))
    target = bytearray(b'\xa5' * 200)
    with pytest.raises(InputError):
        encode_pair_into(input_pair(lines), target)
    assert target == b'\xa5' * 200


def test_exact_per_mate_limit_and_one_over_independently_of_total_limit():
    # Two mates may differ in length: a small mate cannot subsidize the other.
    lines = (b'@x/1 more', b'', b'+', b'', b'@x/2', b'', b'+', b'')
    limit = 4 + sum(map(len, lines[:4]))
    assert decode_pair_view(fixture(lines), max_record_bytes=limit).r1.normalized_bytes == limit
    with pytest.raises(IntegrityError):
        decode_pair_view(fixture(lines), max_record_bytes=limit-1)
    with pytest.raises(InputError):
        encode_pair_into(input_pair(lines), bytearray(200), max_record_bytes=limit-1)


@pytest.mark.parametrize('sample,ordinal', [(-1, 0), (2**32, 0), (0, -1), (0, 2**64), (True, 0)])
def test_encoder_identity_bounds_and_atomic_failure(sample, ordinal):
    target = bytearray(b'\xa5' * 200)
    with pytest.raises(InputError):
        encode_pair_into(input_pair(sample=sample, ordinal=ordinal), target)
    assert target == b'\xa5' * 200


def test_destination_must_be_writable_complete_and_contiguous():
    with pytest.raises(ConfigurationError):
        encode_pair_into(input_pair(), bytes(200))
    with pytest.raises(ConfigurationError):
        encode_pair_into(input_pair(), memoryview(bytearray(400))[::2])
    target = bytearray(b'\xa5' * (len(fixture())-1))
    with pytest.raises(ResourceError):
        encode_pair_into(input_pair(), target)
    assert target == b'\xa5' * len(target)


@pytest.mark.parametrize('limit', [0, -1, True, 1.5, (0xFFFFFFFF - 40)//2 + 1])
def test_invalid_record_configuration(limit):
    with pytest.raises(ConfigurationError):
        decode_pair_view(fixture(), max_record_bytes=limit)
    with pytest.raises(ConfigurationError):
        encode_pair_into(input_pair(), bytearray(200), max_record_bytes=limit)


def test_frame_bound_is_40_plus_twice_m_and_single_frame_admission():
    assert maximum_frame_bytes(1024) == 2088
    assert maximum_frame_bytes((0xFFFFFFFF - 40)//2) == 0xFFFFFFFE
    assert maximum_frame_bytes(1024, single_frame_limit=2088) == 2088
    with pytest.raises(ResourceError):
        maximum_frame_bytes(1024, single_frame_limit=2087)
    with pytest.raises(ConfigurationError):
        maximum_frame_bytes(1024, single_frame_limit=True)
