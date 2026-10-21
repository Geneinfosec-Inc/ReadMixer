"""Native LF scanning must preserve the bytewise codec contract and allocation bound."""
from __future__ import annotations

import struct
import tracemalloc
from types import SimpleNamespace

import pytest

from readmixer_py import paircodec
from readmixer_py.errors import ConfigurationError, InputError, IntegrityError


_LINES = (b'@read/1 opaque~', b'ABC', b'+tag~', b'!!!',
          b'@read/2 opaque~', b'TGC', b'+tag~', b'###')
_LF_ERROR = 'pair frame line payload contains an LF terminator'
_KINDS = ('bytes', 'bytearray', 'readonly', 'mutable', 'slice', 'signed',
          'multidimensional', 'mixed')


def _view(data, kind, index=0):
    if kind == 'mixed':
        return _view(data, _KINDS[index % (len(_KINDS) - 1)], index)
    if kind == 'bytes':
        return data
    if kind == 'bytearray':
        return bytearray(data)
    if kind == 'readonly':
        return memoryview(data)
    if kind == 'mutable':
        return memoryview(bytearray(data))
    if kind == 'slice':
        # LF in the backing object's excluded prefix/suffix must not be scanned.
        return memoryview(bytearray(b'\n' + data + b'\n'))[1:-1]
    if kind == 'signed':
        return memoryview(bytearray(data)).cast('b')
    if kind == 'multidimensional':
        return memoryview(data).cast('B', shape=(1, len(data))) if data else memoryview(data)
    raise AssertionError(kind)


def _legacy_has_lf(lines):
    # The independent old predicate, after the same public buffer normalization.
    return any(10 in memoryview(line).cast('B') for line in lines)


def _literal_frame(lines):
    payloads = tuple(bytes(memoryview(line).cast('B')) for line in lines)
    return (struct.pack('<IIQ8I', 48 + sum(map(len, payloads)), 7, 19,
                        *map(len, payloads)) + b''.join(payloads))


def _pair(lines):
    return SimpleNamespace(sample_index=7, original_pair_index=19,
                           r1=SimpleNamespace(lines=tuple(lines[:4])),
                           r2=SimpleNamespace(lines=tuple(lines[4:])))


def _assert_public_result(lines, *, max_record_bytes=1024 * 1024):
    frame = _literal_frame(lines)
    target = bytearray(b'\xa5' * (len(frame) + 4))
    if _legacy_has_lf(lines):
        with pytest.raises(InputError) as encoded:
            paircodec.encode_pair_into(_pair(lines), target, max_record_bytes=max_record_bytes)
        assert str(encoded.value) == _LF_ERROR
        assert target == b'\xa5' * len(target)
        with pytest.raises(IntegrityError) as decoded:
            paircodec.decode_pair_view(frame, max_record_bytes=max_record_bytes)
        assert str(decoded.value) == _LF_ERROR
    else:
        assert paircodec.encode_pair_into(_pair(lines), target, max_record_bytes=max_record_bytes) == len(frame)
        assert target == frame + b'\xa5' * 4
        # Also exercise a sliced whole-frame view with LF outside its bounds.
        arena = memoryview(bytearray(b'\n' + frame + b'\n'))[1:-1]
        result = paircodec.decode_pair_view(arena, max_record_bytes=max_record_bytes)
        assert result.sample_index == 7 and result.original_pair_index == 19
        assert tuple(bytes(line) for line in (*result.r1.lines, *result.r2.lines)) == tuple(
            bytes(memoryview(line).cast('B')) for line in lines)


@pytest.mark.parametrize('kind', _KINDS)
@pytest.mark.parametrize('line_index', range(8))
def test_every_byte_matches_old_predicate_in_every_line(kind, line_index):
    for value in range(256):
        lines = list(_LINES)
        field = bytearray(lines[line_index])
        # Headers retain the identity/role prefix and a safe final metadata guard.
        field[-2 if line_index in (0, 2, 4, 6) else 1] = value
        lines[line_index] = bytes(field)
        wrapped = tuple(_view(line, kind, index) for index, line in enumerate(lines))
        assert _legacy_has_lf(wrapped) is (value == 10), (kind, line_index, value)
        _assert_public_result(wrapped)


# Middle LF in the three-byte sequence/quality fields is already checked
# by the every-byte tests using the same mixed buffer representations.
@pytest.mark.parametrize('position,line_index', [
    (position, line_index)
    for position in ('beginning', 'middle', 'end')
    for line_index in range(8)
    if position != 'middle' or line_index in (0, 2, 4, 6)
])
def test_lf_rejection_at_each_boundary_precedes_other_grammar_errors(line_index, position):
    lines = list(_LINES)
    field = bytearray(lines[line_index])
    offset = {'beginning': 0, 'middle': len(field) // 2, 'end': len(field) - 1}[position]
    field[offset] = 10
    lines[line_index] = field
    # Beginning LF may also destroy a header/plus marker; LF still wins.
    _assert_public_result(tuple(_view(bytes(line), 'mixed', index) for index, line in enumerate(lines)))


@pytest.mark.parametrize('kind', _KINDS)
def test_empty_payloads_and_exact_record_boundary(kind):
    lines = (b'@x/1', b'', b'+', b'', b'@x/2', b'', b'+', b'')
    _assert_public_result(tuple(_view(line, kind, index) for index, line in enumerate(lines)),
                          max_record_bytes=9)


def test_cr_nul_and_non_utf8_payloads_remain_opaque():
    lines = (b'@x/1 metadata\xff~', b'\x00\xff', b'+\x80\r', b'!\r',
             b'@x/2 metadata\x80~', b'\xff\x00', b'+\xff\r', b'\x00\r')
    _assert_public_result(tuple(_view(line, 'mixed', index) for index, line in enumerate(lines)))


def test_mutable_view_is_checked_again_after_each_mutation():
    quality = bytearray(b'!!!')
    lines = list(_LINES)
    lines[3] = memoryview(quality)
    _assert_public_result(lines)
    quality[1] = 10
    _assert_public_result(lines)
    quality[1] = 255
    _assert_public_result(lines)


class _UnexpectedMatcher:
    def search(self, _line):
        raise AssertionError('LF matcher must not run before buffer/structure admission')


@pytest.mark.parametrize('line_index', (0, 7))
@pytest.mark.parametrize('reverse', (False, True))
def test_noncontiguous_source_rejection_precedes_matcher(monkeypatch, line_index, reverse):
    lines = list(_LINES)
    lines[0] = b'@read/1\nmetadata'
    raw = bytearray(lines[line_index])
    if reverse:
        lines[line_index] = memoryview(raw)[::-1]
    else:
        expanded = bytearray(len(raw) * 2)
        expanded[::2] = raw
        lines[line_index] = memoryview(expanded)[::2]
    monkeypatch.setattr(paircodec, '_LINE_FEED', _UnexpectedMatcher())
    target = bytearray(b'\xa5' * 1000)
    with pytest.raises(ConfigurationError) as error:
        paircodec.encode_pair_into(_pair(lines), target)
    assert str(error.value) == 'pair codec requires a contiguous byte buffer'
    assert target == b'\xa5' * 1000


def test_noncontiguous_frame_rejection_precedes_matcher(monkeypatch):
    frame = _literal_frame(_LINES)
    expanded = bytearray(len(frame) * 2)
    expanded[::2] = frame
    monkeypatch.setattr(paircodec, '_LINE_FEED', _UnexpectedMatcher())
    with pytest.raises(ConfigurationError) as error:
        paircodec.decode_pair_view(memoryview(expanded)[::2])
    assert str(error.value) == 'pair codec requires a contiguous byte buffer'


@pytest.mark.parametrize('count', (0, 7, 9))
@pytest.mark.parametrize('error_class', (InputError, IntegrityError))
def test_line_count_rejection_precedes_matcher(monkeypatch, count, error_class):
    monkeypatch.setattr(paircodec, '_LINE_FEED', _UnexpectedMatcher())
    with pytest.raises(error_class) as error:
        paircodec._validate_lines((memoryview(b'\n'),) * count, 1024, error_class)
    assert str(error.value) == 'pair frame must contain exactly eight lines'


def test_destination_validation_remains_after_payload_validation():
    lines = list(_LINES)
    lines[7] = b'!\n!'
    with pytest.raises(InputError) as error:
        paircodec.encode_pair_into(_pair(lines), memoryview(bytearray(1000))[::2])
    assert str(error.value) == _LF_ERROR


@pytest.mark.parametrize('error_class', (InputError, IntegrityError))
def test_empty_lines_keep_marker_error(error_class):
    with pytest.raises(error_class) as error:
        paircodec._validate_lines((memoryview(b''),) * 8, 1024, error_class)
    assert str(error.value) == 'pair frame contains an invalid FASTQ marker'


def test_global_frame_bound_still_precedes_lf_validation():
    lines = list(_LINES)
    lines[3] = b'!\n!'
    frame = _literal_frame(lines)
    for call, error_class in (
        (lambda: paircodec.encode_pair_into(_pair(lines), bytearray(1000), max_record_bytes=9), InputError),
        (lambda: paircodec.decode_pair_view(frame, max_record_bytes=9), IntegrityError),
    ):
        with pytest.raises(error_class) as error:
            call()
        assert str(error.value) == 'pair frame exceeds the configured maximum frame size'


@pytest.mark.parametrize('method', ('encode', 'decode'))
@pytest.mark.parametrize('invalid', (False, True))
@pytest.mark.parametrize('payload_bytes', (1024, 2 * 1024 * 1024))
def test_native_scan_has_bounded_python_allocations(method, invalid, payload_bytes):
    payload = b'A' * payload_bytes
    quality = bytearray(b'!' * payload_bytes)
    if invalid:
        # Force traversal through every earlier field and the final payload.
        quality[-1] = 10
    lines = (b'@x/1', payload, b'+', payload, b'@x/2', payload, b'+', memoryview(quality))
    frame = _literal_frame(lines)
    pair = _pair(lines)
    target = bytearray(len(frame))
    limit = 9 + 2 * payload_bytes
    # Prepare all payloads, destinations, imports, and regex state before tracing.
    paircodec.decode_pair_view(_literal_frame(_LINES))
    action = (lambda: paircodec.encode_pair_into(pair, target, max_record_bytes=limit)) if method == 'encode' else (
        lambda: paircodec.decode_pair_view(memoryview(frame), max_record_bytes=limit))
    observed_error = None
    tracemalloc.start()
    try:
        before = tracemalloc.get_traced_memory()[0]
        tracemalloc.reset_peak()
        try:
            action()
        except (InputError, IntegrityError) as error:
            observed_error = (type(error), str(error))
        peak_extra = tracemalloc.get_traced_memory()[1] - before
    finally:
        tracemalloc.stop()
    # A copy of even one large line would exceed this fixed bound by 32 times.
    assert peak_extra < 64 * 1024
    if invalid:
        assert observed_error == (InputError if method == 'encode' else IntegrityError, _LF_ERROR)
        if method == 'encode':
            assert target == bytearray(len(frame))
    else:
        assert observed_error is None
        if method == 'encode':
            assert target == frame
