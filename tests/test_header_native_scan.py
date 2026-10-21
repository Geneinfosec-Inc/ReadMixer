"""Header token scanner equivalence, borrowed storage, and bounded retention."""
from array import array
from itertools import product
import random
import tracemalloc

import pytest

from readmixer_py.errors import InputError
from readmixer_py.fastq import header_identity


def legacy_header_identity(header):
    """Independent pre-optimization bytewise grammar, with no runtime helpers."""
    whitespace = frozenset(b' \t\n\r\v\f')

    def terminal_role(token):
        if len(token) >= 2 and token[-2] == 47 and token[-1] in (49, 50):
            return token[-1] - 48
        return None

    header = memoryview(header).cast('B')
    if not header or header[0] != 64:
        raise InputError('mate header is missing its header marker')
    cursor = 1
    while cursor < len(header) and header[cursor] in whitespace:
        cursor += 1
    start = cursor
    while cursor < len(header) and header[cursor] not in whitespace:
        cursor += 1
    token = header[start:cursor]
    role = terminal_role(token)
    base = token[:-2] if role is not None else token
    if not base:
        raise InputError('mate header has an empty base identifier')
    first_metadata = last_metadata = None
    while cursor < len(header):
        while cursor < len(header) and header[cursor] in whitespace:
            cursor += 1
        start = cursor
        while cursor < len(header) and header[cursor] not in whitespace:
            cursor += 1
        if start != cursor:
            last_metadata = header[start:cursor]
            if first_metadata is None:
                first_metadata = last_metadata
    candidates = [role]
    if first_metadata is not None and len(first_metadata) >= 2:
        if first_metadata[0] in (49, 50) and first_metadata[1] == 58:
            candidates.append(first_metadata[0] - 48)
    if last_metadata is not None:
        candidates.append(terminal_role(last_metadata))
        if len(last_metadata) == 1 and last_metadata[0] in (49, 50):
            candidates.append(last_metadata[0] - 48)
    for candidate in candidates:
        if candidate is not None:
            if role is not None and candidate != role:
                raise InputError('mate header contains conflicting role markers')
            role = candidate
    return base, role


def outcome(function, header):
    try:
        base, role = function(header)
        return 'value', bytes(base), role
    except Exception as error:
        return 'error', type(error), str(error)


def equivalent(header):
    assert outcome(header_identity, header) == outcome(legacy_header_identity, header)


def test_every_byte_preserves_identifier_and_metadata_boundaries():
    for value in range(256):
        byte = bytes((value,))
        for header in (
            b'@' + byte, b'@' + byte + b'id/1', b'@id' + byte + b'/1',
            b'@id/1' + byte + b'meta', b'@id' + byte + b'1:N:0',
            b'@id/1 ' + byte + b'2:conflict', b'@id/1 first ' + byte + b'/2',
            b'@id/1 first 2' + byte, b'@id first' + byte + b'2',
        ):
            equivalent(header)


def test_exhaustive_short_byte_classes_match_legacy_grammar():
    # Every whitespace kind, both roles, separators, marker, ordinary/high/NUL
    # bytes: exhaustively cover all strings up to four representative bytes.
    alphabet = b' \t\n\r\v\f12/:@A\xff\x00'
    for length in range(5):
        for suffix in product(alphabet, repeat=length):
            equivalent(b'@' + bytes(suffix))


@pytest.mark.parametrize('separator', (b' ', b'\t', b'\n', b'\r', b'\v', b'\f', b' \t\v\f\r\n'))
def test_first_last_and_conflicting_role_combinations(separator):
    tokens = (b'', b'1', b'2', b'1:', b'2:opaque', b'meta/1', b'meta/2',
              b'1:prefix/2', b'2:prefix/1', b'opaque', b'\xff\x00')
    for identifier, first, last in product((b'id', b'id/1', b'id/2', b'/1', b'/2', b''), tokens, tokens):
        equivalent(b'@' + identifier + separator + first + separator + b'ignored/2' + separator + last)


def test_seeded_opaque_headers_and_slices_match_legacy_grammar():
    random_source = random.Random(0x484541444552)
    for _ in range(5000):
        payload = random_source.randbytes(random_source.randrange(257))
        header = random_source.choice((b'@', b'@id/1 ', b'@id/2 ', b'')) + payload
        equivalent(header)
        padded = bytearray(b'guard' + header + b'trailer')
        equivalent(memoryview(padded)[5:5 + len(header)])


@pytest.mark.parametrize('kind', ('bytes', 'bytearray', 'array', 'readonly_view', 'signed_view',
                                  'char_view', 'word_view', 'multidimensional_view'))
def test_existing_contiguous_buffer_types_and_formats_remain_accepted(kind):
    header = b'@identifier/1 first last/1   '
    header += b' ' * (-len(header) % 4)
    if kind == 'bytes':
        value = header
    elif kind == 'bytearray':
        value = bytearray(header)
    elif kind == 'array':
        value = array('B', header)
    else:
        value = memoryview(bytearray(header))
        if kind == 'readonly_view':
            value = value.toreadonly()
        elif kind == 'signed_view':
            value = value.cast('b')
        elif kind == 'char_view':
            value = value.cast('c')
        elif kind == 'word_view':
            value = value.cast('I')
        elif kind == 'multidimensional_view':
            value = value.cast('B', shape=(4, len(header) // 4))
    equivalent(value)
    assert outcome(header_identity, value) == ('value', b'identifier', 1)


def test_noncontiguous_released_empty_and_nonbuffer_behavior_is_unchanged():
    source = bytearray(b'@identifier/1 first last/1')
    for view in (memoryview(source)[::2], memoryview(source)[::-1], memoryview(source)[1::3],
                 memoryview(source)[:1:2], memoryview(source)[:0:2]):
        equivalent(view)
    released = memoryview(source)
    released.release()
    equivalent(released)
    for value in (None, '@id', 42, (), b'', b'@', b'@ \t\n\r\v\f', b'@/1', b'@/2'):
        equivalent(value)


def test_returned_base_borrows_original_mutable_storage():
    original = bytearray(b'padding@identifier/1 meta 1suffix')
    header = memoryview(original)[7:-6]
    base, role = header_identity(header)
    assert role == 1 and bytes(base) == b'identifier' and base.obj is original
    original[8:18] = b'0123456789'
    assert bytes(base) == b'0123456789'
    with pytest.raises(BufferError):
        original.extend(b'cannot-resize-borrowed-buffer')


@pytest.mark.parametrize('many_tokens', (False, True))
def test_long_headers_do_not_copy_payload_or_retain_a_token_collection(many_tokens):
    # Construct inside the test: retaining a large parametrized module constant
    # would inflate unrelated whole-suite admission baselines.
    payload = b'opaque ' if many_tokens else b'opaque'
    header = b'@id/1 ' + payload * 131072 + b' 1'
    assert len(header) > 750_000
    tracemalloc.start()
    try:
        tracemalloc.reset_peak()
        before, _ = tracemalloc.get_traced_memory()
        base, role = header_identity(header)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert bytes(base) == b'id' and role == 1
    # Generous fixed overhead allowance, far below even one full header copy.
    assert peak - before < 64 << 10
