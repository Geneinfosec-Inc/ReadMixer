"""Plan 05 transcripts checked independently with standard-library HMAC."""
from __future__ import annotations

import hmac
from unittest.mock import patch

import pytest

from readmixer_py.crypto import (
    OperationKeys, context, derive, encode_path, observations_identity,
    pairs_identity, registry_identity, sort_identity, validate_logical_identity,
)
from readmixer_py.errors import ConfigurationError, ResourceError

ROOT = bytes(range(32))
RUN_ID = bytes(range(16))


def oracle_context(domain, fields):
    # This oracle deliberately does not call any production transcript helper.
    encoded_domain = domain.encode('ascii')
    return (b'RMPY1\0' + len(encoded_domain).to_bytes(2, 'big') + encoded_domain
            + len(fields).to_bytes(2, 'big')
            + b''.join(len(field).to_bytes(4, 'big') + field for field in fields))


def oracle_derive(key, domain, fields):
    prk = hmac.digest(b'readmixer-python/schema1/hkdf-sha256', key, 'sha256')
    return hmac.digest(prk, oracle_context(domain, fields) + b'\x01', 'sha256')


def keys():
    return OperationKeys._from_test(ROOT, RUN_ID)


def test_context_binary_golden_and_unambiguous_boundaries():
    assert context('x', [b'ab', b'c']) == bytes.fromhex(
        '524d5059310000017800020000000261620000000163')
    assert context('x', [b'ab', b'c']) != context('x', [b'a', b'bc'])
    assert context('x', [b'']) != context('x', [])
    assert context('x', [b'y']) != context('xy', [])


@pytest.mark.parametrize('domain,expected', [
    ('root/shuffle', '8ac0db879dbfec4e77b776310760942325bcead50e714bf14e769a16faad498a'),
    ('root/rank', '64eee8467e5543a1a814ec93a5bc3e59dd174f1859c08dc578f471b7015b61ea'),
    ('root/tag', '03c5d45059a969744eee89ddad77f5c4df7229685b76e7dcc1df4429f9555490'),
    ('root/scratch', 'c9a5d87e07f9be7d14ed863b412b9aa3fe16b37f1cd0f0ceaa2b581e90074152'),
])
def test_root_hkdf_independent_hmac_oracle_and_frozen_vectors(domain, expected):
    assert derive(ROOT, domain, []) == oracle_derive(ROOT, domain, []) == bytes.fromhex(expected)


def test_operation_roots_path_and_rank_vectors():
    operation = keys()
    assert operation.run_id == RUN_ID
    for purpose in ('shuffle', 'rank', 'tag', 'scratch'):
        assert getattr(operation, purpose + '_root') == oracle_derive(ROOT, 'root/' + purpose, [])
    assert operation.bucket_key((1, 63)) == bytes.fromhex(
        'f35fb85d6f225339fdf6312ed6deb822662e7808c8a9924493d1249435a48446')
    assert operation.rank_key((1, 63), 258, 7) == bytes.fromhex(
        '9b04a43bdefc715b91fe2534c32f12a8254488e80c265bda9aa156380cf8beae')
    rank_root = oracle_derive(ROOT, 'root/rank', [])
    assert operation.rank_key((1, 63), 258, 7) == oracle_derive(
        rank_root, 'shuffle/rank', [b'\x02\x01\x3f', b'\0\0\0\0\0\0\x01\x02', b'\x07'])
    assert len({operation.rank_key((1,), 1, attempt) for attempt in range(8)}) == 8
    assert operation.rank_key((1,), 1, 0) != operation.rank_key((1,), 2, 0)
    assert operation.bucket_key(()) != operation.bucket_key((0,))


def test_path_boundaries():
    assert encode_path(()) == b'\x00'
    assert encode_path((63,)) == b'\x01\x3f'
    assert encode_path((63,) * 32) == b'\x20' + b'\x3f' * 32


@pytest.mark.parametrize('path', [(0,) * 33, (-1,), (64,), (True,), (1.0,), [1], b'\x01'])
def test_invalid_paths(path):
    with pytest.raises(ConfigurationError):
        encode_path(path)


@pytest.mark.parametrize('domain,fields', [
    ('root/tag', [b'']), ('not-a-schema-domain', []), ('shuffle/bucket', []),
    ('shuffle/bucket', [b'']), ('shuffle/bucket', [b'\x01']),
    ('shuffle/bucket', [b'\x00\x00']), ('shuffle/bucket', [b'\x01\x40']),
    ('shuffle/rank', [b'\0', b'\0'*7, b'\0']),
    ('shuffle/rank', [b'\0', b'\0'*8, b'\x08']),
    ('shuffle/rank', [b'\0', b'\0'*8, b'']),
    ('scratch/segment', [RUN_ID, RUN_ID, ROOT, b'\0', b'']),
])
def test_derive_rejects_unknown_domains_and_malformed_fields(domain, fields):
    with pytest.raises(ConfigurationError):
        derive(ROOT, domain, fields)


@pytest.mark.parametrize('domain,fields', [('\u00e9', []), ('x' * 65536, []), ('x', [b''] * 65536),
                                           ('x', ['text']), ('x', b'raw'), (b'x', [])])
def test_context_rejects_invalid_widths_and_types(domain, fields):
    with pytest.raises(ConfigurationError):
        context(domain, fields)


@pytest.mark.parametrize('key', [b'', ROOT[:-1], ROOT + b'x', bytearray(ROOT)])
def test_derive_checks_key_width(key):
    with pytest.raises(ConfigurationError):
        derive(key, 'root/tag', [])


def test_fresh_independent_os_draws_and_redacted_repr():
    with patch('readmixer_py.crypto.os.urandom', side_effect=[ROOT, RUN_ID]) as entropy:
        operation = OperationKeys.new()
    assert [call.args for call in entropy.call_args_list] == [(32,), (16,)]
    assert operation.run_id == RUN_ID
    for name in ('shuffle_root', 'rank_root', 'tag_root', 'scratch_root'):
        assert getattr(operation, name).hex() not in repr(operation)
        assert repr(getattr(operation, name)) not in repr(operation)
    with pytest.raises(ConfigurationError):
        OperationKeys()
    assert OperationKeys.new().run_id != OperationKeys.new().run_id


@pytest.mark.parametrize('draws', [[OSError('test entropy failure')], [ROOT, OSError('second draw')],
                                  [b'', RUN_ID], [ROOT, b'']])
def test_entropy_failure_is_typed_before_key_creation(draws):
    with patch('readmixer_py.crypto.os.urandom', side_effect=draws):
        with pytest.raises(ResourceError):
            OperationKeys.new()


def test_tag_aes_identity_block_frozen_openssl_vector_and_distinct_identities():
    # Independently produced using openssl enc -aes-256-ecb -nopad on the exact
    # b'RMP1' || u32BE || u64BE block and the independently checked HKDF key.
    assert keys().tag(0x12345678, 0x0102030405060708) == 'd7a44ffb2b88a7ac74695b65421e223b'
    identities = [(0, 0), (0, 1), (1, 0), (0xFFFFFFFF, 0xFFFFFFFFFFFFFFFF)]
    tags = {keys().tag(sample, ordinal) for sample, ordinal in identities}
    assert len(tags) == len(identities)
    assert all(len(tag) == 32 and tag == tag.lower() for tag in tags)


@pytest.mark.parametrize('sample,ordinal', [(-1, 0), (2**32, 0), (0, -1), (0, 2**64), (True, 0)])
def test_tag_rejects_identity_overflow(sample, ordinal):
    with pytest.raises(ConfigurationError):
        keys().tag(sample, ordinal)


@pytest.mark.parametrize('count,attempt', [(-1, 0), (2**64, 0), (1, -1), (1, 8), (1, 256), (True, 0)])
def test_rank_rejects_counter_and_attempt_overflow(count, attempt):
    with pytest.raises(ConfigurationError):
        keys().rank_key((), count, attempt)


@pytest.mark.parametrize('purpose,logical', [
    (1, pairs_identity((63, 0), 0xFFFFFFFF, 0xFFFFFFFFFFFFFFFF)),
    (2, observations_identity('locators', 4)),
    (3, sort_identity('identity', 3, 19)),
    (4, registry_identity(RUN_ID, 6, 4)),
    (5, observations_identity('verify', 8)),
])
def test_segment_context_oracle_and_domain_separation(purpose, logical):
    validate_logical_identity(purpose, logical)
    segment = b'\x80' + b'\0' * 15
    salt = b'\xff' * 32
    fields = [RUN_ID, segment, salt, bytes([purpose]), logical]
    expected = oracle_derive(oracle_derive(ROOT, 'root/scratch', []), 'scratch/segment', fields)
    assert keys().segment_key(segment, salt, purpose, logical) == expected
    assert keys().segment_key(segment, b'\x00' * 32, purpose, logical) != expected
    assert keys().segment_key(bytes(16), salt, purpose, logical) != expected
    assert OperationKeys._from_test(ROOT, bytes(16)).segment_key(segment, salt, purpose, logical) != expected


@pytest.mark.parametrize('purpose,logical', [
    (0, pairs_identity((), 0, 0)), (6, pairs_identity((), 0, 0)),
    (1, observations_identity('pairs', 0)),
    (1, context('segment/pairs', [b'\0', b'\0'*3, b'\0'*8])),
    (1, context('segment/pairs', [b'\x01\x40', b'\0'*4, b'\0'*8])),
    (3, context('segment/sort', [b'\xff', b'\0'*4, b'\0'*8])),
    (4, context('segment/registry', [b'\0'*15, b'\0'*8, b'\0'*8])),
    (5, context('segment/observations', [b'stage', b'\0'*9])),
    (5, context('segment/observations', [b'x'*4096, b'\0'*8])),
    (1, pairs_identity((), 0, 0) + b'trailing'),
    (1, pairs_identity((), 0, 0)[:-1]),
    (1, b'RMPY1\0\xff\xff'), (1, b'RMPY1\0\0\0\xff\xff'),
])
def test_malformed_logical_context_rejected(purpose, logical):
    with pytest.raises(ConfigurationError):
        validate_logical_identity(purpose, logical)
    with pytest.raises(ConfigurationError):
        keys().segment_key(RUN_ID, ROOT, purpose, logical)


def test_segment_key_individual_field_widths():
    logical = pairs_identity((), 0, 0)
    for segment, salt in [(b'', ROOT), (RUN_ID, b''), (RUN_ID + b'x', ROOT)]:
        with pytest.raises(ConfigurationError):
            keys().segment_key(segment, salt, 1, logical)
