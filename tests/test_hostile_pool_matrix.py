"""P15-T03: public structural proofs after independently repairing outer hashes.

The tiny pool deliberately repeats complete original records. Occurrence identity,
not biology or original identifier text, must establish the restored domain.
Mutators use literal index structs and a stored-DEFLATE encoder, never production
key/index serialization. Every failure records the actual structural checkpoint.
"""

import gzip
import hashlib
import json
import os
import shutil
import struct
from contextlib import contextmanager
from unittest.mock import patch

import pytest

from readmixer_py.config import MixConfig, RecoverConfig, RecoveryMode, StorageMode, VerifyConfig
from readmixer_py.crypto import OperationKeys
from readmixer_py.errors import IntegrityError, UnsupportedFormatError
from readmixer_py.mixing import _mix
from readmixer_py.recovery import recover
from readmixer_py.verification import verify
from test_secured_field_mutations import stored_bgzf

ROLES = ('r1', 'r2', 'key', 'r1_gzi', 'r2_gzi', 'index')
OPTIONS = dict(workers=1, max_ram=512 << 20, storage=StorageMode.EXTERNAL)


@pytest.fixture(scope='module')
def hostile_base(tmp_path_factory):
    root = tmp_path_factory.mktemp('hostile-matrix')
    inputs = root / 'originals'
    inputs.mkdir()
    for sample in ('A', 'B'):
        for role in (1, 2):
            # Repeated complete records at different ordinals and across samples.
            (inputs / f'{sample}_R{role}.fq').write_bytes(
                f'@repeated/{role} opaque\nACGT\n+opaque\n!!!!\n'.encode() * 3)
    report = _mix(MixConfig(input_dir=inputs, output=root / 'pool', max_record_bytes=1024,
                           compression_level=0, **OPTIONS),
                  _keys=OperationKeys._from_test(b'M' * 32, b'X' * 16))
    manifest = root / 'pool' / report.manifest
    assert verify(VerifyConfig(manifest=manifest, **OPTIONS)).pairs == 6
    document = json.loads(manifest.read_bytes())
    assert len(document['bundles']) == 1
    for role in ('r1', 'r2', 'key'):
        raw = (manifest.parent / document['bundles'][0][role]['name']).read_bytes()
        assert raw == stored_bgzf(gzip.decompress(raw))
    return manifest


class HostilePool:
    def __init__(self, original, root):
        self.root = root / 'source'
        shutil.copytree(original.parent, self.root)
        self.manifest = self.root / original.name
        self.document = json.loads(self.manifest.read_bytes())
        self.bundle = self.document['bundles'][0]
        self.index = bytearray(self.read('index'))
        self.key = gzip.decompress(self.read('key')).splitlines(keepends=True)

    def read(self, role):
        return (self.root / self.bundle[role]['name']).read_bytes()

    def write(self, role, raw):
        descriptor = self.bundle[role]
        (self.root / descriptor['name']).write_bytes(raw)
        descriptor.update(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())

    def lane(self, role, decoded):
        self.write(role, stored_bgzf(decoded))
        self.bundle[role]['uncompressed_bytes'] = len(decoded)
        self.document['totals'][role + '_uncompressed_bytes'] = len(decoded)

    def rows(self):
        # All rows occupy the first data member; regenerate later virtual starts
        # independently whenever a mutated row changes its encoded width.
        cursor = len(self.key[0])
        for position, row in enumerate(self.key[1:]):
            struct.pack_into('<Q', self.index, 64 + 40 * position + 32, cursor)
            cursor += len(row)
        self.lane('key', b''.join(self.key))

    def save(self):
        self.write('index', self.index)
        self.manifest.write_bytes(json.dumps(self.document, separators=(',', ':')).encode() + b'\n')
        for role in ROLES:
            raw = self.read(role)
            assert len(raw) == self.bundle[role]['bytes']
            assert hashlib.sha256(raw).hexdigest() == self.bundle[role]['sha256']


@contextmanager
def observe(monkeypatch, module, name):
    original = getattr(module, name)
    calls = []
    def traced(*args, **kwargs):
        calls.append(True)
        return original(*args, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(module, name, traced)
        yield calls


def operate(pool, destination, operation, *, samples=('A', 'B')):
    if operation == 'verify':
        return verify(VerifyConfig(manifest=pool.manifest, **OPTIONS))
    return recover(RecoverConfig(manifest=pool.manifest, output=destination,
        sample=samples, io=RecoveryMode(operation), **OPTIONS))


def assert_rejected(pool, destination, operation, error, message, calls, *, samples=('A', 'B')):
    source_before = {path.name: path.read_bytes() for path in pool.root.iterdir()}
    destination.mkdir(mode=0o700)
    foreign = destination / 'foreign.txt'
    foreign.write_bytes(b'foreign sentinel')
    descriptors = len(os.listdir('/proc/self/fd')) if os.path.isdir('/proc/self/fd') else None
    import readmixer_py.verification as verification_module
    import readmixer_py.recovery as recovery_module
    module = verification_module if operation == 'verify' else recovery_module
    resource_plan = module.operation_resources
    budgets = []
    def admitted(*args, **kwargs):
        plan, budget = resource_plan(*args, **kwargs)
        budgets.append(budget)
        return plan, budget
    with patch.object(module, 'operation_resources', admitted):
        with pytest.raises(error, match=message) as retained:
            operate(pool, destination, operation, samples=samples)
    assert len(budgets) == 1
    assert budgets[0].active_leases == budgets[0].used_metadata_bytes == budgets[0].used_descriptors == 0
    assert budgets[0].used_bytes == budgets[0].baseline_bytes
    assert calls, 'the intended structural validator was never reached'
    assert {path.name: path.read_bytes() for path in pool.root.iterdir()} == source_before
    assert list(destination.iterdir()) == [foreign]
    assert foreign.read_bytes() == b'foreign sentinel'
    assert 'opaque' not in str(retained.value) and str(pool.root) not in str(retained.value)
    # Retain the traceback while proving all owned files are closed.
    assert retained.value.__traceback__ is not None
    if descriptors is not None:
        assert len(os.listdir('/proc/self/fd')) == descriptors


HEADER_MUTATIONS = (
    ('magic', 0, '<8s', b'INVALID!', UnsupportedFormatError, 'unsupported mapping index'),
    ('version', 8, '<I', 2, UnsupportedFormatError, 'unsupported mapping index'),
    ('width', 12, '<I', 41, IntegrityError, 'width or reserved'),
    ('run', 16, '<16s', b'Y' * 16, IntegrityError, 'header disagrees'),
    ('bundle', 32, '<Q', 1, IntegrityError, 'header disagrees'),
    ('first', 40, '<Q', 1, IntegrityError, 'header disagrees'),
    ('count', 48, '<Q', 5, IntegrityError, 'header disagrees'),
    ('zero_count', 48, '<Q', 0, IntegrityError, 'count or physical length'),
    ('reserved', 56, '<Q', 1, IntegrityError, 'width or reserved'),
)


@pytest.mark.parametrize('operation', ('verify', 'indexed'))
@pytest.mark.parametrize('name,offset,wire,value,error,message', HEADER_MUTATIONS,
                         ids=[case[0] for case in HEADER_MUTATIONS])
def test_P15_T03_every_index_header_field_reaches_public_reader(
        hostile_base, tmp_path, monkeypatch, operation, name, offset, wire, value, error, message):
    import readmixer_py.artifacts.index as index_module
    pool = HostilePool(hostile_base, tmp_path)
    struct.pack_into(wire, pool.index, offset, value)
    pool.save()
    with observe(monkeypatch, index_module, 'decode_header') as calls:
        assert_rejected(pool, tmp_path / 'recovery', operation, error, message, calls)


ENTRY_MUTATIONS = (
    ('sample', 0, '<I', 2, 'outside the declared source domain'),
    ('reserved', 4, '<I', 1, 'reserved field'),
    ('original', 8, '<Q', 3, 'outside the declared source domain'),
    ('r1', 16, '<Q', 1, None),
    ('r2', 24, '<Q', 1, None),
    ('key', 32, '<Q', 191, None),
)


@pytest.mark.parametrize('operation', ('verify', 'indexed'))
@pytest.mark.parametrize('name,offset,wire,value,message', ENTRY_MUTATIONS,
                         ids=[case[0] for case in ENTRY_MUTATIONS])
def test_P15_T03_every_index_entry_field_reaches_public_join(
        hostile_base, tmp_path, monkeypatch, operation, name, offset, wire, value, message):
    import readmixer_py.artifacts.index as index_module
    pool = HostilePool(hostile_base, tmp_path)
    struct.pack_into(wire, pool.index, 64 + offset, value)
    pool.save()
    if message is None:
        message = ('observed physical pair boundaries' if operation == 'verify' else
                   'does not begin a text line' if name == 'key' else 'selected key offset differs')
    with observe(monkeypatch, index_module, 'decode_entry') as calls:
        assert_rejected(pool, tmp_path / 'recovery', operation, IntegrityError, message, calls)


@pytest.mark.parametrize('operation', ('verify', 'indexed'))
@pytest.mark.parametrize('lane', (16, 24, 32), ids=('r1', 'r2', 'key'))
def test_P15_T03_unselected_index_lane_order_still_rejects(
        hostile_base, tmp_path, monkeypatch, operation, lane):
    import readmixer_py.artifacts.index as index_module
    pool = HostilePool(hostile_base, tmp_path)
    unselected_sample = struct.unpack_from('<I', pool.index, 104)[0]
    selected = ('B' if unselected_sample == 0 else 'A',)
    prior = struct.unpack_from('<Q', pool.index, 64 + lane)[0]
    struct.pack_into('<Q', pool.index, 104 + lane, prior)
    pool.save()
    with observe(monkeypatch, index_module, 'decode_entry') as calls:
        assert_rejected(pool, tmp_path / 'recovery', operation, IntegrityError,
                        'do not increase strictly', calls, samples=selected)
    assert len(calls) == 2


# Every decimal field, both metadata roles, and all relationships have explicit
# errors. Width-changing mutations repair all later key virtual addresses.
KEY_MUTATIONS = (
    ('sample_domain', 0, b'2', 'outside the declared source domain'),
    ('original_domain', 1, b'3', 'outside the declared source domain'),
    ('secure_ordinal', 2, b'1', 'secured ordinal'),
    ('tag_hex', 3, b'A' * 32, 'mapping tag is invalid'),
    ('r1_offset', 4, b'1', 'offset'),
    ('r1_length', 5, b'93', 'length'),
    ('r2_offset', 6, b'1', 'offset'),
    ('r2_length', 7, b'93', 'length'),
    ('r1_header_marker', 8, b'7872657065617465642f31', 'invalid markers'),
    ('r1_plus_marker', 9, b'786f7061717565', 'invalid markers'),
    ('r2_header_marker', 10, b'7872657065617465642f32', 'invalid markers'),
    ('r2_plus_marker', 11, b'786f7061717565', 'invalid markers'),
    ('r1_header_lf', 8, b'4072657065617465642f310a', 'invalid markers'),
    ('r2_plus_lf', 11, b'2b0a', 'invalid markers'),
    ('hex_uppercase', 9, b'2B', 'hexadecimal metadata'),
    ('hex_odd', 11, b'2', 'hexadecimal metadata'),
    ('wrong_original_role', 8, b'4072657065617465642f32', 'mate identities disagree'),
    ('wrong_original_identity', 10, b'406f746865722f32', 'mate identities disagree'),
    ('impossible_record_length', 5, b'84', 'record length is impossible'),
    ('endpoint_overflow', 4, b'18446744073709551615', 'endpoint exceeds u64'),
    ('integer_overflow', 6, b'18446744073709551616', 'unsigned field width'),
)
KEY_MUTATIONS += tuple((f'decimal_{field}', field, b'00', 'canonical unsigned decimal')
                       for field in (0, 1, 2, 4, 5, 6, 7))


@pytest.mark.parametrize('operation', ('verify', 'indexed', 'sequential'))
@pytest.mark.parametrize('name,field,replacement,message', KEY_MUTATIONS,
                         ids=[case[0] for case in KEY_MUTATIONS])
def test_P15_T03_key_matrix_reaches_inner_validation_after_repairing_outer_integrity(
        hostile_base, tmp_path, monkeypatch, operation, name, field, replacement, message):
    import readmixer_py.scan as scan
    pool = HostilePool(hostile_base, tmp_path)
    fields = pool.key[1].rstrip(b'\n').split(b',')
    fields[field] = replacement
    pool.key[1] = b','.join(fields) + b'\n'
    pool.rows()
    pool.save()
    with observe(monkeypatch, scan, 'parse_key_row') as calls:
        assert_rejected(pool, tmp_path / 'recovery', operation, IntegrityError, message, calls)
    assert len(calls) == 1


@pytest.mark.parametrize('operation', ('verify', 'indexed', 'sequential'))
@pytest.mark.parametrize('mutation', ('duplicate_missing', 'substituted_sample'))
def test_P15_T03_equal_totals_and_identical_biology_do_not_hide_domain_substitution(
        hostile_base, tmp_path, monkeypatch, operation, mutation):
    import readmixer_py.scan as scan
    pool = HostilePool(hostile_base, tmp_path)
    rows = [row.rstrip(b'\n').split(b',') for row in pool.key[1:]]
    target = next(i for i, row in enumerate(rows) if row[:2] == [b'0', b'2'])
    if mutation == 'duplicate_missing':
        rows[target][1] = b'1'
    else:
        # Equal per-sample totals survive too: exchange ownership of two
        # occurrences while producing a gap/duplicate inside each sample.
        other = next(i for i, row in enumerate(rows) if row[:2] == [b'1', b'1'])
        rows[target][0], rows[other][0] = b'1', b'0'
    for position, row in enumerate(rows):
        pool.key[position + 1] = b','.join(row) + b'\n'
        struct.pack_into('<I', pool.index, 64 + 40 * position, int(row[0]))
        struct.pack_into('<Q', pool.index, 64 + 40 * position + 8, int(row[1]))
    pool.rows()
    pool.save()
    with observe(monkeypatch, scan, '_original') as calls:
        assert_rejected(pool, tmp_path / 'recovery', operation, IntegrityError, 'gap, duplicate', calls)
    assert len(calls) == 6, 'all inner joins must succeed before the identity proof rejects'


@pytest.mark.parametrize('operation', ('verify', 'indexed', 'sequential'))
def test_P15_T03_duplicate_tags_preserve_domain_and_have_explicit_scope(
        hostile_base, tmp_path, monkeypatch, operation):
    import readmixer_py.scan as scan
    pool = HostilePool(hostile_base, tmp_path)
    first, second = (row.split(b',')[3] for row in pool.key[1:3])
    assert first != second
    for role in ('r1', 'r2', 'key'):
        decoded = gzip.decompress(pool.read(role))
        assert decoded.count(second) == 1
        pool.lane(role, decoded.replace(second, first))
    pool.save()
    with observe(monkeypatch, scan, '_original') as calls:
        if operation == 'verify':
            assert_rejected(pool, tmp_path / 'recovery', operation, IntegrityError,
                            'duplicated secured pair tag', calls)
        else:
            report = operate(pool, tmp_path / 'recovery', operation)
            assert report.completed and report.pairs == 6
            assert report.validation_scope == ('selected_records' if operation == 'indexed'
                                               else 'complete_bundle_streams')
    assert len(calls) == 6


@pytest.mark.parametrize('mutation', ('unsupported_header', 'bad_entry', 'trailing'))
def test_P15_T03_sequential_recovery_explicitly_bypasses_hostile_indexes(
        hostile_base, tmp_path, monkeypatch, mutation):
    import readmixer_py.pool as pool_module
    pool = HostilePool(hostile_base, tmp_path)
    if mutation == 'unsupported_header':
        pool.index[:8] = b'INVALID!'
    elif mutation == 'bad_entry':
        struct.pack_into('<I', pool.index, 68, 1)
    else:
        pool.index.extend(b'hostile tail')
        # Sequential mode still validates the manifest schema; the declared
        # canonical size stays unchanged because the bypassed file is untrusted.
    pool.save()
    if mutation == 'trailing':
        pool.document['bundles'][0]['index']['bytes'] = 64 + 40 * 6
        pool.manifest.write_bytes(json.dumps(pool.document).encode() + b'\n')
    original = pool_module.Pool.open
    opened = []
    @contextmanager
    def open_source(self, descriptor, **kwargs):
        assert not descriptor['name'].endswith('.index.rmpyidx')
        opened.append(descriptor['name'])
        with original(self, descriptor, **kwargs) as source:
            yield source
    monkeypatch.setattr(pool_module.Pool, 'open', open_source)
    result = operate(pool, tmp_path / 'recovery', 'sequential')
    assert result.completed and result.index_validation == 'explicitly_bypassed'
    assert len(opened) == 5


@pytest.fixture(scope='module')
def long_key_base(tmp_path_factory):
    root = tmp_path_factory.mktemp('hostile-long-key')
    inputs = root / 'originals'
    inputs.mkdir()
    for sample in ('A', 'B'):
        for role in (1, 2):
            (inputs / f'{sample}_R{role}.fq').write_bytes(
                (f'@repeated/{role}\nACGT\n+'.encode() + b'x' * 12000 + b'\n!!!!\n') * 3)
    report = _mix(MixConfig(input_dir=inputs, output=root / 'pool', max_record_bytes=32768,
                           compression_level=0, **OPTIONS),
                  _keys=OperationKeys._from_test(b'N' * 32, b'Z' * 16))
    manifest = root / 'pool' / report.manifest
    assert verify(VerifyConfig(manifest=manifest, **OPTIONS)).pairs == 6
    return manifest


@pytest.mark.parametrize('operation', ('verify', 'indexed'))
@pytest.mark.parametrize('address', ('record_interior', 'member_header', 'prior_member_endpoint',
                                     'eof_start', 'fabricated_inside_skipped_payload'))
def test_P15_T03_physical_address_matrix_reaches_boundary_proof(
        long_key_base, tmp_path, monkeypatch, operation, address):
    import readmixer_py.scan as scan
    from readmixer_py.artifacts.bgzf import BgzfReader
    pool = HostilePool(long_key_base, tmp_path)
    raw = bytearray(pool.read('key'))
    starts, cursor = [], 0
    while cursor < len(raw):
        starts.append(cursor)
        cursor += struct.unpack_from('<H', raw, cursor + 16)[0] + 1
    assert len(starts) >= 5 and cursor == len(raw)
    assert len(gzip.decompress(raw[starts[0]:starts[1]])) == 65280
    if address == 'record_interior':
        virtual, expected = 191, 'does not begin a text line'
    elif address == 'member_header':
        virtual, expected = (starts[1] + 1) << 16, 'not a BGZF member boundary'
    elif address == 'prior_member_endpoint':
        virtual, expected = 65280, 'not a canonical data position'
    elif address == 'eof_start':
        virtual, expected = starts[-1] << 16, 'targets EOF or an absent member'
    else:
        target = starts[2] + 100
        fake = stored_bgzf(b'forged\n')[:-28]
        assert target + len(fake) < starts[3]
        raw[target:target + len(fake)] = fake
        assert gzip.decompress(bytes(raw[target:target + len(fake)])) == b'forged\n'
        pool.write('key', raw)
        virtual, expected = target << 16, 'not a BGZF member boundary'
    struct.pack_into('<Q', pool.index, 96, virtual)
    pool.save()
    skipped = []
    original_skip = BgzfReader._skip
    def skip(self, count):
        skipped.append(count)
        return original_skip(self, count)
    monkeypatch.setattr(BgzfReader, '_skip', skip)
    with observe(monkeypatch, scan.PoolScanner, '_index') as calls:
        assert_rejected(pool, tmp_path / 'recovery', operation, IntegrityError,
            'observed physical pair boundaries' if operation == 'verify' else expected, calls)
    if operation == 'indexed' and address == 'fabricated_inside_skipped_payload':
        assert skipped, 'must traverse a skipped member before encountering the fabricated address'


@pytest.mark.parametrize('operation', ('verify', 'indexed', 'sequential'))
@pytest.mark.parametrize('role', ('index', 'r1_gzi', 'r2_gzi'))
def test_P15_T02_hostile_index_and_sidecars_cannot_exceed_common_cap(
        hostile_base, tmp_path, monkeypatch, operation, role):
    import readmixer_py.pool as pool_module
    pool = HostilePool(hostile_base, tmp_path)
    pool.document['limits'].update(max_artifact_bytes=65564, max_metadata_bytes=65564)
    oversized = pool.read(role) + b'\0' * (65565 - len(pool.read(role)))
    assert len(oversized) == 65565
    if role == 'index':
        pool.index = bytearray(oversized)
    else:
        pool.write(role, oversized)
    pool.save()
    # Length and digest are truthful, so the cap itself is the earliest invalid
    # invariant. Index bypass does not waive manifest-level format bounds.
    with observe(monkeypatch, pool_module, 'parse_manifest') as calls:
        assert_rejected(pool, tmp_path / 'recovery', operation, IntegrityError,
                        'manifest integer is outside', calls)


@pytest.mark.parametrize('operation', ('verify', 'indexed', 'sequential'))
@pytest.mark.parametrize('mutation', ('key_header', 'tag_join', 'raw_embedded_lf'))
def test_P15_T03_key_framing_and_valid_tag_join_have_public_witnesses(
        hostile_base, tmp_path, monkeypatch, operation, mutation):
    import readmixer_py.scan as scan
    pool = HostilePool(hostile_base, tmp_path)
    if mutation == 'key_header':
        pool.key[0] = b'X' + pool.key[0][1:]
        witness, message = '_readers', 'key header is not canonical'
    elif mutation == 'tag_join':
        fields = pool.key[1].split(b',')
        assert fields[3] != b'0' * 32
        fields[3] = b'0' * 32
        pool.key[1] = b','.join(fields)
        witness, message = '_original', 'secured FASTQ header, role, plus'
    else:
        fields = pool.key[1].split(b',')
        fields[9] = b'2b\n6f'
        pool.key[1] = b','.join(fields)
        witness, message = 'parse_key_row', 'exactly twelve fields'
    pool.rows()
    pool.save()
    target = scan.PoolScanner if mutation == 'key_header' else scan
    with observe(monkeypatch, target, witness) as calls:
        assert_rejected(pool, tmp_path / 'recovery', operation, IntegrityError, message, calls)


@pytest.mark.parametrize('operation', ('verify', 'indexed', 'sequential'))
@pytest.mark.parametrize('mutation', ('unselected_key_tag', 'unselected_duplicate_domain'))
def test_P15_T03_unselected_corruption_respects_recovery_scope(
        hostile_base, tmp_path, monkeypatch, operation, mutation):
    import readmixer_py.scan as scan
    pool = HostilePool(hostile_base, tmp_path)
    position = next(i for i, row in enumerate(pool.key[1:]) if row.startswith(b'1,2,'))
    fields = pool.key[position + 1].rstrip(b'\n').split(b',')
    if mutation == 'unselected_key_tag':
        fields[3] = b'A' * 32
    else:
        fields[1] = b'1'
        struct.pack_into('<Q', pool.index, 64 + 40 * position + 8, 1)
    pool.key[position + 1] = b','.join(fields) + b'\n'
    pool.rows()
    pool.save()
    rejects = operation == 'verify' or operation == 'sequential' and mutation == 'unselected_key_tag'
    with observe(monkeypatch, scan, 'parse_key_row') as calls:
        if rejects:
            assert_rejected(pool, tmp_path / 'recovery', operation, IntegrityError,
                'mapping tag is invalid' if mutation == 'unselected_key_tag' else 'gap, duplicate',
                calls, samples=('A',))
        else:
            result = operate(pool, tmp_path / 'recovery', operation, samples=('A',))
            assert result.completed and result.pairs == 3
            assert len(calls) == (3 if operation == 'indexed' else 6)
            assert result.validation_scope == ('selected_records' if operation == 'indexed'
                                               else 'complete_bundle_streams')


@pytest.mark.parametrize('operation', ('verify', 'indexed', 'sequential'))
def test_P15_T03_internally_consistent_identical_occurrence_permutation_is_valid(
        hostile_base, tmp_path, operation):
    pool = HostilePool(hostile_base, tmp_path)
    altered = 0
    for position, line in enumerate(pool.key[1:]):
        fields = line.rstrip(b'\n').split(b',')
        if fields[0] == b'0' and fields[1] in (b'0', b'1'):
            fields[1] = b'1' if fields[1] == b'0' else b'0'
            pool.key[position + 1] = b','.join(fields) + b'\n'
            struct.pack_into('<Q', pool.index, 64 + 40 * position + 8, int(fields[1]))
            altered += 1
    assert altered == 2
    pool.rows()
    pool.save()
    # Unsigned digests and a valid bijection cannot prove prior placement of
    # indistinguishable repeated occurrences; historical provenance is excluded.
    result = operate(pool, tmp_path / 'recovery', operation)
    assert result.completed and result.pairs == 6


@pytest.mark.parametrize('operation', ('verify', 'indexed', 'sequential'))
@pytest.mark.parametrize('role', ('r1', 'r2', 'key'))
def test_P15_T03_noncanonical_member_header_reaches_decoder_with_matching_outer_hash(
        hostile_base, tmp_path, monkeypatch, operation, role):
    from readmixer_py.artifacts.bgzf import BgzfReader
    pool = HostilePool(hostile_base, tmp_path)
    raw = bytearray(pool.read(role))
    assert raw[3] == 4
    raw[3] = 0
    pool.write(role, raw)
    pool.save()
    with observe(monkeypatch, BgzfReader, '_next') as calls:
        assert_rejected(pool, tmp_path / 'recovery', operation, IntegrityError,
                        'BGZF header is not canonical', calls)


@pytest.mark.parametrize('operation', ('verify', 'indexed', 'sequential'))
def test_P15_T03_truthful_trailing_index_length_fails_canonical_inventory_admission(
        hostile_base, tmp_path, monkeypatch, operation):
    import readmixer_py.pool as pool_module
    pool = HostilePool(hostile_base, tmp_path)
    pool.index.extend(b'trailing byte')
    pool.save()
    # A truthful outer length cannot pass the manifest's exact 64+40*N bound.
    # Claiming the old length instead is caught by file preflight. The bounded
    # IndexReader trailing-byte unit case supplies its separate defensive check.
    with observe(monkeypatch, pool_module, 'parse_manifest') as calls:
        assert_rejected(pool, tmp_path / 'recovery', operation, IntegrityError,
                        'mapping index length or count disagrees', calls)


@pytest.mark.parametrize('operation', ('verify', 'indexed', 'sequential'))
@pytest.mark.parametrize('role,line', (('r1', 1), ('r2', 3)), ids=('r1_sequence', 'r2_quality'))
@pytest.mark.parametrize('selected_body', (True, False), ids=('selected_body', 'unselected_body'))
def test_P15_T03_selfconsistent_biological_body_mutation_reaches_normalized_digest(
        hostile_base, tmp_path, monkeypatch, operation, role, line, selected_body):
    import readmixer_py.scan as scan
    pool = HostilePool(hostile_base, tmp_path)
    sample = b'0' if selected_body else b'1'
    position = next(i for i, row in enumerate(pool.key[1:]) if row.split(b',')[0] == sample)
    decoded = gzip.decompress(pool.read(role))
    lines = decoded.split(b'\n')
    payload = lines[4 * position + line]
    assert len(payload) == 4
    lines[4 * position + line] = (b'T' if line == 1 else b'#') + payload[1:]
    changed = b'\n'.join(lines)
    assert len(changed) == len(decoded) and changed != decoded
    pool.lane(role, changed)
    pool.save()
    # A changed biological byte has valid framing, mate lengths, identities and
    # tags. Only the original normalized digest can expose this broken join.
    with observe(monkeypatch, scan, '_original') as calls:
        if operation == 'verify' or selected_body:
            assert_rejected(pool, tmp_path / 'recovery', operation, IntegrityError,
                            'digest', calls, samples=('A', 'B') if selected_body else ('A',))
            assert len(calls) == 6
        else:
            result = operate(pool, tmp_path / 'recovery', operation, samples=('A',))
            assert result.completed and result.pairs == 3
            assert len(calls) == (3 if operation == 'indexed' else 6)
            assert result.validation_scope == ('selected_records' if operation == 'indexed'
                                               else 'complete_bundle_streams')
