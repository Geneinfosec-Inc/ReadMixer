"""P15-T03: outer-integrity-consistent secured fields fail the public join.

These cases preserve every lane length and virtual offset. The independent
stored-DEFLATE encoder updates CRC and SHA-256 so the intended inner check is
reached, rather than an unrelated framing or artifact-hash rejection.
"""

import gzip
import hashlib
import json
import shutil
import struct
import zlib

import pytest

from readmixer_py.config import MixConfig, RecoverConfig, RecoveryMode, StorageMode, VerifyConfig
from readmixer_py.crypto import OperationKeys
from readmixer_py.errors import IntegrityError
from readmixer_py.mixing import _mix
from readmixer_py.recovery import recover
from readmixer_py.verification import verify


def stored_bgzf(data):
    assert 0 < len(data) < 65280
    compressor = zlib.compressobj(0, zlib.DEFLATED, -15)
    payload = compressor.compress(data) + compressor.flush()
    size = 26 + len(payload)
    return (bytes.fromhex('1f8b08040000000000ff060042430200')
            + struct.pack('<H', size - 1) + payload
            + struct.pack('<II', zlib.crc32(data), len(data))
            + bytes.fromhex('1f8b08040000000000ff0600424302001b0003000000000000000000'))


@pytest.fixture(scope='module')
def secured_pool(tmp_path_factory):
    root = tmp_path_factory.mktemp('secured-fields')
    original = root / 'original'
    original.mkdir(mode=0o700)
    for sample in ('A', 'B'):
        for role in (1, 2):
            (original / f'{sample}_R{role}.fastq').write_bytes(
                f'@original/{role} opaque description\nACGT\n+original annotation\n!!!!\n'.encode())
    report = _mix(MixConfig(input_dir=original, output=root / 'pool', workers=1,
                           max_ram=512 << 20, storage=StorageMode.EXTERNAL,
                           max_record_bytes=1024, compression_level=0),
                  _keys=OperationKeys._from_test(b'K' * 32, b'R' * 16))
    manifest = root / 'pool' / report.manifest
    # Start with an independently valid artifact and a successful public proof.
    assert verify(VerifyConfig(manifest=manifest, workers=1, max_ram=512 << 20)).completed
    shutil.rmtree(original)
    return manifest


@pytest.mark.parametrize('operation', ('verify', 'indexed', 'sequential'))
@pytest.mark.parametrize('mutation', ('role', 'non_bare_plus', 'run_id', 'pair_tag'))
def test_secured_field_mutation_reaches_inner_join_despite_matching_outer_integrity(
        secured_pool, tmp_path, monkeypatch, operation, mutation):
    source = tmp_path / 'source'
    shutil.copytree(secured_pool.parent, source)
    manifest = source / secured_pool.name
    document = json.loads(manifest.read_bytes())
    assert len(document['bundles']) == 1 and document['bundles'][0]['pairs'] == 2
    descriptor = document['bundles'][0]['r1']
    path = source / descriptor['name']
    before = path.read_bytes()
    decoded = gzip.decompress(before)
    assert before == stored_bgzf(decoded), 'fixture must use the independently encoded stored member'
    lines = decoded.split(b'\n')
    assert len(lines) == 9 and not lines[-1]
    header, sequence, plus, quality = lines[:4]
    assert plus == b'+' and len(sequence) == len(quality) == 4
    index = (source / document['bundles'][0]['index']['name']).read_bytes()
    sample_index, reserved, original_index = struct.unpack_from('<IIQ', index, 64)
    assert reserved == 0
    if mutation == 'role':
        assert header.endswith(b'/1')
        header = header[:-1] + b'2'
    elif mutation == 'non_bare_plus':
        # Keep total record bytes, all offsets and sequence/quality equality.
        # The basic FASTQ parser accepts this; the secured bare-plus rule must not.
        sequence, quality, plus = sequence[:-1], quality[:-1], b'+xx'
    else:
        fields = header.split(b'.')
        field = 1 if mutation == 'run_id' else 2
        first = b'0' if fields[field][:1] != b'0' else b'1'
        fields[field] = first + fields[field][1:]
        header = b'.'.join(fields)
    changed_record = b'\n'.join((header, sequence, plus, quality)) + b'\n'
    changed = changed_record + b'\n'.join(lines[4:])
    assert len(changed) == len(decoded) and changed != decoded
    encoded = stored_bgzf(changed)
    assert len(encoded) == len(before) and gzip.decompress(encoded) == changed
    path.write_bytes(encoded)
    descriptor['sha256'] = hashlib.sha256(encoded).hexdigest()
    manifest.write_text(json.dumps(document, separators=(',', ':')) + '\n')
    # Every declared length/hash still agrees, including unchanged sidecars and
    # indexes. Only the selected secured semantic invariant was made invalid.
    for bundle in document['bundles']:
        for role in ('r1', 'r2', 'key', 'r1_gzi', 'r2_gzi', 'index'):
            raw = (source / bundle[role]['name']).read_bytes()
            assert len(raw) == bundle[role]['bytes']
            assert hashlib.sha256(raw).hexdigest() == bundle[role]['sha256']

    import readmixer_py.scan as scan
    original_join = scan._original
    reached = []
    def observe_join(row, r1, r2, run_id, arena, record_limit):
        assert r1 == changed_record
        reached.append((row.sample_index, row.original_pair_index, len(r1)))
        return original_join(row, r1, r2, run_id, arena, record_limit)
    monkeypatch.setattr(scan, '_original', observe_join)
    destination = tmp_path / 'rejected'
    with pytest.raises(IntegrityError, match='secured FASTQ header, role, plus, or line framing is invalid'):
        if operation == 'verify':
            verify(VerifyConfig(manifest=manifest, workers=1, max_ram=512 << 20,
                                storage=StorageMode.EXTERNAL))
        else:
            mode = RecoveryMode.INDEXED if operation == 'indexed' else RecoveryMode.SEQUENTIAL
            recover(RecoverConfig(manifest=manifest, output=destination, sample=('A', 'B'),
                                  io=mode, workers=1, max_ram=512 << 20, storage=StorageMode.EXTERNAL))
    assert reached == [(sample_index, original_index, len(changed_record))]
    assert not destination.exists() or not list(destination.iterdir())
