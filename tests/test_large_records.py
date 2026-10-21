"""Public workflows preserve raised record bounds independently of secured sizes."""

import gzip
import hashlib
import json
import struct

import pytest

from readmixer_py import mix, recover, verify
from readmixer_py.config import MixConfig, RecoverConfig, RecoveryMode, StorageMode, VerifyConfig
from readmixer_py.errors import IntegrityError, ResourceError
from readmixer_py.storage import SegmentWriter


_MIB = 1 << 20
_RESOURCES = dict(storage=StorageMode.EXTERNAL, max_ram=1 << 30, workers=2)


def _make_sources(directory, *, large):
    """The oracle is the fixture's raw fields plus LF, without production codecs."""
    directory.mkdir(mode=0o700)
    expected = {}
    counts = {}
    frame_sizes = []
    # Each large mate has more than 1 MiB of header OR plus metadata. Non-ASCII,
    # NUL, comma and interior CR exercise opaque preservation through hex rows.
    metadata = (b'\x00\x80\xff,\t\ropaque' * ((_MIB + 113) // 12 + 1))[:_MIB + 113]
    for sample_index, sample in enumerate(('alpha', 'beta')):
        counts[sample] = 2 if large else 1
        roles = {}
        record_sizes = [[], []]
        for role in (1, 2):
            normalized = bytearray()
            ingress = bytearray()
            for ordinal in range(counts[sample]):
                if large:
                    header = f'@pair{ordinal}/{role} '.encode() + (metadata if sample_index == 0 else b'short\xff')
                    plus = b'+' + (metadata if sample_index == 1 else b'short\xfe')
                    sequence = b'ACGT'
                    quality = b'!#~\r'
                else:
                    header, plus, sequence, quality = f'@r/{role}'.encode(), b'+', b'A', b'!'
                fields = (header, sequence, plus, quality)
                record = b'\n'.join(fields) + b'\n'
                normalized.extend(record)
                # Added CR is a physical terminator; existing terminal CR is
                # retained in the normalized quality payload after ingress.
                ingress.extend(b'\r\n'.join(fields) + b'\r\n')
                record_sizes[role - 1].append(len(record))
            path = directory / f'{sample}_R{role}.fastq.gz'
            path.write_bytes(gzip.compress(ingress, mtime=0))
            roles[f'r{role}'] = bytes(normalized)
        frame_sizes.extend(40 + a + b for a, b in zip(*record_sizes))
        expected[sample] = roles
    return expected, counts, frame_sizes


def _assert_source_manifest_oracle(document, expected, counts):
    for sample in document['samples']:
        name = sample['sample_id']
        assert sample['pairs'] == counts[name]
        for role in ('r1', 'r2'):
            original = expected[name][role]
            assert sample[role]['normalized_bytes'] == len(original)
            assert sample[role]['normalized_sha256'] == hashlib.sha256(original).hexdigest()


def _assert_recovery_oracle(directory, report, expected, counts):
    # stdlib gzip, receipt part order, and direct source bytes form a separate
    # oracle from ReadMixer's readers, pair frames, sidecars and key-row parser.
    receipt = json.loads((directory / report.receipt).read_bytes())
    assert [sample['sample_id'] for sample in receipt['samples']] == list(expected)
    for sample in receipt['samples']:
        name = sample['sample_id']
        assert sample['pairs'] == counts[name]
        for role in ('r1', 'r2'):
            digest = hashlib.sha256()
            byte_count = line_count = 0
            for part in sample['parts']:
                with gzip.open(directory / part[role]['name'], 'rb') as stream:
                    while chunk := stream.read(65536):
                        digest.update(chunk)
                        byte_count += len(chunk)
                        line_count += chunk.count(b'\n')
            assert byte_count == len(expected[name][role])
            assert line_count == counts[name] * 4
            assert digest.digest() == hashlib.sha256(expected[name][role]).digest()


def _count_bgzf_data_members(path):
    """Walk only the public BGZF BSIZE framing, without the production reader."""
    count = 0
    with path.open('rb') as stream:
        while header := stream.read(18):
            assert len(header) == 18 and header[:4] == b'\x1f\x8b\x08\x04'
            member_bytes = struct.unpack_from('<H', header, 16)[0] + 1
            remainder = stream.read(member_bytes - 18)
            assert len(remainder) == member_bytes - 18
            decoded_bytes = struct.unpack_from('<I', remainder, len(remainder) - 4)[0]
            if not decoded_bytes:
                assert member_bytes == 28 and not stream.read(1)
                return count
            count += 1
    pytest.fail('BGZF stream has no final EOF member')


def test_raised_original_record_limit_survives_encrypted_blocks_key_rows_and_both_recoveries(tmp_path, monkeypatch):
    inputs = tmp_path / 'input'
    expected, counts, frame_sizes = _make_sources(inputs, large=True)
    assert min(frame_sizes) > 2 * _MIB + 40
    assert max(frame_sizes) < 2 * (2 * _MIB) + 40
    original_file_hashes = {path.name: hashlib.sha256(path.read_bytes()).digest() for path in inputs.iterdir()}
    observed = []
    original_seal = SegmentWriter.seal

    def seal(writer):
        result = original_seal(writer)
        # Retain aggregate evidence only, never segment refs or frame payloads.
        if result.purpose == 1:
            observed.append((result.decoded_bytes, result.data_block_count, result.physical_bytes))
        return result

    pool_dir = tmp_path / 'pool'
    with monkeypatch.context() as patch:
        patch.setattr(SegmentWriter, 'seal', seal)
        mixed = mix(MixConfig(input_dir=inputs, output=pool_dir, max_record_bytes=2 * _MIB,
                              max_artifact_bytes=16 * _MIB, **_RESOURCES))
    assert observed and all(size > 2 * _MIB and blocks >= 3 and physical > 0
                            for size, blocks, physical in observed)
    assert mixed.completed and mixed.cleanup_complete and mixed.pairs == 4
    manifest = pool_dir / mixed.manifest
    document = json.loads(manifest.read_bytes())
    assert document['limits']['max_input_record_bytes'] == 2 * _MIB
    _assert_source_manifest_oracle(document, expected, counts)
    rows = 0
    for bundle in document['bundles']:
        key_path = pool_dir / bundle['key']['name']
        assert _count_bgzf_data_members(key_path) > 64
        with gzip.open(key_path, 'rb') as key:
            assert key.readline().startswith(b'sample_index,')
            for row in key:
                assert len(row) > 64 * 65280
                rows += 1
        for role in ('r1', 'r2'):
            secured = gzip.decompress((pool_dir / bundle[role]['name']).read_bytes())
            lines = secured.split(b'\n')
            assert len(lines) == 4 * bundle['pairs'] + 1
            assert all(len(lines[index]) == 78 for index in range(0, len(lines) - 1, 4))
            assert bundle[role]['uncompressed_bytes'] < 1024
    assert rows == mixed.pairs
    # A caller can tighten a persisted limit; large key rows/originals must not
    # become acceptable merely because the secured FASTQ records are tiny.
    with pytest.raises((IntegrityError, ResourceError), match='record|mapping|line'):
        verify(VerifyConfig(manifest=manifest, max_record_bytes=_MIB, **_RESOURCES))
    # An omitted caller bound inherits the raised limit persisted by mix.
    verified = verify(VerifyConfig(manifest=manifest, **_RESOURCES))
    assert verified.completed and verified.validation_scope == 'pool_complete'
    assert verified.pairs == 4
    assert verified.normalized_bytes == sum(len(data) for roles in expected.values() for data in roles.values())
    for mode in (RecoveryMode.INDEXED, RecoveryMode.SEQUENTIAL):
        destination = tmp_path / mode.value
        result = recover(RecoverConfig(manifest=manifest, output=destination, sample=('beta', 'alpha'),
                                       io=mode, max_record_bytes=2 * _MIB, **_RESOURCES))
        assert result.completed and result.cleanup_complete and result.pairs == 4
        _assert_recovery_oracle(destination, result, expected, counts)
    assert {path.name: hashlib.sha256(path.read_bytes()).digest() for path in inputs.iterdir()} == original_file_hashes


def test_secured_header_can_exceed_the_tiny_original_record_limit(tmp_path):
    # This is a record-boundary check; the preceding large-record workflow
    # exercises spawn. Keep admission independent of its allocator history.
    resources = dict(_RESOURCES, workers=1)
    inputs = tmp_path / 'input'
    expected, counts, frame_sizes = _make_sources(inputs, large=False)
    original_limit = len(expected['alpha']['r1'])
    assert original_limit == 11 and max(frame_sizes) == 62
    pool_dir = tmp_path / 'pool'
    mixed = mix(MixConfig(input_dir=inputs, output=pool_dir, max_record_bytes=original_limit, **resources))
    manifest = pool_dir / mixed.manifest
    document = json.loads(manifest.read_bytes())
    _assert_source_manifest_oracle(document, expected, counts)
    for bundle in document['bundles']:
        for role in ('r1', 'r2'):
            with gzip.open(pool_dir / bundle[role]['name'], 'rb') as stream:
                assert len(stream.readline()) - 1 == 78 > original_limit
    checked = verify(VerifyConfig(manifest=manifest, max_record_bytes=original_limit, **resources))
    assert checked.completed and checked.pairs == 2
    for mode in (RecoveryMode.INDEXED, RecoveryMode.SEQUENTIAL):
        destination = tmp_path / mode.value
        result = recover(RecoverConfig(manifest=manifest, output=destination, sample=('alpha', 'beta'),
                                       io=mode, max_record_bytes=original_limit, **resources))
        assert result.completed and result.cleanup_complete and result.pairs == 2
        _assert_recovery_oracle(destination, result, expected, counts)
