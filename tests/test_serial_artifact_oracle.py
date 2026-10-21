"""P15-T01: a bounded independent reference for all six serial pool artifacts.

Only the actual-operation side imports ReadMixer. Expected ordering, sizing,
records, member boundaries, sidecars and index entries come from explicit test
fields, stdlib primitives and AES, not runtime parsers or artifact helpers.
This reference has at most 128 pairs and one fixed, unrefined 64-bucket topology;
it is not a production engine or a Fisher-Yates fixed-seed equality oracle.
"""

from collections import defaultdict
from dataclasses import dataclass
import gzip
import hashlib
import hmac
import json
import struct
import zlib

from Crypto.Cipher import AES
import pytest

from readmixer_py import __version__, recover, verify
from readmixer_py.config import MixConfig, RecoverConfig, RecoveryMode, StorageMode, VerifyConfig
from readmixer_py.crypto import OperationKeys
from readmixer_py.mixing import _mix


# Public artificial fixture entropy, injected only through the private seam.
ROOT = hashlib.sha256(b'readmixer-python/shuffle-test/v1/0').digest()
RUN = bytes(range(16))
CAP = 131_100
RECORD_LIMIT = 65_536
SAMPLES = (('alpha', 9), ('beta', 5), ('empty', 0))
ROLES = ('r1', 'r1_gzi', 'r2', 'r2_gzi', 'key', 'index')
KEY_HEADER = (b'sample_index,original_pair_index,secure_pair_index,pair_tag,'
              b'r1_uncompressed_offset,r1_record_bytes,r2_uncompressed_offset,r2_record_bytes,'
              b'r1_header_hex,r1_plus_hex,r2_header_hex,r2_plus_hex\n')
BGZF_PREFIX = bytes.fromhex('1f8b08040000000000ff060042430200')
BGZF_EOF = bytes.fromhex('1f8b08040000000000ff0600424302001b0003000000000000000000')


@dataclass(frozen=True)
class Pair:
    sample: int
    ordinal: int
    mates: tuple

    @property
    def identity(self):
        return self.sample, self.ordinal

    @property
    def encoded_frame_bytes(self):
        return 48 + sum(len(line) for mate in self.mates for line in mate)


def derive(key, domain, *fields):
    """One-block HKDF-SHA256, independently spelling the schema-1 transcript."""
    label = domain.encode('ascii')
    info = (b'RMPY1\0' + struct.pack('>H', len(label)) + label + struct.pack('>H', len(fields))
            + b''.join(struct.pack('>I', len(field)) + field for field in fields))
    extracted = hmac.digest(b'readmixer-python/schema1/hkdf-sha256', key, 'sha256')
    return hmac.digest(extracted, info + b'\x01', 'sha256')


def chacha20(key, length):
    """RFC-style scalar ChaCha20, counter zero and the specified 12 zero nonce."""
    assert len(key) == 32 and 0 <= length <= 16 * 128
    def rotate(word, count):
        return ((word << count) | (word >> (32 - count))) & 0xffffffff
    def quarter(state, a, b, c, d):
        for left, right, target, count in ((a, b, d, 16), (c, d, b, 12),
                                           (a, b, d, 8), (c, d, b, 7)):
            state[left] = (state[left] + state[right]) & 0xffffffff
            state[target] = rotate(state[target] ^ state[left], count)
    blocks = []
    for counter in range((length + 63) // 64):
        initial = list(struct.unpack('<4I', b'expand 32-byte k'))
        initial += list(struct.unpack('<8I', key)) + [counter, 0, 0, 0]
        state = initial.copy()
        for _ in range(10):
            for indices in ((0, 4, 8, 12), (1, 5, 9, 13), (2, 6, 10, 14), (3, 7, 11, 15),
                            (0, 5, 10, 15), (1, 6, 11, 12), (2, 7, 8, 13), (3, 4, 9, 14)):
                quarter(state, *indices)
        blocks.append(struct.pack('<16I', *((left + right) & 0xffffffff
                                           for left, right in zip(state, initial))))
    return b''.join(blocks)[:length]


def canonical_order(pairs):
    assert 2 <= len(pairs) <= 128
    assert len({pair.identity for pair in pairs}) == len(pairs)
    bucket_key = derive(derive(ROOT, 'root/shuffle'), 'shuffle/bucket', b'\0')
    rank_root = derive(ROOT, 'root/rank')
    groups = defaultdict(list)
    for pair in pairs:
        digest = hashlib.blake2b(struct.pack('>IQ', *pair.identity),
                                 key=bucket_key, digest_size=8).digest()
        groups[int.from_bytes(digest, 'little') & 63].append(pair)
    ordered, leaves = [], []
    for bucket in sorted(groups):
        source = sorted(groups[bucket], key=lambda pair: pair.identity)
        count = len(source)
        frame_bytes = sum(pair.encoded_frame_bytes for pair in source)
        assert count <= 32_768 and frame_bytes <= 64 << 20
        assert frame_bytes + 128 * count + (32 << 20) <= 128 << 20
        path = bytes((1, bucket))
        for attempt in range(8):
            key = derive(rank_root, 'shuffle/rank', path, struct.pack('>Q', count), bytes((attempt,)))
            stream = chacha20(key, 16 * count)
            high = struct.unpack('<' + 'Q' * count, stream[:8 * count])
            low = struct.unpack('<' + 'Q' * count, stream[8 * count:])
            ranks = list(zip(high, low))
            if len(set(ranks)) == count:
                break
        else:
            raise AssertionError('the fixed tiny reference exhausted rank attempts')
        permutation = sorted(range(count), key=ranks.__getitem__)
        ordered.extend(source[index] for index in permutation)
        leaves.append((bucket, tuple(pair.identity for pair in source), tuple(ranks),
                       tuple(permutation), frame_bytes, attempt))
    return ordered, leaves


def tag(pair):
    key = derive(ROOT, 'root/tag')
    return AES.new(key, AES.MODE_ECB).encrypt(b'RMP1' + struct.pack('>IQ', *pair.identity)).hex()


def fixtures(profile):
    """Explicit opaque fields; occurrence identities do not depend on content."""
    assert profile in ('tiny', 'rollover')
    pairs = []
    for sample, (_label, count) in enumerate(SAMPLES):
        for ordinal in range(count):
            mates = []
            for role in (1, 2):
                if profile == 'tiny':
                    metadata = b'opaque,\x00\xff\tembedded\rX'
                    sequence = b'' if ordinal == 0 else (b'X\0\xff' if role == 1 else b'Y\rZ') * 3
                else:
                    metadata = (b'opaque,\x00\xff\t\rX' * 400)[:4096]
                    size = 20_000 if role == 1 else 17_000
                    sequence = ((b'X\0\xff' if role == 1 else b'Y\rZ') * ((size + 2) // 3))[:size]
                quality = b'!' * (len(sequence) - 1) + b'\r' if sequence else b''
                mate = (f'@repeat/{role} '.encode() + metadata, sequence,
                        b'+private,\xfe\0' + metadata, quality)
                assert sum(len(line) + 1 for line in mate) <= RECORD_LIMIT
                mates.append(mate)
            pairs.append(Pair(sample, ordinal, tuple(mates)))
    assert len(pairs) == 14 and len(pairs) <= 128
    # Repeated bytes at distinct ordinals and across samples are intentional.
    lookup = {pair.identity: pair for pair in pairs}
    assert lookup[0, 1].mates == lookup[0, 2].mates == lookup[1, 1].mates
    return pairs


def original_bytes(pairs, sample, mate):
    return b''.join(b'\n'.join(pair.mates[mate]) + b'\n'
                    for pair in pairs if pair.sample == sample)


def write_inputs(root, pairs):
    root.mkdir(mode=0o700)
    samples, source_bytes = [], {}
    for sample, (label, count) in enumerate(SAMPLES):
        descriptor = dict(sample_index=sample, sample_id=label, pairs=count)
        for mate in range(2):
            # R1 is concatenated gzip with CRLF. R2 alternates LF/CRLF by sample.
            separator = b'\r\n' if mate == 0 or sample % 2 else b'\n'
            selected = [pair.mates[mate] for pair in pairs if pair.sample == sample]
            physical_lines = [line for record in selected for line in record]
            raw = b''.join(line + (b'\r\n' if line.endswith(b'\r') else separator)
                           for line in physical_lines[:-1])
            raw += physical_lines[-1] if physical_lines else b''
            # A nonempty final quality deliberately lacks LF and ends in a
            # payload CR. Empty inputs remain zero decoded bytes.
            assert not selected or selected[-1][3].endswith(b'\r')
            normalized = original_bytes(pairs, sample, mate)
            compressed = mate == 0
            name = f'{label}_R{mate + 1}.fastq' + ('.gz' if compressed else '')
            midpoint = len(raw) // 2
            physical = (gzip.compress(raw[:midpoint], mtime=0)
                        + gzip.compress(raw[midpoint:], mtime=0)) if compressed else raw
            source_bytes[name] = physical
            descriptor[f'r{mate + 1}'] = dict(
                source_file_id=2 * sample + mate, relative_path_hex=name.encode('ascii').hex(),
                input_codec='gzip' if compressed else 'plain', physical_bytes=len(physical),
                decoded_bytes=len(raw), normalized_bytes=len(normalized),
                normalized_sha256=hashlib.sha256(normalized).hexdigest())
        samples.append(descriptor)
    # Creation order is deliberately unrelated to canonical sample/role order.
    for name in sorted(source_bytes, reverse=True):
        (root / name).write_bytes(source_bytes[name])
    return samples, source_bytes


def bgzf(data, level):
    """Independent canonical encoder; returns bytes and actual member starts."""
    result = bytearray()
    starts = []
    for decoded_start in range(0, len(data), 65280):
        payload = data[decoded_start:decoded_start + 65280]
        starts.append((len(result), decoded_start, len(payload)))
        for compression_level in (level, 0):
            compressor = zlib.compressobj(compression_level, zlib.DEFLATED, -15)
            compressed = compressor.compress(payload) + compressor.flush(zlib.Z_FINISH)
            size = 26 + len(compressed)
            if size <= 65536:
                break
        else:
            raise AssertionError('independent BGZF encoder exceeded the physical member bound')
        result.extend(BGZF_PREFIX + struct.pack('<H', size - 1) + compressed
                      + struct.pack('<II', zlib.crc32(payload), len(payload)))
    result.extend(BGZF_EOF)
    return bytes(result), starts


def virtual(starts, offset):
    for compressed, decoded, length in starts:
        if decoded <= offset < decoded + length:
            return (compressed << 16) | (offset - decoded)
    raise AssertionError('reference start is not in a data member')


def key_row(pair, secure, pair_tag, records, offsets):
    numeric = (pair.sample, pair.ordinal, secure, pair_tag,
               offsets[0], len(records[0]), offsets[1], len(records[1]))
    metadata = tuple(line.hex() for mate in pair.mates for line in (mate[0], mate[2]))
    return (','.join(map(str, (*numeric, *metadata))) + '\n').encode('ascii')


def reference(pairs, level):
    ordered, leaves = canonical_order(pairs)
    bundles, artifacts, placements, members = [], {}, [], {}
    # This bounded reference may materialize complete test lanes and placement
    # records. The production implementation must remain streaming.
    current = []
    decoded = [b'', b'', KEY_HEADER]

    def finish():
        bundle_index = len(bundles)
        prefix = f'pool.{RUN.hex()}.b{bundle_index:06d}'
        names = dict(r1=prefix + '.R1.fastq.gz', r2=prefix + '.R2.fastq.gz',
                     key=prefix + '.key.csv.gz', index=prefix + '.index.rmpyidx')
        names.update(r1_gzi=names['r1'] + '.gzi', r2_gzi=names['r2'] + '.gzi')
        descriptor = dict(bundle_index=bundle_index, first_secure_pair_index=current[0][1], pairs=len(current))
        lane_starts = []
        for role, data in zip(('r1', 'r2', 'key'), decoded):
            physical, starts = bgzf(data, level)
            lane_starts.append(starts)
            members[names[role]] = starts
            artifacts[names[role]] = physical
            descriptor[role] = dict(name=names[role], bytes=len(physical),
                sha256=hashlib.sha256(physical).hexdigest(), uncompressed_bytes=len(data), records=len(current))
        for role, starts in zip(('r1', 'r2'), lane_starts):
            data = struct.pack('<Q', len(starts) - 1)
            data += b''.join(struct.pack('<QQ', compressed, uncompressed)
                             for compressed, uncompressed, _length in starts[1:])
            sidecar = role + '_gzi'
            artifacts[names[sidecar]] = data
            descriptor[sidecar] = dict(name=names[sidecar], bytes=len(data),
                sha256=hashlib.sha256(data).hexdigest(), entries=len(starts) - 1)
        index = struct.pack('<8sII16sQQQQ', b'RMPYIDX1', 1, 40, RUN,
                            bundle_index, current[0][1], len(current), 0)
        for pair, secure, offsets, record_lengths in current:
            addresses = tuple(virtual(starts, offset) for starts, offset in zip(lane_starts, offsets))
            index += struct.pack('<IIQQQQ', pair.sample, 0, pair.ordinal, *addresses)
            placements.append((bundle_index, pair.identity, secure, offsets, record_lengths, addresses))
        artifacts[names['index']] = index
        descriptor['index'] = dict(name=names['index'], bytes=len(index),
                                  sha256=hashlib.sha256(index).hexdigest(), entries=len(current))
        assert all(descriptor[role]['bytes'] <= CAP for role in ROLES)
        bundles.append(descriptor)

    for secure, pair in enumerate(ordered):
        pair_tag = tag(pair)
        records = tuple(b'\n'.join((f'@ReadMixer.{RUN.hex()}.{pair_tag}/{role + 1}'.encode('ascii'),
                                  pair.mates[role][1], b'+', pair.mates[role][3])) + b'\n'
                        for role in range(2))
        while True:
            offsets = tuple(map(len, decoded))
            row = key_row(pair, secure, pair_tag, records, offsets)
            projected = tuple(offset + len(data) for offset, data in zip(offsets, (*records, row)))
            data_members = tuple((length + 65279) // 65280 for length in projected)
            safe_decoded = ((CAP - 28) // 65536) * 65280
            fits = (all(length <= safe_decoded for length in projected)
                    and 64 + 40 * (len(current) + 1) <= CAP
                    and all(8 + 16 * (count - 1) <= CAP for count in data_members[:2]))
            if fits:
                break
            assert current, 'the artificial reference pair must fit an empty bundle'
            finish()
            current, decoded = [], [b'', b'', KEY_HEADER]
        current.append((pair, secure, offsets, tuple(map(len, records))))
        decoded = [before + data for before, data in zip(decoded, (*records, row))]
    finish()
    return bundles, artifacts, placements, members, leaves


def expected_manifest(samples, bundles, level):
    return dict(
        format='readmixer-python-pool', schema_version=1, normalization_version=1,
        run_id=RUN.hex(), producer=dict(name='readmixer-py', version=__version__),
        shuffle=dict(algorithm_version=1, tag_algorithm='aes256-identity-prp-v1'),
        limits=dict(max_artifact_bytes=CAP, max_input_record_bytes=RECORD_LIMIT,
                    max_metadata_bytes=CAP, max_artifacts=100_000),
        codec=dict(format='bgzf', compression_level=level, payload_bytes=65280,
                   max_member_bytes=65536, eof_bytes=28, digest_algorithm='sha256'),
        samples=samples, bundles=bundles,
        totals=dict(samples=3, pairs=14, reads=28, bundles=len(bundles), artifacts=6 * len(bundles) + 1,
                    **{role + '_uncompressed_bytes': sum(bundle[role]['uncompressed_bytes'] for bundle in bundles)
                       for role in ('r1', 'r2', 'key')}))


def reject_duplicate_keys(items):
    result = dict(items)
    assert len(items) == len(result), 'the manifest repeated a JSON object key'
    return result


def check_restored(destination, report, pairs, mode):
    receipt = json.loads((destination / report.receipt).read_bytes(), object_pairs_hook=reject_duplicate_keys)
    assert [sample['sample_id'] for sample in receipt['samples']] == ['alpha', 'beta', 'empty']
    assert receipt['validation_scope'] == report.validation_scope == (
        'selected_records' if mode is RecoveryMode.INDEXED else 'complete_bundle_streams')
    assert receipt['index_validation'] == report.index_validation == (
        'validated' if mode is RecoveryMode.INDEXED else 'explicitly_bypassed')
    for sample in receipt['samples']:
        sample_index = sample['sample_index']
        assert sample['pairs'] == SAMPLES[sample_index][1]
        for mate, role in enumerate(('r1', 'r2')):
            actual = b''.join(gzip.decompress((destination / part[role]['name']).read_bytes())
                              for part in sample['parts'])
            assert actual == original_bytes(pairs, sample_index, mate)
            # Field-by-field and ordinal comparison uses literal LF splitting;
            # generic splitlines() would corrupt admitted standalone CR bytes.
            lines = actual.split(b'\n')
            assert lines[-1] == b'' and len(lines) == 4 * sample['pairs'] + 1
            observed = [tuple(lines[position:position + 4]) for position in range(0, len(lines) - 1, 4)]
            expected = [pair.mates[mate] for pair in pairs if pair.sample == sample_index]
            assert observed == expected
    assert report.completed and report.samples == 3 and report.pairs == 14 and report.reads == 28
    assert report.cleanup_complete and not report.warnings


def test_P15_T01_reference_primitives_have_independent_literal_goldens():
    # Schema-1 HKDF root vector, ChaCha20's all-zero first block, and the
    # AES-256 FIPS-197 known-answer block pin the reference primitives.
    assert derive(bytes(range(32)), 'root/tag').hex() == (
        '03c5d45059a969744eee89ddad77f5c4df7229685b76e7dcc1df4429f9555490')
    assert chacha20(bytes(32), 64).hex() == (
        '76b8e0ada0f13d90405d6ae55386bd28bdd219b8a08ded1aa836efcc8b770dc7da4159'
        '7c5157488d7724e03fb8d84a376a43b8f41518a11cc387b669b2ee6586')
    assert AES.new(bytes(range(32)), AES.MODE_ECB).encrypt(
        bytes.fromhex('00112233445566778899aabbccddeeff')).hex() == '8ea2b7ca516745bfeafc49904b496089'
    # Stored DEFLATE, the standard CRC32 check string and fixed EOF are literal,
    # not generated by a runtime artifact encoder or its expected-size helper.
    expected = bytes.fromhex(
        '1f8b08040000000000ff0600424302002700'
        '010900f6ff3132333435363738392639f4cb09000000'
        '1f8b08040000000000ff0600424302001b0003000000000000000000')
    encoded, starts = bgzf(b'123456789', 0)
    assert encoded == expected and starts == [(0, 0, 9)]
    assert gzip.decompress(expected) == b'123456789'
    assert bgzf(b'', 6) == (BGZF_EOF, [])
    assert len(KEY_HEADER) == 190


def test_P15_T01_complete_tiny_reference_has_frozen_order_tags_and_artifact_hashes():
    pairs = fixtures('tiny')
    bundles, artifacts, placements, _members, leaves = reference(pairs, 0)
    assert [entry[1] for entry in placements] == [
        (0, 7), (1, 3), (0, 6), (1, 0), (1, 2), (0, 0), (1, 4),
        (0, 3), (0, 2), (0, 5), (0, 1), (0, 8), (1, 1), (0, 4)]
    shared, = [leaf for leaf in leaves if len(leaf[1]) > 1]
    assert shared[:4] == (62, ((0, 4), (1, 1)),
        ((7597989965849973711, 13945710113608762002),
         (6226451461704481360, 2517762122980901004)), (1, 0))
    assert [tag(pair) for pair in pairs[:3]] == [
        'bf85c98155085f96de5185b08b40c2cb', 'f3dece02f59a3072517ae5f132ee147c',
        '34f63c11c70e7b6983074e3aa3823389']
    expected_hashes = {
        'r1': '3df7d5163627620acb4e79391255023e4656873bb54918f914babb8628af5926',
        'r1_gzi': 'af5570f5a1810b7af78caf4bc70a660f0df51e42baf91d4de5b2328de0e83dfc',
        'r2': '6dd486ec96c51e6342608cd838973be1bdcfade8d4b681d231a0de09b3a4a787',
        'r2_gzi': 'af5570f5a1810b7af78caf4bc70a660f0df51e42baf91d4de5b2328de0e83dfc',
        'key': '30b02e750d31cf918cd9c505aed53305d18197589b202c005d16908274cff9b8',
        'index': '35e606380ac7805bc0ce4d1bca824f9d670d6e1219adf51287d08a602c618eb8',
    }
    assert len(bundles) == 1
    for role, digest in expected_hashes.items():
        assert hashlib.sha256(artifacts[bundles[0][role]['name']]).hexdigest() == digest


@pytest.mark.parametrize('profile', ('tiny', 'rollover'))
@pytest.mark.parametrize('level', (0, 6))
@pytest.mark.parametrize('mode', tuple(StorageMode))
def test_P15_T01_serial_six_artifacts_match_complete_independent_reference(
        tmp_path, profile, level, mode):
    pairs = fixtures(profile)
    samples, source_bytes = write_inputs(tmp_path / 'originals', pairs)
    bundles, artifacts, placements, members, leaves = reference(pairs, level)
    expected = expected_manifest(samples, bundles, level)
    # Build/level/topology/root/input/cap are fixed within each comparison. A
    # level-6 result is not compared against another zlib build's DEFLATE bytes.
    codec_build = zlib.ZLIB_VERSION, zlib.ZLIB_RUNTIME_VERSION
    assert all(leaf[-1] == 0 for leaf in leaves)
    assert any(len(leaf[1]) > 1 and leaf[3] != tuple(range(len(leaf[1]))) for leaf in leaves)
    assert len({tag(pair) for pair in pairs}) == len(pairs)
    assert {entry[1] for entry in placements} == {pair.identity for pair in pairs}
    assert [entry[2] for entry in placements] == list(range(14))
    if profile == 'tiny':
        assert len(bundles) == 1
        assert any(not mate[1] for pair in pairs for mate in pair.mates)
    else:
        assert [bundle['pairs'] for bundle in bundles] == [3, 3, 3, 3, 2]
        for role in ('r1', 'r2', 'key'):
            assert all(len(members[bundle[role]['name']]) == 2 for bundle in bundles)
            column = ('r1', 'r2', 'key').index(role)
            assert any(entry[5][column] >> 16 for entry in placements)
            # At least one complete record/row really crosses a member, rather
            # than merely placing two small records into different members.
            spans = []
            for bundle in bundles:
                starts = [entry[3][column] for entry in placements if entry[0] == bundle['bundle_index']]
                ends = [*starts[1:], bundle[role]['uncompressed_bytes']]
                spans.extend(zip(starts, ends))
            assert any(start // 65280 != (end - 1) // 65280 for start, end in spans)

    destination = tmp_path / 'pool'
    report = _mix(MixConfig(input_dir=tmp_path / 'originals', output=destination,
                           workers=1, max_ram=512 << 20, storage=mode,
                           compression_level=level, max_artifact_bytes=CAP,
                           max_record_bytes=RECORD_LIMIT),
                  _keys=OperationKeys._from_test(ROOT, RUN))
    manifest_name = f'pool.{RUN.hex()}.manifest.json'
    assert report.manifest == manifest_name
    manifest_bytes = (destination / manifest_name).read_bytes()
    document = json.loads(manifest_bytes, object_pairs_hook=reject_duplicate_keys)
    execution = document.pop('execution')
    assert document == expected
    assert execution['profile_version'] == 1 and execution['workers_effective'] == 1
    assert execution['storage_mode'] == mode.value
    assert all(leaf[4] + 128 * len(leaf[1]) + (32 << 20) <= execution['terminal_slice_bytes']
               for leaf in leaves), 'actual admission must retain the fixed unrefined topology'
    assert set(path.name for path in destination.iterdir()) == {*artifacts, manifest_name}
    for name, reference_bytes in artifacts.items():
        actual = (destination / name).read_bytes()
        assert actual == reference_bytes, (profile, mode.value, level, codec_build, name)
        assert (destination / name).stat().st_mode & 0o777 == 0o600
        assert len(actual) <= CAP
    assert manifest_bytes.endswith(b'\n') and len(manifest_bytes) <= CAP
    assert report.completed and report.cleanup_complete and not report.warnings
    assert report.samples == 3 and report.pairs == 14 and report.reads == 28
    assert report.artifacts == 6 * len(bundles) + 1 and report.bundles == len(bundles)
    assert report.physical_bytes == len(manifest_bytes) + sum(map(len, artifacts.values()))
    assert report.normalized_bytes == sum(sample[role]['normalized_bytes'] for sample in samples for role in ('r1', 'r2'))

    verified = verify(VerifyConfig(manifest=destination / manifest_name, max_ram=512 << 20,
                                   workers=1, storage=mode))
    assert verified.completed and verified.validation_scope == 'pool_complete'
    assert verified.pairs == 14 and verified.normalized_bytes == report.normalized_bytes
    for recovery_mode in (RecoveryMode.INDEXED, RecoveryMode.SEQUENTIAL):
        restored = tmp_path / recovery_mode.value
        recovered = recover(RecoverConfig(manifest=destination / manifest_name, output=restored,
            sample=('empty', 'beta', 'alpha'), io=recovery_mode, workers=1,
            max_ram=512 << 20, storage=mode))
        check_restored(restored, recovered, pairs, recovery_mode)
    assert {name: (tmp_path / 'originals' / name).read_bytes() for name in source_bytes} == source_bytes
