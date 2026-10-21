"""Independent CSV/physical-member oracles for reachable bundle cap boundaries.

Default-cap tests below are arithmetic proofs, not multi-gigabyte host runs.
Ordered-emitter fixtures isolate decimal rollover; public mix fixtures also run
the complete independent verifier. No production placement/sizing helper defines
the expected boundary.
"""

import gzip
import hashlib
import json
import struct

import pytest

from readmixer_py import mix, verify
from readmixer_py.artifacts.bundles import BundleEmitter
from readmixer_py.config import MixConfig, StorageMode, VerifyConfig
from readmixer_py.crypto import OperationKeys
from readmixer_py.resources import Budget
from readmixer_py.shuffle import OrderedPair
from readmixer_py.transaction import OutputTransaction


CSV_HEADER = (b'sample_index,original_pair_index,secure_pair_index,pair_tag,'
              b'r1_uncompressed_offset,r1_record_bytes,r2_uncompressed_offset,r2_record_bytes,'
              b'r1_header_hex,r1_plus_hex,r2_header_hex,r2_plus_hex\n')
ROLES = ('r1', 'r1_gzi', 'r2', 'r2_gzi', 'key', 'index')
EOF = bytes.fromhex('1f8b08040000000000ff0600424302001b0003000000000000000000')


def source_lines(r1_bases=0, r2_bases=0, plus_padding=0):
    return (b'@x/1', b'A' * r1_bases, b'+' + b'p' * plus_padding, b'!' * r1_bases,
            b'@x/2', b'C' * r2_bases, b'+', b'#' * r2_bases)


def expected_row(lines, sample, original, secure, tag, r1_offset, r2_offset):
    fields = [str(sample), str(original), str(secure), tag,
              str(r1_offset), str(83 + 2 * len(lines[1])),
              str(r2_offset), str(83 + 2 * len(lines[5])),
              lines[0].hex(), lines[2].hex(), lines[4].hex(), lines[6].hex()]
    return (','.join(fields) + '\n').encode('ascii')


def member_starts(data):
    """Parse actual BGZF physical headers; gzip is the independent decoder."""
    compressed = decoded = 0
    starts = []
    while compressed < len(data):
        assert data[compressed:compressed + 4] == b'\x1f\x8b\x08\x04'
        physical = 1 + struct.unpack_from('<H', data, compressed + 16)[0]
        assert physical <= 65536
        member = data[compressed:compressed + physical]
        payload = gzip.decompress(member)
        if not payload:
            assert member == EOF and compressed + physical == len(data)
        else:
            assert len(payload) <= 65280
            starts.append((compressed, decoded))
            decoded += len(payload)
        compressed += physical
    assert compressed == len(data)
    return starts


def check_artifacts(root, bundles, cap):
    for bundle in bundles:
        for role in ROLES:
            descriptor = bundle[role]
            physical = (root / descriptor['name']).read_bytes()
            assert len(physical) == descriptor['bytes'] <= cap
            assert hashlib.sha256(physical).hexdigest() == descriptor['sha256']
        for mate in ('r1', 'r2'):
            starts = member_starts((root / bundle[mate]['name']).read_bytes())
            expected = struct.pack('<Q', len(starts) - 1)
            expected += b''.join(struct.pack('<QQ', *start) for start in starts[1:])
            assert (root / bundle[mate + '_gzi']['name']).read_bytes() == expected
        member_starts((root / bundle['key']['name']).read_bytes())
        assert bundle['index']['bytes'] == 64 + 40 * bundle['pairs']


def test_P15_T02_index_and_sidecars_are_dominated_at_every_valid_cap_step():
    # This literal is the smallest valid schema row: two @x headers, bare plus,
    # and two minimum secured-record lengths. It is independent of key.py.
    minimum_row = b'0,0,0,' + b'0' * 32 + b',0,83,0,83,4078,2b,4078,2b\n'
    assert len(minimum_row) == 65 and len(CSV_HEADER) == 190
    maximum_members = (5_000_000_000 - 28) // 65536
    for members in range(1, maximum_members + 1):
        # Within each quotient interval the largest sidecar/index is fixed and
        # the smallest possible cap is the strictest physical comparison.
        minimum_cap = 65536 * members + 28
        safe_decoded = 65280 * members
        sidecar_bytes = 8 + 16 * (members - 1)
        maximum_pairs = (safe_decoded - len(CSV_HEADER)) // len(minimum_row)
        index_bytes = 64 + 40 * maximum_pairs
        assert sidecar_bytes < minimum_cap
        assert index_bytes < safe_decoded < minimum_cap
    assert 65280 * maximum_members == 4_980_407_040


@pytest.mark.parametrize('cap', [65564, 131099, 131100])
@pytest.mark.parametrize('limiting_lane', ['r1', 'r2', 'key'])
@pytest.mark.parametrize('projection_over', [0, 1])
def test_P15_T02_public_reachable_lane_exact_and_one_over(tmp_path, cap, limiting_lane, projection_over):
    safe = ((cap - 28) // 65536) * 65280
    if limiting_lane == 'key':
        count = 2 if projection_over == 0 else 3
        prototypes = [source_lines() for _ in range(count)]
        base = len(CSV_HEADER) + sum(len(expected_row(lines, i % 2, i // 2, i, '0' * 32,
                                                      83 * i, 83 * i))
                                      for i, lines in enumerate(prototypes))
        assert (safe + projection_over - base) % 2 == 0
        padding = (safe + projection_over - base) // 2
        records = [source_lines(plus_padding=padding), *prototypes[1:]]
    else:
        # Every secured mate has 83+2*n bytes. Two records can reach even S;
        # three can reach odd S+1. Every proper prefix remains below S.
        count = 2 if projection_over == 0 else 3
        total_bases = (safe + projection_over - 83 * count) // 2
        lengths = [total_bases // 2, total_bases - total_bases // 2]
        if count == 3:
            lengths.append(0)
        records = [source_lines(**{limiting_lane + '_bases': length}) for length in lengths]
    inputs = tmp_path / 'input'
    inputs.mkdir()
    originals = {}
    for sample in (0, 1):
        for mate in (0, 1):
            payload = bytearray()
            for index, lines in enumerate(records):
                if index % 2 == sample:
                    originals[sample, index // 2] = lines
                    payload.extend(b'\n'.join(lines[4 * mate:4 * mate + 4]) + b'\n')
            (inputs / f'sample{sample}_R{mate + 1}.fq').write_bytes(payload)
    result = mix(MixConfig(input_dir=inputs, output=tmp_path / 'pool', max_ram=512 << 20,
                           storage=StorageMode.EXTERNAL, workers=1, compression_level=0,
                           max_artifact_bytes=cap))
    manifest_path = tmp_path / 'pool' / result.manifest
    manifest = json.loads(manifest_path.read_bytes())
    bundles = manifest['bundles']
    assert [bundle['pairs'] for bundle in bundles] == ([2] if projection_over == 0 else [2, 1])
    all_rows = []
    cumulative = {'r1': 0, 'r2': 0}
    projected_key = len(CSV_HEADER)
    for bundle in bundles:
        encoded = gzip.decompress((manifest_path.parent / bundle['key']['name']).read_bytes())
        assert encoded.startswith(CSV_HEADER)
        local = {'r1': 0, 'r2': 0}
        for line in encoded[len(CSV_HEADER):].splitlines(keepends=True):
            fields = line.rstrip(b'\n').decode('ascii').split(',')
            sample, ordinal, secure = map(int, fields[:3])
            lines = originals[sample, ordinal]
            assert line == expected_row(lines, sample, ordinal, secure, fields[3], local['r1'], local['r2'])
            projected_key += len(expected_row(lines, sample, ordinal, secure, fields[3],
                                              cumulative['r1'], cumulative['r2']))
            for mate in ('r1', 'r2'):
                size = 83 + 2 * len(lines[1 if mate == 'r1' else 5])
                local[mate] += size
                cumulative[mate] += size
            all_rows.append(line)
        assert local == {mate: bundle[mate]['uncompressed_bytes'] for mate in ('r1', 'r2')}
    projection = projected_key if limiting_lane == 'key' else cumulative[limiting_lane]
    assert projection == safe + projection_over
    for lane in ('r1', 'r2', 'key'):
        if lane != limiting_lane:
            assert (projected_key if lane == 'key' else cumulative[lane]) < safe
    if limiting_lane == 'key' and projection_over:
        # Using the observed new zero-offset row instead of the prospective old
        # offsets would falsely claim the valid producer rolled over too early.
        assert bundles[0]['key']['uncompressed_bytes'] + len(all_rows[-1]) <= safe
    check_artifacts(manifest_path.parent, bundles, cap)
    assert all(path.stat().st_size <= cap for path in manifest_path.parent.iterdir())
    checked = verify(VerifyConfig(manifest=manifest_path, max_ram=512 << 20))
    assert checked.completed and checked.pairs == count


@pytest.mark.parametrize('previous_r1_offset', [99, 101, 999, 1001, 9999, 10001])
def test_P15_T02_rollover_rerenders_decimal_offsets_before_empty_bundle_admission(tmp_path, previous_r1_offset):
    # Real offsets 9/10 are unreachable: the first secured record is >=83 bytes.
    # Neighboring reachable odd offsets straddle the 100/1000/10000 transitions.
    first = source_lines(r1_bases=(previous_r1_offset - 83) // 2)
    second = source_lines(plus_padding=32508)
    expected_new = expected_row(second, 0, 1, 1, '0' * 32, 0, 0)
    expected_old = expected_row(second, 0, 1, 1, '0' * 32, previous_r1_offset, 83)
    assert len(CSV_HEADER) + len(expected_new) == 65279
    assert len(CSV_HEADER) + len(expected_old) > 65280
    budget = Budget(256 << 20)
    keys = OperationKeys._from_test(hashlib.sha256(b'rollover-boundary-fixture').digest(), bytes(range(16)))
    with OutputTransaction.begin(tmp_path, 'manifest', budget, max_artifact_bytes=65564) as transaction:
        with BundleEmitter(transaction, keys, (2,), max_record_bytes=1 << 20,
                           compression_level=0) as emitter:
            for index, lines in enumerate((first, second)):
                frame = struct.pack('<IIQ8I', 48 + sum(map(len, lines)), 0, index, *map(len, lines)) + b''.join(lines)
                emitter.append(OrderedPair(index, 0, index, memoryview(frame)))
            bundles = list(emitter.finish())
        assert [bundle['pairs'] for bundle in bundles] == [1, 1]
        transaction.publish_all(b'{"fixture":"decimal-rollover"}\n')
    actual = gzip.decompress((tmp_path / bundles[1]['key']['name']).read_bytes())
    fields = actual[len(CSV_HEADER):].rstrip(b'\n').decode('ascii').split(',')
    assert fields[4] == fields[6] == '0'
    assert actual == CSV_HEADER + expected_row(second, 0, 1, 1, fields[3], 0, 0)
    check_artifacts(tmp_path, bundles, 65564)
    assert all(path.stat().st_size <= 65564 for path in tmp_path.iterdir())
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
