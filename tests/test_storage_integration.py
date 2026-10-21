"""Synthetic composition of foundation primitives, not the future mix format.

A caller-defined deterministic numeric order exercises protected sorting. These
artifacts deliberately are test-only canonical FASTQ, numeric identities and a
custom completion marker; they make no public bundle/shuffle compatibility claim.
"""
from contextlib import ExitStack
import gzip
import hashlib
import json

import pytest

from readmixer_py.crypto import OperationKeys
from readmixer_py.disk import DiskBudget
from readmixer_py.errors import IntegrityError
from readmixer_py.fastq import PairedReader, RecordFramer, open_decoded_stream
from readmixer_py.paircodec import decode_pair_view, encode_pair_into, maximum_frame_bytes
from readmixer_py.resources import Budget
from readmixer_py.sorting import ExternalSorter, IdentityCoverage
from readmixer_py.storage import SegmentStore
from readmixer_py.transaction import OutputTransaction

_COUNTS = (9, 0, 5)
_RECORD_LIMIT = 512


def _fixture(input_root):
    """Keep an independent canonical-byte oracle before generating raw files."""
    input_root.mkdir()
    canonical = {}
    sources = []
    physical_digests = {}
    for sample, count in enumerate(_COUNTS):
        mate_sources = []
        for mate in (1, 2):
            raw = bytearray()
            for ordinal in range(count):
                base = f's{sample}-p{ordinal}'.encode('ascii')
                role = str(mate).encode('ascii')
                header = b'@' + base + b'/' + role + b' ' + role + b':N:0:1 ' + b'opaque\xff' * 20
                # Empty reads are explicit physical lines. Other reads contain
                # opaque NUL and CR; quality ending in standalone CR survives
                # both ingress EOF and canonical-LF rendering.
                sequence = b'' if ordinal == 0 else b'A\x00\r'
                quality = b'' if ordinal == 0 else b'!~\r'
                lines = (header, sequence, b'+full plus metadata\xfe', quality)
                canonical[(sample, ordinal, mate)] = b'\n'.join(lines) + b'\n'
                for line_number, line in enumerate(lines):
                    raw.extend(line)
                    final_quality_at_eof = ordinal == count - 1 and line_number == 3
                    if not final_quality_at_eof:
                        raw.extend(b'\r\n' if line.endswith(b'\r') or (ordinal + mate) % 2 else b'\n')
            compressed = (sample + mate) % 2 == 1
            suffix = '.fastq.gz' if compressed else '.fastq'
            path = input_root / f's{sample}_R{mate}{suffix}'
            if compressed:
                # A valid gzip member boundary may occur in the middle of a
                # physical FASTQ line and must not change normalized payloads.
                split = len(raw) // 2
                payload = gzip.compress(bytes(raw[:split]), mtime=0) + gzip.compress(bytes(raw[split:]), mtime=0)
            else:
                payload = bytes(raw)
            path.write_bytes(payload)
            mate_sources.append((path, compressed))
            physical_digests[path] = hashlib.sha256(payload).digest()
        sources.append(tuple(mate_sources))
    return sources, canonical, physical_digests


def _identity(frame):
    # Independent extraction from the specified PYPAIR1 LE numeric header.
    return int.from_bytes(frame[4:8], 'little'), int.from_bytes(frame[8:16], 'little')


def _ordering(identity):
    sample, ordinal = identity
    return ordinal % 3, -sample, -ordinal


def _run_foundations(root, mode, budget, disk, *, omit_identity=None):
    source_root, output_root, scratch_root = root / 'input', root / 'output', root / 'scratch'
    output_root.mkdir(mode=0o700)
    scratch_root.mkdir(mode=0o700)
    sources, canonical, original_digests = _fixture(source_root)
    identities = [(sample, ordinal) for sample, count in enumerate(_COUNTS) for ordinal in range(count)]
    expected_order = sorted(identities, key=_ordering)
    admitted_order = [identity for identity in expected_order if identity != omit_identity]
    paths = ('foundation-r1.fastq', 'foundation-r2.fastq', 'foundation-identities.tsv')
    expected_artifacts = [
        b''.join(canonical[(sample, ordinal, mate)] for sample, ordinal in expected_order)
        for mate in (1, 2)
    ]
    expected_artifacts.append(b''.join(f'{sample}\t{ordinal}\n'.encode('ascii')
                                       for sample, ordinal in expected_order))
    completion = json.dumps({'test_only': True, 'record_pairs': sum(_COUNTS)}, sort_keys=True).encode('ascii')
    try:
        with OutputTransaction.begin(output_root, 'manifest', budget, disk_budget=disk,
                                     input_root=source_root, completion_basename='foundation-complete.json',
                                     max_artifacts=4) as transaction:
            artifacts = [transaction.create_partial(name) for name in paths]
            digests = [hashlib.sha256() for _ in paths]
            lengths = [0, 0, 0]
            with SegmentStore(mode, budget,
                              OperationKeys._from_test(bytes(range(32)), bytes(range(16))),
                              temp_dir=scratch_root, input_root=source_root, disk_budget=disk) as store:
                with ExternalSorter(store, key=lambda frame: _ordering(_identity(frame)),
                                    run_bytes=1024, max_record_bytes=maximum_frame_bytes(_RECORD_LIMIT),
                                    fan_in=2, sort_purpose='foundation-integration') as sorter:
                    reports = []
                    with budget.acquire('test-input-codecs', 8 << 20, descriptors=2):
                        with budget.acquire('test-pair-arena', maximum_frame_bytes(_RECORD_LIMIT)):
                            arena = bytearray(maximum_frame_bytes(_RECORD_LIMIT))
                            for sample, mate_sources in enumerate(sources):
                                with ExitStack() as stack:
                                    framers = []
                                    for path, compressed in mate_sources:
                                        raw = stack.enter_context(path.open('rb'))
                                        decoded = stack.enter_context(open_decoded_stream(raw, advertised_gzip=compressed))
                                        framers.append(RecordFramer(decoded, line_mode='ingress',
                                                                    max_record_bytes=_RECORD_LIMIT,
                                                                    read_chunk_bytes=17))
                                    paired = PairedReader(*framers, sample_index=sample)
                                    while (pair := paired.next_pair()) is not None:
                                        written = encode_pair_into(pair, arena, max_record_bytes=_RECORD_LIMIT)
                                        if (pair.sample_index, pair.original_pair_index) != omit_identity:
                                            sorter.append(bytes(memoryview(arena)[:written]))
                                    reports.append(paired.report())
                    assert [report.pair_count for report in reports] == list(_COUNTS)
                    for sample, report in enumerate(reports):
                        assert report.complete
                        for mate in (1, 2):
                            original = b''.join(canonical[(sample, ordinal, mate)]
                                                for ordinal in range(_COUNTS[sample]))
                            assert getattr(report, f'r{mate}_sha256') == hashlib.sha256(original).hexdigest()
                    with IdentityCoverage(store, tuple(report.pair_count for report in reports),
                                          bitset_limit_bytes=None if mode == 'memory' else 0,
                                          run_bytes=700, fan_in=2) as coverage:
                        with sorter.iter_sorted() as frames:
                            observed = 0
                            for frame in frames:
                                decoded = decode_pair_view(frame, max_record_bytes=_RECORD_LIMIT)
                                identity = decoded.sample_index, decoded.original_pair_index
                                assert identity == admitted_order[observed]
                                assert _identity(frame) == identity
                                coverage.add(*identity)
                                for output_number, record in enumerate((decoded.r1, decoded.r2)):
                                    normalized = b''.join(bytes(line) + b'\n' for line in record.lines)
                                    assert normalized == canonical[(*identity, output_number + 1)]
                                    artifacts[output_number].write(normalized)
                                    digests[output_number].update(normalized)
                                    lengths[output_number] += len(normalized)
                                numeric_identity = f'{identity[0]}\t{identity[1]}\n'.encode('ascii')
                                artifacts[2].write(numeric_identity)
                                digests[2].update(numeric_identity)
                                lengths[2] += len(numeric_identity)
                                observed += 1
                            assert observed == len(admitted_order)
                        assert sorter.merge_passes >= 3
                        coverage.finish()
                assert not store._reader_pins
            # All source seals and complete identity coverage have passed, and
            # protected scratch has been removed before final links are made.
            assert not list(scratch_root.iterdir())
            for artifact, length, digest in zip(artifacts, lengths, digests):
                artifact.close()
                transaction.register_closed(artifact, length, digest.digest())
            assert not any((output_root / name).exists() for name in (*paths, 'foundation-complete.json'))
            report = transaction.publish_all(completion)
        assert report.committed and report.cleanup_complete
        assert sorted(path.name for path in output_root.iterdir()) == sorted((*paths, 'foundation-complete.json'))
        for name, expected in zip(paths, expected_artifacts):
            assert (output_root / name).read_bytes() == expected
        assert (output_root / 'foundation-complete.json').read_bytes() == completion
        assert disk.committed_bytes == sum(map(len, expected_artifacts)) + len(completion)
    finally:
        for path, digest in original_digests.items():
            assert hashlib.sha256(path.read_bytes()).digest() == digest


@pytest.mark.parametrize('mode', ['memory', 'external', 'auto'])
def test_paired_foundations_preserve_all_bytes_and_publish_after_coverage(tmp_path, mode):
    budget, disk = Budget(128 << 20, descriptors=64), DiskBudget()
    _run_foundations(tmp_path, mode, budget, disk)
    assert budget.used_bytes == budget.used_descriptors == 0
    assert disk.pending_bytes == 0


@pytest.mark.parametrize('mode', ['memory', 'external', 'auto'])
def test_missing_identity_after_parsing_aborts_all_final_partials(tmp_path, mode):
    budget, disk = Budget(128 << 20, descriptors=64), DiskBudget()
    with pytest.raises(IntegrityError, match='missing|gap'):
        _run_foundations(tmp_path, mode, budget, disk, omit_identity=(0, 4))
    assert not list((tmp_path / 'output').iterdir())
    assert not list((tmp_path / 'scratch').iterdir())
    assert budget.used_bytes == budget.used_descriptors == 0
    assert disk.pending_bytes == disk.committed_bytes == 0
