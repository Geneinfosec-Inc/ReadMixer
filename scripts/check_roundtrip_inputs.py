#!/usr/bin/env python3
"""Development-only, bounded-prefix real-data mix/verify/recovery check.

At most 128 pairs per admitted sample are copied into private temporary fixtures.
The source files are read only. This is NOT a full-source validation/roundtrip.
Only aggregate counts and success flags are emitted; private source identifiers,
records, digests and ephemeral keys are never printed. All temporary fixture,
pool and recovery files are removed before a success report is returned.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))

from readmixer_py import mix, recover, verify
from readmixer_py.catalog import build_catalog
from readmixer_py.config import MixConfig, RecoverConfig, RecoveryMode, StorageMode, VerifyConfig, positive_integer
from readmixer_py.errors import ConfigurationError, InputError, IntegrityError, ReadMixerError
from readmixer_py.fastq import PairedReader, RecordFramer, open_decoded_stream
from readmixer_py.paths import exclude_input_tree
from readmixer_py.resources import checked_add

_SCOPE = 'bounded_prefix_mix_verify_recovery_check'
_RECORD_BYTES = 1 << 20
_RAM_BYTES = 512 << 20
_ARTIFACT_BYTES = 1 << 20


class _SafeArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        self.exit(2, 'invalid validation arguments; use --help\n')


def _private_output(path):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    return os.fdopen(descriptor, 'wb')


def _make_fixtures(catalog, directory, pairs_per_sample):
    """Compute the oracle directly from source parser fields before production mix."""
    expected = []
    normalized = pairs = nonempty = 0
    for sample in catalog:
        alias = f'sample{sample.index:06d}'
        hashes = [hashlib.sha256(), hashlib.sha256()]
        byte_counts = [0, 0]
        sampled = 0
        with _private_output(directory / f'{alias}_R1.fastq') as output1, \
             _private_output(directory / f'{alias}_R2.fastq') as output2:
            with catalog.open_input(sample.r1) as raw1, catalog.open_input(sample.r2) as raw2:
                with open_decoded_stream(raw1, advertised_gzip=sample.r1.advertised_gzip) as stream1, \
                     open_decoded_stream(raw2, advertised_gzip=sample.r2.advertised_gzip) as stream2:
                    reader = PairedReader(
                        RecordFramer(stream1, line_mode='ingress', max_record_bytes=_RECORD_BYTES),
                        RecordFramer(stream2, line_mode='ingress', max_record_bytes=_RECORD_BYTES),
                        sample_index=sample.index)
                    for _ in range(pairs_per_sample):
                        pair = reader.next_pair()
                        if pair is None:
                            break
                        for role, mate, output in ((0, pair.r1, output1), (1, pair.r2, output2)):
                            for field in (mate.header, mate.sequence, mate.plus, mate.quality):
                                hashes[role].update(field)
                                hashes[role].update(b'\n')
                                byte_counts[role] = checked_add(byte_counts[role], len(field) + 1)
                                output.write(field)
                                # Later ingress strips exactly this added CR,
                                # preserving any CR already present in a field.
                                output.write(b'\r\n')
                        sampled += 1
                        del pair, mate, field
                    del reader
        expected.append((alias, sampled, tuple(byte_counts), tuple(digest.digest() for digest in hashes)))
        pairs = checked_add(pairs, sampled)
        normalized = checked_add(normalized, sum(byte_counts))
        nonempty += sampled > 0
    if nonempty < 2:
        raise InputError('roundtrip prefix check requires two nonempty admitted samples')
    return expected, pairs, normalized, nonempty


def _check_recovered(report, directory, expected):
    """Use independent stdlib gzip plus raw-byte hashes/newline counts as oracle."""
    states = [[hashlib.sha256(), hashlib.sha256(), [0, 0], [0, 0], 0] for _ in expected]
    prior_sample = -1
    for part in report.outputs:
        index = part['sample_index']
        if type(index) is not int or not 0 <= index < len(expected) or index < prior_sample:
            raise IntegrityError('roundtrip outputs have invalid numeric sample ordering')
        prior_sample = index
        state = states[index]
        if part['part_index'] != state[4]:
            raise IntegrityError('roundtrip outputs have invalid numeric part ordering')
        state[4] += 1
        for number, role in enumerate(('r1', 'r2')):
            path = directory / part[role]['name']
            with gzip.open(path, 'rb') as stream:
                while chunk := stream.read(65536):
                    state[number].update(chunk)
                    state[2][number] = checked_add(state[2][number], len(chunk))
                    state[3][number] = checked_add(state[3][number], chunk.count(b'\n'))
    for state, (_alias, pairs, sizes, digests) in zip(states, expected):
        if (state[4] < 1 or tuple(state[2]) != sizes or state[3] != [4 * pairs, 4 * pairs]
                or tuple(state[number].digest() for number in range(2)) != digests):
            raise IntegrityError('roundtrip normalized output differs from independently observed source prefix')


def validate_prefix(input_dir: Path, *, pairs_per_sample=128, workers=4):
    positive_integer(pairs_per_sample, 'pairs per sample', 128)
    positive_integer(workers, 'workers')
    expected = None
    private_path = None
    temp_parent = Path(tempfile.gettempdir()).resolve()
    with build_catalog(input_dir=input_dir, max_metadata_bytes=16 << 20) as catalog:
        exclude_input_tree(temp_parent, catalog.root)
        with tempfile.TemporaryDirectory(prefix='readmixer-roundtrip-', dir=temp_parent) as private:
            private_path = Path(private)
            exclude_input_tree(private_path, catalog.root)
            private_path.chmod(0o700)
            fixtures = private_path / 'input'
            fixtures.mkdir(mode=0o700)
            expected, pairs, normalized, nonempty = _make_fixtures(catalog, fixtures, pairs_per_sample)
            samples = len(expected)
            catalog.close()
            pool_dir = private_path / 'pool'
            mixed = mix(MixConfig(input_dir=fixtures, output=pool_dir, storage=StorageMode.EXTERNAL,
                                  max_ram=_RAM_BYTES, workers=workers, max_artifact_bytes=_ARTIFACT_BYTES))
            if (not mixed.completed or not mixed.cleanup_complete or mixed.samples != samples
                    or mixed.pairs != pairs or mixed.normalized_bytes != normalized):
                raise IntegrityError('roundtrip mix report differs from independently observed prefix totals')
            manifest = pool_dir / mixed.manifest
            verified = verify(VerifyConfig(manifest=manifest, storage=StorageMode.EXTERNAL,
                                            max_ram=_RAM_BYTES, workers=workers))
            if not verified.completed or verified.pairs != pairs or verified.samples != samples:
                raise IntegrityError('roundtrip full-pool verification disagrees with source prefix totals')
            selected = tuple(item[0] for item in expected)
            recovery_parts = recovery_artifacts = 0
            for mode in (RecoveryMode.INDEXED, RecoveryMode.SEQUENTIAL):
                destination = private_path / ('recover-' + mode.value)
                report = recover(RecoverConfig(manifest=manifest, output=destination, sample=selected, io=mode,
                                               storage=StorageMode.EXTERNAL, max_ram=_RAM_BYTES, workers=workers,
                                               max_artifact_bytes=_ARTIFACT_BYTES))
                if (not report.completed or not report.cleanup_complete or report.pairs != pairs
                        or report.samples != samples or report.normalized_bytes != normalized):
                    raise IntegrityError('roundtrip recovery report differs from independently observed prefix totals')
                _check_recovered(report, destination, expected)
                recovery_parts += report.parts
                recovery_artifacts += report.artifacts
            report = dict(completed=True, scope=_SCOPE, full_input_validation=False,
                          source_files_modified=False, pairs_per_sample_limit=pairs_per_sample,
                          samples=samples, nonempty_prefixes=nonempty, pairs=pairs, reads=2*pairs,
                          normalized_bytes=normalized, prefix_pool_fully_verified=True,
                          indexed_recovery_equal=True, sequential_recovery_equal=True,
                          independent_gzip_hashes_and_counts_equal=True, external_storage=True,
                          workers_requested=workers, mix_compression_slots=mixed.compression_slots,
                          pool_bundles=mixed.bundles, pool_artifacts=mixed.artifacts,
                          recovery_parts_total=recovery_parts, recovery_artifacts_total=recovery_artifacts)
    if private_path is None or private_path.exists():
        raise IntegrityError('roundtrip private fixture cleanup did not complete')
    report['private_temp_cleanup_complete'] = True
    return report


def main(argv=None):
    parser = _SafeArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--input', required=True, type=Path)
    parser.add_argument('--pairs-per-sample', type=int, default=128, help='prefix bound in 1..128 (default128)')
    parser.add_argument('--workers', type=int, default=4, help='CPU upper bound (default4)')
    arguments = parser.parse_args(argv)
    try:
        report = validate_prefix(arguments.input, pairs_per_sample=arguments.pairs_per_sample, workers=arguments.workers)
    except KeyboardInterrupt:
        print(json.dumps(dict(completed=False, scope=_SCOPE, error='interrupted'), sort_keys=True))
        return 130
    except Exception as exc:
        # Development evidence must never expose chained private paths or
        # record material, including unexpected implementation failures.
        print(json.dumps(dict(completed=False, scope=_SCOPE, error=type(exc).__name__), sort_keys=True))
        return 1
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
