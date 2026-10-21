#!/usr/bin/env python3
"""Private, complete supplied-data qualification of a frozen installed candidate.

This harness never adds the checkout to Python's import path. Run it using the
isolated candidate interpreter. Public stdout contains aggregates only; source
identities, snapshots and digests remain in a new 0700 validation workspace.
The independent oracle uses bounded stdlib byte reads, never ReadMixer framing.
Failed workspaces are retained for inspection; they are not resumable operations.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager, ExitStack
import gzip
import hashlib
from importlib import metadata
import json
import os
from pathlib import Path
import stat
import sys
import tarfile
import tempfile
import time
import zipfile

import readmixer_py
from readmixer_py import mix, recover, verify
from readmixer_py.catalog import build_catalog
from readmixer_py.config import (MixConfig, RecoverConfig, RecoveryMode, StorageMode,
                                VerifyConfig, parse_size, positive_integer)
from readmixer_py.errors import ConfigurationError, IntegrityError, ResourceError
from readmixer_py.paths import exclude_input_tree

SCOPE = 'complete_private_supplied_data_qualification'
HISTORICAL = dict(samples=48, pairs=30_042_389, reads=60_084_778,
                  normalized_bytes=10_983_150_366, physical_bytes=1_691_881_688)
_MIB = 1 << 20
_ROLES = ('r1', 'r2')


def _digest_file(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as source:
        while block := source.read(_MIB):
            digest.update(block)
    return digest.hexdigest()


def candidate_identity(wheel, wheel_sha256, sdist, sdist_sha256):
    """Bind this harness and every imported package module to exact archives."""
    for path, expected in ((wheel, wheel_sha256), (sdist, sdist_sha256)):
        if (not isinstance(expected, str) or len(expected) != 64 or
                any(char not in '0123456789abcdef' for char in expected) or
                _digest_file(path) != expected):
            raise IntegrityError('candidate archive fingerprint does not match')
    package = Path(readmixer_py.__file__).resolve().parent
    source_tree = Path(__file__).resolve().parents[1] / 'src' / 'readmixer_py'
    if package == source_tree.resolve() or 'site-packages' not in package.parts:
        raise ConfigurationError('qualification requires an outside-checkout installed package')
    checked = set()
    with zipfile.ZipFile(wheel) as archive:
        for name in archive.namelist():
            if name.startswith('readmixer_py/') and name.endswith('.py'):
                relative = Path(name).relative_to('readmixer_py')
                if '..' in relative.parts or archive.read(name) != (package / relative).read_bytes():
                    raise IntegrityError('installed package differs from frozen wheel')
                checked.add(relative.as_posix())
    if not checked or checked != {path.relative_to(package).as_posix() for path in package.rglob('*.py')}:
        raise IntegrityError('installed module inventory differs from frozen wheel')
    harness_digest = _digest_file(__file__)
    with tarfile.open(sdist, 'r:*') as archive:
        members = [item for item in archive.getmembers()
                   if item.name.endswith('/scripts/qualify_supplied_data.py')]
        if len(members) != 1 or not members[0].isfile() or members[0].size > _MIB:
            raise IntegrityError('frozen source archive lacks the qualification harness')
        stream = archive.extractfile(members[0])
        if stream is None or hashlib.sha256(stream.read(_MIB + 1)).hexdigest() != harness_digest:
            raise IntegrityError('qualification harness differs from frozen source archive')
    return dict(version=readmixer_py.__version__, installed_package_matches_wheel=True,
                outside_checkout=True, package_python_files=len(checked),
                wheel_sha256=wheel_sha256, sdist_sha256=sdist_sha256,
                harness_sha256=harness_digest, python=sys.version.split()[0],
                dependencies={name: metadata.version(name)
                              for name in ('numpy', 'pycryptodome', 'psutil')})


def _record(stream, maximum, *, canonical=False):
    """Read precisely four LF-delimited byte fields without universal newlines.

    Ingress drops exactly one CR immediately preceding a consumed LF. A final
    nonempty quality line may lack LF and then retains a terminal standalone CR.
    Restored records require LF and preserve every preceding byte, including CR.
    All reads and the retained four-field tuple are bounded by the record cap.
    """
    fields = []
    normalized = 4
    for number in range(4):
        line = stream.readline(maximum + 2)
        if not line and number == 0:
            return None
        terminated = line.endswith(b'\n')
        if not terminated and (canonical or number != 3 or not line):
            raise IntegrityError('independent oracle found an incomplete physical record')
        field = line[:-1] if terminated else line
        if terminated and not canonical and field.endswith(b'\r'):
            field = field[:-1]
        normalized += len(field)
        if normalized > maximum:
            raise ResourceError('independent oracle record exceeds its admitted limit')
        fields.append(field)
    if not fields[0].startswith(b'@') or not fields[2].startswith(b'+'):
        raise IntegrityError('independent oracle found invalid record markers')
    if len(fields[1]) != len(fields[3]):
        raise IntegrityError('independent oracle sequence and quality lengths differ')
    return tuple(fields)


@contextmanager
def _decoded(raw, advertised_gzip):
    # Buffered peek does not change the logical position or rewind the input.
    compressed = raw.peek(2)[:2] == b'\x1f\x8b'
    if advertised_gzip and not compressed:
        raise IntegrityError('independent oracle expected a gzip member')
    if compressed:
        with gzip.GzipFile(fileobj=raw, mode='rb') as decoded:
            yield decoded
    else:
        yield raw


def _hash_fields(digest, fields):
    for field in fields:
        digest.update(field)
        digest.update(b'\n')
    return sum(map(len, fields)) + 4


def _snapshot(catalog):
    return tuple((sample.sample_id, tuple((str(source.relative_path),
                source.snapshot.device, source.snapshot.inode, source.snapshot.size,
                source.snapshot.mtime_ns, source.snapshot.ctime_ns)
                for source in (sample.r1, sample.r2))) for sample in catalog)


def scan_originals(catalog, maximum, progress=lambda *_: None):
    """EOF scan retaining only per-sample counters/hashes, never record identities."""
    samples = []
    pairs = normalized = nonempty = maximum_seen = 0
    for sample in catalog:
        digests = [hashlib.sha256(), hashlib.sha256()]
        sizes = [0, 0]
        count = 0
        with ExitStack() as stack:
            streams = [stack.enter_context(_decoded(
                stack.enter_context(catalog.open_input(source)), source.advertised_gzip))
                for source in (sample.r1, sample.r2)]
            while True:
                fields = [_record(stream, maximum) for stream in streams]
                if fields == [None, None]:
                    break
                if any(item is None for item in fields):
                    raise IntegrityError('independent oracle found unequal mate counts')
                for role in range(2):
                    length = _hash_fields(digests[role], fields[role])
                    sizes[role] += length
                    maximum_seen = max(maximum_seen, length)
                count += 1
        pairs += count
        normalized += sum(sizes)
        nonempty += bool(count)
        samples.append(dict(sample_index=sample.index, sample_id=sample.sample_id,
                            pairs=count, sizes=sizes,
                            digests=[item.hexdigest() for item in digests]))
        progress(sample.index + 1, pairs, normalized)
    if nonempty < 2:
        raise IntegrityError('complete validation needs two nonempty samples')
    aggregate = dict(samples=len(samples), nonempty_samples=nonempty, pairs=pairs,
                     reads=2 * pairs, normalized_bytes=normalized,
                     physical_bytes=catalog.physical_bytes, max_record_bytes_observed=maximum_seen)
    return samples, aggregate


def _safe_name(directory, name):
    if not isinstance(name, str) or not name or Path(name).name != name or name in ('.', '..'):
        raise IntegrityError('validation output has an invalid generated basename')
    path = directory / name
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise IntegrityError('validation output is not an owned regular file')
    return path


def _restored_records(directory, parts, role, maximum):
    for part in parts:
        path = _safe_name(directory, part[role]['name'])
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(descriptor, 'rb') as raw, gzip.GzipFile(fileobj=raw, mode='rb') as stream:
            count = 0
            while (fields := _record(stream, maximum, canonical=True)) is not None:
                count += 1
                yield fields
            if count != part['pairs']:
                raise IntegrityError('independent oracle found a restored part count mismatch')


def compare_recovered(catalog, report, directory, expected, maximum, progress=lambda *_: None):
    """Compare all fields and ordinal occurrences directly against original EOF."""
    if len(catalog) != len(expected):
        raise IntegrityError('independent oracle sample inventory differs')
    parts_by_sample = [[] for _ in expected]
    prior = -1
    for part in report.outputs:
        index = part.get('sample_index')
        if (type(index) is not int or not 0 <= index < len(expected) or index < prior or
                part.get('part_index') != len(parts_by_sample[index]) or
                type(part.get('pairs')) is not int or part['pairs'] < 0):
            raise IntegrityError('independent oracle found noncanonical sample or part order')
        parts_by_sample[index].append(part)
        prior = index
    count = normalized = 0
    for sample, reference, parts in zip(catalog, expected, parts_by_sample):
        if not parts or sample.index != reference['sample_index'] or sample.sample_id != reference['sample_id']:
            raise IntegrityError('independent oracle lacks a complete sample selection')
        with ExitStack() as stack:
            originals = [stack.enter_context(_decoded(
                stack.enter_context(catalog.open_input(source)), source.advertised_gzip))
                for source in (sample.r1, sample.r2)]
            restored = [_restored_records(directory, parts, role, maximum) for role in _ROLES]
            for iterator in restored:
                stack.callback(iterator.close)
            sizes = [0, 0]
            hashes = [hashlib.sha256(), hashlib.sha256()]
            ordinal = 0
            while True:
                before = [_record(stream, maximum) for stream in originals]
                after = [next(iterator, None) for iterator in restored]
                if before == [None, None] and after == [None, None]:
                    break
                if any(item is None for item in before + after) or before != after:
                    raise IntegrityError('restored fields or numeric occurrence differ from original')
                for role in range(2):
                    sizes[role] += _hash_fields(hashes[role], before[role])
                ordinal += 1
            if (ordinal != reference['pairs'] or sizes != reference['sizes'] or
                    [item.hexdigest() for item in hashes] != reference['digests']):
                raise IntegrityError('original content differs from the admitted private inventory')
        count += ordinal
        normalized += sum(sizes)
        progress(sample.index + 1, count, normalized)
    return dict(fields_and_order_equal=True, samples=len(expected), pairs=count,
                reads=2 * count, normalized_bytes=normalized,
                original_and_restored_streams_consumed_to_eof=True,
                private_inventory_digests_equal=True)


class OwnedFiles:
    """Delete only registered regular-file identities; preserve foreign residue."""
    def __init__(self, directory):
        self.directory = directory
        self.identity = (directory.stat().st_dev, directory.stat().st_ino)
        self.files = {}

    def add(self, name):
        path = _safe_name(self.directory, name)
        info = path.lstat()
        self.files[name] = (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)

    def remove(self):
        try:
            root = self.directory.lstat()
        except FileNotFoundError:
            return True
        if not stat.S_ISDIR(root.st_mode) or (root.st_dev, root.st_ino) != self.identity:
            return False
        complete = True
        for name, expected in self.files.items():
            path = self.directory / name
            try:
                info = path.lstat()
                if (stat.S_ISREG(info.st_mode) and
                        (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns) == expected):
                    path.unlink()
                else:
                    complete = False
            except FileNotFoundError:
                pass
            except OSError:
                complete = False
        try:
            self.directory.rmdir()
        except OSError:
            complete = False
        return complete


def _register_recovery(directory, report):
    owned = OwnedFiles(directory)
    owned.add(report.receipt)
    for part in report.outputs:
        for role in ('r1', 'r2', 'r1_gzi', 'r2_gzi'):
            owned.add(part[role]['name'])
    return owned


def _register_pool(directory, report):
    owned = OwnedFiles(directory)
    path = _safe_name(directory, report.manifest)
    with path.open('rb') as stream:
        content = stream.read((16 << 20) + 1)
    if len(content) > 16 << 20:
        raise IntegrityError('pool manifest exceeds validation metadata bound')
    manifest = json.loads(content)
    owned.add(report.manifest)
    for bundle in manifest['bundles']:
        for role in ('r1', 'r2', 'r1_gzi', 'r2_gzi', 'key', 'index'):
            owned.add(bundle[role]['name'])
    return owned


def _write_private(path, value):
    encoded = json.dumps(value, sort_keys=True, separators=(',', ':')).encode() + b'\n'
    # Fixed-size inventory/journal updates; no per-read progress log grows on disk.
    if len(encoded) > 16 << 20:
        raise ResourceError('private qualification journal exceeds its bound')
    descriptor, temporary = tempfile.mkstemp(prefix='.journal-', dir=path.parent)
    try:
        with os.fdopen(descriptor, 'wb') as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def space_admission(workspace, scratch, inventory, phase):
    """Plan coexistence using free bytes on actual devices, not pathname labels.

    These deliberately conservative planning allowances are not measured peaks
    or mathematical runtime bounds. ReadMixer retains its actual disk admission.
    Recovery modes are sequential, so only one restored copy is reserved.
    """
    decoded, pairs = inventory['normalized_bytes'], inventory['pairs']
    framed = decoded + 40 * pairs
    scratch_allowance = 6 * framed + (128 << 20)
    pool_allowance = 3 * decoded + 1024 * pairs + (32 << 20)
    recovery_allowance = 2 * decoded + 128 * pairs + (32 << 20)
    output_allowance = pool_allowance if phase == 'mix' else (
        recovery_allowance if phase.startswith('recover') else 0)
    if phase == 'initial':
        output_allowance = pool_allowance + recovery_allowance
    devices = {}
    for path, amount, role in ((workspace, output_allowance, 'output'),
                               (scratch, scratch_allowance, 'scratch')):
        info = path.stat()
        free = os.statvfs(path)
        available = free.f_bavail * free.f_frsize
        entry = devices.setdefault(info.st_dev, dict(roles=[], required_bytes=0,
                                                      available_bytes=available))
        entry['roles'].append(role)
        entry['required_bytes'] += amount
        entry['available_bytes'] = min(entry['available_bytes'], available)
    values = list(devices.values())
    if any(item['required_bytes'] > item['available_bytes'] for item in values):
        raise ResourceError('full-data validation lacks planned device headroom')
    return dict(phase=phase, devices=values, shared_scratch_output_device=len(values) == 1,
                storage_schedule='retain pool; recover indexed, compare and remove; recover sequential',
                allowance_scope='conservative planning allowances; actual growth remains runtime-admitted')


def _measure(operation, scratch, output, call):
    from resource_telemetry import ResourceTelemetry
    started = time.perf_counter()
    telemetry = ResourceTelemetry(scratch, output=output, interval=0.1, detail=False)
    try:
        with telemetry:
            result = call()
    except BaseException as exc:
        exc._qualification_observation = dict(operation=operation, completed=False,
            wall_seconds=time.perf_counter() - started, telemetry=telemetry.result,
            error='interrupted' if isinstance(exc, KeyboardInterrupt) else type(exc).__name__)
        raise
    measurement = dict(operation=operation, wall_seconds=time.perf_counter() - started,
                       completed=True, telemetry=telemetry.result)
    for name in ('workers_effective', 'cpu_budget', 'compression_slots', 'physical_bytes',
                 'consumed_artifact_bytes', 'pairs', 'normalized_bytes'):
        if hasattr(result, name):
            measurement[name] = getattr(result, name)
    measurement['actual_ram_budget_bytes'] = result.resources.effective_max_ram
    measurement['terminal_slice_bytes'] = result.resources.terminal_bytes
    return result, measurement


def validate_supplied(input_dir, *, workspace, wheel, wheel_sha256, sdist, sdist_sha256,
                      max_ram=2 << 30, workers=4, max_record_bytes=_MIB,
                      max_artifact_bytes=5_000_000_000, scratch_parent=None,
                      allow_inventory_change=False, retain_pool=False):
    """Execute one complete correctness lane, retaining private failures honestly."""
    positive_integer(workers, 'workers')
    positive_integer(max_ram, 'max_ram')
    positive_integer(max_record_bytes, 'max_record_bytes')
    candidate = candidate_identity(wheel, wheel_sha256, sdist, sdist_sha256)
    input_dir, workspace = Path(input_dir).resolve(), Path(workspace).absolute()
    exclude_input_tree(workspace.resolve(), input_dir)
    scratch_parent = workspace if scratch_parent is None else Path(scratch_parent).resolve()
    exclude_input_tree(scratch_parent, input_dir)
    workspace.mkdir(mode=0o700)
    workspace.chmod(0o700)
    scratch = Path(tempfile.mkdtemp(prefix='readmixer-full-scratch-', dir=scratch_parent))
    scratch.chmod(0o700)
    report = dict(report_version=1, scope=SCOPE, completed=False, candidate=candidate,
                  full_input_validation=True, original_files_unchanged=False,
                  indexed_recovery_equal=False, sequential_recovery_equal=False,
                  historical_inventory=HISTORICAL.copy(), operations=[], comparisons={},
                  disk_admission=[], workspace_cleanup_complete=False,
                  source_names_or_fingerprints_published=False, cache_treatment='unchanged; no cache clearing',
                  performance_comparison=False, automatic_resume=False,
                  validation_passes='original EOF inventory; mix ingress; original EOF comparison per recovery mode')
    private_inventory = workspace / 'private-inventory.json'
    journal = workspace / 'private-progress.json'
    phase = 'inventory'
    snapshot = None
    started = time.perf_counter()

    def checkpoint(**extra):
        _write_private(journal, dict(phase=phase, elapsed_seconds=time.perf_counter() - started,
                                    completed=False, **extra))

    try:
        checkpoint()
        with build_catalog(input_dir=input_dir, max_metadata_bytes=16 << 20) as catalog:
            snapshot = _snapshot(catalog)
            expected, inventory = scan_originals(catalog, max_record_bytes,
                lambda samples, pairs, normalized: checkpoint(samples_complete=samples,
                    pairs_observed=pairs, normalized_bytes_observed=normalized))
            report['inventory'] = inventory
            differences = {key: dict(historical=value, observed=inventory[key])
                           for key, value in HISTORICAL.items() if inventory[key] != value}
            report['historical_aggregate_inventory_matches'] = not differences
            report['inventory_change'] = differences
            report['inventory_change_explicitly_allowed'] = bool(allow_inventory_change)
            _write_private(private_inventory, dict(snapshot=snapshot, samples=expected,
                                                   aggregate=inventory, candidate=candidate))
            if differences and not allow_inventory_change:
                raise IntegrityError('current aggregate inventory differs from the historical local scope')
            report['disk_admission'].append(space_admission(workspace, scratch, inventory, 'initial'))
            selected = tuple(sample.sample_id for sample in catalog)
            pool = workspace / 'pool'
            common = dict(storage=StorageMode.EXTERNAL, max_ram=max_ram, workers=workers,
                          temp_dir=scratch, max_record_bytes=max_record_bytes)
            phase = 'mix'
            checkpoint()
            report['disk_admission'].append(space_admission(workspace, scratch, inventory, phase))
            mixed, observation = _measure(phase, scratch, pool, lambda: mix(MixConfig(
                input_dir=input_dir, output=pool, max_artifact_bytes=max_artifact_bytes, **common)))
            report['operations'].append(observation)
            if not observation['telemetry']['complete'] or not observation['telemetry']['workers']['complete']:
                raise IntegrityError('operation resource observation is incomplete')
            if (not mixed.completed or not mixed.cleanup_complete or
                    (mixed.samples, mixed.pairs, mixed.normalized_bytes) !=
                    (inventory['samples'], inventory['pairs'], inventory['normalized_bytes'])):
                raise IntegrityError('complete mix disagrees with independently observed inventory')
            ingress = observation['telemetry']['io']['original']
            report['mix_ingress'] = {key: ingress[key] for key in ('opens', 'read_bytes', 'read_calls', 'seek_calls')}
            report['mix_ingress']['scope'] = 'actual decoder source streams; discovery/stat descriptor opens excluded'
            report['mix_ingress']['exact_single_forward_pass'] = (
                ingress['opens'] == 2 * inventory['samples'] and
                ingress['read_bytes'] == inventory['physical_bytes'] and ingress['seek_calls'] == 0)
            if not report['mix_ingress']['exact_single_forward_pass']:
                raise IntegrityError('mix source-pass observation is incomplete or differs from inventory')
            pool_owned = _register_pool(pool, mixed)
            manifest = pool / mixed.manifest
            phase = 'verify'
            checkpoint(pool_committed=True)
            report['disk_admission'].append(space_admission(workspace, scratch, inventory, phase))
            verified, observation = _measure(phase, scratch, pool, lambda: verify(VerifyConfig(
                manifest=manifest, **common)))
            report['operations'].append(observation)
            if not observation['telemetry']['complete'] or not observation['telemetry']['workers']['complete']:
                raise IntegrityError('operation resource observation is incomplete')
            if not verified.completed or verified.pairs != inventory['pairs'] or verified.samples != inventory['samples']:
                raise IntegrityError('full pool verification disagrees with inventory')
            report['pool_fully_verified'] = True
            for mode in (RecoveryMode.INDEXED, RecoveryMode.SEQUENTIAL):
                phase = 'recover_' + mode.value
                checkpoint(pool_committed=True, pool_fully_verified=True)
                report['disk_admission'].append(space_admission(workspace, scratch, inventory, phase))
                destination = workspace / phase
                recovered, observation = _measure(phase, scratch, destination, lambda: recover(RecoverConfig(
                    manifest=manifest, output=destination, sample=selected, io=mode,
                    max_artifact_bytes=max_artifact_bytes, **common)))
                report['operations'].append(observation)
                if not observation['telemetry']['complete'] or not observation['telemetry']['workers']['complete']:
                    raise IntegrityError('recovery resource observation is incomplete')
                if (not recovered.completed or not recovered.cleanup_complete or
                        (recovered.samples, recovered.pairs, recovered.normalized_bytes) !=
                        (inventory['samples'], inventory['pairs'], inventory['normalized_bytes'])):
                    raise IntegrityError('complete recovery disagrees with admitted inventory')
                owned = _register_recovery(destination, recovered)
                phase = 'compare_' + mode.value
                checkpoint()
                compare_started = time.perf_counter()
                comparison = compare_recovered(catalog, recovered, destination, expected, max_record_bytes,
                    lambda samples, pairs, normalized: checkpoint(samples_complete=samples,
                        pairs_compared=pairs, normalized_bytes_compared=normalized))
                comparison['wall_seconds'] = time.perf_counter() - compare_started
                report['comparisons'][mode.value] = comparison
                report[mode.value + '_recovery_equal'] = True
                phase = 'cleanup_' + mode.value
                checkpoint()
                if not owned.remove():
                    raise IntegrityError('validation-owned recovery cleanup left preserved residue')
                comparison['owned_recovery_removed_before_next_mode'] = True
            phase = 'final_source_check'
            with build_catalog(input_dir=input_dir, max_metadata_bytes=16 << 20) as current:
                report['original_files_unchanged'] = _snapshot(current) == snapshot
            if not report['original_files_unchanged']:
                raise IntegrityError('source snapshot or inventory changed during full qualification')
            actual_budgets = [item['actual_ram_budget_bytes'] for item in report['operations']]
            report['larger_than_four_times_every_actual_budget'] = inventory['normalized_bytes'] > 4 * max(actual_budgets)
            report['actual_ram_budgets_bytes'] = actual_budgets
            report['pool_artifacts_retained'] = bool(retain_pool)
            phase = 'cleanup_pool'
            checkpoint()
            if not retain_pool and not pool_owned.remove():
                raise IntegrityError('validation-owned pool cleanup left preserved residue')
        scratch.rmdir()  # Never recursively delete unknown or unauthenticated scratch residue.
        if retain_pool:
            _write_private(journal, dict(phase='complete', completed=True,
                                        pool_retained=True, elapsed_seconds=time.perf_counter() - started))
        else:
            private_inventory.unlink()
            journal.unlink()
            workspace.rmdir()
        report['workspace_cleanup_complete'] = not retain_pool
        report['completed'] = True
    except BaseException as exc:
        # Do not serialize exception text, chained tracebacks, source paths or IDs.
        report['error'] = 'interrupted' if isinstance(exc, KeyboardInterrupt) else type(exc).__name__
        report['failed_phase'] = phase
        observation = getattr(exc, '_qualification_observation', None)
        if observation is not None:
            report['operations'].append(observation)
        report['private_workspace_retained'] = workspace.exists()
        if snapshot is not None:
            try:
                with build_catalog(input_dir=input_dir, max_metadata_bytes=16 << 20) as current:
                    report['original_files_unchanged'] = _snapshot(current) == snapshot
            except Exception:
                report['original_snapshot_check_failed'] = True
        try:
            _write_private(journal, dict(phase=phase, completed=False, error=report['error'],
                                        elapsed_seconds=time.perf_counter() - started))
        except Exception:
            report['private_journal_update_failed'] = True
    report['wall_seconds'] = time.perf_counter() - started
    return report


class _Parser(argparse.ArgumentParser):
    def error(self, _message):
        self.exit(2, 'invalid qualification arguments; use --help\n')


def main(argv=None):
    parser = _Parser(description=__doc__)
    for name in ('input', 'workspace', 'wheel', 'sdist'):
        parser.add_argument('--' + name, required=True, type=Path)
    parser.add_argument('--wheel-sha256', required=True)
    parser.add_argument('--sdist-sha256', required=True)
    parser.add_argument('--max-ram', type=parse_size, default=2 << 30)
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--max-record-bytes', type=parse_size, default=_MIB)
    parser.add_argument('--max-artifact-bytes', type=parse_size, default=5_000_000_000)
    parser.add_argument('--scratch-parent', type=Path)
    parser.add_argument('--allow-inventory-change', action='store_true',
                        help='explicitly record a different current dataset scope')
    parser.add_argument('--retain-pool', action='store_true')
    args = vars(parser.parse_args(argv))
    args['input_dir'] = args.pop('input')
    try:
        report = validate_supplied(**args)
    except BaseException as exc:
        report = dict(completed=False, scope=SCOPE,
                      error='interrupted' if isinstance(exc, KeyboardInterrupt) else type(exc).__name__)
    print(json.dumps(report, sort_keys=True))
    return 0 if report['completed'] else (130 if report.get('error') == 'interrupted' else 1)


if __name__ == '__main__':
    raise SystemExit(main())
