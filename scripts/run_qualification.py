#!/usr/bin/env python3
"""Execute frozen Plan 16 cells against an isolated installed candidate.

Each cell writes an exclusive result. Re-running requires a new evidence directory;
failed observations are retained and never discarded from performance summaries.
An exclusive private JSONL journal fsyncs each finished workflow observation.
Interruption may leave an incomplete final line; prior complete lines remain
evidence, never a completed qualification or an automatic-resume checkpoint.
The private full supplied-data task is a separate, non-repeated runner.
"""
from __future__ import annotations

import argparse
import hashlib
from importlib import metadata
import json
import os
from pathlib import Path
import shutil
import re
import tarfile
import sys
import tempfile
import time

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
from benchmark_pipeline import benchmark
from generate_fixtures import FixtureConfig
from qualification_host import host_snapshot, run_host, scratch_snapshot, verified_controls
from qualification_matrix import qualification_matrix, validate_result
from qualify_supplied_data import candidate_identity
from readmixer_py.config import StorageMode


def _write_new(path, value):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, 'w') as sink:
        json.dump(value, sink, sort_keys=True)
        sink.write('\n')
        sink.flush()
        os.fsync(sink.fileno())


class _ObservationJournal:
    """Private append-only evidence for one invocation, with no resume behavior.

    Only newline-terminated records are readable after an interrupted write.
    The declaration and rows are fsynced before execution proceeds. A failed
    append poisons the sink: no later marker can disguise a partial final line.
    """
    def __init__(self, path, declaration):
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        self._sink = os.fdopen(descriptor, 'w')
        self._failed = False
        self._fixture_written = False
        self.observations = 0
        try:
            self.append(dict(kind='declaration', journal_version=1,
                observation_scope='completed workflow iterations after owned cleanup; startup lanes excluded',
                resume_supported=False, **declaration))
        except BaseException:
            try:
                self._sink.close()
            except OSError:
                pass
            raise

    def append(self, record):
        if self._failed:
            raise OSError('qualification observation journal is incomplete')
        try:
            json.dump(record, self._sink, sort_keys=True)
            self._sink.write('\n')
            self._sink.flush()
            os.fsync(self._sink.fileno())
        except BaseException:
            self._failed = True
            raise

    def observe(self, kind, index, row, fixture):
        if not self._fixture_written:
            self.append(dict(kind='fixture', fixture=fixture))
            self._fixture_written = True
        self.append(dict(kind='observation', phase=kind, index=index, observation=row))
        self.observations += 1

    def __enter__(self):
        return self

    def __exit__(self, error_type, error, traceback):
        try:
            if error is not None and not self._failed:
                try:
                    self.append(dict(kind='interrupted' if isinstance(error, KeyboardInterrupt) else 'aborted',
                        completed=False, completed_observations=self.observations,
                        diagnostic_text_published=False))
                except BaseException:
                    error.add_note('qualification observation journal final marker also failed')
        finally:
            try:
                self._sink.close()
            except OSError:
                if error is None:
                    raise
                error.add_note('qualification observation journal close also failed')
        return False


def _observed_host(cell, controls, scratch_parent):
    snapshot = host_snapshot()
    if controls.get('scratch_mount') and scratch_parent:
        snapshot.update(scratch_snapshot(scratch_parent))
    checked = verified_controls(controls, snapshot)
    required = cell.get('host_requirements', {})
    available = shutil.disk_usage(tempfile.gettempdir()).free
    sufficient = available >= required.get('free_workspace_bytes', 0)
    distinct = None
    if scratch_parent:
        distinct = os.stat(scratch_parent).st_dev != os.stat(tempfile.gettempdir()).st_dev
    return dict(snapshot=snapshot, verified=checked['verified'] and sufficient,
                controls=checked, available_workspace_bytes=available,
                distinct_scratch_output_devices=distinct)


def bind_environment(sdist, dependency_lock):
    """Bind executed tools/lock to the candidate and observe every exact pin.

    Archive installation hashes are supplied by Plan 17A's environment build;
    this runtime check verifies the immutable lock and actual selected versions.
    """
    lock = Path(dependency_lock).read_bytes()
    if len(lock) > 1 << 20:
        raise ValueError('qualification dependency lock exceeds its bound')
    pins = {}
    pattern = re.compile(r'([A-Za-z0-9_.-]+)==([^ \t]+) --hash=sha256:([a-f0-9]{64})')
    for line in lock.decode('utf-8').splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        match = pattern.fullmatch(line)
        if match is None:
            raise ValueError('qualification lock needs exact versions and selected wheel hashes')
        name, version, _ = match.groups()
        normalized = re.sub(r'[-_.]+', '-', name).lower()
        if normalized in pins or metadata.version(name) != version:
            raise ValueError('installed dependency version differs from immutable lock')
        pins[normalized] = version
    if not {'numpy', 'pycryptodome', 'psutil'} <= pins.keys():
        raise ValueError('qualification lock omits runtime dependencies')
    tools = {path.name: path.read_bytes() for path in _HERE.glob('*.py')}
    artifacts = {**{'scripts/' + name: value for name, value in tools.items()},
                 Path(dependency_lock).name: lock}
    with tarfile.open(sdist, 'r:*') as archive:
        members = archive.getmembers()
        for name, expected in artifacts.items():
            matches = [item for item in members if item.name.endswith('/' + name)]
            if len(matches) != 1 or not matches[0].isfile() or matches[0].size != len(expected):
                raise ValueError('frozen source archive lacks exact qualification inputs')
            stream = archive.extractfile(matches[0])
            if stream is None or stream.read(len(expected) + 1) != expected:
                raise ValueError('executed qualification input differs from frozen source archive')
    return dict(dependency_lock_sha256=hashlib.sha256(lock).hexdigest(),
                installed_dependencies=pins, installed_dependency_versions_match_lock=True,
                qualification_tooling_sha256={name: hashlib.sha256(value).hexdigest() for name, value in tools.items()},
                qualification_tooling_matches_sdist=True)


def run_cell(cell, candidate, *, host_controls=None, scratch_parent=None, observation_sink=None):
    from qualification_matrix import qualify_observation
    controls = dict(cell['private_controls'])
    for name in ('memory_cgroup_bytes', 'cpu_quota', 'rlimit_nofile',
                 'scratch_device_quota_bytes', 'require_distinct_devices'):
        controls.pop(name, None)
    if scratch_parent:
        controls['scratch_parent'] = str(scratch_parent)
    if controls.get('defer_scratch_space_refusal'):
        if scratch_parent is None or not (host_controls or {}).get('scratch_mount'):
            raise ValueError('actual ENOSPC requires a verified dedicated scratch mount')
        controls['scratch_space_probe_path'] = str(Path(scratch_parent).resolve(strict=True))
    observation = _observed_host(cell, host_controls or {}, scratch_parent)
    if cell['private_controls'].get('require_distinct_devices'):
        observation['verified'] &= observation['distinct_scratch_output_devices'] is True
    if not observation['verified']:
        return dict(completed=False, workflow_completed=False, executed=False,
            matrix_cell_id=cell['id'], matrix_definition_sha256=qualification_matrix()['definition_sha256'],
            candidate=candidate, host=observation, error='host_controls_or_capacity_unverified',
            warmup=None, measured_runs=[], summary=None)
    config = dict(cell['fixture'])
    config['counts'] = tuple(config['counts'])
    arguments = cell['configuration']
    started = time.perf_counter()
    report = benchmark(fixture_config=FixtureConfig(**config), workers=arguments['workers_requested'],
        storage=StorageMode(arguments['storage']), max_ram=arguments['max_ram'],
        max_artifact_bytes=arguments['max_artifact_bytes'], compression_level=arguments['compression_level'],
        selected=tuple(arguments['selected_sample_indices']), repetitions=cell['measured_runs'],
        telemetry=True, private_options=controls, observation_sink=observation_sink)
    observed_configuration = {key: report['configuration'].get(key) for key in arguments}
    if isinstance(observed_configuration.get('selected_sample_indices'), (tuple, list)):
        observed_configuration['selected_sample_indices'] = list(observed_configuration['selected_sample_indices'])
    report['benchmark_configuration'] = report['configuration']
    report.update(matrix_cell_id=cell['id'], matrix_definition_sha256=qualification_matrix()['definition_sha256'],
                  candidate=candidate, configuration=observed_configuration, host=observation,
                  runner_wall_seconds=time.perf_counter()-started)
    translated = []
    for row in (report['warmup'], *report['measured_runs']):
        row = qualify_observation(cell, row, report['fixture'], host_verified=observation['verified'])
        translated.append(row)
        witnesses = row['qualification']['witnesses']
        if observation['distinct_scratch_output_devices'] is not None:
            witnesses['distinct_scratch_output_devices'] = observation['distinct_scratch_output_devices']
        if 'nofile' in (host_controls or {}):
            witnesses['descriptor_limit_observed'] = observation['snapshot']['nofile'][0]
    report['warmup'], report['measured_runs'] = translated[0], translated[1:]
    report['acceptance'] = validate_result(cell, report)
    # Cell completion means its declared acceptance, including expected refusal.
    report['workflow_completed'] = report['completed']
    report['completed'] = report['acceptance']['completed']
    if not report['completed']:
        report['summary'] = None
    return report


def _main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--wheel', required=True, type=Path)
    parser.add_argument('--sdist', required=True, type=Path)
    parser.add_argument('--wheel-sha256', required=True)
    parser.add_argument('--sdist-sha256', required=True)
    parser.add_argument('--dependency-lock', required=True, type=Path)
    parser.add_argument('--results', required=True, type=Path)
    parser.add_argument('--cell', action='append')
    parser.add_argument('--tier', action='append', choices=('bounded', 'scale', 'controlled-host', 'release-host'))
    parser.add_argument('--inside-host', type=Path)
    args = parser.parse_args(argv)
    candidate = candidate_identity(args.wheel, args.wheel_sha256, args.sdist, args.sdist_sha256)
    candidate.update(bind_environment(args.sdist, args.dependency_lock))
    matrix = qualification_matrix()
    names = set(args.cell or [])
    if names - {row['id'] for row in matrix['cells']}:
        parser.error('unknown cell')
    chosen = [cell for cell in matrix['cells'] if cell['id'] in names or cell['tier'] in (args.tier or [])]
    if not chosen:
        parser.error('select at least one cell or tier')
    if args.inside_host is not None and len(chosen) != 1:
        parser.error('inside-host execution requires exactly one cell')
    args.results.mkdir(mode=0o700, exist_ok=args.inside_host is not None)
    if args.inside_host:
        context = json.loads(args.inside_host.read_text())
        cell = chosen[0]
        journal_path = args.results / (cell['id'] + '.observations.jsonl')
        declaration = dict(candidate=candidate, matrix_cell=cell,
                           matrix_definition_sha256=matrix['definition_sha256'])
        with _ObservationJournal(journal_path, declaration) as journal:
            result = run_cell(cell, candidate, host_controls=context['host_controls'],
                              scratch_parent=context.get('scratch_parent'), observation_sink=journal.observe)
            journal.append(dict(kind='observations_finished',
                                completed_observations=journal.observations,
                                qualification_requires_final_report=True))
        # Only publish a complete result after every journal flush and close has
        # succeeded. A checkpoint failure cannot leave a successful final report.
        _write_new(args.results / (cell['id'] + '.json'), result)
        return 0 if result['completed'] else 1
    _write_new(args.results/'matrix.json', matrix)
    for cell in chosen:
        controls = cell['private_controls']
        limits = {}
        if 'memory_cgroup_bytes' in controls:
            limits['memory_bytes'] = controls['memory_cgroup_bytes']
        if 'cpu_quota' in controls:
            limits['cpu_quota'] = controls['cpu_quota']
        if 'rlimit_nofile' in controls:
            limits['nofile'] = controls['rlimit_nofile']
        # Every cell has recorded, isolated process-group CPU and RAM controls.
        # The physical-cap fixture alone writes over 10 GiB of plain input.
        # Its reclaimable file cache also enters memory.current; an 8 GiB
        # cgroup leaves no conservatively admissible application headroom.
        # This host envelope is separate from the cell's unchanged RAM budget.
        limits.setdefault('memory_bytes', (64 if cell['tier'] == 'release-host' else 8) << 30)
        limits.setdefault('cpu_quota', max(1, cell['configuration']['workers_requested']))
        limits['affinity'] = sorted(os.sched_getaffinity(0))[:limits['cpu_quota']]
        with tempfile.TemporaryDirectory(prefix='readmixer-cell-control-') as temporary:
            context_path = Path(temporary)/'context.json'
            scratch_parent = None
            if controls.get('require_distinct_devices'):
                scratch_parent = tempfile.mkdtemp(prefix='readmixer-device-', dir='/dev/shm')
            if 'scratch_device_quota_bytes' in controls:
                scratch_parent = str(Path(temporary)/'scratch')
                Path(scratch_parent).mkdir(mode=0o700)
                limits['scratch_mount'] = dict(path=scratch_parent, bytes=controls['scratch_device_quota_bytes'])
            _write_new(context_path, dict(host_controls=limits, scratch_parent=scratch_parent))
            command = [sys.executable, str(Path(__file__).resolve()),
                '--wheel', str(args.wheel.resolve()), '--wheel-sha256', args.wheel_sha256,
                '--sdist', str(args.sdist.resolve()), '--sdist-sha256', args.sdist_sha256,
                '--dependency-lock', str(args.dependency_lock.resolve()), '--results', str(args.results.resolve()),
                '--cell', cell['id'], '--inside-host', str(context_path)]
            try:
                outcome = run_host(command, controls=limits, report=args.results/(cell['id']+'.host.json'))
            except Exception as failure:
                outcome = dict(returncode=None, report_written=False, error=type(failure).__name__,
                               diagnostic_text_published=False)
            finally:
                if scratch_parent and controls.get('require_distinct_devices'):
                    try:
                        Path(scratch_parent).rmdir()
                    except OSError:
                        if 'outcome' in locals():
                            outcome['scratch_parent_cleanup_complete'] = False
            _write_new(args.results/(cell['id']+'.launch.json'), outcome)
            print(json.dumps(dict(cell=cell['id'], **outcome)), flush=True)
    reports = [args.results/(cell['id']+'.json') for cell in chosen]
    return 0 if all(path.exists() and json.loads(path.read_text()).get('completed') for path in reports) else 1


def main(argv=None):
    try:
        return _main(argv)
    except KeyboardInterrupt:
        print(json.dumps(dict(completed=False, error='interrupted'), sort_keys=True), flush=True)
        return 130


if __name__ == '__main__':
    raise SystemExit(main())
