#!/usr/bin/env python3
"""Run bounded, native-build-bound Plan17 components/startup/calibration supplements.

The frozen scripts retain warm-up plus five observations. This external adapter
adds exact candidate/environment verification; it never changes runtime charges,
patches archived code, or claims a universal memory envelope or release approval.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import sys
import time

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))
import resource_lane as resource

HARNESS = ('supplement_lane.py', 'resource_lane.py', 'candidate.py', 'lock_environment.py')
LABELS = ('components-startup', 'calibration-default', 'calibration-64mib', 'fresh-cpu1', 'fresh-cpu4')
STAGES = ('parser', 'frame_codec', 'bucket_hash', 'ranks', 'lexsort', 'tag_rendering',
          'scratch_aead_codec', 'bgzf', 'indexed_gather', 'sort_merge')
CASES = ('parser', 'packed_terminal', 'bulk_tags', 'bgzf', 'scratch_aead', 'metadata', 'resident_catalog', 'memory_handoff')
PHASES = {'mix', 'verify', 'recover_indexed', 'recover_sequential'}


def remaining_supplements():
    serial = ['tests/test_serial_artifact_oracle.py::test_P15_T01_serial_six_artifacts_match_complete_independent_reference[' + storage + '-' + str(level) + '-' + size + ']'
              for storage in ('auto', 'external', 'memory') for level in (0, 6) for size in ('tiny', 'rollover')]
    parallel = ['tests/test_parallel_artifact_oracle.py::test_P15_T01_spawned_six_artifacts_match_independent_serial_reference[' + case + ']'
                for case in ('tiny-memory-2', 'tiny-external-4', 'tiny-auto-4', 'rollover-memory-4', 'rollover-external-2', 'rollover-auto-4')]
    return [dict(id='repeated-independent-six-artifact-reference', nodeids=serial + parallel,
                 configurations=18, warmups=18, measured_observations=90, total_observations=108,
                 action='Run these exact archived installed tests once as warm-up and five measured repetitions; retain individual JUnit outcomes and native-build bindings.'),
            dict(id='compression-full-retained-window-charges', cpu_budgets=[2, 4], threads=[1, 3], retained_jobs=[2, 6],
                 configurations=2, warmups=2, measured_observations=10, total_observations=12,
                 action='Adapt the separately preserved Plan16 compression probe to the actual verified native build; measure live precodec threads, full retained results and codec overlap, validate decoded output, join threads and prove Budget zero.'),
            dict(id='historical-larger-fresh-workload-comparison', counts=[256, 128, 0], cpu_budgets=[1, 4],
                 action='The current three-pair probes qualify tiny startup/correctness only. Run the larger profile separately if reproducing the historical workload comparison; do not equate their timings.')]


def _rows(lane, key='measured_runs'):
    measured = lane.get(key)
    if not isinstance(measured, list) or len(measured) != 5 or not isinstance(lane.get('warmup'), dict):
        raise ValueError('exactly one warm-up plus five measured observations required')
    return [lane['warmup'], *measured]


def assess(label, report):
    """Recheck complete protocols and reject failed/missing observations/excesses."""
    errors, count = [], 0
    try:
        if report.get('completed') is not True:
            errors.append('measurement_incomplete')
        if label == 'components-startup':
            if set(report['stages']) != set(STAGES) or set(report['startup']['lanes']) != {'interpreter', 'api_import', 'cli_inspect'}:
                raise ValueError('component/startup inventory mismatch')
            if report.get('scratch_cleanup_complete') is not True or report['startup'].get('completed') is not True:
                errors.append('component_or_startup_cleanup_incomplete')
            for name, lane in report['stages'].items():
                rows = _rows(lane)
                count += len(rows)
                expected = lane['expected']
                for row in rows:
                    if row.get('completed') is not True or row.get('error') is not None or any(row['result'].get(key) != expected[key] for key in ('sha256', 'logical_bytes', 'items')):
                        errors.append('component_correctness:' + name)
            for name, lane in report['startup']['lanes'].items():
                rows = _rows(lane)
                count += len(rows)
                for row in rows:
                    if row.get('completed') is not True or row.get('exit_code') != 0 or row.get('timed_out') is not False or row.get('stderr_bytes') != 0:
                        errors.append('startup_observation:' + name)
                    if name != 'interpreter' and (row['child'].get('candidate_matches') is not True or row['child'].get('completed') is not True):
                        errors.append('startup_candidate:' + name)
        elif label in ('calibration-default', 'calibration-64mib'):
            names = CASES if label == 'calibration-default' else ('parser', 'packed_terminal')
            scale = 1 << 20 if label == 'calibration-default' else 64 << 20
            if [lane['case'] for lane in report['cases']] != list(names) or report['scale_bytes'] != scale or report['pair_count'] != 32768 or report['measured_runs'] != 5:
                raise ValueError('calibration configuration mismatch')
            if report.get('envelope_qualified') is not False:
                errors.append('unsupported_envelope_claim')
            for lane in report['cases']:
                rows = _rows(lane, 'measured')
                count += len(rows)
                if lane.get('observed_charge_excess') is not False:
                    errors.append('sampled_charge_excess:' + lane['case'])
                for row in rows:
                    if row.get('completed') is not True or row.get('child_reaped') is not True or row.get('cleanup_complete') is not True or row.get('sample_errors') != 0:
                        errors.append('calibration_observation:' + lane['case'])
                    if row.get('sampled_growth_exceeds_charge') is not False or row['sampled_positive_rss_growth_bytes'] > row['charged_bytes']:
                        errors.append('sampled_charge_excess:' + lane['case'])
        elif label in ('fresh-cpu1', 'fresh-cpu4'):
            workers = int(label[-1])
            if report['configuration']['workers'] != workers or report['configuration']['max_ram'] != 2 << 30 or report.get('installed_qualification') is not True:
                raise ValueError('fresh workflow configuration mismatch')
            rows = _rows(report)
            count += len(rows)
            for row in rows:
                if row.get('completed') is not True or row.get('exit_code') != 0 or row.get('owned_child_output_cleanup_complete') is not True:
                    errors.append('fresh_child_or_cleanup_failed')
                child = row['child']
                workflow = child['workflow']
                if child.get('candidate_matches') is not True or child.get('completed') is not True or workflow.get('completed') is not True:
                    errors.append('fresh_candidate_or_workflow_failed')
                if not workflow.get('correctness') or not all(value is True for value in workflow['correctness'].values()) or workflow.get('algorithmic_scratch_empty') is not True:
                    errors.append('fresh_independent_correctness_or_cleanup_failed')
                if set(workflow['phases']) != PHASES:
                    errors.append('fresh_phase_inventory_mismatch')
                check = resource.observed_cpu_check({'configuration': {'workers_requested': workers}}, {'warmup': workflow, 'measured_runs': []})
                if not check['passed']:
                    errors.append('fresh_actual_cpu_budget_mismatch')
        else:
            raise ValueError('unknown supplemental lane')
    except (KeyError, TypeError, ValueError):
        errors.append('malformed_or_incomplete_protocol')
    return {'accepted': not errors, 'observations': count, 'reasons': sorted(set(errors)),
            'envelope_qualified': False, 'scope': 'observed fixed probes only; sampled lower bounds are not universal ceilings'}


def _check_harness(expected):
    if {name: resource.fingerprint(HERE / name) for name in HARNESS} != expected:
        raise ValueError('supplement harness changed during execution')


class RetainedObservations:
    """Write returned observations between archived clocks; restore all hooks."""
    def __init__(self, context, candidate):
        self.directory = Path(context['result']).with_name(context['label'] + '.observations')
        self.context, self.candidate = context, candidate
        self.counts, self.originals = {}, []

    def __enter__(self):
        self.directory.mkdir(mode=0o700)
        return self

    def wrap(self, module, name, descriptor):
        original = getattr(module, name)

        def observed(*args, **kwargs):
            case, measurement_path = descriptor(args, kwargs)
            number = self.counts.get(case, 0)
            self.counts[case] = number + 1
            phase, index = ('warmup', 0) if number == 0 else ('measured', number - 1)
            label = 'warmup' if number == 0 else f'measured-{index:04d}'
            def retain(row, returned):
                canonical = json.dumps(row, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode('ascii')
                resource.write_new(self.directory / (case.replace('/', '-') + '-' + label + '.json'),
                    {'schema': 'readmixer.release.supplement-observation.v1',
                     'candidate': resource._candidate_identity(self.candidate, self.context['dependency_lock_sha256']),
                     'environment_manifest_sha256': self.context['environment_manifest_sha256'],
                     'allocator_environment': self.context['allocator_environment'], 'harness': self.context['harness'],
                     'aggregate_result_name': self.context['label'] + '.json',
                     'measurement_path': measurement_path, 'case': case,
                     'configuration': {'id': self.context['label'] + '/' + case},
                     'phase': phase, 'index': index, 'observation_label': label,
                     'observation': row, 'observation_sha256': hashlib.sha256(canonical).hexdigest(),
                     'original_returned': returned, 'post_run_binding_verification_pending': True,
                     'timing_scope': 'Journal serialization/fsync occurs after the archived observation returned and before the next observation; original row clocks exclude this evidence I/O.',
                     'release_qualified': False})
            try:
                row = original(*args, **kwargs)
            except BaseException as error:
                try:
                    retain({'completed': False, 'error_type': type(error).__name__}, False)
                except BaseException:
                    error.add_note('supplement observation retention also failed')
                raise
            # Do not wrap, copy, normalize or replace the original result.
            retain(row, True)
            return row

        self.originals.append((module, name, original))
        setattr(module, name, observed)

    def __exit__(self, *_):
        for module, name, original in reversed(self.originals):
            setattr(module, name, original)


def stage_observation_path(args, kwargs):
    name = args[0].__name__
    if name not in STAGES:
        raise ValueError('unexpected archived component observation')
    return 'stage/' + name, ['measurement', 'stages', name]


def startup_observation_path(args, kwargs):
    name = args[0]
    if name not in ('interpreter', 'api_import', 'cli_inspect'):
        raise ValueError('unexpected archived startup observation')
    return 'startup/' + name, ['measurement', 'startup', 'lanes', name]


def calibration_observation_path(cases):
    def descriptor(args, kwargs):
        name = args[0]
        return 'calibration/' + name, ['measurement', 'cases', cases.index(name)]
    return descriptor


def fresh_observation_path(args, kwargs):
    return 'fresh-workflow', ['measurement']


def _measure(context, candidate):
    label = context['label']
    work = Path(context['work'])
    with RetainedObservations(context, candidate) as journal:
        if label == 'components-startup':
            import benchmark_stages as archived
            import benchmark_pipeline as pipeline
            journal.wrap(archived, '_observe', stage_observation_path)
            journal.wrap(pipeline, '_startup_observation', startup_observation_path)
            measurement = archived.benchmark_stages(pairs=256, repetitions=5, work_dir=work, startup=True)
            invocation = {'script': 'benchmark_stages.py', 'arguments': ['--startup', '--pairs', '256', '--repetitions', '5'], 'api': 'benchmark_stages'}
        elif label in ('calibration-default', 'calibration-64mib'):
            import calibrate_resources as archived
            scale = 1 << 20 if label == 'calibration-default' else 64 << 20
            cases = archived.CASES if label == 'calibration-default' else ('parser', 'packed_terminal')
            journal.wrap(archived, '_child_observation', calibration_observation_path(cases))
            measurement = archived.calibrate(repetitions=5, scale_bytes=scale, pair_count=32768, cases=cases)
            invocation = {'script': 'calibrate_resources.py', 'arguments': ['--runs', '5', '--scale-bytes', str(scale), '--pair-count', '32768'] + ([] if label == 'calibration-default' else ['--case', 'parser', '--case', 'packed_terminal']), 'api': 'calibrate'}
        else:
            import benchmark_fresh as archived
            from generate_fixtures import FixtureConfig
            wheel, sdist = candidate['archives']['wheel'], candidate['archives']['sdist']
            identity = archived._candidate(Path(context['candidate']) / wheel['name'], wheel['sha256'],
                                  Path(context['candidate']) / sdist['name'], sdist['sha256'])
            identity.update(candidate_id=candidate['candidate_id'], dependency_lock_sha256=context['dependency_lock_sha256'],
                            environment_manifest_sha256=context['environment_manifest_sha256'])
            workers = int(label[-1])
            journal.wrap(archived, '_observation', fresh_observation_path)
            measurement = archived.fresh_benchmark(fixture_config=FixtureConfig(counts=(2, 1, 0)), work_dir=work,
                repetitions=5, workers=workers, storage='external', max_ram=2 << 30, max_artifact_bytes=1 << 20,
                telemetry=True, require_installed=True, candidate=identity)
            invocation = {'script': 'benchmark_fresh.py', 'arguments': ['--counts', '2,1,0', '--workers', str(workers),
                '--storage', 'external', '--max-ram', '2GiB', '--max-artifact-bytes', '1MiB', '--repetitions', '5', '--telemetry'],
                'api': 'unchanged _candidate validator followed by fresh_benchmark'}
    return measurement, invocation


def _child(context_path):
    context = json.loads(Path(context_path).read_bytes())
    _check_harness(context['harness'])
    if any(os.environ.get(name) != '1' for name in resource.NATIVE):
        raise ValueError('native thread controls were not inherited before imports')
    allocator_receipt = resource.verify_allocator_environment(context['allocator_environment'])
    candidate, environment = resource._bind(context, installed=True)
    installed = resource._verify_installed(candidate)
    sys.path.insert(0, context['archived_scripts'])
    label = context['label']
    measurement, invocation = _measure(context, candidate)
    acceptance = assess(label, measurement)
    result_path = Path(context['result'])
    resource.write_new(result_path.with_name(result_path.stem + '.measurement.json'),
        {'schema': 'readmixer.release.supplement-measurement.v1', 'label': label,
         'candidate': resource._candidate_identity(candidate, context['dependency_lock_sha256']),
         'environment_manifest_sha256': context['environment_manifest_sha256'],
         'allocator_environment': allocator_receipt, 'allocator_environment_verified': True,
         'harness': context['harness'], 'invocation': invocation, 'measurement': measurement,
         'acceptance_before_revalidation': acceptance, 'accepted': False,
         'post_run_binding_verification_pending': True, 'release_qualified': False})
    # Recheck exact installed payloads and archives after all measured children.
    resource._bind(context, installed=True)
    _check_harness(context['harness'])
    result = {'schema': 'readmixer.release.supplement-result.v1', 'label': label,
              'candidate': resource._candidate_identity(candidate, context['dependency_lock_sha256']),
              'environment_manifest_sha256': context['environment_manifest_sha256'],
              'installed': installed, 'installed_dependencies': environment['installed_dependency_payloads'],
              'allocator_environment': allocator_receipt, 'allocator_environment_verified': True,
              'harness': context['harness'], 'invocation': invocation,
              'measurement': measurement, 'acceptance': acceptance,
              'completed': measurement.get('completed') is True, 'accepted': acceptance['accepted'],
              'envelope_qualified': False, 'release_qualified': False,
              'fixture_policy': 'fixed bounded public synthetic definitions; no supplied input is used',
              'fresh_workflow_scope': 'Three pairs across counts=(2,1,0): tiny startup/correctness only, not representative throughput or equivalent to historical counts=(256,128,0).',
              'cache_and_timing_scope': 'See unchanged archived measurement report; shared host and existing OS caches, no cold-cache claim. Exclusive observation journals serialize/fsync after each archived row clock closes and before the next call; parent/run elapsed time includes this evidence I/O.'}
    resource.write_new(result_path, result)
    return 0 if acceptance['accepted'] else 1


def run_lane(args):
    candidate_path, environment_path, scripts, work, report = [Path(p).resolve() for p in
        (args.candidate, args.environment, args.archived_scripts, args.workspace, args.report)]
    checkout = HERE.parents[1]
    if work.is_relative_to(checkout) or any(work.is_relative_to(p) or p.is_relative_to(work) for p in (candidate_path, environment_path, scripts, report)):
        raise ValueError('supplement workspace must be separate and outside checkout')
    if report.is_relative_to(candidate_path) or report.is_relative_to(environment_path):
        raise ValueError('report cannot modify the candidate or dependency environment')
    if work.exists() or report.exists():
        raise ValueError('supplement report and workspace must be new')
    context = {'candidate': str(candidate_path), 'environment': str(environment_path),
               'archived_scripts': str(scripts), 'system': platform.system(),
               'allocator_environment': resource.allocator_environment(),
               'environment_manifest_sha256': resource.fingerprint(environment_path / 'environment.json')['sha256'],
               'dependency_lock_sha256': resource.fingerprint(environment_path / 'requirements.lock')['sha256'],
               'harness': {name: resource.fingerprint(HERE / name) for name in HARNESS}}
    candidate, environment = resource._bind(context)
    previous = os.umask(0o077)
    try:
        work.mkdir(mode=0o700)
        report.mkdir(mode=0o700, parents=True)
        private = report / 'private'
        private.mkdir(mode=0o700)
        snapshots = report / 'harness'
        snapshots.mkdir(mode=0o700)
        for name in HARNESS:
            shutil.copyfile(HERE / name, snapshots / name)
        for name in ('environment.json', 'requirements.lock'):
            shutil.copyfile(environment_path / name, report / name)
        declaration = {'schema': 'readmixer.release.supplement-lane.v1',
            'candidate': resource._candidate_identity(candidate, context['dependency_lock_sha256']),
            'environment_manifest_sha256': context['environment_manifest_sha256'],
            'allocator_environment': context['allocator_environment'],
            'harness': context['harness'], 'labels': list(LABELS), 'envelope_qualified': False,
            'release_qualified': False, 'completed': False, 'accepted': False, 'results': [],
            'native_build_supplements_complete': False, 'remaining': remaining_supplements(),
            'observation_retention': 'Exclusive/fsynced per-case journals retain original returned rows after archived clocks close and before the next call; parent/run elapsed includes evidence I/O. Incomplete raised calls retain no invented metrics. Hooks restore on every exit.',
            'fresh_workflow_scope': 'Three-pair startup/correctness probes; no representative-throughput or historical larger-fixture equivalence claim.'}
        resource.write_new(report / 'declaration.json', declaration)
        env = resource.inherited_environment(work)
        for name in tuple(env):
            if name.startswith(('PYTEST', 'READMIXER_', 'P15_')):
                env.pop(name)
        for label in LABELS:
            child_context = dict(context, label=label, work=str(work / label), result=str(report / (label + '.json')))
            context_path = private / (label + '.context.json')
            resource.write_new(context_path, child_context)
            command = [str(Path(args.installed_python).absolute()), '-I', str(HERE / 'supplement_lane.py'), '--child', str(context_path)]
            started_utc = resource.utc_now()
            command_path = private / (label + '.command.json')
            resource.write_new(command_path, resource.command_record(command, env, started_utc))
            started = time.monotonic()
            row = {'label': label, 'status': 'failed', 'command_ref': 'private/' + label + '.command.json',
                   'started_utc': started_utc}
            try:
                with (private / (label + '.stdout')).open('xb') as stdout, (private / (label + '.stderr')).open('xb') as stderr:
                    launch = resource.portable_launch(command, environment=env, stdout=stdout, stderr=stderr, timeout=args.timeout,
                        diagnostic_prefix=private / (label + '.portable-launch'))
                row['launch'] = launch
                result_path = report / (label + '.json')
                if result_path.exists():
                    measured = json.loads(result_path.read_bytes())
                    recomputed = assess(label, measured['measurement'])
                    row['report'] = resource.fingerprint(result_path)
                    row['observations'] = recomputed['observations']
                    row['acceptance_recomputed'] = recomputed
                    if launch['returncode'] == 0 and measured['accepted'] is True and recomputed == measured['acceptance'] and recomputed['accepted']:
                        row['status'] = 'accepted'
            except Exception as error:
                row['error_type'] = type(error).__name__
            row['portable_launch_evidence'] = resource.portable_launch_evidence(private / (label + '.portable-launch'))
            if resource.portable_launch_requires_hold(row['portable_launch_evidence']):
                row.update(status='failed', halted_for_unresolved_launch_cleanup=True)
            row['wall_seconds'] = time.monotonic() - started
            row['ended_utc'] = resource.utc_now()
            execution_path = private / (label + '.execution.json')
            resource.write_new(execution_path, dict(command_ref=label + '.command.json',
                command=resource.fingerprint(command_path), started_utc=started_utc, ended_utc=row['ended_utc'],
                returncode=(row.get('launch') or {}).get('returncode'), error_type=row.get('error_type'), status=row['status']))
            row['execution'] = resource.fingerprint(execution_path)
            for suffix in ('context.json', 'command.json', 'stdout', 'stderr'):
                path = private / (label + '.' + suffix)
                if path.exists():
                    row[suffix.replace('.', '_')] = resource.fingerprint(path)
            declaration['results'].append(row)
            resource.write_new(report / (label + '.launch.json'), row)
            if row.get('halted_for_unresolved_launch_cleanup'):
                break
        _check_harness(context['harness'])
        resource._bind(context)
        declaration['completed'] = len(declaration['results']) == len(LABELS)
        declaration['accepted'] = declaration['completed'] and all(row['status'] == 'accepted' for row in declaration['results'])
        declaration['observations'] = sum(row.get('observations', 0) for row in declaration['results'])
        declaration['limitations'] = ['These observations do not prove universal resident memory bounds.',
            'Signals, constrained hosts, delayed-result windows, scale and physical-cap measurements are separate.',
            'Archive scripts and measured runtime are unchanged; this adapter verifies the actual native dependency lock.',
            'Per-observation journal serialization/fsync occurs between archived observation clocks; parent/run elapsed includes that evidence I/O. OS caches are reused.',
            'No cold-cache, isolated-host, or CPU-speedup claim; keep individual observations and failed attempts.']
        resource.write_new(report / 'supplement-lane.json', declaration)
        return 0 if declaration['accepted'] else 1
    finally:
        os.umask(previous)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate', type=Path)
    parser.add_argument('--environment', type=Path)
    parser.add_argument('--installed-python', type=Path)
    parser.add_argument('--archived-scripts', type=Path)
    parser.add_argument('--workspace', type=Path)
    parser.add_argument('--report', type=Path)
    parser.add_argument('--timeout', type=int, default=3600)
    parser.add_argument('--child', type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.child:
        return _child(args.child)
    if args.timeout < 1 or any(getattr(args, name) is None for name in ('candidate', 'environment', 'installed_python', 'archived_scripts', 'workspace', 'report')):
        parser.error('candidate, environment, installed interpreter, archived scripts, new workspace/report and positive timeout required')
    return run_lane(args)


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Exception as failure:
        print(json.dumps({'completed': False, 'accepted': False, 'error_type': type(failure).__name__,
                          'diagnostic_text_published': False}), file=sys.stderr)
        raise SystemExit(1)
