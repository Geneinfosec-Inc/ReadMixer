#!/usr/bin/env python3
"""Finish native-build supplemental protocols without changing candidate bytes.

Run after ordinary correctness, using the platform lane's installed interpreter
and archived scripts. Each oracle and compression observation has its own fresh
child, JSON, logs, command record and (for pytest) JUnit. Larger fresh workflows
retain their archived warm-up and five measured children. No Linux enforcement,
cold-cache, universal envelope or release-closure claim is made.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import sys
import time
import traceback
import xml.etree.ElementTree as ET

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))
import resource_lane as resource
import supplement_lane as supplement

HARNESS = ('native_supplement_lane.py', 'supplement_lane.py', 'resource_lane.py',
           'candidate.py', 'lock_environment.py')
PROFILES = ('oracle', 'compression', 'fresh')
COMPRESSION_CHECKS = ('all_bgzf_outputs_valid', 'leases_live_until_drain',
                      'all_threads_joined', 'budget_zero')
ORACLE_CACHE_TREATMENT = (
    'Candidate archived pytest setup may collect prior-case unreachable Python objects and, on Darwin, '
    'release allocator free heap pages before each unit test. Exact bound test sources declare this '
    'setup-only treatment; live owners and in-test assertions retain their ordinary behavior. '
    'OS file caches are reused; no cold-cache claim.')


def observation_plan(profile):
    if profile not in (*PROFILES, 'all'):
        raise ValueError('unknown native supplement profile')
    rows = []
    if profile in ('all', 'oracle'):
        for index, nodeid in enumerate(supplement.remaining_supplements()[0]['nodeids']):
            for repetition in range(6):
                rows.append(dict(id=f'oracle-{index:02d}-{repetition}', kind='oracle',
                    nodeid=nodeid, cpus=int(nodeid[-2]) if 'spawned_' in nodeid else 1,
                    warmup_or_measured='warmup' if repetition == 0 else 'measured',
                    repetition=repetition))
    if profile in ('all', 'compression'):
        for cpus in (2, 4):
            for repetition in range(6):
                rows.append(dict(id=f'compression-cpu{cpus}-{repetition}', kind='compression',
                    cpus=cpus, warmup_or_measured='warmup' if repetition == 0 else 'measured',
                    repetition=repetition))
    if profile in ('all', 'fresh'):
        # The archived driver owns six children for each declared CPU setting.
        for cpus in (1, 4):
            rows.append(dict(id=f'fresh-larger-cpu{cpus}', kind='fresh', cpus=cpus,
                             counts=[256, 128, 0], expected_observations=6))
    return rows


def verify_tests(root, candidate):
    root = Path(root).resolve(strict=True)
    if (root / 'src').exists():
        raise ValueError('oracle tests must run outside the source tree')
    expected = {name: value for name, value in candidate['source_manifest'].items()
                if name.startswith('tests/') and name.endswith('.py')}
    actual = {path.relative_to(root).as_posix(): resource.fingerprint(path)
              for path in (root / 'tests').rglob('*.py')}
    if not expected or actual != expected:
        raise ValueError('oracle test bytes differ from the frozen candidate')
    return {'test_files': len(actual), 'all_archived_test_sources_verified': True}


def _check_harness(expected):
    if {name: resource.fingerprint(HERE / name) for name in HARNESS} != expected:
        raise ValueError('native supplemental harness changed during execution')


def assess_oracle(nodeid, result):
    try:
        cpus = int(nodeid[-2]) if 'spawned_' in nodeid else 1
        rows = result['outcomes']
        phases = rows[0]['phases']
        passed = (result['exit_code'] == 0 and result['collected'] == [nodeid]
            and len(rows) == 1 and rows[0]['nodeid'] == nodeid
            and len(phases) == 3 and {row['phase'] for row in phases} == {'setup', 'call', 'teardown'}
            and all(row['outcome'] == 'passed' and row['xfail'] is False for row in phases)
            and result['junit'] == {'executed': 1, 'passed': 1, 'failed': 0, 'errors': 0, 'skipped': 0}
            and result['host_cpu_plan']['effective_limit'] == result['host_cpu_plan']['requested_workers'] == cpus)
    except (KeyError, TypeError, IndexError):
        passed = False
    return dict(accepted=passed, observations=int(isinstance(result.get('outcomes'), list)
        and 'exit_code' in result and 'junit' in result), envelope_qualified=False)


def assess_compression(result, cpus):
    try:
        witnesses, admitted = result['witnesses'], result['admitted']
        snapshots = result['snapshots']
        passed = (result['completed'] is True and result['cpus'] == cpus
            and result['cpu_plan']['effective_limit'] == cpus
            and witnesses['slots'] == cpus - 1 and witnesses['capacity'] == 2 * (cpus - 1)
            and witnesses['concurrent_native_workspaces'] == cpus - 1
            and witnesses['retained_completed_jobs'] == 2 * (cpus - 1)
            and witnesses['retained_result_bytes'] > 0
            and all(witnesses[key] is True for key in COMPRESSION_CHECKS)
            and all(snapshot.get('error') is False and isinstance(snapshot.get('rss_bytes'), int)
                    for snapshot in snapshots.values())
            and set(snapshots) == {'before_service', 'after_service_creation', 'threads_before_codec',
                                  'native_workspaces_live', 'completed_window_retained', 'after_join'}
            and result['observed_precodec_thread_rss_growth_bytes'] <= admitted['thread_bytes']
            and result['observed_combined_positive_rss_growth_bytes'] <= admitted['total_budget_bytes']
            and result['observed_thread_growth_exceeds_thread_charge'] is False
            and result['observed_combined_growth_exceeds_combined_charge'] is False
            and result['original_compressobj_restored'] is True)
    except (KeyError, TypeError, ValueError):
        passed = False
    return dict(accepted=passed, observations=int(bool(result.get('snapshots'))), envelope_qualified=False)


def assess_fresh(result, cpus):
    accepted = supplement.assess(f'fresh-cpu{cpus}', result)
    if result.get('fixture', {}).get('pairs') != 384 or result.get('fixture', {}).get('reads') != 768:
        accepted['accepted'] = False
        accepted['reasons'].append('larger_fresh_fixture_mismatch')
    return accepted


def assess(plan, measurement):
    if plan['kind'] == 'oracle':
        return assess_oracle(plan['nodeid'], measurement)
    if plan['kind'] == 'compression':
        return assess_compression(measurement, plan['cpus'])
    return assess_fresh(measurement, plan['cpus'])


def _oracle(context):
    from readmixer_py.execution import resolve_cpu_budget
    cpus = context['plan'].get('cpus', 1)
    host_cpu_plan = resolve_cpu_budget(cpus)
    if host_cpu_plan.effective_limit != cpus:
        raise ValueError('actual host CPU admission is below oracle topology')
    return dict(_pytest_observation(context), host_cpu_plan=asdict(host_cpu_plan),
        cpu_scope='Independent actual-host preflight. Archived spawned oracle injects fixed CpuObservations to compare topology; its own worker/reaping assertions remain unchanged.',
        test_process_cache_treatment=ORACLE_CACHE_TREATMENT)


def _pytest_observation(context):
    import pytest
    root, work = Path(context['test_root']), Path(context['work'])
    work.mkdir(mode=0o700)
    os.chdir(root)
    nodeid = context['plan']['nodeid']
    collected, outcomes = [], {}

    class Recorder:
        def pytest_collection_finish(self, session):
            collected.extend(item.nodeid for item in session.items)

        def pytest_runtest_logreport(self, report):
            outcomes.setdefault(report.nodeid, {'nodeid': report.nodeid, 'phases': []})['phases'].append(
                dict(phase=report.when, outcome=report.outcome, xfail=bool(getattr(report, 'wasxfail', False)),
                     seconds=report.duration))

    junit = Path(context['junit'])
    code = int(pytest.main([nodeid, '-ra', '-o', 'addopts=', '-o', 'pythonpath=',
        '--junitxml=' + str(junit), '--basetemp=' + str(work / 'pytest')], plugins=[Recorder()]))
    counts = dict(executed=0, passed=0, failed=0, errors=0, skipped=0)
    for case in ET.parse(junit).iter('testcase'):
        counts['executed'] += 1
        outcome = ('failed' if case.find('failure') is not None else 'errors' if case.find('error') is not None
                   else 'skipped' if case.find('skipped') is not None else 'passed')
        counts[outcome] += 1
    return dict(exit_code=code, collected=collected, outcomes=list(outcomes.values()), junit=counts)


def compression_observation(cpus):
    """Portable adaptation of the preserved dev10 full-window gated protocol."""
    import gc
    import resource as limits
    import threading
    import zlib
    import psutil
    from readmixer_py.execution import CompressionService, JOB_BYTES, THREAD_BYTES, resolve_cpu_budget
    from readmixer_py.resources import Budget, ENVELOPE_VERSION
    from readmixer_py.artifacts import bgzf
    from resource_telemetry import _process_snapshot

    if cpus not in (2, 4):
        raise ValueError('compression requires CPU budget two or four')
    plan = resolve_cpu_budget(cpus)
    if plan.effective_limit != cpus:
        raise ValueError('host did not admit declared compression CPU budget')
    count, capacity = cpus - 1, 2 * (cpus - 1)
    payloads = [hashlib.shake_256(b'readmixer-python/compression-supplement/v1/' + str(n).encode()).digest(
                bgzf.PAYLOAD_BYTES) for n in range(capacity)]
    budget = Budget(512 << 20)
    lock, local = threading.Lock(), threading.local()
    before_gate, native_gate = threading.Event(), threading.Event()
    before_ready, native_ready = threading.Event(), threading.Event()
    state = dict(entered=0, native_live=0, max_native_live=0)
    original = zlib.compressobj

    def snapshot():
        row = _process_snapshot()
        row.update(virtual_bytes=psutil.Process().memory_info().vms,
            rlimit_stack_bytes=list(limits.getrlimit(limits.RLIMIT_STACK)),
            threading_stack_size_setting_bytes=threading.stack_size())
        return row

    class GatedCompressor:
        def __init__(self, raw):
            self.raw = raw

        def compress(self, payload):
            result = self.raw.compress(payload)
            if getattr(local, 'hold', False):
                with lock:
                    state['native_live'] += 1
                    state['max_native_live'] = max(state['max_native_live'], state['native_live'])
                    if state['native_live'] == count:
                        native_ready.set()
                try:
                    if not native_gate.wait(30):
                        raise TimeoutError('native workspace gate timed out')
                finally:
                    with lock:
                        state['native_live'] -= 1
            return result

        def flush(self, *args):
            return self.raw.flush(*args)

    def compressobj(*args, **kwargs):
        return GatedCompressor(original(*args, **kwargs))

    def encoder(payload, level):
        with lock:
            ordinal = state['entered']
            state['entered'] += 1
            if state['entered'] == count:
                before_ready.set()
        local.hold = ordinal < count
        if local.hold and not before_gate.wait(30):
            raise TimeoutError('thread entry gate timed out')
        return bgzf.encode_member(payload, level)

    consumed, service, checks, snaps, admitted, error = [], None, {}, {}, {}, None
    snapshot()
    gc.collect()
    snaps['before_service'] = snapshot()
    started = time.perf_counter()
    zlib.compressobj = compressobj
    try:
        service = CompressionService(budget, plan, encoder=encoder, max_lanes=1)
        assert service.slots == count and service.capacity == capacity
        admitted = dict(thread_bytes=service._thread_lease.bytes,
            job_bytes=sum(lease.bytes for lease in service._available), metadata_bytes=service._metadata.bytes,
            total_budget_bytes=budget.used_bytes, thread_charge_constant=THREAD_BYTES, job_charge_constant=JOB_BYTES)
        snaps['after_service_creation'] = snapshot()
        lane = service.open_lane(consumed.append)
        for payload in payloads:
            lane.submit(payload, 6)
        assert before_ready.wait(30)
        assert state['entered'] == count and service.pending_jobs == capacity
        assert not any(job.future.done() for job in service._pending)
        snaps['threads_before_codec'] = snapshot()
        before_gate.set()
        assert native_ready.wait(30)
        assert state['native_live'] == count and not any(job.future.done() for job in service._pending)
        snaps['native_workspaces_live'] = snapshot()
        native_gate.set()
        for job in service._pending:
            job.future.result(timeout=30)
        assert all(job.future.done() for job in service._pending)
        assert service.pending_jobs == capacity and lane.drained == 0 and not consumed
        assert all(not job.lease.released for job in service._pending)
        retained = sum(len(job.future.result()) for job in service._pending)
        snaps['completed_window_retained'] = snapshot()
        for payload, job in zip(payloads, service._pending):
            encoded = job.future.result()
            assert len(encoded) <= 65536 and zlib.decompress(encoded, wbits=31) == payload
        checks.update(slots=count, capacity=capacity, concurrent_native_workspaces=state['max_native_live'],
            retained_completed_jobs=service.pending_jobs, retained_result_bytes=retained,
            leases_live_until_drain=True, all_bgzf_outputs_valid=True)
        lane.finish()
        assert len(consumed) == capacity and service.pending_jobs == 0
        service.close()
        checks.update(all_threads_joined=not any(t.name.startswith('readmixer-zlib') for t in threading.enumerate()),
                      budget_zero=budget.used_bytes == 0)
    except Exception as failure:
        error = type(failure).__name__
        traceback.print_exc(file=sys.stderr)
    finally:
        before_gate.set()
        native_gate.set()
        if service is not None:
            service.abort()
        zlib.compressobj = original
        snaps['after_join'] = snapshot()
    before = snaps['before_service']['rss_bytes']
    thread_growth = (max(0, snaps['threads_before_codec']['rss_bytes'] - snaps['after_service_creation']['rss_bytes'])
        if all(isinstance(snaps.get(name, {}).get('rss_bytes'), int)
               for name in ('threads_before_codec', 'after_service_creation')) else None)
    active_growth = (max(0, max(row['rss_bytes'] for row in snaps.values()) - before)
        if all(isinstance(row.get('rss_bytes'), int) for row in snaps.values()) else None)
    return dict(completed=error is None and all(checks.get(key) is True for key in COMPRESSION_CHECKS),
        error_type=error, cpus=cpus,
        actual_budget=budget.limit_bytes,
        budget_scope='Direct component Budget cap with zero coordinator RSS baseline; no public operation admission claim.',
        cpu_plan=asdict(plan), envelope_version=ENVELOPE_VERSION, wall_seconds=time.perf_counter() - started,
        snapshots=snaps, admitted=admitted, witnesses=checks,
        observed_precodec_thread_rss_growth_bytes=thread_growth,
        observed_combined_positive_rss_growth_bytes=active_growth,
        observed_thread_growth_exceeds_thread_charge=(thread_growth > admitted['thread_bytes']
            if thread_growth is not None and admitted else None),
        observed_combined_growth_exceeds_combined_charge=(active_growth > admitted['total_budget_bytes']
            if active_growth is not None and admitted else None),
        original_compressobj_restored=zlib.compressobj is original)


def _fresh(context, candidate):
    import benchmark_fresh as archived
    from generate_fixtures import FixtureConfig
    wheel, sdist = candidate['archives']['wheel'], candidate['archives']['sdist']
    identity = archived._candidate(Path(context['candidate']) / wheel['name'], wheel['sha256'],
                          Path(context['candidate']) / sdist['name'], sdist['sha256'])
    identity.update(candidate_id=candidate['candidate_id'], dependency_lock_sha256=context['dependency_lock_sha256'],
                    environment_manifest_sha256=context['environment_manifest_sha256'])
    original = archived._observation
    journal = Path(context['fresh_journal'])
    journal.mkdir(mode=0o700)
    def durable_observation(root, *args, **kwargs):
        return record_fresh_observation(journal, identity, context['plan'], original, root, *args, **kwargs)
    archived._observation = durable_observation
    try:
        return archived.fresh_benchmark(fixture_config=FixtureConfig(counts=(256, 128, 0)), work_dir=Path(context['work']),
            repetitions=5, workers=context['plan']['cpus'], storage='external', max_ram=2 << 30,
            max_artifact_bytes=1 << 20, telemetry=True, require_installed=True, candidate=identity)
    finally:
        archived._observation = original


def record_fresh_observation(journal, identity, plan, original, root, *args, **kwargs):
    try:
        row = original(root, *args, **kwargs)
    except Exception as failure:
        traceback.print_exc(file=sys.stderr)
        row = dict(completed=False, error=type(failure).__name__, owned_child_output_cleanup_complete=False)
    # Preserve completed and failed observations even if a later child or
    # the coordinator is interrupted before the archived final report.
    resource.write_new(journal / (root.name + '.json'), dict(candidate=identity,
        configuration=plan, observation_label=root.name, observation=row))
    return row


def _child(path):
    context = json.loads(Path(path).read_bytes())
    _check_harness(context['harness'])
    if any(os.environ.get(name) != '1' for name in resource.NATIVE):
        raise ValueError('native thread controls must precede imports')
    allocator_receipt = resource.verify_allocator_environment(context['allocator_environment'])
    candidate, environment = resource._bind(context, installed=True)
    installed = resource._verify_installed(candidate)
    verify_tests(context['test_root'], candidate)
    sys.path.insert(0, context['archived_scripts'])
    plan = context['plan']
    started = time.monotonic()
    try:
        measurement = (_oracle(context) if plan['kind'] == 'oracle' else
                       compression_observation(plan['cpus']) if plan['kind'] == 'compression' else _fresh(context, candidate))
    except Exception as failure:
        traceback.print_exc(file=sys.stderr)
        measurement = dict(completed=False, error_type=type(failure).__name__)
    acceptance = assess(plan, measurement)
    result_path = Path(context['result'])
    resource.write_new(result_path.with_name(result_path.stem + '.measurement.json'),
        dict(schema='readmixer.release.native-supplement-measurement.v1', plan=plan,
             candidate=resource._candidate_identity(candidate, context['dependency_lock_sha256']),
             environment_manifest_sha256=context['environment_manifest_sha256'], harness=context['harness'],
             allocator_environment=allocator_receipt, allocator_environment_verified=True,
             measurement=measurement, acceptance_before_revalidation=acceptance,
             post_run_binding_verification_pending=True, release_qualified=False))
    resource._bind(context, installed=True)
    verify_tests(context['test_root'], candidate)
    _check_harness(context['harness'])
    resource.write_new(result_path, dict(schema='readmixer.release.native-supplement-result.v1',
        plan=plan, candidate=resource._candidate_identity(candidate, context['dependency_lock_sha256']),
        environment_manifest_sha256=context['environment_manifest_sha256'], installed=installed,
        allocator_environment=allocator_receipt, allocator_environment_verified=True,
        installed_dependencies=environment['installed_dependency_payloads'], harness=context['harness'],
        measurement=measurement, acceptance=acceptance, wall_seconds=time.monotonic() - started,
        accepted=acceptance['accepted'], envelope_qualified=False, release_qualified=False))
    return 0 if acceptance['accepted'] else 1


def run_lane(args):
    candidate_path, environment_path, scripts, work, report = [Path(p).resolve() for p in
        (args.candidate, args.environment, args.archived_scripts, args.workspace, args.report)]
    checkout = HERE.parents[1]
    protected = (checkout, candidate_path, environment_path, scripts.parent)
    if any(work.is_relative_to(path) or path.is_relative_to(work) for path in (*protected, report)):
        raise ValueError('workspace must be separate and outside checkout/candidate/environment/test trees')
    if any(report.is_relative_to(path) or path.is_relative_to(report) for path in (candidate_path, environment_path, scripts.parent)):
        raise ValueError('report must not overlap candidate/environment/test trees')
    if work.exists() or report.exists():
        raise ValueError('native supplement workspace and report must be new')
    context = dict(candidate=str(candidate_path), environment=str(environment_path), archived_scripts=str(scripts),
        test_root=str(scripts.parent), system=platform.system(),
        allocator_environment=resource.allocator_environment(),
        environment_manifest_sha256=resource.fingerprint(environment_path / 'environment.json')['sha256'],
        dependency_lock_sha256=resource.fingerprint(environment_path / 'requirements.lock')['sha256'],
        harness={name: resource.fingerprint(HERE / name) for name in HARNESS})
    candidate, _environment = resource._bind(context)
    verified_tests = verify_tests(scripts.parent, candidate)
    plans = observation_plan(args.profile)
    previous = os.umask(0o077)
    try:
        work.mkdir(mode=0o700)
        report.mkdir(mode=0o700, parents=True)
        private, snapshots = report / 'private', report / 'harness'
        private.mkdir(mode=0o700)
        snapshots.mkdir(mode=0o700)
        for name in HARNESS:
            shutil.copyfile(HERE / name, snapshots / name)
        for name in ('environment.json', 'requirements.lock'):
            shutil.copyfile(environment_path / name, report / name)
        declaration = dict(schema='readmixer.release.native-supplement-lane.v1', profile=args.profile,
            candidate=resource._candidate_identity(candidate, context['dependency_lock_sha256']),
            environment_manifest_sha256=context['environment_manifest_sha256'], harness=context['harness'],
            allocator_environment=context['allocator_environment'],
            verified_tests=verified_tests, plans=plans,
            cache_treatment='OS file caches reused; no clearing or cold-cache claim. Oracle unit-test setup treatment is declared separately; benchmark scripts retain their existing cache protocols.',
            oracle_test_process_cache_treatment=ORACLE_CACHE_TREATMENT,
            compression_protocol='Portable adaptation of preserved dev10_compression_supplement.py; actual live threads/native workspaces and full retained completed-result windows, independent decoding, joins and zero Budget.',
            actual_controls='before-import native thread variables=1; application CPU admission read back; fresh POSIX process groups; no kernel RAM/CPU enforcement claimed',
            limitations=['sampled RSS and explicit live checkpoints cannot prove a universal envelope',
                         'precodec growth includes interpreter/executor bookkeeping; virtual stack limits are separate from resident bytes',
                         'private bounded gates alter scheduling but preserve encoder bytes',
                         'Archived pytest setup may reclaim prior-case unreachable objects/free heap pages before oracle tests; the external adapter adds no collection or allocator-cache intervention to live tests or benchmark resource/supplement scripts.',
                         'fresh fixture generation is excluded from launch intervals; warm-up and OS caches retained'],
            envelope_qualified=False, release_qualified=False)
        resource.write_new(report / 'declaration.json', declaration)
        env = resource.inherited_environment(work)
        for name in tuple(env):
            if name.startswith(('PYTEST', 'READMIXER_', 'P15_')):
                env.pop(name)
        results = []
        for plan in plans:
            label = plan['id']
            child_context = dict(context, plan=plan, work=str(work / label), result=str(report / (label + '.json')),
                                 junit=str(report / (label + '.xml')), fresh_journal=str(report / (label + '.observations')))
            control = private / (label + '.context.json')
            resource.write_new(control, child_context)
            command = [str(Path(args.installed_python).absolute()), '-I', str(HERE / 'native_supplement_lane.py'), '--child', str(control)]
            started = time.time()
            resource.write_new(private / (label + '.command.json'), resource.command_record(command, env,
                time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(started))))
            row = dict(plan=plan, status='failed', started_utc=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(started)))
            try:
                with (private / (label + '.stdout')).open('xb') as stdout, (private / (label + '.stderr')).open('xb') as stderr:
                    row['launch'] = resource.portable_launch(command, environment=env, stdout=stdout, stderr=stderr, timeout=args.timeout,
                        diagnostic_prefix=private / (label + '.portable-launch'))
                result = report / (label + '.json')
                if result.exists():
                    measured = json.loads(result.read_bytes())
                    acceptance = assess(plan, measured['measurement'])
                    row.update(report=resource.fingerprint(result), acceptance_recomputed=acceptance,
                               observations=acceptance['observations'])
                    if row['launch']['returncode'] == 0 and measured['acceptance'] == acceptance and measured['accepted'] is True and acceptance['accepted']:
                        row['status'] = 'accepted'
            except Exception as error:
                row['error_type'] = type(error).__name__
            row['portable_launch_evidence'] = resource.portable_launch_evidence(private / (label + '.portable-launch'))
            if resource.portable_launch_requires_hold(row['portable_launch_evidence']):
                row.update(status='failed', halted_for_unresolved_launch_cleanup=True)
            row.update(ended_utc=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), wall_seconds=time.time() - started)
            for suffix in ('context.json', 'command.json', 'stdout', 'stderr'):
                path = private / (label + '.' + suffix)
                if path.exists():
                    row[suffix.replace('.', '_')] = resource.fingerprint(path)
            junit = report / (label + '.xml')
            if junit.exists():
                row['junit_xml'] = resource.fingerprint(junit)
            results.append(row)
            resource.write_new(report / (label + '.launch.json'), row)
            if row.get('halted_for_unresolved_launch_cleanup'):
                break
        _check_harness(context['harness'])
        resource._bind(context)
        verify_tests(scripts.parent, candidate)
        accepted = all(row['status'] == 'accepted' for row in results) and len(results) == len(plans)
        resource.write_new(report / 'native-supplement-lane.json', dict(declaration, results=results,
            completed=len(results) == len(plans), accepted=accepted,
            observations=sum(row.get('observations', 0) for row in results),
            accepted_observations=sum(row.get('observations', 0) for row in results if row['status'] == 'accepted')))
        return 0 if accepted else 1
    finally:
        os.umask(previous)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('candidate', 'environment', 'installed-python', 'archived-scripts', 'workspace', 'report'):
        parser.add_argument('--' + name, type=Path)
    parser.add_argument('--profile', choices=(*PROFILES, 'all'), default='all')
    parser.add_argument('--timeout', type=int, default=3600)
    parser.add_argument('--child', type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.child:
        return _child(args.child)
    if args.timeout < 1 or any(getattr(args, name) is None for name in
        ('candidate', 'environment', 'installed_python', 'archived_scripts', 'workspace', 'report')):
        parser.error('candidate, environment, installed interpreter, archived scripts, new workspace/report and positive timeout required')
    return run_lane(args)


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Exception as failure:
        print(json.dumps(dict(completed=False, accepted=False, error_type=type(failure).__name__,
                              diagnostic_text_published=False)), file=sys.stderr)
        raise SystemExit(1)
