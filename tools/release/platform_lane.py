"""Run an immutable candidate's complete ordinary platform qualification lane.

This external release harness is deliberately outside the frozen runtime archive.
Reports certify only the observed cell; they never approve the release gate.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import stat
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
import zipfile

HERE = Path(__file__).resolve().parent
# -I deliberately ignores the script directory. Only release-harness siblings
# are admitted here; candidate imports happen in separate isolated interpreters.
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))
from platform_probe import applicability_records, host, sha256

CELLS = ('linux-x86_64-py311', 'linux-x86_64-py314', 'macos-arm64-py311', 'macos-arm64-py314')
ORACLES = ('test_htslib_bgzip_and_pysam_read_produced_canonical_stream',
           'test_htslib_sidecar_and_virtual_offset_oracles')
THREAD_NAMES = ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS',
                'NUMEXPR_NUM_THREADS', 'VECLIB_MAXIMUM_THREADS', 'BLIS_NUM_THREADS')
FRESH_SCENARIO_FILES = frozenset(('ready.json', 'native-context.private.json',
    'receipt.json', 'stdout.log', 'stderr.log', 'failure-traceback.log',
    'request.private.json', 'command.private.json', 'start.json'))
FRESH_SCENARIO_FILES |= frozenset(name + '.partial' for name in FRESH_SCENARIO_FILES)
FRESH_SCENARIO_NAME = re.compile(r'fresh-scenario-[0-9a-f]{12}\Z')


def _component(name):
    if name in ('', '.', '..') or any(value in name for value in ('/', '\\', '\0')):
        raise ValueError('unsafe fresh-scenario path component')
    return name


def _directory_fd(path):
    """Open every ancestor without following links, including the leaf."""
    path = Path(path)
    if not path.is_absolute() or '..' in path.parts:
        raise ValueError('fresh-scenario paths must be absolute and safe')
    descriptor = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts[1:]:
            child = os.open(_component(part), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                            dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _bounded_names(descriptor, limit):
    names = []
    with os.scandir(descriptor) as entries:
        for entry in entries:
            if len(names) == limit:
                raise ValueError('fresh-scenario discovery count exceeds bound')
            names.append(_component(entry.name))
    return sorted(names)


def _file_identity(information):
    return (information.st_dev, information.st_ino, information.st_size,
            information.st_mtime_ns, information.st_ctime_ns, information.st_nlink)


def _private_json_at(descriptor, name, value):
    _component(name)
    data = (json.dumps(value, indent=2, sort_keys=True) + '\n').encode('utf-8')
    output = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=descriptor)
    os.fchmod(output, 0o600)
    with os.fdopen(output, 'wb') as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    return hashlib.sha256(data).hexdigest()


def capture_fresh_scenarios(basetemp, private, mode, *, max_directories=32,
                            max_files=576, max_file_bytes=16 << 20,
                            max_total_bytes=128 << 20, max_discovery_entries=16384):
    """Copy only bounded fresh-case receipts, never workflow workspaces or inputs.

    Discovery is exactly basetemp/case-directory/fresh-scenario-<12hex>.
    Unrelated pytest aliases are not traversed. Every captured ancestor and
    artifact is opened without following symlinks. All size/count validation
    precedes creation of the destination; invalid or partial JSON stays raw.
    """
    if mode not in ('source', 'installed'):
        raise ValueError('unknown fresh-scenario suite mode')
    limits = dict(max_directories=max_directories, max_files=max_files,
                  max_file_bytes=max_file_bytes, max_total_bytes=max_total_bytes,
                  max_discovery_entries=max_discovery_entries)
    if any(type(value) is not int or value < 1 for value in limits.values()):
        raise ValueError('fresh-scenario bounds must be positive integers')
    destination_name = mode + '-fresh-scenarios'
    started_utc = datetime.now(timezone.utc).isoformat()
    with ExitStack() as stack:
        base_fd = _directory_fd(basetemp)
        stack.callback(os.close, base_fd)
        private_fd = _directory_fd(private)
        stack.callback(os.close, private_fd)
        if stat.S_IMODE(os.fstat(private_fd).st_mode) != 0o700:
            raise ValueError('fresh-scenario private directory must have mode 0700')
        plans, total, count = [], 0, 0
        for case_name in _bounded_names(base_fd, max_discovery_entries):
            information = os.stat(case_name, dir_fd=base_fd, follow_symlinks=False)
            if not stat.S_ISDIR(information.st_mode):
                continue  # No traversal of pytest's *current aliases or files.
            case_fd = os.open(case_name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                              dir_fd=base_fd)
            try:
                for scenario in _bounded_names(case_fd, max_discovery_entries):
                    if not scenario.startswith('fresh-scenario-'):
                        continue
                    if not FRESH_SCENARIO_NAME.fullmatch(scenario):
                        raise ValueError('unsafe fresh-scenario directory name')
                    if len(plans) == max_directories:
                        raise ValueError('fresh-scenario directory count exceeds bound')
                    scenario_fd = os.open(scenario, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                          dir_fd=case_fd)
                    stack.callback(os.close, scenario_fd)
                    files = []
                    for name in _bounded_names(scenario_fd, len(FRESH_SCENARIO_FILES)):
                        if name not in FRESH_SCENARIO_FILES:
                            raise ValueError('unrecognized fresh-scenario artifact')
                        facts = os.stat(name, dir_fd=scenario_fd, follow_symlinks=False)
                        if not stat.S_ISREG(facts.st_mode) or facts.st_nlink != 1:
                            raise ValueError('fresh-scenario artifacts must be regular files with one link')
                        count += 1
                        total += facts.st_size
                        if count > max_files or facts.st_size > max_file_bytes or total > max_total_bytes:
                            raise ValueError('fresh-scenario artifact bytes/count exceed bound')
                        files.append((name, facts))
                    plans.append((case_name, scenario, scenario_fd, files))
            finally:
                os.close(case_fd)
        # Complete metadata preflight above: no raw byte has yet been copied.
        os.mkdir(destination_name, 0o700, dir_fd=private_fd)
        target_fd = os.open(destination_name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                            dir_fd=private_fd)
        os.fchmod(target_fd, 0o700)
        stack.callback(os.close, target_fd)
        manifest = {'schema': 'readmixer.release.fresh-scenario-capture.v1',
                    'scope': 'Exact private child receipts/commands/logs only; no input or workflow workspace.',
                    'mode': mode, 'complete': False, 'limits': limits, 'started_utc': started_utc,
                    'directory_count': len(plans), 'expected_file_count': count,
                    'expected_total_bytes': total, 'files': [],
                    'directories': [case + '/' + scenario for case, scenario, _, _ in plans]}
        cases = {}
        try:
            for case, scenario, source_fd, files in plans:
                if case not in cases:
                    os.mkdir(case, 0o700, dir_fd=target_fd)
                    cases[case] = os.open(case, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                         dir_fd=target_fd)
                    os.fchmod(cases[case], 0o700)
                    stack.callback(os.close, cases[case])
                os.mkdir(scenario, 0o700, dir_fd=cases[case])
                output_fd = os.open(scenario, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                    dir_fd=cases[case])
                os.fchmod(output_fd, 0o700)
                stack.callback(os.close, output_fd)
                for name, before in files:
                    entry = {'relative_path': case + '/' + scenario + '/' + name,
                             'bytes': 0, 'sha256': hashlib.sha256(b'').hexdigest(),
                             'capture_complete': False}
                    source = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=source_fd)
                    try:
                        if _file_identity(os.fstat(source)) != _file_identity(before):
                            raise ValueError('fresh-scenario source changed before capture')
                        output = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                                         0o600, dir_fd=output_fd)
                        os.fchmod(output, 0o600)
                        manifest['files'].append(entry)
                        digest = hashlib.sha256()
                        try:
                            while payload := os.read(source, min(1 << 20, before.st_size - entry['bytes'] + 1)):
                                if entry['bytes'] + len(payload) > before.st_size:
                                    raise ValueError('fresh-scenario source grew during capture')
                                remaining = memoryview(payload)
                                while remaining:
                                    written = os.write(output, remaining)
                                    if written <= 0:
                                        raise OSError('fresh-scenario capture write made no progress')
                                    digest.update(remaining[:written])
                                    entry['bytes'] += written
                                    entry['sha256'] = digest.hexdigest()
                                    remaining = remaining[written:]
                            os.fsync(output)
                        finally:
                            os.close(output)
                        if entry['bytes'] != before.st_size or _file_identity(os.fstat(source)) != _file_identity(before):
                            raise ValueError('fresh-scenario source changed during capture')
                        entry['capture_complete'] = True
                    finally:
                        os.close(source)
                if _bounded_names(source_fd, len(FRESH_SCENARIO_FILES)) != [name for name, _ in files]:
                    raise ValueError('fresh-scenario artifact inventory changed during capture')
            manifest['complete'] = True
        except (Exception, KeyboardInterrupt) as error:
            manifest['error_type'] = type(error).__name__
            manifest['ended_utc'] = datetime.now(timezone.utc).isoformat()
            try:
                digest = _private_json_at(target_fd, 'manifest.json', manifest)
                error.fresh_scenario_capture_manifest = {
                    'partial_manifest_ref': 'private/' + destination_name + '/manifest.json',
                    'partial_manifest_sha256': digest}
            except (Exception, KeyboardInterrupt):
                pass  # Keep the actual copy/interruption failure causal.
            raise
        manifest['ended_utc'] = datetime.now(timezone.utc).isoformat()
        digest = _private_json_at(target_fd, 'manifest.json', manifest)
        return {'captured': True, 'manifest_ref': 'private/' + destination_name + '/manifest.json',
                'manifest_sha256': digest, 'directory_count': len(plans),
                'file_count': count, 'total_bytes': total}


def retain_suite_fresh_scenarios(outcome, basetemp, private, mode):
    """Keep capture failure secondary to the suite's active exception."""
    active = sys.exc_info()[1]
    try:
        record = capture_fresh_scenarios(basetemp, private, mode)
    except (Exception, KeyboardInterrupt) as error:
        record = {'captured': False, 'error_type': type(error).__name__}
        record.update(getattr(error, 'fresh_scenario_capture_manifest', {}))
        diagnostic_interrupt = None
        try:
            import traceback
            name = mode + '-fresh-scenarios-capture-failure.json'
            private_fd = _directory_fd(private)
            try:
                if stat.S_IMODE(os.fstat(private_fd).st_mode) != 0o700:
                    raise ValueError('fresh-scenario private directory must have mode 0700')
                digest = _private_json_at(private_fd, name, {
                    'error_type': type(error).__name__, 'traceback': traceback.format_exc(),
                    'recorded_utc': datetime.now(timezone.utc).isoformat(),
                    'active_suite_exception_type': type(active).__name__ if active else None})
            finally:
                os.close(private_fd)
            record.update(error_ref='private/' + name, error_sha256=digest)
        except (Exception, KeyboardInterrupt) as diagnostic_error:
            record['diagnostic_error_type'] = type(diagnostic_error).__name__
            if isinstance(diagnostic_error, KeyboardInterrupt):
                diagnostic_interrupt = diagnostic_error
        outcome.setdefault('fresh_scenarios', {})[mode] = record
        if diagnostic_interrupt is not None and active is None:
            raise diagnostic_interrupt
        if isinstance(error, KeyboardInterrupt) and active is None:
            raise
        return record
    outcome.setdefault('fresh_scenarios', {})[mode] = record
    return record


def write_json(path, value):
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, 'w') as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())


def clean_environment(venv=None):
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(('PYTHON', 'PYTEST', 'PIP_', 'READMIXER_', 'P15_'))
           and key not in ('VIRTUAL_ENV', '__PYVENV_LAUNCHER__')}
    env.update({name: '1' for name in THREAD_NAMES})
    env.update({'PYTHONNOUSERSITE': '1', 'PIP_CONFIG_FILE': os.devnull, 'PIP_DISABLE_PIP_VERSION_CHECK': '1'})
    if venv:
        env['PATH'] = str(Path(venv) / 'bin') + os.pathsep + env.get('PATH', os.defpath)
    return env


def wheel_members(path):
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)):
            raise ValueError('duplicate wheel member')
        if any(name.startswith('/') or '..' in Path(name).parts for name in names):
            raise ValueError('unsafe wheel member')
        return {name: hashlib.sha256(archive.read(name)).hexdigest() for name in names if not name.endswith('/')}


def compare_wheels(candidate, rebuilt):
    before, after = wheel_members(candidate), wheel_members(rebuilt)
    differences = sorted(name for name in set(before) | set(after) if before.get(name) != after.get(name))
    return {'all_member_bytes_equal': not differences, 'member_count': len(before),
            'different_members': differences, 'archive_bytes_equal': sha256(candidate) == sha256(rebuilt),
            'candidate_sha256': sha256(candidate), 'rebuilt_sha256': sha256(rebuilt),
            'scope': 'all ZIP member contents, including metadata/RECORD; archive timestamps may differ'}


def junit_summary(path):
    root = ET.parse(path).getroot()
    cases = list(root.iter('testcase'))
    counts = {'executed': len(cases), 'passed': 0, 'failed': 0, 'errors': 0, 'skipped': 0}
    for case in cases:
        outcome = 'errors' if case.find('error') is not None else 'failed' if case.find('failure') is not None else 'skipped' if case.find('skipped') is not None else 'passed'
        counts[outcome] += 1
    return {'sha256': sha256(path), 'counts': counts}


def assess_suite(collection, execution, junit, expected_count, expected_not_applicable=()):
    collected, observed = collection['collected'], execution['collected']
    outcomes = execution['outcomes']
    not_applicable = list(expected_not_applicable)
    allowed = {record['nodeid'] for record in not_applicable}
    nodes = []
    for item in outcomes:
        phases = item['phases']
        passed = len(phases) == 3 and {p['phase'] for p in phases if p['outcome'] == 'passed'} == {'setup', 'call', 'teardown'}
        passed = passed and all(p['outcome'] == 'passed' and not p['xfail'] for p in phases)
        excluded = item['nodeid'] in allowed and len(phases) == 2 and {p['phase'] for p in phases} == {'setup', 'teardown'} and all(
            (p['phase'] == 'setup' and p['outcome'] == 'skipped' and p.get('not_applicable') is True and not p['xfail']) or
            (p['phase'] == 'teardown' and p['outcome'] == 'passed' and not p['xfail']) for p in phases)
        if item['nodeid'] in allowed:
            passed = False
        nodes.append({'nodeid': item['nodeid'], 'outcome': 'not_applicable' if excluded else 'passed' if passed else 'not_passed',
                      'phases': phases})
    actual = [item['nodeid'] for item in nodes]
    oracles = {name: any(item['nodeid'] == 'tests/test_bgzf.py::' + name and item['outcome'] == 'passed'
                         for item in nodes) for name in ORACLES}
    checks = {'collection_exit_zero': collection['exit_code'] == 0,
              'execution_exit_zero': execution['exit_code'] == 0,
              'expected_collection_count': len(collected) == expected_count,
              'collection_unique': len(collected) == len(set(collected)),
              'execution_collection_identical': collected == observed,
              'every_collected_node_executed_once': len(actual) == len(set(actual)) and set(actual) == set(collected),
              'every_applicable_phase_passed_no_unexpected_skips_xfail': all(item['outcome'] in ('passed', 'not_applicable') for item in nodes),
              'applicability_exactly_declared': collection.get('not_applicable', []) == not_applicable and execution.get('not_applicable', []) == not_applicable,
              'applicability_nodes_seen': {item['nodeid'] for item in nodes if item['outcome'] == 'not_applicable'} == allowed,
              'junit_agrees': junit['counts'] == {'executed': expected_count, 'passed': expected_count - len(allowed),
                                                 'failed': 0, 'errors': 0, 'skipped': len(allowed)},
              'required_independent_oracles_passed': all(oracles.values())}
    return {'passed': all(checks.values()), 'checks': checks, 'counts': junit['counts'],
            'collected': len(collected), 'not_applicable': not_applicable, 'not_applicable_count': len(allowed), 'nodes': nodes, 'junit': junit, 'independent_oracles': oracles,
            'package': execution['package']}


def check_wheel_package(package, wheel):
    expected = {name.removeprefix('readmixer_py/'): digest for name, digest in wheel_members(wheel).items()
                if name.startswith('readmixer_py/') and name.endswith('.py')}
    actual = {item['name']: item['sha256'] for item in package['package_files']}
    if expected != actual:
        raise ValueError('runtime package bytes differ from candidate wheel')
    return {'matched': True, 'files': len(actual)}


class Runner:
    def __init__(self, report, timeout):
        self.report = report
        self.private = report / 'private'
        self.private.mkdir(mode=0o700)
        self.timeout = timeout
        self.stages = []

    def command(self, name, command, cwd, environment, timeout=None):
        record = {'name': name, 'command_ref': 'private/' + name + '.command.json',
                  'stdout_ref': 'private/' + name + '.stdout', 'stderr_ref': 'private/' + name + '.stderr',
                  'started_utc': datetime.now(timezone.utc).isoformat()}
        write_json(self.private / (name + '.command.json'), {'argv': list(map(str, command)), 'cwd': str(cwd),
                   'environment_controls': {k: environment[k] for k in sorted(environment)
                                            if k.startswith(('PYTHON', 'PYTEST', 'PIP_', 'READMIXER_', 'Malloc'))
                                            or k in THREAD_NAMES or k in ('NSZombieEnabled', 'DYLD_INSERT_LIBRARIES')}})
        start = time.monotonic()
        output = os.open(self.private / (name + '.stdout'), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        error = os.open(self.private / (name + '.stderr'), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(output, 'wb') as stdout, os.fdopen(error, 'wb') as stderr:
                process = subprocess.Popen(list(map(str, command)), cwd=cwd, env=environment,
                                           stdout=stdout, stderr=stderr, start_new_session=True)
                try:
                    code = process.wait(timeout=timeout or self.timeout)
                    record['timed_out'] = False
                except (subprocess.TimeoutExpired, KeyboardInterrupt) as error:
                    record['timed_out'] = isinstance(error, subprocess.TimeoutExpired)
                    record['cancelled'] = isinstance(error, KeyboardInterrupt)
                    # Only the new harness-owned process group is signalled.
                    for signum, grace in ((signal.SIGTERM, 10), (signal.SIGKILL, 5)):
                        try:
                            os.killpg(process.pid, signum)
                        except ProcessLookupError:
                            pass
                        try:
                            process.wait(timeout=grace)
                            break
                        except subprocess.TimeoutExpired:
                            continue
                    # A direct parent can exit on SIGTERM while its same-group
                    # children ignore it. Always finish escalation for that owned
                    # group even after process.wait() returned successfully.
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    code = process.wait(timeout=5)
                record.update({'exit_code': code, 'elapsed_seconds': time.monotonic() - start})
        finally:
            record['ended_utc'] = datetime.now(timezone.utc).isoformat()
            self.stages.append(record)
        for kind in ('stdout', 'stderr', 'command.json'):
            record[kind.replace('.', '_') + '_sha256'] = sha256(self.private / (name + '.' + kind))
        if record.get('cancelled'):
            raise KeyboardInterrupt
        return record.get('exit_code') == 0 and not record.get('timed_out')

    def required(self, *args, **kwargs):
        if not self.command(*args, **kwargs):
            raise RuntimeError('required stage failed: ' + args[0])


def run_lane(bundle, environment, workspace, report, expected_cell, *, timeout=3600):
    from candidate import load_candidate, safe_extract_sdist
    from lock_environment import verify_environment

    bundle, environment, workspace, report = [Path(p).resolve() for p in (bundle, environment, workspace, report)]
    # Reject overlap before mutation; a failed retry must have a new destination.
    checkout = HERE.parents[1]
    if workspace.is_relative_to(checkout) or workspace.is_relative_to(bundle) or workspace.is_relative_to(environment):
        raise ValueError('workspace must be outside checkout, candidate and environment')
    if report.is_relative_to(bundle) or report.is_relative_to(environment):
        raise ValueError('report must be outside candidate and environment')
    if report == workspace or report.is_relative_to(workspace) or workspace.is_relative_to(report):
        raise ValueError('workspace and report must be separate')
    if workspace.exists() or report.exists():
        raise ValueError('workspace and report must be new paths')
    report.mkdir(mode=0o700, parents=True)
    runner = Runner(report, timeout)
    previous_umask = os.umask(0o077)
    outcome = {'schema': 'readmixer.release.platform-lane.v1', 'expected_cell': expected_cell,
               'completed': False, 'accepted': False, 'release_qualified': False,
               'scope': 'ordinary complete source and installed suites; resource/release gates remain separate',
               'harness': [{'name': path.name, 'sha256': sha256(path)} for path in
                           (HERE / 'platform_lane.py', HERE / 'platform_probe.py', HERE / 'candidate.py', HERE / 'lock_environment.py')],
               'stages': runner.stages, 'suites': {}, 'lazy_checks': [], 'fresh_scenarios': {}}
    try:
        if expected_cell not in CELLS:
            raise ValueError('unsupported expected cell')
        facts = host()
        outcome['host'] = facts
        if facts['cell'] != expected_cell or facts['implementation'] != 'CPython' or facts['pointer_bits'] != 64:
            raise ValueError('actual platform does not match required CPython/architecture cell')
        snapshot = report / 'harness'
        snapshot.mkdir(mode=0o700)
        for entry in outcome['harness']:
            data = (HERE / entry['name']).read_bytes()
            if hashlib.sha256(data).hexdigest() != entry['sha256']:
                raise ValueError('harness changed while capturing evidence')
            with (snapshot / entry['name']).open('xb') as stream:
                stream.write(data)
        candidate = load_candidate(bundle)
        outcome.update({'candidate_id': candidate['candidate_id'], 'version': candidate['version'],
                        'candidate_manifest_sha256': sha256(bundle / 'candidate.json'), 'archives': candidate['archives']})
        manifest = json.loads((environment / 'environment.json').read_bytes())
        lock = (environment / 'requirements.lock').read_bytes()
        verify_environment(environment / 'wheels', manifest, lock)
        outcome['environment'] = {'manifest_sha256': sha256(environment / 'environment.json'),
                                  'lock_sha256': hashlib.sha256(lock).hexdigest()}
        for name in ('environment.json', 'requirements.lock'):
            with (report / name).open('xb') as stream:
                stream.write((environment / name).read_bytes())
        workspace.mkdir(mode=0o700)
        source = safe_extract_sdist(bundle, workspace / 'source')
        policy_path = HERE / 'platform-applicability.json'
        policy = json.loads(policy_path.read_bytes())
        not_applicable = applicability_records(policy, candidate['candidate_id'], expected_cell, source)
        with (report / 'platform-applicability.json').open('xb') as stream:
            stream.write(policy_path.read_bytes())
        outcome['platform_applicability'] = {'policy_sha256': sha256(policy_path),
            'not_applicable': not_applicable, 'required_replacements': policy['required_macos_replacement_evidence'] if not_applicable else []}
        installed_root = workspace / 'installed-work'
        installed_root.mkdir(mode=0o700)
        for dirname in ('tests', 'scripts'):
            shutil.copytree(source / dirname, installed_root / dirname)
        (installed_root / 'readmixer').mkdir()
        shutil.copyfile(source / 'readmixer' / 'readmixer.py', installed_root / 'readmixer' / 'readmixer.py')
        (installed_root / 'pytest.ini').write_text('[pytest]\ntestpaths = tests\n')
        wheel = bundle / candidate['archives']['wheel']['name']
        expected_count = candidate.get('expected_tests')
        if not isinstance(expected_count, int) or expected_count < 1:
            raise ValueError('candidate must declare complete expected test count')
        venv = workspace / 'venv'
        env = clean_environment()
        runner.required('create-venv', [sys.executable, '-I', '-m', 'venv', str(venv)], workspace, env)
        python = venv / 'bin' / 'python'
        env = clean_environment(venv)
        runner.required('install-locked-dependencies', [python, '-I', '-m', 'pip', 'install', '--no-index',
                        '--find-links', environment / 'wheels', '--require-hashes', '-r', report / 'requirements.lock'], workspace, env)
        runner.required('install-candidate', [python, '-I', '-m', 'pip', 'install', '--no-index', '--no-deps', wheel], workspace, env)
        runner.required('dependency-check', [python, '-I', '-m', 'pip', 'check'], workspace, env)
        probe = HERE / 'platform_probe.py'
        facts_file = runner.private / 'native-facts.json'
        runner.required('native-facts', [python, '-I', probe, 'facts', '--root', installed_root, '--result', facts_file,
                                      '--native-config', runner.private / 'numpy-build.txt'], installed_root, env)
        outcome['native'] = json.loads(facts_file.read_bytes())
        if outcome['native']['host']['allocator_environment'] != facts['allocator_environment']:
            raise ValueError('native child startup allocator environment differs from platform declaration')
        outcome['installed_package_matches_wheel'] = check_wheel_package(outcome['native']['package'], wheel)
        for mode, root in (('source', source), ('installed', installed_root)):
            suite_env = dict(env)
            if mode == 'installed':
                suite_env['READMIXER_TEST_CONSOLE'] = str(venv / 'bin' / 'readmixer-py')
            for collect in (True, False):
                label = mode + ('-collect' if collect else '-suite')
                cmd = [python, '-I', probe, 'suite', '--root', root, '--mode', mode,
                       '--result', runner.private / (label + '.json'), '--junit', runner.private / (label + '.xml'),
                       '--basetemp', workspace / (label + '-tmp'), '--applicability', report / 'platform-applicability.json',
                       '--candidate-id', candidate['candidate_id']]
                if collect:
                    cmd.append('--collect-only')
                    runner.command(label, cmd, root, suite_env)
                else:
                    try:
                        runner.command(label, cmd, root, suite_env)
                    finally:
                        retain_suite_fresh_scenarios(outcome, workspace / (label + '-tmp'),
                                                    runner.private, mode)
            collection = json.loads((runner.private / (mode + '-collect.json')).read_bytes())
            execution = json.loads((runner.private / (mode + '-suite.json')).read_bytes())
            if any(record.get('startup_allocator_environment') != facts['allocator_environment']
                   for record in (collection, execution)):
                raise ValueError('suite child startup allocator environment differs from platform declaration')
            junit = junit_summary(runner.private / (mode + '-suite.xml'))
            outcome['suites'][mode] = assess_suite(collection, execution, junit, expected_count, not_applicable)
            check_wheel_package(execution['package'], wheel)
        # A separate environment proves help/version does not depend on installed natives.
        lazy_venv = workspace / 'dependency-free'
        runner.required('create-dependency-free', [sys.executable, '-I', '-m', 'venv', lazy_venv], workspace, env)
        lazy_python = lazy_venv / 'bin' / 'python'
        lazy_env = clean_environment(lazy_venv)
        runner.required('install-dependency-free', [lazy_python, '-I', '-m', 'pip', 'install', '--no-index', '--no-deps', wheel], workspace, lazy_env)
        for entry in ('module', 'console', 'forwarder'):
            for flag in ('--help', '--version'):
                label = 'lazy-' + entry + '-' + flag.removeprefix('--')
                target = lazy_venv / 'bin' / 'readmixer-py' if entry == 'console' else installed_root / 'readmixer' / 'readmixer.py'
                runner.required(label, [lazy_python, '-I', probe, 'lazy', '--root', installed_root,
                                '--result', runner.private / (label + '.json'), '--entry', entry,
                                '--entry-path', target, '--flag=' + flag], installed_root, lazy_env)
                outcome['lazy_checks'].append(json.loads((runner.private / (label + '.json')).read_bytes()))
        # Rebuild only from a second fresh extraction, never replacing the candidate.
        rebuild_root = safe_extract_sdist(bundle, workspace / 'rebuild-source')
        rebuild_output = report / 'rebuilt'
        rebuild_output.mkdir(mode=0o700)
        runner.required('sdist-rebuild', [python, '-I', '-m', 'build', '--wheel', '--no-isolation',
                        '--outdir', rebuild_output, rebuild_root], workspace, env)
        rebuilt = list(rebuild_output.glob('*.whl'))
        if len(rebuilt) != 1:
            raise ValueError('sdist build did not create exactly one wheel')
        os.chmod(rebuilt[0], 0o600)
        outcome['sdist_rebuild'] = compare_wheels(wheel, rebuilt[0])
        load_candidate(bundle)
        verify_environment(environment / 'wheels', manifest, lock)
        outcome['suite_collections_identical'] = [item['nodeid'] for item in outcome['suites']['source']['nodes']] == [item['nodeid'] for item in outcome['suites']['installed']['nodes']]
        for entry in outcome['harness']:
            if sha256(HERE / entry['name']) != entry['sha256']:
                raise ValueError('harness changed during the invocation')
        if sha256(environment / 'environment.json') != outcome['environment']['manifest_sha256'] or sha256(environment / 'requirements.lock') != outcome['environment']['lock_sha256']:
            raise ValueError('environment identity changed during the invocation')
        outcome['completed'] = True
        outcome['accepted'] = outcome['suite_collections_identical'] and all(s['passed'] for s in outcome['suites'].values()) and outcome['sdist_rebuild']['all_member_bytes_equal'] and all(s['passed'] for s in outcome['lazy_checks']) and all(s.get('exit_code') == 0 and not s.get('timed_out') for s in runner.stages) and set(outcome['fresh_scenarios']) == {'source', 'installed'} and all(s['captured'] for s in outcome['fresh_scenarios'].values())
    except (Exception, KeyboardInterrupt) as exc:
        outcome['cancelled'] = isinstance(exc, KeyboardInterrupt)
        # Private tracebacks are useful; aggregate errors disclose only type/stage.
        import traceback
        diagnostic = runner.private / 'failure.txt'
        descriptor = os.open(diagnostic, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, 'w') as stream:
            traceback.print_exc(file=stream)
        outcome['failure'] = {'type': type(exc).__name__, 'stage': runner.stages[-1]['name'] if runner.stages else 'preflight',
                              'private_diagnostic': 'private/failure.txt', 'sha256': sha256(diagnostic)}
    finally:
        outcome['limitations'] = ['Observed host/filesystem only; no network filesystem claim.',
            'Ordinary suite is not replacement for resource/scale/performance qualification.',
            'Raw commands, paths, logs and JUnit diagnostics are private; aggregate node IDs name artificial tests.',
            'No candidate/dependency change inherits this lane result.']
        try:
            write_json(report / 'report.json', outcome)
        finally:
            os.umask(previous_umask)
    return outcome


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--environment', type=Path, required=True)
    parser.add_argument('--workspace', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--expected-cell', choices=CELLS, required=True)
    parser.add_argument('--timeout', type=int, default=3600, help='seconds per subprocess; default 3600')
    args = parser.parse_args(argv)
    if args.timeout < 1:
        parser.error('--timeout must be positive')
    interrupted_signal = signal.SIGINT
    previous = signal.getsignal(signal.SIGTERM)
    def interrupted(signum, _frame):
        nonlocal interrupted_signal
        interrupted_signal = signum
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    try:
        result = run_lane(args.candidate, args.environment, args.workspace, args.report, args.expected_cell, timeout=args.timeout)
        print(json.dumps({'accepted': result['accepted'], 'completed': result['completed'], 'release_qualified': False}))
        return 128 + interrupted_signal if result.get('cancelled') else 0 if result['accepted'] else 1
    finally:
        signal.signal(signal.SIGTERM, previous)


if __name__ == '__main__':
    raise SystemExit(main())
