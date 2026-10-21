#!/usr/bin/env python3
"""External Plan 17 resource runner; never modifies the frozen candidate.

Every selected frozen cell retains its one warm-up and five measured iterations.
This runner does not certify native charge calibration, startup supplements or
ordinary signal tests; those remain separately required evidence. No resume.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
from importlib import metadata
import json
import os
from pathlib import Path
import platform
import resource
import re
import shutil
import signal
import subprocess
import sys
import time
import zipfile

NATIVE = ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS',
          'VECLIB_MAXIMUM_THREADS', 'NUMEXPR_NUM_THREADS', 'BLIS_NUM_THREADS')
ALLOCATOR_CONTROLS = ('MallocNanoZone', 'MallocLargeCache', 'MallocAggressiveMadvise', 'MallocMaxMagazines')
LINUX_ONLY = frozenset(('constrained-below-minimum', 'constrained-serial',
                       'constrained-parallel', 'scratch-device-exhaustion'))
PORTABLE_HOST = frozenset(('low-descriptors', 'slow-storage-ordered-backpressure',
    'memory-ordered-backpressure', 'space-query-failure', 'separate-scratch-output-devices'))
HARNESS = ('resource_lane.py', 'candidate.py', 'lock_environment.py')
PENDING = ('native_workspace_charge_calibration', 'component_and_startup_measurements',
           'ordinary_suite_real_signals_and_publication', 'filesystem_mount_provenance')


def fingerprint(path):
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise ValueError('expected a regular evidence file')
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1 << 20), b''):
            digest.update(block)
    return dict(bytes=path.stat().st_size, sha256=digest.hexdigest())


def write_new(path, value):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, 'w') as stream:
        json.dump(value, stream, sort_keys=True, indent=2)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())


def utc_now():
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())


def command_record(command, environment, started_utc):
    """Private exact invocation; retain controlled variables without secrets."""
    return dict(schema='readmixer.release.command.v1', argv=list(command), cwd=str(Path.cwd()),
        started_utc=started_utc, native_thread_environment={name: environment.get(name) for name in NATIVE},
        allocator_environment=allocator_environment(environment))


def allocator_environment(environment=None):
    values = os.environ if environment is None else environment
    return {name: values.get(name) for name in ALLOCATOR_CONTROLS}


def verify_allocator_environment(expected):
    """Read inherited startup context before candidate/native imports."""
    if (not isinstance(expected, dict) or set(expected) != set(ALLOCATOR_CONTROLS)
            or any(value is not None and not isinstance(value, str) for value in expected.values())):
        raise ValueError('allocator startup context is missing or malformed')
    observed = allocator_environment()
    if observed != expected:
        raise ValueError('allocator startup context differs from launcher declaration')
    return observed


def snapshot_harness(directory):
    directory.mkdir(mode=0o700)
    for name in HARNESS:
        shutil.copyfile(Path(__file__).parent / name, directory / name)
        os.chmod(directory / name, 0o600)


def verify_scripts(directory, candidate):
    directory = Path(directory).resolve(strict=True)
    if (directory.parent / 'src').exists():
        raise ValueError('archived scripts must be staged without adjacent source imports')
    expected = {Path(name).name: value for name, value in candidate['source_manifest'].items()
                if name.startswith('scripts/') and name.endswith('.py')}
    actual = {path.name: fingerprint(path) for path in directory.glob('*.py')}
    if not expected or actual != expected:
        raise ValueError('executed scripts differ from the frozen source manifest')
    return actual


def select_cells(matrix, profile, system):
    if profile not in ('bounded', 'scale', 'physical') or system not in ('Linux', 'Darwin'):
        raise ValueError('unsupported resource profile or platform')
    if profile == 'bounded':
        selected = [row for row in matrix['cells'] if row['tier'] == 'bounded'
                    or row['id'] in PORTABLE_HOST or (system == 'Linux' and row['id'] in LINUX_ONLY)]
        excluded = [dict(cell_id=row['id'], status='not_applicable_on_platform',
            reason='Linux enforcement is outside the macOS portable contract')
            for row in matrix['cells'] if system == 'Darwin' and row['id'] in LINUX_ONLY]
    else:
        tier = 'scale' if profile == 'scale' else 'release-host'
        selected = [row for row in matrix['cells'] if row['tier'] == tier]
        excluded = []
    if not selected:
        raise ValueError('empty qualification profile')
    return selected, excluded


def host_limits(cell, system, affinity=()):
    private = cell['private_controls']
    limits = {}
    if 'rlimit_nofile' in private:
        limits['nofile'] = private['rlimit_nofile']
    if system == 'Linux':
        limits['memory_bytes'] = private.get('memory_cgroup_bytes',
            (64 if cell['tier'] == 'release-host' else 8) << 30)
        limits['cpu_quota'] = private.get('cpu_quota', max(1, cell['configuration']['workers_requested']))
        if len(affinity) < limits['cpu_quota']:
            raise ValueError('insufficient actual CPU affinity for declared cell')
        limits['affinity'] = sorted(affinity)[:limits['cpu_quota']]
    elif system != 'Darwin' or cell['id'] in LINUX_ONLY:
        raise ValueError('Linux-only controls are not portable controls')
    return limits


def inherited_environment(workspace):
    environment = dict(os.environ)
    for name in ('PYTHONPATH', 'PYTHONHOME', 'PYTHONSTARTUP'):
        environment.pop(name, None)
    environment.update({name: '1' for name in NATIVE})
    environment.update(TMPDIR=str(workspace), TMP=str(workspace), TEMP=str(workspace))
    return environment


import errno


class PortableCleanupError(RuntimeError):
    """The child status is known but owned-group absence is unverified."""


def _portable_birth(pid):
    """Diagnostic ps birth text at second precision; not signal authorization."""
    command = ['/bin/ps', '-p', str(pid), '-o', 'pid=,lstart=']
    result = subprocess.run(command, stdin=subprocess.DEVNULL, capture_output=True,
        timeout=2, env={'LC_ALL': 'C', 'TZ': 'UTC', 'PATH': '/usr/bin:/bin'})
    rows = result.stdout.decode('ascii', errors='strict').splitlines()
    if result.returncode != 0 or len(rows) != 1:
        raise ProcessLookupError('owned child birth readback unavailable')
    fields = rows[0].split(None, 1)
    if len(fields) != 2 or fields[0] != str(pid):
        raise ValueError('owned child birth readback has an unexpected PID')
    return dict(source='ps lstart', argv=command, returncode=result.returncode,
        birth_text_utc=fields[1], precision='one second; insufficient for post-reap signal ownership')


def _portable_numeric_group(group):
    """Bounded numeric diagnostics, never absence or signal authorization."""
    command = ['/bin/ps', '-A', '-o', 'pid=,pgid=,uid=']
    result = subprocess.run(command, stdin=subprocess.DEVNULL, capture_output=True,
                            timeout=2, env={'LC_ALL': 'C', 'PATH': '/usr/bin:/bin'})
    if result.returncode != 0 or len(result.stdout) > 4 << 20:
        raise RuntimeError('numeric process census failed or exceeded its byte bound')
    matching, seen = [], set()
    lines = result.stdout.decode('ascii', errors='strict').splitlines()
    if len(lines) > 32768:
        raise RuntimeError('numeric process census exceeded its row bound')
    for line in lines:
        parts = line.split()
        if len(parts) != 3 or not all(part.isdecimal() for part in parts):
            raise ValueError('numeric process census has an invalid row')
        pid, pgid, uid = map(int, parts)
        if pid in seen:
            raise ValueError('numeric process census has duplicate PIDs')
        seen.add(pid)
        if pgid == group:
            matching.append(dict(pid=pid, pgid=pgid, uid=uid))
    return dict(argv=command, returncode=result.returncode,
                matching_members=matching, complete_numeric_rows=len(lines),
                inference='sampled numeric membership only; not kernel absence proof')


class _PortableJournal:
    def __init__(self, prefix, request):
        self.prefix = Path(prefix) if prefix is not None else None
        self.events = []
        if self.prefix is not None:
            # Reject links in all ancestors before any evidence mutation.
            for path in (self.prefix.parent, *self.prefix.parent.parents):
                if path.is_symlink():
                    raise ValueError('launcher evidence ancestor is a symlink')
            self.paths = {kind: Path(str(self.prefix) + '.' + kind)
                for kind in ('request.json', 'events.jsonl', 'result.json')}
            if any(path.exists() or path.is_symlink() for path in self.paths.values()):
                raise FileExistsError('launcher diagnostic destination already exists')
            write_new(self.paths['request.json'], request)
            descriptor = os.open(self.paths['events.jsonl'],
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            os.close(descriptor)

    def event(self, phase, **value):
        row = dict(sequence=len(self.events), utc=utc_now(), phase=phase, **value)
        self.events.append(row)
        if self.prefix is not None:
            descriptor = os.open(self.paths['events.jsonl'],
                os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW)
            with os.fdopen(descriptor, 'w') as stream:
                stream.write(json.dumps(row, sort_keys=True) + '\n')
                stream.flush()
                os.fsync(stream.fileno())

    def finish(self, result):
        if self.prefix is not None:
            write_new(self.paths['result.json'], result)


def _portable_error(error):
    return dict(error_type=type(error).__name__, errno=getattr(error, 'errno', None))


def _portable_absence(process, journal, maximum=2):
    """ESRCH is the only accepted absence witness; EPERM stays unresolved."""
    deadline = time.monotonic() + maximum
    sampled = False
    while True:
        try:
            os.killpg(process.pid, 0)
            state = 'PRESENT'
        except ProcessLookupError as error:
            if error.errno != errno.ESRCH:
                raise
            journal.event('group_absence', pgid=process.pid,
                          proof='killpg(group,0) raised ESRCH', verified=True)
            return True
        except PermissionError as error:
            if error.errno != errno.EPERM:
                raise
            state = 'UNRESOLVED_EPERM'
            journal.event('group_probe', pgid=process.pid, state=state,
                          **_portable_error(error))
        # Do not signal an already-reaped leader's numeric process group.
        if not sampled:
            sampled = True
            try:
                journal.event('numeric_group_readback', **_portable_numeric_group(process.pid))
            except Exception as error:
                journal.event('numeric_group_readback_failed', **_portable_error(error))
        if time.monotonic() >= deadline:
            journal.event('group_absence', pgid=process.pid, state=state,
                          proof=None, verified=False)
            return False
        time.sleep(min(0.05, max(0, deadline - time.monotonic())))


def _portable_signal_unreaped(process, signum, journal):
    # A Python wait interrupted between waitpid and returncode storage leaves
    # returncode=None without ownership. Obtain fresh kernel child proof first.
    if process.returncode is not None:
        journal.event('signal_refused_reaped_leader', pgid=process.pid,
                      signal=signal.Signals(signum).name)
        return dict(signal_sent=False, literal_returncode=None)
    if signal.getsignal(signal.SIGCHLD) != signal.SIG_DFL:
        raise PortableCleanupError('custom SIGCHLD disposition prevents sole-waiter ownership proof')
    journal.event('before_direct_child_proof', pid=process.pid,
                  source='waitpid(pid,WNOHANG)', sole_waiter_contract=True)
    waited_pid, wait_status = os.waitpid(process.pid, os.WNOHANG)
    if waited_pid == process.pid:
        literal = os.waitstatus_to_exitcode(wait_status)
        process.returncode = literal
        journal.event('direct_child_reaped_by_proof', pid=process.pid,
                      wait_status=wait_status, returncode=literal)
        return dict(signal_sent=False, literal_returncode=literal,
                    literal_status_source='waitpid(pid,WNOHANG)', wait_status=wait_status)
    if (waited_pid, wait_status) != (0, 0):
        raise PortableCleanupError('direct-child proof returned an unexpected PID/status')
    # With the sole waiter/default SIGCHLD contract this still-live child holds
    # its PID until a later deliberate wait; a reused numeric session cannot
    # satisfy the child proof. Never signal after proof errors, including ECHILD.
    pgid, sid = os.getpgid(process.pid), os.getsid(process.pid)
    if pgid != process.pid or sid != process.pid or pgid == os.getpgrp():
        raise PortableCleanupError('live direct child is not the original isolated session')
    journal.event('before_signal', pid=process.pid, pgid=pgid, sid=sid,
                  signal=signal.Signals(signum).name, direct_child_live_proved=True,
                  proof='waitpid(pid,WNOHANG)==(0,0)')
    try:
        os.killpg(process.pid, signum)
        journal.event('signal_sent', pgid=process.pid, signal=signal.Signals(signum).name)
        return dict(signal_sent=True, literal_returncode=None)
    except (ProcessLookupError, PermissionError) as error:
        if error.errno not in (errno.ESRCH, errno.EPERM):
            raise
        journal.event('signal_error', pgid=process.pid,
                      signal=signal.Signals(signum).name, **_portable_error(error))
        return dict(signal_sent=False, literal_returncode=None)


def _portable_wait(process, timeout, journal):
    """Return only decoded raw kernel status; never Popen's ECHILD zero."""
    deadline = None if timeout is None else time.monotonic() + timeout
    while True:
        waited_pid, wait_status = os.waitpid(process.pid, os.WNOHANG)
        if waited_pid == process.pid:
            literal = os.waitstatus_to_exitcode(wait_status)
            process.returncode = literal
            journal.event('direct_child_raw_wait_status', pid=process.pid,
                wait_status=wait_status, returncode=literal,
                source='waitpid(pid,WNOHANG)')
            return literal
        if (waited_pid, wait_status) != (0, 0):
            raise PortableCleanupError('kernel wait returned an unexpected PID/status')
        if deadline is not None and time.monotonic() >= deadline:
            raise subprocess.TimeoutExpired(getattr(process, 'args', 'owned child'), timeout)
        time.sleep(0.01 if deadline is None else min(0.01, max(0, deadline - time.monotonic())))


def portable_launch(command, *, environment, stdout, stderr, timeout=None,
                    diagnostic_prefix=None):
    """Fresh POSIX session, literal child status, and verified group absence.

    The launcher sends no post-reap signals. An unresolved remaining group fails
    closed with its sampled numeric membership retained for owned follow-up.
    """
    started, started_utc = time.monotonic(), utc_now()
    journal = _PortableJournal(diagnostic_prefix, dict(
        schema='readmixer.release.portable-launch-request.v2',
        argv=list(command), started_utc=started_utc,
        timeout=timeout, post_reap_signals_permitted=False))
    process, primary, cleanup_verified, phase = None, None, False, 'prelaunch_contract'
    result = dict(schema='readmixer.release.portable-launch.v2',
        started_utc=started_utc, returncode=None, cleanup_verified=False,
        post_reap_signals_sent=False)
    secondary_errors = []
    literal_returncode, literal_status_source = None, None

    def secondary(error, stage):
        secondary_errors.append(dict(phase=stage, **_portable_error(error)))
        if primary is not None and hasattr(primary, 'add_note'):
            primary.add_note('Portable launcher secondary failure in ' + stage + ': ' + type(error).__name__)

    try:
        if signal.getsignal(signal.SIGCHLD) != signal.SIG_DFL:
            raise PortableCleanupError('default SIGCHLD and sole-waiter ownership are required before launch')
        journal.event('prelaunch_contract', sigchld_disposition='SIG_DFL', sole_waiter_contract=True)
        phase = 'Popen'
        process = subprocess.Popen(command, env=environment, stdout=stdout,
            stderr=stderr, start_new_session=True)
        result['pid'] = process.pid
        phase = 'Popen_returned'
        journal.event(phase, pid=process.pid, expected_pgid=process.pid,
                      expected_sid=process.pid)
        phase = 'native_identity'
        try:
            identity = dict(pid=process.pid, pgid=os.getpgid(process.pid),
                sid=os.getsid(process.pid), parent_uid=os.getuid())
        except OSError as error:
            result['native_identity_error'] = _portable_error(error)
            journal.event('native_identity_unavailable', **_portable_error(error))
        else:
            result['native_identity'] = identity
            # A positive mismatch is a failed isolation check, never optional
            # diagnostic unavailability. No group signal is authorized by it.
            if identity['pgid'] != process.pid or identity['sid'] != process.pid:
                journal.event('native_identity_mismatch', **identity)
                raise PortableCleanupError('Popen child is not in its requested new session')
            try:
                identity['birth'] = _portable_birth(process.pid)
            except Exception as error:
                identity['birth_error'] = _portable_error(error)
            journal.event('native_identity', **identity)
        phase = 'wait'
        journal.event('before_wait', pid=process.pid, timeout=timeout)
        literal_returncode = _portable_wait(process, timeout, journal)
        literal_status_source = 'waitpid(pid,WNOHANG) returned raw status'
        journal.event('child_reaped', pid=process.pid, returncode=literal_returncode)
        phase = 'post_wait_absence'
        cleanup_verified = _portable_absence(process, journal)
        if not cleanup_verified:
            raise PortableCleanupError('child reaped; process-group absence unresolved; no post-reap signal sent')
    except BaseException as error:
        primary = error
        import traceback
        result['primary_exception'] = dict(phase=phase, **_portable_error(error))
        try:
            result['primary_traceback_private'] = traceback.format_exception(type(error), error, error.__traceback__)
        except BaseException as failure:
            secondary(failure, 'primary_traceback_format')
        try:
            journal.event('primary_exception', error_phase=phase, **_portable_error(error))
        except BaseException as failure:
            secondary(failure, 'primary_exception_journal')
        # Escalate only while this direct child remains unreaped. If wait has
        # reaped it, a later numeric PGID is never treated as signal ownership.
        if process is not None and process.returncode is None and not isinstance(error, ChildProcessError):
            for signum, seconds in ((signal.SIGINT, 10), (signal.SIGTERM, 5), (signal.SIGKILL, 5)):
                if process.returncode is not None:
                    break
                try:
                    proof = _portable_signal_unreaped(process, signum, journal)
                    if proof['literal_returncode'] is not None:
                        literal_returncode = proof['literal_returncode']
                        literal_status_source = proof['literal_status_source']
                except BaseException as failure:
                    secondary(failure, 'ownership_or_signal_' + signal.Signals(signum).name)
                    # ECHILD/invalid/failed proof forbids further destructive
                    # waits too: Popen.wait could otherwise synthesize exit0.
                    break
                if process.returncode is not None:
                    break
                try:
                    literal_returncode = _portable_wait(process, seconds, journal)
                    literal_status_source = 'waitpid(pid,WNOHANG) returned raw cleanup status'
                    journal.event('child_reaped_during_cleanup', pid=process.pid,
                                  returncode=literal_returncode)
                except subprocess.TimeoutExpired:
                    continue
                except ChildProcessError as failure:
                    secondary(failure, 'cleanup_wait_ownership_unknown')
                    break
                except BaseException as failure:
                    secondary(failure, 'cleanup_wait')
        if process is not None and literal_returncode is not None and phase != 'post_wait_absence':
            try:
                cleanup_verified = _portable_absence(process, journal)
            except BaseException as failure:
                secondary(failure, 'cleanup_absence')
    finally:
        if process is not None:
            # Never promote a Popen synthetic zero after ECHILD into a literal
            # status. Only a returned wait result/raw status is authoritative.
            result['returncode'] = literal_returncode
            result['literal_status_source'] = literal_status_source
            result['popen_returncode_untrusted_diagnostic'] = process.returncode
            result['coordinator_reaped'] = True if literal_returncode is not None else None
        else:
            result['coordinator_reaped'] = None
        result.update(ended_utc=utc_now(), wall_seconds=time.monotonic() - started,
            cleanup_verified=cleanup_verified, secondary_errors=secondary_errors,
            cleanup_scope='original direct child reaped and owned group ESRCH verified' if cleanup_verified
                else 'cleanup unresolved; no post-reap group signal authorized',
            events=journal.events)
        try:
            journal.finish(result)
        except BaseException as failure:
            if primary is not None:
                secondary(failure, 'final_receipt')
            else:
                raise
    if primary is not None:
        raise primary
    return result


def portable_launch_evidence(prefix):
    """Retain partial diagnostics without masking an earlier launch exception."""
    import traceback
    files, errors, final = {}, [], None
    for suffix in ('request.json', 'events.jsonl', 'result.json'):
        path = Path(str(prefix) + '.' + suffix)
        try:
            if path.exists() or path.is_symlink():
                files[suffix] = dict(path=str(path), **fingerprint(path))
                if suffix == 'result.json':
                    parsed = json.loads(path.read_bytes())
                    if not isinstance(parsed, dict):
                        raise ValueError('portable final evidence is not a JSON object')
                    final = parsed
        except Exception as error:
            detail = dict(path=str(path), **_portable_error(error))
            try:
                detail['traceback_private'] = traceback.format_exception(type(error), error, error.__traceback__)
            except BaseException as secondary:
                detail['traceback_format_error'] = _portable_error(secondary)
            errors.append(detail)
    return dict(files=files, read_errors=errors,
        observed_returncode=final.get('returncode') if final is not None else None,
        primary_exception=final.get('primary_exception') if final is not None else None,
        cleanup_verified=final.get('cleanup_verified') if final is not None else None,
        coordinator_reaped=final.get('coordinator_reaped') if final is not None else None)


def portable_launch_requires_hold(evidence):
    # No next measurement while an unreaped child/remaining group is possible.
    return bool(evidence['read_errors'] or evidence['cleanup_verified'] is not True
                or evidence['coordinator_reaped'] is not True)

def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _candidate_identity(candidate, environment_sha):
    return dict(version=candidate['version'], candidate_id=candidate['candidate_id'],
        wheel_sha256=candidate['archives']['wheel']['sha256'],
        sdist_sha256=candidate['archives']['sdist']['sha256'],
        dependency_lock_sha256=environment_sha)


def _verify_installed(candidate):
    import readmixer_py
    package = Path(readmixer_py.__file__).resolve().parent
    prefix = Path(sys.prefix).resolve()
    configuration = prefix / 'pyvenv.cfg'
    if (sys.prefix == sys.base_prefix or not package.is_relative_to(prefix)
            or not configuration.is_file()
            or 'include-system-site-packages = false' not in configuration.read_text().lower()):
        raise ValueError('runtime environment is not a fully isolated virtual environment')
    if not any(part in ('site-packages', 'dist-packages') for part in package.parts):
        raise ValueError('runtime import is not an installed package')
    expected = {name.removeprefix('src/readmixer_py/'): value
                for name, value in candidate['source_manifest'].items()
                if name.startswith('src/readmixer_py/') and name.endswith('.py')}
    actual = {str(path.relative_to(package)): fingerprint(path) for path in package.rglob('*.py')}
    if not expected or actual != expected:
        raise ValueError('installed module bytes differ from the frozen candidate')
    return dict(installed_module_count=len(actual), installed_module_bytes_verified=True,
                installed_location_checked=True, private_location_published=False)


def check_harness(expected):
    actual = {name: fingerprint(Path(__file__).parent / name) for name in HARNESS}
    if actual != expected:
        raise ValueError('external harness changed during the declared invocation')


def observed_cpu_check(cell, result):
    reasons = []
    requested = cell['configuration']['workers_requested']
    for index, row in enumerate([result.get('warmup'), *result.get('measured_runs', [])]):
        if not isinstance(row, dict) or row.get('outcome') == 'typed_resource_refusal':
            continue
        for phase, facts in (row.get('phases') or {}).items():
            # Verification's public report exposes its admitted CPU budget in
            # workers_effective; producer/recovery reports expose cpu_budget.
            actual = facts.get('workers_effective') if phase == 'verify' else facts.get('cpu_budget')
            if actual != requested:
                reasons.append(f'observation {index}: {phase} did not admit the declared CPU budget')
    return dict(passed=not reasons, reasons=reasons)


def apply_portable_limits(controls):
    if set(controls) - {'nofile'}:
        raise ValueError('nonportable limit requested from portable launcher')
    if 'nofile' in controls:
        value = controls['nofile']
        if type(value) is not int or value <= 0:
            raise ValueError('descriptor ceiling must be a positive integer')
        _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(resource.RLIMIT_NOFILE, (value, hard))
    actual = list(resource.getrlimit(resource.RLIMIT_NOFILE))
    if 'nofile' in controls and actual[0] != controls['nofile']:
        raise ValueError('portable descriptor ceiling was not observed')
    return {'nofile': actual, 'native_thread_environment': {key: os.environ.get(key) for key in NATIVE}}


def _child(context_path):
    context = json.loads(Path(context_path).read_text())
    check_harness(context['harness_files'])
    # Child was started with native-thread variables already set, before Python
    # initialization. Check rather than setting them after native imports.
    if any(os.environ.get(name) != '1' for name in NATIVE):
        raise ValueError('native thread environment was not inherited')
    allocator_receipt = verify_allocator_environment(context['allocator_environment'])
    if context['system'] == 'Darwin':
        apply_portable_limits(context['host_controls'])
    candidate, environment = _bind(context, installed=True)
    installed = _verify_installed(candidate)
    scripts = Path(context['archived_scripts'])
    sys.path.insert(0, str(scripts))
    from run_qualification import run_cell, _ObservationJournal
    from qualification_matrix import qualification_matrix, validate_result
    matrix = qualification_matrix()
    cell = next(row for row in matrix['cells'] if row['id'] == context['cell_id'])
    identity = _candidate_identity(candidate, context['dependency_lock_sha256'])
    journal = Path(context['report_directory']) / (cell['id'] + '.observations.jsonl')
    declaration = dict(candidate=identity, matrix_cell=cell,
                       matrix_definition_sha256=matrix['definition_sha256'])
    with _ObservationJournal(journal, declaration) as sink:
        result = run_cell(cell, identity, host_controls=context['host_controls'],
            scratch_parent=context.get('scratch_parent'), observation_sink=sink.observe)
        sink.append(dict(kind='observations_finished', completed_observations=sink.observations,
                         qualification_requires_final_report=True))
    # Independently call the frozen acceptance checker, and never derive a pass
    # solely from an exit code or declared requested controls.
    acceptance = validate_result(cell, result)
    cpu_check = observed_cpu_check(cell, result)
    if not cpu_check['passed']:
        acceptance['completed'] = False
        acceptance['reasons'].extend(cpu_check['reasons'])
    result['external_plan17'] = dict(installed=installed,
        allocator_environment=allocator_receipt, allocator_environment_verified=True,
        installed_dependencies=environment['installed_dependency_payloads'],
        cpu_partition_check=cpu_check,
        harness_files={name: fingerprint(Path(__file__).parent / name) for name in HARNESS},
        environment_manifest_sha256=context['environment_manifest_sha256'],
        harness_sha256=fingerprint(__file__)['sha256'],
        portable_limits_only=context['system'] == 'Darwin',
        acceptance_recomputed=acceptance)
    result['completed'] = result.get('completed') is True and acceptance['completed']
    check_harness(context['harness_files'])
    write_new(Path(context['report_directory']) / (cell['id'] + '.json'), result)
    return 0 if result['completed'] else 1


def verify_installed_dependencies(base, environment):
    """Verify selected versions and wheel code bytes, including native modules.

    Installer-generated RECORD, launch shebangs and bytecode are intentionally
    excluded; module payloads and distribution METADATA retain exact wheel bytes.
    """
    prefix = Path(sys.prefix).resolve()
    normalize = lambda name: re.sub(r'[-_.]+', '-', name).lower()
    selected = {normalize(row['name']) for row in environment['wheels']} | {'readmixer-py'}
    installed = {normalize(dist.metadata['Name']) for dist in metadata.distributions()}
    if installed != selected:
        raise ValueError('installed distribution set differs from selected isolated environment')
    observed = []
    for wheel in environment['wheels']:
        distribution = metadata.distribution(wheel['name'])
        if distribution.version != wheel['version']:
            raise ValueError('installed dependency differs from selected version')
        checked = 0
        with zipfile.ZipFile(base / 'wheels' / wheel['filename']) as archive:
            for member in archive.infolist():
                name = member.filename
                if member.is_dir() or name.endswith(('/RECORD', '.pyc', '.pyo')):
                    continue
                if '.data/' in name:
                    _, relative = name.split('.data/', 1)
                    category, _, name = relative.partition('/')
                    if category not in ('purelib', 'platlib'):
                        continue
                target = Path(distribution.locate_file(name)).resolve()
                if not target.is_relative_to(prefix) or not target.is_file():
                    raise ValueError('selected dependency payload is outside isolated install')
                digest = hashlib.sha256()
                with archive.open(member) as payload:
                    for block in iter(lambda: payload.read(1 << 20), b''):
                        digest.update(block)
                expected = dict(bytes=member.file_size, sha256=digest.hexdigest())
                if fingerprint(target) != expected:
                    raise ValueError('installed dependency payload differs from selected wheel')
                checked += 1
        if checked == 0:
            raise ValueError('selected dependency has no checked installed files')
        observed.append(dict(name=wheel['name'], version=wheel['version'], checked_files=checked))
    return observed


def _bind(context, *, installed=False):
    # Imported externally, never from the frozen candidate's embedded Linux lock.
    from candidate import load_candidate
    from lock_environment import verify_environment
    candidate = load_candidate(Path(context['candidate']))
    from candidate import read_json
    base = Path(context['environment'])
    environment = read_json(base / 'environment.json')
    if installed:
        if context.get('system') != platform.system():
            raise ValueError('child platform differs from declared launcher platform')
        for name, path in (('environment_manifest_sha256', base / 'environment.json'),
                           ('dependency_lock_sha256', base / 'requirements.lock')):
            if context.get(name) != fingerprint(path)['sha256']:
                raise ValueError('child environment identity differs from launcher declaration')
        environment = verify_environment(base / 'wheels', environment,
                                         (base / 'requirements.lock').read_bytes())
        environment = dict(environment, installed_dependency_payloads=verify_installed_dependencies(base, environment))
    verify_scripts(context['archived_scripts'], candidate)
    return candidate, environment


def _main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate', type=Path)
    parser.add_argument('--environment', type=Path)
    parser.add_argument('--installed-python', type=Path)
    parser.add_argument('--archived-scripts', type=Path)
    parser.add_argument('--workspace', type=Path)
    parser.add_argument('--report', type=Path)
    parser.add_argument('--scratch-parent', type=Path)
    parser.add_argument('--profile', choices=('bounded', 'scale', 'physical'))
    parser.add_argument('--child', type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.child:
        return _child(args.child)
    if any(getattr(args, name) is None for name in ('candidate', 'environment', 'installed_python',
            'archived_scripts', 'workspace', 'report', 'profile')):
        parser.error('all candidate, environment, interpreter, script, workspace, report and profile arguments are required')
    context = dict(candidate=str(args.candidate.resolve()), environment=str(args.environment.resolve()),
        archived_scripts=str(args.archived_scripts.resolve()), system=platform.system(),
        allocator_environment=allocator_environment())
    candidate, environment = _bind(context)
    args.workspace.mkdir(mode=0o700)
    args.report.mkdir(mode=0o700)
    snapshots = args.report / 'harness'
    snapshot_harness(snapshots)
    context.update(report_directory=str(args.report.resolve()),
                   harness_files={name: fingerprint(Path(__file__).parent / name) for name in HARNESS})
    sys.path.insert(0, context['archived_scripts'])
    from qualification_matrix import qualification_matrix
    matrix = qualification_matrix()
    cells, excluded = select_cells(matrix, args.profile, context['system'])
    environment_file = args.environment / 'environment.json'
    context['environment_manifest_sha256'] = fingerprint(environment_file)['sha256']
    context['dependency_lock_sha256'] = fingerprint(args.environment / 'requirements.lock')['sha256']
    header = dict(resource_lane_schema=1, profile=args.profile, system=context['system'],
        machine=platform.machine(), candidate=_candidate_identity(candidate, context['dependency_lock_sha256']),
        allocator_environment=context['allocator_environment'],
        environment_manifest_sha256=context['environment_manifest_sha256'],
        harness_sha256=fingerprint(__file__)['sha256'],
        harness_files={name: fingerprint(Path(__file__).parent / name) for name in HARNESS}, matrix_definition_sha256=matrix['definition_sha256'],
        selected_cells=[cell['id'] for cell in cells], excluded_cells=excluded,
        pending_separate_evidence=list(PENDING), plan17_complete=False,
        scope='Selected frozen cells only; native calibration, startup, signals and filesystem provenance remain separate.')
    write_new(args.report / 'declaration.json', header)
    environment_vars = inherited_environment(args.workspace.resolve())
    outcomes = []
    for cell in cells:
        scratch = None
        if cell['private_controls'].get('require_distinct_devices'):
            if args.scratch_parent is None or args.scratch_parent.stat().st_dev == args.workspace.stat().st_dev:
                outcomes.append(dict(cell_id=cell['id'], status='not_run', reason='a distinct scratch device is required'))
                continue
            scratch = args.scratch_parent.resolve()
        limits = host_limits(cell, context['system'],
            os.sched_getaffinity(0) if hasattr(os, 'sched_getaffinity') else ())
        if 'scratch_device_quota_bytes' in cell['private_controls']:
            scratch = args.workspace / (cell['id'] + '-mount')
            scratch.mkdir(mode=0o700)
            limits['scratch_mount'] = dict(path=str(scratch.resolve()),
                bytes=cell['private_controls']['scratch_device_quota_bytes'])
        child_context = dict(context, cell_id=cell['id'], host_controls=limits,
                            scratch_parent=str(scratch) if scratch else None)
        context_path = args.report / (cell['id'] + '.context.private.json')
        write_new(context_path, child_context)
        command = [str(args.installed_python.absolute()), '-I', str(Path(__file__).resolve()),
                   '--child', str(context_path.resolve())]
        started_utc = utc_now()
        command_path = args.report / (cell['id'] + '.command.private.json')
        write_new(command_path, command_record(command, environment_vars, started_utc))
        started = time.monotonic()
        try:
            with (args.report / (cell['id'] + '.stdout.private')).open('xb') as stdout, \
                 (args.report / (cell['id'] + '.stderr.private')).open('xb') as stderr:
                os.fchmod(stdout.fileno(), 0o600)
                os.fchmod(stderr.fileno(), 0o600)
                if context['system'] == 'Linux':
                    # Preserve the frozen Linux launcher and its kernel witnesses.
                    # An environment-only wrapper sets native limits/TMPDIR before
                    # the installed interpreter starts; no shell is involved.
                    host = _load_module('_p17_frozen_host', args.archived_scripts / 'qualification_host.py')
                    with _temporary_environment(environment_vars):
                        launch = host.run_host(command, controls=limits,
                            report=args.report / (cell['id'] + '.host.json'), stdout=stdout)
                else:
                    launch = portable_launch(command, environment=environment_vars, stdout=stdout, stderr=stderr, diagnostic_prefix=args.report / (cell['id'] + '.portable-launch.private'))
            result_path = args.report / (cell['id'] + '.json')
            result = json.loads(result_path.read_text()) if result_path.exists() else {}
            status = 'accepted' if launch.get('returncode') == 0 and result.get('completed') is True else 'failed'
            outcomes.append(dict(cell_id=cell['id'], status=status, launch=launch,
                report=fingerprint(result_path) if result_path.exists() else None,
                wall_seconds=time.monotonic() - started))
        except Exception as error:
            outcomes.append(dict(cell_id=cell['id'], status='failed', error_type=type(error).__name__,
                                 diagnostic_text_published=False))
        finally:
            if scratch is not None and 'scratch_device_quota_bytes' in cell['private_controls']:
                try:
                    scratch.rmdir()
                except OSError:
                    if not outcomes or outcomes[-1]['cell_id'] != cell['id']:
                        outcomes.append(dict(cell_id=cell['id'], status='failed'))
                    outcomes[-1]['owned_scratch_parent_cleanup_complete'] = False
                    outcomes[-1]['status'] = 'failed'
        if context['system'] != 'Linux':
            outcomes[-1]['portable_launch_evidence'] = portable_launch_evidence(
                args.report / (cell['id'] + '.portable-launch.private'))
        if context['system'] != 'Linux' and portable_launch_requires_hold(outcomes[-1]['portable_launch_evidence']):
            outcomes[-1].update(status='failed', halted_for_unresolved_launch_cleanup=True)
        outcomes[-1].update(started_utc=started_utc, ended_utc=utc_now(),
            command_ref=command_path.name, command=fingerprint(command_path))
        for suffix in ('stdout.private', 'stderr.private'):
            path = args.report / (cell['id'] + '.' + suffix)
            if path.exists():
                outcomes[-1][suffix.replace('.', '_')] = fingerprint(path)
        execution_path = args.report / (cell['id'] + '.execution.private.json')
        write_new(execution_path, dict(command_ref=command_path.name, command=fingerprint(command_path),
            started_utc=started_utc, ended_utc=outcomes[-1]['ended_utc'],
            returncode=(outcomes[-1].get('launch') or {}).get('returncode'),
            error_type=outcomes[-1].get('error_type'), status=outcomes[-1]['status']))
        outcomes[-1]['execution'] = fingerprint(execution_path)
        write_new(args.report / (cell['id'] + '.launch.json'), outcomes[-1])
        if outcomes[-1].get('halted_for_unresolved_launch_cleanup'):
            break
    complete = len(outcomes) == len(cells) and all(row['status'] == 'accepted' for row in outcomes)
    files = {path.name: fingerprint(path) for path in args.report.iterdir() if path.is_file()}
    write_new(args.report / 'resource-lane.json', dict(header, cells=outcomes,
        selected_profile_complete=complete, files=files))
    return 0 if complete else 1


class _temporary_environment:
    """Single-threaded external launcher only; restore its parent environment."""
    def __init__(self, environment):
        self.environment = environment
    def __enter__(self):
        self.previous = dict(os.environ)
        os.environ.clear()
        os.environ.update(self.environment)
    def __exit__(self, *_):
        os.environ.clear()
        os.environ.update(self.previous)


def main(argv=None):
    # With -I Python does not add this script directory to sys.path. Only this
    # separately fingerprinted orchestration directory is added, never src/.
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    previous = signal.getsignal(signal.SIGTERM)
    signal_number = signal.SIGINT
    def interrupted(signum, _frame):
        nonlocal signal_number
        signal_number = signum
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    try:
        return _main(argv)
    except KeyboardInterrupt:
        return 128 + signal_number
    except Exception as error:
        print(json.dumps(dict(completed=False, error_type=type(error).__name__,
                              diagnostic_text_published=False), sort_keys=True), file=sys.stderr)
        return 1
    finally:
        signal.signal(signal.SIGTERM, previous)


if __name__ == '__main__':
    raise SystemExit(main())
