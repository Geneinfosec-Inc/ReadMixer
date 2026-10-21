#!/usr/bin/env python3
"""Measure a correct synthetic mix -> verify -> both recovery modes workflow.

One warm-up and at least five measured runs are mandatory. This tool does not
clear OS caches, compare speedups, qualify a memory envelope, or complete the
release host matrix. Timing includes operation initialization within one already-imported Python
process. --startup adds separate fresh-interpreter, API-import, and CLI-inspect
lanes. Fresh production entropy is used.
By default all generated files live in a new temporary directory and are
removed; --work-dir retains fixtures and pools in a new directory. Restored
outputs are independently checked and removed after each recovery mode.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager, nullcontext
import gzip
import hashlib
from importlib import metadata
import json
import os
from pathlib import Path
import platform
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import zlib

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_HERE.parent / 'src'))

import psutil
from generate_fixtures import (FixtureConfig, add_fixture_arguments, config_from_arguments,
                               generate_fixtures, parse_counts)
import readmixer_py
from readmixer_py import __version__, mix, recover, verify
from readmixer_py.config import MixConfig, RecoverConfig, RecoveryMode, StorageMode, VerifyConfig, parse_size
from readmixer_py.errors import ConfigurationError, IntegrityError

_PHASES = ('mix', 'verify', 'recover_indexed', 'recover_sequential')


def _reaped_child_cpu():
    # Unix getrusage retains CPU for waited children even when they disappear
    # entirely between process-tree observations. Never add sampled live CPU to
    # this value: doing so can count the same child twice after it is reaped.
    try:
        import resource
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    except (ImportError, AttributeError, OSError):
        return None
    return usage.ru_utime + usage.ru_stime


def _cpu_result(coordinator, before, after):
    reaped = None if before is None or after is None else max(0.0, after - before)
    return dict(process_cpu_seconds=coordinator, reaped_child_cpu_seconds=reaped,
                aggregate_accounted_cpu_seconds=None if reaped is None else coordinator + reaped,
                child_cpu_accounting='waited child/descendant usage; unjoined children are excluded'
                    if reaped is not None else 'unavailable on this platform')



def _scratch_size(directory):
    total = 0
    # Streaming traversal: do not retain every scratch filename in memory.
    def visit(path):
        nonlocal total
        try:
            with os.scandir(path) as entries:
                for entry in entries:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            visit(entry.path)
                        elif entry.is_file(follow_symlinks=False):
                            total += entry.stat(follow_symlinks=False).st_size
                    except FileNotFoundError:
                        pass  # Scratch ownership may change between samples.
        except FileNotFoundError:
            pass
    visit(directory)
    return total


class ProcessSampler:
    """Sample an observed root and its descendants, retaining one thread snapshot.

    Short-lived peaks between observations can be missed; summed RSS can count
    shared pages more than once. Descriptor and scratch observations have the
    same sampled, lower-bound peak limitation. The monitor adds one thread.
    """
    def __init__(self, scratch, interval=0.02, *, process=None):
        self.scratch = Path(scratch)
        self.interval = interval
        self.stop = threading.Event()
        self.process = psutil.Process() if process is None else process
        self._thread_previous = {}
        self.thread = None
        self.result = dict(sample_interval_seconds=interval, observations=0,
            observation_errors=0, process_tree_peak_rss_bytes=0,
            process_tree_peak_descriptors=None, process_tree_peak_threads=0,
            process_tree_peak_processes=0, scratch_peak_physical_bytes=0,
            process_tree_last_rss_bytes=0, process_tree_last_processes=0,
            process_tree_last_descendants=0, sampled_live_descendant_cpu_seconds=0.0,
            sampled_peak_active_os_threads=0, sampled_os_thread_cpu_delta_seconds=0.0,
            thread_scope='OS thread CPU deltas between samples, including Python and monitor threads; not library attribution')

    def _sample(self):
        try:
            processes = [self.process, *self.process.children(recursive=True)]
        except psutil.Error:
            self.result['observation_errors'] += 1
            return
        rss = threads = descriptors = alive = active_threads = 0
        descendants = 0
        live_cpu = thread_cpu_delta = 0.0
        current_threads = {}
        has_descriptors = hasattr(self.process, 'num_fds')
        for process in processes:
            try:
                with process.oneshot():
                    rss += process.memory_info().rss
                    threads += process.num_threads()
                    identity = (process.pid, process.create_time())
                    for thread in process.threads():
                        key = (*identity, thread.id)
                        value = thread.user_time + thread.system_time
                        current_threads[key] = value
                        previous = self._thread_previous.get(key)
                        if previous is not None and value > previous:
                            active_threads += 1
                            thread_cpu_delta += value - previous
                    if process.pid != self.process.pid:
                        usage = process.cpu_times()
                        live_cpu += usage.user + usage.system
                        descendants += 1
                    if has_descriptors:
                        descriptors += process.num_fds()
                    alive += 1
            except psutil.Error:
                self.result['observation_errors'] += 1
        self._thread_previous = current_threads
        self.result['observations'] += 1
        self.result['process_tree_last_rss_bytes'] = rss
        self.result['process_tree_last_processes'] = alive
        self.result['process_tree_last_descendants'] = descendants
        self.result['sampled_live_descendant_cpu_seconds'] = live_cpu
        self.result['sampled_peak_active_os_threads'] = max(self.result['sampled_peak_active_os_threads'], active_threads)
        self.result['sampled_os_thread_cpu_delta_seconds'] += thread_cpu_delta
        for field, value in (('process_tree_peak_rss_bytes', rss),
                             ('process_tree_peak_threads', threads),
                             ('process_tree_peak_processes', alive)):
            self.result[field] = max(self.result[field], value)
        if has_descriptors:
            self.result['process_tree_peak_descriptors'] = max(
                self.result['process_tree_peak_descriptors'] or 0, descriptors)
        try:
            self.result['scratch_peak_physical_bytes'] = max(
                self.result['scratch_peak_physical_bytes'], _scratch_size(self.scratch))
        except OSError:
            self.result['observation_errors'] += 1

    def _run(self):
        while not self.stop.wait(self.interval):
            self._sample()

    def __enter__(self):
        self._sample()
        self.thread = threading.Thread(target=self._run, name='readmixer-benchmark-monitor', daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join()
        self._sample()


def summarize(values):
    values = list(values)
    if not values:
        raise ValueError('cannot summarize an empty measurement series')
    return dict(observations=values, median=statistics.median(values), minimum=min(values),
                maximum=max(values), range=max(values) - min(values))


def summarize_optional(values):
    values = list(values)
    return None if any(value is None for value in values) else summarize(values)


def check_recovered(report, directory, expected):
    """Independent stdlib gzip/hash/newline oracle; no production record parser."""
    states = {sample['sample_index']: dict(hashes=[hashlib.sha256(), hashlib.sha256()],
              sizes=[0, 0], lines=[0, 0], next_part=0, pairs=sample['pairs']) for sample in expected}
    previous = -1
    for part in report.outputs:
        index = part['sample_index']
        if type(index) is not int or index not in states or index < previous:
            raise IntegrityError('benchmark recovery returned unexpected sample ordering')
        previous = index
        state = states[index]
        if part['part_index'] != state['next_part']:
            raise IntegrityError('benchmark recovery returned unexpected part ordering')
        state['next_part'] += 1
        for role, key in enumerate(('r1', 'r2')):
            with gzip.open(directory / part[key]['name'], 'rb') as source:
                while block := source.read(1 << 16):
                    state['hashes'][role].update(block)
                    state['sizes'][role] += len(block)
                    state['lines'][role] += block.count(b'\n')
    for sample in expected:
        state = states[sample['sample_index']]
        if state['next_part'] < 1:
            raise IntegrityError('benchmark recovery omitted a selected sample')
        for role, key in enumerate(('r1', 'r2')):
            if (state['hashes'][role].hexdigest() != sample[key]['normalized_sha256']
                    or state['sizes'][role] != sample[key]['normalized_bytes']
                    or state['lines'][role] != 4 * sample['pairs']):
                raise IntegrityError('benchmark recovered bytes differ from the independent fixture oracle')


class PhaseFailure(Exception):
    """A private failed observation, excluding exception text and input values."""
    def __init__(self, measurement):
        super().__init__('benchmark phase failed')
        self.measurement = measurement


def _timed_phase(action, scratch, *, output=None, telemetry=False):
    if telemetry:
        from resource_telemetry import ResourceTelemetry
        observer = ResourceTelemetry(scratch, output=output)
    else:
        observer = nullcontext(None)
    error = None
    report = None
    with ProcessSampler(scratch) as sampler, observer as detailed:
        children_start = _reaped_child_cpu()
        wall_start, cpu_start = time.perf_counter(), time.process_time()
        try:
            report = action()
            if not report.completed:
                raise IntegrityError('benchmark operation did not complete')
            if not getattr(report, 'cleanup_complete', True):
                raise IntegrityError('benchmark operation left cleanup residue')
        except Exception as failure:
            error = type(failure).__name__
        wall = time.perf_counter() - wall_start
        cpu = time.process_time() - cpu_start
        children_end = _reaped_child_cpu()
    measured = dict(completed=error is None, wall_seconds=wall,
        **_cpu_result(cpu, children_start, children_end), sampled=sampler.result,
        detailed=None if detailed is None else detailed.result)
    if error is not None:
        measured.update(error=error, cleanup_complete=False)
        raise PhaseFailure(measured)
    measured.update(reported_elapsed_seconds=report.elapsed_seconds,
        samples=report.samples, pairs=report.pairs, reads=report.reads,
        normalized_bytes=report.normalized_bytes,
        physical_bytes=getattr(report, 'physical_bytes', None),
        consumed_artifact_bytes=getattr(report, 'consumed_artifact_bytes', None),
        workers_effective=report.workers_effective,
        cpu_budget=getattr(report, 'cpu_budget', None),
        compression_slots=getattr(report, 'compression_slots', 0),
        resources=report.resources.to_dict(), cleanup_complete=True,
        normalized_bytes_per_second=report.normalized_bytes / wall,
        pairs_per_second=report.pairs / wall)
    return report, measured


def _iteration(root, fixture, *, workers, storage, max_ram, max_artifact_bytes,
               compression_level, selected, telemetry=False, private_options=None):
    root.mkdir(mode=0o700)
    _iteration_root_owned.set(True)
    scratch_parent = (private_options or {}).get('scratch_parent')
    scratch = (Path(tempfile.mkdtemp(prefix='readmixer-scratch-', dir=scratch_parent))
               if scratch_parent else root / 'scratch')
    _iteration_scratch.set(scratch)
    scratch.mkdir(mode=0o700, exist_ok=bool(scratch_parent))
    expected = [fixture['samples'][index] for index in selected]
    expected_pairs = sum(sample['pairs'] for sample in expected)
    expected_bytes = sum(sample[key]['normalized_bytes'] for sample in expected for key in ('r1', 'r2'))
    common = dict(workers=workers, storage=storage, max_ram=max_ram, temp_dir=scratch,
                  max_record_bytes=max(1, fixture['max_record_bytes']))
    phases = _iteration_phases.get()
    if phases is None:
        phases = {}
    children_start = _reaped_child_cpu()
    start, cpu_start = time.perf_counter(), time.process_time()
    pool = root / 'pool'
    mix_action = _iteration_controls.get().mix if _iteration_controls.get() else mix
    mixed, phases['mix'] = _timed_phase(lambda: mix_action(MixConfig(
        input_dir=root.parent / 'fixtures' / 'input', output=pool,
        max_artifact_bytes=max_artifact_bytes, compression_level=compression_level, **common)), scratch, output=pool, telemetry=telemetry)
    if (mixed.samples != len(fixture['samples']) or mixed.pairs != fixture['pairs']
            or mixed.reads != fixture['reads'] or mixed.normalized_bytes != fixture['normalized_bytes']):
        raise IntegrityError('benchmark mix totals disagree with the fixture oracle')
    artifact_summary = dict(count=0, largest_artifact_bytes=0, largest_data_artifact_bytes=0, bundles=mixed.bundles if hasattr(mixed, 'bundles') else 0)
    artifact_hashes = {}
    for artifact in pool.iterdir():
        if artifact.is_file() and artifact.name != mixed.manifest:
            size = artifact.stat().st_size
            artifact_summary['count'] += 1
            artifact_summary['largest_artifact_bytes'] = max(artifact_summary['largest_artifact_bytes'], size)
            if artifact.name.endswith('.fastq.gz'):
                artifact_summary['largest_data_artifact_bytes'] = max(artifact_summary['largest_data_artifact_bytes'], size)
            if (private_options or {}).get('fixed_test_context'):
                digest = hashlib.sha256()
                with artifact.open('rb') as stream:
                    while block := stream.read(1 << 20):
                        digest.update(block)
                artifact_hashes[artifact.name] = digest.hexdigest()
    manifest = pool / mixed.manifest
    artifact_summary['bundles'] = len(json.loads(manifest.read_bytes())['bundles'])
    verified, phases['verify'] = _timed_phase(lambda: verify(VerifyConfig(manifest=manifest, **common)), scratch, telemetry=telemetry)
    if (verified.samples != mixed.samples or verified.pairs != fixture['pairs']
            or verified.reads != fixture['reads'] or verified.normalized_bytes != fixture['normalized_bytes']):
        raise IntegrityError('benchmark full verification totals disagree with the fixture oracle')
    checks = 0.0
    for mode in (RecoveryMode.INDEXED, RecoveryMode.SEQUENTIAL):
        destination = root / ('recover_' + mode.value)
        restored, phases['recover_' + mode.value] = _timed_phase(lambda: recover(RecoverConfig(
            manifest=manifest, output=destination, sample=tuple(sample['sample_id'] for sample in expected),
            io=mode, max_artifact_bytes=max_artifact_bytes, compression_level=compression_level, **common)), scratch, output=destination, telemetry=telemetry)
        if (restored.samples != len(expected) or restored.pairs != expected_pairs
                or restored.reads != 2 * expected_pairs or restored.normalized_bytes != expected_bytes):
            raise IntegrityError('benchmark recovery totals disagree with the fixture oracle')
        check_start = time.perf_counter()
        check_recovered(restored, destination, expected)
        checks += time.perf_counter() - check_start
        if not (private_options or {}).get('retain_recovery', False):
            shutil.rmtree(destination)
    if any(scratch.iterdir()):
        raise IntegrityError('benchmark workflow left scratch residue')
    return dict(wall_seconds=time.perf_counter() - start,
                **_cpu_result(time.process_time() - cpu_start, children_start, _reaped_child_cpu()),
                independent_recovery_check_seconds=checks, phases=phases, artifact_summary=artifact_summary,
                fixed_context_artifact_sha256=artifact_hashes or None,
                completed=True, correctness=dict(full_pool_verified=True, indexed_recovery_equal=True,
                                 sequential_recovery_equal=True, independent_bytes_and_counts_equal=True,
                                 exact_identity_coverage_checked_by_pipeline=True))


# Context-local observations permit private concurrent callers without a global
# report list. Runtime configuration and all public operations remain unchanged.
from contextvars import ContextVar
_iteration_phases = ContextVar('benchmark_iteration_phases', default=None)
_iteration_controls = ContextVar('benchmark_iteration_controls', default=None)
_iteration_scratch = ContextVar('benchmark_iteration_scratch', default=None)
_iteration_root_owned = ContextVar('benchmark_iteration_root_owned', default=False)


def _runtime_helpers(children):
    """Recognize the stdlib tracker by its authoritative PID and exact command.

    Spawn workers are operation-owned and must exit. Python's resource tracker
    intentionally persists until interpreter shutdown; it is reported separately
    and never treated as an unknown worker or silently excluded by process name.
    """
    from multiprocessing import resource_tracker
    tracker = getattr(resource_tracker._resource_tracker, '_pid', None)
    known = set()
    for child in children:
        if child.pid != tracker:
            continue
        try:
            command = child.cmdline()
            index = command.index('-c')
            code = command[index + 1]
            if (child.ppid() == os.getpid() and
                    re.fullmatch(r'from multiprocessing\.resource_tracker import main;main\([0-9]+\)', code)):
                known.add(child.pid)
        except (psutil.Error, ValueError, IndexError):
            pass
    return known


def _observed_iteration(root, fixture, *, retain=False, **options):
    if root.exists() or root.is_symlink():
        raise FileExistsError('benchmark iteration requires a new owned directory')
    phases = {}
    original_root = root.parent / 'fixtures' / 'input'
    def snapshots():
        values = {}
        for path in original_root.iterdir():
            info = path.lstat()
            values[path.name] = (info.st_dev, info.st_ino, info.st_mode, info.st_size,
                                 info.st_mtime_ns, info.st_ctime_ns)
        return values
    before = snapshots()
    sentinel = root.parent / 'foreign.keep'
    sentinel_expected = sentinel.exists()
    children_before = {child.pid for child in psutil.Process().children(recursive=True)}
    token = _iteration_phases.set(phases)
    scratch_token = _iteration_scratch.set(None)
    owner_token = _iteration_root_owned.set(False)
    control_options = dict(options.get('private_options') or {})
    for option in ('scratch_parent', 'retain_recovery', 'require_normalized_over_four_actual_budgets'):
        control_options.pop(option, None)
    context = None
    result = None
    start, cpu_start, child_start = time.perf_counter(), time.process_time(), _reaped_child_cpu()
    try:
        if control_options:
            from qualification_controls import QualificationControls
            context = QualificationControls(control_options)
        else:
            context = nullcontext(None)
        try:
            with context as controls:
                control_token = _iteration_controls.set(controls)
                try:
                    result = _iteration(root, fixture, **options)
                finally:
                    _iteration_controls.reset(control_token)
        except PhaseFailure as failure:
            phase = next(name for name in _PHASES if name not in phases)
            phases[phase] = failure.measurement
            result = dict(completed=False, error=failure.measurement['error'], failed_phase=phase,
                wall_seconds=time.perf_counter()-start,
                **_cpu_result(time.process_time()-cpu_start, child_start, _reaped_child_cpu()),
                phases=phases, correctness=None)
        except Exception as failure:
            result = dict(completed=False, error=type(failure).__name__, failed_phase='independent_validation',
                wall_seconds=time.perf_counter()-start,
                **_cpu_result(time.process_time()-cpu_start, child_start, _reaped_child_cpu()),
                phases=phases, correctness=None)
        scratch = _iteration_scratch.get()
        result['algorithmic_scratch_empty'] = scratch is None or not scratch.exists() or not any(scratch.iterdir())
        result['private_control_observations'] = getattr(context, 'result', None)
        result['originals_unchanged'] = before == snapshots()
        children_after = psutil.Process().children(recursive=True)
        helpers = _runtime_helpers(children_after)
        result['children_reaped'] = not ({child.pid for child in children_after} - children_before - helpers)
        result['persistent_runtime_helpers'] = dict(resource_tracker_count=len(helpers),
            created_during_iteration=len(helpers - children_before),
            scope='stdlib resource tracker persists to interpreter shutdown; all other new descendants must be reaped')
        if sentinel_expected:
            try:
                with sentinel.open('rb') as source:
                    result['foreign_files_unchanged'] = source.read(64) == b'qualification-owned sentinel\n'
            except OSError:
                result['foreign_files_unchanged'] = False
        else:
            result['foreign_files_unchanged'] = None
        if result['completed'] and (not result['algorithmic_scratch_empty'] or
                not result['originals_unchanged'] or not result['children_reaped'] or
                (sentinel_expected and not result['foreign_files_unchanged'])):
            result.update(completed=False, error='QualificationIntegrityError',
                          failed_phase='post_workflow_validation')
        return result
    finally:
        scratch, owned = _iteration_scratch.get(), _iteration_root_owned.get()
        _iteration_phases.reset(token)
        _iteration_scratch.reset(scratch_token)
        _iteration_root_owned.reset(owner_token)
        cleanup_start = time.perf_counter()
        pending_error = sys.exc_info()[1]
        try:
            if scratch is not None and scratch.parent != root and scratch.exists():
                shutil.rmtree(scratch)
            if owned and not retain and root.exists():
                shutil.rmtree(root)
        except OSError as failure:
            if result is not None:
                if result.get('completed'):
                    result.update(completed=False, error=type(failure).__name__, failed_phase='owned_harness_cleanup')
                result['owned_cleanup_error'] = type(failure).__name__
            elif pending_error is not None:
                pending_error.add_note('owned benchmark cleanup also failed')
            else:
                raise
        if result is not None:
            result['owned_iteration_cleanup_seconds'] = time.perf_counter() - cleanup_start
            result['iteration_outputs_retained'] = retain and owned


# This child deliberately imports monitoring dependencies before its first
# ReadMixer import. Parent-observed launch time includes that overhead; the
# import/operation interval and before/after snapshots are separately labeled.
_STARTUP_CHILD = r'''
import contextlib, io, json, os, pathlib, runpy, sys, time
import psutil
kind, package_file, input_dir, maximum_ram, record_limit = sys.argv[1:]
process = psutil.Process()
def snapshot():
    cpu = process.cpu_times()
    try:
        import resource
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        peak = peak if sys.platform == 'darwin' else peak * 1024
    except (ImportError, AttributeError, OSError):
        peak = None
    return dict(rss_bytes=process.memory_info().rss, lifetime_peak_rss_bytes=peak, os_threads=process.num_threads(),
                descriptors=process.num_fds() if hasattr(process, 'num_fds') else None,
                process_cpu_seconds=cpu.user + cpu.system)
before = snapshot()
start = time.perf_counter()
import readmixer_py
candidate_matches = pathlib.Path(readmixer_py.__file__).resolve() == pathlib.Path(package_file).resolve()
import_seconds = time.perf_counter() - start
operation = None
operation_seconds = None
exit_code = 0
if kind == 'cli_inspect':
    sys.argv = ['readmixer-py', 'inspect', '--input', input_dir, '--deep', '--json',
                '--workers', '1', '--max-ram', maximum_ram, '--max-record-bytes', record_limit]
    capture = io.StringIO()
    start = time.perf_counter()
    with contextlib.redirect_stdout(capture):
        try:
            runpy.run_module('readmixer_py', run_name='__main__')
        except SystemExit as error:
            exit_code = error.code or 0
    operation_seconds = time.perf_counter() - start
    operation = json.loads(capture.getvalue())
after = snapshot()
print(json.dumps(dict(completed=exit_code == 0 and candidate_matches,
    candidate_matches=candidate_matches, exit_code=exit_code,
    import_seconds=import_seconds, cli_operation_seconds=operation_seconds,
    before_readmixer_import=before, after_operation=after, operation=operation)))
'''


def _startup_observation(kind, input_dir, fixture, *, max_ram, timeout=120):
    # Import the exact package already measured by the in-process harness,
    # including when the scripts are copied outside an installed wheel tree.
    environment = os.environ.copy()
    package_file = Path(readmixer_py.__file__).resolve()
    environment['PYTHONPATH'] = str(package_file.parent.parent)
    command = ([sys.executable, '-c', 'pass'] if kind == 'interpreter' else
               [sys.executable, '-c', _STARTUP_CHILD, kind, str(package_file), str(input_dir),
                str(max_ram), str(max(1, fixture['max_record_bytes']))])
    before = _reaped_child_cpu()
    wall_start, parent_cpu_start = time.perf_counter(), time.process_time()
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=environment)
    timed_out = False
    try:
        try:
            observed = psutil.Process(process.pid)
        except psutil.NoSuchProcess:
            observed = None
        if observed is not None:
            with ProcessSampler(input_dir.parent / 'no-startup-scratch', process=observed) as sampler:
                try:
                    stdout, stderr = process.communicate(timeout=timeout)
                except subprocess.TimeoutExpired:
                    timed_out = True
                    process.kill()
                    stdout, stderr = process.communicate()
            sampled = sampler.result
        else:
            stdout, stderr = process.communicate(timeout=timeout)
            sampled = None
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()
    wall = time.perf_counter() - wall_start
    parent_cpu = time.process_time() - parent_cpu_start
    after = _reaped_child_cpu()
    child_cpu = None if before is None or after is None else max(0.0, after - before)
    detail = None
    error = None
    completed = process.returncode == 0 and not stderr and not timed_out
    if completed and kind != 'interpreter':
        try:
            detail = json.loads(stdout)
            completed = detail['completed'] is True and detail['candidate_matches'] is True
            if kind == 'cli_inspect':
                operation = detail['operation']
                completed = completed and operation['completed'] is True and all(
                    operation[key] == fixture[key] for key in ('pairs', 'reads', 'normalized_bytes'))
                completed = completed and operation['samples'] == len(fixture['samples'])
        except (ValueError, KeyError, TypeError):
            completed = False
            error = 'invalid_child_report'
    elif completed and stdout:
        completed, error = False, 'unexpected_interpreter_output'
    if not completed and error is None:
        error = 'timeout' if timed_out else 'child_failed_or_correctness_mismatch'
    # Diagnostic contents can include paths. Preserve status/byte counts only.
    return dict(completed=completed, wall_seconds=wall, child_cpu_seconds=child_cpu,
                parent_monitor_cpu_seconds=parent_cpu, exit_code=process.returncode,
                timed_out=timed_out, error=error, stdout_bytes=len(stdout), stderr_bytes=len(stderr),
                sampled=sampled, child=detail)


def startup_benchmark(input_dir, fixture, *, repetitions=5, max_ram=512 << 20):
    """Separate launch/import/real CLI lanes; failed observations stay in JSON."""
    if type(repetitions) is not int or not 5 <= repetitions <= 1000:
        raise ValueError('startup benchmark requires 5..1000 repetitions')
    lanes = {}
    for kind in ('interpreter', 'api_import', 'cli_inspect'):
        warmup = _startup_observation(kind, Path(input_dir), fixture, max_ram=max_ram)
        measured = [_startup_observation(kind, Path(input_dir), fixture, max_ram=max_ram)
                    for _ in range(repetitions)]
        complete = all(run['completed'] for run in (warmup, *measured))
        # Failed cells retain observations, but never acquire a performance
        # summary that might be mistaken for a correct measured configuration.
        summary = None
        if complete:
            summary = dict(wall_seconds=summarize(run['wall_seconds'] for run in measured))
            if all(run['child_cpu_seconds'] is not None for run in measured):
                summary['child_cpu_seconds'] = summarize(run['child_cpu_seconds'] for run in measured)
        lanes[kind] = dict(completed=complete, warmup=warmup, measured_runs=measured, summary=summary)
    return dict(startup_version=1, completed=all(lane['completed'] for lane in lanes.values()),
                versions=_versions(), lanes=lanes,
                launch_scope='parent monotonic interval includes process creation, execution, monitoring and wait',
                import_scope='fresh child import interval excludes earlier stdlib/psutil observer imports',
                cli_scope='python module entry point performs deep inspect; not a complete fresh-process mix workflow',
                cpu_scope='getrusage waited-child CPU, including exited descendants accounted by their parent; observer CPU separate',
                native_scope='before ReadMixer import and after operation OS-thread counts plus sampled thread deltas; no library attribution',
                cache_policy='one warm-up per lane; inputs and OS caches reused; no cache clearing',
                limitations=['observer imports affect fresh-child RSS and measured launch time',
                             'short process/thread/RSS peaks can fall between observations',
                             'API import is lazy and does not represent all operation native imports',
                             'no release performance or native-workspace qualification'])


def _versions():
    libraries = {}
    for name in ('numpy', 'pycryptodome', 'psutil'):
        libraries[name] = metadata.version(name)
    package = Path(readmixer_py.__file__).resolve().parent
    files = sorted(package.rglob('*.py'))
    if not files:
        raise RuntimeError('cannot fingerprint the imported package Python files')
    digest = hashlib.sha256()
    for path in files:
        relative = Path('readmixer_py') / path.relative_to(package)
        digest.update(relative.as_posix().encode('utf-8') + b'\0')
        digest.update(path.read_bytes())
    candidate = ('source-tree' if package == (_HERE.parent / 'src' / 'readmixer_py').resolve()
                 else 'installed-package')
    tooling = {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
               for path in sorted(_HERE.glob('*.py')) if path.is_file()}
    return dict(readmixer_py=__version__, python=platform.python_version(), tooling_sha256=tooling,
                python_implementation=platform.python_implementation(),
                platform=platform.system(), platform_release=platform.release(),
                architecture=platform.machine(), dependencies=libraries,
                zlib_build=zlib.ZLIB_VERSION, zlib_runtime=zlib.ZLIB_RUNTIME_VERSION,
                measured_candidate=candidate, source_python_sha256=digest.hexdigest(),
                fingerprinted_python_files=len(files))


@contextmanager
def _workspace(path):
    if path is not None:
        path = Path(path)
        path.mkdir(mode=0o700, exist_ok=False)
        yield path
    else:
        with tempfile.TemporaryDirectory(prefix='readmixer-benchmark-') as directory:
            # macOS's system temporary base can contain the /var symlink.
            # Resolve only this harness-owned path; runtime paths stay strict.
            yield Path(directory).resolve()


def benchmark(*, fixture_config=FixtureConfig(), work_dir=None, repetitions=5, workers=1,
              storage=StorageMode.AUTO, max_ram=512 << 20, max_artifact_bytes=1 << 20,
              compression_level=6, selected=None, startup=False, telemetry=False, private_options=None,
              observation_sink=None):
    """Measure complete workflows, optionally retaining each finished observation.

    The synchronous sink receives (kind, index, row, fixture) after each
    iteration's owned cleanup. It must persist without changing the borrowed
    dictionaries. Sink failures propagate; no successful report is returned.
    These checkpoints cover workflow observations, not optional startup lanes.
    """
    if observation_sink is not None and not callable(observation_sink):
        raise ValueError('observation_sink must be callable or None')
    if type(startup) is not bool or type(telemetry) is not bool:
        raise ValueError('startup and telemetry must be booleans')
    if type(repetitions) is not int or not 5 <= repetitions <= 1000:
        raise ValueError('benchmark requires 5..1000 measured repetitions after one warm-up')
    if sum(count > 0 for count in fixture_config.counts) < 2:
        raise ValueError('benchmark mix requires at least two nonempty samples')
    if selected is None:
        selected = tuple(range(len(fixture_config.counts)))
    if (type(selected) is not tuple or not selected or len(set(selected)) != len(selected)
            or any(type(index) is not int or not 0 <= index < len(fixture_config.counts) for index in selected)):
        raise ValueError('selected must contain distinct valid numeric sample indices')
    selected = tuple(sorted(selected))
    # Validate public runtime options before creating any output.
    MixConfig(input_dir=Path('input'), output=Path('output'), workers=workers, storage=storage,
              max_ram=max_ram, max_artifact_bytes=max_artifact_bytes, compression_level=compression_level,
              max_record_bytes=max(1, fixture_config.max_record_bytes))
    with _workspace(work_dir) as root:
        fixture = generate_fixtures(root / 'fixtures', fixture_config)
        (root / 'foreign.keep').write_bytes(b'qualification-owned sentinel\n')
        options = dict(workers=workers, storage=storage, max_ram=max_ram,
                       max_artifact_bytes=max_artifact_bytes, compression_level=compression_level,
                       selected=selected, telemetry=telemetry, private_options=private_options)
        warmup = _observed_iteration(root / 'warmup', fixture, retain=work_dir is not None, **options)
        if observation_sink is not None:
            observation_sink('warmup', 0, warmup, fixture)
        measured = []
        for number in range(repetitions):
            row = _observed_iteration(root / f'run-{number:04d}', fixture,
                                      retain=work_dir is not None, **options)
            if observation_sink is not None:
                observation_sink('measured', number, row, fixture)
            measured.append(row)
        startup_report = startup_benchmark(root / 'fixtures' / 'input', fixture, repetitions=repetitions,
                                           max_ram=max_ram) if startup else None
        correct = all(run['completed'] for run in (warmup, *measured))
        report = dict(completed=correct and (startup_report is None or startup_report['completed']), benchmark_version=3, scope='synthetic_complete_pipeline',
            versions=_versions(), fixture=fixture, startup=startup_report,
            configuration=dict(workers_requested=workers, storage=storage.value, max_ram=max_ram, startup_requested=startup, telemetry_requested=telemetry,
                max_artifact_bytes=max_artifact_bytes, compression_level=compression_level,
                selected_sample_indices=selected, selected_pairs=sum(fixture_config.counts[index] for index in selected),
                selected_pair_fraction=sum(fixture_config.counts[index] for index in selected) / fixture['pairs']),
            cache_policy='one warm-up; OS caches were not cleared; measured runs reuse the same input files',
            timing_scope='in-process operations after harness/API imports; lazy operation imports may occur;  fixture generation excluded; independent recovery checks included in total',
            cpu_time_scope='coordinator including threads plus getrusage waited-child CPU; live sampled descendants are reported separately, never double-added',
            sampling_scope='current process tree RSS sum; shared pages can be counted more than once; peaks between observations can be missed',
            monitor_overhead='one sampling thread; periodic observations overlap operation timings; entry/final observations and join are excluded from individual phases but included in workflow totals',
            warmup=warmup, measured_runs=measured,
            summary=None if not correct else dict(wall_seconds=summarize(run['wall_seconds'] for run in measured),
                         process_cpu_seconds=summarize(run['process_cpu_seconds'] for run in measured),
                         reaped_child_cpu_seconds=summarize_optional(run['reaped_child_cpu_seconds'] for run in measured),
                         aggregate_accounted_cpu_seconds=summarize_optional(run['aggregate_accounted_cpu_seconds'] for run in measured),
                         phases={phase: {
                             metric: summarize_optional(run['phases'][phase][metric] for run in measured)
                             for metric in ('wall_seconds', 'process_cpu_seconds', 'reaped_child_cpu_seconds', 'aggregate_accounted_cpu_seconds')}
                             for phase in _PHASES}),
            limitations=['not a release or performance acceptance matrix',
                         'no cold-cache measurements or speedup comparison',
                         'sampled peaks do not qualify the resource envelope',
                         'detailed coordinator/worker stage, I/O and backpressure observations require telemetry=True',
                         'startup lanes measure interpreter/API import/CLI inspection; full workflows remain in-process'],
            artifacts_retained=work_dir is not None)
        if work_dir is not None:
            report['work_dir'] = str(root.resolve())
    report['temporary_workspace_removed'] = work_dir is None
    return report


def _parse_size(value):
    try:
        return parse_size(value)
    except ConfigurationError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from None


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--work-dir', type=Path, help='new directory retaining fixtures and pools; checked restorations are removed')
    parser.add_argument('--repetitions', type=int, default=5, help='measured runs after one warm-up, minimum 5')
    parser.add_argument('--workers', type=int, default=1)
    parser.add_argument('--storage', choices=tuple(mode.value for mode in StorageMode), default='auto')
    parser.add_argument('--max-ram', type=_parse_size, default=512 << 20)
    parser.add_argument('--max-artifact-bytes', type=_parse_size, default=1 << 20)
    parser.add_argument('--compression-level', type=int, default=6)
    parser.add_argument('--telemetry', action='store_true', help='collect bounded stage, I/O and backpressure observations')
    parser.add_argument('--startup', action='store_true', help='also measure fresh interpreter, API-import and CLI-inspect lanes')
    parser.add_argument('--select', type=parse_counts, help='comma-separated numeric sample indices; default all')
    add_fixture_arguments(parser)
    args = parser.parse_args(argv)
    try:
        report = benchmark(fixture_config=config_from_arguments(args), work_dir=args.work_dir,
            repetitions=args.repetitions, workers=args.workers, storage=StorageMode(args.storage),
            max_ram=args.max_ram, max_artifact_bytes=args.max_artifact_bytes,
            compression_level=args.compression_level, selected=args.select, startup=args.startup, telemetry=args.telemetry)
    except KeyboardInterrupt:
        print(json.dumps(dict(completed=False, error='interrupted'), sort_keys=True))
        return 130
    except Exception as exc:
        print(json.dumps(dict(completed=False, error=type(exc).__name__), sort_keys=True))
        return 1
    print(json.dumps(report, sort_keys=True))
    return 0 if report['completed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
