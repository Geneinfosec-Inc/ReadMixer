#!/usr/bin/env python3
"""Time complete correct workflows in fresh installed-candidate interpreters.

Fixture generation precedes every measured launch. Each of one warm-up and at
least five measured children imports the candidate, mixes, fully verifies, runs
both recoveries, independently checks bytes/counts, and removes owned outputs.
Public reports retain every observation and omit child diagnostic contents.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import time

_HERE = Path(__file__).resolve().parent
_LIMIT = 8 << 20
from generate_fixtures import FixtureConfig, generate_fixtures

_CHILD = r'''
import json, os, pathlib, sys, time
import psutil
configuration = json.loads(pathlib.Path(sys.argv[1]).read_bytes())
def snapshot():
    process = psutil.Process()
    cpu = process.cpu_times()
    try:
        import resource
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        peak = peak if sys.platform == 'darwin' else peak * 1024
    except (ImportError, AttributeError, OSError):
        peak = None
    return dict(rss_bytes=process.memory_info().rss, lifetime_peak_rss_bytes=peak,
        os_threads=process.num_threads(), descriptors=process.num_fds() if hasattr(process, 'num_fds') else None,
        process_cpu_seconds=cpu.user + cpu.system)
response = dict(completed=False)
try:
    before = snapshot()
    started = time.perf_counter()
    import readmixer_py
    api_import = time.perf_counter() - started
    matches = pathlib.Path(readmixer_py.__file__).resolve() == pathlib.Path(configuration['package_file']).resolve()
    if not matches:
        raise RuntimeError('candidate mismatch')
    started = time.perf_counter()
    sys.path.insert(0, configuration['scripts'])
    import benchmark_pipeline as benchmark
    from readmixer_py.config import StorageMode
    harness_import = time.perf_counter() - started
    fixture = json.loads(pathlib.Path(configuration['fixture']).read_bytes())
    options = configuration['options']
    options['storage'] = StorageMode(options['storage'])
    options['selected'] = tuple(options['selected'])
    workflow = benchmark._observed_iteration(pathlib.Path(configuration['root']), fixture, **options)
    response = dict(completed=workflow['completed'], candidate_matches=matches,
        before_readmixer_import=before, after_workflow=snapshot(),
        api_import_seconds=api_import, harness_import_seconds=harness_import, workflow=workflow)
except BaseException as failure:
    response.update(completed=False, error=type(failure).__name__)
encoded = json.dumps(response, sort_keys=True).encode()
if len(encoded) > 8 << 20:
    encoded = b'{"completed":false,"error":"child_report_bound"}'
descriptor = os.open(configuration['response'], os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(descriptor, 'wb') as destination:
    destination.write(encoded)
'''


def _candidate(wheel, wheel_sha256, sdist, sdist_sha256):
    from qualify_supplied_data import candidate_identity
    identity = candidate_identity(wheel, wheel_sha256, sdist, sdist_sha256)
    names = ('benchmark_fresh.py', 'benchmark_pipeline.py', 'generate_fixtures.py', 'resource_telemetry.py')
    with tarfile.open(sdist, 'r:*') as archive:
        entries = archive.getmembers()
        fingerprints = {}
        for name in names:
            matches = [item for item in entries if item.name.endswith('/scripts/' + name)]
            current = (_HERE / name).read_bytes()
            if len(matches) != 1 or not matches[0].isfile() or matches[0].size != len(current):
                raise ValueError('frozen source archive lacks an exact benchmark tool')
            stream = archive.extractfile(matches[0])
            if stream is None or stream.read(len(current) + 1) != current:
                raise ValueError('benchmark tooling differs from frozen source archive')
            fingerprints[name] = hashlib.sha256(current).hexdigest()
    identity['fresh_benchmark_tools_sha256'] = fingerprints
    return identity


def _kill_group(process):
    """Bounded cancellation covers the harness-owned fresh process and workers."""
    if os.name == 'posix':
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    elif process.poll() is None:
        process.terminate()
    try:
        process.wait(timeout=1)
    except subprocess.TimeoutExpired:
        pass
    if os.name == 'posix':
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    elif process.poll() is None:
        process.kill()
    process.wait(timeout=5)


def _observation(root, fixture_path, options, *, require_installed, timeout=3600):
    import psutil
    import readmixer_py
    import benchmark_pipeline as benchmark
    configuration = root.parent / (root.name + '-control.json')
    response = root.parent / (root.name + '-response.json')
    package = Path(readmixer_py.__file__).resolve()
    if require_installed and ('site-packages' not in package.parts or
            package.parent == (_HERE.parent / 'src' / 'readmixer_py').resolve()):
        raise ValueError('fresh qualification requires an outside-checkout installed candidate')
    document = dict(root=str(root), fixture=str(fixture_path), options=options,
                    package_file=str(package), scripts=str(_HERE), response=str(response))
    encoded = json.dumps(document).encode()
    if len(encoded) > 64 << 10:
        raise ValueError('fresh workflow control message exceeds its bound')
    descriptor = os.open(configuration, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, 'wb') as stream:
        stream.write(encoded)
    environment = os.environ.copy()
    environment.pop('PYTHONPATH', None)
    environment.pop('PYTHONHOME', None)
    if not require_installed:
        # Explicit development-only test mode; CLI never enables this path.
        environment['PYTHONPATH'] = str(package.parent.parent)
    started, cpu_started = time.perf_counter(), time.process_time()
    child_cpu_before = benchmark._reaped_child_cpu()
    error = None
    detail = None
    sampled = None
    process = None
    sizes = [0, 0]
    with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
        try:
            process = subprocess.Popen([sys.executable, '-c', _CHILD, str(configuration)],
                stdout=stdout, stderr=stderr, cwd=root.parent, env=environment,
                start_new_session=os.name == 'posix')
            try:
                observed = psutil.Process(process.pid)
            except psutil.NoSuchProcess:
                observed = None
            monitor = benchmark.ProcessSampler(root / 'scratch', interval=0.05,
                                               process=observed) if observed is not None else None
            if monitor is not None:
                monitor.__enter__()
            try:
                while process.poll() is None:
                    sizes = [os.fstat(stream.fileno()).st_size for stream in (stdout, stderr)]
                    if time.perf_counter() - started > timeout:
                        error = 'timeout'
                        _kill_group(process)
                        break
                    if max(sizes) > _LIMIT:
                        error = 'child_diagnostic_bound'
                        _kill_group(process)
                        break
                    time.sleep(0.02)
                process.wait(timeout=5)
            finally:
                if monitor is not None:
                    monitor.__exit__(None, None, None)
                    sampled = monitor.result
            sizes = [os.fstat(stream.fileno()).st_size for stream in (stdout, stderr)]
            if error is None:
                if process.returncode != 0 or any(sizes):
                    error = 'child_failed_or_emitted_diagnostics'
                elif not response.exists() or response.stat().st_size > _LIMIT:
                    error = 'missing_or_oversize_child_report'
                else:
                    try:
                        detail = json.loads(response.read_bytes())
                        if not isinstance(detail, dict) or detail.get('candidate_matches') is not True or detail.get('completed') is not True:
                            error = 'child_failed_or_correctness_mismatch'
                    except (ValueError, TypeError):
                        error = 'invalid_child_report'
        finally:
            if process is not None and process.poll() is None:
                _kill_group(process)
    children_after = benchmark._reaped_child_cpu()
    measured = dict(completed=error is None, error=error, child=detail,
        wall_seconds=time.perf_counter() - started,
        parent_monitor_cpu_seconds=time.process_time() - cpu_started,
        child_cpu_seconds=None if child_cpu_before is None or children_after is None else max(0.0, children_after-child_cpu_before),
        exit_code=None if process is None else process.returncode,
        sampled=sampled, stdout_bytes=sizes[0], stderr_bytes=sizes[1],
        timed_out=error == 'timeout')
    # Files belong to this exclusively created benchmark workspace. Diagnostics
    # and control paths are never included in the public observation.
    for path in (configuration, response):
        path.unlink(missing_ok=True)
    if root.exists():
        shutil.rmtree(root)
    measured['owned_child_output_cleanup_complete'] = not root.exists()
    return measured


def fresh_benchmark(*, fixture_config=None, work_dir=None, repetitions=5, workers=1,
                    storage='external', max_ram=512 << 20, max_artifact_bytes=1 << 20,
                    compression_level=6, selected=None, telemetry=False, timeout=3600,
                    require_installed=True, candidate=None):
    import benchmark_pipeline as benchmark
    if fixture_config is None:
        fixture_config = FixtureConfig(counts=(2, 1, 0))
    if type(repetitions) is not int or not 5 <= repetitions <= 1000:
        raise ValueError('fresh workflow benchmark requires 5..1000 measured repetitions')
    if not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or timeout <= 0:
        raise ValueError('fresh workflow timeout must be positive')
    if require_installed and not candidate:
        raise ValueError('fresh qualification requires frozen candidate identity')
    selected = tuple(range(len(fixture_config.counts))) if selected is None else tuple(selected)
    if not selected or len(set(selected)) != len(selected) or any(type(index) is not int or not 0 <= index < len(fixture_config.counts) for index in selected):
        raise ValueError('fresh workflow selection is invalid')
    options = dict(workers=workers, storage=storage, max_ram=max_ram,
        max_artifact_bytes=max_artifact_bytes, compression_level=compression_level,
        selected=selected, telemetry=telemetry)
    with benchmark._workspace(work_dir) as root:
        fixture = generate_fixtures(root / 'fixtures', fixture_config)
        fixture_path = root / 'fixtures' / 'fixtures.json'
        def observe(directory):
            started = time.perf_counter()
            try:
                return _observation(directory, fixture_path, options,
                                    require_installed=require_installed, timeout=timeout)
            except Exception as failure:
                return dict(completed=False, error=type(failure).__name__,
                    wall_seconds=time.perf_counter() - started, child_cpu_seconds=None,
                    child=None, owned_child_output_cleanup_complete=False)
        warmup = observe(root / 'warmup')
        measured = [observe(root / f'measured-{number:04d}') for number in range(repetitions)]
        complete = all(item['completed'] for item in (warmup, *measured))
        summary = None if not complete else dict(
            launch_to_exit_seconds=benchmark.summarize(item['wall_seconds'] for item in measured),
            waited_child_cpu_seconds=benchmark.summarize_optional(item['child_cpu_seconds'] for item in measured),
            api_import_seconds=benchmark.summarize(item['child']['api_import_seconds'] for item in measured),
            complete_workflow_seconds=benchmark.summarize(item['child']['workflow']['wall_seconds'] for item in measured))
        report = dict(report_version=1, scope='fresh_process_complete_correct_workflow',
            completed=complete, candidate=candidate, installed_qualification=require_installed,
            versions=benchmark._versions(), warmup=warmup, measured_runs=measured, summary=summary,
            configuration=options, fixture=dict(samples=len(fixture['samples']), pairs=fixture['pairs'],
                reads=fixture['reads'], normalized_bytes=fixture['normalized_bytes']),
            cache_treatment='one warm-up then five or more launches reusing fixtures and OS caches; no clearing',
            launch_scope='process creation, observer/stdlib imports, API/tool imports, complete checked workflow, owned output cleanup and exit',
            fixture_scope='fixture generation excluded from every launch interval',
            import_scope='ReadMixer API import excludes earlier stdlib and psutil; benchmark helper import measured separately',
            cpu_scope='waited fresh-child and reaped descendants; parent observer CPU reported separately',
            limitations=['sampling can miss short peaks and sum shared RSS more than once',
                         'observer imports and sampling are included in launch overhead',
                         'no native library thread attribution or cold-cache claim'])
    report['temporary_workspace_removed'] = work_dir is None
    report['fixtures_retained'] = work_dir is not None
    return report


class _Parser(argparse.ArgumentParser):
    def error(self, _message):
        self.exit(2, 'invalid fresh benchmark arguments; use --help\n')


def main(argv=None):
    from generate_fixtures import add_fixture_arguments, config_from_arguments, parse_counts
    from readmixer_py.config import parse_size
    parser = _Parser(description=__doc__)
    add_fixture_arguments(parser)
    parser.add_argument('--work-dir', type=Path)
    parser.add_argument('--repetitions', type=int, default=5)
    parser.add_argument('--workers', type=int, default=1)
    parser.add_argument('--storage', choices=('auto', 'external', 'memory'), default='external')
    parser.add_argument('--max-ram', type=parse_size, default=512 << 20)
    parser.add_argument('--max-artifact-bytes', type=parse_size, default=1 << 20)
    parser.add_argument('--compression-level', type=int, default=6)
    parser.add_argument('--select', type=parse_counts)
    parser.add_argument('--timeout', type=float, default=3600)
    parser.add_argument('--telemetry', action='store_true')
    parser.add_argument('--wheel', required=True, type=Path)
    parser.add_argument('--wheel-sha256', required=True)
    parser.add_argument('--sdist', required=True, type=Path)
    parser.add_argument('--sdist-sha256', required=True)
    args = parser.parse_args(argv)
    try:
        identity = _candidate(args.wheel, args.wheel_sha256, args.sdist, args.sdist_sha256)
        report = fresh_benchmark(fixture_config=config_from_arguments(args), work_dir=args.work_dir,
            repetitions=args.repetitions, workers=args.workers, storage=args.storage,
            max_ram=args.max_ram, max_artifact_bytes=args.max_artifact_bytes,
            compression_level=args.compression_level, selected=args.select,
            telemetry=args.telemetry, timeout=args.timeout, candidate=identity)
    except BaseException as failure:
        report = dict(completed=False, error='interrupted' if isinstance(failure, KeyboardInterrupt) else type(failure).__name__)
    print(json.dumps(report, sort_keys=True))
    return 0 if report['completed'] else (130 if report.get('error') == 'interrupted' else 1)


if __name__ == '__main__':
    raise SystemExit(main())
