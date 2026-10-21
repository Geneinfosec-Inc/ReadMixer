"""Private subprocess probes for platform_lane; never imports checkout runtime code."""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import importlib.metadata
import importlib.util
import importlib.machinery
import io
import json
import os
from pathlib import Path
import platform
import runpy
import shutil
import stat
import struct
import subprocess
import sys
import sysconfig
import zlib

NATIVE_ROOTS = ('numpy', 'Crypto', 'psutil', 'pysam')
ALLOCATOR_CONTROLS = ('MallocNanoZone', 'MallocLargeCache', 'MallocAggressiveMadvise', 'MallocMaxMagazines')


def allocator_environment():
    return {name: os.environ.get(name) for name in ALLOCATOR_CONTROLS}


def private_allocator_environment():
    return {name: value for name, value in sorted(os.environ.items())
            if name.startswith('Malloc') or name in ('PYTHONMALLOC', 'NSZombieEnabled', 'DYLD_INSERT_LIBRARIES')}


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


def write_json(path, value):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, 'w') as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write('\n')


def host():
    system = {'Linux': 'linux', 'Darwin': 'macos'}.get(platform.system(), 'unsupported')
    machine = platform.machine().lower()
    return {'cell': f'{system}-{machine}-py{sys.version_info.major}{sys.version_info.minor}',
            'implementation': platform.python_implementation(), 'python_version': platform.python_version(),
            'python_build': list(platform.python_build()), 'machine': machine,
            'pointer_bits': struct.calcsize('P') * 8, 'system': platform.system(),
            'release': platform.release(), 'sysconfig_platform': sysconfig.get_platform(),
            'soabi': sysconfig.get_config_var('SOABI'), 'executable_sha256': sha256(sys.executable),
            'isolated': bool(sys.flags.isolated), 'base_prefix_distinct': sys.prefix != sys.base_prefix,
            'cpu_count': os.cpu_count(), 'affinity': sorted(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else None,
            'allocator_environment': allocator_environment()}


def package_check(mode, root):
    root = Path(root).resolve()
    if mode == 'source':
        sys.path.insert(0, str(root / 'src'))
    import readmixer_py
    path = Path(readmixer_py.__file__).resolve()
    prefix = Path(sys.prefix).resolve()
    expected = root / 'src' / 'readmixer_py' if mode == 'source' else prefix
    if not path.is_relative_to(expected):
        raise RuntimeError('runtime import did not resolve to required source/install')
    if mode == 'installed' and (root / 'src').exists():
        raise RuntimeError('installed test root contains source tree')
    configuration = Path(sys.prefix) / 'pyvenv.cfg'
    if not configuration.is_file() or 'include-system-site-packages = false' not in configuration.read_text().lower():
        raise RuntimeError('environment is not fully isolated')
    return {'mode': mode, 'location_verified': True, 'system_site_packages': False,
            'version': readmixer_py.__version__, 'package_files': [
                {'name': p.relative_to(path.parent).as_posix(), 'bytes': p.stat().st_size, 'sha256': sha256(p)}
                for p in sorted(path.parent.rglob('*.py'))]}


def filesystem(root):
    directory = Path(root) / 'filesystem-probe'
    directory.mkdir(mode=0o700)
    first = directory / 'first'
    descriptor = os.open(first, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, 'wb') as stream:
        stream.write(b'artificial release filesystem probe\n')
        stream.flush()
        os.fsync(stream.fileno())
    second = directory / 'second'
    os.link(first, second)
    same_inode = first.stat().st_ino == second.stat().st_ino
    collision_rejected = False
    try:
        os.link(first, second)
    except FileExistsError:
        collision_rejected = True
    link = directory / 'symlink'
    link.symlink_to(first.name)
    nofollow_rejected = False
    if hasattr(os, 'O_NOFOLLOW'):
        try:
            descriptor = os.open(link, os.O_RDONLY | os.O_NOFOLLOW)
        except OSError:
            nofollow_rejected = True
        else:
            os.close(descriptor)
    directory_synced = False
    descriptor = os.open(directory, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
    try:
        os.fsync(descriptor)
        directory_synced = True
    finally:
        os.close(descriptor)
    fs = os.statvfs(directory)
    result = {'directory_mode': stat.S_IMODE(directory.stat().st_mode),
              'file_mode': stat.S_IMODE(first.stat().st_mode), 'hard_link_same_inode': same_inode,
              'hard_link_collision_rejected': collision_rejected, 'nofollow_rejected': nofollow_rejected,
              'directory_fsync_completed': directory_synced, 'device': directory.stat().st_dev,
              'block_size': fs.f_bsize, 'fragment_size': fs.f_frsize, 'flags': fs.f_flag,
              'scope': 'new private workspace on this device; not a network-filesystem qualification'}
    result['passed'] = all((result['directory_mode'] == 0o700, result['file_mode'] == 0o600,
                            same_inode, collision_rejected, nofollow_rejected, directory_synced))
    link.unlink()
    second.unlink()
    first.unlink()
    directory.rmdir()
    return result


def facts(args):
    allocator_receipt = Path(args.native_config).with_name('startup-allocator-environment.json')
    write_json(allocator_receipt, {'environment': private_allocator_environment(),
        'scope': 'process startup environment readback; does not prove active allocator implementation'})
    check = package_check('installed', args.root)
    import numpy
    import Crypto
    import psutil
    import pysam
    bgzip = shutil.which('bgzip')
    if bgzip is None:
        raise RuntimeError('required independent bgzip executable missing')
    version = subprocess.run([bgzip, '--version'], capture_output=True, text=True, check=True, timeout=15)
    configuration = io.StringIO()
    with contextlib.redirect_stdout(configuration):
        numpy.show_config()
    native_files = []
    for module in (numpy, Crypto, psutil, pysam):
        base = Path(module.__file__).resolve().parent
        for path in sorted(base.rglob('*')):
            if path.is_file() and (path.name.endswith(('.so', '.dylib', '.pyd')) or '.so.' in path.name):
                native_files.append({'name': module.__name__ + '/' + path.relative_to(base).as_posix(),
                                     'bytes': path.stat().st_size, 'sha256': sha256(path)})
    python_extensions = []
    for name, module in sorted(sys.modules.items()):
        location = getattr(module, '__file__', None)
        if location and any(str(location).endswith(suffix) for suffix in importlib.machinery.EXTENSION_SUFFIXES):
            extension = Path(location)
            python_extensions.append({'module': name, 'filename': extension.name,
                                      'bytes': extension.stat().st_size, 'sha256': sha256(extension)})
    result = {'host': host(), 'package': check, 'loaded_python_extensions': python_extensions,
              'startup_allocator_environment_sha256': sha256(allocator_receipt),
              'dependencies': sorted([{'name': d.metadata['Name'], 'version': d.version}
                                       for d in importlib.metadata.distributions()], key=lambda d: d['name'].lower()),
              'zlib': {'build': zlib.ZLIB_VERSION, 'runtime': zlib.ZLIB_RUNTIME_VERSION},
              'native_files': native_files,
              'native_file_scope': 'package subtrees; wheel identities bind sibling vendored libraries, whose installed payload bytes are separately verified by resource/supplement lanes',
              'numpy_config_sha256': hashlib.sha256(configuration.getvalue().encode()).hexdigest(),
              'thread_environment': {name: os.environ.get(name) for name in (
                  'OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS', 'NUMEXPR_NUM_THREADS',
                  'VECLIB_MAXIMUM_THREADS', 'BLIS_NUM_THREADS')},
              'bgzip': {'sha256': sha256(bgzip), 'version': version.stdout.splitlines()[0],
                        'executable_name': Path(bgzip).name},
              'pysam': {'version': pysam.__version__, 'samtools_version': pysam.__samtools_version__},
              'filesystem': filesystem(args.root)}
    # Build configuration can contain private build paths; retain only its digest publicly.
    Path(args.native_config).write_text(configuration.getvalue())
    os.chmod(args.native_config, 0o600)
    write_json(args.result, result)
    return 0 if result['filesystem']['passed'] else 1


def applicability_records(policy, candidate_id, cell, root):
    """Recognize only predeclared Linux launcher tests on the bound macOS cell."""
    if policy.get('schema') != 'readmixer.release.applicability.v1' or policy.get('candidate_id') != candidate_id:
        raise ValueError('applicability policy candidate/schema mismatch')
    if policy.get('declared_before_first_macos_attempt') is not True:
        raise ValueError('applicability must be declared before the first macOS attempt')
    if not cell.startswith('macos-arm64-'):
        return []
    records = policy['macos-arm64']
    names = [record['nodeid'] for record in records]
    if len(records) != 8 or len(set(names)) != len(names):
        raise ValueError('expected exactly eight distinct frozen Linux launcher tests')
    for record in records:
        path = record['nodeid'].split('::')[0]
        if path not in ('tests/test_qualification_host.py', 'tests/test_run_qualification.py'):
            raise ValueError('applicability cannot waive production or interoperability tests')
        if sha256(Path(root) / path) != record['source_sha256']:
            raise ValueError('applicability source fingerprint mismatch')
        if not isinstance(record['reason'], str) or not record['reason']:
            raise ValueError('applicability requires an explicit reason')
    return records


def suite(args):
    startup_allocator_environment = allocator_environment()
    check = package_check(args.mode, args.root)
    import pytest
    collected = []
    outcomes = {}
    not_applicable = []
    policy_hash = None
    if args.applicability:
        policy = json.loads(Path(args.applicability).read_bytes())
        not_applicable = applicability_records(policy, args.candidate_id, host()['cell'], args.root)
        policy_hash = sha256(args.applicability)
    not_applicable_names = {record['nodeid'] for record in not_applicable}
    skip_reason = 'readmixer-plan17:not_applicable:' + str(policy_hash)


    class Recorder:
        def pytest_collection_modifyitems(self, session, config, items):
            available = {item.nodeid for item in items}
            if not not_applicable_names <= available:
                raise pytest.UsageError('predeclared applicability node was not collected')
            for item in items:
                if item.nodeid in not_applicable_names:
                    item.add_marker(pytest.mark.skip(reason=skip_reason))

        def pytest_collection_finish(self, session):
            collected.extend(item.nodeid for item in session.items)

        def pytest_runtest_logreport(self, report):
            item = outcomes.setdefault(report.nodeid, {'nodeid': report.nodeid, 'phases': []})
            item['phases'].append({'phase': report.when, 'outcome': report.outcome,
                                   'xfail': bool(getattr(report, 'wasxfail', False)), 'seconds': report.duration,
                                   'not_applicable': report.nodeid in not_applicable_names and report.when == 'setup'
                                      and report.skipped and skip_reason in str(report.longrepr)})

    command = ['tests', '-ra', '-o', 'addopts=', '-o', 'pythonpath=' + ('src' if args.mode == 'source' else ''),
               '--junitxml=' + args.junit, '--basetemp=' + args.basetemp]
    if args.collect_only:
        command.append('--collect-only')
    code = int(pytest.main(command, plugins=[Recorder()]))
    result = {'package': check, 'exit_code': code, 'collected': collected, 'outcomes': list(outcomes.values()),
              'startup_allocator_environment': startup_allocator_environment,
              'collect_only': args.collect_only, 'not_applicable': not_applicable, 'applicability_policy_sha256': policy_hash, 'scope': 'entire archived tests directory; no selection/deselection'}
    write_json(args.result, result)
    return code


def lazy(args):
    check = package_check('installed', args.root)
    forbidden = []
    attempts = []

    class DenyNative:
        def find_spec(self, fullname, path=None, target=None):
            if fullname.split('.')[0] in NATIVE_ROOTS:
                forbidden.append(fullname)
                raise RuntimeError('native dependency imported by help/version')
            return None

    def audit(event, values):
        if event in ('subprocess.Popen', 'os.fork', 'os.forkpty', 'os.posix_spawn', 'os.spawn'):
            attempts.append(event)
            raise RuntimeError('help/version attempted child process creation')

    absent = all(importlib.util.find_spec(name) is None for name in NATIVE_ROOTS)
    if not absent:
        raise RuntimeError('dependency-free probe environment contains a native dependency')
    sys.meta_path.insert(0, DenyNative())
    sys.addaudithook(audit)
    output, error = io.StringIO(), io.StringIO()
    sys.argv = ['readmixer-py', args.flag]
    code = 0
    with contextlib.redirect_stdout(output), contextlib.redirect_stderr(error):
        try:
            if args.entry == 'module':
                runpy.run_module('readmixer_py', run_name='__main__')
            else:
                runpy.run_path(args.entry_path, run_name='__main__')
        except SystemExit as exc:
            code = exc.code or 0
    loaded = sorted(name for name in sys.modules if name.split('.')[0] in NATIVE_ROOTS)
    result = {'package': check, 'entry': args.entry, 'flag': args.flag, 'exit_code': code,
              'native_dependencies_absent': absent, 'native_modules_loaded': loaded,
              'native_import_attempts': forbidden, 'child_creation_attempts': attempts,
              'stdout_contains_program_name': 'readmixer-py' in output.getvalue(),
              'stderr_empty': not error.getvalue(), 'stdout_sha256': hashlib.sha256(output.getvalue().encode()).hexdigest()}
    result['passed'] = code == 0 and absent and not (loaded or forbidden or attempts or error.getvalue()) and result['stdout_contains_program_name']
    write_json(args.result, result)
    return 0 if result['passed'] else 1


def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest='command', required=True)
    facts_parser = commands.add_parser('facts')
    facts_parser.add_argument('--native-config', required=True)
    for name in ('suite', 'lazy'):
        child = commands.add_parser(name)
        if name == 'suite':
            child.add_argument('--mode', choices=('source', 'installed'), required=True)
            child.add_argument('--junit', required=True)
            child.add_argument('--basetemp', required=True)
            child.add_argument('--collect-only', action='store_true')
            child.add_argument('--applicability')
            child.add_argument('--candidate-id')
        else:
            child.add_argument('--entry', choices=('module', 'console', 'forwarder'), required=True)
            child.add_argument('--entry-path')
            child.add_argument('--flag', choices=('--help', '--version'), required=True)
    for child in commands.choices.values():
        child.add_argument('--root', required=True)
        child.add_argument('--result', required=True)
    args = parser.parse_args()
    return {'facts': facts, 'suite': suite, 'lazy': lazy}[args.command](args)


if __name__ == '__main__':
    raise SystemExit(main())
