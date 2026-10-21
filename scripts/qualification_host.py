#!/usr/bin/env python3
"""Run a qualification command inside invocation-owned Linux resource controls.

No global cgroups, mounts, cache settings or parent rlimits are changed. A new
systemd user unit and (optionally) a private user/mount namespace die with the
command. Reports distinguish requested settings from kernel-observed limits.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import resource
import stat
import signal
import subprocess
import sys
import tempfile
import time
import uuid

_NATIVE = ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS',
           'VECLIB_MAXIMUM_THREADS', 'NUMEXPR_NUM_THREADS', 'BLIS_NUM_THREADS')


def host_snapshot():
    result = dict(platform=sys.platform, affinity=sorted(os.sched_getaffinity(0))
                  if hasattr(os, 'sched_getaffinity') else None,
                  nofile=list(resource.getrlimit(resource.RLIMIT_NOFILE)),
                  native_thread_environment={key: os.environ.get(key) for key in _NATIVE})
    try:
        relative = next(line[3:] for line in Path('/proc/self/cgroup').read_text().splitlines()
                        if line.startswith('0::'))
        directory = Path('/sys/fs/cgroup') / relative.lstrip('/')
        values = {}
        for name in ('memory.max', 'memory.current', 'memory.peak', 'memory.events',
                     'memory.swap.max', 'cpu.max', 'cpu.stat', 'pids.current', 'pids.peak'):
            try:
                values[name] = (directory / name).read_text().strip()
            except OSError:
                values[name] = None
        result['cgroup'] = values
    except (OSError, StopIteration):
        result['cgroup'] = None
    return result


def _validate(controls):
    if set(controls) - {'memory_bytes', 'cpu_quota', 'affinity', 'nofile', 'scratch_mount'}:
        raise ValueError('unknown host control')
    for name in ('memory_bytes', 'cpu_quota', 'nofile'):
        value = controls.get(name)
        if name in controls and (type(value) is not int or value < 1):
            raise ValueError('host limits must be positive integers')
    affinity = controls.get('affinity')
    if affinity is not None and (not isinstance(affinity, list) or not affinity
            or any(type(cpu) is not int or cpu < 0 for cpu in affinity)):
        raise ValueError('invalid CPU affinity')
    mount = controls.get('scratch_mount')
    if mount is not None:
        if (not isinstance(mount, dict) or set(mount) != {'path', 'bytes'} or
                type(mount['bytes']) is not int or mount['bytes'] < 1 or not isinstance(mount['path'], str)):
            raise ValueError('invalid private scratch mount')
        if not Path(mount['path']).is_absolute():
            raise ValueError('scratch mount must be absolute')


def scratch_snapshot(path):
    """Require the exact dedicated tmpfs mount, including kernel-rounded size."""
    path = Path(path).resolve(strict=True)
    result = dict(private_scratch_mounted=False, scratch_capacity_bytes=None,
                  scratch_block_bytes=None, scratch_distinct_parent_device=False,
                  scratch_filesystem=None)
    information = path.stat()
    result['scratch_distinct_parent_device'] = information.st_dev != path.parent.stat().st_dev
    facts = os.statvfs(path)
    result.update(scratch_capacity_bytes=facts.f_blocks * facts.f_frsize,
                  scratch_block_bytes=facts.f_frsize, scratch_available_bytes=facts.f_bavail * facts.f_frsize)
    def decode(value):
        for encoded, byte in ((r'\040', ' '), (r'\011', '\t'), (r'\012', '\n'), (r'\134', '\\')):
            value = value.replace(encoded, byte)
        return value
    for line in Path('/proc/self/mountinfo').read_text().splitlines():
        prefix, separator, suffix = line.partition(' - ')
        parts = prefix.split()
        following = suffix.split()
        if separator and len(parts) >= 6 and following and decode(parts[4]) == str(path):
            result['scratch_filesystem'] = following[0]
            result['private_scratch_mounted'] = (following[0] == 'tmpfs' and
                result['scratch_distinct_parent_device'])
            break
    return result


def verified_controls(controls, observed):
    """Compare explicit requests to actual kernel state, never assume success."""
    reasons = []
    cgroup = observed.get('cgroup') or {}
    if 'memory_bytes' in controls:
        if cgroup.get('memory.max') != str(controls['memory_bytes']):
            reasons.append('memory.max mismatch')
        if cgroup.get('memory.swap.max') != '0':
            reasons.append('swap ceiling mismatch')
    if 'cpu_quota' in controls:
        try:
            quota, period = map(int, cgroup.get('cpu.max', '').split())
            if quota != controls['cpu_quota'] * period:
                reasons.append('CPU quota mismatch')
        except (ValueError, TypeError):
            reasons.append('CPU quota unavailable')
    if 'nofile' in controls and observed.get('nofile', [None])[0] != controls['nofile']:
        reasons.append('descriptor ceiling mismatch')
    if 'affinity' in controls and observed.get('affinity') != sorted(set(controls['affinity'])):
        reasons.append('affinity mismatch')
    if 'scratch_mount' in controls:
        if observed.get('private_scratch_mounted') is not True:
            reasons.append('private scratch mount absent')
        size, block = observed.get('scratch_capacity_bytes'), observed.get('scratch_block_bytes')
        requested = controls['scratch_mount']['bytes']
        if (type(size) is not int or type(block) is not int or block <= 0 or
                size != ((requested + block - 1) // block) * block):
            reasons.append('private scratch capacity mismatch')
    return dict(verified=not reasons, reasons=reasons)


def _child(config_path, command):
    config = json.loads(Path(config_path).read_text())
    controls = config['controls']
    _validate(controls)
    if controls.get('affinity') is not None:
        os.sched_setaffinity(0, controls['affinity'])
    if controls.get('nofile') is not None:
        _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(resource.RLIMIT_NOFILE, (controls['nofile'], hard))
    environment = dict(os.environ)
    for key in _NATIVE:
        environment[key] = '1'
        os.environ[key] = '1'
    before = host_snapshot()
    mount = controls.get('scratch_mount')
    if mount is not None:
        # Caller created this empty path exclusively. This branch only executes
        # under unshare's private mount namespace; refuse a shared namespace.
        if os.readlink('/proc/self/ns/mnt') == config['parent_mount_namespace']:
            raise RuntimeError('refusing to mount in the parent namespace')
        path = Path(mount['path'])
        if not stat.S_ISDIR(path.lstat().st_mode) or any(path.iterdir()):
            raise RuntimeError('scratch mount is not a new empty directory')
        subprocess.run(['mount', '--make-rprivate', '/'], check=True)
        subprocess.run(['mount', '-t', 'tmpfs', '-o', f"size={mount['bytes']},mode=0700,nosuid,nodev,noexec", 'tmpfs', str(path)], check=True)
        before.update(scratch_snapshot(path))
    check = verified_controls(controls, before)
    start = time.perf_counter()
    process = subprocess.run(command, env=environment, check=False) if check['verified'] else None
    result = dict(host_report_version=1, requested=controls, before=before, after=host_snapshot(),
                  controls=check, returncode=None if process is None else process.returncode,
                  wall_seconds=time.perf_counter() - start,
                  scope='invocation-owned cgroup; no global cache/limit changes')
    # Scratch path is an artificial workspace path; never source data.
    if 'scratch_mount' in result['requested']:
        result['requested']['scratch_mount'] = {'bytes': mount['bytes'], 'path': '<private-workspace>'}
    descriptor = os.open(config['report'], os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, 'w') as sink:
        sink.write(json.dumps(result, sort_keys=True) + '\n')
    return 1 if process is None else process.returncode


def run_host(command, *, controls=None, report, stdout=None, timeout=None):
    controls = dict(controls or {})
    _validate(controls)
    if not command or not all(isinstance(value, str) for value in command):
        raise ValueError('command must be a nonempty string argument vector')
    report = Path(report).resolve()
    if report.exists():
        raise FileExistsError('refusing to replace a host report')
    unit = 'readmixer-qualification-' + uuid.uuid4().hex + '.scope'
    with tempfile.TemporaryDirectory(prefix='readmixer-host-') as temporary:
        config = Path(temporary) / 'controls.json'
        config.write_text(json.dumps(dict(controls=controls, report=str(report),
            parent_mount_namespace=os.readlink('/proc/self/ns/mnt'))))
        config.chmod(0o600)
        child = [sys.executable, str(Path(__file__).resolve()), '--child', str(config), '--', *command]
        if controls.get('scratch_mount'):
            child = ['unshare', '--user', '--map-root-user', '--mount', *child]
        scoped = 'memory_bytes' in controls or 'cpu_quota' in controls
        if scoped:
            properties = []
            if 'memory_bytes' in controls:
                properties += ['-p', f"MemoryMax={controls['memory_bytes']}", '-p', 'MemorySwapMax=0']
            if 'cpu_quota' in controls:
                properties += ['-p', f"CPUQuota={100 * controls['cpu_quota']}%"]
            child = ['systemd-run', '--user', '--scope', '--quiet', '--collect',
                     '--unit=' + unit, *properties, *child]
        process = None
        with tempfile.TemporaryFile() as diagnostic:
            try:
                process = subprocess.Popen(child, stdout=stdout, stderr=diagnostic, start_new_session=True)
                process.communicate(timeout=timeout)
            finally:
                # SIGINT, monitor failure and timeout all reap the owned launch;
                # failed systemctl cleanup must never skip process-group cleanup.
                pending_error = sys.exc_info()[1]
                if process is not None and process.poll() is None:
                    if scoped:
                        try:
                            subprocess.run(['systemctl', '--user', 'stop', unit], stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL, check=False, timeout=30)
                        except (OSError, subprocess.TimeoutExpired):
                            pass
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    try:
                        process.communicate(timeout=10)
                    except BaseException:
                        if pending_error is None:
                            raise
                        pending_error.add_note('owned qualification process reaping also failed')
            return dict(returncode=process.returncode, report_written=report.is_file(),
                        diagnostic_bytes=os.fstat(diagnostic.fileno()).st_size,
                        diagnostic_text_published=False)



def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--child', type=Path, required=True)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    return _child(args.child, command)


if __name__ == '__main__':
    raise SystemExit(main())
