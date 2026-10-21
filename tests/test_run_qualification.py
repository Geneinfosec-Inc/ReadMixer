"""Executed observations, frozen tooling, and dedicated scratch controls fail closed."""
import copy
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
from types import SimpleNamespace

import pytest

_SCRIPTS = Path(__file__).parents[1] / 'scripts'
sys.path.insert(0, str(_SCRIPTS))
import benchmark_pipeline as benchmark
import qualification_host as host
import qualification_matrix as matrix
import run_qualification as runner
from generate_fixtures import FixtureConfig, generate_fixtures


def _cell(name='balanced-cpu1-external'):
    return next(cell for cell in matrix.qualification_matrix()['cells'] if cell['id'] == name)


def _row():
    return dict(completed=True, wall_seconds=1.0, correctness=dict.fromkeys(matrix._CORRECTNESS, True),
        originals_unchanged=True, foreign_files_unchanged=True, algorithmic_scratch_empty=True,
        children_reaped=True, private_control_observations={'witnesses': {'refinements': 0}},
        phases={name: dict(wall_seconds=0.1, resources={'effective_max_ram': 512 << 20},
            cpu_budget=1, workers_effective=1,
            detailed=dict(complete=True, errors=0, io={'encrypted_scratch': {'write_bytes': 1}}))
            for name in matrix._PHASES})


def _report(cell):
    return dict(completed=True, configuration={**cell['configuration'], 'startup_requested': False},
        cache_policy='unchanged', fixture={'normalized_bytes': cell['expected_normalized_bytes']},
        warmup=_row(), measured_runs=[_row() for _ in range(5)], summary={'wall_seconds': 1.0})


def _host_ok(*_):
    return dict(verified=True, snapshot={'nofile': [96, 1024]}, distinct_scratch_output_devices=None)


def test_P16_T05_runner_preserves_actual_configuration_instead_of_overwriting_with_requested_cell(monkeypatch):
    cell = _cell()
    report = _report(cell)
    report['configuration']['workers_requested'] = 99
    monkeypatch.setattr(runner, 'benchmark', lambda **_: report)
    monkeypatch.setattr(runner, '_observed_host', _host_ok)
    result = runner.run_cell(cell, dict(wheel_sha256='a'*64, dependency_lock_sha256='b'*64))
    assert result['configuration']['workers_requested'] == 99
    assert not result['completed'] and result['summary'] is None
    assert 'configuration differs from frozen cell' in result['acceptance']['reasons']


def test_P16_T03_actual_enospc_requires_verified_mount_and_injects_only_dedicated_probe_path(tmp_path, monkeypatch):
    cell = _cell('scratch-device-exhaustion')
    observed = {}
    def benchmark_call(**arguments):
        observed.update(arguments['private_options'])
        return _report(cell)
    monkeypatch.setattr(runner, 'benchmark', benchmark_call)
    monkeypatch.setattr(runner, '_observed_host', _host_ok)
    with pytest.raises(ValueError, match='verified dedicated scratch'):
        runner.run_cell(cell, {})
    controls = {'scratch_mount': {'path': str(tmp_path), 'bytes': 1 << 20}}
    runner.run_cell(cell, {}, host_controls=controls, scratch_parent=tmp_path)
    assert observed['scratch_parent'] == observed['scratch_space_probe_path'] == str(tmp_path.resolve())
    assert observed['defer_scratch_space_refusal']
    assert 'scratch_device_quota_bytes' not in observed


def test_P16_T03_unverified_host_is_not_an_executed_or_successful_cell(monkeypatch):
    monkeypatch.setattr(runner, '_observed_host', lambda *_: {'verified': False})
    monkeypatch.setattr(runner, 'benchmark', lambda **_: pytest.fail('unverified host must not execute fixture workload'))
    result = runner.run_cell(_cell(), {})
    assert not result['completed'] and not result['executed']
    assert result['measured_runs'] == [] and result['warmup'] is None
    assert result['error'] == 'host_controls_or_capacity_unverified'


def _frozen_environment(tmp_path, monkeypatch, *, change=None):
    scripts = tmp_path / 'scripts'
    scripts.mkdir()
    (scripts / 'tool.py').write_bytes(b'print(42)\n')
    lock = tmp_path / 'lock.txt'
    names = ('numpy', 'pycryptodome', 'psutil')
    lock.write_text('\n'.join(f'{name}==1.0 --hash=sha256:' + 'a'*64 for name in names))
    monkeypatch.setattr(runner, '_HERE', scripts)
    monkeypatch.setattr(runner.metadata, 'version', lambda name: '2.0' if change == 'installed' and name == 'numpy' else '1.0')
    archive_path = tmp_path / 'source.tar.gz'
    with tarfile.open(archive_path, 'w:gz') as archive:
        for relative, path in (('scripts/tool.py', scripts/'tool.py'), ('lock.txt', lock)):
            data = path.read_bytes()
            info = tarfile.TarInfo('candidate/' + relative)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    if change == 'tool':
        (scripts/'tool.py').write_bytes(b'changed\n')
    elif change == 'lock':
        lock.write_text(lock.read_text() + '\n# changed scope\n')
    elif change == 'unpinned':
        lock.write_text('numpy>=1.0\n')
    elif change == 'missing_runtime':
        lock.write_text('numpy==1.0 --hash=sha256:' + 'a'*64)
    return archive_path, lock


@pytest.mark.parametrize('change', ['tool', 'lock', 'installed', 'unpinned', 'missing_runtime'])
def test_P16_T05_environment_binding_rejects_changed_tools_lock_or_installed_dependencies(tmp_path, monkeypatch, change):
    archive, lock = _frozen_environment(tmp_path, monkeypatch, change=change)
    with pytest.raises(ValueError):
        runner.bind_environment(archive, lock)


def test_P16_T05_environment_binding_records_actual_pins_and_frozen_hashes(tmp_path, monkeypatch):
    archive, lock = _frozen_environment(tmp_path, monkeypatch)
    result = runner.bind_environment(archive, lock)
    assert result['qualification_tooling_matches_sdist'] and result['installed_dependency_versions_match_lock']
    assert result['dependency_lock_sha256'] == hashlib.sha256(lock.read_bytes()).hexdigest()
    assert result['installed_dependencies'] == {'numpy': '1.0', 'pycryptodome': '1.0', 'psutil': '1.0'}
    assert str(tmp_path) not in json.dumps(result)


def test_P16_T03_host_report_and_result_creation_are_private_and_exclusive(tmp_path):
    path = tmp_path / 'result.json'
    runner._write_new(path, {'completed': True})
    assert path.stat().st_mode & 0o777 == 0o600
    with pytest.raises(FileExistsError):
        runner._write_new(path, {'completed': False})
    assert json.loads(path.read_text()) == {'completed': True}


@pytest.mark.parametrize('capacity,block,mounted,expected', [
    (4096, 4096, True, True), (8192, 4096, True, False),
    (4096, 4096, False, False), (None, 4096, True, False), (4096, 0, True, False)])
def test_P16_T03_scratch_mount_requests_need_actual_kernel_rounded_capacity(capacity, block, mounted, expected):
    result = host.verified_controls({'scratch_mount': {'path': '/private', 'bytes': 1024}},
        {'private_scratch_mounted': mounted, 'scratch_capacity_bytes': capacity, 'scratch_block_bytes': block})
    assert result['verified'] is expected


@pytest.mark.skipif(sys.platform != 'linux', reason='Linux /proc mountinfo observation')
def test_P16_T03_unmounted_directory_cannot_qualify_as_dedicated_scratch(tmp_path):
    observed = host.scratch_snapshot(tmp_path)
    assert not observed['private_scratch_mounted']
    assert observed['scratch_capacity_bytes'] > 0
    assert observed['scratch_filesystem'] is None


def test_P16_T04_runtime_tracker_is_explicitly_reported_but_unknown_children_are_not_hidden(monkeypatch):
    from multiprocessing import resource_tracker
    monkeypatch.setattr(resource_tracker._resource_tracker, '_pid', 123)
    def child(pid, code):
        return SimpleNamespace(pid=pid, ppid=lambda: os.getpid(), cmdline=lambda: ['python', '-c', code])
    actual = child(123, 'from multiprocessing.resource_tracker import main;main(8)')
    impostor = child(124, 'from multiprocessing.resource_tracker import main;main(8)')
    wrong_code = child(123, 'unrelated child')
    assert benchmark._runtime_helpers([actual, impostor]) == {123}
    assert benchmark._runtime_helpers([wrong_code, impostor]) == set()


def test_P16_T03_interrupted_iteration_cleans_owned_scratch_and_restores_contextvars(tmp_path, monkeypatch):
    fixture = generate_fixtures(tmp_path/'fixtures', FixtureConfig(counts=(1, 1)))
    outside = tmp_path/'external-scratch'
    outside.mkdir()
    root = tmp_path/'run'
    def interrupt(root, fixture, **kwargs):
        root.mkdir()
        benchmark._iteration_root_owned.set(True)
        scratch = outside/'owned'
        scratch.mkdir()
        (scratch/'residue').write_bytes(b'owned')
        benchmark._iteration_scratch.set(scratch)
        raise KeyboardInterrupt()
    monkeypatch.setattr(benchmark, '_iteration', interrupt)
    with pytest.raises(KeyboardInterrupt):
        benchmark._observed_iteration(root, fixture)
    assert not root.exists() and list(outside.iterdir()) == []
    assert benchmark._iteration_scratch.get() is None
    assert benchmark._iteration_phases.get() is None
    assert benchmark._iteration_controls.get() is None
    assert not benchmark._iteration_root_owned.get()


def test_P16_T03_existing_iteration_directory_is_never_removed(tmp_path):
    root = tmp_path/'existing'
    root.mkdir()
    (root/'foreign').write_bytes(b'preserve')
    with pytest.raises(FileExistsError):
        benchmark._observed_iteration(root, {})
    assert (root/'foreign').read_bytes() == b'preserve'


@pytest.mark.skipif(sys.platform != 'linux', reason=(
    'Linux /proc namespace launcher; native portable timeout coverage runs in '
    'tools/release/tests/test_resource_lane.py'))
def test_P16_T04_host_timeout_reaps_its_owned_process_and_keeps_parent_limits(tmp_path, monkeypatch):
    original = host.subprocess.Popen
    launched = []
    def capture(*args, **kwargs):
        process = original(*args, **kwargs)
        launched.append(process)
        return process
    monkeypatch.setattr(host.subprocess, 'Popen', capture)
    before = host.host_snapshot()['nofile']
    with pytest.raises(subprocess.TimeoutExpired):
        host.run_host([sys.executable, '-c', 'import time; time.sleep(60)'],
                      controls={'nofile': 96}, report=tmp_path/'host.json', timeout=0.1)
    assert launched[0].poll() is not None
    assert host.host_snapshot()['nofile'] == before


@pytest.mark.skipif(sys.platform != 'linux', reason='Linux systemd scope and /proc namespace context')
def test_P16_T04_scoped_launcher_preserves_namespace_context_and_exact_owned_unit_suffix(tmp_path, monkeypatch):
    commands = []
    class Process:
        pid = 123
        returncode = 0
        def communicate(self, timeout=None):
            return None, None
        def poll(self):
            return 0
    def launch(command, **kwargs):
        commands.append(command)
        return Process()
    monkeypatch.setattr(host.subprocess, 'Popen', launch)
    result = host.run_host([sys.executable, '-c', 'pass'], controls={'memory_bytes': 1024}, report=tmp_path/'host.json')
    assert result['returncode'] == 0
    command = commands[0]
    assert '--scope' in command and '--wait' not in command and '--pipe' not in command
    assert next(value for value in command if value.startswith('--unit=')).endswith('.scope')


@pytest.mark.skipif(sys.platform != 'linux', reason='Linux systemd scope cleanup and /proc namespace context')
def test_P16_T04_scoped_timeout_preserves_causal_failure_when_stop_and_reap_also_fail(tmp_path, monkeypatch):
    calls = []
    primary = subprocess.TimeoutExpired(['owned'], 1)
    class Process:
        pid = 123
        returncode = None
        count = 0
        def communicate(self, timeout=None):
            self.count += 1
            if self.count == 1:
                raise primary
            raise OSError('private reap failure')
        def poll(self):
            return None
    monkeypatch.setattr(host.subprocess, 'Popen', lambda *args, **kwargs: Process())
    def failed_stop(command, **kwargs):
        calls.append(command)
        raise subprocess.TimeoutExpired(command, 30)
    monkeypatch.setattr(host.subprocess, 'run', failed_stop)
    killed = []
    monkeypatch.setattr(host.os, 'killpg', lambda pid, sig: killed.append(pid))
    with pytest.raises(subprocess.TimeoutExpired) as caught:
        host.run_host([sys.executable, '-c', 'pass'], controls={'memory_bytes': 1024},
                      report=tmp_path/'host.json', timeout=1)
    assert caught.value is primary
    assert killed == [123] and calls[0][-1].endswith('.scope')
    assert primary.__notes__ == ['owned qualification process reaping also failed']


@pytest.mark.skipif(sys.platform != 'linux', reason='Linux CPU affinity and cgroup host launcher configuration')
@pytest.mark.parametrize('cell_id,expected_host_bytes', [
    ('balanced-cpu1-external', 8 << 30),
    ('physical-default-cap-high-entropy', 64 << 30),
    ('constrained-below-minimum', 128 << 20),
])
def test_P16_T03_launcher_separates_fixture_cache_headroom_from_application_and_explicit_limits(
        tmp_path, monkeypatch, cell_id, expected_host_bytes):
    cell = _cell(cell_id)
    requested_budget = cell['configuration']['max_ram']
    seen = []
    monkeypatch.setattr(runner, 'candidate_identity', lambda *_: {})
    monkeypatch.setattr(runner, 'bind_environment', lambda *_: {})
    monkeypatch.setattr(runner.os, 'sched_getaffinity', lambda _: set(range(8)))

    def launch(command, *, controls, report):
        context = json.loads(Path(command[command.index('--inside-host') + 1]).read_text())
        assert context['host_controls'] == controls
        seen.append(controls)
        # Simulate the child's completed result, without starting cgroups or
        # allocating a physical-ceiling fixture in the unit-test process.
        runner._write_new(report.with_name(cell_id + '.json'), {'completed': True})
        return {'returncode': 0, 'report_written': True}

    monkeypatch.setattr(runner, 'run_host', launch)
    results = tmp_path / 'results'
    assert runner.main([
        '--wheel', str(tmp_path / 'candidate.whl'), '--wheel-sha256', 'a' * 64,
        '--sdist', str(tmp_path / 'candidate.tar.gz'), '--sdist-sha256', 'b' * 64,
        '--dependency-lock', str(tmp_path / 'lock.txt'), '--results', str(results),
        '--cell', cell_id,
    ]) == 0
    assert len(seen) == 1 and seen[0]['memory_bytes'] == expected_host_bytes
    frozen = next(row for row in json.loads((results / 'matrix.json').read_text())['cells']
                  if row['id'] == cell_id)
    assert frozen['configuration']['max_ram'] == requested_budget
