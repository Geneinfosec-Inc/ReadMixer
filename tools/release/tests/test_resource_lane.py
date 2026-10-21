"""External resource launcher contracts; no production benchmark work here."""
from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest

SPEC = importlib.util.spec_from_file_location('release_resource_lane', Path(__file__).parents[1] / 'resource_lane.py')
lane = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(lane)


def cells():
    result = [dict(id='bounded-fixture', tier='bounded', private_controls={}, configuration={'workers_requested': 1}),
              dict(id='scale-fixture', tier='scale', private_controls={}, configuration={'workers_requested': 1}),
              dict(id='physical-fixture', tier='release-host', private_controls={}, configuration={'workers_requested': 4})]
    result += [dict(id=name, tier='controlled-host', private_controls={}, configuration={'workers_requested': 4})
               for name in sorted(lane.LINUX_ONLY | lane.PORTABLE_HOST)]
    return {'cells': result}


def test_mac_profile_explicitly_excludes_linux_enforcement():
    chosen, excluded = lane.select_cells(cells(), 'bounded', 'Darwin')
    assert {row['id'] for row in chosen} == lane.PORTABLE_HOST | {'bounded-fixture'}
    assert {row['cell_id'] for row in excluded} == lane.LINUX_ONLY
    assert all(row['status'] == 'not_applicable_on_platform' for row in excluded)
    assert not any('passed' in row.values() for row in excluded)


def test_linux_profile_includes_enforcement_and_keeps_scale_separate():
    chosen, excluded = lane.select_cells(cells(), 'bounded', 'Linux')
    assert {row['id'] for row in chosen} == lane.LINUX_ONLY | lane.PORTABLE_HOST | {'bounded-fixture'}
    assert excluded == []
    assert [row['id'] for row in lane.select_cells(cells(), 'scale', 'Linux')[0]] == ['scale-fixture']
    assert [row['id'] for row in lane.select_cells(cells(), 'physical', 'Darwin')[0]] == ['physical-fixture']


@pytest.mark.parametrize('profile,system', [('unknown', 'Linux'), ('bounded', 'Windows')])
def test_unknown_profiles_and_platforms_fail(profile, system):
    with pytest.raises(ValueError):
        lane.select_cells(cells(), profile, system)


def test_controls_are_measured_separately_from_application_budget():
    cell = cells()['cells'][2]
    assert lane.host_limits(cell, 'Linux', range(8)) == {'memory_bytes': 64 << 30, 'cpu_quota': 4, 'affinity': [0, 1, 2, 3]}
    assert lane.host_limits(cell, 'Darwin') == {}
    with pytest.raises(ValueError, match='affinity'):
        lane.host_limits(cell, 'Linux', [0])
    with pytest.raises(ValueError, match='Linux-only'):
        lane.host_limits(dict(cell, id='constrained-serial'), 'Darwin')
    assert lane.host_limits(dict(cell, private_controls={'rlimit_nofile': 96}), 'Darwin') == {'nofile': 96}


def test_environment_is_clean_before_interpreter_start_and_parent_unchanged(tmp_path, monkeypatch):
    monkeypatch.setenv('PYTHONPATH', '/private/source')
    monkeypatch.setenv('OMP_NUM_THREADS', '99')
    monkeypatch.setenv('MallocNanoZone', '0')
    before = dict(os.environ)
    env = lane.inherited_environment(tmp_path)
    assert 'PYTHONPATH' not in env
    assert all(env[key] == '1' for key in lane.NATIVE)
    assert env['TMPDIR'] == str(tmp_path)
    assert lane.allocator_environment(env)['MallocNanoZone'] == '0'
    assert dict(os.environ) == before
    with lane._temporary_environment(env):
        assert dict(os.environ) == env
    assert dict(os.environ) == before


def test_scripts_exact_and_no_adjacent_src(tmp_path):
    scripts = tmp_path / 'scripts'
    scripts.mkdir()
    script = scripts / 'example.py'
    script.write_text('pass\n')
    candidate = {'source_manifest': {'scripts/example.py': lane.fingerprint(script)}}
    assert lane.verify_scripts(scripts, candidate) == {'example.py': lane.fingerprint(script)}
    script.write_text('raise RuntimeError\n')
    with pytest.raises(ValueError, match='frozen'):
        lane.verify_scripts(scripts, candidate)
    script.write_text('pass\n')
    (tmp_path / 'src').mkdir()
    with pytest.raises(ValueError, match='adjacent'):
        lane.verify_scripts(scripts, candidate)


def test_portable_launcher_inherits_native_limits_and_reaps(tmp_path, monkeypatch):
    import datetime
    monkeypatch.setenv('PRIVATE_FAKE_TOKEN', 'do-not-record')
    output = tmp_path / 'stdout'
    command = [sys.executable, '-I', '-c',
        'import os; assert os.environ["OPENBLAS_NUM_THREADS"] == "1"; print("done")']
    record = lane.command_record(command, lane.inherited_environment(tmp_path), lane.utc_now())
    command_path = tmp_path / 'command.json'
    lane.write_new(command_path, record)
    with output.open('wb') as sink, (tmp_path / 'stderr').open('wb') as err:
        result = lane.portable_launch(command,
            environment=lane.inherited_environment(tmp_path), stdout=sink, stderr=err, timeout=10)
    assert result['returncode'] == 0 and output.read_text() == 'done\n'
    assert datetime.datetime.fromisoformat(record['started_utc']) <= datetime.datetime.fromisoformat(result['started_utc']) <= datetime.datetime.fromisoformat(result['ended_utc'])
    assert json.loads(command_path.read_bytes())['argv'] == command
    assert 'PRIVATE_FAKE_TOKEN' not in command_path.read_text() and 'do-not-record' not in command_path.read_text()
    assert command_path.stat().st_mode & 0o777 == 0o600
    assert record['allocator_environment'] == lane.allocator_environment()


@pytest.mark.parametrize('controlled', [False, True])
def test_allocator_startup_receipt_matches_exact_child_context_without_parent_mutation(tmp_path, controlled):
    before = dict(os.environ)
    env = lane.inherited_environment(tmp_path)
    expected = {name: value for name, value in zip(lane.ALLOCATOR_CONTROLS,
        ('0', '0', '1', '1') if controlled else (None,) * 4)}
    for name, value in expected.items():
        if value is None:
            env.pop(name, None)
        else:
            env[name] = value
    result = tmp_path / 'allocator.json'
    program = ('import importlib.util,pathlib; '
        f's=importlib.util.spec_from_file_location("resource_adapter",{str(Path(lane.__file__))!r}); '
        'm=importlib.util.module_from_spec(s); s.loader.exec_module(m); '
        f'observed=m.verify_allocator_environment({expected!r}); '
        f'm.write_new(pathlib.Path({str(result)!r}),observed)')
    with (tmp_path / 'stdout').open('wb') as out, (tmp_path / 'stderr').open('wb') as err:
        status = lane.portable_launch([sys.executable, '-I', '-c', program], environment=env,
            stdout=out, stderr=err, timeout=10)
    assert status['returncode'] == 0 and json.loads(result.read_bytes()) == expected
    assert dict(os.environ) == before
    assert result.stat().st_mode & 0o777 == 0o600


def test_allocator_context_mismatch_fails_without_changing_parent(monkeypatch):
    before = lane.allocator_environment()
    monkeypatch.setenv('MallocNanoZone', 'unexpected-context')
    with pytest.raises(ValueError, match='differs from launcher'):
        lane.verify_allocator_environment(before)
    assert os.environ['MallocNanoZone'] == 'unexpected-context'


def test_portable_timeout_kills_owned_group_without_touching_parent(tmp_path):
    with (tmp_path / 'stdout').open('wb') as sink, (tmp_path / 'stderr').open('wb') as err:
        started = time.monotonic()
        with pytest.raises(subprocess.TimeoutExpired):
            lane.portable_launch([sys.executable, '-I', '-c', 'import time; time.sleep(60)'],
                environment=lane.inherited_environment(tmp_path), stdout=sink, stderr=err, timeout=0.1)
    assert time.monotonic() - started < 8
    assert os.getpid() > 0


def test_candidate_identity_keeps_external_lock_separate():
    candidate = dict(version='test', candidate_id='identity', archives={
        'wheel': {'sha256': 'w' * 64}, 'sdist': {'sha256': 's' * 64}})
    identity = lane._candidate_identity(candidate, 'l' * 64)
    assert identity['dependency_lock_sha256'] == 'l' * 64
    assert identity['wheel_sha256'] == 'w' * 64


def test_portable_success_cannot_hide_cpu_downgrade():
    cell = {'configuration': {'workers_requested': 4}}
    good = {'warmup': {'outcome': 'complete', 'phases': {'mix': {'cpu_budget': 4},
        'verify': {'workers_effective': 4}}}, 'measured_runs': []}
    assert lane.observed_cpu_check(cell, good)['passed']
    good['warmup']['phases']['mix']['cpu_budget'] = 1
    assert not lane.observed_cpu_check(cell, good)['passed']
    assert lane.observed_cpu_check(cell, {'warmup': {'outcome': 'typed_resource_refusal'}})['passed']


def test_fingerprint_refuses_symlink_even_with_same_bytes(tmp_path):
    original = tmp_path / 'original'
    original.write_text('same')
    link = tmp_path / 'link'
    link.symlink_to(original)
    with pytest.raises(ValueError):
        lane.fingerprint(link)


def test_portable_child_nofile_observed_parent_unchanged_and_reports_exclusive(tmp_path):
    """Portable equivalent of the frozen Linux-only namespace launcher test."""
    import resource
    before = resource.getrlimit(resource.RLIMIT_NOFILE)
    result = tmp_path / 'child.json'
    script = ('import importlib.util, pathlib; '
              f's=importlib.util.spec_from_file_location("portable",{str(Path(lane.__file__))!r}); '
              'm=importlib.util.module_from_spec(s); s.loader.exec_module(m); '
              'observed=m.apply_portable_limits({"nofile":96}); '
              'assert observed["nofile"][0]==96; '
              f'm.write_new(pathlib.Path({str(result)!r}),observed)')
    with (tmp_path / 'stdout').open('wb') as out, (tmp_path / 'stderr').open('wb') as err:
        outcome = lane.portable_launch([sys.executable, '-I', '-c', script],
            environment=lane.inherited_environment(tmp_path), stdout=out, stderr=err, timeout=10)
    assert outcome['returncode'] == 0
    assert resource.getrlimit(resource.RLIMIT_NOFILE) == before
    assert result.stat().st_mode & 0o777 == 0o600
    with pytest.raises(FileExistsError):
        lane.write_new(result, {'nofile': [97, before[1]]})
    assert json.loads(result.read_text()) == {
        'nofile': [96, before[1]],
        'native_thread_environment': {name: '1' for name in lane.NATIVE},
    }


@pytest.mark.parametrize('controls', [{'affinity': [0]}, {'nofile': True}, {'nofile': 0}])
def test_portable_controls_fail_closed_before_mutating(controls):
    import resource
    before = resource.getrlimit(resource.RLIMIT_NOFILE)
    with pytest.raises(ValueError):
        lane.apply_portable_limits(controls)
    assert resource.getrlimit(resource.RLIMIT_NOFILE) == before


def test_changed_external_helper_cannot_inherit_invocation_identity(monkeypatch, tmp_path):
    monkeypatch.setattr(lane, '__file__', str(tmp_path / 'resource_lane.py'))
    for name in lane.HARNESS:
        (tmp_path / name).write_text('pass\n')
    expected = {name: lane.fingerprint(tmp_path / name) for name in lane.HARNESS}
    lane.check_harness(expected)
    snapshot = tmp_path / 'snapshot'
    lane.snapshot_harness(snapshot)
    assert {path.name: lane.fingerprint(path) for path in snapshot.iterdir()} == expected
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in snapshot.iterdir())
    (tmp_path / 'candidate.py').write_text('changed\n')
    with pytest.raises(ValueError, match='harness changed'):
        lane.check_harness(expected)
    assert lane.fingerprint(snapshot / 'candidate.py') == expected['candidate.py']
