"""External Plan17 harness checks; these are not frozen candidate pytest nodes."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import stat
import sys
import xml.etree.ElementTree as ET
import zipfile

import pytest

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
import platform_lane as lane
import platform_probe as probe


def suite_data():
    names = ['tests/test_bgzf.py::' + name for name in lane.ORACLES]
    collection = {'collected': names, 'exit_code': 0}
    execution = {'collected': names, 'exit_code': 0, 'package': {}, 'outcomes': [
        {'nodeid': name, 'phases': [{'phase': phase, 'outcome': 'passed', 'xfail': False, 'seconds': .01}
                                  for phase in ('setup', 'call', 'teardown')]} for name in names]}
    junit = {'sha256': '0' * 64, 'counts': {'executed': 2, 'passed': 2, 'failed': 0, 'errors': 0, 'skipped': 0}}
    return collection, execution, junit


@pytest.mark.parametrize('fault', ['skip', 'xfail', 'teardown', 'duplicate', 'duplicate-phase', 'missing', 'collection', 'junit', 'exit', 'oracle', 'count'])
def test_incomplete_or_failed_suite_never_qualifies(fault):
    collection, execution, junit = suite_data()
    expected = 2
    if fault == 'skip': execution['outcomes'][0]['phases'][1]['outcome'] = 'skipped'
    if fault == 'xfail': execution['outcomes'][0]['phases'][1]['xfail'] = True
    if fault == 'teardown': execution['outcomes'][0]['phases'][2]['outcome'] = 'failed'
    if fault == 'duplicate': execution['outcomes'].append(copy.deepcopy(execution['outcomes'][0]))
    if fault == 'duplicate-phase': execution['outcomes'][0]['phases'].append(copy.deepcopy(execution['outcomes'][0]['phases'][0]))
    if fault == 'missing': execution['outcomes'].pop()
    if fault == 'collection': execution['collected'] = execution['collected'][::-1]
    if fault == 'junit': junit['counts']['skipped'] = 1
    if fault == 'exit': execution['exit_code'] = 1
    if fault == 'oracle':
        execution['outcomes'][0]['nodeid'] = 'tests/test_fake.py::test_fake'
        collection['collected'][0] = 'tests/test_fake.py::test_fake'
    if fault == 'count': expected = 1
    assert not lane.assess_suite(collection, execution, junit, expected)['passed']


def test_junit_individual_case_counts_do_not_trust_suite_totals(tmp_path):
    path = tmp_path / 'report.xml'
    path.write_text('<testsuites><testsuite tests="999"><testcase name="pass"/><testcase name="fail"><failure/></testcase>'
                    '<testcase name="error"><error/></testcase><testcase name="skip"><skipped/></testcase></testsuite></testsuites>')
    assert lane.junit_summary(path)['counts'] == {'executed': 4, 'passed': 1, 'failed': 1, 'errors': 1, 'skipped': 1}


def write_wheel(path, payload, time=(2024, 1, 1, 0, 0, 0)):
    with zipfile.ZipFile(path, 'w') as archive:
        for name, value in payload.items():
            info = zipfile.ZipInfo(name, date_time=time)
            archive.writestr(info, value)


def test_rebuild_all_member_bytes_compared_independently_of_zip_timestamps(tmp_path):
    first, second = tmp_path / 'first.whl', tmp_path / 'second.whl'
    payload = {'readmixer_py/__init__.py': b'x=1', 'readmixer.dist-info/METADATA': b'version: 1'}
    write_wheel(first, payload)
    write_wheel(second, payload, (2025, 1, 1, 0, 0, 0))
    result = lane.compare_wheels(first, second)
    assert result['all_member_bytes_equal'] and not result['archive_bytes_equal']
    payload['readmixer.dist-info/METADATA'] = b'version: 2'
    write_wheel(second, payload)
    assert not lane.compare_wheels(first, second)['all_member_bytes_equal']


def test_imported_package_checked_against_actual_wheel(tmp_path):
    wheel = tmp_path / 'package.whl'
    write_wheel(wheel, {'readmixer_py/__init__.py': b'one'})
    facts = {'package_files': [{'name': '__init__.py', 'sha256': lane.hashlib.sha256(b'one').hexdigest()}]}
    assert lane.check_wheel_package(facts, wheel)['matched']
    facts['package_files'].append({'name': 'extra.py', 'sha256': '0' * 64})
    with pytest.raises(ValueError, match='package bytes'):
        lane.check_wheel_package(facts, wheel)


def test_clean_environment_clears_source_overrides_plugins_and_native_threads(monkeypatch, tmp_path):
    for name in ('PYTHONPATH', 'PYTHONHOME', 'PYTEST_ADDOPTS', 'PYTEST_PLUGINS', 'READMIXER_TEST_CONSOLE', 'PIP_INDEX_URL', 'VIRTUAL_ENV'):
        monkeypatch.setenv(name, 'private sentinel')
    monkeypatch.setenv('OPENBLAS_NUM_THREADS', '128')
    environment = lane.clean_environment(tmp_path / 'venv')
    assert all('private sentinel' != value for value in environment.values())
    assert all(environment[name] == '1' for name in lane.THREAD_NAMES)
    assert environment['PATH'].startswith(str(tmp_path / 'venv' / 'bin') + os.pathsep)


def test_allocator_startup_context_is_preserved_and_read_back(monkeypatch):
    for name, value in zip(probe.ALLOCATOR_CONTROLS, ('0', '0', '1', '1')):
        monkeypatch.setenv(name, value)
    monkeypatch.setenv('MallocDiagnosticSentinel', 'explicit test value')
    monkeypatch.setenv('UNRELATED_PRIVATE_SENTINEL', 'must not be copied')
    expected = dict(zip(probe.ALLOCATOR_CONTROLS, ('0', '0', '1', '1')))
    assert {name: lane.clean_environment()[name] for name in expected} == expected
    assert probe.host()['allocator_environment'] == expected
    assert probe.private_allocator_environment()['MallocDiagnosticSentinel'] == 'explicit test value'
    assert 'UNRELATED_PRIVATE_SENTINEL' not in probe.private_allocator_environment()


def test_allocator_receipt_precedes_candidate_import_and_is_exclusive(monkeypatch, tmp_path):
    from types import SimpleNamespace
    monkeypatch.setenv('MallocLargeCache', '0')
    def stop_before_import(mode, root):
        raise RuntimeError('stop before candidate import')
    monkeypatch.setattr(probe, 'package_check', stop_before_import)
    args = SimpleNamespace(root=tmp_path, native_config=str(tmp_path / 'numpy-build.txt'))
    with pytest.raises(RuntimeError, match='stop before candidate import'):
        probe.facts(args)
    receipt = tmp_path / 'startup-allocator-environment.json'
    assert json.loads(receipt.read_bytes())['environment']['MallocLargeCache'] == '0'
    assert stat.S_IMODE(receipt.stat().st_mode) == 0o600
    with pytest.raises(FileExistsError):
        probe.facts(args)


def test_real_filesystem_probe_preserves_only_aggregate_facts(tmp_path):
    result = probe.filesystem(tmp_path)
    assert result['passed']
    assert str(tmp_path) not in json.dumps(result)
    assert list(tmp_path.iterdir()) == []


def test_command_failure_and_timeout_are_recorded_privately(tmp_path):
    runner = lane.Runner(tmp_path, timeout=1)
    assert not runner.command('failure', [sys.executable, '-I', '-c', 'print("private")\nraise SystemExit(3)'], tmp_path, lane.clean_environment())
    assert runner.stages[0]['exit_code'] == 3
    assert not runner.command('timeout', [sys.executable, '-I', '-c', 'import time;time.sleep(10)'], tmp_path, lane.clean_environment())
    assert runner.stages[1]['timed_out']
    from datetime import datetime
    for stage in runner.stages:
        assert datetime.fromisoformat(stage['started_utc']) <= datetime.fromisoformat(stage['ended_utc'])
        assert stage['elapsed_seconds'] >= 0
    assert 'private' not in json.dumps(runner.stages).replace('private/', '')
    assert stat.S_IMODE((tmp_path / 'private').stat().st_mode) == 0o700
    assert all(stat.S_IMODE(path.stat().st_mode) == 0o600 for path in (tmp_path / 'private').iterdir())


def test_json_evidence_is_exclusive_and_private(tmp_path):
    path = tmp_path / 'report.json'
    lane.write_json(path, {'accepted': False})
    with pytest.raises(FileExistsError):
        lane.write_json(path, {'accepted': True})
    assert json.loads(path.read_text()) == {'accepted': False}
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


def fresh_capture_fixture(tmp_path):
    base, private = tmp_path / 'basetemp', tmp_path / 'private'
    base.mkdir(mode=0o700)
    private.mkdir(mode=0o700)
    case = base / 'test_case0'
    case.mkdir(mode=0o700)
    fresh = case / 'fresh-scenario-0123456789ab'
    fresh.mkdir(mode=0o700)
    return base, private, case, fresh


def test_fresh_scenario_capture_preserves_exact_partial_raw_bytes_and_permissions(tmp_path):
    base, private, case, fresh = fresh_capture_fixture(tmp_path)
    payload = {'ready.json': b'{"pid": 7}\n', 'receipt.json.partial': b'{"incomplete":',
               'native-context.private.json': b'{}\n', 'stdout.log': b'\x00\xffraw\n',
               'stderr.log': b'', 'command.private.json.partial': b'{'}
    for name, data in payload.items():
        (fresh / name).write_bytes(data)
    (case / 'pool').mkdir()
    (case / 'pool' / 'must-not-export').write_bytes(b'unrelated artificial workspace')
    (base / 'test_casecurrent').symlink_to(case, target_is_directory=True)
    result = lane.capture_fresh_scenarios(base, private, 'source')
    target = private / 'source-fresh-scenarios'
    manifest = json.loads((target / 'manifest.json').read_bytes())
    assert result['captured'] and manifest['complete']
    assert result['file_count'] == len(payload) and result['directory_count'] == 1
    assert result['total_bytes'] == sum(map(len, payload.values()))
    assert result['manifest_sha256'] == lane.sha256(target / 'manifest.json')
    assert {row['relative_path'] for row in manifest['files']} == {
        'test_case0/' + fresh.name + '/' + name for name in payload}
    for row in manifest['files']:
        path = target / row['relative_path']
        assert path.read_bytes() == payload[path.name] == (fresh / path.name).read_bytes()
        assert row['bytes'] == len(payload[path.name]) and row['sha256'] == lane.sha256(path)
        assert row['capture_complete']
    assert all(stat.S_IMODE(path.stat().st_mode) == (0o700 if path.is_dir() else 0o600)
               for path in [target, *target.rglob('*')])
    assert not any(path.name in ('pool', 'must-not-export', 'test_casecurrent') for path in target.rglob('*'))
    assert 'candidate_id' not in result and 'accepted' not in result


def test_fresh_scenario_empty_capture_is_explicit_and_does_not_touch_candidate(tmp_path):
    base, private = tmp_path / 'basetemp', tmp_path / 'private'
    base.mkdir(mode=0o700)
    private.mkdir(mode=0o700)
    candidate = base / 'candidate.whl'
    candidate.write_bytes(b'unrelated immutable candidate sentinel')
    result = lane.capture_fresh_scenarios(base, private, 'installed')
    manifest = json.loads((private / 'installed-fresh-scenarios' / 'manifest.json').read_bytes())
    assert result['captured'] and result['file_count'] == result['directory_count'] == 0
    assert manifest['complete'] and manifest['files'] == []
    assert candidate.read_bytes() == b'unrelated immutable candidate sentinel'


def test_fresh_scenario_source_ancestor_and_leaf_symlinks_are_refused(tmp_path):
    base, private, case, fresh = fresh_capture_fixture(tmp_path)
    (fresh / 'receipt.json').write_bytes(b'{}')
    alias = tmp_path / 'alias'
    alias.symlink_to(base, target_is_directory=True)
    with pytest.raises(OSError):
        lane.capture_fresh_scenarios(alias, private, 'source')
    ancestor = tmp_path / 'ancestor'
    ancestor.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(OSError):
        lane.capture_fresh_scenarios(ancestor / 'basetemp', private, 'source')
    assert not (private / 'source-fresh-scenarios').exists()


def test_fresh_scenario_matched_links_nonregular_and_unknown_files_are_refused(tmp_path):
    for fault in ('scenario-link', 'file-link', 'directory', 'fifo', 'hardlink', 'unknown', 'unsafe-name'):
        root = tmp_path / fault
        root.mkdir()
        base, private, case, fresh = fresh_capture_fixture(root)
        sentinel = root / 'foreign'
        sentinel.write_bytes(b'keep')
        if fault == 'scenario-link':
            fresh.rmdir()
            fresh.symlink_to(root, target_is_directory=True)
        elif fault == 'file-link':
            (fresh / 'receipt.json').symlink_to(sentinel)
        elif fault == 'directory':
            (fresh / 'receipt.json').mkdir()
        elif fault == 'fifo':
            os.mkfifo(fresh / 'receipt.json')
        elif fault == 'hardlink':
            os.link(sentinel, fresh / 'receipt.json')
        elif fault == 'unknown':
            (fresh / 'private-input.bin').write_bytes(b'must not copy')
        else:
            fresh.rename(case / 'fresh-scenario-not-a-hash')
        with pytest.raises((OSError, ValueError)):
            lane.capture_fresh_scenarios(base, private, 'source')
        assert not (private / 'source-fresh-scenarios').exists()
        assert sentinel.read_bytes() == b'keep'


def test_fresh_scenario_all_bounds_are_checked_before_copy(tmp_path):
    for fault, limits in [('directories', {'max_directories': 1}), ('files', {'max_files': 1}),
                          ('individual-bytes', {'max_file_bytes': 1}),
                          ('total-bytes', {'max_total_bytes': 3}),
                          ('discovery', {'max_discovery_entries': 1})]:
        root = tmp_path / fault
        root.mkdir()
        base, private, case, fresh = fresh_capture_fixture(root)
        (fresh / 'ready.json').write_bytes(b'{}')
        (fresh / 'receipt.json').write_bytes(b'{}')
        if fault == 'directories':
            (case / 'fresh-scenario-abcdef012345').mkdir()
        if fault == 'discovery':
            (base / 'test_other0').mkdir()
        with pytest.raises(ValueError, match='bound'):
            lane.capture_fresh_scenarios(base, private, 'source', **limits)
        assert not (private / 'source-fresh-scenarios').exists()
        assert (fresh / 'ready.json').read_bytes() == b'{}'


def test_fresh_scenario_capture_destination_is_exclusive_and_modes_are_required(tmp_path):
    base, private, case, fresh = fresh_capture_fixture(tmp_path)
    (fresh / 'receipt.json').write_bytes(b'{}')
    private.chmod(0o755)
    with pytest.raises(ValueError, match='0700'):
        lane.capture_fresh_scenarios(base, private, 'source')
    private.chmod(0o700)
    lane.capture_fresh_scenarios(base, private, 'source')
    before = (private / 'source-fresh-scenarios' / 'manifest.json').read_bytes()
    with pytest.raises(FileExistsError):
        lane.capture_fresh_scenarios(base, private, 'source')
    assert (private / 'source-fresh-scenarios' / 'manifest.json').read_bytes() == before
    with pytest.raises(ValueError):
        lane.capture_fresh_scenarios(base, private, '../unsafe')


def test_fresh_scenario_changed_source_retains_incomplete_manifest(tmp_path):
    base, private, case, fresh = fresh_capture_fixture(tmp_path)
    receipt = fresh / 'receipt.json'
    receipt.write_bytes(b'{}')
    original = lane.os.open
    def changing(name, flags, *args, **kwargs):
        if name == 'receipt.json' and flags & os.O_CREAT == 0:
            receipt.write_bytes(b'{"changed":true}')
        return original(name, flags, *args, **kwargs)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(lane.os, 'open', changing)
        with pytest.raises(ValueError, match='changed') as failure:
            lane.capture_fresh_scenarios(base, private, 'source')
    manifest = json.loads((private / 'source-fresh-scenarios' / 'manifest.json').read_bytes())
    assert not manifest['complete'] and manifest['error_type'] == 'ValueError'
    assert not manifest['files']
    reference = failure.value.fresh_scenario_capture_manifest
    assert reference['partial_manifest_ref'] == 'private/source-fresh-scenarios/manifest.json'
    assert reference['partial_manifest_sha256'] == lane.sha256(private / 'source-fresh-scenarios' / 'manifest.json')


def test_fresh_scenario_capture_failure_never_replaces_primary_suite_exception(tmp_path):
    private = tmp_path / 'private'
    private.mkdir(mode=0o700)
    original = KeyboardInterrupt('original suite interruption')
    def failed_capture(*_args, **_kwargs):
        raise ValueError('secondary capture failure')
    outcome = {'stages': [{'exit_code': 3}]}
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(lane, 'capture_fresh_scenarios', failed_capture)
        try:
            try:
                raise original
            finally:
                lane.retain_suite_fresh_scenarios(outcome, tmp_path / 'missing', private, 'source')
        except KeyboardInterrupt as caught:
            assert caught is original
    result = outcome['fresh_scenarios']['source']
    assert result['captured'] is False and outcome['stages'][0]['exit_code'] == 3
    diagnostic = private / Path(result['error_ref']).name
    assert lane.sha256(diagnostic) == result['error_sha256']
    assert json.loads(diagnostic.read_bytes())['active_suite_exception_type'] == 'KeyboardInterrupt'


def test_fresh_scenario_new_capture_interrupt_is_recorded_and_propagated(tmp_path):
    private = tmp_path / 'private'
    private.mkdir(mode=0o700)
    interrupt = KeyboardInterrupt('capture interrupted')
    def interrupted(*_args, **_kwargs):
        raise interrupt
    outcome = {}
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(lane, 'capture_fresh_scenarios', interrupted)
        with pytest.raises(KeyboardInterrupt) as caught:
            lane.retain_suite_fresh_scenarios(outcome, tmp_path / 'missing', private, 'installed')
    assert caught.value is interrupt and not outcome['fresh_scenarios']['installed']['captured']


def test_fresh_scenario_unsafe_destination_cannot_write_secondary_diagnostic(tmp_path):
    base, private, case, fresh = fresh_capture_fixture(tmp_path)
    alias = tmp_path / 'private-alias'
    alias.symlink_to(private, target_is_directory=True)
    outcome = {}
    result = lane.retain_suite_fresh_scenarios(outcome, base, alias, 'source')
    assert not result['captured'] and 'diagnostic_error_type' in result
    assert list(private.iterdir()) == []


def test_fresh_scenario_interrupt_during_error_recording_is_not_swallowed(tmp_path):
    private = tmp_path / 'private'
    private.mkdir(mode=0o700)
    interrupt = KeyboardInterrupt('diagnostic write interrupted')
    def failed_capture(*_args, **_kwargs):
        raise ValueError('capture failed')
    def interrupted_write(*_args, **_kwargs):
        raise interrupt
    outcome = {}
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(lane, 'capture_fresh_scenarios', failed_capture)
        patch.setattr(lane, '_private_json_at', interrupted_write)
        with pytest.raises(KeyboardInterrupt) as caught:
            lane.retain_suite_fresh_scenarios(outcome, tmp_path / 'missing', private, 'source')
    assert caught.value is interrupt
    assert not outcome['fresh_scenarios']['source']['captured']
    assert outcome['fresh_scenarios']['source']['diagnostic_error_type'] == 'KeyboardInterrupt'


def policy_fixture(tmp_path):
    (tmp_path / 'tests').mkdir()
    path = tmp_path / 'tests' / 'test_run_qualification.py'
    path.write_text('# frozen Linux host launcher tests\n')
    records = [{'nodeid': 'tests/test_run_qualification.py::test_linux_' + str(index),
                'source_sha256': lane.sha256(path), 'reason': 'Linux-only archived launcher'} for index in range(8)]
    return {'schema': 'readmixer.release.applicability.v1', 'candidate_id': 'candidate-test',
            'declared_before_first_macos_attempt': True, 'macos-arm64': records}


def test_applicability_binds_candidate_source_and_real_cell(tmp_path):
    policy = policy_fixture(tmp_path)
    assert probe.applicability_records(policy, 'candidate-test', 'linux-x86_64-py311', tmp_path) == []
    assert probe.applicability_records(policy, 'candidate-test', 'macos-arm64-py314', tmp_path) == policy['macos-arm64']
    with pytest.raises(ValueError, match='candidate'):
        probe.applicability_records(policy, 'replacement-candidate', 'macos-arm64-py314', tmp_path)
    (tmp_path / 'tests' / 'test_run_qualification.py').write_text('# changed')
    with pytest.raises(ValueError, match='fingerprint'):
        probe.applicability_records(policy, 'candidate-test', 'macos-arm64-py314', tmp_path)


@pytest.mark.parametrize('fault', ['duplicate', 'oracle', 'missing-declaration', 'unknown-count'])
def test_applicability_never_allows_arbitrary_waivers(tmp_path, fault):
    policy = policy_fixture(tmp_path)
    if fault == 'duplicate': policy['macos-arm64'][1] = policy['macos-arm64'][0]
    if fault == 'oracle': policy['macos-arm64'][0]['nodeid'] = 'tests/test_bgzf.py::' + lane.ORACLES[0]
    if fault == 'missing-declaration': policy['declared_before_first_macos_attempt'] = False
    if fault == 'unknown-count': policy['macos-arm64'].pop()
    with pytest.raises(ValueError):
        probe.applicability_records(policy, 'candidate-test', 'macos-arm64-py311', tmp_path)


def test_predeclared_na_stays_skipped_and_receives_no_pass_credit():
    collection, execution, junit = suite_data()
    na = {'nodeid': 'tests/test_run_qualification.py::test_linux_only', 'reason': 'Linux launcher'}
    collection['collected'].append(na['nodeid'])
    collection['not_applicable'] = [na]
    execution['not_applicable'] = [na]
    execution['outcomes'].append({'nodeid': na['nodeid'], 'phases': [
        {'phase': 'setup', 'outcome': 'skipped', 'not_applicable': True, 'xfail': False, 'seconds': .01},
        {'phase': 'teardown', 'outcome': 'passed', 'not_applicable': False, 'xfail': False, 'seconds': .01}]})
    junit['counts'].update(executed=3, skipped=1)
    result = lane.assess_suite(collection, execution, junit, 3, [na])
    assert result['passed']
    assert result['counts']['passed'] == 2 and result['counts']['skipped'] == 1
    assert result['not_applicable_count'] == 1
    assert result['nodes'][-1]['outcome'] == 'not_applicable'
    # An ordinary skip with the same node name cannot impersonate the hook.
    execution['outcomes'][-1]['phases'][0]['not_applicable'] = False
    assert not lane.assess_suite(collection, execution, junit, 3, [na])['passed']
    # No applicability policy means this remains an unqualified skipped test.
    assert not lane.assess_suite(collection, execution, junit, 3)['passed']


def test_real_pytest_hook_collects_every_na_node_and_records_skips(tmp_path, monkeypatch):
    from types import SimpleNamespace
    policy = policy_fixture(tmp_path)
    source = tmp_path / 'tests' / 'test_run_qualification.py'
    source.write_text('\n'.join('def test_linux_' + str(index) + '():\n    raise AssertionError("Linux launcher must not run on simulated macOS")\n' for index in range(8)))
    for row in policy['macos-arm64']:
        row['source_sha256'] = lane.sha256(source)
    policy_path = tmp_path / 'policy.json'
    policy_path.write_text(json.dumps(policy))
    monkeypatch.setattr(probe, 'package_check', lambda mode, root: {'mode': mode})
    monkeypatch.setattr(probe, 'host', lambda: {'cell': 'macos-arm64-py311'})
    (tmp_path / 'pytest.ini').write_text('[pytest]\ntestpaths = tests\n')
    monkeypatch.chdir(tmp_path)
    args = SimpleNamespace(mode='source', root=str(tmp_path), applicability=str(policy_path),
                           candidate_id='candidate-test', junit=str(tmp_path / 'junit.xml'),
                           basetemp=str(tmp_path / 'pytest-tmp'), collect_only=False, result=str(tmp_path / 'result.json'))
    assert probe.suite(args) == 0
    result = json.loads((tmp_path / 'result.json').read_bytes())
    assert len(result['collected']) == len(result['outcomes']) == 8
    assert all(row['phases'][0]['not_applicable'] for row in result['outcomes'])
    assert lane.junit_summary(tmp_path / 'junit.xml')['counts'] == {'executed': 8, 'passed': 0, 'failed': 0, 'errors': 0, 'skipped': 8}


def test_wrong_host_is_failed_evidence_before_environment_or_workspace_creation(tmp_path, monkeypatch):
    monkeypatch.setattr(lane, 'host', lambda: {'cell': 'linux-x86_64-py311', 'implementation': 'CPython', 'pointer_bits': 64})
    work, report = tmp_path / 'new-work', tmp_path / 'report'
    result = lane.run_lane(tmp_path / 'candidate', tmp_path / 'environment', work, report, 'macos-arm64-py311')
    assert not result['completed'] and not result['accepted']
    assert result['failure']['stage'] == 'preflight'
    assert not work.exists()
    assert str(tmp_path) not in json.dumps(result)
    assert (report / 'private' / 'failure.txt').exists()


def test_existing_destinations_are_never_reused_or_deleted(tmp_path):
    work, report = tmp_path / 'existing-work', tmp_path / 'not-created'
    work.mkdir()
    sentinel = work / 'foreign'
    sentinel.write_bytes(b'keep')
    with pytest.raises(ValueError, match='new paths'):
        lane.run_lane(tmp_path / 'candidate', tmp_path / 'environment', work, report, 'linux-x86_64-py311')
    assert sentinel.read_bytes() == b'keep' and not report.exists()


def test_timeout_kills_ignoring_grandchild_even_when_parent_exits_on_term(tmp_path):
    import signal
    import time
    import psutil
    ready = tmp_path / 'grandchild.pid'
    child = 'import os,pathlib,signal,sys,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);pathlib.Path(sys.argv[1]).write_text(str(os.getpid()));time.sleep(60)'
    parent = 'import pathlib,subprocess,sys,time;p=subprocess.Popen([sys.executable,"-I","-c",sys.argv[1],sys.argv[2]]);ready=pathlib.Path(sys.argv[2]);\nwhile not ready.exists():time.sleep(.01)\ntime.sleep(60)'
    runner = lane.Runner(tmp_path, timeout=3)
    pid = None
    try:
        assert not runner.command('owned-group-timeout', [sys.executable, '-I', '-c', parent, child, str(ready)], tmp_path, lane.clean_environment())
        assert ready.exists(), 'grandchild must acknowledge SIGTERM-ignore before the timeout'
        pid = int(ready.read_text())
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            try:
                if psutil.Process(pid).status() == psutil.STATUS_ZOMBIE:
                    break  # Killed; an orphan's final wait belongs to the host init.
            except psutil.NoSuchProcess:
                break
            time.sleep(.01)
        else:
            pytest.fail('owned grandchild survived timeout escalation')
        assert runner.stages[0]['timed_out'] is True
    finally:
        if pid is None and ready.exists():
            pid = int(ready.read_text())
        if pid is not None:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
