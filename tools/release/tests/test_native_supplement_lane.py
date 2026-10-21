"""Native supplemental protocols preserve failed runs and exact inventories."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import native_supplement_lane as lane


def oracle_result(nodeid):
    return dict(exit_code=0, collected=[nodeid], outcomes=[dict(nodeid=nodeid, phases=[
        dict(phase=phase, outcome='passed', xfail=False, seconds=.01)
        for phase in ('setup', 'call', 'teardown')])],
        junit=dict(executed=1, passed=1, failed=0, errors=0, skipped=0),
        host_cpu_plan=dict(effective_limit=1, requested_workers=1))


def compression_result(cpus):
    return dict(completed=True, cpus=cpus, cpu_plan={'effective_limit': cpus},
        witnesses=dict(slots=cpus - 1, capacity=2 * (cpus - 1), concurrent_native_workspaces=cpus - 1,
            retained_completed_jobs=2 * (cpus - 1), retained_result_bytes=131000,
            **{name: True for name in lane.COMPRESSION_CHECKS}),
        admitted=dict(thread_bytes=20, total_budget_bytes=40),
        snapshots={name: dict(error=False, rss_bytes=10) for name in ('before_service', 'after_service_creation',
            'threads_before_codec', 'native_workspaces_live', 'completed_window_retained', 'after_join')},
        observed_precodec_thread_rss_growth_bytes=10, observed_combined_positive_rss_growth_bytes=20,
        observed_thread_growth_exceeds_thread_charge=False, observed_combined_growth_exceeds_combined_charge=False,
        original_compressobj_restored=True)


def test_all_inventory_is_exact_and_preserves_repetition_identity():
    plans = lane.observation_plan('all')
    assert len(plans) == len({row['id'] for row in plans}) == 122
    oracle = [row for row in plans if row['kind'] == 'oracle']
    assert len(oracle) == 108
    assert {row['nodeid'] for row in oracle} == set(lane.supplement.remaining_supplements()[0]['nodeids'])
    for nodeid in {row['nodeid'] for row in oracle}:
        observations = [row for row in oracle if row['nodeid'] == nodeid]
        assert [row['repetition'] for row in observations] == list(range(6))
        assert [row['warmup_or_measured'] for row in observations] == ['warmup'] + ['measured'] * 5
    assert len([row for row in plans if row['kind'] == 'compression']) == 12
    assert [row['counts'] for row in plans if row['kind'] == 'fresh'] == [[256, 128, 0]] * 2
    assert sum(row.get('expected_observations', 1) for row in plans) == 132


@pytest.mark.parametrize('fault', ['skip', 'xfail', 'duplicate', 'teardown', 'junit', 'wrong-node', 'exit'])
def test_oracle_requires_single_actual_node_all_phases_and_raw_junit(fault):
    nodeid = 'tests/test_oracle.py::test_one[rollover]'
    result = oracle_result(nodeid)
    assert lane.assess_oracle(nodeid, result)['accepted']
    if fault == 'skip': result['outcomes'][0]['phases'][0]['outcome'] = 'skipped'
    if fault == 'xfail': result['outcomes'][0]['phases'][1]['xfail'] = True
    if fault == 'duplicate': result['outcomes'].append(copy.deepcopy(result['outcomes'][0]))
    if fault == 'teardown': result['outcomes'][0]['phases'].pop()
    if fault == 'junit': result['junit']['skipped'] = 1
    if fault == 'wrong-node': result['collected'] = ['tests/test_other.py::test_other']
    if fault == 'exit': result['exit_code'] = 1
    assert not lane.assess_oracle(nodeid, result)['accepted']


@pytest.mark.parametrize('fault', ['cpu', 'slots', 'capacity', 'live', 'retained', 'decode', 'join', 'budget',
    'sample-error', 'missing-snapshot', 'thread-excess', 'combined-excess', 'restoration'])
def test_full_compression_window_never_qualifies_missing_witnesses_or_charge_excess(fault):
    result = compression_result(4)
    assert lane.assess_compression(result, 4)['accepted']
    if fault == 'cpu': result['cpu_plan']['effective_limit'] = 2
    if fault == 'slots': result['witnesses']['slots'] = 1
    if fault == 'capacity': result['witnesses']['capacity'] = 2
    if fault == 'live': result['witnesses']['concurrent_native_workspaces'] = 1
    if fault == 'retained': result['witnesses']['retained_completed_jobs'] = 1
    if fault == 'decode': result['witnesses']['all_bgzf_outputs_valid'] = False
    if fault == 'join': result['witnesses']['all_threads_joined'] = False
    if fault == 'budget': result['witnesses']['budget_zero'] = False
    if fault == 'sample-error': result['snapshots']['after_join']['error'] = True
    if fault == 'missing-snapshot': result['snapshots'].pop('native_workspaces_live')
    if fault == 'thread-excess': result['observed_precodec_thread_rss_growth_bytes'] = 21
    if fault == 'combined-excess': result['observed_combined_positive_rss_growth_bytes'] = 41
    if fault == 'restoration': result['original_compressobj_restored'] = False
    assert not lane.assess_compression(result, 4)['accepted']


def test_modified_archived_test_cannot_inherit_candidate_identity(tmp_path):
    tests = tmp_path / 'tests'
    tests.mkdir()
    test = tests / 'test_reference.py'
    test.write_text('def test_reference(): assert True\n')
    candidate = {'source_manifest': {'tests/test_reference.py': lane.resource.fingerprint(test)}}
    lane.verify_tests(tmp_path, candidate)
    test.write_text('def test_reference(): pass\n')
    with pytest.raises(ValueError, match='frozen candidate'):
        lane.verify_tests(tmp_path, candidate)


def test_later_fresh_failure_cannot_discard_earlier_observation_or_overwrite_it(tmp_path):
    journal = tmp_path / 'journal'
    journal.mkdir()
    identity, plan = {'candidate_id': 'exact candidate'}, {'cpus': 4, 'counts': [256, 128, 0]}
    successful = {'completed': True, 'elapsed_seconds': .2}
    lane.record_fresh_observation(journal, identity, plan, lambda root: successful, tmp_path / 'warmup')
    def failure(root):
        raise RuntimeError('later fresh child failed')
    row = lane.record_fresh_observation(journal, identity, plan, failure, tmp_path / 'measured-0000')
    assert row['completed'] is False and row['error'] == 'RuntimeError'
    assert json.loads((journal / 'warmup.json').read_bytes())['observation'] == successful
    assert json.loads((journal / 'measured-0000.json').read_bytes())['observation'] == row
    assert (journal / 'warmup.json').stat().st_mode & 0o777 == 0o600
    with pytest.raises(FileExistsError):
        lane.record_fresh_observation(journal, identity, plan, lambda root: successful, tmp_path / 'warmup')


def test_post_run_binding_failure_preserves_raw_measurement_without_acceptance(tmp_path, monkeypatch):
    plan = lane.observation_plan('oracle')[0]
    result = tmp_path / 'observation.json'
    context = dict(plan=plan, result=str(result), harness={}, test_root=str(tmp_path), archived_scripts=str(tmp_path),
        dependency_lock_sha256='l' * 64, environment_manifest_sha256='e' * 64,
        allocator_environment=lane.resource.allocator_environment())
    control = tmp_path / 'context.json'
    control.write_text(json.dumps(context))
    candidate = dict(version='test', candidate_id='exact', archives={
        'wheel': {'sha256': 'w' * 64}, 'sdist': {'sha256': 's' * 64}})
    calls = []
    def bind(context, *, installed):
        calls.append(True)
        if len(calls) > 1:
            raise ValueError('post-run dependency payload changed')
        return candidate, {'installed_dependency_payloads': []}
    monkeypatch.setattr(lane.resource, '_bind', bind)
    monkeypatch.setattr(lane.resource, '_verify_installed', lambda candidate: {'verified': True})
    monkeypatch.setattr(lane, '_check_harness', lambda expected: None)
    monkeypatch.setattr(lane, 'verify_tests', lambda root, candidate: {})
    monkeypatch.setattr(lane, '_oracle', lambda context: oracle_result(plan['nodeid']))
    for name in lane.resource.NATIVE:
        monkeypatch.setenv(name, '1')
    with pytest.raises(ValueError, match='post-run dependency'):
        lane._child(control)
    raw = json.loads((tmp_path / 'observation.measurement.json').read_bytes())
    assert raw['measurement']['outcomes'][0]['nodeid'] == plan['nodeid']
    assert raw['post_run_binding_verification_pending'] is True
    assert not result.exists()


def test_wrong_allocator_startup_context_is_rejected_before_candidate_imports(tmp_path, monkeypatch):
    expected = lane.resource.allocator_environment()
    monkeypatch.setenv('MallocNanoZone', 'different-from-declaration')
    for name in lane.resource.NATIVE:
        monkeypatch.setenv(name, '1')
    control = tmp_path / 'context.json'
    control.write_text(json.dumps(dict(harness={}, allocator_environment=expected)))
    monkeypatch.setattr(lane, '_check_harness', lambda expected: None)
    def forbidden_bind(*args, **kwargs):
        raise AssertionError('candidate imports must not happen after allocator mismatch')
    monkeypatch.setattr(lane.resource, '_bind', forbidden_bind)
    with pytest.raises(ValueError, match='differs from launcher'):
        lane._child(control)


@pytest.mark.parametrize('body,passed', [
    ('def test_probe(): assert 1 + 1 == 2\n', True),
    ('import pytest\n@pytest.mark.skip(reason="unavailable")\ndef test_probe(): pass\n', False),
    ('import pytest\n@pytest.fixture(autouse=True)\ndef cleanup():\n yield\n assert False\ndef test_probe(): pass\n', False),
])
def test_oracle_recorder_executes_real_pytest_and_preserves_skips_teardown_failures(tmp_path, body, passed):
    tests, work, junit = tmp_path / 'tests', tmp_path / 'work', tmp_path / 'junit.xml'
    tests.mkdir()
    (tests / 'test_probe.py').write_text(body)
    result = tmp_path / 'record.json'
    context = dict(test_root=str(tmp_path), work=str(work), junit=str(junit),
                   plan={'nodeid': 'tests/test_probe.py::test_probe'})
    program = ('import json,pathlib,sys; sys.path.insert(0,' + repr(str(Path(lane.__file__).parent)) + '); '
        'import native_supplement_lane as lane; result=lane._pytest_observation(' + repr(context) + '); '
        'pathlib.Path(' + repr(str(result)) + ').write_text(json.dumps(result))')
    env = dict(os.environ)
    env.pop('PYTHONPATH', None)
    completed = subprocess.run([sys.executable, '-I', '-c', program], env=env,
        capture_output=True, timeout=30)
    assert completed.returncode == 0, completed.stderr.decode()
    recorded = json.loads(result.read_bytes())
    recorded['host_cpu_plan'] = dict(effective_limit=1, requested_workers=1)
    assert lane.assess_oracle(context['plan']['nodeid'], recorded)['accepted'] is passed
    assert junit.is_file() and recorded['junit']['executed'] == 1


@pytest.mark.parametrize('kind', ['oracle', 'compression', 'fresh'])
def test_missing_measurements_never_receive_success(kind):
    plan = next(row for row in lane.observation_plan(kind))
    assert not lane.assess(plan, {})['accepted']
