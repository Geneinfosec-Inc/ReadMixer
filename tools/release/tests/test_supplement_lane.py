"""Closed supplemental protocol checks; no fabricated native qualification."""
import copy
import datetime
import hashlib
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import supplement_lane as lane


def repeated(row, key='measured_runs'):
    return {'warmup': copy.deepcopy(row), key: [copy.deepcopy(row) for _ in range(5)]}


def calibration(label='calibration-default'):
    names = lane.CASES if label == 'calibration-default' else ('parser', 'packed_terminal')
    row = {'completed': True, 'child_reaped': True, 'cleanup_complete': True, 'sample_errors': 0,
           'sampled_growth_exceeds_charge': False, 'sampled_positive_rss_growth_bytes': 10, 'charged_bytes': 20}
    return {'completed': True, 'envelope_qualified': False,
            'scale_bytes': 1 << 20 if label == 'calibration-default' else 64 << 20,
            'pair_count': 32768, 'measured_runs': 5,
            'cases': [dict(case=name, observed_charge_excess=False, **repeated(row, 'measured')) for name in names]}


def components():
    expected = {'sha256': '0' * 64, 'logical_bytes': 10, 'items': 1}
    row = {'completed': True, 'error': None, 'result': expected}
    startup_row = {'completed': True, 'exit_code': 0, 'timed_out': False, 'stderr_bytes': 0,
                   'child': {'candidate_matches': True, 'completed': True}}
    return {'completed': True, 'scratch_cleanup_complete': True,
            'stages': {name: dict(expected=copy.deepcopy(expected), **repeated(row)) for name in lane.STAGES},
            'startup': {'completed': True, 'lanes': {name: repeated(startup_row) for name in ('interpreter', 'api_import', 'cli_inspect')}}}


def fresh(workers=4):
    workflow = {'completed': True, 'correctness': {'independent': True}, 'algorithmic_scratch_empty': True,
                'phases': {name: {'cpu_budget': workers, 'workers_effective': workers} for name in lane.PHASES}}
    row = {'completed': True, 'exit_code': 0, 'owned_child_output_cleanup_complete': True,
           'child': {'candidate_matches': True, 'completed': True, 'workflow': workflow}}
    return {'completed': True, 'configuration': {'workers': workers, 'max_ram': 2 << 30},
            'installed_qualification': True, **repeated(row)}


@pytest.mark.parametrize('label,report,count', [('components-startup', components(), 78),
    ('calibration-default', calibration(), 48), ('calibration-64mib', calibration('calibration-64mib'), 12),
    ('fresh-cpu1', fresh(1), 6), ('fresh-cpu4', fresh(4), 6)])
def test_exact_bound_protocols_keep_warmup_and_all_five_measurements(label, report, count):
    result = lane.assess(label, report)
    assert result['accepted'] and result['observations'] == count
    assert result['envelope_qualified'] is False


@pytest.mark.parametrize('fault', ['fewer', 'failure', 'excess-flag', 'excess-measured', 'missing-case', 'scale', 'reap', 'cleanup', 'envelope'])
def test_calibration_failures_and_excesses_are_never_qualified(fault):
    report = calibration()
    selected = report['cases'][0]
    if fault == 'fewer': selected['measured'].pop()
    if fault == 'failure': selected['warmup']['completed'] = False
    if fault == 'excess-flag': selected['observed_charge_excess'] = True
    if fault == 'excess-measured': selected['warmup']['sampled_positive_rss_growth_bytes'] = 21
    if fault == 'missing-case': report['cases'].pop()
    if fault == 'scale': report['scale_bytes'] = 64 << 20
    if fault == 'reap': selected['warmup']['child_reaped'] = False
    if fault == 'cleanup': selected['warmup']['cleanup_complete'] = False
    if fault == 'envelope': report['envelope_qualified'] = True
    assert not lane.assess('calibration-default', report)['accepted']


@pytest.mark.parametrize('fault', ['fewer', 'digest', 'startup-candidate', 'startup-stderr', 'stage-missing', 'residue'])
def test_component_and_startup_assertions_are_rechecked(fault):
    report = components()
    if fault == 'fewer': report['stages']['parser']['measured_runs'].pop()
    if fault == 'digest': report['stages']['parser']['warmup']['result']['sha256'] = '1' * 64
    if fault == 'startup-candidate': report['startup']['lanes']['api_import']['warmup']['child']['candidate_matches'] = False
    if fault == 'startup-stderr': report['startup']['lanes']['interpreter']['warmup']['stderr_bytes'] = 1
    if fault == 'stage-missing': report['stages'].pop('parser')
    if fault == 'residue': report['scratch_cleanup_complete'] = False
    assert not lane.assess('components-startup', report)['accepted']


@pytest.mark.parametrize('fault', ['fewer', 'cpu', 'candidate', 'correctness-empty', 'correctness-false', 'phase', 'cleanup', 'install'])
def test_complete_fresh_workflows_require_actual_cpu_and_independent_checks(fault):
    report = fresh()
    row = report['warmup']
    workflow = row['child']['workflow']
    if fault == 'fewer': report['measured_runs'].pop()
    if fault == 'cpu': workflow['phases']['verify']['workers_effective'] = 1
    if fault == 'candidate': row['child']['candidate_matches'] = False
    if fault == 'correctness-empty': workflow['correctness'] = {}
    if fault == 'correctness-false': workflow['correctness']['independent'] = False
    if fault == 'phase': workflow['phases'].pop('verify')
    if fault == 'cleanup': row['owned_child_output_cleanup_complete'] = False
    if fault == 'install': report['installed_qualification'] = False
    assert not lane.assess('fresh-cpu4', report)['accepted']


@pytest.mark.parametrize('label', lane.LABELS + ('unrecognized',))
def test_missing_reports_never_receive_vacuous_success(label):
    assert not lane.assess(label, {})['accepted']


def test_remaining_native_build_work_is_explicit_and_never_claims_historical_equivalence():
    pending = lane.remaining_supplements()
    assert pending[0]['configurations'] == 18 and pending[0]['total_observations'] == 108
    assert len(set(pending[0]['nodeids'])) == 18
    assert pending[1]['cpu_budgets'] == [2, 4] and pending[1]['total_observations'] == 12
    assert pending[2]['counts'] == [256, 128, 0]


def test_failed_supplement_launch_retains_exact_command_utc_exit_and_private_logs(tmp_path, monkeypatch):
    from types import SimpleNamespace
    harness = tmp_path / 'external' / 'tools' / 'release'
    harness.mkdir(parents=True)
    for name in lane.HARNESS:
        (harness / name).write_bytes((lane.HERE / name).read_bytes())
    monkeypatch.setattr(lane, 'HERE', harness)
    candidate = dict(version='test', candidate_id='identity', archives={
        'wheel': {'sha256': 'w' * 64}, 'sdist': {'sha256': 's' * 64}})
    for name in ('candidate', 'environment', 'scripts'):
        (tmp_path / name).mkdir()
    (tmp_path / 'environment/environment.json').write_text('{}')
    (tmp_path / 'environment/requirements.lock').write_text('fake==1\n')
    monkeypatch.setattr(lane.resource, '_bind', lambda context: (candidate, {}))
    monkeypatch.setattr(lane, 'LABELS', ('components-startup',))
    def launch(command, *, environment, stdout, stderr, timeout, diagnostic_prefix):
        stderr.write(b'preserved failed child diagnostics\n')
        journal = lane.resource._PortableJournal(diagnostic_prefix,
            {'schema': 'DATA_ONLY_FAKE_LAUNCH_REQUEST'})
        journal.event('group_absence', proof='killpg(group,0) raised ESRCH', verified=True)
        result = dict(returncode=7, cleanup_verified=True, coordinator_reaped=True,
                      post_reap_signals_sent=False, events=journal.events)
        journal.finish(result)
        return result
    monkeypatch.setattr(lane.resource, 'portable_launch', launch)
    args = SimpleNamespace(candidate=tmp_path / 'candidate', environment=tmp_path / 'environment',
        archived_scripts=tmp_path / 'scripts', workspace=tmp_path / 'workspace', report=tmp_path / 'report',
        installed_python=Path(sys.executable), timeout=10)
    assert lane.run_lane(args) == 1
    private = args.report / 'private'
    command = json.loads((private / 'components-startup.command.json').read_bytes())
    execution = json.loads((private / 'components-startup.execution.json').read_bytes())
    assert command['argv'] == [str(Path(sys.executable).absolute()), '-I', str(lane.HERE / 'supplement_lane.py'),
                              '--child', str(private / 'components-startup.context.json')]
    assert command['allocator_environment'] == lane.resource.allocator_environment()
    assert execution['returncode'] == 7 and execution['status'] == 'failed'
    assert datetime.datetime.fromisoformat(command['started_utc']) <= datetime.datetime.fromisoformat(execution['ended_utc'])
    assert (private / 'components-startup.stderr').read_bytes() == b'preserved failed child diagnostics\n'
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in private.iterdir())


def retention_context(tmp_path, label='calibration-default'):
    candidate = dict(version='test', candidate_id='exact', archives={
        'wheel': {'sha256': 'w' * 64}, 'sdist': {'sha256': 's' * 64}})
    context = dict(label=label, result=str(tmp_path / (label + '.json')), harness={},
        environment_manifest_sha256='e' * 64, dependency_lock_sha256='l' * 64,
        allocator_environment=lane.resource.allocator_environment())
    return context, candidate


def test_journals_preserve_exact_arguments_result_identity_clocks_and_each_case_repetition(tmp_path):
    from types import SimpleNamespace
    context, candidate = retention_context(tmp_path)
    token, keyword_token = object(), object()
    calls, returned = [], []
    def original(case, supplied, *, marker):
        assert supplied is token and marker is keyword_token
        calls.append(case)
        row = {'completed': len(calls) != 3, 'wall_seconds': .125, 'cpu_seconds': .0625,
               'error': 'retained failure' if len(calls) == 3 else None}
        returned.append(row)
        return row
    module = SimpleNamespace(observe=original)
    cases = ('parser', 'packed_terminal')
    with lane.RetainedObservations(context, candidate) as journal:
        journal.wrap(module, 'observe', lane.calibration_observation_path(cases))
        for case in cases:
            for _ in range(6):
                result = module.observe(case, token, marker=keyword_token)
                assert result is returned[-1]
    assert module.observe is original and calls == ['parser'] * 6 + ['packed_terminal'] * 6
    paths = sorted(journal.directory.glob('*.json'))
    assert len(paths) == 12
    for case_index, case in enumerate(cases):
        for number in range(6):
            name = 'warmup' if number == 0 else f'measured-{number - 1:04d}'
            path = journal.directory / f'calibration-{case}-{name}.json'
            value = json.loads(path.read_bytes())
            expected = returned[case_index * 6 + number]
            assert value['observation'] == expected
            canonical = json.dumps(expected, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode('ascii')
            assert value['observation_sha256'] == hashlib.sha256(canonical).hexdigest()
            assert value['measurement_path'] == ['measurement', 'cases', case_index]
            assert value['aggregate_result_name'] == 'calibration-default.json'
            assert value['phase'] == ('warmup' if number == 0 else 'measured')
            assert value['index'] == (0 if number == 0 else number - 1)
            assert value['original_returned'] is True
            assert path.stat().st_mode & 0o777 == 0o600


def test_interrupted_observation_keeps_prior_rows_reraises_same_exception_and_restores_hooks(tmp_path):
    from types import SimpleNamespace
    context, candidate = retention_context(tmp_path, 'components-startup')
    primary = KeyboardInterrupt('owned interrupted observation')
    count = []
    def original(*args, **kwargs):
        count.append(True)
        if len(count) == 1:
            return {'completed': True, 'wall_seconds': .25}
        raise primary
    other_original = lambda *args, **kwargs: {'completed': True}
    module, other = SimpleNamespace(observe=original), SimpleNamespace(observe=other_original)
    descriptor = lambda args, kwargs: ('stage/parser', ['measurement', 'stages', 'parser'])
    with pytest.raises(KeyboardInterrupt) as captured:
        with lane.RetainedObservations(context, candidate) as journal:
            journal.wrap(module, 'observe', descriptor)
            journal.wrap(other, 'observe', descriptor)
            assert module.observe()['completed'] is True
            module.observe()
    assert captured.value is primary
    assert module.observe is original and other.observe is other_original
    warmup = json.loads((journal.directory / 'stage-parser-warmup.json').read_bytes())
    failed = json.loads((journal.directory / 'stage-parser-measured-0000.json').read_bytes())
    assert warmup['observation'] == {'completed': True, 'wall_seconds': .25}
    assert failed['observation'] == {'completed': False, 'error_type': 'KeyboardInterrupt'}
    assert failed['original_returned'] is False


def test_journal_write_failure_blocks_success_and_restores_original(tmp_path, monkeypatch):
    from types import SimpleNamespace
    context, candidate = retention_context(tmp_path)
    original = lambda: {'completed': True, 'wall_seconds': .25}
    module = SimpleNamespace(observe=original)
    def failed_write(*args, **kwargs):
        raise OSError('retention destination unavailable')
    monkeypatch.setattr(lane.resource, 'write_new', failed_write)
    with pytest.raises(OSError, match='retention destination'):
        with lane.RetainedObservations(context, candidate) as journal:
            journal.wrap(module, 'observe', lambda args, kwargs: ('parser', ['measurement', 'cases', 0]))
            module.observe()
    assert module.observe is original


def test_failed_observation_retention_cannot_replace_the_original_interrupt(tmp_path, monkeypatch):
    from types import SimpleNamespace
    context, candidate = retention_context(tmp_path)
    primary = KeyboardInterrupt('original observation interruption')
    def original():
        raise primary
    def failed_write(*args, **kwargs):
        raise KeyboardInterrupt('journal interruption')
    module = SimpleNamespace(observe=original)
    monkeypatch.setattr(lane.resource, 'write_new', failed_write)
    with pytest.raises(KeyboardInterrupt) as captured:
        with lane.RetainedObservations(context, candidate) as journal:
            journal.wrap(module, 'observe', lambda args, kwargs: ('parser', ['measurement', 'cases', 0]))
            module.observe()
    assert captured.value is primary
    assert primary.__notes__ == ['supplement observation retention also failed']
    assert module.observe is original


def test_post_run_binding_failure_keeps_existing_supplement_measurement_without_acceptance(tmp_path, monkeypatch):
    context, candidate = retention_context(tmp_path, 'components-startup')
    context['archived_scripts'] = str(tmp_path)
    control = tmp_path / 'context.json'
    control.write_text(json.dumps(context))
    calls = []
    def bind(context, *, installed):
        calls.append(True)
        if len(calls) > 1:
            raise ValueError('post-run payload changed')
        return candidate, {'installed_dependency_payloads': []}
    monkeypatch.setattr(lane.resource, '_bind', bind)
    monkeypatch.setattr(lane.resource, '_verify_installed', lambda candidate: {})
    monkeypatch.setattr(lane, '_check_harness', lambda expected: None)
    monkeypatch.setattr(lane, '_measure', lambda context, candidate: (components(), {'script': 'unchanged'}))
    for name in lane.resource.NATIVE:
        monkeypatch.setenv(name, '1')
    with pytest.raises(ValueError, match='post-run payload'):
        lane._child(control)
    value = json.loads((tmp_path / 'components-startup.measurement.json').read_bytes())
    assert value['measurement'] == components()
    assert value['accepted'] is False and value['post_run_binding_verification_pending'] is True
    assert not (tmp_path / 'components-startup.json').exists()
