"""P16-T01/T02/T03/T05: frozen definitions cannot masquerade as executed cells."""
import copy
import hashlib
import json

import pytest

from test_synthetic_tooling import load_script

matrix = load_script('qualification_matrix')


def definition():
    return matrix.qualification_matrix()


def cell(name):
    return next(value for value in definition()['cells'] if value['id'] == name)


def evidence(spec, *, refuse=False):
    # Deliberate validation-fixture evidence, not actual benchmark observations.
    q = dict(originals_unchanged=True, foreign_files_unchanged=True,
        scratch_cleanup_complete=True, children_reaped=True, host_controls_verified=True, telemetry_complete=True,
        algorithmic_disk_spill_bytes=0, witnesses=copy.deepcopy(spec['minimum_witnesses']),
        independent_normalized_bytes=spec['expected_normalized_bytes'],
        actual_operation_budgets={phase: spec['configuration']['max_ram'] for phase in matrix._PHASES},
        largest_artifact_bytes=spec['configuration']['max_artifact_bytes'])
    row = dict(completed=not refuse, wall_seconds=1,
        outcome='typed_resource_refusal' if refuse else 'complete',
        error='ResourceError' if refuse else None,
        correctness={key: True for key in matrix._CORRECTNESS},
        phases={phase: {'wall_seconds': 0.1} for phase in matrix._PHASES}, qualification=q)
    return dict(matrix_cell_id=spec['id'], matrix_definition_sha256=definition()['definition_sha256'],
        candidate=dict(wheel_sha256='a' * 64, dependency_lock_sha256='b' * 64),
        cache_policy='warm reuse; no clearing', configuration=copy.deepcopy(spec['configuration']),
        warmup=copy.deepcopy(row), measured_runs=[copy.deepcopy(row) for _ in range(5)])


def test_P16_T01_frozen_bounded_matrix_covers_dimensions_and_records_no_executions():
    report = definition()
    assert report == definition()
    payload = dict(report)
    digest = payload.pop('definition_sha256')
    assert digest == hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    assert len(report['cells']) == 25 and len({c['id'] for c in report['cells']}) == 25
    bounded = [c for c in report['cells'] if c['tier'] == 'bounded']
    assert len(bounded) == 12
    assert {c['configuration']['workers_requested'] for c in bounded} == {1, 2, 4}
    assert {c['configuration']['storage'] for c in bounded} == {'auto', 'memory', 'external'}
    assert {c['fixture']['compression'] for c in bounded} == {'plain', 'gzip', 'mixed'}
    assert all(c['status'] == 'not_run' and c['rationale'] and c['warmups'] == 1 and c['measured_runs'] == 5 for c in report['cells'])
    assert any(c['fixture']['length_mode'] == 'varied-v2' for c in bounded)
    raised = cell('variable-long-records')
    assert matrix.FixtureConfig(**raised['fixture']).max_record_bytes > 1 << 20
    assert raised['fixture']['profile'] == 'low-compressibility-v3'
    assert any(len(c['fixture']['counts']) > 30 for c in bounded)
    assert all(c['fixture']['counts'][-1] == 0 for c in bounded if c['id'] != 'imbalanced-sparse-refinement')
    assert not matrix.validate_result(report['cells'][0], report['cells'][0])['completed']


def test_P16_T02_scale_definitions_use_normalized_bytes_and_actual_budget_inequality():
    spec = cell('scale-over-four-budgets-external')
    assert spec['expected_normalized_bytes'] > 4 * spec['configuration']['max_ram']
    report = evidence(spec)
    assert matrix.validate_result(spec, report)['successful_scale_workflow']
    report['measured_runs'][2]['qualification']['actual_operation_budgets']['recover_indexed'] = 1 << 30
    verdict = matrix.validate_result(spec, report)
    assert not verdict['completed'] and not verdict['successful_scale_workflow']
    assert any('every actual B' in reason for reason in verdict['reasons'])


def test_P16_T02_exact_four_budgets_is_not_greater_than_four():
    spec = cell('scale-over-four-budgets-auto')
    report = evidence(spec)
    for row in [report['warmup'], *report['measured_runs']]:
        row['qualification']['actual_operation_budgets'] = {phase: spec['expected_normalized_bytes']//4 for phase in matrix._PHASES}
    assert not matrix.validate_result(spec, report)['completed']


def test_P16_T02_strict_memory_refusal_is_valid_only_with_cleanup_and_never_scale_success():
    spec = cell('scale-over-four-budgets-memory')
    report = evidence(spec, refuse=True)
    verdict = matrix.validate_result(spec, report)
    assert verdict['completed'] and not verdict['successful_scale_workflow']
    report['measured_runs'][0]['qualification']['algorithmic_disk_spill_bytes'] = 1
    assert not matrix.validate_result(spec, report)['completed']


@pytest.mark.parametrize('mutation', [
    lambda r: r.pop('candidate'),
    lambda r: r.update(matrix_definition_sha256='f' * 64),
    lambda r: r.update(measured_runs=r['measured_runs'][:4]),
    lambda r: r.pop('warmup'),
    lambda r: r.pop('cache_policy'),
    lambda r: r['configuration'].update(workers_requested=8),
    lambda r: r['measured_runs'][0]['correctness'].update(indexed_recovery_equal=False),
    lambda r: r['measured_runs'][0]['qualification'].update(children_reaped=False),
    lambda r: r['measured_runs'][0].update(wall_seconds=float('nan')),
    lambda r: r['warmup'].update(completed=False),
    lambda r: r['measured_runs'][0].update(phases=None),
    lambda r: r['measured_runs'][0].update(correctness=None),
])
def test_P16_T05_missing_or_failed_required_evidence_never_passes(mutation):
    spec = cell('balanced-cpu1-external')
    report = evidence(spec)
    mutation(report)
    assert not matrix.validate_result(spec, report)['completed']


def test_P16_T03_physical_near_cap_requires_actual_observed_sizes_and_host():
    spec = cell('physical-default-cap-high-entropy')
    report = evidence(spec)
    assert matrix.validate_result(spec, report)['completed']
    report['warmup']['qualification']['witnesses']['largest_data_artifact_bytes'] = 4_749_999_999
    assert not matrix.validate_result(spec, report)['completed']
    report = evidence(spec)
    report['warmup']['qualification']['largest_artifact_bytes'] = 5_000_000_001
    assert not matrix.validate_result(spec, report)['completed']
    report = evidence(spec)
    report['warmup']['qualification']['host_controls_verified'] = False
    assert not matrix.validate_result(spec, report)['completed']


def test_P16_T04_requested_backpressure_controls_are_not_execution_witnesses():
    spec = cell('slow-storage-ordered-backpressure')
    report = evidence(spec)
    report['warmup']['qualification']['witnesses'] = dict(spec['private_controls'])
    assert not matrix.validate_result(spec, report)['completed']


def test_P16_T02_refusals_cannot_acquire_performance_summary():
    spec = cell('scale-over-four-budgets-memory')
    report = evidence(spec, refuse=True)
    report['summary'] = {'wall_seconds': {'median': 0.1}}
    assert not matrix.validate_result(spec, report)['completed']


@pytest.mark.parametrize('malformed', [None, [], 'passed', {'candidate': None}, {'measured_runs': None}])
def test_P16_T01_malformed_results_fail_closed(malformed):
    assert not matrix.validate_result(cell('balanced-cpu1-memory'), malformed)['completed']


def test_P16_T01_adapter_uses_actual_budgets_io_and_counters_without_double_counting():
    spec = cell('scale-over-four-budgets-external')
    row = dict(completed=True, originals_unchanged=True, foreign_files_unchanged=True,
        algorithmic_scratch_empty=True, children_reaped=True,
        private_control_observations={'witnesses': {'merge_passes': 3}},
        phases={phase: dict(resources={'effective_max_ram': 512 << 20}, cpu_budget=2,
            workers_effective=2, detailed=dict(complete=True, errors=0,
                counters={'merge_passes': 7, 'spills': 0},
                io={'encrypted_scratch': {'write_bytes': 123}})) for phase in matrix._PHASES})
    qualified = matrix.qualify_observation(spec, row, {'normalized_bytes': spec['expected_normalized_bytes']}, True)
    q = qualified['qualification']
    assert q['witnesses']['merge_passes'] == 3
    assert q['witnesses']['ciphertext_spill_bytes'] == 4 * 123
    assert q['actual_operation_budgets'] == {phase: 512 << 20 for phase in matrix._PHASES}
    assert q['telemetry_complete'] and q['host_controls_verified']
    assert q['witnesses']['actual_process_slots'] == 1
    assert 'qualification' not in row


def test_P16_T01_adapter_never_infers_no_spill_from_absent_telemetry():
    spec = cell('scale-over-four-budgets-memory')
    observed = matrix.qualify_observation(spec, {'completed': False, 'error': 'ResourceError'}, {})
    assert observed['outcome'] == 'typed_resource_refusal'
    assert observed['qualification']['telemetry_complete'] is False
    assert 'algorithmic_disk_spill_bytes' not in observed['qualification']
    assert observed['qualification']['actual_operation_budgets'] == {}


def test_P16_T04_only_full_retained_result_window_qualifies():
    spec = cell('slow-storage-ordered-backpressure')
    report = evidence(spec)
    assert matrix.validate_result(spec, report)['completed']
    report['warmup']['qualification']['witnesses']['late_result_window_observed'] = 1
    assert not matrix.validate_result(spec, report)['completed']
    report = evidence(spec)
    report['warmup']['qualification']['witnesses']['earliest_terminal_incomplete'] = False
    assert not matrix.validate_result(spec, report)['completed']


def test_P16_T01_raised_limit_cell_requires_actual_above_default_record():
    spec = cell('variable-long-records')
    assert spec['minimum_witnesses']['max_record_bytes_observed'] == (1 << 20) + 1
    report = evidence(spec)
    assert matrix.validate_result(spec, report)['completed']
    report['warmup']['qualification']['witnesses']['max_record_bytes_observed'] = 1 << 20
    assert not matrix.validate_result(spec, report)['completed']


def test_P16_T05_short_read_performance_has_matched_predetermined_serial_parallel_cells():
    serial, parallel = cell('short-read-throughput-cpu1'), cell('short-read-throughput-cpu4')
    assert serial['fixture'] == parallel['fixture']
    assert serial['fixture']['counts'] == (50_000, 50_000, 0)
    assert serial['fixture']['sequence_bytes'] == 150
    assert serial['expected_normalized_bytes'] == 87_800_000
    assert serial['configuration']['max_ram'] == parallel['configuration']['max_ram'] == 2 << 30
    assert serial['private_controls'] == parallel['private_controls']
    assert serial['warmups'] == parallel['warmups'] == 1
    assert serial['measured_runs'] == parallel['measured_runs'] == 5
