#!/usr/bin/env python3
"""Freeze Plan 16 named workloads before timings; definitions are never results.

Emit JSON, or validate separately recorded evidence against one immutable cell.
Private controls name instrumentation seams to be witnessed by the runner; their
presence in this document does not establish that execution reached a checkpoint.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
import json
import math
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from generate_fixtures import FixtureConfig

_PHASES = ('mix', 'verify', 'recover_indexed', 'recover_sequential')
_CORRECTNESS = ('full_pool_verified', 'indexed_recovery_equal', 'sequential_recovery_equal',
                'independent_bytes_and_counts_equal', 'exact_identity_coverage_checked_by_pipeline')


def _cell(name, *, fixture=None, workers=1, storage='external', max_ram=2 << 30,
          selected=None, cap=131_100, controls=None, witnesses=None, tier='bounded',
          scenarios=('P16-T01', 'P16-T05'), rationale, expectation='complete', host=None):
    fixture = fixture or FixtureConfig(counts=(128, 128, 0), sequence_bytes=64,
        metadata_bytes=8, compression='mixed', profile='high-compressibility-v2')
    selected = tuple(range(len(fixture.counts))) if selected is None else selected
    return dict(id=name, status='not_run', required=True, tier=tier, scenarios=list(scenarios),
        rationale=rationale, fixture=asdict(fixture),
        configuration=dict(workers_requested=workers, storage=storage, max_ram=max_ram,
            max_artifact_bytes=cap, compression_level=6, selected_sample_indices=list(selected)),
        private_controls=controls or {}, minimum_witnesses=witnesses or {}, host_requirements=host or {},
        expected_outcome=expectation, warmups=1, measured_runs=5,
        comparison='independent normalized equality and coverage; six-artifact bytes only under fixed root/run/topology/codec/cap',
        expected_normalized_bytes=(2 * sum(fixture.counts) * fixture.max_record_bytes
                                   if fixture.length_mode == 'fixed' else None))


def qualification_matrix():
    # Seven comparable balanced cells isolate CPU/storage dimensions. Three
    # deliberate input-layout cells cover orthogonal record/selection/topology
    # behavior instead of multiplying every dimension into a giant Cartesian set.
    cells = []
    for workers, modes in ((1, ('memory', 'external', 'auto')), (2, ('external',)),
                           (4, ('memory', 'external', 'auto'))):
        for storage in modes:
            cells.append(_cell(f'balanced-cpu{workers}-{storage}', workers=workers, storage=storage,
                controls=dict(fixed_test_context='matrix-v1', terminal_pairs=32768),
                witnesses=dict(refinements=0),
                rationale='Matched balanced tiny baseline isolates storage and admitted CPU partition; empty companion and all nonempty samples selected.'))
    cells.extend([
        _cell('imbalanced-sparse-refinement', workers=4, storage='external',
            fixture=FixtureConfig(counts=(8, 700, 0, 5), sequence_bytes=128, metadata_bytes=16,
                                  compression='gzip', profile='low-compressibility-v3'), selected=(0, 2, 3),
            controls=dict(fixed_test_context='matrix-v1', terminal_pairs=4),
            witnesses=dict(refinements=2, sibling_refinement_groups=1, union_shared_selected_members=1),
            rationale='Strongly imbalanced original jobs, sparse overlapping selected occupants and selected empty sample; explicit private terminal bound forces witnessed sibling refinement.'),
        _cell('many-small-files-multipass', workers=2, storage='external',
            fixture=FixtureConfig(counts=(3,) * 32 + (0,), sequence_bytes=0, metadata_bytes=2,
                                  compression='plain', profile='high-compressibility-v2'),
            controls=dict(fixed_test_context='matrix-v1', sort_run_bytes=1024, sort_fan_in=2,
                          scatter_strategy='sorter'), witnesses=dict(merge_passes=2),
            rationale='Many independent small plain originals and empty read fields; forced low-run/fan-in private sort controls require real multiple merge passes.'),
        _cell('variable-long-records', workers=4, storage='auto',
            fixture=FixtureConfig(counts=(12, 9, 0), sequence_bytes=524_288, metadata_bytes=65_536,
                newline='crlf', compression='mixed', profile='low-compressibility-v3', length_mode='varied-v2'),
            cap=2_097_500, controls=dict(fixed_test_context='matrix-v1', terminal_pairs=1),
            witnesses=dict(refinements=1, bundles=2, max_record_bytes_observed=(1 << 20) + 1),
            rationale='Above-default 1 MiB valid record limits, long opaque metadata, variable lengths, CRLF and low observed compressibility; legitimate small cap produces multiple artifacts.'),
    ])
    # The tiny matched cells isolate initialization. These predetermined
    # conventional short-read lanes measure sustained Python record throughput;
    # large long-record scale fixtures cannot substitute for that workload.
    for workers in (1, 4):
        cells.append(_cell(f'short-read-throughput-cpu{workers}', workers=workers,
            fixture=FixtureConfig(counts=(50_000, 50_000, 0), sequence_bytes=150,
                metadata_bytes=32, compression='mixed', profile='v1', payload='varied'),
            cap=5_000_000_000, controls=dict(fixed_test_context='matrix-v1', terminal_pairs=32768),
            witnesses=dict(refinements=0),
            rationale='100,000 pairs at a predetermined conventional 150-byte read length measure complete-workflow throughput separately from tiny initialization and long-record scale cells; compare exact six-artifact hashes between admitted serial and CPU-four partitions.'))
    scale = FixtureConfig(counts=(4200, 4200, 0), sequence_bytes=65_536,
        metadata_bytes=32, compression='gzip', profile='high-compressibility-v2')
    for mode in ('external', 'auto', 'memory'):
        cells.append(_cell(f'scale-over-four-budgets-{mode}', fixture=scale, storage=mode,
            max_ram=512 << 20, cap=5_000_000_000, tier='scale',
            scenarios=('P16-T02', 'P16-T05'),
            controls=dict(require_normalized_over_four_actual_budgets=True),
            witnesses={} if mode == 'memory' else dict(ciphertext_spill_bytes=1, merge_passes=1),
            expectation='strict_memory_complete_or_typed_refusal' if mode == 'memory' else 'complete',
            host=dict(free_workspace_bytes=24 << 30, dedicated_resource_observation=True),
            rationale='Known normalized bytes exceed four times requested 512 MiB; runner must additionally prove every actual resolved operation budget. Auto needs observed ciphertext spill; memory refusal is never successful scale throughput.'))
    cells.append(_cell('physical-default-cap-high-entropy',
        fixture=FixtureConfig(counts=(20_000, 20_000, 0), sequence_bytes=65_536, metadata_bytes=32,
                              compression='plain', profile='low-compressibility-v3'),
        workers=4, max_ram=2 << 30, cap=5_000_000_000, tier='release-host',
        scenarios=('P16-T03', 'P16-T05'), witnesses=dict(bundles=2, largest_data_artifact_bytes=4_750_000_000),
        host=dict(free_workspace_bytes=80 << 30, dedicated_resource_observation=True),
        rationale='Actual low-compressibility physical artifact must reach at least 95% of the decimal default cap while every artifact remains capped; arithmetic predictions cannot pass this host cell.'))
    # Host controls are separately named cells, not ordinary CI assumptions.
    host_controls = (
        ('constrained-below-minimum', dict(memory_cgroup_bytes=128 << 20), 'typed_resource_refusal', {}),
        ('constrained-serial', dict(memory_cgroup_bytes=1 << 30, cpu_quota=1), 'complete', {'actual_cpu_budget': 1}),
        ('constrained-parallel', dict(memory_cgroup_bytes=4 << 30, cpu_quota=4), 'complete', {'actual_process_slots': 1}),
        ('low-descriptors', dict(rlimit_nofile=96), 'complete', {'descriptor_limit_observed': 96}),
        ('slow-storage-ordered-backpressure', dict(storage_write_delay_seconds=0.001, block_first_terminal=True), 'complete', {'late_results_retained': 1, 'backpressure_events': 1, 'late_result_window_expected': 2, 'late_result_window_observed': 2, 'earliest_terminal_incomplete': True}),
        ('memory-ordered-backpressure', dict(block_first_terminal=True), 'complete', {'late_results_retained': 2, 'backpressure_events': 1, 'late_result_window_expected': 2, 'late_result_window_observed': 2, 'earliest_terminal_incomplete': True}),
        ('space-query-failure', dict(fail_space_query=True), 'typed_resource_refusal', {'space_query_failures': 1}),
        ('scratch-device-exhaustion', dict(scratch_device_quota_bytes=1 << 20, defer_scratch_space_refusal=True), 'typed_resource_refusal', {'os_enospc_observed': 1, 'synthetic_admission_capacity_queries': 1}),
        ('separate-scratch-output-devices', dict(require_distinct_devices=True), 'complete', {'distinct_scratch_output_devices': True}),
    )
    for name, controls, expected, witnesses in host_controls:
        fixture = (FixtureConfig(counts=(4096, 4096, 0), sequence_bytes=256, metadata_bytes=32,
            profile='low-compressibility-v3', compression='plain') if name == 'scratch-device-exhaustion' else None)
        cells.append(_cell(name, fixture=fixture, storage='memory' if name == 'memory-ordered-backpressure' else 'external', workers=4 if 'parallel' in name or 'backpressure' in name else 1,
            max_ram=4 << 30 if name == 'memory-ordered-backpressure' else 2 << 30 if 'parallel' in name or 'backpressure' in name else 512 << 20,
            tier='controlled-host', scenarios=('P16-T03', 'P16-T04'), controls=controls,
            witnesses=witnesses, expectation=expected,
            host=dict(dedicated_host=True, apply_controls_before_import=True, actual_limits_required=True),
            rationale=('Dedicated scratch mount with explicitly injected admission-query bypass to reach actual OS ENOSPC; unchanged source and owned cleanup are still mandatory.' if name == 'scratch-device-exhaustion' else 'Controlled-host resource/failure qualification; requested controls alone cannot establish observed enforcement or checkpoint reachability.')))
    definition = dict(matrix_version=1, status='definitions_only', cells=cells,
        cache_policy='one identified warm-up and at least five retained measured observations; OS caches reused, never implicitly cleared',
        artifact_comparison_scope='matched balanced cells use one private fixed context; differing topology requires preservation and cap invariants',
        host_scope='Linux dedicated cgroup/quota/device lanes; macOS portable measurements are separately reported and never labeled Linux enforcement',
        required_supplemental_evidence=['stage_benchmark', 'fresh_process_complete_workflow_startup',
            'native_workspace_charge_calibration', 'full_supplied_data_private_validation'],
        rationale='12 bounded representative workflow cells, 3 scale modes, 1 physical-cap host case and 9 independent controlled-host gates; no unrun cell is a passing result')
    definition['definition_sha256'] = hashlib.sha256(json.dumps(definition, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    return definition


def _integer(value):
    return type(value) is int and value >= 0


def validate_result(cell, result):
    """Fail closed on incomplete cells; retain ordinary failure versus refusal.

    Runner evidence adds ``qualification`` per iteration: witnessed counters,
    actual operation budgets, ciphertext spill, source preservation and cleanup.
    This checker does not infer observations from requested configuration.
    """
    reasons = []
    if not isinstance(result, dict):
        return dict(completed=False, matrix_cell_id=cell['id'], reasons=['invalid result object'], successful_scale_workflow=False)
    if result.get('matrix_cell_id') != cell['id']:
        reasons.append('matrix cell identity is absent or different')
    if result.get('matrix_definition_sha256') != qualification_matrix()['definition_sha256']:
        reasons.append('frozen matrix fingerprint is absent or different')
    candidate = result.get('candidate', {})
    if not isinstance(candidate, dict):
        candidate = {}
    if not isinstance(result.get('cache_policy'), str) or not result['cache_policy']:
        reasons.append('explicit cache treatment is absent')
    if not all(isinstance(candidate.get(key), str) and len(candidate[key]) == 64
               and all(c in '0123456789abcdef' for c in candidate[key])
               for key in ('wheel_sha256', 'dependency_lock_sha256')):
        reasons.append('candidate or dependency-lock fingerprint is absent')
    if result.get('configuration') != cell['configuration']:
        reasons.append('configuration differs from frozen cell')
    runs = result.get('measured_runs', [])
    if not isinstance(runs, list) or len(runs) < cell['measured_runs']:
        reasons.append('fewer than five measured observations')
        runs = runs if isinstance(runs, list) else []
    warmup = result.get('warmup')
    if not isinstance(warmup, dict):
        reasons.append('identified warm-up is absent')
        warmup = {}
    for number, row in enumerate([warmup, *runs]):
        if not isinstance(row, dict):
            reasons.append(f'observation {number}: invalid report')
            continue
        qualification = row.get('qualification', {})
        if not isinstance(qualification, dict):
            qualification = {}
        prefix = f'observation {number}: '
        refusal = row.get('outcome') == 'typed_resource_refusal'
        allowed_refusal = cell['expected_outcome'] in ('typed_resource_refusal', 'strict_memory_complete_or_typed_refusal')
        if refusal:
            if not allowed_refusal or row.get('error') != 'ResourceError':
                reasons.append(prefix + 'unexpected or untyped refusal')
            if row.get('performance_summary') is not None:
                reasons.append(prefix + 'refusal acquired a successful performance summary')
        elif cell['expected_outcome'] == 'typed_resource_refusal':
            reasons.append(prefix + 'required typed refusal was not observed')
        else:
            correctness = row.get('correctness', {})
            if not isinstance(correctness, dict):
                correctness = {}
            if row.get('completed') is not True or not all(correctness.get(key) is True for key in _CORRECTNESS):
                reasons.append(prefix + 'complete independent workflow correctness is absent')
            phases = row.get('phases', {})
            if not isinstance(phases, dict):
                phases = {}
            if set(phases) != set(_PHASES):
                reasons.append(prefix + 'four operation phases are required')
            durations = [row.get('wall_seconds'), *(phase.get('wall_seconds') if isinstance(phase, dict) else None for phase in phases.values())]
            if any(type(value) not in (int, float) or not math.isfinite(value) or value <= 0 for value in durations):
                reasons.append(prefix + 'positive finite workflow and operation timings are absent')
        if not all(qualification.get(key) is True for key in ('originals_unchanged', 'foreign_files_unchanged', 'scratch_cleanup_complete', 'children_reaped')):
            reasons.append(prefix + 'preservation or cleanup witnesses are absent')
        if qualification.get('telemetry_complete') is not True:
            reasons.append(prefix + 'complete actual resource telemetry is absent')
        witnesses = qualification.get('witnesses', {})
        if not isinstance(witnesses, dict):
            witnesses = {}
        for key, minimum in cell['minimum_witnesses'].items():
            value = witnesses.get(key)
            if type(minimum) is bool:
                valid = value is minimum
            elif minimum == 0:
                valid = type(value) is int and value == 0
            else:
                valid = _integer(value) and value >= minimum
            if not valid:
                reasons.append(prefix + f'missing or insufficient {key} witness')
        if cell['private_controls'].get('block_first_terminal'):
            expected_window = witnesses.get('late_result_window_expected')
            if not _integer(expected_window) or expected_window < 2 or witnesses.get('late_result_window_observed') != expected_window or witnesses.get('earliest_terminal_incomplete') is not True:
                reasons.append(prefix + 'maximum retained completed-result window is unproved')
        if cell['configuration']['storage'] == 'memory' and qualification.get('algorithmic_disk_spill_bytes') != 0:
            reasons.append(prefix + 'strict-memory no-spill proof is absent')
        if cell['tier'] == 'scale' and not refusal:
            budgets = qualification.get('actual_operation_budgets', {})
            if not isinstance(budgets, dict):
                budgets = {}
            normalized = qualification.get('independent_normalized_bytes')
            if (set(budgets) != set(_PHASES) or not all(_integer(value) and value > 0 for value in budgets.values())
                    or not _integer(normalized) or normalized != cell['expected_normalized_bytes']
                    or normalized <= 4 * max(budgets.values(), default=0)):
                reasons.append(prefix + 'D > 4 * every actual B is unproved')
        if cell['tier'] == 'release-host':
            maximum = qualification.get('largest_artifact_bytes')
            if not _integer(maximum) or maximum > cell['configuration']['max_artifact_bytes']:
                reasons.append(prefix + 'actual physical artifact cap is unproved')
        if cell['host_requirements'] and qualification.get('host_controls_verified') is not True:
            reasons.append(prefix + 'host controls or required capacity were not verified')
    if any(isinstance(row, dict) and row.get('outcome') == 'typed_resource_refusal' for row in [warmup, *runs]) and result.get('summary') is not None:
        reasons.append('refused observation acquired a successful performance summary')
    return dict(completed=not reasons, matrix_cell_id=cell['id'], reasons=reasons,
                successful_scale_workflow=not reasons and cell['tier'] == 'scale'
                    and all(row.get('completed') is True and row.get('outcome') != 'typed_resource_refusal'
                            for row in [warmup, *runs]))



def qualify_observation(cell, row, fixture, host_verified=False):
    """Translate executed benchmark data, leaving absent observations absent.

    ``host_verified`` must come from actual runner controls/device/capacity
    checks, never from merely requesting them. The returned shallow copy keeps
    original phase evidence and adds a bounded aggregate qualification object.
    """
    result = dict(row)
    phases = row.get('phases') or {}
    controls = row.get('private_control_observations') or {}
    witnesses = dict(controls.get('witnesses') or {})
    counters, ciphertext, budgets, cpu, processes = {}, 0, {}, [], []
    telemetry_complete = bool(phases)
    for name, phase in phases.items():
        detailed = phase.get('detailed') or {}
        telemetry_complete &= detailed.get('complete') is True and detailed.get('errors') == 0
        for key, value in (detailed.get('counters') or {}).items():
            if _integer(value):
                counters[key] = counters.get(key, 0) + value
        written = detailed.get('io', {}).get('encrypted_scratch', {}).get('write_bytes')
        if _integer(written):
            ciphertext += written
        else:
            telemetry_complete = False
        budget = (phase.get('resources') or {}).get('effective_max_ram')
        if _integer(budget) and budget > 0:
            budgets[name] = budget
        observed_cpu = phase.get('cpu_budget')
        if _integer(observed_cpu) and observed_cpu > 0:
            cpu.append(observed_cpu)
        workers = phase.get('workers_effective')
        if _integer(workers) and workers > 0:
            processes.append(workers - 1)
    # Controls and telemetry observe the same calls. Prefer explicit controls;
    # adding both sources would double-count refinement and merge witnesses.
    for key, value in counters.items():
        witnesses.setdefault(key, value)
    if telemetry_complete:
        witnesses['ciphertext_spill_bytes'] = ciphertext
    observed_record = fixture.get('observed_lengths', {}).get('record_bytes', {}).get('maximum')
    if _integer(observed_record):
        witnesses['max_record_bytes_observed'] = observed_record
    artifacts = row.get('artifact_summary') or {}
    for key in ('bundles', 'largest_data_artifact_bytes'):
        if _integer(artifacts.get(key)):
            witnesses[key] = artifacts[key]
    if cpu:
        witnesses['actual_cpu_budget'] = max(cpu)
    if processes:
        witnesses['actual_process_slots'] = max(processes)
    # Host wrappers record measured limits/errno/device identities here.
    # Never infer these from the matrix's requested host configuration.
    for key, value in (row.get('host_witnesses') or {}).items():
        if type(value) is bool or _integer(value):
            witnesses[key] = value
    q = dict(originals_unchanged=row.get('originals_unchanged'),
        foreign_files_unchanged=row.get('foreign_files_unchanged'),
        scratch_cleanup_complete=row.get('algorithmic_scratch_empty'),
        children_reaped=row.get('children_reaped'), telemetry_complete=bool(telemetry_complete),
        witnesses=witnesses, actual_operation_budgets=budgets,
        independent_normalized_bytes=fixture.get('normalized_bytes'),
        largest_artifact_bytes=artifacts.get('largest_artifact_bytes'),
        host_controls_verified=host_verified is True)
    if telemetry_complete:
        q['algorithmic_disk_spill_bytes'] = ciphertext
    result['qualification'] = q
    if row.get('completed') is True:
        result['outcome'] = 'complete'
    elif row.get('error') == 'ResourceError':
        result['outcome'] = 'typed_resource_refusal'
    else:
        result['outcome'] = 'failed'
    return result


def qualification_result(cell, benchmark_result, *, candidate, host_verified=False):
    """Adapt a complete benchmark report to ``validate_result``'s strict schema."""
    fixture = benchmark_result.get('fixture') or {}
    configuration = benchmark_result.get('configuration') or {}
    selected = configuration.get('selected_sample_indices')
    configuration = {key: configuration.get(key) for key in cell['configuration']}
    if selected is not None:
        configuration['selected_sample_indices'] = list(selected)
    report = dict(matrix_cell_id=cell['id'], matrix_definition_sha256=qualification_matrix()['definition_sha256'],
        candidate=dict(candidate), configuration=configuration,
        cache_policy=benchmark_result.get('cache_policy'),
        warmup=qualify_observation(cell, benchmark_result.get('warmup') or {}, fixture, host_verified),
        measured_runs=[qualify_observation(cell, row, fixture, host_verified)
                       for row in benchmark_result.get('measured_runs', [])],
        summary=benchmark_result.get('summary'))
    report['acceptance'] = validate_result(cell, report)
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--validate', type=Path)
    parser.add_argument('--cell')
    args = parser.parse_args(argv)
    matrix = qualification_matrix()
    if args.validate is not None:
        cells = {cell['id']: cell for cell in matrix['cells']}
        if args.cell not in cells:
            parser.error('--cell must identify a frozen matrix cell')
        result = validate_result(cells[args.cell], json.loads(args.validate.read_text()))
        print(json.dumps(result, sort_keys=True))
        return 0 if result['completed'] else 1
    print(json.dumps(matrix, sort_keys=True, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
