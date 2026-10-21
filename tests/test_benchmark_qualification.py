"""Failed measurements survive and cannot become successful performance cells."""
from pathlib import Path
import json
import pytest
from test_synthetic_tooling import benchmark, fixtures
from readmixer_py.errors import IntegrityError


def test_failed_phase_preserves_every_observation_and_suppresses_summary(monkeypatch):
    def fail(*args, **kwargs):
        raise IntegrityError('PRIVATE-UNPUBLISHED-DIAGNOSTIC')
    monkeypatch.setattr(benchmark, 'verify', fail)
    report = benchmark.benchmark(fixture_config=fixtures.FixtureConfig(counts=(1, 1, 0), sequence_bytes=3))
    assert not report['completed'] and report['summary'] is None
    assert report['temporary_workspace_removed']
    for run in (report['warmup'], *report['measured_runs']):
        assert run['failed_phase'] == 'verify' and run['error'] == 'IntegrityError'
        assert run['phases']['mix']['completed']
        assert not run['phases']['verify']['completed']
        assert run['phases']['verify']['wall_seconds'] >= 0
        assert run['originals_unchanged'] and run['foreign_files_unchanged'] and run['children_reaped']
        assert run['algorithmic_scratch_empty']
    assert 'PRIVATE-UNPUBLISHED-DIAGNOSTIC' not in json.dumps(report)


def test_real_detailed_workflow_records_actual_budgets_io_and_cleanup():
    report = benchmark.benchmark(fixture_config=fixtures.FixtureConfig(counts=(2, 1, 0), sequence_bytes=7), telemetry=True)
    assert report['completed']
    for run in (report['warmup'], *report['measured_runs']):
        assert run['artifact_summary']['count'] == 6
        assert run['artifact_summary']['bundles'] == 1
        assert run['artifact_summary']['largest_data_artifact_bytes'] > 0
        assert run['originals_unchanged'] and run['algorithmic_scratch_empty'] and run['children_reaped']
        for phase in run['phases'].values():
            assert phase['detailed']['complete']
            assert phase['resources']['effective_max_ram'] > 0
        original = run['phases']['mix']['detailed']['io']['original']
        assert original['opens'] == 6 and original['seek_calls'] == 0


def test_separate_scratch_directory_is_owned_and_removed(tmp_path):
    root = tmp_path/'retained'
    scratch_parent = tmp_path/'scratch-device'
    scratch_parent.mkdir()
    sentinel = scratch_parent/'foreign'
    sentinel.write_bytes(b'keep')
    report = benchmark.benchmark(fixture_config=fixtures.FixtureConfig(counts=(1, 1), sequence_bytes=3),
        work_dir=root, private_options={'scratch_parent': str(scratch_parent)})
    assert report['completed'] and report['artifacts_retained']
    assert list(scratch_parent.iterdir()) == [sentinel]
    assert sentinel.read_bytes() == b'keep'
    assert all(row['algorithmic_scratch_empty'] for row in [report['warmup'], *report['measured_runs']])
