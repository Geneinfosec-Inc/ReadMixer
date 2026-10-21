"""P16-T01/T05: fresh-process complete workflow scope and failed observations."""
import importlib.util
import json
from pathlib import Path
import sys

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / 'scripts'
sys.path.insert(0, str(_SCRIPTS))
_spec = importlib.util.spec_from_file_location('benchmark_fresh', _SCRIPTS / 'benchmark_fresh.py')
fresh = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = fresh
_spec.loader.exec_module(fresh)
from generate_fixtures import FixtureConfig, generate_fixtures


@pytest.mark.parametrize('options', [dict(repetitions=4), dict(repetitions=True), dict(repetitions=1001),
                                    dict(timeout=0), dict(timeout=True), dict(selected=()),
                                    dict(selected=(0, 0)), dict(selected=(3,))])
def test_P16_T05_fresh_benchmark_admits_protocol_before_fixture_creation(tmp_path, options):
    with pytest.raises(ValueError):
        fresh.fresh_benchmark(work_dir=tmp_path / 'absent', require_installed=False,
                              fixture_config=FixtureConfig(counts=(1, 1, 0)), **options)
    assert not (tmp_path / 'absent').exists()


def test_P16_T05_qualification_requires_frozen_identity_before_fixture_creation(tmp_path):
    with pytest.raises(ValueError, match='frozen candidate'):
        fresh.fresh_benchmark(work_dir=tmp_path / 'absent')
    assert not (tmp_path / 'absent').exists()


def test_P16_T05_six_fresh_interpreters_complete_every_workflow_and_independent_check(tmp_path):
    report = fresh.fresh_benchmark(fixture_config=FixtureConfig(counts=(1, 1, 0),
        sequence_bytes=2, metadata_bytes=0, compression='mixed'),
        work_dir=tmp_path / 'bench', require_installed=False)
    assert report['completed']
    assert len(report['measured_runs']) == 5
    assert report['summary'] is not None
    assert report['fixture']['pairs'] == 2
    for observation in [report['warmup'], *report['measured_runs']]:
        assert observation['completed'] and observation['exit_code'] == 0
        assert observation['stdout_bytes'] == observation['stderr_bytes'] == 0
        assert observation['owned_child_output_cleanup_complete']
        child = observation['child']
        assert child['candidate_matches']
        assert child['api_import_seconds'] >= 0 and child['harness_import_seconds'] >= 0
        assert child['before_readmixer_import']['rss_bytes'] > 0
        workflow = child['workflow']
        assert workflow['completed'] and workflow['algorithmic_scratch_empty']
        assert all(workflow['correctness'].values())
        assert set(workflow['phases']) == {'mix', 'verify', 'recover_indexed', 'recover_sequential'}
    assert {path.name for path in (tmp_path / 'bench').iterdir()} == {'fixtures'}
    assert str(tmp_path) not in json.dumps(report)


def _observation(tmp_path, monkeypatch, code, timeout=10):
    root = tmp_path / 'workspace'
    root.mkdir()
    fixture = generate_fixtures(root / 'fixtures', FixtureConfig(counts=(1, 1)))
    monkeypatch.setattr(fresh, '_CHILD', code)
    return fresh._observation(root / 'run', root / 'fixtures' / 'fixtures.json',
        dict(workers=1, storage='external', max_ram=512 << 20, max_artifact_bytes=1 << 20,
             compression_level=6, selected=[0, 1], telemetry=False),
        require_installed=False, timeout=timeout)


@pytest.mark.parametrize('code,expected', [
    ("raise RuntimeError('private path must not leak')", 'child_failed_or_emitted_diagnostics'),
    ("import sys; sys.exit(2)", 'child_failed_or_emitted_diagnostics'),
    ('pass', 'missing_or_oversize_child_report'),
    ("import sys; print('private records')", 'child_failed_or_emitted_diagnostics'),
    ("import json,pathlib,sys; cfg=json.loads(pathlib.Path(sys.argv[1]).read_bytes()); pathlib.Path(cfg['response']).write_text('bad-json')", 'invalid_child_report'),
    ("import json,pathlib,sys; cfg=json.loads(pathlib.Path(sys.argv[1]).read_bytes()); pathlib.Path(cfg['response']).write_text('{\"completed\":true,\"candidate_matches\":false}')", 'child_failed_or_correctness_mismatch'),
])
def test_P16_T05_fresh_child_failures_keep_status_without_private_diagnostics(tmp_path, monkeypatch, code, expected):
    observation = _observation(tmp_path, monkeypatch, code)
    assert not observation['completed'] and observation['error'] == expected
    assert observation['owned_child_output_cleanup_complete']
    public = json.dumps(observation)
    assert 'private path' not in public and 'private records' not in public and str(tmp_path) not in public
    assert {item.name for item in (tmp_path / 'workspace').iterdir()} == {'fixtures'}


def test_P16_T05_fresh_timeout_kills_and_reaps_owned_process(tmp_path, monkeypatch):
    observation = _observation(tmp_path, monkeypatch, 'import time; time.sleep(60)', timeout=0.1)
    assert not observation['completed'] and observation['timed_out']
    assert observation['error'] == 'timeout' and observation['exit_code'] != 0
    assert observation['wall_seconds'] < 10
    assert observation['owned_child_output_cleanup_complete']


def test_P16_T05_failed_cell_retains_warmup_and_all_five_observations_without_summary(tmp_path, monkeypatch):
    sequence = []
    def observation(*args, **kwargs):
        sequence.append(len(sequence))
        return dict(completed=len(sequence) != 3, wall_seconds=1.0, child_cpu_seconds=0.1,
                    error=None if len(sequence) != 3 else 'injected')
    monkeypatch.setattr(fresh, '_observation', observation)
    report = fresh.fresh_benchmark(fixture_config=FixtureConfig(counts=(1, 1)),
        work_dir=tmp_path / 'bench', require_installed=False)
    assert not report['completed'] and report['summary'] is None
    assert len(sequence) == 6 and len(report['measured_runs']) == 5
    assert report['measured_runs'][1]['error'] == 'injected'


def test_P16_T05_fresh_launch_failure_is_retained_for_every_required_observation(tmp_path, monkeypatch):
    def unavailable(*_args, **_kwargs):
        raise OSError('private interpreter path')
    monkeypatch.setattr(fresh.subprocess, 'Popen', unavailable)
    report = fresh.fresh_benchmark(fixture_config=FixtureConfig(counts=(1, 1)),
                                  require_installed=False)
    assert not report['completed'] and report['summary'] is None
    assert len(report['measured_runs']) == 5
    assert all(item['error'] == 'OSError' for item in [report['warmup'], *report['measured_runs']])
    assert report['temporary_workspace_removed']
    assert 'private interpreter path' not in json.dumps(report)


@pytest.mark.parametrize('changed', [False, True])
def test_P16_T05_fresh_tooling_is_bound_to_frozen_source_archive(tmp_path, monkeypatch, changed):
    import io
    import tarfile
    import qualify_supplied_data
    monkeypatch.setattr(qualify_supplied_data, 'candidate_identity', lambda *_: {'version': 'test'})
    archive_path = tmp_path / 'source.tar.gz'
    names = ('benchmark_fresh.py', 'benchmark_pipeline.py', 'generate_fixtures.py', 'resource_telemetry.py')
    with tarfile.open(archive_path, 'w:gz') as archive:
        for name in names:
            data = (_SCRIPTS / name).read_bytes()
            if changed and name == 'benchmark_pipeline.py':
                data += b'changed'
            info = tarfile.TarInfo('candidate/scripts/' + name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    if changed:
        with pytest.raises(ValueError, match='exact benchmark tool'):
            fresh._candidate(tmp_path / 'wheel', '0' * 64, archive_path, '0' * 64)
    else:
        result = fresh._candidate(tmp_path / 'wheel', '0' * 64, archive_path, '0' * 64)
        assert set(result['fresh_benchmark_tools_sha256']) == set(names)
