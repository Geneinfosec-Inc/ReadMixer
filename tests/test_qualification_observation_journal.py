"""Completed observations survive interruption without qualifying partial runs."""
import json
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / 'scripts'))
import run_qualification as runner
from test_synthetic_tooling import benchmark, fixtures


def _cell():
    return next(row for row in runner.qualification_matrix()['cells']
                if row['id'] == 'balanced-cpu1-external')


def _rows(path):
    raw = path.read_bytes()
    assert raw.endswith(b'\n')
    return [json.loads(row) for row in raw.splitlines()]


def _arguments(tmp_path):
    context = tmp_path / 'context.json'
    context.write_text(json.dumps({'host_controls': {}}))
    return ['--wheel', str(tmp_path/'candidate.whl'), '--wheel-sha256', 'a'*64,
        '--sdist', str(tmp_path/'candidate.tar.gz'), '--sdist-sha256', 'b'*64,
        '--dependency-lock', str(tmp_path/'lock.txt'), '--results', str(tmp_path/'results'),
        '--cell', _cell()['id'], '--inside-host', str(context)]


def _bind(monkeypatch):
    monkeypatch.setattr(runner, 'candidate_identity', lambda *_: {'wheel_sha256': 'a'*64})
    monkeypatch.setattr(runner, 'bind_environment', lambda *_: {'dependency_lock_sha256': 'b'*64})


def test_P16_T05_journal_is_exclusive_private_and_fsyncs_each_complete_record(tmp_path, monkeypatch):
    path = tmp_path/'observations.jsonl'
    synced = []
    actual_fsync = os.fsync
    def fsync(descriptor):
        actual_fsync(descriptor)
        synced.append(path.read_bytes())
    monkeypatch.setattr(runner.os, 'fsync', fsync)
    declaration = {'candidate': {'wheel_sha256': 'a'*64}, 'matrix_cell': _cell()}
    with runner._ObservationJournal(path, declaration) as journal:
        journal.observe('warmup', 0, {'completed': True}, {'pairs': 2})
        journal.observe('measured', 0, {'completed': False, 'error': 'ResourceError'}, {'pairs': 2})
    assert path.stat().st_mode & 0o777 == 0o600
    rows = _rows(path)
    assert len(synced) == len(rows) == 4
    assert all(value.endswith(b'\n') for value in synced)
    assert rows[0]['candidate'] == declaration['candidate']
    assert rows[0]['matrix_cell'] == json.loads(json.dumps(_cell())) and not rows[0]['resume_supported']
    assert [row['kind'] for row in rows] == ['declaration', 'fixture', 'observation', 'observation']
    assert rows[-1]['observation']['error'] == 'ResourceError'
    original = path.read_bytes()
    with pytest.raises(FileExistsError):
        runner._ObservationJournal(path, {})
    assert path.read_bytes() == original
    link = tmp_path/'link.jsonl'
    link.symlink_to(path)
    with pytest.raises(FileExistsError):
        runner._ObservationJournal(link, {})
    assert path.read_bytes() == original


def test_P16_T05_benchmark_sink_runs_after_owned_cleanup_and_retains_prior_rows_on_interrupt(tmp_path, monkeypatch):
    calls = []
    roots = []
    primary = KeyboardInterrupt('PRIVATE INTERRUPTED PAYLOAD')
    def iteration(root, fixture, **options):
        roots.append(root)
        root.mkdir()
        benchmark._iteration_root_owned.set(True)
        scratch = root/'scratch'
        scratch.mkdir()
        (scratch/'owned').write_bytes(b'scratch')
        benchmark._iteration_scratch.set(scratch)
        calls.append(root.name)
        if len(calls) == 3:
            raise primary
        return {'completed': False, 'error': 'ResourceError', 'phases': {}}
    monkeypatch.setattr(benchmark, '_iteration', iteration)
    path = tmp_path/'observations.jsonl'
    with pytest.raises(KeyboardInterrupt) as caught:
        with runner._ObservationJournal(path, {'candidate': {}}) as journal:
            def sink(*args):
                assert not roots[-1].exists()
                assert benchmark._iteration_scratch.get() is None
                assert args[2]['owned_iteration_cleanup_seconds'] >= 0
                journal.observe(*args)
            benchmark.benchmark(fixture_config=fixtures.FixtureConfig(counts=(1, 1)), observation_sink=sink)
    assert caught.value is primary
    assert calls == ['warmup', 'run-0000', 'run-0001']
    assert all(not root.exists() and not root.parent.exists() for root in roots)
    rows = _rows(path)
    observations = [row for row in rows if row['kind'] == 'observation']
    assert [(row['phase'], row['index']) for row in observations] == [('warmup', 0), ('measured', 0)]
    assert all(row['observation']['originals_unchanged'] for row in observations)
    assert rows[-1] == dict(kind='interrupted', completed=False, completed_observations=2,
                           diagnostic_text_published=False)
    assert 'PRIVATE' not in path.read_text()


def test_P16_T05_callback_failure_propagates_after_cleanup_and_stops_later_observations(monkeypatch):
    roots = []
    def iteration(root, fixture, **options):
        root.mkdir()
        benchmark._iteration_root_owned.set(True)
        roots.append(root)
        return {'completed': False, 'error': 'ResourceError'}
    monkeypatch.setattr(benchmark, '_iteration', iteration)
    primary = OSError('PRIVATE JOURNAL FAILURE')
    def fail(*_):
        assert not roots[0].exists()
        raise primary
    with pytest.raises(OSError) as caught:
        benchmark.benchmark(fixture_config=fixtures.FixtureConfig(counts=(1, 1)), observation_sink=fail)
    assert caught.value is primary and len(roots) == 1
    assert not roots[0].parent.exists()


def test_P16_T05_invalid_sink_fails_before_creating_workspace(tmp_path):
    work = tmp_path/'must-not-exist'
    with pytest.raises(ValueError, match='observation_sink'):
        benchmark.benchmark(work_dir=work, observation_sink=42)
    assert not work.exists()


def test_P16_T05_interrupt_remains_causal_if_final_marker_write_also_fails(tmp_path, monkeypatch):
    path = tmp_path/'observations.jsonl'
    primary = KeyboardInterrupt('PRIVATE CAUSE')
    with pytest.raises(KeyboardInterrupt) as caught:
        with runner._ObservationJournal(path, {}) as journal:
            journal.observe('warmup', 0, {'completed': False}, {'pairs': 2})
            def fail(_):
                raise OSError('PRIVATE FSYNC FAILURE')
            monkeypatch.setattr(runner.os, 'fsync', fail)
            raise primary
    assert caught.value is primary
    assert primary.__notes__ == ['qualification observation journal final marker also failed']
    assert journal._sink.closed and journal._failed
    assert 'PRIVATE' not in path.read_text()
    assert any(row['kind'] == 'observation' for row in _rows(path))


def test_P16_T05_inside_host_interruption_returns_130_and_retains_candidate_bound_rows(tmp_path, monkeypatch, capsys):
    _bind(monkeypatch)
    primary = KeyboardInterrupt('PRIVATE TEST INTERRUPT')
    def interrupted(cell, candidate, **options):
        options['observation_sink']('warmup', 0, {'completed': True}, {'pairs': 2})
        options['observation_sink']('measured', 0, {'completed': False}, {'pairs': 2})
        raise primary
    monkeypatch.setattr(runner, 'run_cell', interrupted)
    args = _arguments(tmp_path)
    assert runner.main(args) == 130
    path = tmp_path/'results'/(_cell()['id']+'.observations.jsonl')
    rows = _rows(path)
    assert rows[0]['candidate']['wheel_sha256'] == 'a'*64
    assert rows[0]['matrix_definition_sha256'] == runner.qualification_matrix()['definition_sha256']
    assert rows[-1]['kind'] == 'interrupted' and rows[-1]['completed_observations'] == 2
    assert not path.with_name(_cell()['id']+'.json').exists()
    assert 'PRIVATE' not in path.read_text()+capsys.readouterr().out
    before = path.read_bytes()
    with pytest.raises(FileExistsError):
        runner.main(args)
    assert path.read_bytes() == before


@pytest.mark.parametrize('failure_point', ['observation', 'final_marker', 'close'])
def test_P16_T05_failed_checkpoint_cannot_publish_a_successful_final_report(tmp_path, monkeypatch, failure_point):
    _bind(monkeypatch)
    primary = OSError('PRIVATE CHECKPOINT FAILURE')
    def completed(cell, candidate, **options):
        options['observation_sink']('warmup', 0, {'completed': True}, {'pairs': 2})
        return {'completed': True}
    monkeypatch.setattr(runner, 'run_cell', completed)
    original = runner._ObservationJournal.append
    def append(journal, row):
        if row['kind'] == {'observation': 'observation', 'final_marker': 'observations_finished'}.get(failure_point):
            raise primary
        return original(journal, row)
    monkeypatch.setattr(runner._ObservationJournal, 'append', append)
    if failure_point == 'close':
        original_exit = runner._ObservationJournal.__exit__
        def close(journal, *args):
            original_exit(journal, *args)
            raise primary
        monkeypatch.setattr(runner._ObservationJournal, '__exit__', close)
    with pytest.raises(OSError) as caught:
        runner.main(_arguments(tmp_path))
    assert caught.value is primary
    results = tmp_path/'results'
    assert not (results/(_cell()['id']+'.json')).exists()
    path = results/(_cell()['id']+'.observations.jsonl')
    assert 'PRIVATE' not in path.read_text()


def test_P16_T05_successful_final_report_follows_durable_complete_observations(tmp_path, monkeypatch):
    _bind(monkeypatch)
    def completed(cell, candidate, **options):
        for phase, count in [('warmup', 1), ('measured', 5)]:
            for index in range(count):
                options['observation_sink'](phase, index, {'completed': True}, {'pairs': 2})
        return {'completed': True}
    monkeypatch.setattr(runner, 'run_cell', completed)
    actual_write = runner._write_new
    def write(path, report):
        journal = path.with_suffix('.observations.jsonl')
        rows = _rows(journal)
        assert rows[-1] == dict(kind='observations_finished', completed_observations=6,
                              qualification_requires_final_report=True)
        assert len([row for row in rows if row['kind'] == 'observation']) == 6
        actual_write(path, report)
    monkeypatch.setattr(runner, '_write_new', write)
    assert runner.main(_arguments(tmp_path)) == 0


def test_P16_T05_partial_write_preserves_prior_complete_lines_and_poisoned_sink(tmp_path, monkeypatch):
    path = tmp_path/'observations.jsonl'
    primary = OSError('PRIVATE SHORT WRITE')
    with pytest.raises(OSError) as caught:
        with runner._ObservationJournal(path, {}) as journal:
            journal.observe('warmup', 0, {'completed': True}, {'pairs': 2})
            previous = path.read_bytes()
            def fail(record, sink, **kwargs):
                sink.write('{"kind":')
                raise primary
            monkeypatch.setattr(runner.json, 'dump', fail)
            journal.observe('measured', 0, {'completed': True}, {'pairs': 2})
    assert caught.value is primary and journal._sink.closed and journal._failed
    assert path.read_bytes() == previous + b'{"kind":'
    assert journal.observations == 1
    assert 'PRIVATE' not in path.read_text()
    with pytest.raises(OSError, match='journal is incomplete'):
        journal.append({'kind': 'must-not-follow-partial-line'})
    assert path.read_bytes() == previous + b'{"kind":'


def test_P16_T05_real_workflow_sink_rows_match_complete_report_after_cleanup(tmp_path):
    path = tmp_path/'observations.jsonl'
    with runner._ObservationJournal(path, {}) as journal:
        report = benchmark.benchmark(
            fixture_config=fixtures.FixtureConfig(counts=(1, 1), sequence_bytes=3),
            observation_sink=journal.observe)
    assert report['completed'] and report['temporary_workspace_removed']
    rows = [row for row in _rows(path) if row['kind'] == 'observation']
    assert [(row['phase'], row['index']) for row in rows] == [('warmup', 0), *[('measured', n) for n in range(5)]]
    # Serialization changes tuples to lists, but keeps every original field.
    expected = json.loads(json.dumps([report['warmup'], *report['measured_runs']]))
    assert [row['observation'] for row in rows] == expected
    for row in rows:
        observation = row['observation']
        assert observation['owned_iteration_cleanup_seconds'] >= 0
        assert observation['children_reaped'] and observation['algorithmic_scratch_empty']
        assert not observation['iteration_outputs_retained']
