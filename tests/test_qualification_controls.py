"""P16-T01/T03/T04: private controls execute real checkpoints and always restore."""
import json
from pathlib import Path
import sys

import pytest

from readmixer_py import shuffle, sorting, stage_scheduler
from readmixer_py.config import MixConfig, StorageMode
from readmixer_py.errors import ResourceError
from test_synthetic_tooling import load_script

controls_module = load_script('qualification_controls')
fixtures = load_script('generate_fixtures')


def config(tmp_path, count=32, workers=1):
    data = tmp_path / 'fixture'
    fixture = fixtures.generate_fixtures(data, fixtures.FixtureConfig(counts=(count, count, 0),
        sequence_bytes=0, metadata_bytes=0, compression='plain'))
    scratch = tmp_path / 'scratch'
    scratch.mkdir()
    return MixConfig(input_dir=data / 'input', output=tmp_path / 'pool', workers=workers,
        storage=StorageMode.EXTERNAL, max_ram=2 << 30, max_record_bytes=fixture['max_record_bytes'], temp_dir=scratch)


def test_P16_T01_actual_forced_refinement_and_multiple_merges_restore_all_methods(tmp_path):
    cfg = config(tmp_path)
    originals = (shuffle.SerialShuffler.__init__, shuffle.SerialShuffler._refine,
                 sorting.ExternalSorter.__init__, stage_scheduler.worker_main)
    with controls_module.QualificationControls(dict(fixed_test_context='matrix-v1',
            terminal_pairs=1, sort_run_bytes=1024, sort_fan_in=2, scatter_strategy='sorter')) as controls:
        report = controls.mix(cfg)
        assert report.completed and report.pairs == 64
        assert controls.result['witnesses']['refinements'] > 1
        assert controls.result['witnesses']['sibling_refinement_groups'] > 0
        assert controls.result['witnesses']['merge_passes'] > 1
    assert originals == (shuffle.SerialShuffler.__init__, shuffle.SerialShuffler._refine,
                         sorting.ExternalSorter.__init__, stage_scheduler.worker_main)
    assert not list(cfg.temp_dir.iterdir())


def test_P16_T03_real_capacity_query_failure_preserves_sources_and_restores(tmp_path):
    cfg = config(tmp_path)
    original_files = {p.name: p.read_bytes() for p in cfg.input_dir.iterdir()}
    import os
    original_query = os.fstatvfs
    with controls_module.QualificationControls(dict(fail_space_query=True)) as controls:
        with pytest.raises(ResourceError, match='query capacity'):
            controls.mix(cfg)
        assert controls.result['witnesses']['space_query_failures'] >= 1
    assert os.fstatvfs is original_query
    assert {p.name: p.read_bytes() for p in cfg.input_dir.iterdir()} == original_files
    assert not list(cfg.temp_dir.iterdir())
    assert not list(cfg.output.iterdir())


@pytest.mark.parametrize('mode', ['external', 'memory'])
def test_P16_T04_real_late_completion_window_and_delayed_writes_with_spawn(tmp_path, mode):
    from dataclasses import replace
    cfg = replace(config(tmp_path, count=128, workers=4), storage=StorageMode(mode), max_ram=4 << 30)
    with controls_module.QualificationControls(dict(block_first_terminal=True,
            storage_write_delay_seconds=0.00001)) as controls:
        report = controls.mix(cfg)
        assert report.completed and report.workers_effective >= 3
        assert controls.result['witnesses']['late_results_retained'] == 2
        assert controls.result['witnesses']['late_result_window_expected'] == 2
        assert controls.result['witnesses']['late_result_window_observed'] == 2
        assert controls.result['witnesses']['earliest_terminal_incomplete'] is True
        assert controls.result['witnesses']['retained_job_workspace_bytes'] > 0
        assert controls.result['witnesses']['backpressure_events'] == 1
        assert (controls.result['witnesses']['delayed_writes'] > 0) == (mode == 'external')
    assert not list(cfg.temp_dir.iterdir())


def test_P16_T01_indexed_selected_member_witness_comes_from_actual_recovery(tmp_path):
    from readmixer_py.config import RecoverConfig
    from readmixer_py.recovery import recover
    cfg = config(tmp_path)
    with controls_module.QualificationControls(dict(fixed_test_context='matrix-v1')) as controls:
        mixed = controls.mix(cfg)
        assert controls.result['witnesses']['union_shared_selected_members'] == 0
        recovered = recover(RecoverConfig(manifest=cfg.output / mixed.manifest,
            output=tmp_path / 'recovered', sample=('sample000000', 'sample000001'),
            workers=1, storage=StorageMode.EXTERNAL, max_ram=2 << 30,
            max_record_bytes=cfg.max_record_bytes, temp_dir=cfg.temp_dir))
        assert recovered.completed
        assert controls.result['witnesses']['union_shared_selected_members'] == 2  # one distinct member in each mate lane


@pytest.mark.parametrize('settings', [dict(public_seed=3), dict(terminal_pairs=0),
    dict(terminal_pairs=True), dict(terminal_pairs=32769), dict(sort_fan_in=1),
    dict(fixed_test_context='secret'), dict(scatter_strategy='random'),
    dict(storage_write_delay_seconds=float('nan')), dict(block_first_terminal='yes')])
def test_P16_T01_invalid_controls_are_rejected_before_patching(settings):
    original = shuffle.SerialShuffler.__init__
    with pytest.raises(ValueError):
        controls_module.QualificationControls(settings)
    assert shuffle.SerialShuffler.__init__ is original


def test_P16_T01_nested_context_is_refused_and_exception_restores():
    original = shuffle.SerialShuffler.__init__
    with pytest.raises(RuntimeError, match='injected'):
        with controls_module.QualificationControls({}):
            with pytest.raises(RuntimeError, match='exclusive'):
                with controls_module.QualificationControls({}):
                    pass
            raise RuntimeError('injected')
    assert shuffle.SerialShuffler.__init__ is original
    with controls_module.QualificationControls({}) as controls:
        assert controls.result['controls_applied']


@pytest.mark.skipif(sys.platform != 'linux', reason=(
    'Linux /proc/self/fd observer for dedicated-scratch ENOSPC qualification; '
    'native ENOSPC cleanup remains covered by storage and transaction tests'))
def test_P16_T03_admission_bypass_is_scoped_to_unchanged_owned_scratch_only(tmp_path, monkeypatch):
    from readmixer_py.disk import DiskBudget
    import errno
    import os
    from readmixer_py import storage
    scratch, foreign = tmp_path / 'scratch', tmp_path / 'foreign'
    scratch.mkdir()
    foreign.mkdir()
    original_facts = DiskBudget._facts
    monkeypatch.setattr(DiskBudget, '_facts', lambda self, target: (1, 17))
    original_write = os.write
    def exhausted(fd, data):
        raise OSError(errno.ENOSPC, 'test supplied kernel-like error')
    monkeypatch.setattr(os, 'write', exhausted)
    with controls_module.QualificationControls(dict(defer_scratch_space_refusal=True,
            scratch_space_probe_path=scratch)) as controls:
        budget = DiskBudget()
        assert budget._facts(scratch) == (1, 4 << 30)
        assert budget._facts(foreign) == (1, 17)
        assert 'injected admission bypass' in controls.result['admission_bypass_scope']
        for folder in (scratch, foreign):
            fd = os.open(folder / 'file', os.O_CREAT | os.O_WRONLY, 0o600)
            try:
                with pytest.raises(OSError) as error:
                    storage.os.write(fd, b'x')
                assert error.value.errno == errno.ENOSPC
            finally:
                os.close(fd)
        assert controls.result['witnesses']['os_enospc_observed'] == 1
        assert controls.result['witnesses']['synthetic_admission_capacity_queries'] == 1
    assert os.write is exhausted


def test_P16_T03_admission_bypass_refuses_changed_root_and_requires_explicit_path(tmp_path):
    from readmixer_py.disk import DiskBudget
    with pytest.raises(ValueError, match='explicit dedicated'):
        controls_module.QualificationControls(dict(defer_scratch_space_refusal=True))
    scratch = tmp_path / 'scratch'
    scratch.mkdir()
    with controls_module.QualificationControls(dict(defer_scratch_space_refusal=True,
            scratch_space_probe_path=scratch)):
        scratch.rename(tmp_path / 'old')
        scratch.mkdir()
        with pytest.raises(ResourceError, match='changed identity'):
            DiskBudget()._facts(scratch)
