"""P16-T01: bounded instrumentation counts real work, preserves runtime behavior."""
import io
import json
import os
from pathlib import Path
import sys

import pytest

from test_synthetic_tooling import benchmark, fixtures
import resource_telemetry as telemetry
from readmixer_py.config import MixConfig, StorageMode, VerifyConfig, RecoverConfig
from readmixer_py import mix, verify, recover


@pytest.mark.parametrize('name,expected', [
    ('s.R1.fastq.gz', 'r1'), ('s.R2.fastq.gz', 'r2'), ('s.R1.fastq.gz.gzi', 'gzi'),
    ('s.key.csv.gz', 'key'), ('s.index.rmpyidx', 'index'), ('s.manifest.json', 'manifest'),
    ('recovery-receipt.json', 'receipt'), ('arbitrary-private-name', 'other_output')])
def test_closed_artifact_classes(name, expected):
    assert telemetry.artifact_class(name) == expected


def test_real_stream_io_short_reads_seeks_and_private_names_are_not_retained(tmp_path):
    from readmixer_py.fastq import DecodedStream
    raw = io.BytesIO(b'opaque\nbytes')
    with telemetry.ResourceTelemetry(tmp_path) as observed:
        with DecodedStream(raw) as source:
            assert source.read(3) == b'op'
            assert source.read(3) == b'aqu'
            source._raw.seek(0)
            while source.read(4):
                pass
    original = observed.result['io']['original']
    assert original['opens'] == 1 and original['seek_calls'] == 1
    assert original['read_bytes'] == 5 + len(b'opaque\nbytes')
    encoded = json.dumps(observed.result)
    assert str(tmp_path) not in encoded and 'opaque' not in encoded
    assert observed.result['complete']


def test_nested_observer_fails_without_disabling_outer_and_restores_on_error(tmp_path):
    from readmixer_py.fastq import DecodedStream
    original = DecodedStream.__init__
    original_write = os.write
    with pytest.raises(RuntimeError, match='sentinel'):
        with telemetry.ResourceTelemetry(tmp_path) as observed:
            with pytest.raises(RuntimeError, match='overlap'):
                with telemetry.ResourceTelemetry(tmp_path):
                    pass
            assert DecodedStream.__init__ is not original
            raise RuntimeError('sentinel')
    assert DecodedStream.__init__ is original and os.write is original_write
    with telemetry.ResourceTelemetry(tmp_path) as next_observer:
        pass
    assert next_observer.result['complete'] and observed.result['complete']


def test_install_failure_restores_already_applied_patches_and_lock(tmp_path, monkeypatch):
    original = os.write
    install = telemetry.ResourceTelemetry._install
    def fail(instance):
        instance._patch(os, 'write', lambda *_: None)
        raise ValueError('install failed')
    monkeypatch.setattr(telemetry.ResourceTelemetry, '_install', fail)
    with pytest.raises(ValueError, match='install failed'):
        with telemetry.ResourceTelemetry(tmp_path):
            pass
    assert os.write is original
    monkeypatch.setattr(telemetry.ResourceTelemetry, '_install', install)
    with telemetry.ResourceTelemetry(tmp_path) as observed:
        pass
    assert observed.result['complete']


def test_coexistence_counts_artifact_classes_and_ignores_symlinks(tmp_path):
    scratch = tmp_path / 'scratch'
    output = tmp_path / 'output'
    scratch.mkdir()
    output.mkdir()
    (scratch / 'secret-name').write_bytes(b'x' * 41)
    (output / 'sample.R1.fastq.gz').write_bytes(b'y' * 29)
    (output / 'link').symlink_to(scratch / 'secret-name')
    with telemetry.ResourceTelemetry(scratch, output, interval=1) as observed:
        observed._sample_storage()
        (scratch / 'secret-name').unlink()
    storage = observed.result['storage']
    assert storage['peak_coexisting_apparent_bytes'] == 70
    assert storage['scratch_when_coexistence_peaked'] == 41
    assert storage['output_when_coexistence_peaked'] == 29
    assert storage['peak_by_class']['encrypted_scratch'] == 41
    assert storage['peak_by_class']['r1'] == 29
    assert 'secret-name' not in json.dumps(observed.result)


@pytest.mark.parametrize('interval', [0, -1, 61, True])
def test_invalid_interval_rejected_before_patching(tmp_path, interval):
    with pytest.raises(ValueError):
        telemetry.ResourceTelemetry(tmp_path, interval=interval)


def test_overlapping_storage_roots_are_rejected(tmp_path):
    with pytest.raises(ValueError, match='disjoint'):
        telemetry.ResourceTelemetry(tmp_path, tmp_path / 'output')


def test_missing_and_corrupt_child_observations_invalidate_measurement(tmp_path):
    with telemetry.ResourceTelemetry(tmp_path) as observed:
        observed.result['workers']['expected'] = 2
        root = Path(observed._reports.name)
        (root / 'worker-fake.json').write_text('{"private": "never public"}')
    assert not root.exists()
    assert not observed.result['complete']
    assert observed.result['workers'] == dict(expected=2, received=0, missing=2,
        complete=False, lifetime_cpu_seconds=0.0, peak_individual_rss_bytes=0)
    assert 'never public' not in json.dumps(observed.result)


@pytest.mark.parametrize('detail', [False, True])
def test_full_serial_workflow_accounts_for_all_artifact_io_and_source_pass(tmp_path, detail):
    config = fixtures.FixtureConfig(counts=(5, 3, 0), compression='mixed', sequence_bytes=43)
    fixture = fixtures.generate_fixtures(tmp_path / 'fixture', config)
    scratch = tmp_path / 'scratch'
    scratch.mkdir()
    output = tmp_path / 'pool'
    with telemetry.ResourceTelemetry(scratch, output, detail=detail) as observed:
        report = mix(MixConfig(input_dir=tmp_path / 'fixture' / 'input', output=output,
            workers=1, max_ram=1 << 30, max_record_bytes=config.max_record_bytes,
            storage=StorageMode.EXTERNAL, temp_dir=scratch))
    result = observed.result
    assert result['complete'] and result['workers']['complete']
    assert result['io']['original']['opens'] == 6
    assert result['io']['original']['read_bytes'] == fixture['physical_bytes']
    assert result['io']['original']['seek_calls'] == 0
    assert result['counters']['source_pairs'] == fixture['pairs']
    assert result['stages']['frame_encode']['calls'] > 0 if detail else result['stages']['frame_encode']['calls'] == 0
    assert result['stages']['bgzf_encode']['calls'] > 0
    assert result['stages']['ranks']['calls'] > 0
    assert result['native']['lexsort']['observations'] <= 8
    assert result['io']['encrypted_scratch']['read_bytes'] > 0
    assert result['io']['encrypted_scratch']['write_bytes'] > 0
    document = json.loads((output / report.manifest).read_bytes())
    for category in ('r1', 'r2', 'key', 'index'):
        assert result['io'][category]['write_bytes'] == sum(bundle[category]['bytes'] for bundle in document['bundles'])
    assert result['io']['gzi']['write_bytes'] == sum(bundle[role]['bytes'] for bundle in document['bundles'] for role in ('r1_gzi', 'r2_gzi'))
    assert result['io']['manifest']['write_bytes'] == (output / report.manifest).stat().st_size
    with telemetry.ResourceTelemetry(scratch, detail=detail) as observed_verify:
        verification = verify(VerifyConfig(manifest=output / report.manifest, workers=1,
            max_ram=1 << 30, storage=StorageMode.EXTERNAL, temp_dir=scratch,
            max_record_bytes=config.max_record_bytes))
    assert verification.completed and observed_verify.result['complete']
    actual = observed_verify.result['io']
    assert actual['manifest']['read_bytes'] == (output / report.manifest).stat().st_size
    assert sum(values['read_bytes'] for key, values in actual.items() if key not in ('original', 'encrypted_scratch')) == verification.consumed_artifact_bytes
    destination = tmp_path / 'recovered'
    with telemetry.ResourceTelemetry(scratch, destination, detail=detail) as observed_recovery:
        recovered = recover(RecoverConfig(manifest=output / report.manifest, output=destination,
            sample=tuple(sample['sample_id'] for sample in fixture['samples']), workers=1,
            max_ram=1 << 30, storage=StorageMode.EXTERNAL, temp_dir=scratch,
            max_record_bytes=config.max_record_bytes))
    benchmark.check_recovered(recovered, destination, fixture['samples'])
    assert observed_recovery.result['stages']['indexed_gather']['calls'] > 0
    assert observed_recovery.result['stages']['indexed_gather']['failures'] == 0
    assert observed_recovery.result['io']['receipt']['write_bytes'] > 0
    assert not list(scratch.iterdir())


def test_real_spawn_workers_return_complete_aggregate_source_and_stage_observations(tmp_path):
    config = fixtures.FixtureConfig(counts=(5, 3, 0), compression='mixed', sequence_bytes=43)
    fixture = fixtures.generate_fixtures(tmp_path / 'fixture', config)
    scratch = tmp_path / 'scratch'
    scratch.mkdir()
    with telemetry.ResourceTelemetry(scratch, tmp_path / 'pool', detail=True) as observed:
        report = mix(MixConfig(input_dir=tmp_path / 'fixture' / 'input', output=tmp_path / 'pool',
            workers=2, max_ram=2 << 30, max_record_bytes=config.max_record_bytes,
            storage=StorageMode.EXTERNAL, temp_dir=scratch))
    result = observed.result
    assert report.workers_effective == 2
    assert result['workers']['expected'] == result['workers']['received'] == 1
    assert result['workers']['missing'] == 0
    assert result['complete'] and result['workers']['complete']
    assert result['io']['original']['opens'] == 6
    assert result['io']['original']['read_bytes'] == fixture['physical_bytes']
    assert result['io']['original']['seek_calls'] == 0
    assert result['counters']['source_pairs'] == fixture['pairs']
    assert result['counters']['worker_policy_jobs'] > 0
    assert result['counters']['scatter_generations'] > 0
    assert result['stages']['ranks']['calls'] > 0
    assert result['peaks']['scheduler_pending_jobs'] > 0
    assert result['io']['encrypted_scratch']['write_bytes'] > 0
    assert not list(scratch.iterdir())
    assert len(json.dumps(result)) < 128 << 10


def test_partial_syscall_write_counts_committed_bytes_and_preserves_cause(tmp_path, monkeypatch):
    native = os.write
    calls = []
    def short_then_fail(fd, data):
        if calls:
            raise OSError(28, 'private failure path')
        calls.append(True)
        return native(fd, memoryview(data)[:3])
    monkeypatch.setattr(os, 'write', short_then_fail)
    fd = os.open(tmp_path / 'output', os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        with telemetry.ResourceTelemetry(tmp_path) as observed:
            with observed._scope('write_class', 'r1'):
                assert os.write(fd, b'abcdef') == 3
                with pytest.raises(OSError) as caught:
                    os.write(fd, b'def')
                assert caught.value.errno == 28
    finally:
        os.close(fd)
    values = observed.result['io']['r1']
    assert values['write_bytes'] == 3 and values['write_calls'] == 1 and values['errors'] == 1
    assert (tmp_path / 'output').read_bytes() == b'abc'
    assert 'private failure path' not in json.dumps(observed.result)


def test_real_spill_and_multi_pass_merge_have_execution_witnesses(tmp_path):
    from readmixer_py.crypto import OperationKeys, pairs_identity
    from readmixer_py.resources import Budget
    from readmixer_py.storage import SegmentStore
    from readmixer_py.sorting import ExternalSorter
    budget = Budget(256 << 20)
    with telemetry.ResourceTelemetry(tmp_path, detail=False) as observed:
        with SegmentStore('auto', budget, OperationKeys.new(), temp_dir=tmp_path) as store:
            with store.new_segment(1, pairs_identity((0,), 0, 0)) as writer:
                writer.append(b'opaque' * 1000, item_count=1)
                ref = writer.seal()
            assert not ref.physical_bytes
            ref = store.spill(ref)
            assert ref.physical_bytes
            store.drop(ref)
            with ExternalSorter(store, key=int, run_bytes=700, max_record_bytes=12, fan_in=2) as sorter:
                for value in range(39, -1, -1):
                    sorter.append(str(value).encode())
                with sorter.iter_sorted() as values:
                    assert list(map(int, values)) == list(range(40))
                passes = sorter.merge_passes
    assert passes >= 4
    assert observed.result['counters']['spills'] >= 1
    assert observed.result['counters']['spill_decoded_bytes'] >= 6000
    assert observed.result['counters']['merge_passes'] == passes
    assert observed.result['counters']['merge_groups'] > passes
    assert observed.result['counters']['sort_runs'] > 1
    assert budget.used_bytes == 0 and not list(tmp_path.iterdir())


def test_raw_source_codec_admission_is_separate_from_one_forward_data_pass(tmp_path):
    from readmixer_py.catalog import build_catalog
    root = tmp_path / 'private-inputs'
    root.mkdir()
    for role in (1, 2):
        (root / f'A_R{role}.fastq').write_bytes(f'@a/{role}\nX\n+\n!\n'.encode())
    with telemetry.ResourceTelemetry(None) as observed:
        with build_catalog(input_dir=root):
            pass
    assert observed.result['io']['input_admission']['read_bytes'] == 4
    assert observed.result['io']['input_admission']['read_calls'] == 2
    assert observed.result['io']['original']['read_bytes'] == 0
    assert observed.result['io']['original']['opens'] == 0


def _exiting_worker(*_args, **_kwargs):
    os._exit(23)


def test_instrumentation_composes_with_private_worker_and_missing_report_invalidates(tmp_path, monkeypatch):
    from readmixer_py import stage_scheduler
    from readmixer_py.errors import ResourceError
    config = fixtures.FixtureConfig(counts=(1, 1), compression='plain', sequence_bytes=10)
    fixtures.generate_fixtures(tmp_path / 'fixture', config)
    scratch = tmp_path / 'scratch'
    scratch.mkdir()
    monkeypatch.setattr(stage_scheduler, 'worker_main', _exiting_worker)
    with telemetry.ResourceTelemetry(scratch, tmp_path / 'pool') as observed:
        with pytest.raises(ResourceError):
            mix(MixConfig(input_dir=tmp_path / 'fixture' / 'input', output=tmp_path / 'pool',
                workers=2, max_ram=2 << 30, max_record_bytes=config.max_record_bytes,
                storage=StorageMode.EXTERNAL, temp_dir=scratch))
    assert stage_scheduler.worker_main is _exiting_worker
    assert observed.result['workers']['expected'] == 1
    assert observed.result['workers']['missing'] == 1
    assert not observed.result['complete'] and not observed.result['workers']['complete']
    assert not list(scratch.iterdir())


@pytest.mark.parametrize('mode', [StorageMode.EXTERNAL, StorageMode.MEMORY])
def test_telemetry_and_maximum_ordered_window_controls_compose_and_restore(tmp_path, mode):
    from qualification_controls import QualificationControls
    from readmixer_py import stage_scheduler, scan
    cfg = fixtures.FixtureConfig(counts=(128, 128, 0), compression='plain', sequence_bytes=5, metadata_bytes=0)
    fixture = fixtures.generate_fixtures(tmp_path / 'fixture', cfg)
    scratch = tmp_path / 'scratch'
    scratch.mkdir()
    originals = (stage_scheduler.worker_main, stage_scheduler.StageScheduler.take_next, scan.PoolScanner.indexed, os.write)
    with QualificationControls(dict(fixed_test_context='matrix-v1', block_first_terminal=True)) as controls:
        with telemetry.ResourceTelemetry(scratch, tmp_path / 'pool', detail=False) as observed:
            report = controls.mix(MixConfig(input_dir=tmp_path / 'fixture' / 'input',
                output=tmp_path / 'pool', workers=4, max_ram=4 << 30,
                max_record_bytes=cfg.max_record_bytes, storage=mode, temp_dir=scratch))
    assert originals == (stage_scheduler.worker_main, stage_scheduler.StageScheduler.take_next, scan.PoolScanner.indexed, os.write)
    assert report.completed and report.workers_effective >= 3
    assert observed.result['complete'] and observed.result['workers']['complete']
    witnesses = controls.result['witnesses']
    assert witnesses['late_results_retained'] >= 2
    assert observed.result['peaks']['scheduler_completed_retained_jobs'] >= witnesses['late_results_retained']
    ready = observed.result['worker_admission']
    assert ready['ready_observations'] == observed.result['workers']['expected']
    assert ready['max_ready_threads'] == 1
    assert 0 < ready['max_ready_rss_bytes'] <= ready['max_reserved_baseline_bytes']
    assert ready['max_reserved_job_bytes'] > 0 and ready['max_reserved_channel_bytes'] > 0
    assert ready['max_job_capacity'] >= 4 and ready['max_process_slots'] >= 2
    assert observed.result['io']['original']['read_bytes'] == fixture['physical_bytes']
    if mode is StorageMode.MEMORY:
        assert observed.result['io']['encrypted_scratch']['write_bytes'] == 0
    assert not list(scratch.iterdir())
