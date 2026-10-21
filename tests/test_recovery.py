"""Recovery proves selected normalized originals without retaining source inputs."""

from dataclasses import replace
import gzip
import json
from pathlib import Path
import shutil

import pytest

from readmixer_py.artifacts.manifest import encode_manifest, parse_manifest
from readmixer_py.config import MixConfig, RecoverConfig, RecoveryMode, StorageMode
from readmixer_py.crypto import OperationKeys
from readmixer_py.errors import ConfigurationError, IntegrityError, ResourceError
from readmixer_py.mixing import _mix
from readmixer_py.recovery import recover


def make_pool(root, *, counts=(3, 5, 0), metadata_padding=0, cap=5_000_000_000, record_limit=1 << 20):
    original = root / 'original'
    original.mkdir(mode=0o700)
    expected = {}
    for sample_index, count in enumerate(counts):
        sample_id = chr(65 + sample_index)
        expected[sample_id] = {}
        for role in (1, 2):
            ingress = bytearray()
            canonical = bytearray()
            for ordinal in range(count):
                header = f'@read{ordinal}/{role} description'.encode() + b'\xff' + b'h' * metadata_padding
                plus = b'+annotation\xfe' + b'p' * metadata_padding
                sequence = b'AC'
                quality = b'!\r' if ordinal == count - 1 else b'!?'
                fields = (header, sequence, plus, quality)
                canonical.extend(b'\n'.join(fields) + b'\n')
                if ordinal == count - 1:
                    ingress.extend(b'\r\n'.join(fields))  # Lone CR at final quality EOF is payload.
                else:
                    ingress.extend(b'\r\n'.join(fields) + b'\r\n')
            (original / f'{sample_id}_R{role}.fastq').write_bytes(ingress)
            expected[sample_id][f'r{role}'] = bytes(canonical)
    destination = root / 'pool'
    # Content fixtures use a deterministic serial setup; spawned recovery is
    # exercised explicitly by the compression-service case below.
    report = _mix(MixConfig(input_dir=original, output=destination, max_ram=1 << 30, workers=1,
                           storage=StorageMode.EXTERNAL, max_record_bytes=record_limit,
                           max_artifact_bytes=cap),
                  _keys=OperationKeys._from_test(b'K' * 32, b'R' * 16))
    manifest = destination / report.manifest
    return manifest, expected, original


def config(manifest, destination, samples=('A', 'B', 'C'), mode=RecoveryMode.INDEXED, workers=1, **kwargs):
    return RecoverConfig(manifest=manifest, output=destination, sample=samples, io=mode,
                         storage=StorageMode.EXTERNAL, max_ram=1 << 30, workers=workers, **kwargs)


def restored_files(destination, report):
    receipt = json.loads((destination / report.receipt).read_bytes())
    restored = {}
    for sample in receipt['samples']:
        restored[sample['sample_id']] = {}
        for role in ('r1', 'r2'):
            restored[sample['sample_id']][role] = b''.join(
                gzip.decompress((destination / part[role]['name']).read_bytes()) for part in sample['parts'])
    return receipt, restored


@pytest.mark.parametrize('mode', [RecoveryMode.INDEXED, RecoveryMode.SEQUENTIAL])
def test_union_recovery_preserves_opaque_normalized_bytes_and_empty_sample(tmp_path, mode):
    manifest, expected, original = make_pool(tmp_path)
    shutil.rmtree(original)
    destination = tmp_path / 'restored'
    report = recover(config(manifest, destination, samples=('C', 'B', 'A'), mode=mode))
    receipt, actual = restored_files(destination, report)
    assert actual == expected
    assert report.samples == 3 and report.pairs == 8 and report.reads == 16
    assert report.parts == 3 and report.artifacts == 13
    assert report.cleanup_complete and not report.warnings
    assert [sample['sample_id'] for sample in receipt['samples']] == ['A', 'B', 'C']
    assert receipt['source_manifest_sha256'] == report.source_manifest_sha256
    assert receipt['totals']['artifacts'] == len(list(destination.iterdir())) == 13
    assert report.validation_scope == ('selected_records' if mode is RecoveryMode.INDEXED else 'complete_bundle_streams')
    assert receipt['index_validation'] == ('validated' if mode is RecoveryMode.INDEXED else 'explicitly_bypassed')
    empty = receipt['samples'][-1]
    assert empty['pairs'] == 0 and len(empty['parts']) == 1
    for role in ('r1', 'r2'):
        assert empty['parts'][0][role]['bytes'] == 28
        assert empty['parts'][0][role + '_gzi']['bytes'] == 8
    for path in destination.iterdir():
        assert path.stat().st_mode & 0o777 == 0o600
        assert 'description' not in path.name and 'annotation' not in path.name
    assert all(not path.is_dir() for path in destination.iterdir())


def test_union_equals_single_sample_recoveries_and_repeated_ids_are_fresh(tmp_path):
    manifest, expected, _original = make_pool(tmp_path)
    batch_destination = tmp_path / 'batch'
    batch = recover(config(manifest, batch_destination))
    _, batch_data = restored_files(batch_destination, batch)
    single_destination = tmp_path / 'singles'
    receipts = []
    for sample_id in ('A', 'B', 'C', 'A'):
        report = recover(config(manifest, single_destination, samples=(sample_id,)))
        _, actual = restored_files(single_destination, report)
        assert actual[sample_id] == batch_data[sample_id] == expected[sample_id]
        receipts.append(report.receipt)
    assert len(set(receipts)) == 4


def test_sequential_does_not_access_absent_indices_but_indexed_requires_them(tmp_path, monkeypatch):
    manifest, expected, _original = make_pool(tmp_path)
    document = parse_manifest(manifest.read_bytes())
    names = {bundle['index']['name'] for bundle in document['bundles']}
    for name in names:
        (manifest.parent / name).unlink()
    indexed_destination = tmp_path / 'indexed'
    with pytest.raises(IntegrityError):
        recover(config(manifest, indexed_destination, samples=('A',)))
    assert not indexed_destination.exists()
    import readmixer_py.pool as pool_module
    real_stat = pool_module.os.stat

    def stat(path, *args, **kwargs):
        assert str(path) not in names, 'sequential recovery tried to stat an index'
        return real_stat(path, *args, **kwargs)

    monkeypatch.setattr(pool_module.os, 'stat', stat)
    destination = tmp_path / 'sequential'
    report = recover(config(manifest, destination, samples=('A',), mode=RecoveryMode.SEQUENTIAL))
    _, actual = restored_files(destination, report)
    assert actual == {'A': expected['A']}


@pytest.mark.parametrize('mode', [RecoveryMode.INDEXED, RecoveryMode.SEQUENTIAL])
def test_restored_metadata_uses_new_synchronized_parts_under_smaller_cap(tmp_path, mode):
    manifest, expected, _original = make_pool(tmp_path, counts=(2, 2), metadata_padding=20_000,
                                              cap=1_000_000, record_limit=131_072)
    document = parse_manifest(manifest.read_bytes())
    assert len(document['bundles']) == 1
    destination = tmp_path / 'small-parts'
    report = recover(config(manifest, destination, samples=('B', 'A'), mode=mode, max_artifact_bytes=65_564))
    receipt, actual = restored_files(destination, report)
    assert actual == expected
    assert report.parts == 4
    assert all(len(sample['parts']) == 2 for sample in receipt['samples'])
    assert all(path.stat().st_size <= 65_564 for path in destination.iterdir())
    assert all(part['r1']['records'] == part['r2']['records'] == part['pairs'] == 1
               for sample in receipt['samples'] for part in sample['parts'])


def test_oversized_restored_singleton_aborts_all_selected_outputs(tmp_path):
    manifest, _expected, _original = make_pool(tmp_path, counts=(1, 1), metadata_padding=35_000,
                                               cap=1_000_000, record_limit=131_072)
    destination = tmp_path / 'too-small'
    with pytest.raises(ResourceError, match='restored pair'):
        recover(config(manifest, destination, samples=('A', 'B'), max_artifact_bytes=65_564))
    assert not list(destination.iterdir())


def test_unknown_sample_and_raised_cap_fail_before_output_creation(tmp_path):
    manifest, _expected, _original = make_pool(tmp_path, cap=65_564)
    destination = tmp_path / 'uncreated'
    with pytest.raises(ConfigurationError, match='absent'):
        recover(config(manifest, destination, samples=('A', 'missing')))
    assert not destination.exists()
    with pytest.raises(ConfigurationError, match='cannot exceed'):
        recover(config(manifest, destination, samples=('A',), max_artifact_bytes=65_565))
    assert not destination.exists()
    with pytest.raises(ConfigurationError):
        config(manifest, destination, samples=('A', 'A'))


def test_selected_byte_limit_counts_selected_normalized_content_once(tmp_path):
    manifest, expected, _original = make_pool(tmp_path)
    limit = sum(map(len, expected['A'].values()))
    destination = tmp_path / 'exact'
    report = recover(config(manifest, destination, samples=('A',), max_total_uncompressed=limit))
    assert report.normalized_bytes == limit
    failed = tmp_path / 'one-under'
    with pytest.raises(ResourceError, match='max_total_uncompressed'):
        recover(config(manifest, failed, samples=('A',), max_total_uncompressed=limit - 1))
    assert not list(failed.iterdir())


def test_mismatched_selected_digest_prevents_batch_receipt(tmp_path):
    manifest, _expected, _original = make_pool(tmp_path)
    document = parse_manifest(manifest.read_bytes())
    document['samples'][1]['r1']['normalized_sha256'] = '00' * 32
    manifest.write_bytes(encode_manifest(document))
    destination = tmp_path / 'mismatch'
    with pytest.raises(IntegrityError, match='digest'):
        recover(config(manifest, destination, samples=('A', 'B')))
    assert not list(destination.iterdir())


def test_recovery_rejects_unadmittable_memory_before_publication(tmp_path):
    manifest, _expected, _original = make_pool(tmp_path)
    destination = tmp_path / 'low-memory'
    with pytest.raises(ResourceError):
        recover(replace(config(manifest, destination), max_ram=64 << 20))
    assert not destination.exists() or not list(destination.iterdir())


@pytest.mark.parametrize('mode', [RecoveryMode.INDEXED, RecoveryMode.SEQUENTIAL])
def test_many_source_bundles_are_gathered_in_one_union_pass(tmp_path, monkeypatch, mode):
    manifest, expected, _original = make_pool(tmp_path, counts=(10, 15, 0), metadata_padding=1800, cap=65_564)
    document = parse_manifest(manifest.read_bytes())
    assert len(document['bundles']) >= 5
    import readmixer_py.pool as pool_module
    real_open = pool_module.Pool.open
    opened = []

    def open_artifact(self, descriptor, **kwargs):
        opened.append(descriptor['name'])
        return real_open(self, descriptor, **kwargs)

    monkeypatch.setattr(pool_module.Pool, 'open', open_artifact)
    destination = tmp_path / 'union'
    report = recover(config(manifest, destination, mode=mode))
    _, actual = restored_files(destination, report)
    assert actual == expected
    for bundle in document['bundles']:
        for role in ('r1', 'r2', 'key', 'r1_gzi', 'r2_gzi'):
            assert opened.count(bundle[role]['name']) == 1
        assert opened.count(bundle['index']['name']) == int(mode is RecoveryMode.INDEXED)


def test_indexed_empty_selection_does_not_claim_unread_payload_validation(tmp_path):
    manifest, _expected, _original = make_pool(tmp_path)
    document = parse_manifest(manifest.read_bytes())
    payload = manifest.parent / document['bundles'][0]['r1']['name']
    changed = bytearray(payload.read_bytes())
    changed[20] ^= 0x80  # Same physical length and mode, corrupted DEFLATE payload.
    payload.write_bytes(changed)
    destination = tmp_path / 'empty-indexed'
    report = recover(config(manifest, destination, samples=('C',)))
    receipt, actual = restored_files(destination, report)
    assert actual == {'C': {'r1': b'', 'r2': b''}}
    assert report.validation_scope == receipt['validation_scope'] == 'selected_records'
    sequential = tmp_path / 'empty-sequential'
    with pytest.raises(IntegrityError):
        recover(config(manifest, sequential, samples=('C',), mode=RecoveryMode.SEQUENTIAL))
    assert not list(sequential.iterdir())


def test_late_required_index_digest_failure_invalidates_provisional_selected_bodies(tmp_path):
    manifest, _expected, _original = make_pool(tmp_path)
    document = parse_manifest(manifest.read_bytes())
    document['bundles'][-1]['index']['sha256'] = '00' * 32
    manifest.write_bytes(encode_manifest(document))
    destination = tmp_path / 'late-failure'
    with pytest.raises(IntegrityError, match='digest'):
        recover(config(manifest, destination, samples=('A',)))
    assert not list(destination.iterdir())


def test_shared_compression_service_preserves_bytes_and_closes_before_commit(tmp_path, monkeypatch):
    manifest, _expected, _original = make_pool(tmp_path, counts=(10, 15, 0), metadata_padding=1800, cap=65_564)
    import readmixer_py.recovery as recovery_module
    from readmixer_py.execution import CpuObservations, resolve_cpu_budget
    services = []
    real_service = recovery_module.CompressionService
    real_publish = recovery_module.OutputTransaction.publish_all

    def service(*args, **kwargs):
        instance = real_service(*args, **kwargs)
        services.append(instance)
        return instance

    def publish(transaction, data):
        assert services[-1]._closed and services[-1].pending_jobs == 0
        assert getattr(transaction.budget, '_stage_scheduler', None) is None
        return real_publish(transaction, data)

    monkeypatch.setattr(recovery_module, 'CompressionService', service)
    monkeypatch.setattr(recovery_module, 'resolve_cpu_budget',
                        lambda workers: resolve_cpu_budget(workers, observations=CpuObservations(4, 4)))
    monkeypatch.setattr(recovery_module.OutputTransaction, 'publish_all', publish)
    reports = []
    for workers in (1, 2):
        destination = tmp_path / f'workers{workers}'
        report = recover(replace(config(manifest, destination), workers=workers))
        receipt, _actual = restored_files(destination, report)
        reports.append((report, receipt, destination))
    assert len(services) == 2  # Exactly one invocation-wide service each time.
    assert reports[0][0].compression_slots == 0 and reports[0][0].cpu_budget == 1
    # Profile 1 partitions C=2 into coordinator + one process, with synchronous
    # compression. Both execution paths must preserve recovered artifact bytes.
    assert reports[1][0].compression_slots == 0 and reports[1][0].cpu_budget == 2
    assert reports[1][0].workers_effective == 2
    assert all(service._closed and service.pending_jobs == 0 for service in services)
    for left_sample, right_sample in zip(reports[0][1]['samples'], reports[1][1]['samples']):
        for left_part, right_part in zip(left_sample['parts'], right_sample['parts']):
            for role in ('r1', 'r2', 'r1_gzi', 'r2_gzi'):
                left = (reports[0][2] / left_part[role]['name']).read_bytes()
                right = (reports[1][2] / right_part[role]['name']).read_bytes()
                assert left == right


def test_source_descriptor_close_failure_remains_precommit(tmp_path, monkeypatch):
    manifest, _expected, _original = make_pool(tmp_path)
    import readmixer_py.recovery as recovery_module
    from readmixer_py.errors import CancelledError
    real_close = recovery_module.Pool.close_sources
    interrupted = [False]

    def close_sources(pool):
        real_close(pool)
        if not interrupted[0]:
            interrupted[0] = True
            raise CancelledError(signal=15)

    monkeypatch.setattr(recovery_module.Pool, 'close_sources', close_sources)
    destination = tmp_path / 'close-interrupted'
    with pytest.raises(CancelledError) as error:
        recover(config(manifest, destination))
    assert error.value.signal == 15
    assert not list(destination.iterdir())


def test_compression_worker_failure_joins_service_and_rolls_back(tmp_path, monkeypatch):
    manifest, _expected, _original = make_pool(tmp_path)
    import readmixer_py.recovery as recovery_module
    from readmixer_py.execution import CpuObservations, resolve_cpu_budget
    services = []
    real_service = recovery_module.CompressionService

    def service(*args, **kwargs):
        instance = real_service(*args, **kwargs)
        services.append(instance)
        return instance

    def fail_encode(*args, **kwargs):
        raise ResourceError('injected compression worker failure')

    monkeypatch.setattr(recovery_module, 'CompressionService', service)
    monkeypatch.setattr(recovery_module, 'resolve_cpu_budget',
                        lambda workers: resolve_cpu_budget(2, observations=CpuObservations(4, 4)))
    monkeypatch.setattr(recovery_module, 'encode_member', fail_encode)
    destination = tmp_path / 'compression-failed'
    with pytest.raises(ResourceError, match='compression worker'):
        recover(config(manifest, destination))
    assert len(services) == 1 and services[0]._closed
    assert services[0]._executor is None and services[0].pending_jobs == 0
    assert not list(destination.iterdir())


@pytest.mark.parametrize('second_fails', [False, True])
def test_emitter_close_attempts_all_writers_and_releases_metadata(second_fails):
    from types import SimpleNamespace
    from readmixer_py.recovery import _RecoveryEmitter
    from readmixer_py.resources import Budget

    budget = Budget(1 << 20)
    transaction = SimpleNamespace(budget=budget, max_artifact_bytes=5_000_000_000)
    emitter = _RecoveryEmitter(transaction, None, 'a' * 32, 'b' * 32, 8192, None)
    primary = ResourceError('injected first recovery writer close failure')
    secondary = ResourceError('injected second recovery writer close failure')
    calls = []

    class Writer:
        def __init__(self, name, failure):
            self.name, self.failure = name, failure

        def close(self, earlier):
            calls.append((self.name, earlier))
            if self.failure is not None:
                raise self.failure

    emitter._active = {'writers': {'r1': Writer('r1', primary),
                                   'r2': Writer('r2', secondary if second_fails else None)}}
    with pytest.raises(ResourceError) as error:
        emitter.close()
    assert error.value is primary
    assert calls == [('r1', None), ('r2', primary)]
    assert emitter._active is None
    assert budget.used_bytes == 0
    emitter.close()  # Cleanup remains idempotent after the first failure.
    assert len(calls) == 2
