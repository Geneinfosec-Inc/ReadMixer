"""Plan 12: bounded inventories and witnessed ownership handoff failures."""
import errno
import hashlib
import os

import pytest

from readmixer_py.crypto import OperationKeys, sort_identity
from readmixer_py.disk import DiskBudget, DiskRefund
from readmixer_py.errors import PublicationError, ResourceError
from readmixer_py.resources import Budget
from readmixer_py.storage import SegmentStore
from readmixer_py.transaction import OutputTransaction


def begin(root, mode='external', **kwargs):
    budget = Budget(128 << 20, descriptors=192)
    disk = DiskBudget(budget=budget)
    store = SegmentStore(mode, budget, OperationKeys.new(), temp_dir=root, disk_budget=disk)
    tx = OutputTransaction.begin(root, 'manifest', budget, disk_budget=disk, store=store, **kwargs)
    return tx, store, budget, disk


def registered(tx, name='data', data=b'payload'):
    artifact = tx.create_partial(name)
    artifact.write(data)
    artifact.close()
    tx.register_closed(artifact, len(data), hashlib.sha256(data).digest())
    return artifact


def finish(tx, disk, budget):
    tx.abort()
    disk.close()
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0


@pytest.mark.parametrize('mode', ['memory', 'auto', 'external'])
def test_P12_T01_T02_many_inventory_and_link_pages_with_bounded_handles(tmp_path, mode, monkeypatch):
    tx, store, budget, disk = begin(tmp_path, mode, max_artifacts=1001)
    for ordinal in range(1000):
        registered(tx, f'data.{ordinal:05}.' + 'x' * 100)
        assert len(tx._active) == 0
        assert not hasattr(tx, '_artifacts') and not hasattr(tx, '_names')
    assert tx._journal.inventory.root.page_count >= 3
    if mode == 'memory':
        assert store.scratch_directory is None
    else:
        protected = list(store.scratch_directory.iterdir())
        assert protected
        assert all(b'payload' not in path.read_bytes() and b'data.00000' not in path.read_bytes()
                   for path in protected)
    counts = []
    real_validate = tx._journal.validate_links
    def validate():
        real_validate()
        counts.append((tx._journal.inventory.root.page_count, tx._journal.links.root.page_count,
                       tx._journal.inventory_count, tx._journal.link_count))
    monkeypatch.setattr(tx._journal, 'validate_links', validate)
    with pytest.raises(ResourceError, match='inventory'):
        tx.create_partial('one-too-many')
    report = tx.publish_all(b'marker')
    assert report.cleanup_complete
    assert counts and min(counts[0][:2]) >= 3 and counts[0][2:] == (1000, 1000)
    assert len(list(tmp_path.iterdir())) == 1001
    assert disk.committed_bytes == 7006
    finish(tx, disk, budget)


def test_P12_T01_closed_unregistered_handles_remain_bounded(tmp_path):
    tx, store, budget, disk = begin(tmp_path)
    for ordinal in range(64):
        tx.create_partial(f'pending.{ordinal}').close()
    with pytest.raises(ResourceError, match='active output handle'):
        tx.create_partial('over')
    assert len(tx._active) == 64
    finish(tx, disk, budget)
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize('phase', ['before', 'after'])
@pytest.mark.parametrize('lane', ['inventory', 'links'])
def test_P12_T03_interrupted_journal_handoff_has_exactly_one_owner(tmp_path, monkeypatch, phase, lane):
    tx, store, budget, disk = begin(tmp_path)
    ledger = getattr(tx._journal, lane)
    real_commit = ledger._commit_owned_boundary
    reached = []
    def commit(boundary):
        reached.append(phase)
        if phase == 'after':
            real_commit(boundary)
        raise KeyboardInterrupt()
    monkeypatch.setattr(ledger, '_commit_owned_boundary', commit)
    if lane == 'inventory':
        with pytest.raises(KeyboardInterrupt):
            registered(tx)
        assert tx._journal.inventory_count == int(phase == 'after')
        tx.abort()
    else:
        registered(tx)
        with pytest.raises(KeyboardInterrupt):
            tx.publish_all(b'marker')
    assert reached == [phase]
    assert not list(tmp_path.iterdir())
    assert disk.committed_bytes == 0
    finish(tx, disk, budget)


def test_P12_T05_postcommit_store_failure_returns_warning_and_releases_handles(tmp_path, monkeypatch):
    tx, store, budget, disk = begin(tmp_path)
    registered(tx)
    reached = []
    def fail_close():
        reached.append(tx._state)
        raise OSError(errno.EIO, 'private details must not escape')
    monkeypatch.setattr(store, 'close', fail_close)
    report = tx.publish_all(b'marker')
    assert reached == ['committed']
    assert report.committed and not report.cleanup_complete
    assert len(report.warnings) <= 4
    assert 'private details' not in str(report)
    assert (tmp_path / 'manifest.json').read_bytes() == b'marker'
    assert (tmp_path / 'data').read_bytes() == b'payload'
    finish(tx, disk, budget)


def test_P12_T05_refund_commit_interrupted_return_does_not_double_debit(tmp_path, monkeypatch):
    tx, store, budget, disk = begin(tmp_path)
    registered(tx, 'one')
    registered(tx, 'two')
    real_commit = DiskRefund.commit
    reached = []
    def commit(token):
        real_commit(token)
        if not reached:
            reached.append(token.amount)
            raise KeyboardInterrupt()
    monkeypatch.setattr(DiskRefund, 'commit', commit)
    with pytest.raises(KeyboardInterrupt):
        tx.abort()
    assert reached == [7]
    assert disk.committed_bytes == 0
    assert not list(tmp_path.iterdir())
    finish(tx, disk, budget)


def test_P12_T06_data_segments_must_close_before_first_final_link(tmp_path):
    tx, store, budget, disk = begin(tmp_path)
    registered(tx)
    with store.new_segment(3, sort_identity('test', 0, 0)) as writer:
        writer.append(b'data')
        writer.seal()
    with pytest.raises(ResourceError, match='data-processing scratch'):
        tx.publish_all(b'marker')
    assert not list(tmp_path.iterdir())
    finish(tx, disk, budget)


def test_P12_T02_strict_memory_refusal_never_creates_scratch(tmp_path):
    tx, store, budget, disk = begin(tmp_path, 'memory')
    blocker = budget.acquire('test-pressure', budget.remaining_bytes - (1 << 20))
    try:
        for ordinal in range(100):
            registered(tx, f'data.{ordinal}')
        with pytest.raises(ResourceError):
            tx.publish_all(b'marker')
        assert store.scratch_directory is None
        assert not list(tmp_path.iterdir())
    finally:
        blocker.release()
        finish(tx, disk, budget)


def test_P12_T06_public_mix_verify_and_both_recoveries_cross_ownership_pages(tmp_path, monkeypatch):
    """An 80-pair public fixture forces one bundle/part per long pair."""
    import gzip
    from readmixer_py import mix, verify, recover
    from readmixer_py.config import MixConfig, VerifyConfig, RecoverConfig, RecoveryMode, StorageMode
    from readmixer_py.ownership import OutputOwnershipJournal
    inputs = tmp_path / 'inputs'
    inputs.mkdir()
    originals = {}
    for sample in ('alpha', 'beta'):
        for mate in (1, 2):
            path = inputs / f'{sample}_R{mate}.fastq'
            digest = hashlib.sha256()
            with path.open('wb') as stream:
                for ordinal in range(40):
                    record = (f'@{sample}.{ordinal}/{mate}\n'.encode() + b'A' * 250000 +
                              b'\n+opaque\n' + b'!' * 250000 + b'\n')
                    stream.write(record)
                    digest.update(record)
            originals[(sample, mate)] = digest.digest()
    pages = []
    real_validate = OutputOwnershipJournal.validate_links
    def validate(journal):
        real_validate(journal)
        pages.append((journal.inventory.root.page_count, journal.links.root.page_count))
    monkeypatch.setattr(OutputOwnershipJournal, 'validate_links', validate)
    options = dict(max_ram=512 << 20, workers=1, storage=StorageMode.EXTERNAL)
    report = mix(MixConfig(input_dir=inputs, output=tmp_path / 'pool',
                           max_artifact_bytes=524316, **options))
    assert report.bundles == 80 and report.cleanup_complete
    manifest = tmp_path / 'pool' / report.manifest
    assert verify(VerifyConfig(manifest=manifest, **options)).pairs == 80
    for mode in RecoveryMode:
        output = tmp_path / mode.value
        restored = recover(RecoverConfig(manifest=manifest, sample=('alpha', 'beta'),
            output=output, io=mode, **options))
        assert restored.parts == 80 and restored.cleanup_complete
        for sample in ('alpha', 'beta'):
            for mate in (1, 2):
                digest = hashlib.sha256()
                for part in restored.outputs:
                    if part['sample_index'] == ('alpha', 'beta').index(sample):
                        with gzip.open(output / part[f'r{mate}']['name'], 'rb') as stream:
                            while chunk := stream.read(65536):
                                digest.update(chunk)
                assert digest.digest() == originals[(sample, mate)]
    assert len(pages) == 3 and all(min(pair) >= 2 for pair in pages)
    assert not list(tmp_path.rglob('readmixer-scratch-*'))
    assert not list(tmp_path.rglob('rmpy-stage-*'))
