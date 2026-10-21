"""Shared metadata admission across real operations and retained object owners."""

import os
from pathlib import Path
import subprocess
import sys

import pytest
import readmixer_py

from readmixer_py import mix, recover, verify
from readmixer_py.artifacts.bundles import BundleEmitter
from readmixer_py.config import MixConfig, RecoverConfig, RecoveryMode, StorageMode, VerifyConfig
from readmixer_py.errors import ResourceError
from readmixer_py.metadata import serialization_lease
from readmixer_py.pool import Pool
from readmixer_py.resources import Budget
from readmixer_py.transaction import OutputTransaction
import readmixer_py.mixing as mixing
import readmixer_py.recovery as recovery
import readmixer_py.verification as verification


def inputs_at(tmp_path):
    inputs = tmp_path / 'inputs'
    inputs.mkdir()
    for sample in ('alpha', 'beta'):
        for role in (1, 2):
            (inputs / f'{sample}_R{role}.fastq').write_bytes(
                b''.join(f'@{sample}-{n}/{role}\nACGT\n+opaque\n!#~?\n'.encode() for n in range(3)))
    return inputs


def observe_budgets(monkeypatch, *, ceiling=None):
    budgets = []
    for module in (mixing, verification, recovery):
        original = module.operation_resources
        def resources(*args, _original=original, **kwargs):
            plan, budget = _original(*args, **kwargs)
            if ceiling is not None:
                budget = Budget(plan.effective_max_ram, descriptors=plan.descriptor_allowance,
                                baseline_bytes=plan.coordinator_rss, metadata_limit_bytes=ceiling)
            budgets.append(budget)
            return plan, budget
        monkeypatch.setattr(module, 'operation_resources', resources)
    return budgets


@pytest.mark.parametrize('mode', list(StorageMode))
@pytest.mark.parametrize('ram', [256 << 20, 512 << 20])
def test_shared_metadata_complete_workflows_and_returned_report_ownership(tmp_path, mode, ram):
    # Resource boundaries need a fresh interpreter: earlier native-array tests
    # can legitimately leave allocator arenas resident in the pytest process.
    # Use the exact package already selected by source/installed-wheel testing.
    env = os.environ.copy()
    env['PYTHONPATH'] = str(Path(readmixer_py.__file__).resolve().parent.parent)
    result = subprocess.run([sys.executable, str(Path(__file__).resolve()),
                             str(tmp_path), mode.value, str(ram)], env=env,
                            capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr[-8000:]


def _complete_workflows(tmp_path, monkeypatch, mode, ram):
    budgets = observe_budgets(monkeypatch)
    sources = inputs_at(tmp_path)
    result = mix(MixConfig(input_dir=sources, output=tmp_path / 'pool', max_ram=ram,
                           workers=1, storage=mode))
    manifest = tmp_path / 'pool' / result.manifest
    checked = verify(VerifyConfig(manifest=manifest, max_ram=ram, workers=1, storage=mode))
    reports = []
    for io in RecoveryMode:
        reports.append(recover(RecoverConfig(manifest=manifest, output=tmp_path / io.value,
            sample=('alpha', 'beta'), max_ram=ram, workers=1, storage=mode, io=io)))
    assert result.pairs == checked.pairs == 6
    assert all(report.completed and report.pairs == 6 and len(report.outputs) == 2 for report in reports)
    # Returned report graphs remain usable after invocation-owned accounting ends.
    assert all(report.outputs[0]['r1']['bytes'] > 0 for report in reports)
    assert len(budgets) == 4
    for budget in budgets:
        assert 0 < budget.peak_metadata_bytes <= budget.metadata_limit_bytes
        assert budget.used_metadata_bytes == budget.used_descriptors == budget.active_leases == 0
        assert budget.used_bytes == budget.baseline_bytes


def test_bundle_descriptors_remain_charged_after_codec_close_until_publication(tmp_path, monkeypatch):
    sources = inputs_at(tmp_path)
    observations = []
    old_close, old_publish = BundleEmitter.close, OutputTransaction.publish_all
    def close(self):
        old_close(self)
        if self.bundles:
            assert not self._metadata_lease.released
            assert self._metadata_lease.metadata_bytes >= 8192 * len(self.bundles)
            observations.append('closed-codecs')
    def publish(self, data):
        owners = {lease.owner for lease in self.budget._state.leases}
        assert {'mix-source-metadata', 'mix-bundle-descriptors', 'metadata-serialization'} <= owners
        observations.append('publishing')
        return old_publish(self, data)
    monkeypatch.setattr(BundleEmitter, 'close', close)
    monkeypatch.setattr(OutputTransaction, 'publish_all', publish)
    mix(MixConfig(input_dir=sources, output=tmp_path / 'pool', workers=1,
                  storage=StorageMode.EXTERNAL, max_ram=512 << 20))
    assert observations.index('closed-codecs') < observations.index('publishing')


def test_shared_metadata_refusal_leaves_no_marker_or_live_owners(tmp_path, monkeypatch):
    sources = inputs_at(tmp_path)
    budgets = observe_budgets(monkeypatch, ceiling=65536)
    with pytest.raises(ResourceError) as error:
        mix(MixConfig(input_dir=sources, output=tmp_path / 'pool', workers=1,
                      storage=StorageMode.EXTERNAL, max_ram=512 << 20))
    assert 'metadata' in str(error.value)
    # Keep the traceback alive; no catalog/source/storage graph may pin a lease.
    assert error.value.__traceback__ is not None
    assert len(budgets) == 1
    assert budgets[0].used_metadata_bytes == budgets[0].active_leases == 0
    assert not (tmp_path / 'pool').exists() or not list((tmp_path / 'pool').iterdir())
    assert sorted(path.name for path in sources.iterdir()) == [
        'alpha_R1.fastq', 'alpha_R2.fastq', 'beta_R1.fastq', 'beta_R2.fastq']


def test_pool_close_sources_retains_manifest_and_identity_metadata(tmp_path):
    sources = inputs_at(tmp_path)
    result = mix(MixConfig(input_dir=sources, output=tmp_path / 'pool', workers=1,
                          max_ram=512 << 20, storage=StorageMode.EXTERNAL))
    budget = Budget(64 << 20)
    pool = Pool(tmp_path / 'pool' / result.manifest, budget, metadata_bytes=1 << 20)
    pool.preflight()
    metadata = budget.used_metadata_bytes
    pool.close_sources()
    assert pool.manifest['totals']['pairs'] == 6 and pool._files
    assert budget.used_metadata_bytes == metadata and budget.used_descriptors == 0
    alias = pool.manifest
    pool.close()
    assert not alias and not pool._files
    assert budget.used_metadata_bytes == budget.used_bytes == budget.active_leases == 0


def test_json_serialization_overlap_competes_with_retained_metadata():
    document = {'samples': [{'name': 'artificial', 'count': 17}]}
    budget = Budget(8 << 20)
    with budget.acquire('retained-document', 4096, metadata_bytes=4096):
        prior = budget.used_metadata_bytes
        with serialization_lease(budget, document, max_bytes=4096):
            peak = budget.used_metadata_bytes
            assert peak > prior + 65536
        assert budget.used_metadata_bytes == prior
    # A competing owner consumes precisely the remaining byte needed by the
    # preflight/encoding overlap; the serializer must fail before yielding.
    with budget.acquire('competing-owner', budget.metadata_limit_bytes - peak + 1,
                        metadata_bytes=budget.metadata_limit_bytes - peak + 1), \
         budget.acquire('retained-document', 4096, metadata_bytes=4096):
        before = budget.used_metadata_bytes
        with pytest.raises(ResourceError, match='metadata'):
            with serialization_lease(budget, document, max_bytes=4096):
                pytest.fail('serialization admitted beyond the shared ceiling')
        assert budget.used_metadata_bytes == before
    assert budget.used_bytes == budget.used_metadata_bytes == 0


def test_pool_parser_is_not_entered_without_shared_overlap_admission(tmp_path, monkeypatch):
    import readmixer_py.pool as pool_module
    sources = inputs_at(tmp_path)
    result = mix(MixConfig(input_dir=sources, output=tmp_path / 'pool', workers=1,
                          max_ram=512 << 20, storage=StorageMode.EXTERNAL))
    entered = []
    original = pool_module.parse_manifest
    def parse(*args, **kwargs):
        entered.append(True)
        return original(*args, **kwargs)
    monkeypatch.setattr(pool_module, 'parse_manifest', parse)
    budget = Budget(64 << 20, metadata_limit_bytes=16384)
    with pytest.raises(ResourceError) as error:
        Pool(tmp_path / 'pool' / result.manifest, budget, metadata_bytes=1 << 20)
    assert not entered
    assert error.value.__traceback__ is not None
    assert budget.used_bytes == budget.used_metadata_bytes == budget.active_leases == 0


def test_recovery_report_descriptors_stay_charged_through_final_cleanup(tmp_path, monkeypatch):
    from readmixer_py.disk import DiskBudget
    sources = inputs_at(tmp_path)
    mixed = mix(MixConfig(input_dir=sources, output=tmp_path / 'pool', workers=1,
                          max_ram=512 << 20, storage=StorageMode.EXTERNAL))
    budgets = observe_budgets(monkeypatch)
    observed = []
    original_close = DiskBudget.close
    def close(self):
        leases = [lease for lease in self.budget._leases if lease.owner == 'recovery-output-metadata']
        assert len(leases) == 1 and leases[0].metadata_bytes >= 2 * 8192
        observed.append(leases[0])
        return original_close(self)
    monkeypatch.setattr(DiskBudget, 'close', close)
    report = recover(RecoverConfig(manifest=tmp_path / 'pool' / mixed.manifest,
        output=tmp_path / 'restored', sample=('alpha', 'beta'), workers=1,
        max_ram=512 << 20, storage=StorageMode.EXTERNAL))
    assert observed and report.outputs[0]['r1']['bytes'] > 0
    assert all(lease.released for lease in observed)
    assert budgets[0].used_metadata_bytes == budgets[0].active_leases == 0


if __name__ == '__main__':
    with pytest.MonkeyPatch.context() as patch:
        _complete_workflows(Path(sys.argv[1]), patch, StorageMode(sys.argv[2]), int(sys.argv[3]))
