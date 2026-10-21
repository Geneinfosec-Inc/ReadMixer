"""Self-consistent artifact mutations must still fail independent proofs."""

import pytest

from readmixer_py.config import MixConfig, StorageMode, VerifyConfig
from readmixer_py.errors import InputError, IntegrityError
from readmixer_py.verification import verify
from test_pipeline import fixtures, mix_fixture


def test_P07_005_independent_verifier_rejects_producer_early_rollover(tmp_path, monkeypatch):
    from readmixer_py.artifacts.bundles import BundleEmitter
    fit = BundleEmitter._fits
    monkeypatch.setattr(BundleEmitter, '_fits', lambda self, *args: self._active['pairs'] < 2 and fit(self, *args))
    _, _, produced, manifest = mix_fixture(tmp_path)
    assert produced.bundles == 7
    with pytest.raises(IntegrityError, match='earlier than canonical'):
        verify(VerifyConfig(manifest=manifest, max_ram=512 << 20))


def test_P04_parse_failure_after_protected_writes_removes_output_and_scratch(tmp_path, monkeypatch):
    import readmixer_py.mixing as mixing
    from readmixer_py.shuffle import SerialShuffler
    from readmixer_py.storage import SegmentWriter
    inputs = tmp_path / 'inputs'
    fixtures(inputs)
    target = inputs / 'alpha_R2.fq'
    target.write_bytes(target.read_bytes() + b'\n@unfinished\n')
    sealed = []
    original_seal = SegmentWriter.seal
    def record_seal(self):
        result = original_seal(self)
        sealed.append(result.purpose)
        return result
    monkeypatch.setattr(SegmentWriter, 'seal', record_seal)
    monkeypatch.setattr(mixing, 'SerialShuffler', lambda *args, **kwargs: SerialShuffler(*args, **dict(kwargs, sort_run_bytes=4096)))
    with pytest.raises(InputError):
        mixing._mix(MixConfig(input_dir=inputs, output=tmp_path / 'pool', workers=1, storage=StorageMode.EXTERNAL, max_ram=512 << 20, max_record_bytes=1024), _scatter_strategy='sorter')
    assert 3 in sealed
    assert not list((tmp_path / 'pool').iterdir())


def test_P07_borrowed_original_views_released_before_tag_sort(tmp_path, monkeypatch):
    import weakref
    from contextlib import contextmanager
    import readmixer_py.verification as verification
    from readmixer_py.sorting import ExternalSorter

    _, _, _, manifest = mix_fixture(tmp_path)
    original_decode = verification.decode_pair_view
    original_sorted = ExternalSorter.iter_sorted
    views = []
    tag_passes = []

    def observe_decode(*args, **kwargs):
        pair = original_decode(*args, **kwargs)
        views.extend(weakref.ref(view) for record in (pair.r1, pair.r2) for view in record.lines)
        views.append(weakref.ref(pair.frame))
        return pair

    @contextmanager
    def observe_sort(self):
        if self.sort_purpose == 'verify-tags':
            assert views and all(reference() is None for reference in views)
            tag_passes.append(True)
        with original_sorted(self) as records:
            yield records

    monkeypatch.setattr(verification, 'decode_pair_view', observe_decode)
    monkeypatch.setattr(ExternalSorter, 'iter_sorted', observe_sort)
    assert verify(VerifyConfig(manifest=manifest, max_ram=512 << 20)).completed
    assert tag_passes == [True]
    assert all(reference() is None for reference in views)
