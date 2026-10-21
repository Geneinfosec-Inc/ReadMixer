"""Recovery releases input ownership even when callers retain error tracebacks."""

import sys
import weakref

import pytest

from readmixer_py import mix, recover
from readmixer_py.config import MixConfig, RecoverConfig, RecoveryMode, StorageMode
from readmixer_py.errors import ResourceError
import readmixer_py.recovery as recovery_module
from readmixer_py.pool import _Input
from readmixer_py.sorting import ExternalSorter


@pytest.fixture
def lifetime_pool(tmp_path):
    inputs = tmp_path / 'inputs'
    inputs.mkdir()
    for sample in ('alpha', 'beta'):
        for role in (1, 2):
            records = b''.join(f'@{sample}-{ordinal}/{role}\nACGT\n+opaque\n!#~?\n'.encode()
                               for ordinal in range(3))
            (inputs / f'{sample}_R{role}.fastq').write_bytes(records)
    report = mix(MixConfig(input_dir=inputs, output=tmp_path / 'pool', workers=1,
                          storage=StorageMode.EXTERNAL, max_ram=512 << 20))
    return tmp_path / 'pool' / report.manifest


def _configuration(manifest, destination, mode, **kwargs):
    return RecoverConfig(manifest=manifest, output=destination, sample=('alpha', 'beta'),
                         io=mode, workers=1, storage=StorageMode.EXTERNAL,
                         max_ram=512 << 20, **kwargs)


def _observe_owners(monkeypatch):
    budgets, streams = [], []
    original_resources = recovery_module.operation_resources
    original_input = _Input.__init__

    def resources(*args, **kwargs):
        plan, budget = original_resources(*args, **kwargs)
        budgets.append(budget)
        return plan, budget

    def input_reader(self, *args, **kwargs):
        original_input(self, *args, **kwargs)
        streams.append(self.stream)

    monkeypatch.setattr(recovery_module, 'operation_resources', resources)
    monkeypatch.setattr(_Input, '__init__', input_reader)
    return budgets, streams


def _assert_released(budgets, streams):
    assert streams and all(stream.closed for stream in streams)
    assert len(budgets) == 1
    budget = budgets[0]
    assert budget.used_descriptors == budget.active_leases == 0
    assert budget.used_bytes == budget.baseline_bytes


@pytest.mark.parametrize('mode', list(RecoveryMode))
@pytest.mark.parametrize('failure', ['sorter', 'limit', 'sorter_and_cleanup'])
def test_failed_gather_closes_all_sources_and_leases_with_traceback_retained(
        lifetime_pool, tmp_path, monkeypatch, mode, failure):
    budgets, streams = _observe_owners(monkeypatch)
    primary = ResourceError('injected selected-sort admission failure')
    original_append = ExternalSorter.append

    def fail_append(self, record):
        if self.sort_purpose == 'recover-originals':
            raise primary
        return original_append(self, record)

    if failure != 'limit':
        monkeypatch.setattr(ExternalSorter, 'append', fail_append)
    if failure == 'sorter_and_cleanup':
        name = 'indexed' if mode is RecoveryMode.INDEXED else 'sequential'
        original_source = getattr(recovery_module.PoolScanner, name)

        class FailingClose:
            def __init__(self, source):
                self.source = source

            def __iter__(self):
                return self

            def __next__(self):
                return next(self.source)

            def close(self):
                self.source.close()
                raise ResourceError('injected later scanner-close failure')

        def source(self, *args, **kwargs):
            return FailingClose(original_source(self, *args, **kwargs))

        monkeypatch.setattr(recovery_module.PoolScanner, name, source)
    destination = tmp_path / 'failed'
    options = {'max_total_uncompressed': 1} if failure == 'limit' else {}
    with pytest.raises(ResourceError) as error:
        recover(_configuration(lifetime_pool, destination, mode, **options))
    # Keep the exception and its complete traceback live throughout these
    # assertions. Garbage collection must not be needed to close six inputs.
    assert error.value.__traceback__ is not None
    if failure == 'limit':
        assert 'max_total_uncompressed' in str(error.value)
    else:
        assert error.value is primary
    if failure == 'sorter_and_cleanup':
        assert any('source cleanup also failed' in note for note in error.value.__notes__)
    _assert_released(budgets, streams)
    assert not list(destination.iterdir())


def _reference_counts(frames):
    # CPython may borrow comprehension locals rather than owning them. Use the
    # same expression for production frames and independent registry-only bytes.
    return [sys.getrefcount(frame) for frame in frames]


def _assert_only_registry_owns(frames):
    controls = [bytes(bytearray([ordinal % 256]) * 32)
                for ordinal in range(len(frames))]
    assert len({id(frame) for frame in controls}) == len(controls)
    expected = _reference_counts(controls)
    observed = _reference_counts(frames)
    assert observed == expected
    return observed


def test_reference_count_oracle_detects_an_extra_owner():
    if sys.implementation.name != 'cpython':
        pytest.skip('direct reference ownership check requires CPython')
    frames = [bytes(bytearray([ordinal]) * 32) for ordinal in range(6)]
    baseline = _assert_only_registry_owns(frames)
    extra_owner = [frames[0]]
    with pytest.raises(AssertionError):
        _assert_only_registry_owns(frames)
    assert _reference_counts(frames) == [baseline[0] + 1, *baseline[1:]]
    extra_owner.clear()
    assert _assert_only_registry_owns(frames) == baseline


@pytest.mark.parametrize('mode', list(RecoveryMode))
def test_gathered_frames_have_no_production_owner_when_output_starts(
        lifetime_pool, tmp_path, monkeypatch, mode):
    # Exact bytes cannot be weak-referenced. CPython's direct reference count
    # proves that the test registry is their only owner after external sort
    # flushes. This checks actual object lifetime rather than local variable names.
    if sys.implementation.name != 'cpython':
        pytest.skip('direct reference ownership check requires CPython')
    original_append = ExternalSorter.append
    original_start = recovery_module._RecoveryEmitter.start_sample
    gathered = []
    observations = []

    def append(self, record):
        if self.sort_purpose == 'recover-originals':
            gathered.append(record)
        return original_append(self, record)

    def start(self, sample):
        assert len(gathered) == 6
        counts = _assert_only_registry_owns(gathered)
        observations.append(counts)
        return original_start(self, sample)

    monkeypatch.setattr(ExternalSorter, 'append', append)
    monkeypatch.setattr(recovery_module._RecoveryEmitter, 'start_sample', start)
    report = recover(_configuration(lifetime_pool, tmp_path / 'success', mode))
    assert report.completed and report.pairs == 6
    assert len(observations) == 2


def test_emitter_failure_releases_borrowed_view_before_returning_error(
        lifetime_pool, tmp_path, monkeypatch):
    budgets, streams = _observe_owners(monkeypatch)
    borrowed = []
    primary = ResourceError('injected recovered-output write failure')

    def fail_append(self, pair):
        borrowed.append(weakref.ref(pair.frame))
        del pair  # The injection itself must not pin the view in its traceback.
        raise primary

    monkeypatch.setattr(recovery_module._RecoveryEmitter, 'append', fail_append)
    destination = tmp_path / 'failed-output'
    with pytest.raises(ResourceError) as error:
        recover(_configuration(lifetime_pool, destination, RecoveryMode.INDEXED))
    assert error.value is primary and error.value.__traceback__ is not None
    assert borrowed and all(reference() is None for reference in borrowed)
    _assert_released(budgets, streams)
    assert not list(destination.iterdir())
