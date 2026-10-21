"""Auto sort output must remain bounded before a dataset-sized merge seals."""
import errno

import pytest

from readmixer_py.crypto import OperationKeys, observations_identity
from readmixer_py.errors import ConfigurationError, ResourceError
from readmixer_py.resources import Budget
from readmixer_py.sorting import ExternalSorter
from readmixer_py.storage import BLOCK_BYTES, SegmentStore, deserialize_segment_ref


def _store(path, budget, mode='auto'):
    return SegmentStore(mode, budget, OperationKeys._from_test(bytes(32), bytes(16)), temp_dir=path)


def test_auto_merge_output_larger_than_whole_ram_budget_is_streamed(tmp_path):
    budget = Budget(96 << 20, 128)
    record_bytes, count = 1 << 16, 2048
    assert count * record_bytes > budget.limit_bytes
    with _store(tmp_path, budget) as store:
        with ExternalSorter(store, key=lambda row: row[:8], run_bytes=2 << 20,
                            max_record_bytes=record_bytes, fan_in=2) as sorter:
            for ordinal in reversed(range(count)):
                sorter.append(ordinal.to_bytes(8, 'big') + b'x' * (record_bytes - 8))
            assert sorter.run_count > sorter.fan_in
            observed = 0
            with sorter.iter_sorted() as records:
                for ordinal, row in enumerate(records):
                    assert row == ordinal.to_bytes(8, 'big') + b'x' * (record_bytes - 8)
                    observed += 1
            assert observed == count
            assert sorter.merge_passes >= 6
            with sorter._runs.open_reverse() as cursor:
                final = deserialize_segment_ref(next(cursor))
            assert final.decoded_bytes > budget.limit_bytes
            assert final.physical_bytes > 0
            assert not store._reader_pins
        assert store.resident_segments == 0
    assert (budget.used_bytes, budget.used_metadata_bytes, budget.used_descriptors) == (0, 0, 0)
    assert not list(tmp_path.iterdir())


def test_explicit_auto_disk_writer_does_not_change_other_writer_policy(tmp_path):
    budget = Budget(96 << 20, 128)
    payload = b'x' * (BLOCK_BYTES + 17)
    with _store(tmp_path, budget) as store:
        with store.new_segment(5, observations_identity('disk-first', 0), external=True) as disk, \
             store.new_segment(5, observations_identity('resident', 0)) as resident:
            disk.append(payload)
            resident.append(payload)
            assert store.mode == 'auto'
            assert disk._sink is not None and not disk.entry.blocks
            assert resident._sink is None and resident.entry.blocks
            disk_ref, resident_ref = disk.seal(), resident.seal()
        assert store.is_external(disk_ref)
        assert not store.is_external(resident_ref)
        for ref in (disk_ref, resident_ref):
            with store.open_verified(ref) as reader:
                assert reader.read() == payload
            store.drop(ref)
    assert (budget.used_bytes, budget.used_metadata_bytes, budget.used_descriptors) == (0, 0, 0)
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize('external,error', [(True, ResourceError), (1, ConfigurationError),
                                            (None, ConfigurationError), ('yes', ConfigurationError)])
def test_memory_rejects_explicit_disk_output_before_any_ownership_changes(tmp_path, external, error):
    budget = Budget(64 << 20, 128)
    with _store(tmp_path, budget, mode='memory') as store:
        before = (budget.used_bytes, budget.used_metadata_bytes, budget.used_descriptors, store._next_id)
        with pytest.raises(error):
            store.new_segment(5, observations_identity('refused', 0), external=external)
        assert before == (budget.used_bytes, budget.used_metadata_bytes, budget.used_descriptors, store._next_id)
        assert store.resident_segments == 0
        assert store.scratch_directory is None
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize('fault', ['enospc', 'short-write'])
def test_auto_merge_disk_failure_preserves_cause_and_cleans_owned_parents(tmp_path, monkeypatch, fault):
    import readmixer_py.storage as storage
    foreign = tmp_path / 'foreign'
    foreign.write_bytes(b'keep')
    budget = Budget(96 << 20, 128)
    state = {'merge': False, 'reached': False}
    original_merge, original_write = ExternalSorter._merge_group, storage.os.write

    def merge(self, refs, heads):
        state['merge'] = True
        return original_merge(self, refs, heads)

    def write(fd, data):
        if state['merge'] and not state['reached']:
            state['reached'] = True
            if fault == 'enospc':
                raise OSError(errno.ENOSPC, 'injected synthetic scratch exhaustion')
            return 0
        return original_write(fd, data)

    monkeypatch.setattr(ExternalSorter, '_merge_group', merge)
    monkeypatch.setattr(storage.os, 'write', write)
    with _store(tmp_path, budget) as store:
        with pytest.raises(ResourceError, match='encrypted scratch write failed') as failure:
            with ExternalSorter(store, key=lambda row: row, run_bytes=1024,
                                max_record_bytes=8, fan_in=2) as sorter:
                for ordinal in reversed(range(20)):
                    sorter.append(ordinal.to_bytes(8, 'big'))
                with sorter.iter_sorted() as records:
                    list(records)
        assert state['reached']
        assert isinstance(failure.value.__cause__, OSError)
        if fault == 'enospc':
            assert failure.value.__cause__.errno == errno.ENOSPC
        assert not store._reader_pins
        assert store.resident_segments == 0
    assert (budget.used_bytes, budget.used_metadata_bytes, budget.used_descriptors) == (0, 0, 0)
    assert list(tmp_path.iterdir()) == [foreign]
    assert foreign.read_bytes() == b'keep'
