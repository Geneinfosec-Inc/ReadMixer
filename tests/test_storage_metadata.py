"""P11 shared metadata admission across protected storage and bounded sort owners."""

import pytest

from readmixer_py.crypto import OperationKeys, observations_identity
from readmixer_py.disk import DiskBudget
from readmixer_py.errors import ResourceError
from readmixer_py.ledger import PagedLedger
from readmixer_py.resources import Budget
from readmixer_py.shuffle import SerialShuffler
from readmixer_py.sorting import ExternalSorter, IdentityCoverage
from readmixer_py.storage import SegmentStore


def exhaust_metadata(budget):
    lease = budget.acquire('competing-owner', 1, metadata_bytes=1)
    amount = lease.bytes + budget.remaining_metadata_bytes
    lease.resize(amount, metadata_bytes=amount)
    assert budget.remaining_metadata_bytes == 0
    assert budget.remaining_bytes > 1 << 20
    return lease


def make_segment(store, payload=b'body'):
    with store.new_segment(5, observations_identity('metadata-test', 0)) as writer:
        writer.append(payload, item_count=1)
        return writer.seal()


def test_shared_metadata_rejects_segment_before_identity_or_registry_mutation():
    budget = Budget(64 << 20, metadata_limit_bytes=64 << 10)
    with SegmentStore('memory', budget, OperationKeys.new()) as store:
        initial = store._next_id
        with exhaust_metadata(budget):
            admitted = budget.used_metadata_bytes
            with pytest.raises(ResourceError, match='metadata'):
                store.new_segment(5, observations_identity('metadata-test', 0))
            assert store._next_id == initial
            assert store.resident_segments == 0
            assert budget.used_metadata_bytes == admitted
        ref = make_segment(store)
        assert store.resident_segments == 1
        store.drop(ref)
    assert budget.used_bytes == budget.used_metadata_bytes == 0


def test_reader_pin_admission_competes_with_other_metadata_owner():
    budget = Budget(64 << 20, metadata_limit_bytes=96 << 10)
    with SegmentStore('memory', budget, OperationKeys.new()) as store:
        ref = make_segment(store)
        entry = store._entries[ref.segment_id]
        with exhaust_metadata(budget):
            with pytest.raises(ResourceError, match='metadata'):
                store.open_verified(ref)
            assert entry.pinned == 0 and not store._reader_pins
        before = budget.used_metadata_bytes
        with store.open_verified(ref) as reader:
            assert entry.pinned == 1 and budget.used_metadata_bytes > before
            assert reader.read() == b'body'
        assert budget.used_metadata_bytes == before
        store.drop(ref)
        assert entry.blocks is entry.ref is entry.writer is None
    assert budget.used_bytes == budget.used_metadata_bytes == 0


def test_payload_blocks_do_not_consume_their_byte_length_as_metadata():
    budget = Budget(64 << 20, metadata_limit_bytes=64 << 10)
    with SegmentStore('memory', budget, OperationKeys.new()) as store:
        ref = make_segment(store, b'x' * (3 << 20))
        assert budget.used_metadata_bytes < 64 << 10
        assert budget.used_bytes > 3 << 20
        store.drop(ref)
    assert budget.used_metadata_bytes == 0


@pytest.mark.parametrize('mode', ['memory', 'external', 'auto'])
def test_registry_page_retention_and_authenticated_handoff_use_shared_metadata(tmp_path, mode):
    budget = Budget(128 << 20, metadata_limit_bytes=2 << 20)
    with SegmentStore(mode, budget, OperationKeys.new(), temp_dir=tmp_path) as store:
        with PagedLedger(store, page_bytes=8192, max_record_bytes=2048) as ledger:
            ledger.append(b'a' * 2000)
            ledger.flush()
            first = budget.used_metadata_bytes
            ledger.append(b'b' * 2000)
            ledger.flush()
            second = budget.used_metadata_bytes
            if mode == 'memory':
                assert second > first + 2000  # Complete retained encoded registry page.
            else:
                assert second == first  # Only trusted root/current-page reservations remain.
                assert store.resident_segments == 0
            with ledger.open_reverse() as records:
                assert list(records) == [b'b' * 2000, b'a' * 2000]
            assert budget.used_metadata_bytes == second
    assert budget.used_bytes == budget.used_metadata_bytes == 0
    assert not list(tmp_path.iterdir())


def test_failed_page_metadata_admission_retains_pending_bytes_and_original_root(tmp_path):
    budget = Budget(64 << 20, metadata_limit_bytes=2 << 20)
    with SegmentStore('external', budget, OperationKeys.new(), temp_dir=tmp_path) as store:
        with PagedLedger(store, page_bytes=8192, max_record_bytes=2048) as ledger:
            ledger.append(b'already sealed')
            ledger.flush()
            root = ledger.root
            ledger.append(b'pending ownership')
            pending = bytes(ledger._pending)
            with exhaust_metadata(budget):
                with pytest.raises(ResourceError, match='metadata'):
                    ledger.flush()
                assert ledger.root is root
                assert bytes(ledger._pending) == pending
                assert ledger._pending_count == 1
            ledger.flush()
            with ledger.open_reverse() as records:
                assert list(records) == [b'pending ownership', b'already sealed']
    assert budget.used_metadata_bytes == 0


@pytest.mark.parametrize('mode', ['memory', 'external', 'auto'])
def test_authenticated_ledger_cleanup_retains_metadata_overlap_under_pressure(tmp_path, mode):
    budget = Budget(64 << 20, metadata_limit_bytes=2 << 20)
    with SegmentStore(mode, budget, OperationKeys.new(), temp_dir=tmp_path) as store:
        ledger = PagedLedger(store, page_bytes=8192, max_record_bytes=2048)
        ledger.append(b'page')
        ledger.flush()
        with exhaust_metadata(budget):
            ledger.close()
            assert ledger._closed and not store._ledger_roots
    assert budget.used_bytes == budget.used_metadata_bytes == 0


def test_sort_row_and_coverage_metadata_remain_charged_until_disposal():
    budget = Budget(64 << 20, metadata_limit_bytes=2 << 20)
    with SegmentStore('memory', budget, OperationKeys.new()) as store:
        with ExternalSorter(store, key=lambda row: int.from_bytes(row, 'big'),
                            run_bytes=4096, max_record_bytes=8) as sorter:
            with exhaust_metadata(budget):
                with pytest.raises(ResourceError, match='metadata'):
                    sorter.append(b'\x02')
                assert sorter.records == 0 and not sorter._rows
                assert sorter._lease.metadata_bytes == 0
            sorter.append(b'\x02')
            sorter.append(b'\x01')
            assert sorter._lease.metadata_bytes > 0
            with sorter.iter_sorted() as rows:
                assert list(rows) == [b'\x01', b'\x02']
            assert sorter._lease.released and not sorter._rows
        before = budget.used_metadata_bytes
        coverage = IdentityCoverage(store, [1, 0, 1])
        assert budget.used_metadata_bytes > before
        coverage.add(0, 0)
        coverage.add(2, 0)
        coverage.finish()
        assert coverage.counts == (1, 0, 1)
        assert budget.used_metadata_bytes > before
        coverage.close()
        assert coverage.counts == () and coverage.offsets == []
        assert budget.used_metadata_bytes == before
    assert budget.used_metadata_bytes == 0


def test_shuffle_metadata_rejection_does_not_allocate_untracked_frontier():
    budget = Budget(256 << 20, metadata_limit_bytes=40 << 10)
    with SegmentStore('memory', budget, OperationKeys.new()) as store:
        before = budget.used_bytes, budget.used_metadata_bytes
        with pytest.raises(ResourceError, match='metadata'):
            SerialShuffler(store, terminal_bytes=64 << 20)
        assert (budget.used_bytes, budget.used_metadata_bytes) == before
        assert store.resident_segments == store._ledger_roots == 0
    assert budget.used_metadata_bytes == 0


def test_disk_registry_is_lazy_shared_and_disposed_only_at_owner_close(tmp_path):
    budget = Budget(64 << 20, metadata_limit_bytes=64 << 10)
    with DiskBudget(budget=budget) as disk:
        assert budget.used_metadata_bytes == 0
        with exhaust_metadata(budget):
            with pytest.raises(ResourceError, match='metadata'):
                disk.reserve(tmp_path, 100)
            assert disk._active == 0 and not disk._devices
        with disk.reserve(tmp_path, 100) as ticket:
            with pytest.raises(ResourceError, match='active'):
                disk.close()
            ticket.commit(40)
        assert budget.used_metadata_bytes > 0
        disk.release(tmp_path, 40)
        assert budget.used_metadata_bytes > 0  # Retained device/peak bookkeeping.
        assert disk.peak_bytes == 100
    assert budget.used_bytes == budget.used_metadata_bytes == 0
    assert disk.peak_bytes == 100 and disk.committed_bytes == 0
    assert not disk._devices


def test_disk_registry_allocation_failure_rolls_back_lazy_admission(tmp_path, monkeypatch):
    import readmixer_py.disk as module
    budget = Budget(64 << 20)
    with DiskBudget(budget=budget) as disk:
        def fail(*args):
            raise MemoryError('injected ticket allocation')
        monkeypatch.setattr(module, 'DiskReservation', fail)
        with pytest.raises(MemoryError):
            disk.reserve(tmp_path, 10)
        assert not disk._devices and disk._active == 0
        assert budget.used_bytes == budget.used_metadata_bytes == 0


def test_reader_pin_allocation_failure_rolls_back_counts_and_metadata():
    class FailAfterInsertion(dict):
        fail = True

        def __setitem__(self, key, value):
            super().__setitem__(key, value)
            if self.fail:
                self.fail = False
                raise MemoryError('injected pin insertion')

    budget = Budget(64 << 20)
    with SegmentStore('memory', budget, OperationKeys.new()) as store:
        ref = make_segment(store)
        with store.open_verified(ref):
            before = budget.used_bytes, budget.used_metadata_bytes
            entry = store._entries[ref.segment_id]
            store._reader_pins = FailAfterInsertion(store._reader_pins)
            with pytest.raises(MemoryError):
                store.open_verified(ref)
            assert store._reader_pins[ref.segment_id] == entry.pinned == 1
            assert (budget.used_bytes, budget.used_metadata_bytes) == before
        store.drop(ref)
    assert budget.used_bytes == budget.used_metadata_bytes == 0


def test_retained_page_failure_traceback_does_not_retain_decoded_metadata():
    from dataclasses import replace
    from readmixer_py.errors import IntegrityError

    budget = Budget(64 << 20)
    with SegmentStore('memory', budget, OperationKeys.new()) as store:
        with PagedLedger(store) as ledger:
            ledger.append(b'opaque metadata' * 1000)
            ledger.flush()
            good = ledger.root
            ledger.root = replace(good, aggregate_digest=bytes(32))
            try:
                with pytest.raises(IntegrityError) as caught:
                    with ledger.open_reverse():
                        pass
            finally:
                ledger.root = good
        traceback = caught.value.__traceback__
        checked = False
        while traceback is not None:
            if traceback.tb_frame.f_code.co_name == '_page':
                assert traceback.tb_frame.f_locals['data'] is None
                assert traceback.tb_frame.f_locals['ranges'] is None
                checked = True
            traceback = traceback.tb_next
        assert checked
    assert budget.used_bytes == budget.used_metadata_bytes == 0
    assert caught.value.__traceback__ is not None


def test_disk_context_keeps_primary_error_and_pending_metadata_owner(tmp_path):
    budget = Budget(64 << 20)
    disk = DiskBudget(budget=budget)
    with pytest.raises(RuntimeError, match='primary') as caught:
        with disk:
            ticket = disk.reserve(tmp_path, 10)
            raise RuntimeError('primary')
    assert any('disk bookkeeping' in note for note in caught.value.__notes__)
    assert budget.used_metadata_bytes > 0 and disk._active == 1
    ticket.release()
    disk.close()
    assert budget.used_bytes == budget.used_metadata_bytes == 0


def test_segment_registry_insertion_failure_retires_transferred_buffer():
    class FailAfterInsertion(dict):
        def __setitem__(self, key, value):
            super().__setitem__(key, value)
            raise MemoryError('injected segment insertion')

    budget = Budget(64 << 20)
    with SegmentStore('memory', budget, OperationKeys.new()) as store:
        before = budget.used_bytes, budget.used_metadata_bytes
        supplied = budget.acquire('supplied-buffer', (1 << 20) + 256)
        store._entries = FailAfterInsertion()
        with pytest.raises(MemoryError):
            store.new_segment(5, observations_identity('metadata-test', 0), supplied)
        assert supplied.released
        assert store.resident_segments == 0
        assert (budget.used_bytes, budget.used_metadata_bytes) == before
    assert budget.used_bytes == budget.used_metadata_bytes == 0


def test_segment_allocation_cleanup_failure_preserves_primary_and_retry_owner(monkeypatch):
    import readmixer_py.storage as module

    class FailBeforeInsertion(dict):
        def __setitem__(self, key, value):
            raise MemoryError('primary insertion failure')

    original = module.SegmentWriter.abort
    failed = [False]

    def abort_once(writer):
        if not failed[0]:
            failed[0] = True
            raise ResourceError('secondary cleanup failure')
        return original(writer)

    budget = Budget(64 << 20)
    with SegmentStore('memory', budget, OperationKeys.new()) as store:
        supplied = budget.acquire('supplied-buffer', (1 << 20) + 256)
        store._entries = FailBeforeInsertion()
        monkeypatch.setattr(module.SegmentWriter, 'abort', abort_once)
        with pytest.raises(MemoryError, match='primary') as caught:
            store.new_segment(5, observations_identity('metadata-test', 0), supplied)
        assert any('allocation cleanup' in note for note in caught.value.__notes__)
        assert store._pending_allocation is not None and budget.used_metadata_bytes > 0
        with pytest.raises(ResourceError, match='incomplete allocation'):
            store.new_segment(5, observations_identity('metadata-test', 1))
    assert budget.used_bytes == budget.used_metadata_bytes == 0
