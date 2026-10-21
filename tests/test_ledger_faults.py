"""Allocation and mutation faults must preserve the trusted ownership chain."""

from dataclasses import replace
import weakref

import pytest

from readmixer_py import ledger as ledger_module
from readmixer_py.crypto import OperationKeys
from readmixer_py.errors import IntegrityError, ResourceError
from readmixer_py.ledger import PagedLedger
from readmixer_py.resources import Budget
from readmixer_py.storage import SegmentStore


class _FailClearOnce(bytearray):
    failed = False

    def clear(self):
        if not self.failed:
            self.failed = True
            raise MemoryError("injected pending-buffer clear failure")
        super().clear()


class _FailPartialExtend(bytearray):
    def extend(self, data):
        super().extend(data[:4])
        raise MemoryError("injected record-data append failure after its length prefix")


def test_flush_handoff_never_drops_a_page_owned_by_the_new_root(tmp_path):
    budget = Budget(64 << 20)
    with SegmentStore("external", budget, OperationKeys.new(), temp_dir=tmp_path) as store:
        with PagedLedger(store) as ledger:
            ledger.append(b"one entry")
            ledger._pending = _FailClearOnce(ledger._pending)
            try:
                ledger.flush()
            except MemoryError:
                pass
            # Either the implementation avoids fallible clear after handoff, or
            # it preserves a retryable state. Retrying must never duplicate data.
            ledger.flush()
            assert ledger.root.record_count == 1
            with ledger.open_reverse() as cursor:
                assert list(cursor) == [b"one entry"]
    assert budget.used_bytes == 0
    assert not list(tmp_path.iterdir())


def test_cleanup_allocates_successor_root_before_deleting_the_page(tmp_path, monkeypatch):
    budget = Budget(64 << 20)
    with SegmentStore("external", budget, OperationKeys.new(), temp_dir=tmp_path) as store:
        with PagedLedger(store) as ledger:
            ledger.append(b"old")
            ledger.flush()
            ledger.append(b"new")
            ledger.flush()
            root = ledger.root
            real_root = ledger_module.LedgerRoot

            def fail_allocation(*args, **kwargs):
                raise MemoryError("injected successor root allocation failure")

            monkeypatch.setattr(ledger_module, "LedgerRoot", fail_allocation)
            with pytest.raises(MemoryError):
                ledger.close()
            monkeypatch.setattr(ledger_module, "LedgerRoot", real_root)
            assert ledger.root == root
            with pytest.raises(ResourceError, match="cleaning"):
                ledger.append(b"late")
            with pytest.raises(ResourceError, match="cleaning"):
                ledger.flush()
            with ledger.open_reverse(flush=False, check_cancel=False) as cursor:
                assert list(cursor) == [b"new", b"old"]
    assert budget.used_bytes == 0
    assert not list(tmp_path.iterdir())


def test_failed_append_cannot_leave_an_unaccounted_length_prefix(tmp_path):
    with SegmentStore("memory", Budget(64 << 20), OperationKeys.new()) as store:
        with PagedLedger(store) as ledger:
            ledger.append(b"accepted")
            prior = bytes(ledger._pending)
            prior_count, prior_bytes = ledger._pending_count, ledger._pending_bytes
            ledger._pending = _FailPartialExtend(ledger._pending)
            with pytest.raises(MemoryError):
                ledger.append(b"rejected")
            assert bytes(ledger._pending) == prior
            assert (ledger._pending_count, ledger._pending_bytes) == (prior_count, prior_bytes)
            ledger.flush()
            with ledger.open_reverse() as cursor:
                assert list(cursor) == [b"accepted"]


def test_cleanup_releases_previous_page_ranges_before_decoding_next_page(tmp_path, monkeypatch):
    """Dense short records must not retain two unadmitted Python range tables."""
    class TrackedRanges(list):
        pass

    with SegmentStore("external", Budget(64 << 20), OperationKeys.new(), temp_dir=tmp_path) as store:
        with PagedLedger(store, page_bytes=8192, max_record_bytes=1) as ledger:
            for _ in range(2400):
                ledger.append(b"")
            ledger.flush()
            assert ledger.root.page_count >= 3
            real_page = ledger._page
            last = [None]
            decoded_pages = [0]

            def tracked_page(*args, **kwargs):
                assert last[0] is None or last[0]() is None, "old range table still retained during next page allocation"
                data, ranges, previous, previous_digest, total = real_page(*args, **kwargs)
                tracked = TrackedRanges(ranges)
                last[0] = weakref.ref(tracked)
                decoded_pages[0] += 1
                return data, tracked, previous, previous_digest, total

            monkeypatch.setattr(ledger, "_page", tracked_page)
            ledger.close()
            assert decoded_pages[0] >= 3


def test_drop_failure_keeps_remaining_chain_retryable(tmp_path, monkeypatch):
    with SegmentStore("external", Budget(64 << 20), OperationKeys.new(), temp_dir=tmp_path) as store:
        with PagedLedger(store) as ledger:
            ledger.append(b"old")
            ledger.flush()
            ledger.append(b"new")
            ledger.flush()
            real_drop = store.drop
            calls = [0]

            def drop(ref, **kwargs):
                calls[0] += 1
                if calls[0] == 2:
                    raise ResourceError("injected pre-deletion failure")
                return real_drop(ref, **kwargs)

            monkeypatch.setattr(store, "drop", drop)
            with pytest.raises(ResourceError):
                ledger.close()
            monkeypatch.setattr(store, "drop", real_drop)
            assert ledger.root.page_count == ledger.root.record_count == 1
            with pytest.raises(ResourceError, match="cleaning"):
                ledger.append(b"late")
            with pytest.raises(ResourceError, match="cleaning"):
                ledger.flush()
            with ledger.open_reverse(flush=False, check_cancel=False) as cursor:
                assert list(cursor) == [b"old"]
    assert not list(tmp_path.iterdir())


def test_interrupt_after_page_deletion_keeps_bounded_successor_ownership(tmp_path, monkeypatch):
    budget = Budget(64 << 20)
    with SegmentStore("external", budget, OperationKeys.new(), temp_dir=tmp_path) as store:
        with PagedLedger(store) as ledger:
            ledger.append(b"old")
            ledger.flush()
            ledger.append(b"new")
            ledger.flush()
            real_drop = store.drop
            interrupted = [False]

            def drop(ref, **kwargs):
                real_drop(ref, **kwargs)
                if not interrupted[0]:
                    interrupted[0] = True
                    raise KeyboardInterrupt()

            monkeypatch.setattr(store, "drop", drop)
            with pytest.raises(KeyboardInterrupt):
                ledger.close()
            monkeypatch.setattr(store, "drop", real_drop)
            # The completed deletion may be retried, but must not require
            # rereading the now-absent page to rediscover the previous reference.
            ledger.close()
    assert budget.used_bytes == 0
    assert not list(tmp_path.iterdir())


def test_wrong_backward_aggregate_fails_before_any_page_is_dropped(tmp_path, monkeypatch):
    with SegmentStore("memory", Budget(64 << 20), OperationKeys.new()) as store:
        with PagedLedger(store) as ledger:
            ledger.append(b"entry")
            ledger.flush()
            good_root = ledger.root
            calls = []
            real_drop = store.drop

            def drop(ref, **kwargs):
                calls.append(ref)
                return real_drop(ref, **kwargs)

            monkeypatch.setattr(store, "drop", drop)
            ledger.root = replace(good_root, aggregate_digest=b"\0" * 32)
            try:
                with pytest.raises(IntegrityError):
                    ledger.close()
                assert not calls
            finally:
                ledger.root = good_root


def test_ledger_rejects_budget_that_cannot_admit_authenticated_cleanup():
    budget = Budget(4 << 20)
    with SegmentStore("memory", budget, OperationKeys.new()) as store:
        baseline = budget.used_bytes
        with pytest.raises(ResourceError):
            PagedLedger(store)
        assert store._ledger_roots == 0
        assert budget.used_bytes == baseline
        assert budget.used_descriptors == 0


@pytest.mark.parametrize("mode", ["memory", "external", "auto"])
def test_canceled_cleanup_has_reserved_reader_capacity_when_budget_is_saturated(tmp_path, mode):
    budget = Budget(64 << 20, descriptors=16)
    cancelled = [False]
    with SegmentStore(mode, budget, OperationKeys.new(), temp_dir=tmp_path,
                      cancel=lambda: cancelled[0]) as store:
        with PagedLedger(store, cancel=lambda: cancelled[0]) as ledger:
            ledger.append(b"older page")
            ledger.flush()
            ledger.append(b"newer page")
            ledger.flush()
            remaining_fds = budget.descriptor_limit - budget.used_descriptors
            occupied = budget.acquire("other-stage", 1, descriptors=remaining_fds)
            occupied.resize(occupied.bytes + budget.remaining_bytes, descriptors=remaining_fds)
            try:
                assert budget.remaining_bytes == 0
                assert budget.used_descriptors == budget.descriptor_limit
                cancelled[0] = True
                ledger.close()
                assert ledger._closed
            finally:
                occupied.release()
    assert budget.used_bytes == budget.used_descriptors == 0
    assert not list(tmp_path.iterdir())
