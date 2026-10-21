"""P03-T05 / P05-T02 chained registry paging without a historical ref table."""

from dataclasses import replace
import os

import pytest

from readmixer_py.crypto import OperationKeys
from readmixer_py.disk import DiskBudget
from readmixer_py.errors import CancelledError, IntegrityError, ResourceError
from readmixer_py.ledger import PagedLedger
from readmixer_py.resources import Budget
from readmixer_py.storage import SegmentStore


@pytest.mark.parametrize("mode", ["memory", "external", "auto"])
def test_many_pages_preserve_reverse_records_and_drop_all_owned_bytes(tmp_path, mode):
    budget, disk = Budget(128 << 20), DiskBudget()
    expected = [i.to_bytes(4, "big") + b"opaque-row" * 90 for i in range(60)]
    with SegmentStore(mode, budget, OperationKeys.new(), temp_dir=tmp_path, disk_budget=disk) as store:
        with PagedLedger(store, page_bytes=8192, max_record_bytes=1024) as ledger:
            for record in expected:
                ledger.append(record)
            ledger.flush()
            assert ledger.root.page_count >= 10
            assert ledger.root.record_count == len(expected)
            assert ledger.root.decoded_bytes == sum(map(len, expected))
            if mode != "memory":
                assert store.resident_segments == 0  # Only one root owns the page chain.
                assert disk.committed_bytes > 0
            with ledger.open_reverse() as cursor:
                assert list(cursor) == expected[::-1]
            with ledger.open_reverse() as cursor:
                assert next(cursor) == expected[-1]  # Exit drains/verifies all remaining pages.
        assert disk.committed_bytes == 0
    assert budget.used_bytes == 0 and not list(tmp_path.iterdir())


def test_root_totals_and_digest_cannot_bless_incomplete_chain(tmp_path):
    with SegmentStore("memory", Budget(64 << 20), OperationKeys.new()) as store:
        with PagedLedger(store) as ledger:
            ledger.append(b"one")
            ledger.flush()
            good = ledger.root
            try:
                ledger.root = replace(good, record_count=good.record_count + 1)
                with pytest.raises(IntegrityError):
                    with ledger.open_reverse():
                        pass
                ledger.root = replace(good, aggregate_digest=b"\0" * 32)
                with pytest.raises(IntegrityError):
                    with ledger.open_reverse():
                        pass
            finally:
                ledger.root = good


def test_external_page_corruption_is_checked_even_if_caller_only_reads_newest(tmp_path):
    budget = Budget(64 << 20)
    with SegmentStore("external", budget, OperationKeys.new(), temp_dir=tmp_path) as store:
        ledger = PagedLedger(store)
        try:
            ledger.append(b"old")
            ledger.flush()
            oldest = ledger.root.last
            ledger.append(b"new")
            ledger.flush()
            path = store.scratch_directory / (oldest.segment_id.hex() + ".segment")
            original = path.read_bytes()
            changed = bytearray(original)
            changed[-1] ^= 1
            path.write_bytes(changed)
            try:
                with pytest.raises(IntegrityError):
                    with ledger.open_reverse() as cursor:
                        assert next(cursor) == b"new"
            finally:
                path.write_bytes(original)
        finally:
            ledger.close()


def test_failed_page_handoff_retains_original_root_and_pending_records(tmp_path, monkeypatch):
    with SegmentStore("external", Budget(64 << 20), OperationKeys.new(), temp_dir=tmp_path) as store:
        with PagedLedger(store) as ledger:
            ledger.append(b"prior")
            ledger.flush()
            root = ledger.root
            ledger.append(b"pending")
            original = store.detach
            monkeypatch.setattr(store, "detach", lambda ref: (_ for _ in ()).throw(ResourceError("injected")))
            with pytest.raises(ResourceError):
                ledger.flush()
            assert ledger.root == root
            monkeypatch.setattr(store, "detach", original)
            ledger.flush()
            with ledger.open_reverse() as cursor:
                assert list(cursor) == [b"pending", b"prior"]


def test_cancellation_still_allows_authenticated_cleanup(tmp_path):
    cancelled = [False]
    budget = Budget(64 << 20)
    with SegmentStore("external", budget, OperationKeys.new(), temp_dir=tmp_path, cancel=lambda: cancelled[0]) as store:
        with PagedLedger(store, cancel=lambda: cancelled[0]) as ledger:
            ledger.append(b"entry")
            ledger.flush()
            cancelled[0] = True
            with pytest.raises(CancelledError):
                with ledger.open_reverse():
                    pass
    assert not list(tmp_path.iterdir()) and budget.used_bytes == 0


def test_closed_and_concurrent_cursor_ownership(tmp_path):
    with SegmentStore("memory", Budget(64 << 20), OperationKeys.new()) as store:
        with PagedLedger(store) as ledger:
            ledger.append(b"entry")
            with ledger.open_reverse() as cursor:
                with pytest.raises(ResourceError):
                    ledger.append(b"unallowed")
                with pytest.raises(ResourceError):
                    ledger.open_reverse()
                with pytest.raises(ResourceError):
                    ledger.close()
            with pytest.raises(ResourceError):
                next(cursor)
        with pytest.raises(ResourceError):
            ledger.append(b"closed")
