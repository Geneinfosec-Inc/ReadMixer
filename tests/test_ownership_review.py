"""Preserve the first cleanup interruption across later page-level failures."""

import pytest

from readmixer_py.crypto import OperationKeys
from readmixer_py.errors import CancelledError, ResourceError
from readmixer_py.ownership import OutputOwnershipJournal, OwnershipRecord
from readmixer_py.resources import Budget
from readmixer_py.storage import SegmentStore


def register(journal, ordinal):
    ticket = journal.prepare_inventory(OwnershipRecord(
        ordinal, f'artifact-{ordinal}.test', 1, ordinal + 2, 4, b'x' * 32, 0, 0))
    journal.commit(ticket)


@pytest.mark.parametrize('interrupt_type', (KeyboardInterrupt, CancelledError))
@pytest.mark.parametrize('later_failure', ('authentication', 'drop'))
def test_first_callback_interruption_survives_later_page_failure_and_retry(
        tmp_path, monkeypatch, interrupt_type, later_failure):
    budget = Budget(128 << 20)
    with SegmentStore('external', budget, OperationKeys.new(), temp_dir=tmp_path) as store:
        journal = OutputOwnershipJournal(store)
        register(journal, 0)
        journal.inventory.flush()
        older_root = journal.inventory.root
        page_path = store.scratch_directory / (older_root.last.segment_id.hex() + '.segment')
        original_page = page_path.read_bytes()
        register(journal, 1)  # This pending owner must finish before the older page.
        if later_failure == 'authentication':
            damaged = bytearray(original_page)
            damaged[-1] ^= 1
            page_path.write_bytes(damaged)
        else:
            drop = store.drop
            def fail_after_drop(ref, **kwargs):
                drop(ref, **kwargs)
                raise ResourceError('later page retirement failed after deletion')
            monkeypatch.setattr(store, 'drop', fail_after_drop)

        first = interrupt_type()
        calls = []
        def clean(record, progress):
            calls.append(record.ordinal)
            if record.ordinal == 1 and not progress.flags:
                progress.mark(1)
                raise first

        with pytest.raises(interrupt_type) as caught:
            journal.drain_inventory(clean)
        assert caught.value is first
        assert calls == ([1, 1] if later_failure == 'authentication' else [1, 1, 0])
        assert journal.inventory.root is older_root
        assert journal.inventory.owned_remaining == 1
        assert any('owned ledger cleanup also failed' in note for note in first.__notes__)
        if later_failure == 'authentication':
            assert page_path.exists()
            page_path.write_bytes(original_page)
        else:
            assert not page_path.exists()
            monkeypatch.setattr(store, 'drop', drop)

        # The first exception remains retained while the second attempt uses
        # bounded progress/root state. A deleted page need not be read again,
        # and a successfully completed callback must not run a second time.
        journal.drain_inventory(clean)
        assert calls == [1, 1, 0]
        assert journal.inventory.owned_remaining == 0
        journal.close()
        assert store._ledger_roots == 0
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize('action', ('close', 'abandon'))
@pytest.mark.parametrize('after_debit', (False, True))
def test_owned_root_retirement_interruption_cannot_debit_twice(
        monkeypatch, action, after_debit):
    from readmixer_py.ledger import PagedLedger

    budget = Budget(128 << 20)
    store = SegmentStore('memory', budget, OperationKeys.new())
    ledger = PagedLedger(store, owns_records=True)
    if action == 'abandon':
        ledger.append(b'owned record')
        ledger.flush()
    assert store._ledger_roots == 1
    original = SegmentStore.__setattr__
    first = KeyboardInterrupt()
    injections = []
    def interrupted(owner, name, value):
        if owner is store and name == '_ledger_roots' and value == 0 and not injections:
            injections.append(True)
            if after_debit:
                original(owner, name, value)
            raise first
        original(owner, name, value)
    monkeypatch.setattr(SegmentStore, '__setattr__', interrupted)

    with pytest.raises(KeyboardInterrupt) as caught:
        getattr(ledger, action)()
    assert caught.value is first and injections == [True]
    assert store._ledger_roots == (0 if after_debit else 1)
    assert ledger._closed is after_debit
    getattr(ledger, action)()
    getattr(ledger, action)()
    assert ledger._closed and store._ledger_roots == 0
    getattr(store, action)()
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0


@pytest.mark.parametrize('mode', ('memory', 'external'))
@pytest.mark.parametrize('after_registration', (False, True))
def test_constructor_interruption_restores_previous_root_count_and_admission(
        tmp_path, monkeypatch, mode, after_registration):
    from readmixer_py.ledger import PagedLedger

    budget = Budget(128 << 20)
    with SegmentStore(mode, budget, OperationKeys.new(), temp_dir=tmp_path) as store:
        existing = PagedLedger(store)
        baseline = (budget.used_bytes, budget.used_metadata_bytes, budget.used_descriptors)
        assert store._ledger_roots == 1
        original = SegmentStore.__setattr__
        first = KeyboardInterrupt()
        reached = []
        def interrupted(owner, name, value):
            if owner is store and name == '_ledger_roots' and value == 2 and not reached:
                reached.append(True)
                if after_registration:
                    original(owner, name, value)
                raise first
            original(owner, name, value)
        monkeypatch.setattr(SegmentStore, '__setattr__', interrupted)

        with pytest.raises(KeyboardInterrupt) as caught:
            PagedLedger(store, owns_records=True)
        assert caught.value is first and reached == [True]
        assert store._ledger_roots == 1
        assert (budget.used_bytes, budget.used_metadata_bytes, budget.used_descriptors) == baseline
        # Retain the original exception while showing the older root is intact
        # and the store can admit and retire another root after the failed call.
        with PagedLedger(store, owns_records=True):
            assert store._ledger_roots == 2
        existing.close()
        assert store._ledger_roots == 0
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
    assert not list(tmp_path.iterdir())
