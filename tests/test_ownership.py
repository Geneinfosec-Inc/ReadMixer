"""P12 codec, canonical handoff, authenticated paging, and retryable cleanup."""

from dataclasses import replace

import pytest

from readmixer_py.crypto import OperationKeys
from readmixer_py.errors import CancelledError, ConfigurationError, IntegrityError, ResourceError
from readmixer_py.ownership import (
    INVENTORY, LINK, MAX_RECORD_BYTES, OutputOwnershipJournal, OwnershipRecord,
    decode_record, encode_record,
)
from readmixer_py.resources import Budget
from readmixer_py.storage import SegmentStore


def record(index=0, **changes):
    result = OwnershipRecord(index, f'artifact-{index:06d}.test', 2, 3 + index,
                             4 + index, b'x' * 32, -5, 6)
    return replace(result, **changes)


def add(journal, item, *, link=False):
    ticket = journal.prepare_link(item) if link else journal.prepare_inventory(item)
    assert not ticket.committed
    journal.commit(ticket)
    assert ticket.committed
    return ticket


def finish(journal):
    journal.drain_links(lambda _record, _progress: None)
    journal.drain_inventory(lambda _record, _progress: None)
    journal.close()


def test_P12_T01_independent_exact_private_codec_golden():
    item = record(0x0102030405060708, name='part.bgz')
    expected = (b'RMPYOWN1\x01\x01\x01\x02\x03\x04\x05\x06\x07\x08\x00\x08part.bgz'
                + b'\x00' * 7 + b'\x02' + b'\x00' * 7 + b'\x03'
                + b'\x00' * 7 + b'\x04' + b'x' * 32
                + b'\xff' * 7 + b'\xfb' + b'\x00' * 7 + b'\x06')
    # Explicit small facts keep the large ordinal's fixture independent.
    item = replace(item, inode=3, length=4)
    assert encode_record(item) == expected
    assert decode_record(expected, kind=INVENTORY) == item
    linked = expected[:9] + b'\x02' + expected[10:]
    assert encode_record(item, LINK) == linked
    assert decode_record(linked, kind=LINK) == item
    assert len(encode_record(record(name='a' * 255))) == 347 < MAX_RECORD_BYTES


@pytest.mark.parametrize('changes', [
    {'ordinal': -1}, {'ordinal': 1 << 64}, {'device': True}, {'inode': 1 << 64},
    {'length': -1}, {'mtime_ns': -(1 << 63) - 1}, {'ctime_ns': 1 << 63},
    {'mtime_ns': False}, {'digest': b'x' * 31}, {'digest': bytearray(32)},
    {'name': '../part'}, {'name': '/part'}, {'name': 'a' * 256}, {'name': 'é'}, {'name': ''},
])
def test_P12_T04_codec_rejects_unrepresentable_or_unsafe_facts(changes):
    with pytest.raises(ConfigurationError):
        encode_record(record(**changes))


@pytest.mark.parametrize('mutation', ['magic', 'version', 'kind', 'zero-name', 'long-name',
                                      'truncated', 'trailing', 'nonascii', 'separator'])
def test_P12_T04_decoder_rejects_malformed_private_records(mutation):
    data = bytearray(encode_record(record()))
    if mutation == 'magic':
        data[0] ^= 1
    elif mutation == 'version':
        data[8] = 2
    elif mutation == 'kind':
        data[9] = 3
    elif mutation == 'zero-name':
        data[18:20] = b'\0\0'
    elif mutation == 'long-name':
        data[18:20] = b'\x01\x00'
    elif mutation == 'truncated':
        data.pop()
    elif mutation == 'trailing':
        data.append(0)
    elif mutation == 'nonascii':
        data[20] = 255
    else:
        data[20] = ord('/')
    with pytest.raises(IntegrityError):
        decode_record(bytes(data), kind=INVENTORY)
    with pytest.raises(IntegrityError):
        decode_record(encode_record(record(), LINK), kind=INVENTORY)


@pytest.mark.parametrize('mode', ['memory', 'auto', 'external'])
def test_P12_T01_many_artifacts_have_two_roots_and_bounded_pages(tmp_path, mode):
    budget = Budget(256 << 20)
    with SegmentStore(mode, budget, OperationKeys.new(), temp_dir=tmp_path) as store:
        journal = OutputOwnershipJournal(store)
        expected = [record(index) for index in range(2000)]
        for item in expected:
            add(journal, item)
        assert store._ledger_roots == 2
        journal.freeze_inventory()
        assert journal.inventory.root.page_count >= 3
        with journal.iter_inventory() as rows:
            assert list(rows) == list(reversed(expected))
        for item in reversed(expected):
            add(journal, item, link=True)
        journal.validate_links()
        assert journal.inventory_count == journal.link_count == len(expected)
        assert len(journal._prepared) == 2
        assert journal.inventory._pending_count == journal.links._pending_count == 0
        if mode != 'memory':
            assert store.resident_segments == 0
            paths = list(store.scratch_directory.iterdir())
            assert len(paths) == journal.inventory.root.page_count + journal.links.root.page_count
            assert all(b'artifact-000000.test' not in path.read_bytes() for path in paths)
        else:
            assert store.scratch_directory is None
        linked, staged = [], []
        journal.drain_links(lambda item, _progress: linked.append(item.ordinal))
        journal.drain_inventory(lambda item, _progress: staged.append(item.ordinal))
        assert linked == list(range(2000))
        assert staged == list(reversed(range(2000)))
        journal.close()
        assert store._ledger_roots == 0
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
    assert not list(tmp_path.iterdir())


def test_P12_T01_inventory_limit_and_frozen_state_precede_growth(tmp_path):
    with SegmentStore('memory', Budget(128 << 20), OperationKeys.new()) as store:
        journal = OutputOwnershipJournal(store, max_records=1)
        add(journal, record())
        with pytest.raises(ResourceError):
            journal.prepare_inventory(record(1))
        journal.freeze_inventory()
        with pytest.raises(ResourceError, match='frozen'):
            journal.prepare_inventory(record())
        add(journal, record(), link=True)
        with pytest.raises(ResourceError):
            journal.prepare_link(record(1))
        finish(journal)


@pytest.mark.parametrize('after_commit', [False, True])
def test_P12_T03_canonical_boundary_survives_interrupted_append_return(tmp_path, monkeypatch, after_commit):
    with SegmentStore('memory', Budget(128 << 20), OperationKeys.new()) as store:
        journal = OutputOwnershipJournal(store)
        ticket = journal.prepare_inventory(record())
        ledger = journal.inventory
        real_commit = ledger._commit_owned_boundary

        def interrupted(boundary):
            if after_commit:
                real_commit(boundary)
            raise KeyboardInterrupt

        monkeypatch.setattr(ledger, '_commit_owned_boundary', interrupted)
        with pytest.raises(KeyboardInterrupt):
            journal.commit(ticket)
        assert ticket.committed is after_commit
        assert journal.inventory_count == int(after_commit)
        monkeypatch.setattr(ledger, '_commit_owned_boundary', real_commit)
        journal.commit(ticket)
        journal.commit(ticket)  # A retry cannot register the same ownership twice.
        assert journal.inventory_count == 1
        seen = []
        journal.drain_inventory(lambda item, _progress: seen.append(item))
        assert seen == [record()]
        finish(journal)


def test_P12_T03_discarded_ticket_cannot_alias_a_later_committed_record(tmp_path):
    with SegmentStore('memory', Budget(128 << 20), OperationKeys.new()) as store:
        journal = OutputOwnershipJournal(store)
        discarded = journal.prepare_inventory(record())
        with pytest.raises(ResourceError, match='pending'):
            journal.prepare_inventory(record(1))
        journal.discard(discarded)
        accepted = add(journal, record(0, name='replacement.test'))
        assert accepted.committed and not discarded.committed
        with pytest.raises(IntegrityError):
            journal.commit(discarded)
        finish(journal)


def test_P12_T03_failed_page_seal_preserves_owned_pending_records(tmp_path, monkeypatch):
    with SegmentStore('external', Budget(128 << 20), OperationKeys.new(), temp_dir=tmp_path) as store:
        journal = OutputOwnershipJournal(store)
        ticket = add(journal, record())
        real_detach = store.detach
        monkeypatch.setattr(store, 'detach', lambda ref: (_ for _ in ()).throw(ResourceError('injected seal handoff')))
        with pytest.raises(ResourceError):
            journal.freeze_inventory()
        assert ticket.committed and journal.inventory._pending_count == 1
        monkeypatch.setattr(store, 'detach', real_detach)
        seen = []
        journal.drain_inventory(lambda item, _progress: seen.append(item))
        assert seen == [record()]
        finish(journal)


def test_P12_T05_pending_records_are_drained_before_older_pages(tmp_path):
    with SegmentStore('external', Budget(128 << 20), OperationKeys.new(), temp_dir=tmp_path) as store:
        journal = OutputOwnershipJournal(store)
        add(journal, record(0))
        journal.inventory.flush()
        add(journal, record(1))
        with pytest.raises(ResourceError, match='drain'):
            journal.close()
        assert journal.inventory._pending_count == 1
        seen = []
        journal.drain_inventory(lambda item, _progress: seen.append(item.ordinal))
        assert seen == [1, 0]
        finish(journal)


def test_P12_T05_failed_record_retains_page_progress_and_refund_token(tmp_path):
    with SegmentStore('memory', Budget(128 << 20), OperationKeys.new()) as store:
        journal = OutputOwnershipJournal(store)
        for index in range(4):
            add(journal, record(index))
        journal.inventory.flush()
        attempts, credits = [], []
        token = object()

        def initial(item, progress):
            attempts.append(item.ordinal)
            if item.ordinal == 2:
                progress.flags = 1
                progress.refund = token
                raise ResourceError('injected interrupted cleanup stage')
            credits.append(item.ordinal)

        with pytest.raises(ResourceError):
            journal.drain_inventory(initial)
        assert attempts == [3, 2, 1, 0]
        assert credits == [3, 1, 0]
        assert journal.inventory.root.page_count == 1

        def retry(item, progress):
            assert item.ordinal == 2 and progress.flags == 1 and progress.refund is token
            credits.append(item.ordinal)
            progress.mark(2)

        journal.drain_inventory(retry)
        assert sorted(credits) == [0, 1, 2, 3]
        finish(journal)


def test_P12_T05_keyboard_interrupt_retries_once_and_preserves_cause(tmp_path):
    with SegmentStore('memory', Budget(128 << 20), OperationKeys.new()) as store:
        journal = OutputOwnershipJournal(store)
        add(journal, record())
        calls = []
        first = KeyboardInterrupt()

        def remove(item, progress):
            calls.append(item.ordinal)
            if not progress.flags:
                progress.mark(1)
                raise first

        with pytest.raises(KeyboardInterrupt) as caught:
            journal.drain_inventory(remove)
        assert caught.value is first and calls == [0, 0]
        assert journal.inventory.owned_remaining == 0
        finish(journal)


def test_P12_T04_authentication_failure_cannot_authorize_deletions(tmp_path):
    with SegmentStore('external', Budget(128 << 20), OperationKeys.new(), temp_dir=tmp_path) as store:
        journal = OutputOwnershipJournal(store)
        add(journal, record())
        journal.inventory.flush()
        ref = journal.inventory.root.last
        path = store.scratch_directory / (ref.segment_id.hex() + '.segment')
        original = path.read_bytes()
        corrupted = bytearray(original)
        corrupted[-1] ^= 1
        path.write_bytes(corrupted)
        seen = []
        with pytest.raises(IntegrityError):
            journal.drain_inventory(lambda item, _progress: seen.append(item))
        assert seen == [] and path.exists()
        path.write_bytes(original)
        finish(journal)


def test_P12_T04_whole_page_codec_validation_precedes_any_deletion(tmp_path):
    with SegmentStore('memory', Budget(128 << 20), OperationKeys.new()) as store:
        journal = OutputOwnershipJournal(store)
        journal.inventory.append(b'malformed owned record')
        journal.inventory.append(encode_record(record()))
        seen = []
        with pytest.raises(IntegrityError):
            journal.drain_inventory(lambda item, _progress: seen.append(item))
        assert seen == []
        journal.abandon()  # Explicit terminal disposal; no record actions occur.
        assert store._ledger_roots == 0


def test_P12_T05_page_drop_failure_does_not_repeat_successful_record_actions(tmp_path, monkeypatch):
    with SegmentStore('external', Budget(128 << 20), OperationKeys.new(), temp_dir=tmp_path) as store:
        journal = OutputOwnershipJournal(store)
        add(journal, record())
        journal.inventory.flush()
        seen = []
        real_drop = store.drop
        failed = [False]

        def fail_after_drop(ref, **kwargs):
            real_drop(ref, **kwargs)
            if not failed[0]:
                failed[0] = True
                raise KeyboardInterrupt

        monkeypatch.setattr(store, 'drop', fail_after_drop)
        with pytest.raises(KeyboardInterrupt):
            journal.drain_inventory(lambda item, _progress: seen.append(item.ordinal))
        monkeypatch.setattr(store, 'drop', real_drop)
        journal.drain_inventory(lambda item, _progress: seen.append(item.ordinal))
        assert seen == [0]
        finish(journal)


def test_P12_T05_cleanup_ignores_requested_cancellation(tmp_path):
    cancelled = [False]
    with SegmentStore('external', Budget(128 << 20), OperationKeys.new(), temp_dir=tmp_path,
                      cancel=lambda: cancelled[0]) as store:
        journal = OutputOwnershipJournal(store, cancel=lambda: cancelled[0])
        add(journal, record())
        journal.inventory.flush()
        cancelled[0] = True
        with pytest.raises(CancelledError):
            journal.freeze_inventory()
        finish(journal)


def test_P12_T04_handoff_ordinals_and_complete_link_coverage_are_required():
    with SegmentStore('memory', Budget(128 << 20), OperationKeys.new()) as store:
        journal = OutputOwnershipJournal(store)
        with pytest.raises(IntegrityError, match='ordinal'):
            journal.prepare_inventory(record(1))
        add(journal, record(0))
        add(journal, record(1))
        journal.freeze_inventory()
        with pytest.raises(IntegrityError, match='ordinal'):
            journal.prepare_link(record(0))
        add(journal, record(1), link=True)
        with pytest.raises(IntegrityError, match='cover'):
            journal.validate_links()
        add(journal, record(0), link=True)
        journal.validate_links()
        finish(journal)


@pytest.mark.parametrize('kind', [INVENTORY, LINK])
def test_P12_T04_authenticated_wrong_ordinal_authorizes_no_page_actions(tmp_path, kind):
    budget = Budget(128 << 20)
    store = SegmentStore('external', budget, OperationKeys.new(), temp_dir=tmp_path)
    journal = OutputOwnershipJournal(store)
    if kind == LINK:
        add(journal, record(0))
        add(journal, record(1))
        journal.freeze_inventory()
        # Validly encoded/encrypted facts remain invalid in the wrong position.
        journal.links.append(encode_record(record(0), LINK))
        journal.links.append(encode_record(record(1), LINK))
        verify, drain = journal.validate_links, journal.drain_links
    else:
        journal.inventory.append(encode_record(record(1)))
        journal.inventory.append(encode_record(record(0)))
        verify, drain = journal.freeze_inventory, journal.drain_inventory
    with pytest.raises(IntegrityError, match='ordinal'):
        verify()
    seen = []
    with pytest.raises(IntegrityError, match='ordinal'):
        drain(lambda item, _progress: seen.append(item))
    assert seen == []
    journal.abandon()
    store.abandon()
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0


def test_P12_T05_terminal_abandon_preserves_uncertain_pages_and_releases_budget(tmp_path):
    budget = Budget(128 << 20)
    store = SegmentStore('external', budget, OperationKeys.new(), temp_dir=tmp_path)
    journal = OutputOwnershipJournal(store)
    add(journal, record())
    journal.freeze_inventory()
    directory = store.scratch_directory
    path, = directory.iterdir()
    data = bytearray(path.read_bytes())
    data[-1] ^= 1
    path.write_bytes(data)
    with pytest.raises(IntegrityError):
        journal.drain_inventory(lambda _item, _progress: pytest.fail('unauthenticated deletion'))
    journal.abandon()
    journal.abandon()
    assert store._ledger_roots == 0 and path.read_bytes() == data
    store.abandon()
    store.abandon()
    assert path.exists() and store.keys is None
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0


def test_P12_T05_abandon_invalidates_an_active_decoded_cursor():
    budget = Budget(128 << 20)
    store = SegmentStore('memory', budget, OperationKeys.new())
    journal = OutputOwnershipJournal(store)
    for index in range(3):
        add(journal, record(index))
    journal.freeze_inventory()
    with journal.iter_inventory() as rows:
        assert next(rows) == record(2)
        journal.abandon()
        with pytest.raises(ResourceError, match='cursor is closed'):
            next(rows)
    assert store._ledger_roots == 0
    store.abandon()
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0


def test_P12_T03_second_root_admission_failure_releases_first_root(monkeypatch):
    import readmixer_py.ownership as ownership

    budget = Budget(128 << 20)
    with SegmentStore('memory', budget, OperationKeys.new()) as store:
        baseline = (budget.used_bytes, budget.used_metadata_bytes, budget.used_descriptors)
        original, calls = ownership.PagedLedger, []

        def fail_second(*args, **kwargs):
            calls.append(True)
            if len(calls) == 2:
                raise ResourceError('second root admission')
            return original(*args, **kwargs)

        monkeypatch.setattr(ownership, 'PagedLedger', fail_second)
        with pytest.raises(ResourceError, match='second root'):
            OutputOwnershipJournal(store)
        assert store._ledger_roots == 0
        assert (budget.used_bytes, budget.used_metadata_bytes, budget.used_descriptors) == baseline
