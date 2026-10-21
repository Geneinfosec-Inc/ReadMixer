"""Plan 13 exact encrypted capabilities, credit ownership and crash handoffs."""
from dataclasses import replace
import os

import pytest

from readmixer_py.crypto import OperationKeys, observations_identity
from readmixer_py.errors import IntegrityError, ResourceError
from readmixer_py.resources import Budget
from readmixer_py.storage import SegmentStore, BLOCK_BYTES
from readmixer_py.worker_storage import (
    WorkerStorage, SegmentReadGrant, SegmentWriteGrant, encode_grant,
    decode_read_grant, decode_write_grant, read_granted, write_granted,
)


@pytest.fixture
def owned(tmp_path):
    budget = Budget(128 << 20)
    keys = OperationKeys._from_test(bytes(range(32)), bytes(range(16)))
    store = SegmentStore('auto', budget, keys, temp_dir=tmp_path)
    manager = WorkerStorage(store, max_pending=3)
    yield budget, store, manager
    manager.cleanup(workers_reaped=True)
    store.close()
    assert budget.active_leases == budget.used_descriptors == budget.used_bytes == 0
    assert list(tmp_path.iterdir()) == []


def make_grant(manager, *, task=1, data=b'private opaque payload', items=2):
    return manager.issue_write(task, task, 5, observations_identity('verify', task), len(data), items)


def write(manager, grant, data=b'private opaque payload', items=2):
    return write_granted(grant, data, items,
                         lambda offset, amount: manager.authorize_write(grant, offset, amount),
                         lambda offset, actual: manager.commit_write(grant, offset, actual))


def path(store, ref):
    return store.scratch_directory / (ref.segment_id.hex() + '.segment')


@pytest.mark.parametrize('data', [b'', b'private opaque payload', b'ACGT' * (BLOCK_BYTES // 2 + 99)])
def test_P13_T03_T04_external_capabilities_share_namespace_and_authenticated_codec(owned, data):
    budget, store, manager = owned
    grant = make_grant(manager, data=data)
    assert decode_write_grant(encode_grant(grant)) == grant
    assert not hasattr(grant, 'root')
    assert grant.key == store.keys.segment_key(grant.ref.segment_id, grant.ref.generation_salt,
                                               grant.ref.purpose, grant.ref.logical_identity)
    ref = write(manager, grant, data)
    assert data not in path(store, ref).read_bytes() if data else True
    assert manager.accept_write(grant, ref) == ref
    assert manager.take_write(grant) == ref
    read = manager.issue_read(ref)
    assert decode_read_grant(encode_grant(read)) == read
    assert read_granted(read) == data
    with pytest.raises(ResourceError, match='active readers'):
        store.drop(ref)
    manager.release_read(read)
    manager.cleanup(workers_reaped=True)
    with store.open_verified(ref) as stream:
        assert b''.join(iter(lambda: stream.read(BLOCK_BYTES), b'')) == data
    assert store.disk_budget.pending_bytes == 0
    store.drop(ref)
    assert store.disk_budget.committed_bytes == 0


def test_P13_T04_accepted_but_unreturned_output_is_owned_until_take(owned):
    _, store, manager = owned
    grant = make_grant(manager)
    ref = write(manager, grant)
    manager.accept_write(grant, ref)
    assert ref.segment_id in store._entries
    manager.cleanup(workers_reaped=True)
    assert not path(store, ref).exists()
    assert ref.segment_id not in store._entries
    assert store.disk_budget.committed_bytes == 0


def test_P13_T04_lost_partial_commit_reconciles_after_reap(owned):
    _, store, manager = owned
    grant = make_grant(manager)
    manager.authorize_write(grant, 0, 70)
    with open(path(store, grant.ref), 'wb', buffering=0) as output:
        output.write(b'partial encrypted bytes')
    with pytest.raises(ResourceError, match='reaped'):
        manager.cleanup(workers_reaped=False)
    assert store.disk_budget.pending_bytes == 70
    manager.cleanup(workers_reaped=True)
    assert store.disk_budget.pending_bytes == store.disk_budget.committed_bytes == 0


@pytest.mark.parametrize('mutation', ['key', 'salt', 'digest', 'trailing', 'truncated'])
def test_P13_T03_wrong_key_seal_or_length_is_rejected_before_accept(owned, mutation):
    _, store, manager = owned
    grant = make_grant(manager)
    ref = write(manager, grant)
    if mutation == 'key':
        # A child can encrypt only under its exact capability, never a root key.
        ref = replace(ref, generation_salt=b'x' * 32)
    elif mutation == 'salt':
        ref = replace(ref, segment_id=(123).to_bytes(16, 'big'))
    elif mutation == 'digest':
        ref = replace(ref, digest=b'y' * 32)
    else:
        blob = path(store, ref).read_bytes()
        path(store, ref).write_bytes(blob + b'foreign' if mutation == 'trailing' else blob[:-1])
    with pytest.raises(IntegrityError):
        manager.accept_write(grant, ref)
    if mutation in ('trailing', 'truncated'):
        path(store, ref).write_bytes(blob)


@pytest.mark.parametrize('field,value', [
    ('key', b'x'), ('directory_inode', 0), ('directory_device', -1),
    ('directory', '../secret'), ('directory', '/a/../secret'),
    ('directory', '/' + 'a' * 4096), ('max_decoded', (64 << 20) + 1),
    ('max_items', True), ('task_id', -1), ('grant_id', 1 << 64),
])
def test_P13_T03_malformed_capabilities_are_bounded_and_private(owned, field, value):
    _, _, manager = owned
    grant = make_grant(manager)
    assert repr(grant) == 'SegmentWriteGrant(<protected>)'
    with pytest.raises(IntegrityError) as error:
        encode_grant(replace(grant, **{field: value}))
    assert 'secret' not in str(error.value)
    assert grant.key.hex() not in str(error.value)


def test_P13_T03_all_codec_truncations_and_extra_bytes_fail(owned):
    _, _, manager = owned
    grant = make_grant(manager)
    encoded = encode_grant(grant)
    for end in range(len(encoded)):
        with pytest.raises(IntegrityError):
            decode_write_grant(encoded[:end])
    for blob in (encoded + b'\0', b'x' * (16384 + 1)):
        with pytest.raises(IntegrityError):
            decode_write_grant(blob)
    with pytest.raises(IntegrityError):
        decode_read_grant(encoded)


def test_P13_T04_foreign_replacement_is_preserved_and_cleanup_retryable(owned):
    _, store, manager = owned
    grant = make_grant(manager)
    owned_file = path(store, grant.ref)
    saved = owned_file.with_suffix('.saved')
    owned_file.rename(saved)
    owned_file.write_bytes(b'foreign')
    owned_file.chmod(0o600)
    with pytest.raises(IntegrityError):
        write(manager, grant)
    with pytest.raises(IntegrityError):
        manager.cleanup(workers_reaped=True)
    assert owned_file.read_bytes() == b'foreign'
    owned_file.unlink()
    saved.rename(owned_file)
    manager.cleanup(workers_reaped=True)


def test_P13_T04_credit_duplicate_offset_and_excess_are_rejected(owned):
    _, store, manager = owned
    grant = make_grant(manager)
    manager.authorize_write(grant, 0, 20)
    for offset, amount in ((0, 20), (1, 20), (0, 0), (0, BLOCK_BYTES + 37)):
        with pytest.raises(IntegrityError):
            manager.authorize_write(grant, offset, amount)
    for offset, actual in ((1, 0), (0, 21), (0, 1)):
        with pytest.raises(IntegrityError):
            manager.commit_write(grant, offset, actual)
    manager.commit_write(grant, 0, 0)
    with pytest.raises(IntegrityError):
        manager.commit_write(grant, 0, 0)
    assert store.disk_budget.pending_bytes == 0


def test_P13_T04_multiple_pages_and_out_of_order_transfers_stay_bounded(owned):
    _, store, manager = owned
    earliest = make_grant(manager)
    for task in range(2, 230):
        grant = make_grant(manager, task=task)
        ref = write(manager, grant)
        manager.accept_write(grant, ref)
        manager.take_write(grant)
        store.drop(ref)
    manager.ledger.flush()
    assert manager.ledger.root.page_count >= 2
    assert len(manager._writes) == 1
    assert manager._writes[(1, 1)].grant == earliest
    manager.cleanup(workers_reaped=True)
    assert store.disk_budget.committed_bytes == 0


def test_P13_T04_corrupt_task_page_never_authorizes_deletion(owned):
    _, store, manager = owned
    grant = make_grant(manager)
    manager.ledger.flush()
    root = manager.ledger.root.last
    page = path(store, root)
    original = page.read_bytes()
    damaged = bytearray(original)
    damaged[-1] ^= 1
    page.write_bytes(damaged)
    with pytest.raises(IntegrityError):
        manager.cleanup(workers_reaped=True)
    assert path(store, grant.ref).exists()
    page.write_bytes(original)
    manager.cleanup(workers_reaped=True)


def test_P13_T05_strict_memory_refuses_disk_capabilities(tmp_path):
    budget = Budget(64 << 20)
    with SegmentStore('memory', budget, OperationKeys.new(), temp_dir=tmp_path) as store:
        with pytest.raises(ResourceError, match='strict memory'):
            WorkerStorage(store)
    assert not list(tmp_path.iterdir())
    assert budget.used_bytes == 0


def test_P13_T05_auto_read_spills_encrypts_and_pins_source(owned):
    _, store, manager = owned
    data = b'private opaque payload'
    with store.new_segment(5, observations_identity('verify', 0)) as writer:
        writer.append(data, item_count=1)
        resident = writer.seal()
    assert not store.is_external(resident)
    read = manager.issue_read(resident)
    assert store.is_external(resident)
    assert data not in path(store, read.ref).read_bytes()
    assert repr(read) == 'SegmentReadGrant(<protected>)'
    assert read_granted(read) == data
    manager.release_read(read)
    with pytest.raises(IntegrityError):
        manager.release_read(read)
    store.drop(read.ref)


def test_P13_T04_zero_based_task_and_lost_take_return_keep_receiver_owned(owned, monkeypatch):
    _, store, manager = owned
    grant = manager.issue_write(0, 1, 5, observations_identity('verify', 0), 0, 0)
    assert decode_write_grant(encode_grant(grant)).task_id == 0
    ref = write(manager, grant, b'', 0)
    manager.accept_write(grant, ref)
    append = manager.ledger.append
    def interrupted(record):
        append(record)
        raise KeyboardInterrupt
    monkeypatch.setattr(manager.ledger, 'append', interrupted)
    with pytest.raises(KeyboardInterrupt):
        manager.take_write(grant)
    # Caller registered its result owner before attempting take and closes it
    # even when the transfer's authenticated boundary committed before return.
    store.drop(ref, missing_ok=True)
    manager.cleanup(workers_reaped=True)
    assert store.disk_budget.committed_bytes == 0


def test_P13_T04_lost_accept_return_keeps_manager_owner(owned, monkeypatch):
    _, store, manager = owned
    grant = make_grant(manager)
    ref = write(manager, grant)
    class FailAfterInsert(dict):
        def __setitem__(self, key, value):
            super().__setitem__(key, value)
            raise KeyboardInterrupt
    original = store._entries
    store._entries = FailAfterInsert(original)
    with pytest.raises(KeyboardInterrupt):
        manager.accept_write(grant, ref)
    store._entries = dict(store._entries)
    manager.cleanup(workers_reaped=True)
    assert ref.segment_id not in store._entries
    assert not path(store, ref).exists()


def test_P13_T04_lost_registration_return_is_still_owned(owned, monkeypatch):
    _, store, manager = owned
    append = manager.ledger.append
    def interrupted(record):
        append(record)
        raise KeyboardInterrupt
    monkeypatch.setattr(manager.ledger, 'append', interrupted)
    with pytest.raises(KeyboardInterrupt):
        make_grant(manager)
    assert len(manager._writes) == 1
    assert manager.ledger.owned_count == 1
    manager.cleanup(workers_reaped=True)
    assert store.disk_budget.committed_bytes == 0


def test_P13_T04_registration_failure_before_boundary_cleans_created_inode(owned, monkeypatch):
    _, store, manager = owned
    def interrupted(record):
        raise KeyboardInterrupt
    monkeypatch.setattr(manager.ledger, 'append', interrupted)
    with pytest.raises(KeyboardInterrupt):
        make_grant(manager)
    assert len(manager._writes) == 1
    assert manager.ledger.owned_count == 0
    manager.cleanup(workers_reaped=True)
    assert not list(store.scratch_directory.iterdir())


def test_P13_T04_read_pin_partial_handoff_rolls_back(owned):
    _, store, manager = owned
    grant = make_grant(manager)
    ref = write(manager, grant)
    manager.accept_write(grant, ref)
    manager.take_write(grant)
    class FailAfterInsert(dict):
        def __setitem__(self, key, value):
            super().__setitem__(key, value)
            raise KeyboardInterrupt
    store._reader_pins = FailAfterInsert()
    with pytest.raises(KeyboardInterrupt):
        manager.issue_read(ref)
    assert not manager._reads
    assert not store._reader_pins
    assert store._entries[ref.segment_id].pinned == 0
    store.drop(ref)


def test_P13_T04_credit_return_interruption_retains_reservation(owned, monkeypatch):
    _, store, manager = owned
    grant = make_grant(manager)
    reserve = store.disk_budget.reserve
    def interrupted(*args, **kwargs):
        reserve(*args, **kwargs)
        raise KeyboardInterrupt
    monkeypatch.setattr(store.disk_budget, 'reserve', interrupted)
    with pytest.raises(KeyboardInterrupt):
        manager.authorize_write(grant, 0, 20)
    assert store.disk_budget.pending_bytes == 20
    monkeypatch.setattr(store.disk_budget, 'reserve', reserve)
    manager.cleanup(workers_reaped=True)
    assert store.disk_budget.pending_bytes == store.disk_budget.committed_bytes == 0


def test_P13_T04_credit_retention_callback_failure_does_not_admit(owned, monkeypatch):
    _, store, manager = owned
    grant = make_grant(manager)
    reserve = store.disk_budget.reserve
    def interrupted(*args, retained=None, **kwargs):
        def fail(ticket):
            retained(ticket)
            raise KeyboardInterrupt
        return reserve(*args, retained=fail, **kwargs)
    monkeypatch.setattr(store.disk_budget, 'reserve', interrupted)
    with pytest.raises(KeyboardInterrupt):
        manager.authorize_write(grant, 0, 20)
    assert store.disk_budget.pending_bytes == 0
    assert manager._writes[(grant.task_id, grant.grant_id)].ticket is not None
    monkeypatch.setattr(store.disk_budget, 'reserve', reserve)
    manager.cleanup(workers_reaped=True)


def test_P13_T04_committed_credit_return_interruption_never_double_debits(owned, monkeypatch):
    from readmixer_py.disk import DiskReservation
    _, store, manager = owned
    grant = make_grant(manager)
    manager.authorize_write(grant, 0, 20)
    path(store, grant.ref).write_bytes(b'partial')
    commit = DiskReservation.commit
    def interrupted(self, actual=None):
        commit(self, actual)
        raise KeyboardInterrupt
    monkeypatch.setattr(DiskReservation, 'commit', interrupted)
    with pytest.raises(KeyboardInterrupt):
        manager.commit_write(grant, 0, 7)
    assert store.disk_budget.committed_bytes == 7
    monkeypatch.setattr(DiskReservation, 'commit', commit)
    manager.cleanup(workers_reaped=True)
    assert store.disk_budget.pending_bytes == store.disk_budget.committed_bytes == 0


def test_P13_T04_unlinked_pending_file_refunds_known_growth(owned):
    _, store, manager = owned
    grant = make_grant(manager)
    ref = write(manager, grant)
    path(store, ref).unlink()
    manager.cleanup(workers_reaped=True)
    assert store.disk_budget.committed_bytes == 0


def test_P13_T04_pending_and_transferred_cleanup_retry_after_unlink(owned, monkeypatch):
    import readmixer_py.worker_storage as ws
    _, store, manager = owned
    grant = make_grant(manager)
    ref = write(manager, grant)
    unlink = os.unlink
    interrupted = False
    def fail(name, *args, **kwargs):
        nonlocal interrupted
        unlink(name, *args, **kwargs)
        if name == store._name(ref) and not interrupted:
            interrupted = True
            raise KeyboardInterrupt
    monkeypatch.setattr(ws.os, 'unlink', fail)
    with pytest.raises(KeyboardInterrupt):
        manager.cleanup(workers_reaped=True)
    manager.cleanup(workers_reaped=True)
    assert store.disk_budget.committed_bytes == 0


def test_P13_T04_partial_ENOSPC_is_reconciled_without_child_disk_budget(owned, monkeypatch):
    import errno
    _, store, manager = owned
    grant = make_grant(manager)
    original = os.write
    calls = 0
    def partial(fd, data):
        nonlocal calls
        calls += 1
        if calls == 1:
            return original(fd, data[:13])
        raise OSError(errno.ENOSPC, 'no space')
    monkeypatch.setattr(os, 'write', partial)
    with pytest.raises(OSError) as error:
        write(manager, grant)
    assert error.value.errno == errno.ENOSPC
    assert store.disk_budget.pending_bytes == 0
    assert store.disk_budget.committed_bytes == 13
    monkeypatch.setattr(os, 'write', original)
    manager.cleanup(workers_reaped=True)
    assert store.disk_budget.committed_bytes == 0


def test_P13_T03_read_wrong_derived_key_fails_authentication(owned):
    _, store, manager = owned
    grant = make_grant(manager)
    ref = write(manager, grant)
    manager.accept_write(grant, ref)
    manager.take_write(grant)
    read = manager.issue_read(ref)
    with pytest.raises(IntegrityError, match='authentication'):
        read_granted(replace(read, key=b'\0' * 32))
    manager.release_read(read)
    store.drop(ref)


def test_P13_T03_write_grant_cannot_restart_nonce_sequence(owned):
    _, _, manager = owned
    grant = make_grant(manager)
    write(manager, grant)
    with pytest.raises(IntegrityError, match='length changed'):
        write(manager, grant)


def test_P13_T03_directory_substitution_and_unsafe_mode_fail_closed(owned):
    _, store, manager = owned
    grant = make_grant(manager)
    with pytest.raises(IntegrityError, match='directory identity'):
        write(manager, replace(grant, directory_inode=grant.directory_inode + 1))
    directory = store.scratch_directory
    directory.chmod(0o755)
    try:
        with pytest.raises(IntegrityError, match='directory identity'):
            write(manager, grant)
    finally:
        directory.chmod(0o700)


def test_P13_T04_read_ack_interruption_retains_one_pin(owned, monkeypatch):
    _, store, manager = owned
    grant = make_grant(manager)
    ref = write(manager, grant)
    manager.accept_write(grant, ref)
    manager.take_write(grant)
    read = manager.issue_read(ref)
    original = store._unpin
    def interrupted(segment_id):
        original(segment_id)
        raise KeyboardInterrupt
    monkeypatch.setattr(store, '_unpin', interrupted)
    with pytest.raises(KeyboardInterrupt):
        manager.release_read(read)
    assert store._reader_pins[ref.segment_id] == 1
    assert store._entries[ref.segment_id].pinned == 1
    monkeypatch.setattr(store, '_unpin', original)
    manager.release_read(read)
    store.drop(ref)


def test_P13_T04_journal_retirement_interruption_keeps_transfer_decision(owned):
    _, store, manager = owned
    grant = make_grant(manager)
    ref = write(manager, grant)
    manager.accept_write(grant, ref)
    manager.take_write(grant)
    class OnceAfterPop(dict):
        interrupted = False
        def pop(self, key, *default):
            found = super().pop(key, *default)
            if not self.interrupted:
                self.interrupted = True
                raise KeyboardInterrupt
            return found
    manager._transferred = OnceAfterPop()
    with pytest.raises(KeyboardInterrupt):
        manager.cleanup(workers_reaped=True)
    manager.cleanup(workers_reaped=True)
    assert path(store, ref).exists()
    store.drop(ref)


def test_P13_T04_take_ack_duplicate_is_rejected(owned):
    _, store, manager = owned
    grant = make_grant(manager)
    ref = write(manager, grant)
    manager.accept_write(grant, ref)
    with pytest.raises(IntegrityError):
        manager.accept_write(grant, ref)
    manager.take_write(grant)
    with pytest.raises(IntegrityError):
        manager.take_write(grant)
    store.drop(ref)


def test_P13_T04_interrupted_partial_commit_cannot_refund_other_owned_bytes(owned, monkeypatch):
    from readmixer_py.disk import DiskReservation
    _, store, manager = owned
    first = make_grant(manager)
    first_ref = write(manager, first)
    manager.accept_write(first, first_ref)
    manager.take_write(first)
    first_charge = store.disk_budget.committed_bytes
    grant = make_grant(manager, task=2)
    manager.authorize_write(grant, 0, 20)
    file = path(store, grant.ref)
    file.write_bytes(b'partial')
    commit = DiskReservation.commit
    def interrupted(self, actual=None):
        commit(self, actual)
        raise KeyboardInterrupt
    monkeypatch.setattr(DiskReservation, 'commit', interrupted)
    with pytest.raises(KeyboardInterrupt):
        manager.commit_write(grant, 0, 7)
    monkeypatch.setattr(DiskReservation, 'commit', commit)
    # Same inode, but growth after the acknowledged partial amount is forbidden.
    file.write_bytes(b'x' * 20)
    with pytest.raises(IntegrityError, match='exact committed credit'):
        manager.cleanup(workers_reaped=True)
    assert store.disk_budget.committed_bytes == first_charge + 7
    assert path(store, first_ref).exists()
    file.write_bytes(b'partial')
    manager.cleanup(workers_reaped=True)
    assert store.disk_budget.committed_bytes == first_charge
    store.drop(first_ref)


def test_P13_T03_write_generations_never_alias_historical_task_records(owned):
    _, store, manager = owned
    grant = make_grant(manager)
    ref = write(manager, grant)
    manager.accept_write(grant, ref)
    manager.take_write(grant)
    for task in (1, 2):
        with pytest.raises(IntegrityError, match='reused or regressed'):
            manager.issue_write(task, 1, 5, observations_identity('verify', task), 20, 1)
    next_grant = manager.issue_write(1, 2, 5, observations_identity('verify', 1), 20, 1)
    assert next_grant.ref.segment_id != grant.ref.segment_id
    assert next_grant.ref.generation_salt != grant.ref.generation_salt
    manager.cleanup(workers_reaped=True)
    assert path(store, ref).exists()
    store.drop(ref)
