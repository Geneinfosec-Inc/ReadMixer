"""Private Plan 13 sender receipts and exact worker suballocation grants."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import hashlib

import pytest

from readmixer_py.errors import ConfigurationError, IntegrityError, ResourceError
from readmixer_py.resources import Budget, BUDGET_REGISTRY_BYTES, LEASE_REGISTRY_BYTES
from readmixer_py.worker_memory import LocalWorkspace, MemoryRef, TransferReceipt


RUN = b'r' * 16
DATA = b'PRIVATE FASTQ PAYLOAD'


def owned(payload=DATA):
    budget = Budget(8 << 20)
    original = budget.acquire('sender', max(1, len(payload)))
    return budget, original, MemoryRef(payload, original, RUN, 7, 9, budget=budget)


def test_P13_T05_memory_ref_moves_exclusive_authority_until_exact_receipt():
    budget, original, ref = owned()
    assert original.released
    assert not ref.lease.released
    assert ref.payload == DATA and ref.length == len(DATA)
    assert ref.digest == hashlib.sha256(DATA).digest()
    ref.bind(3, 5)
    receipt = ref.receipt()
    assert receipt == TransferReceipt(RUN, 7, 9, 3, 5, len(DATA), ref.digest)
    assert budget.used_bytes > 0
    ref.accept(receipt)
    assert ref.released and ref.lease.released
    assert budget.used_bytes == budget.used_metadata_bytes == 0
    with pytest.raises(ResourceError):
        _ = ref.payload
    with pytest.raises(ResourceError):
        ref.accept(receipt)
    ref.cancel_after_reap()  # idempotent after an acknowledged handoff


@pytest.mark.parametrize('field,value', [
    ('run_id', b'x' * 16), ('owner_id', 8), ('generation', 10),
    ('task_id', 4), ('grant_id', 6), ('length', len(DATA) + 1), ('digest', b'x' * 32),
])
def test_P13_T03_T05_receipt_mismatch_keeps_sender_owned(field, value):
    budget, _, ref = owned()
    ref.bind(3, 5)
    before = budget.used_bytes
    with pytest.raises(IntegrityError):
        ref.accept(replace(ref.receipt(), **{field: value}))
    assert ref.payload == DATA and not ref.released
    assert budget.used_bytes == before
    ref.cancel_after_reap()
    assert budget.used_bytes == 0


@pytest.mark.parametrize('action', ['unbound-receipt', 'unbound-accept', 'duplicate-bind', 'foreign-type'])
def test_P13_T03_transfer_state_rejects_unissued_and_replayed_handoffs(action):
    budget, _, ref = owned()
    with pytest.raises(IntegrityError):
        if action == 'unbound-receipt':
            ref.receipt()
        elif action == 'unbound-accept':
            ref.accept(TransferReceipt(RUN, 7, 9, 3, 5, len(DATA), ref.digest))
        else:
            ref.bind(3, 5)
            if action == 'duplicate-bind':
                ref.bind(3, 5)
            else:
                ref.accept(object())
    ref.cancel_after_reap()
    assert budget.used_bytes == 0


def test_P13_T03_released_foreign_and_undersized_leases_do_not_gain_authority():
    budget = Budget(8 << 20)
    foreign = Budget(8 << 20)
    lease = budget.acquire('sender', 1)
    for kwargs in ({'payload': DATA}, {'payload': b'x', 'budget': foreign}):
        with pytest.raises((ResourceError, ConfigurationError)):
            MemoryRef(lease=lease, run_id=RUN, owner_id=0, **kwargs)
        assert not lease.released
    lease.release()
    with pytest.raises(ResourceError):
        MemoryRef(b'x', lease, RUN, 0)
    assert budget.used_bytes == foreign.used_bytes == 0


def test_P13_T03_moved_handle_cannot_create_second_ref():
    budget, original, ref = owned()
    with pytest.raises(ResourceError):
        MemoryRef(DATA, original, RUN, 8)
    ref.cancel_after_reap()
    assert budget.used_bytes == 0


@pytest.mark.parametrize('name', ['run_id', 'owner_id', 'generation', 'length', 'digest', 'lease'])
def test_P13_T03_memory_identity_properties_are_read_only(name):
    budget, _, ref = owned()
    with pytest.raises(AttributeError):
        setattr(ref, name, None)
    ref.cancel_after_reap()
    assert budget.used_bytes == 0


def test_P13_T05_concurrent_binding_has_one_owner():
    budget, _, ref = owned()
    def bind(grant):
        try:
            ref.bind(3, grant)
            return grant
        except IntegrityError:
            return None
    with ThreadPoolExecutor(2) as executor:
        results = list(executor.map(bind, (1, 2)))
    assert len([result for result in results if result is not None]) == 1
    assert ref.receipt().grant_id == next(result for result in results if result is not None)
    ref.cancel_after_reap()
    assert budget.used_bytes == 0


def test_P13_T05_lease_mutation_is_not_silent_grant_growth():
    budget, _, ref = owned()
    ref.bind(3, 5)
    receipt = ref.receipt()
    ref.lease.resize(ref.length + 1)
    with pytest.raises(ResourceError, match='changed'):
        ref.accept(receipt)
    ref.cancel_after_reap()
    assert budget.used_bytes == 0


def test_P13_T05_interrupted_release_keeps_charge_for_retry(monkeypatch):
    budget, _, ref = owned()
    ref.bind(3, 5)
    receipt = ref.receipt()
    original = budget._publish_state
    def interrupted(state):
        original(state)
        raise KeyboardInterrupt
    monkeypatch.setattr(budget, '_publish_state', interrupted)
    with pytest.raises(KeyboardInterrupt):
        ref.accept(receipt)
    assert not ref.released and not ref.lease.released and budget.used_bytes > 0
    with pytest.raises(ResourceError, match='retention'):
        _ = ref.payload
    monkeypatch.setattr(budget, '_publish_state', original)
    ref.cancel_after_reap()
    assert budget.used_bytes == 0


@pytest.mark.parametrize('field,value', [
    ('run_id', b'x'), ('run_id', bytearray(RUN)), ('digest', b'x'),
    ('digest', bytearray(32)), ('owner_id', True), ('generation', -1),
    ('task_id', 1 << 64), ('grant_id', 1.0), ('length', -1),
])
def test_P13_T03_receipt_schema_is_closed_and_bounded(field, value):
    kwargs = dict(run_id=RUN, owner_id=7, generation=9, task_id=3, grant_id=5,
                  length=len(DATA), digest=hashlib.sha256(DATA).digest())
    kwargs[field] = value
    with pytest.raises(ConfigurationError):
        TransferReceipt(**kwargs)


def test_P13_T03_private_reprs_hide_data_operation_and_digest():
    budget, _, ref = owned()
    ref.bind(3, 5)
    for representation in (repr(ref), repr(ref.receipt())):
        assert 'PRIVATE' not in representation
        assert repr(RUN) not in representation
        assert repr(ref.digest) not in representation
    ref.cancel_after_reap()
    assert budget.used_bytes == 0


def test_P13_T05_empty_payload_still_requires_admitted_owner():
    budget, _, ref = owned(b'')
    ref.bind(0, 0)
    ref.accept(ref.receipt())
    assert budget.used_bytes == 0


def test_P13_T05_local_caps_are_exact_and_metadata_is_already_issued_subset():
    overhead = BUDGET_REGISTRY_BYTES + LEASE_REGISTRY_BYTES
    workspace = LocalWorkspace(overhead + 100, overhead + 50, 2)
    lease = workspace.acquire('sort-rows', 100, 2, metadata_bytes=50)
    assert workspace.used_bytes == workspace.limit_bytes == overhead + 100
    assert workspace.used_metadata_bytes == workspace.metadata_limit_bytes == overhead + 50
    assert workspace.used_descriptors == workspace.descriptor_limit == 2
    for kwargs in ({'bytes': 101, 'descriptors': 2, 'metadata_bytes': 50},
                   {'bytes': 100, 'descriptors': 3, 'metadata_bytes': 50},
                   {'bytes': 100, 'descriptors': 2, 'metadata_bytes': 51}):
        with pytest.raises(ResourceError):
            lease.resize(**kwargs)
        assert workspace.used_bytes == overhead + 100
        assert workspace.used_metadata_bytes == overhead + 50
        assert workspace.used_descriptors == 2
    with pytest.raises(ResourceError, match='live subleases'):
        workspace.close()
    lease.release()
    workspace.close()
    workspace.close()
    with pytest.raises(ResourceError, match='closed'):
        workspace.acquire('late', 1)


def test_P13_T05_local_split_transfer_and_resize_share_one_grant():
    workspace = LocalWorkspace(20000, 15000, 4)
    parent = workspace.acquire('job-input', 8000, 2, metadata_bytes=4000)
    child = parent.split('job-output', 4000, 1, metadata_bytes=2000)
    assert workspace.used_bytes == 8000 + BUDGET_REGISTRY_BYTES + 2 * LEASE_REGISTRY_BYTES
    replacement = child.transfer('receiver')
    assert child.released
    assert replacement.bytes == 4000 and replacement.metadata_bytes == 2000
    replacement.resize(8000, 2, metadata_bytes=4000)
    assert workspace.used_bytes <= workspace.limit_bytes
    parent.release()
    replacement.release()
    assert workspace.used_bytes == workspace.used_metadata_bytes == workspace.used_descriptors == 0
    workspace.close()


def test_P13_T05_local_registry_count_and_charges_are_bounded():
    workspace = LocalWorkspace(20000, 15000, 0, max_leases=1)
    lease = workspace.acquire('one', 1)
    with pytest.raises(ResourceError, match='registry is full'):
        workspace.acquire('two', 1)
    lease.release()
    workspace.close()
    workspace = LocalWorkspace(4096, 4096)
    with pytest.raises(ResourceError):
        workspace.acquire('one', 1)
    assert workspace.active_leases == 0
    workspace.close()


@pytest.mark.parametrize('kwargs', [
    {'workspace_bytes': 0}, {'workspace_bytes': True}, {'metadata_bytes': -1},
    {'metadata_bytes': 10001}, {'metadata_bytes': (64 << 20) + 1},
    {'descriptors': True}, {'max_leases': 0}, {'max_leases': 1025},
])
def test_P13_T03_local_grants_reject_invalid_or_oversized_caps(kwargs):
    values = dict(workspace_bytes=10000, metadata_bytes=5000)
    values.update(kwargs)
    with pytest.raises(ConfigurationError):
        LocalWorkspace(**values)


def test_P13_T05_local_caps_cannot_be_reconfigured():
    workspace = LocalWorkspace(10000, 5000, 1)
    for name in ('limit_bytes', 'metadata_limit_bytes', 'descriptor_limit'):
        with pytest.raises(AttributeError):
            setattr(workspace, name, 1 << 30)
    assert workspace.used_bytes == 0
    workspace.close()


def test_P13_T05_local_context_releases_subleases_on_stage_failure():
    with pytest.raises(IntegrityError):
        with LocalWorkspace(10000, 5000) as workspace:
            with workspace.acquire('stage', 100):
                raise IntegrityError('invalid stage input')
    assert workspace.active_leases == workspace.used_bytes == 0
    with pytest.raises(ResourceError, match='closed'):
        with workspace:
            pass


def test_P13_T05_interrupted_memory_constructor_restores_sender_authority(monkeypatch):
    from readmixer_py.resources import Lease
    budget = Budget(8 << 20)
    source = budget.acquire('sender', len(DATA))
    previous = budget.used_bytes
    active = Lease._active
    def fail_after_transfer(self):
        if self is not source:
            raise KeyboardInterrupt
        return active(self)
    monkeypatch.setattr(Lease, '_active', fail_after_transfer)
    with pytest.raises(KeyboardInterrupt):
        MemoryRef(DATA, source, RUN, 7)
    assert not source.released
    assert budget.active_leases == 1 and budget.used_bytes == previous
    source.release()
    assert budget.used_bytes == 0


@pytest.mark.parametrize('payload', [bytearray(DATA), memoryview(DATA), 'PRIVATE'])
def test_P13_T03_memory_ref_rejects_mutable_or_nonbinary_payload(payload):
    budget = Budget(8 << 20)
    source = budget.acquire('sender', len(DATA))
    with pytest.raises(ConfigurationError):
        MemoryRef(payload, source, RUN, 7)
    assert not source.released
    source.release()
    assert budget.used_bytes == 0
