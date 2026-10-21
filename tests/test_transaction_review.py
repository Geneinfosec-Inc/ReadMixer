"""Review regressions for ownership cleanup interrupted around refund handoff."""

import pytest

from readmixer_py.disk import DiskBudget, DiskRefund
from readmixer_py.errors import ResourceError
from readmixer_py.resources import Budget
from readmixer_py.transaction import OutputTransaction


@pytest.mark.parametrize('path', ['active', 'marker'])
@pytest.mark.parametrize('boundary', ['before', 'after'])
def test_P12_T05_active_and_marker_refunds_survive_interrupted_handoff(tmp_path, monkeypatch, path, boundary):
    budget = Budget(64 << 20)
    disk = DiskBudget(budget=budget)
    tx = OutputTransaction.begin(tmp_path, 'manifest', budget, disk_budget=disk)
    payload = b'public symbolic bytes'
    interruption = KeyboardInterrupt()
    calls = []
    original = DiskRefund.commit

    def interrupted(token):
        calls.append(token)
        if len(calls) == 1:
            if boundary == 'after':
                original(token)
            raise interruption
        return original(token)

    monkeypatch.setattr(DiskRefund, 'commit', interrupted)
    if path == 'active':
        # No inventory record owns this live writer yet.
        artifact = tx.create_partial('unregistered')
        artifact.write(payload)
        with pytest.raises(KeyboardInterrupt) as caught:
            tx.abort()
        assert caught.value is interruption
    else:
        # Fail at the real marker-linked, pre-durable-commit boundary. Cleanup
        # removes both links before debiting their single physical byte charge.
        primary = ResourceError('injected precommit failure')
        original_poll = tx._poll
        def poll():
            original_poll()
            if tx._completion is not None and tx._completion._linked:
                raise primary
        monkeypatch.setattr(tx, '_poll', poll)
        with pytest.raises(ResourceError) as caught:
            tx.publish_all(payload)
        assert caught.value is primary
        assert 'output cleanup also failed; primary error preserved' in primary.__notes__
    assert len(calls) == 2 and calls[0] is calls[1]
    assert calls[0].done
    assert not list(tmp_path.iterdir())
    assert disk.committed_bytes == disk.pending_bytes == 0
    assert tx._resources_closed
    disk.close()
    assert budget.used_bytes == budget.used_descriptors == budget.used_metadata_bytes == 0


def test_P12_T05_abandon_releases_pending_allocation_outside_registry(tmp_path, monkeypatch):
    from readmixer_py.crypto import OperationKeys, observations_identity
    from readmixer_py.storage import SegmentStore, SegmentWriter

    class RejectInsertion(dict):
        def __setitem__(self, key, value):
            raise MemoryError('injected registry allocation failure')

    budget = Budget(64 << 20)
    store = SegmentStore('memory', budget, OperationKeys.new(), temp_dir=tmp_path)
    store._entries = RejectInsertion()
    provided = budget.acquire('pending-writer-work', 16384)
    with monkeypatch.context() as fault:
        def fail_abort(self):
            raise ResourceError('injected pending allocation cleanup failure')
        fault.setattr(SegmentWriter, 'abort', fail_abort)
        with pytest.raises(MemoryError):
            store.new_segment(5, observations_identity('verify', 0), lease=provided)
    pending = store._pending_allocation
    assert pending is not None and not store._entries
    assert not pending.entry.lease.released
    # Terminal disposal cannot infer/decode unknown files, but it must release
    # this fully initialized owner even though registry insertion never finished.
    store.abandon()
    assert store._closed and store._pending_allocation is None
    assert pending.entry.lease.released
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
    assert budget.active_leases == 0
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize('operation', ['mix', 'recover'])
@pytest.mark.parametrize('boundary', ['yield', 'caller'])
def test_P12_T03_workflow_transaction_handoff_interrupt_has_no_unowned_gap(tmp_path, monkeypatch, operation, boundary):
    import inspect
    import sys
    import readmixer_py.mixing as mixing
    import readmixer_py.recovery as recovery
    from readmixer_py.config import MixConfig, RecoverConfig, StorageMode

    inputs = tmp_path / 'inputs'
    inputs.mkdir()
    for sample in ('alpha', 'beta'):
        for mate in (1, 2):
            (inputs / f'{sample}_R{mate}.fq').write_bytes(b'@artificial\nXY\n+opaque\n!!\n')
    options = dict(max_ram=512 << 20, storage=StorageMode.EXTERNAL, workers=1)
    destination = tmp_path / 'interrupted-output'
    if operation == 'mix':
        module = mixing
        implementation = inspect.unwrap(mixing._mix)
        action = lambda: mixing.mix(MixConfig(input_dir=inputs, output=destination, **options))
    else:
        pool = tmp_path / 'pool'
        mixed = mixing.mix(MixConfig(input_dir=inputs, output=pool, **options))
        module = recovery
        implementation = inspect.unwrap(recovery.recover)
        action = lambda: recovery.recover(RecoverConfig(manifest=pool / mixed.manifest,
            output=destination, sample=('alpha', 'beta'), **options))
    if boundary == 'yield':
        implementation = inspect.unwrap(OutputTransaction.operation.__func__)
        statement = 'yield transaction'
    else:
        statement = 'store = transaction.store'
    lines, first = inspect.getsourcelines(implementation)
    target = next(first + index for index, line in enumerate(lines) if line.strip() == statement)
    budgets = []
    original = module.operation_resources
    def resources(*args, **kwargs):
        plan, budget = original(*args, **kwargs)
        budgets.append(budget)
        return plan, budget
    monkeypatch.setattr(module, 'operation_resources', resources)
    interruption = KeyboardInterrupt()
    reached = []
    def trace(frame, event, arg):
        if frame.f_code is implementation.__code__ and event == 'line' and frame.f_lineno == target:
            reached.append(statement)
            raise interruption
        return trace
    previous = sys.gettrace()
    try:
        sys.settrace(trace)
        with pytest.raises(KeyboardInterrupt) as caught:
            action()
    finally:
        sys.settrace(previous)
    assert caught.value is interruption and reached == [statement]
    assert len(budgets) == 1
    budget = budgets[0]
    assert budget.active_leases == budget.used_metadata_bytes == budget.used_descriptors == 0
    assert budget.used_bytes == budget.baseline_bytes
    assert destination.is_dir() and not list(destination.iterdir())
    assert not list(tmp_path.rglob('rmpy-stage-*'))
    assert not list(tmp_path.rglob('readmixer-scratch-*'))


@pytest.mark.parametrize('boundary', ['before', 'after'])
@pytest.mark.parametrize('detached', [False, True])
def test_P12_T05_ciphertext_missing_file_retry_finishes_one_refund(tmp_path, monkeypatch, boundary, detached):
    from readmixer_py.crypto import OperationKeys, observations_identity
    from readmixer_py.storage import SegmentStore

    budget = Budget(64 << 20)
    disk = DiskBudget(budget=budget)
    store = SegmentStore('external', budget, OperationKeys.new(), temp_dir=tmp_path, disk_budget=disk)
    with store.new_segment(5, observations_identity('verify', 0)) as writer:
        writer.append(b'public artificial ciphertext fixture')
        reference = writer.seal()
    if detached:
        reference = store.detach(reference)
    physical = reference.physical_bytes
    assert physical > 0 and disk.committed_bytes == physical
    original = DiskRefund.commit
    interruption = KeyboardInterrupt()
    calls = []
    def interrupted(token):
        calls.append(token)
        if len(calls) == 1:
            if boundary == 'after':
                original(token)
            raise interruption
        return original(token)
    monkeypatch.setattr(DiskRefund, 'commit', interrupted)
    with pytest.raises(KeyboardInterrupt) as caught:
        store.drop(reference)
    assert caught.value is interruption
    assert not list(store.scratch_directory.iterdir())  # Unlink completed first.
    assert store._pending_delete is not None
    assert disk.committed_bytes == (physical if boundary == 'before' else 0)
    store.drop(reference, missing_ok=True)
    assert len(calls) == 2 and calls[0] is calls[1] and calls[0].done
    assert store._pending_delete is None
    assert store._detached_count == 0
    assert disk.committed_bytes == disk.pending_bytes == 0
    store.close()
    disk.close()
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
    assert budget.active_leases == 0
    assert not list(tmp_path.iterdir())
