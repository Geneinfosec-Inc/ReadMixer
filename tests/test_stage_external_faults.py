"""Reached-checkpoint faults in actual spawned encrypted-worker transfers."""
import inspect
import os
import struct
import sys

import psutil
import pytest

from readmixer_py.crypto import OperationKeys, pairs_identity
from readmixer_py.errors import IntegrityError, ResourceError
from readmixer_py.execution import CpuObservations, resolve_cpu_budget
from readmixer_py.resources import Budget
from readmixer_py.stage_scheduler import StageScheduler
from readmixer_py.storage import SegmentStore


def frame(ordinal):
    fields = (b'@private-test/1', b'ACGT', b'+opaque', b'!!!!',
              b'@private-test/2', b'TGCA', b'+opaque', b'####')
    return struct.pack('<IIQ8I', 48 + sum(map(len, fields)), 0, ordinal,
                       *map(len, fields)) + b''.join(fields)


def source(store):
    payload = frame(1) + frame(0)
    with store.new_segment(1, pairs_identity((), 0, 0)) as writer:
        writer.append(payload, item_count=2)
        return writer.seal(), payload


def read(store, ref):
    with store.open_verified(ref) as reader:
        return reader.read(ref.decoded_bytes)


def scheduler(store):
    plan = resolve_cpu_budget(observations=CpuObservations(2))
    return StageScheduler(store.budget, plan, store.keys.run_id, store=store, max_job_bytes=4096)


def assert_clean_stage(stage, store, ref, payload, pids):
    assert stage._closed
    assert all(not psutil.pid_exists(pid) for pid in pids)
    assert read(store, ref) == payload
    assert store.disk_budget.pending_bytes == 0
    assert store.disk_budget.committed_bytes == ref.physical_bytes
    assert not store._reader_pins
    store.drop(ref)
    assert store.disk_budget.committed_bytes == 0


@pytest.mark.parametrize('checkpoint', ['authorize_write', 'commit_write'])
@pytest.mark.parametrize('error_type', [ResourceError, KeyboardInterrupt])
def test_P13_T04_T07_interrupt_after_real_disk_credit_or_commit(tmp_path, monkeypatch, checkpoint, error_type):
    budget = Budget(768 << 20, descriptors=128)
    with SegmentStore('external', budget, OperationKeys.new(), temp_dir=tmp_path) as store:
        ref, payload = source(store)
        stage = scheduler(store)
        pids = stage.child_pids
        manager = stage.storage
        original = getattr(manager, checkpoint)
        primary = error_type('injected worker disk checkpoint')
        reached = []
        def fail_after(grant, offset, amount):
            original(grant, offset, amount)
            state = manager._writes[(grant.task_id, grant.grant_id)]
            if checkpoint == 'authorize_write':
                assert state.ticket is not None and state.ticket._active
                assert store.disk_budget.pending_bytes >= amount
            else:
                assert state.ticket is None and state.committed == offset + amount
                assert manager._info(grant.ref).st_size == offset + amount
            reached.append((offset, amount))
            raise primary
        monkeypatch.setattr(manager, checkpoint, fail_after)
        try:
            with pytest.raises(error_type) as caught:
                stage.submit_segment(ref)
                stage.take_next()
            assert caught.value is primary
            assert len(reached) == 1 and reached[0][0] == 0 and reached[0][1] > 0
            assert stage.cancellation.error is primary
            assert_clean_stage(stage, store, ref, payload, pids)
        finally:
            stage.abort()
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
    assert not list(tmp_path.iterdir())


def test_P13_T04_T07_unexpected_child_exit_with_outstanding_disk_credit(tmp_path, monkeypatch):
    budget = Budget(768 << 20, descriptors=128)
    with SegmentStore('external', budget, OperationKeys.new(), temp_dir=tmp_path) as store:
        ref, payload = source(store)
        stage = scheduler(store)
        pids = stage.child_pids
        original = stage.storage.authorize_write
        reached = []
        def kill_after_credit(grant, offset, amount):
            original(grant, offset, amount)
            assert store.disk_budget.pending_bytes >= amount
            reached.append((offset, amount))
            process = stage._workers[0].process
            process.kill()
            process.join(timeout=3)
            assert process.exitcode is not None
        monkeypatch.setattr(stage.storage, 'authorize_write', kill_after_credit)
        try:
            with pytest.raises(ResourceError) as caught:
                stage.submit_segment(ref)
                stage.take_next()
            assert reached and reached[0][0] == 0
            assert 'exited unexpectedly' in str(caught.value)
            assert_clean_stage(stage, store, ref, payload, pids)
        finally:
            stage.abort()
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
    assert not list(tmp_path.iterdir())


def test_P13_T04_worker_result_corruption_is_rejected_before_acceptance(tmp_path, monkeypatch):
    budget = Budget(768 << 20, descriptors=128)
    with SegmentStore('external', budget, OperationKeys.new(), temp_dir=tmp_path) as store:
        ref, payload = source(store)
        stage = scheduler(store)
        pids = stage.child_pids
        original = stage.storage.accept_write
        reached = []
        def corrupt_before_verify(grant, result):
            path = store.scratch_directory / store._name(result)
            assert path.stat().st_size == result.physical_bytes
            assert stage.storage._writes[(grant.task_id, grant.grant_id)].ticket is None
            data = bytearray(path.read_bytes())
            data[-1] ^= 1
            path.write_bytes(data)
            reached.append(result.segment_id)
            return original(grant, result)
        monkeypatch.setattr(stage.storage, 'accept_write', corrupt_before_verify)
        try:
            with pytest.raises(IntegrityError, match='authentication') as caught:
                stage.submit_segment(ref)
                stage.take_next()
            assert len(reached) == 1
            assert reached[0] not in store._entries
            assert 'private-test' not in str(caught.value)
            assert_clean_stage(stage, store, ref, payload, pids)
        finally:
            stage.abort()
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
    assert not list(tmp_path.iterdir())


def test_P13_T04_T05_interrupted_external_return_after_ownership_transfer(tmp_path):
    budget = Budget(768 << 20, descriptors=128)
    with SegmentStore('external', budget, OperationKeys.new(), temp_dir=tmp_path) as store:
        ref, payload = source(store)
        stage = scheduler(store)
        pids = stage.child_pids
        stage.submit_segment(ref)
        lines, first = inspect.getsourcelines(StageScheduler.take_next)
        target = next(first + i for i, line in enumerate(lines) if line.strip() == 'job.output = None')
        primary = KeyboardInterrupt('injected after external ownership transfer')
        reached = []
        def trace(frame, event, arg):
            if frame.f_code is StageScheduler.take_next.__code__ and event == 'line' and frame.f_lineno == target:
                job = frame.f_locals['job']
                assert (job.task, job.grant) not in stage.storage._writes
                assert frame.f_locals['result'].ref.segment_id in store._entries
                reached.append(job.output.segment_id)
                raise primary
            return trace
        previous = sys.gettrace()
        try:
            sys.settrace(trace)
            with pytest.raises(KeyboardInterrupt) as caught:
                stage.take_next()
        finally:
            sys.settrace(previous)
        try:
            assert reached and caught.value is primary
            assert reached[0] not in store._entries
            assert_clean_stage(stage, store, ref, payload, pids)
        finally:
            stage.abort()
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
    assert not list(tmp_path.iterdir())


def test_P13_T04_foreign_replacement_is_preserved_until_owned_inode_restored(tmp_path, monkeypatch):
    budget = Budget(768 << 20, descriptors=128)
    foreign_contents = b'foreign file must survive uncertain cleanup'
    preserved = tmp_path / 'foreign-preserved'
    with SegmentStore('external', budget, OperationKeys.new(), temp_dir=tmp_path) as store:
        ref, payload = source(store)
        stage = scheduler(store)
        pids = stage.child_pids
        original = stage.storage.accept_write
        reached = []
        def replace_before_verify(grant, result):
            path = store.scratch_directory / store._name(result)
            backup = tmp_path / 'owned-worker-inode'
            os.rename(path, backup)
            path.write_bytes(foreign_contents)
            path.chmod(0o600)
            assert (path.stat().st_dev, path.stat().st_ino) != (result.device, result.inode)
            reached.append((path, backup))
            return original(grant, result)
        monkeypatch.setattr(stage.storage, 'accept_write', replace_before_verify)
        try:
            with pytest.raises(IntegrityError, match='identity|replaced|ownership|changed'):
                stage.submit_segment(ref)
                stage.take_next()
            assert len(reached) == 1
            path, backup = reached[0]
            assert path.read_bytes() == foreign_contents
            assert all(not psutil.pid_exists(pid) for pid in pids)
            assert not stage._closed  # uncertain ownership remains for retry
            assert budget.used_bytes > 0
            assert read(store, ref) == payload
            os.rename(path, preserved)
            os.rename(backup, path)
            stage.abort()
            assert_clean_stage(stage, store, ref, payload, pids)
        finally:
            # Restore the exact owned inode before retrying cleanup, including
            # when a regression assertion fails. Never delete the foreign file.
            if reached:
                path, backup = reached[0]
                if backup.exists():
                    if path.exists():
                        os.rename(path, preserved)
                    os.rename(backup, path)
            stage.abort()
    assert preserved.read_bytes() == foreign_contents
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
    assert list(tmp_path.iterdir()) == [preserved]


def test_P13_T04_T05_interrupted_return_retains_owner_when_result_cleanup_also_fails(tmp_path, monkeypatch):
    budget = Budget(768 << 20, descriptors=128)
    with SegmentStore('external', budget, OperationKeys.new(), temp_dir=tmp_path) as store:
        ref, payload = source(store)
        stage = scheduler(store)
        pids = stage.child_pids
        stage.submit_segment(ref)
        lines, first = inspect.getsourcelines(StageScheduler.take_next)
        target = next(first + i for i, line in enumerate(lines) if line.strip() == 'job.output = None')
        primary = KeyboardInterrupt('injected returned-owner handoff interruption')
        reached, failed_cleanup = [], []
        original_drop = store.drop
        def fail_drop_once(output, **kwargs):
            if reached and output.segment_id == reached[0] and not failed_cleanup:
                # Return-owner cleanup is downstream of worker reaping even
                # when caller construction/return is the initiating failure.
                assert all(not psutil.pid_exists(pid) for pid in pids)
                failed_cleanup.append(output.segment_id)
                raise ResourceError('injected returned-owner cleanup failure')
            return original_drop(output, **kwargs)
        monkeypatch.setattr(store, 'drop', fail_drop_once)
        def trace(frame, event, arg):
            if frame.f_code is StageScheduler.take_next.__code__ and event == 'line' and frame.f_lineno == target:
                job = frame.f_locals['job']
                assert (job.task, job.grant) not in stage.storage._writes
                reached.append(job.output.segment_id)
                raise primary
            return trace
        previous = sys.gettrace()
        try:
            sys.settrace(trace)
            with pytest.raises(KeyboardInterrupt) as caught:
                stage.take_next()
        finally:
            sys.settrace(previous)
        try:
            assert reached == failed_cleanup
            assert caught.value is stage.cancellation.error is primary
            assert not stage._closed and stage._pending_return is not None
            assert reached[0] in store._entries
            assert budget.used_bytes > 0
            assert all(not psutil.pid_exists(pid) for pid in pids)
            stage.abort()
            assert stage._pending_return is None
            assert_clean_stage(stage, store, ref, payload, pids)
        finally:
            monkeypatch.setattr(store, 'drop', original_drop)
            stage.abort()
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
    assert not list(tmp_path.iterdir())
