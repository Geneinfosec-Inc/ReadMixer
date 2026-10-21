"""Real CLI signals cannot orphan a newly created worker scratch descriptor."""
import dis
import inspect
import os
import signal
import sys

import psutil
import pytest

from readmixer_py import cli
from readmixer_py._cli_cancellation import resource_transition
from readmixer_py.crypto import OperationKeys, observations_identity
from readmixer_py.errors import CancelledError, IntegrityError
from readmixer_py.resources import Budget
from readmixer_py.storage import SegmentStore
from readmixer_py.worker_storage import WorkerStorage


@pytest.mark.parametrize('signum', [signal.SIGINT, signal.SIGTERM], ids=['SIGINT', 'SIGTERM'])
@pytest.mark.parametrize('boundary', ['open-return', 'identity-return', 'close-return'])
def test_real_cli_signal_during_worker_write_creation_retains_cleanup_owner(
        tmp_path, monkeypatch, signum, boundary):
    budget = Budget(128 << 20)
    keys = OperationKeys._from_test(bytes(range(32)), bytes(range(16)))
    store = SegmentStore('auto', budget, keys, temp_dir=tmp_path)
    manager = WorkerStorage(store, max_pending=1)
    store._ensure_directory()
    before_fds = psutil.Process().num_fds()
    previous = {value: signal.getsignal(value) for value in (signal.SIGINT, signal.SIGTERM)}
    original_open, original_fstat, original_close = os.open, os.fstat, os.close
    previous_trace = sys.gettrace()
    opened, sent = [], []
    lines, start = inspect.getsourcelines(WorkerStorage.issue_write)
    line_text, variable = {
        'open-return': ('fd = os.open(', 'fd'),
        'identity-return': ('info = os.fstat(fd)', 'info'),
        'close-return': ('fd = None', 'fd'),
    }[boundary]
    matching = [start + index for index, text in enumerate(lines)
                if text.strip().startswith(line_text)]
    # The last fd=None is the close handoff, after the initial sentinel.
    target_line = matching[-1] if boundary == 'close-return' else matching[0]
    instruction = next(item for item in dis.get_instructions(WorkerStorage.issue_write)
        if item.positions.lineno == target_line and item.opname.startswith('STORE_FAST')
        and variable in (item.argval if isinstance(item.argval, tuple) else (item.argval,)))

    def trace(frame, event, argument):
        if frame.f_code is WorkerStorage.issue_write.__code__:
            frame.f_trace_opcodes = True
            if event == 'opcode' and frame.f_lasti == instruction.offset and not sent:
                assert len(opened) == 1
                sent.append(boundary)
                # The actual OS result is on the production frame's stack;
                # interrupt before Python stores its descriptor/identity.
                os.kill(os.getpid(), signum)
        return trace

    def observed_open(name, flags, *args, **kwargs):
        descriptor = original_open(name, flags, *args, **kwargs)
        if str(name).endswith('.segment') and flags & os.O_EXCL:
            facts = original_fstat(descriptor)
            opened.append((descriptor, str(name), (facts.st_dev, facts.st_ino)))
        return descriptor

    try:
        with monkeypatch.context() as patch:
            patch.setattr(os, 'open', observed_open)
            with pytest.raises(CancelledError) as caught:
                with cli._signals():
                    sys.settrace(trace)
                    try:
                        manager.issue_write(0, 1, 5, observations_identity('verify', 0), 16, 1)
                    finally:
                        sys.settrace(previous_trace)
        assert sent == [boundary] and len(opened) == 1
        assert caught.value.signal == signum
        assert manager._creation is not None
        assert not manager._writes
        assert all(signal.getsignal(value) == handler for value, handler in previous.items())
        manager.cleanup(workers_reaped=True)
        assert psutil.Process().num_fds() == before_fds
        assert not list(store.scratch_directory.glob('*.segment'))
        store.close()
        assert budget.active_leases == budget.used_bytes == budget.used_descriptors == 0
        assert not list(tmp_path.iterdir())
    finally:
        sys.settrace(previous_trace)
        # This runs only after the observations above, including a failing
        # regression against the old code. Close only this test's same inode.
        for descriptor, name, identity in opened:
            try:
                facts = original_fstat(descriptor)
            except OSError:
                pass
            else:
                if (facts.st_dev, facts.st_ino) == identity:
                    original_close(descriptor)
            if store.scratch_directory is not None:
                target = store.scratch_directory / name
                try:
                    facts = target.stat()
                except FileNotFoundError:
                    pass
                else:
                    if (facts.st_dev, facts.st_ino) == identity:
                        target.unlink()
        manager.cleanup(workers_reaped=True)
        store.close()


def test_nested_adoption_dispatches_first_signal_before_following_work():
    reached = []
    with pytest.raises(CancelledError) as caught:
        with cli._signals():
            with resource_transition():
                with resource_transition():
                    os.kill(os.getpid(), signal.SIGINT)
                    os.kill(os.getpid(), signal.SIGTERM)
                reached.append('adopted')
            reached.append('following work')
    assert caught.value.signal == signal.SIGINT
    assert reached == ['adopted']
    # A later invocation has no pending request from the first transition.
    with cli._signals(), resource_transition():
        reached.append('fresh invocation')
    assert reached == ['adopted', 'fresh invocation']


def test_adoption_preserves_primary_failure_and_clears_pending_signal():
    primary = IntegrityError('private scratch creation failed')
    with pytest.raises(IntegrityError) as caught:
        with cli._signals(), resource_transition():
            os.kill(os.getpid(), signal.SIGINT)
            raise primary
    assert caught.value is primary
    assert len(primary.__notes__) == 1
    with cli._signals(), resource_transition():
        pass
