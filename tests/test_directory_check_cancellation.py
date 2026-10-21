"""CLI signals during directory revalidation retain every transient FD."""
import dis
import inspect
import os
import signal
import sys

import psutil
import pytest

from readmixer_py import cli, paths
from readmixer_py.errors import CancelledError


@pytest.mark.parametrize('signum', [signal.SIGINT, signal.SIGTERM], ids=['SIGINT', 'SIGTERM'])
@pytest.mark.parametrize('boundary', ['walk-child', 'check-handoff'])
def test_real_cli_signal_during_directory_check_closes_transient_descriptors(
        tmp_path, monkeypatch, signum, boundary):
    directory = paths.open_directory(tmp_path)
    original_open, original_fstat = os.open, os.fstat
    original_trace = sys.gettrace()
    identity = (directory.device, directory.inode)
    handlers = {value: signal.getsignal(value) for value in (signal.SIGINT, signal.SIGTERM)}
    before_fds = psutil.Process().num_fds()
    if boundary == 'walk-child':
        function, prefix, variable = paths._walk, 'child = os.open(', 'child'
    else:
        function, prefix, variable = paths.AnchoredDirectory.check, 'check_fd = _walk(', 'check_fd'
    lines, first = inspect.getsourcelines(function)
    line = next(first + index for index, text in enumerate(lines) if text.strip().startswith(prefix))
    instruction = next(item for item in dis.get_instructions(function)
        if item.positions.lineno == line and item.opname.startswith('STORE_FAST')
        and variable in (item.argval if isinstance(item.argval, tuple) else (item.argval,)))
    opened, sent = [], []

    def observed_open(*args, **kwargs):
        descriptor = original_open(*args, **kwargs)
        facts = original_fstat(descriptor)
        opened.append((descriptor, (facts.st_dev, facts.st_ino)))
        return descriptor

    def trace(frame, event, argument):
        if frame.f_code is function.__code__:
            frame.f_trace_opcodes = True
            if event == 'opcode' and frame.f_lasti == instruction.offset and not sent:
                assert opened
                sent.append(boundary)
                # The real opened FD is on the production frame's stack, before
                # either the walker or its caller has stored its cleanup owner.
                os.kill(os.getpid(), signum)
        return trace

    try:
        with monkeypatch.context() as patch:
            patch.setattr(os, 'open', observed_open)
            with pytest.raises(CancelledError) as caught:
                with cli._signals():
                    sys.settrace(trace)
                    try:
                        directory.check()
                    finally:
                        sys.settrace(original_trace)
        assert sent == [boundary]
        assert caught.value.signal == signum
        assert psutil.Process().num_fds() == before_fds
        assert all(signal.getsignal(value) == handler for value, handler in handlers.items())
        facts = original_fstat(directory.fd)
        assert (facts.st_dev, facts.st_ino) == identity
        directory.check()
        assert not list(tmp_path.iterdir())
    finally:
        sys.settrace(original_trace)
        # Failed old-code controls are measured before closing any test-owned
        # leaked FD; never close the caller's independent directory anchor.
        for descriptor, expected in opened:
            if descriptor == directory.fd:
                continue
            try:
                facts = original_fstat(descriptor)
            except OSError:
                continue
            if (facts.st_dev, facts.st_ino) == expected:
                os.close(descriptor)
        directory.close()
