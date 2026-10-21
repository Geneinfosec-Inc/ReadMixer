"""Concurrent launch-boundary checks with fake starts, not overlapping spawns."""
import os
from threading import Event, Thread

import pytest

from readmixer_py import stage_scheduler


@pytest.mark.parametrize('failed_start', ['first', 'second'])
def test_P13_T01_concurrent_launches_serialize_and_restore_environment(
        monkeypatch, failed_start):
    watchdog = 10
    names = ('READMIXER_TEST_NATIVE_PRESENT', 'READMIXER_TEST_NATIVE_EMPTY',
             'READMIXER_TEST_NATIVE_ABSENT')
    monkeypatch.setattr(stage_scheduler, '_NATIVE_LIMITS', names)
    monkeypatch.setenv(names[0], '7')
    monkeypatch.setenv(names[1], '')
    monkeypatch.delenv(names[2], raising=False)
    original = {name: os.environ.get(name) for name in names}
    limited = dict.fromkeys(names, '1')
    entered = {label: Event() for label in ('first', 'second')}
    release = {label: Event() for label in entered}
    contended = Event()
    restored = []
    outcomes = {}
    failure = OSError('intentional launch-boundary failure')
    launch_lock = stage_scheduler._LAUNCH_LOCK

    def environment():
        return {name: os.environ.get(name) for name in names}

    class ObservedLaunchLock:
        """Observe contention and restoration while retaining the real lock."""

        def __enter__(self):
            if not launch_lock.acquire(blocking=False):
                contended.set()
                assert launch_lock.acquire(timeout=watchdog), 'launch lock stalled'
            return self

        def __exit__(self, *_exc):
            try:
                restored.append(environment())
            finally:
                launch_lock.release()

    monkeypatch.setattr(stage_scheduler, '_LAUNCH_LOCK', ObservedLaunchLock())

    class FakeProcess:
        def __init__(self, label):
            self.label = label

        def start(self):
            assert environment() == limited
            entered[self.label].set()
            assert release[self.label].wait(watchdog), 'fake start stalled'
            assert environment() == limited
            if self.label == failed_start:
                raise failure

    def launch(label):
        try:
            stage_scheduler._start(FakeProcess(label))
        except BaseException as exc:
            outcomes[label] = exc
        else:
            outcomes[label] = None

    threads = [Thread(target=launch, args=(label,), daemon=True) for label in entered]
    started = []
    try:
        threads[0].start()
        started.append(threads[0])
        assert entered['first'].wait(watchdog), 'first start was not reached'
        threads[1].start()
        started.append(threads[1])
        assert contended.wait(watchdog), 'second launch did not contend for the lock'
        assert not entered['second'].is_set()
        assert environment() == limited
        release['first'].set()
        assert entered['second'].wait(watchdog), 'second start was not reached'
        assert environment() == limited
        release['second'].set()
    finally:
        for event in release.values():
            event.set()
        for thread in started:
            thread.join(timeout=watchdog)
        assert all(not thread.is_alive() for thread in started), 'launch thread leaked'

    assert outcomes[failed_start] is failure
    assert outcomes['second' if failed_start == 'first' else 'first'] is None
    assert restored == [original, original]
    assert environment() == original
