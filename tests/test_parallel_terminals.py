"""P14-T03/T04/T05/T06: canonical leaves on real spawned workers."""
from dataclasses import replace
from functools import partial
import multiprocessing
import time
import weakref

import psutil
import pytest

from readmixer_py import stage_scheduler
from readmixer_py.crypto import OperationKeys
from readmixer_py.errors import CancelledError, IntegrityError, RankCollisionError, ResourceError
from readmixer_py.execution import CpuObservations, resolve_cpu_budget
from readmixer_py.resources import Budget
from readmixer_py.shuffle import SerialShuffler
from readmixer_py.stage_policy import SortPolicy
from readmixer_py.stage_scheduler import StageScheduler, StageResult
from readmixer_py.storage import SegmentStore
from test_shuffle import ROOT, RUN_ID, frame, oracle_order


def _gated_terminal_worker(sock, run_id, max_job_bytes, workspace_bytes, *, gate, entered=None, **kwargs):
    from readmixer_py import worker_bootstrap
    original = worker_bootstrap.execute_policy
    first = True
    def execute(payload, policy, *, checkpoint):
        nonlocal first
        if policy is not None and policy.kind is SortPolicy.TERMINAL_RANK:
            if first and multiprocessing.current_process().name.endswith('-0'):
                deadline = time.monotonic() + 15
                if entered is not None:
                    entered.set()
                while not gate.wait(0.005):
                    checkpoint()
                    if time.monotonic() > deadline:
                        raise ResourceError('terminal test gate watchdog expired')
            first = False
        return original(payload, policy, checkpoint=checkpoint)
    worker_bootstrap.execute_policy = execute
    worker_bootstrap.worker_main(sock, run_id, max_job_bytes, workspace_bytes, **kwargs)


def run(path, *, mode='external', cpus=4, batch=4096, refine=False,
        cancel=None, observe=None, terminal_pairs=2):
    counts = (70, 0, 57)
    budget = Budget(2048 << 20, descriptors=256)
    keys = OperationKeys._from_test(ROOT, RUN_ID)
    cpu = resolve_cpu_budget(observations=CpuObservations(cpus))
    pids = ()
    try:
        with SegmentStore(mode, budget, keys, temp_dir=path) as store:
            with StageScheduler(budget, cpu, RUN_ID, store=store, operation='mix',
                    task_count=64, max_job_bytes=8192, production=True,
                    terminal_bytes=128 << 20, cancel=cancel) as scheduler:
                pids = scheduler.child_pids
                assert scheduler.process_slots == (0 if cpus == 1 else max(1, cpus - 2))
                with SerialShuffler(store, terminal_bytes=128 << 20,
                        max_record_bytes=128, sort_run_bytes=8192,
                        scheduler=scheduler, _tag_batch_size=batch, cancel=cancel) as shuffle:
                    if refine:
                        shuffle._limits = replace(shuffle._limits, pairs=terminal_pairs)
                    for sample in reversed(range(len(counts))):
                        for ordinal in reversed(range(counts[sample])):
                            shuffle.append_frame(frame(sample, ordinal))
                    if observe is not None:
                        observe(scheduler, shuffle)
                    actual = []
                    retained = []
                    with shuffle.iter_ordered(counts) as ordered:
                        for pair in ordered:
                            identity = pair.sample_index, pair.original_pair_index
                            assert pair.secure_pair_index == len(actual)
                            assert bytes(pair.frame) == frame(*identity)
                            assert pair.tag == keys.tag(*identity)
                            retained.append(weakref.ref(pair.frame))
                            actual.append(identity)
                            del pair
                    assert all(reference() is None for reference in retained)
                    assert actual == oracle_order(counts, max_pairs=terminal_pairs if refine else 32768)
                    assert shuffle.refinements > 0 if refine else shuffle.refinements == 0
                    assert scheduler.pending_jobs == 0
                    return actual
    finally:
        assert all(not psutil.pid_exists(pid) for pid in pids)
        assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
        assert not list(path.iterdir())


@pytest.mark.parametrize('mode', ['memory', 'external', 'auto'])
@pytest.mark.parametrize('cpus,batch', [(1, 1), (2, 4096), (4, 1)])
def test_P14_T03_T06_real_terminal_output_matches_independent_order_and_tags(tmp_path, mode, cpus, batch):
    run(tmp_path, mode=mode, cpus=cpus, batch=batch)


@pytest.mark.parametrize('mode', ['memory', 'external'])
def test_P14_T04_refinement_preserves_paths_and_all_eight_derived_rank_keys(tmp_path, mode):
    captured = []
    def observe(scheduler, shuffle):
        original = scheduler.submit_segment
        def submit(ref, *, policy=None):
            if policy is not None and policy.kind is SortPolicy.TERMINAL_RANK:
                assert policy.count <= 2
                assert policy.keys == tuple(shuffle.keys.rank_key(policy.path, policy.count, attempt)
                                            for attempt in range(8))
                captured.append(policy.path)
            return original(ref, policy=policy)
        scheduler.submit_segment = submit
    run(tmp_path, mode=mode, refine=True, observe=observe)
    assert captured == sorted(captured)
    assert any(len(path) > 1 for path in captured)


@pytest.mark.parametrize('cancel_blocked', [False, True], ids=['canonical-drain', 'cancel'])
def test_P14_T05_delayed_first_terminal_keeps_bounded_late_results_and_reaps(tmp_path, monkeypatch, cancel_blocked):
    gate = multiprocessing.get_context('spawn').Event()
    monkeypatch.setattr(stage_scheduler, 'worker_main', partial(_gated_terminal_worker, gate=gate))
    observed = []
    cancelled = False
    def observe(scheduler, shuffle):
        original = scheduler.take_next
        def take():
            nonlocal cancelled
            jobs = list(scheduler._jobs.values())
            if not observed and jobs and jobs[0].policy is not None and jobs[0].policy.kind is SortPolicy.TERMINAL_RANK:
                assert len(jobs) == scheduler.capacity == 4
                deadline = time.monotonic() + 15
                while not all(job.result_ack for job in jobs[2:]):
                    scheduler._service()
                    assert time.monotonic() < deadline
                assert not jobs[0].complete
                assert all(job.complete for job in jobs[2:])
                assert scheduler.pending_bytes <= scheduler.byte_ceiling
                assert all(not owner.released for owner in scheduler._slots)
                observed.append(tuple(job.task for job in jobs))
                if cancel_blocked:
                    cancelled = True
                else:
                    gate.set()
            return original()
        scheduler.take_next = take
    try:
        if cancel_blocked:
            with pytest.raises(CancelledError):
                run(tmp_path, mode='external', observe=observe, cancel=lambda: cancelled)
            assert not gate.is_set()
        else:
            run(tmp_path, mode='external', observe=observe, cancel=lambda: cancelled)
        assert len(observed) == 1
    finally:
        gate.set()


def test_P14_T04_corrupt_completed_terminal_is_rejected_and_sources_cleaned(tmp_path):
    observed = []
    def observe(scheduler, shuffle):
        original = scheduler.take_next
        def take():
            job = next(iter(scheduler._jobs.values()))
            terminal = job.policy is not None and job.policy.kind is SortPolicy.TERMINAL_RANK
            result = original()
            if terminal and not observed:
                assert isinstance(result, StageResult)
                # Inject only after scheduler digest/ACK validation: coordinator
                # frame validation must independently reject malformed framing.
                result._data = b'\xff' * 4 + result.data[4:]
                observed.append(result.task_id)
            return result
        scheduler.take_next = take
    with pytest.raises(IntegrityError, match='terminal result frame length'):
        run(tmp_path, mode='memory', observe=observe)
    assert len(observed) == 1


def _collision_worker(sock, run_id, max_job_bytes, workspace_bytes, *, attempts, fail, **kwargs):
    import struct
    from readmixer_py import shuffle, worker_bootstrap
    expected = OperationKeys._from_test(ROOT, RUN_ID)
    keys = tuple(expected.rank_key((7,), 4, attempt) for attempt in range(8))
    def fill(destination, key, **unused):
        attempt = keys.index(key)
        with attempts.get_lock():
            attempts[attempt] += 1
        destination[:] = (bytes(len(destination)) if fail or attempt < 7 else
                          struct.pack('<8Q', 3, 2, 1, 0, 0, 0, 0, 0))
    shuffle._fill_rank_bytes = fill
    worker_bootstrap.worker_main(sock, run_id, max_job_bytes, workspace_bytes, **kwargs)


@pytest.mark.parametrize('fail', [False, True], ids=['eighth-attempt', 'all-collide'])
def test_P14_T04_spawned_rank_retries_entire_batch_all_eight_attempts(monkeypatch, fail):
    from contextlib import nullcontext
    from readmixer_py.stage_policy import StagePolicy
    attempts = multiprocessing.get_context('spawn').Array('i', 8)
    monkeypatch.setattr(stage_scheduler, 'worker_main', partial(_collision_worker, attempts=attempts, fail=fail))
    budget = Budget(1024 << 20, descriptors=128)
    keys = OperationKeys._from_test(ROOT, RUN_ID)
    payload = b''.join(frame(0, index) for index in range(4))
    policy = StagePolicy.terminal((7,), 4, 128, len(frame(0, 0)),
                                 tuple(keys.rank_key((7,), 4, attempt) for attempt in range(8)))
    expected = pytest.raises(RankCollisionError) if fail else nullcontext()
    pids = ()
    with expected:
        with StageScheduler(budget, resolve_cpu_budget(observations=CpuObservations(2)), RUN_ID,
                operation='mix', task_count=1, production=True, terminal_bytes=128 << 20,
                max_job_bytes=8192) as scheduler:
            pids = scheduler.child_pids
            owner = budget.acquire('test-terminal-source', len(payload))
            scheduler.submit(payload, owner, policy=policy)
            with scheduler.take_next() as result:
                assert result.data == b''.join(frame(0, index) for index in reversed(range(4)))
                assert result.records == 4
    assert list(attempts) == [1] * 8
    assert all(not psutil.pid_exists(pid) for pid in pids)
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0


def test_P14_T08_terminal_result_cleanup_failure_retains_owner_for_retry(tmp_path, monkeypatch):
    from readmixer_py.stage_scheduler import StageSegmentResult
    original = StageSegmentResult.close
    original_terminal = SerialShuffler._parallel_terminal
    failed = []
    active = [False]
    def terminal(shuffle, shard, task):
        active[0] = True
        try:
            yield from original_terminal(shuffle, shard, task)
        finally:
            active[0] = False
    monkeypatch.setattr(SerialShuffler, '_parallel_terminal', terminal)
    def close(result):
        if active[0] and not failed:
            failed.append(result.task_id)
            raise ResourceError('injected terminal result cleanup interruption')
        return original(result)
    monkeypatch.setattr(StageSegmentResult, 'close', close)
    with pytest.raises(ResourceError, match='injected terminal result cleanup interruption'):
        run(tmp_path, mode='external')
    assert len(failed) == 1


def test_P14_T05_early_consumer_auto_drain_drops_views_before_next_terminal(tmp_path, monkeypatch):
    original = SerialShuffler._terminal
    references = []
    terminals = []
    def terminal(shuffle, shard):
        assert all(reference() is None for reference in references)
        terminals.append(shard.path)
        for pair in original(shuffle, shard):
            references.append(weakref.ref(pair.frame))
            yield pair
            del pair
    monkeypatch.setattr(SerialShuffler, '_terminal', terminal)
    budget = Budget(512 << 20, descriptors=128)
    with SegmentStore('memory', budget, OperationKeys._from_test(ROOT, RUN_ID), temp_dir=tmp_path) as store:
        with SerialShuffler(store, terminal_bytes=128 << 20, max_record_bytes=128,
                            sort_run_bytes=8192, scatter_strategy='sorter') as shuffle:
            for sample in (0, 1):
                for ordinal in range(20):
                    shuffle.append_frame(frame(sample, ordinal))
            with shuffle.iter_ordered((20, 20)):
                pass  # context exit must drain without retaining an old arena
            assert shuffle.emitted_pairs == 40
    assert len(terminals) > 1 and all(reference() is None for reference in references)
    assert budget.used_bytes == budget.used_descriptors == budget.used_metadata_bytes == 0


def test_P14_T05_refinement_progresses_while_earlier_terminal_is_blocked(tmp_path, monkeypatch):
    context = multiprocessing.get_context('spawn')
    gate, entered = context.Event(), context.Event()
    monkeypatch.setattr(stage_scheduler, 'worker_main',
                        partial(_gated_terminal_worker, gate=gate, entered=entered))
    original = SerialShuffler._refine
    observed = []
    def refine(shuffle, shard):
        scheduler = shuffle.scheduler
        if not observed and scheduler.pending_jobs:
            deadline = time.monotonic() + 15
            while not entered.is_set():
                scheduler._service()
                assert time.monotonic() < deadline
            assert not next(iter(scheduler._jobs.values())).complete
            assert not gate.is_set()
            result = original(shuffle, shard)
            assert not next(iter(scheduler._jobs.values())).complete
            assert result.ledger.root.record_count > 0
            observed.append(shard.path)
            gate.set()
            return result
        return original(shuffle, shard)
    monkeypatch.setattr(SerialShuffler, '_refine', refine)
    try:
        run(tmp_path, mode='external', refine=True, terminal_pairs=3)
        assert observed == [(1,)]  # bucket0 has3pairs, bucket1 has4
    finally:
        gate.set()


@pytest.mark.parametrize('boundary', ['coordinator', 'worker'])
def test_P14_T04_terminal_count_and_arena_must_fit_the_fixed_slice(monkeypatch, boundary):
    from readmixer_py.errors import ConfigurationError
    from readmixer_py.resources import TERMINAL_OVERHEAD
    from readmixer_py.stage_policy import StagePolicy
    budget = Budget(1024 << 20, descriptors=128)
    keys = OperationKeys._from_test(ROOT, RUN_ID)
    payload = b''.join(frame(0, ordinal) for ordinal in range(3))
    terminal_bytes = TERMINAL_OVERHEAD + 512
    assert len(payload) < 512 < len(payload) + 128 * 3
    policy = StagePolicy.terminal((7,), 3, 128, len(frame(0, 0)),
        tuple(keys.rank_key((7,), 3, attempt) for attempt in range(8)))
    if boundary == 'worker':
        # Bypass only the coordinator check; the real child's independent grant
        # validation must still reject the validly framed but over-T terminal.
        original = StageScheduler._policy_limit
        def limit(scheduler, supplied):
            if supplied is not None and supplied.kind is SortPolicy.TERMINAL_RANK:
                return scheduler.terminal_payload_bytes
            return original(scheduler, supplied)
        monkeypatch.setattr(StageScheduler, '_policy_limit', limit)
    owner = budget.acquire('test-terminal-source', len(payload))
    pids = ()
    try:
        expected = IntegrityError if boundary == 'worker' else (ConfigurationError, ResourceError)
        with pytest.raises(expected):
            with StageScheduler(budget, resolve_cpu_budget(observations=CpuObservations(2)), RUN_ID,
                    operation='mix', task_count=1, production=True, terminal_bytes=terminal_bytes,
                    max_job_bytes=8192) as scheduler:
                pids = scheduler.child_pids
                scheduler.submit(payload, owner, policy=policy)
                with scheduler.take_next():
                    pytest.fail('accepted an over-slice terminal')
    finally:
        owner.release()
    assert all(not psutil.pid_exists(pid) for pid in pids)
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
