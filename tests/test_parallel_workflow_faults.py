"""P14-T08: reached-stage failures in the real spawned production mix path."""
from dataclasses import replace
from functools import partial
import gc
import multiprocessing
import os

import psutil
import pytest

from readmixer_py import mixing, stage_scheduler
from readmixer_py.artifacts.bundles import BundleEmitter
from readmixer_py.config import MixConfig, StorageMode
from readmixer_py.crypto import OperationKeys
from readmixer_py.errors import CancelledError, IntegrityError, ResourceError
from readmixer_py.execution import CpuObservations, resolve_cpu_budget
from readmixer_py.shuffle import SerialShuffler, bucket_index
from readmixer_py.stage_policy import SortPolicy
from test_pipeline import fixtures

_SECRET = 'PRIVATE_FAULT_PAYLOAD_DO_NOT_REPORT'


def _checked_fault_worker(sock, run_id, max_job_bytes, workspace_bytes, *, phase, failure, reached, **kwargs):
    """Top-level spawn target; every injection follows a proved stage checkpoint."""
    from readmixer_py import worker_bootstrap
    from readmixer_py.fastq import PairedReader
    from readmixer_py.worker_scatter import WorkerScatterGeneration

    def fail():
        reached.set()
        if failure == 'exit':
            os._exit(23)
        raise IntegrityError(_SECRET)

    if phase == 'parser':
        original = PairedReader.next_pair
        def next_pair(reader, *args, **options):
            pair = original(reader, *args, **options)
            if pair is not None:
                assert pair.original_pair_index == 0
                fail()  # both input mates opened and one normalized pair decoded
            return pair
        PairedReader.next_pair = next_pair
    elif phase == 'scatter':
        original = WorkerScatterGeneration.finish
        def finish(generation, callback):
            assert generation.pairs > 0
            def handed_off(bucket, maximum, result, count):
                answer = callback(bucket, maximum, result, count)
                if count:
                    assert result.physical_bytes > 0
                    fail()  # encrypted output authenticated and ownership ACK returned
                return answer
            return original(generation, handed_off)
        WorkerScatterGeneration.finish = finish
    else:
        original = worker_bootstrap.execute_policy
        def execute(payload, policy, *, checkpoint):
            result = original(payload, policy, checkpoint=checkpoint)
            matched = (policy.kind is SortPolicy.TERMINAL_RANK if phase == 'terminal'
                       else policy.kind is SortPolicy.BUCKET_FRAMES and bool(policy.path))
            if matched:
                assert result[1] > 0
                fail()  # closed policy completed its native terminal/refinement work
            return result
        worker_bootstrap.execute_policy = execute
    worker_bootstrap.worker_main(sock, run_id, max_job_bytes, workspace_bytes, **kwargs)


def _observe_invocation(monkeypatch):
    schedulers, pids = [], set()
    original = stage_scheduler.StageScheduler._launch_partition
    def launch(scheduler):
        result = original(scheduler)
        schedulers.append(scheduler)
        pids.update(scheduler.child_pids)
        return result
    monkeypatch.setattr(stage_scheduler.StageScheduler, '_launch_partition', launch)
    monkeypatch.setattr(mixing, 'resolve_cpu_budget', lambda workers: resolve_cpu_budget(workers,
        observations=CpuObservations(logical_cpus=4, affinity_cpus=4)))
    return schedulers, pids


def _paths(tmp_path, counts):
    inputs, destination, scratch = [tmp_path / name for name in ('private-inputs', 'output', 'scratch')]
    fixtures(inputs, counts=counts, metadata=13, sequence=40)
    destination.mkdir(mode=0o700)
    scratch.mkdir(mode=0o700)
    (destination / 'foreign').write_bytes(b'preserve foreign output')
    original = {path.name: path.read_bytes() for path in inputs.iterdir()}
    return inputs, destination, scratch, original


def _assert_cleanup(inputs, destination, scratch, source, schedulers, pids, descriptors):
    assert {path.name: path.read_bytes() for path in inputs.iterdir()} == source
    assert {path.name: path.read_bytes() for path in destination.iterdir()} == {'foreign': b'preserve foreign output'}
    assert not list(scratch.iterdir())
    assert pids and all(not psutil.pid_exists(pid) for pid in pids)
    assert schedulers and all(scheduler._closed for scheduler in schedulers)
    assert all(scheduler.budget.active_leases == 0 for scheduler in schedulers)
    assert psutil.Process().num_fds() == descriptors


@pytest.mark.parametrize('phase', ['parser', 'scatter', 'terminal', 'refinement'])
@pytest.mark.parametrize('failure', ['typed', 'exit'])
def test_P14_T08_reached_worker_failure_prevents_publication_and_reaps(tmp_path, monkeypatch, phase, failure):
    context = multiprocessing.get_context('spawn')
    reached = context.Event()  # Start interpreter-owned helper before FD baseline.
    monkeypatch.setattr(stage_scheduler, 'worker_main', partial(_checked_fault_worker,
        phase=phase, failure=failure, reached=reached))
    schedulers, pids = _observe_invocation(monkeypatch)
    counts = (180, 140, 0) if phase == 'refinement' else (17, 11, 0)
    inputs, destination, scratch, source = _paths(tmp_path, counts)
    keys = OperationKeys._from_test(bytes(range(32)), bytes(range(16)))
    if phase == 'refinement':
        # The first canonical bucket must refine before any terminal is queued,
        # ensuring this assertion exercises a spawned BUCKET_FRAMES policy.
        assert sum(bucket_index(keys.bucket_key(()), sample, ordinal, 64) == 0
                   for sample, count in enumerate(counts) for ordinal in range(count)) > 1
        original = SerialShuffler.__init__
        def tiny_terminals(shuffler, *args, **kwargs):
            original(shuffler, *args, **kwargs)
            shuffler._limits = replace(shuffler._limits, pairs=1)
        monkeypatch.setattr(SerialShuffler, '__init__', tiny_terminals)
    # Drop earlier tests' unreachable Event/traceback cycles before counting
    # their semaphore descriptors; this case still strongly owns `reached`.
    gc.collect()
    descriptors = psutil.Process().num_fds()
    expected = IntegrityError if failure == 'typed' else ResourceError
    with pytest.raises(expected) as caught:
        mixing._mix(MixConfig(input_dir=inputs, output=destination, temp_dir=scratch,
            max_ram=2 << 30, workers=2, storage=StorageMode.EXTERNAL, max_record_bytes=4096),
            _keys=keys, _scatter_strategy='direct', _scatter_batch_bytes=1 << 20)
    assert reached.is_set(), f'{phase} checkpoint was never reached'
    assert _SECRET not in str(caught.value)
    assert str(inputs) not in str(caught.value)
    _assert_cleanup(inputs, destination, scratch, source, schedulers, pids, descriptors)


def test_P14_T08_emission_cancellation_with_live_workers_preserves_primary_and_cleanup(tmp_path, monkeypatch):
    # Ensure this test's descriptor baseline includes the interpreter-owned
    # multiprocessing helper even when run by itself.
    helper = multiprocessing.get_context('spawn').Event()
    schedulers, pids = _observe_invocation(monkeypatch)
    inputs, destination, scratch, source = _paths(tmp_path, (37, 29, 0))
    cancelled, reached = [False], []
    original = BundleEmitter.append
    def append(emitter, pair, **kwargs):
        answer = original(emitter, pair, **kwargs)
        if not reached:
            scheduler = schedulers[-1]
            assert scheduler.pending_jobs > 0
            assert scheduler.process_slots == 2
            assert all(psutil.pid_exists(pid) for pid in scheduler.child_pids)
            reached.append((scheduler.pending_jobs, emitter.pairs))
            cancelled[0] = True
        return answer
    monkeypatch.setattr(BundleEmitter, 'append', append)
    # Exclude earlier Event cycles while keeping this case's helper alive.
    gc.collect()
    descriptors = psutil.Process().num_fds()
    with pytest.raises(CancelledError) as caught:
        mixing._mix(MixConfig(input_dir=inputs, output=destination, temp_dir=scratch,
            max_ram=2 << 30, workers=4, storage=StorageMode.EXTERNAL, max_record_bytes=4096,
            cancel=lambda: cancelled[0]),
            _keys=OperationKeys._from_test(bytes(range(32)), bytes(range(16))),
            _scatter_strategy='direct', _scatter_batch_bytes=1 << 20)
    assert reached and reached[0][1] == 1
    assert str(inputs) not in str(caught.value)
    _assert_cleanup(inputs, destination, scratch, source, schedulers, pids, descriptors)
    assert not helper.is_set()
