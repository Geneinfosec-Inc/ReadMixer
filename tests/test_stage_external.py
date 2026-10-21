"""Real spawn integration for coordinator-owned encrypted batch handoffs."""
import hashlib
import struct

import psutil
import pytest

from readmixer_py.crypto import OperationKeys, pairs_identity
from readmixer_py.execution import CpuObservations, resolve_cpu_budget
from readmixer_py.resources import Budget
from readmixer_py.stage_scheduler import StageScheduler, StageSegmentResult
from readmixer_py.storage import SegmentStore


def frame(ordinal, sample=0):
    fields = (b'@synthetic/1', b'A' * 31, b'+opaque', b'!' * 31,
              b'@synthetic/2', b'C' * 31, b'+opaque', b'#' * 31)
    return struct.pack('<IIQ8I', 48 + sum(map(len, fields)), sample, ordinal,
                       *map(len, fields)) + b''.join(fields)


def source(store, ordinal, payload):
    with store.new_segment(1, pairs_identity((), 0, ordinal)) as writer:
        writer.append(payload, item_count=2)
        return writer.seal()


def read(store, ref):
    with store.open_verified(ref) as reader:
        return reader.read(ref.decoded_bytes)


@pytest.mark.parametrize('mode', ['external', 'auto'])
@pytest.mark.parametrize('cpus', [1, 2, 3])
def test_P13_T04_T05_protected_sort_handoff_and_caller_lifetime(tmp_path, mode, cpus):
    budget = Budget(1024 << 20, descriptors=256)
    keys = OperationKeys.new()
    plan = resolve_cpu_budget(observations=CpuObservations(cpus))
    with SegmentStore(mode, budget, keys, temp_dir=tmp_path) as store:
        results = []
        refs = []
        with StageScheduler(budget, plan, keys.run_id, store=store, task_count=4,
                            max_job_bytes=4096) as scheduler:
            pids = scheduler.child_pids
            for turn in range(3):
                payload = frame(turn, 1) + frame(turn, 0)
                ref = source(store, turn, payload)
                refs.append(ref)
                scheduler.submit_segment(ref)
                result = scheduler.take_next()
                results.append(result)
                assert isinstance(result, StageSegmentResult)
                assert result.records == 2
                assert result.ref.digest == hashlib.sha256(frame(turn, 0) + frame(turn, 1)).digest()
                assert store.is_external(result.ref)
            assert scheduler.pending_jobs == 0
        assert all(not psutil.pid_exists(pid) for pid in pids)
        for turn, result in enumerate(results):
            assert read(store, result.ref) == frame(turn, 0) + frame(turn, 1)
            result.close()
        for turn, ref in enumerate(refs):
            assert read(store, ref) == frame(turn, 1) + frame(turn, 0)
            store.drop(ref)
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
    assert not list(tmp_path.iterdir())


def test_P13_T04_completed_external_results_cancelled_before_drain(tmp_path):
    budget = Budget(768 << 20, descriptors=128)
    keys = OperationKeys.new()
    with SegmentStore('external', budget, keys, temp_dir=tmp_path) as store:
        ref = source(store, 0, frame(1) + frame(0))
        with StageScheduler(budget, resolve_cpu_budget(observations=CpuObservations(2)),
                            keys.run_id, store=store, max_job_bytes=4096) as scheduler:
            scheduler.submit_segment(ref)
            while not scheduler._jobs[0].complete:
                scheduler._service()
            output = scheduler._jobs[0].output
            scheduler.abort()
            assert output.segment_id not in store._entries
            assert not (store.scratch_directory / store._name(output)).exists()
        assert read(store, ref) == frame(1) + frame(0)
        store.drop(ref)
    assert budget.used_bytes == budget.used_descriptors == 0
