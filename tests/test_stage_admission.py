"""P13-T02 infrastructure admission; these do not qualify production workers."""
from contextlib import nullcontext
import os
from threading import Thread
from types import SimpleNamespace

import pytest

from readmixer_py import stage_admission as admission
from readmixer_py.errors import ConfigurationError, ResourceError
from readmixer_py.execution import CpuObservations, resolve_cpu_budget
from readmixer_py.resources import Budget, BUDGET_REGISTRY_BYTES, LEASE_REGISTRY_BYTES
from readmixer_py.stage_admission import (
    AggregateResources, ProcessIdentity, ProcessResources, StageAdmission, observe_processes,
)

MIB = 1 << 20


def cpu(count):
    return resolve_cpu_budget(observations=CpuObservations(count))


def stage(budget, count=4, **kwargs):
    options = dict(operation='mix', task_count=100, per_job_bytes=MIB)
    options.update(kwargs)
    return StageAdmission(budget, cpu(count), **options)


def facts(rss, descriptors=0, workers=()):
    return AggregateResources(ProcessResources(ProcessIdentity(os.getpid(), 1.0), rss, descriptors, 1), workers)


def worker(pid=123456789, rss=0, descriptors=0, threads=1):
    return ProcessResources(ProcessIdentity(pid, 2.0), rss, descriptors, threads)


@pytest.mark.parametrize('operation,count,tasks,processes,compression', [
    ('mix', 1, 100, 0, 0), ('recover', 1, 100, 0, 0), ('verify', 1, 100, 0, 0),
    ('mix', 2, 100, 1, 0), ('recover', 3, 100, 1, 1), ('mix', 8, 100, 6, 1),
    ('verify', 8, 100, 7, 0), ('recover', 8, 2, 2, 5), ('mix', 8, 0, 0, 7),
    ('verify', 8, 0, 0, 0), ('mix', 1000, 1000, 64, 32),
])
def test_P13_T02_fixed_partition_and_two_complete_windows(operation, count, tasks, processes, compression):
    budget = Budget(8 << 30)
    with stage(budget, count, operation=operation, task_count=tasks) as current:
        assert current.process_slots == processes
        assert current.workers_effective == 1 + processes
        assert current.compression_target == compression
        assert current.workers_effective + compression <= count
        assert current.job_capacity == 2 * processes == len(current.job_leases)
        assert current.window_bytes == 2 * processes * MIB
        assert all(lease.bytes == MIB and lease.metadata_bytes == 16384 for lease in current.job_leases)
        assert len(current.baseline_leases) == len(current.channel_leases) == processes
        assert all(lease.bytes >= 64 * MIB for lease in current.baseline_leases)
        assert current.baseline_version == 1 and current.baseline_qualified is False
        assert budget.used_metadata_bytes <= budget.metadata_limit_bytes
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0


def default_required(processes):
    # Independent sum of the declared profile, including one control owner,
    # a permanently registered pressure owner, and four lease handles per child.
    control = 16384 + 4096 * processes
    registry = BUDGET_REGISTRY_BYTES + (2 + 4 * processes) * LEASE_REGISTRY_BYTES
    worker_bytes = 64 * MIB + 32768 + 4 * MIB + 262144 + 2 * MIB
    metadata = control + registry + processes * (32768 + 262144 + 2 * 16384)
    return control + registry + processes * worker_bytes, metadata


def test_P13_T02_complete_ram_threshold_exact_and_one_below():
    required, _metadata = default_required(1)
    for allowance, expected in ((required, 1), (required - 1, 0)):
        budget = Budget(allowance)
        with stage(budget, 2) as current:
            assert current.process_slots == expected
            if expected:
                assert budget.used_bytes == allowance
            else:
                assert 'working-set-admission' in current.reduction_reasons
        assert budget.used_bytes == 0


def test_P13_T02_complete_metadata_threshold_exact_and_one_below():
    _required, metadata = default_required(1)
    for allowance, expected in ((metadata, 1), (metadata - 1, 0)):
        budget = Budget(256 * MIB, metadata_limit_bytes=allowance)
        with stage(budget, 2) as current:
            assert current.process_slots == expected
            if expected:
                assert budget.used_metadata_bytes == allowance
        assert budget.used_metadata_bytes == budget.used_bytes == 0


def test_P13_T02_control_metadata_failure_can_reduce_to_serial():
    _required, metadata = default_required(0)
    budget = Budget(256 * MIB, metadata_limit_bytes=metadata)
    with stage(budget, 64) as current:
        assert current.process_slots == 0
        assert budget.used_metadata_bytes == metadata
    assert budget.used_bytes == 0


@pytest.mark.parametrize('descriptors,expected', [(24, 2), (23, 1), (12, 1), (11, 0), (0, 0)])
def test_P13_T02_descriptor_capacity_reduces_before_start(descriptors, expected):
    budget = Budget(512 * MIB, descriptors=descriptors)
    with stage(budget, 4) as current:
        assert current.process_slots == expected
        assert budget.used_descriptors == 12 * expected
    assert budget.used_descriptors == 0


def test_P13_T02_ready_grows_baseline_once_without_charging_workspace_twice():
    budget = Budget(256 * MIB)
    with stage(budget, 2) as current:
        before = budget.used_bytes
        job_bytes = tuple(lease.bytes for lease in current.job_leases)
        current.observe_ready(0, 65 * MIB, 9, 1)
        assert current.ready_count == 1
        assert budget.used_bytes == before + MIB
        assert current.baseline_leases[0].descriptors == 9
        assert tuple(lease.bytes for lease in current.job_leases) == job_bytes
        current.observe_ready(0, 60 * MIB, 4, 1)
        assert budget.used_bytes == before + MIB
        current.seal_ready()
        with pytest.raises(ResourceError, match='after job'):
            current.observe_ready(0, 70 * MIB, 9, 1)
    assert budget.used_bytes == 0


def test_P13_T02_unready_or_multithreaded_child_cannot_enter_jobs():
    budget = Budget(256 * MIB)
    with stage(budget, 2) as current:
        before = budget.used_bytes
        with pytest.raises(ResourceError, match='must be ready'):
            current.seal_ready()
        with pytest.raises(ResourceError, match='native-thread'):
            current.observe_ready(0, 65 * MIB, 8, 2)
        assert current.ready_count == 0 and budget.used_bytes == before


def test_P13_T02_oversized_ready_preserves_idle_child_reservations_until_close():
    required, _metadata = default_required(1)
    budget = Budget(required)
    current = stage(budget, 2)
    before = (budget.used_bytes, budget.used_metadata_bytes, budget.used_descriptors)
    with pytest.raises(ResourceError):
        current.observe_ready(0, 65 * MIB, 8, 1)
    assert current.process_slots == 1 and current.ready_count == 0
    assert before == (budget.used_bytes, budget.used_metadata_bytes, budget.used_descriptors)
    assert not current.baseline_leases[0].released
    # Only the scheduler can prove the child was reaped before this release.
    current.close()
    current.close()
    assert budget.used_bytes == 0


def test_P13_T02_pressure_replaces_previous_excess_without_double_counting():
    budget = Budget(64 * MIB)
    with stage(budget, 1, coordinator_descriptors=10) as current:
        accounted = budget.used_bytes
        current.observe_pressure(facts(accounted + MIB, 13))
        assert current.pressure_bytes == MIB and current.pressure_descriptors == 3
        assert budget.used_bytes == accounted + MIB
        current.observe_pressure(facts(accounted + 2 * MIB, 15))
        assert current.pressure_bytes == 2 * MIB and current.pressure_descriptors == 5
        assert budget.used_bytes == accounted + 2 * MIB
        current.observe_pressure(facts(accounted - 1, 10))
        assert current.pressure_bytes == current.pressure_descriptors == 0
        assert budget.used_bytes == accounted


def test_P13_T02_failed_pressure_growth_retains_previous_atomic_reservation():
    budget = Budget(64 * MIB, descriptors=8)
    with stage(budget, 1) as current:
        accounted = budget.used_bytes
        current.observe_pressure(facts(accounted + MIB, 2))
        before = (budget.used_bytes, budget.used_descriptors)
        for observation in (facts(budget.limit_bytes + 1, 2), facts(accounted + 2 * MIB, 9)):
            with pytest.raises(ResourceError):
                current.observe_pressure(observation)
            assert (budget.used_bytes, budget.used_descriptors) == before
            assert current.pressure_bytes == MIB and current.pressure_descriptors == 2


def test_P13_T02_child_rss_is_not_added_to_its_existing_baseline_and_jobs():
    budget = Budget(256 * MIB)
    with stage(budget, 2) as current:
        accounted = budget.used_bytes
        current.observe_pressure(facts(2 * MIB, workers=(worker(rss=64 * MIB),)))
        assert current.pressure_bytes == 0 and budget.used_bytes == accounted
        current.observe_pressure(facts(2 * MIB, workers=(worker(rss=accounted),)))
        assert current.pressure_bytes == 2 * MIB


def test_P13_T02_missing_registered_worker_preserves_prior_pressure(monkeypatch):
    budget = Budget(256 * MIB)
    with stage(budget, 2) as current:
        monkeypatch.setattr(admission, '_process_resources', lambda identity: worker(identity.pid))
        current.register_process(0, 123456789)
        accounted = budget.used_bytes
        current.observe_pressure(facts(accounted + MIB, workers=(worker(),)))
        before = budget.used_bytes
        def missing(_identity):
            raise ResourceError('registered worker disappeared')
        monkeypatch.setattr(admission, '_process_resources', missing)
        with pytest.raises(ResourceError, match='disappeared'):
            current.observe_pressure()
        assert current.pressure_bytes == MIB and budget.used_bytes == before
        current.unregister_process(0)
        assert not current.baseline_leases[0].released


def test_P13_T02_mismatched_identity_and_extra_native_threads_reject_observation(monkeypatch):
    budget = Budget(256 * MIB)
    with stage(budget, 2) as current:
        monkeypatch.setattr(admission, '_process_resources', lambda identity: worker(identity.pid))
        current.register_process(0, 123456789)
        with pytest.raises(ResourceError, match='identities'):
            current.observe_pressure(facts(0, workers=(worker(123456780),)))
        with pytest.raises(ResourceError, match='native-thread'):
            current.observe_pressure(facts(0, workers=(worker(threads=2),)))
        assert current.pressure_bytes == 0


class FakeProcess:
    def __init__(self, pid):
        self.pid = pid

    def is_running(self):
        return True

    def create_time(self):
        return 10.0

    def oneshot(self):
        return nullcontext()

    def status(self):
        return 'running'

    def memory_info(self):
        return SimpleNamespace(rss=self.pid * 100)

    def num_fds(self):
        return 4

    def num_threads(self):
        return 1


def test_P13_T02_exact_pid_observation_has_no_descendant_discovery(monkeypatch):
    import psutil
    queried = []
    def process(pid):
        queried.append(pid)
        return FakeProcess(pid)
    monkeypatch.setattr(psutil, 'Process', process)
    observation = observe_processes((ProcessIdentity(11, 10.0), 12), coordinator_pid=10)
    assert queried == [10, 11, 12]
    assert observation.rss == 3300 and observation.descriptors == 12 and observation.threads == 3
    assert tuple(row.identity for row in observation.workers) == (ProcessIdentity(11, 10.0), ProcessIdentity(12, 10.0))


@pytest.mark.parametrize('failure', ['missing', 'reused', 'zombie', 'disappears', 'denied'])
def test_P13_T02_unavailable_process_is_not_reported_as_zero(monkeypatch, failure):
    import psutil
    class BrokenProcess(FakeProcess):
        def __init__(self, pid):
            super().__init__(pid)
            self.reads = 0
            if failure == 'missing':
                raise psutil.NoSuchProcess(pid)
            if failure == 'denied':
                raise psutil.AccessDenied(pid)
        def create_time(self):
            return 11.0 if failure == 'reused' else 10.0
        def status(self):
            return psutil.STATUS_ZOMBIE if failure == 'zombie' else 'running'
        def is_running(self):
            self.reads += 1
            return failure != 'disappears' or self.reads == 1
    monkeypatch.setattr(psutil, 'Process', BrokenProcess)
    with pytest.raises(ResourceError):
        observe_processes((ProcessIdentity(11, 10.0),), coordinator_pid=10)


def test_P13_T02_real_coordinator_observation_is_bounded_numeric_state():
    observation = observe_processes()
    assert observation.coordinator.identity.pid == os.getpid()
    assert observation.coordinator.identity.created > 0
    assert observation.rss > 0 and observation.descriptors >= 0 and observation.threads >= 1
    assert observation.workers == ()


@pytest.mark.parametrize('kwargs', [
    {'task_count': -1}, {'task_count': True}, {'per_job_bytes': 0},
    {'per_job_bytes': 16383}, {'per_job_metadata_bytes': -1},
    {'channel_bytes': 0, 'channel_metadata_bytes': 0, 'channel_descriptors': 0},
    {'channel_bytes': 1, 'channel_metadata_bytes': 2}, {'worker_descriptors': -1},
    {'operation': 'inspect'}, {'operation': 'unknown'}, {'coordinator_descriptors': False},
])
def test_P13_T02_invalid_configuration_precedes_any_reservation(kwargs):
    budget = Budget(256 * MIB)
    with pytest.raises(ConfigurationError):
        stage(budget, **kwargs)
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0


def test_P13_T02_interrupted_construction_releases_partial_partition(monkeypatch):
    budget = Budget(512 * MIB)
    real_acquire, calls = budget.acquire, []
    def interrupted(*args, **kwargs):
        calls.append(args[0])
        if len(calls) == 8:
            raise KeyboardInterrupt()
        return real_acquire(*args, **kwargs)
    monkeypatch.setattr(budget, 'acquire', interrupted)
    with pytest.raises(KeyboardInterrupt):
        stage(budget, 4)
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0


def test_P13_T02_coordinator_thread_and_closed_owner_are_enforced():
    budget = Budget(64 * MIB)
    current = stage(budget, 1)
    errors = []
    def other_thread():
        try:
            current.observe_pressure(facts(0))
        except ResourceError as error:
            errors.append(error)
    thread = Thread(target=other_thread)
    thread.start()
    thread.join(3)
    assert not thread.is_alive() and len(errors) == 1
    current.close()
    with pytest.raises(ResourceError, match='closed'):
        current.seal_ready()
    assert budget.used_bytes == 0


@pytest.mark.parametrize('identities', [(1, 1), tuple(range(1, 67)), [1], (False,)])
def test_P13_T02_process_identity_collection_is_bounded_and_unique(identities):
    with pytest.raises(ConfigurationError):
        observe_processes(identities, coordinator_pid=1000)


def test_P13_T02_helper_is_observed_but_remains_interpreter_owned(monkeypatch):
    budget = Budget(256 * MIB)
    # Scheduler reserves the helper before process-slot admission, including its
    # metadata subset. This reservation is independent of StageAdmission.close.
    helper_lease = budget.acquire('stage-helper', 64 * MIB + 4096, metadata_bytes=4096)
    calls = []
    def observed(identity):
        calls.append(identity.pid)
        return worker(identity.pid, rss=10 * MIB)
    monkeypatch.setattr(admission, '_process_resources', observed)
    current = stage(budget, 2)
    current.register_process(0, 123456781)
    current.register_helper(123456782)
    calls.clear()
    # Inject a distinct coordinator sample so all exact identities remain unique.
    original = admission._process_resources
    monkeypatch.setattr(admission, '_process_resources',
                        lambda identity: ProcessResources(ProcessIdentity(os.getpid(), 1.0), MIB, 2, 1)
                        if identity.pid == os.getpid() else original(identity))
    snapshot = current.observe_pressure()
    assert calls == [123456781, 123456782]
    assert len(snapshot.workers) == 2 and snapshot.rss == 21 * MIB
    assert current.pressure_bytes == 0
    with pytest.raises(ResourceError, match='already registered'):
        current.register_helper(123456783)
    with pytest.raises(ResourceError, match='unregistered'):
        current.unregister_helper()
    current.unregister_process(0)
    current.unregister_helper()
    current.close()
    assert not helper_lease.released
    helper_lease.release()
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0


def test_P13_T02_helper_reservation_precedes_fixed_partition_selection():
    required, _metadata = default_required(1)
    budget = Budget(required)
    helper_lease = budget.acquire('stage-helper', 64 * MIB + 4096, metadata_bytes=4096)
    with stage(budget, 2) as current:
        assert current.process_slots == 0
    assert not helper_lease.released
    helper_lease.release()
    assert budget.used_bytes == 0


def test_P13_T02_missing_helper_preserves_prior_pressure(monkeypatch):
    budget = Budget(256 * MIB)
    with stage(budget, 1) as current:
        monkeypatch.setattr(admission, '_process_resources', lambda identity: worker(identity.pid))
        current.register_helper(123456789)
        accounted = budget.used_bytes
        current.observe_pressure(facts(accounted + MIB, workers=(worker(),)))
        def unavailable(_identity):
            raise ResourceError('interpreter helper disappeared')
        monkeypatch.setattr(admission, '_process_resources', unavailable)
        with pytest.raises(ResourceError, match='helper disappeared'):
            current.observe_pressure()
        assert current.pressure_bytes == MIB


def test_P13_T02_helper_and_worker_cannot_share_a_pid(monkeypatch):
    budget = Budget(256 * MIB)
    with stage(budget, 2) as current:
        monkeypatch.setattr(admission, '_process_resources', lambda identity: worker(identity.pid))
        current.register_helper(123456789)
        with pytest.raises(ConfigurationError, match='distinct'):
            current.register_process(0, 123456789)
        current.unregister_helper()
        current.register_process(0, 123456789)
        with pytest.raises(ConfigurationError, match='distinct'):
            current.register_helper(123456789)


def test_P13_T02_sixty_four_workers_and_one_helper_fit_observation_bound(monkeypatch):
    monkeypatch.setattr(admission, '_process_resources', lambda identity: worker(identity.pid))
    observed = observe_processes(tuple(range(1, 66)), coordinator_pid=1000)
    assert len(observed.workers) == 65
    budget = Budget(8 << 30)
    with stage(budget, 1000) as current:
        assert current.process_slots == 64
        for index in range(64):
            current.register_process(index, index + 1)
        current.register_helper(65)
        current.observe_pressure(facts(0, workers=observed.workers))
        assert current.pressure_bytes == 0


def test_P13_T02_ready_descriptor_failure_does_not_partially_grow_ram():
    budget = Budget(256 * MIB, descriptors=12)
    with stage(budget, 2) as current:
        before = budget.used_bytes, budget.used_descriptors
        with pytest.raises(ResourceError):
            current.observe_ready(0, 65 * MIB, 9, 1)
        assert (budget.used_bytes, budget.used_descriptors) == before
        assert current.ready_count == 0


def test_P13_T02_unadmittable_control_owner_releases_every_lease():
    budget = Budget(256 * MIB, metadata_limit_bytes=16384)
    with pytest.raises(ResourceError):
        stage(budget, 2)
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0


def test_P13_T02_lease_registry_capacity_can_reduce_partition():
    budget = Budget(512 * MIB, max_leases=6)
    with stage(budget, 4) as current:
        assert current.process_slots == 1 and budget.active_leases == 6
    assert budget.used_bytes == 0
