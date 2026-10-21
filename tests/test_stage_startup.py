"""P13-T02 READY-only startup reductions and ownership-safe retry boundaries."""
import psutil
import pytest

from readmixer_py import stage_scheduler as scheduler_module
from readmixer_py.errors import CancelledError, ConfigurationError, IntegrityError, ResourceError
from readmixer_py.execution import CpuPlan
from readmixer_py.resources import Budget, Lease
from readmixer_py.stage_admission import CHILD_BASELINE_BYTES, ReadyAdmissionError, StageAdmission
from readmixer_py.worker_protocol import Ready, Stop

MIB = 1 << 20


def cpu(count):
    return CpuPlan(count, count, count, None, count, ('test',))


def admission(budget, **options):
    return StageAdmission(budget, cpu(4), operation='verify', task_count=3,
                          per_job_bytes=MIB, **options)


def assert_empty(budget):
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
    assert budget.active_leases == 0


@pytest.mark.parametrize('headroom,accepted', [(MIB, True), (MIB - 1, False)])
def test_P13_T02_ready_growth_exact_and_one_over_keep_atomic_owners(headroom, accepted):
    budget = Budget(512 * MIB)
    with admission(budget, max_process_slots=1) as current:
        ballast = budget.acquire('test-ready-ballast', budget.remaining_bytes - headroom - 512)
        before = (budget.used_bytes, budget.used_metadata_bytes, budget.used_descriptors)
        try:
            if accepted:
                current.observe_ready(0, CHILD_BASELINE_BYTES + MIB, 8, 1)
                assert current.ready_count == 1
                assert budget.used_bytes == budget.limit_bytes
            else:
                with pytest.raises(ReadyAdmissionError) as caught:
                    current.observe_ready(0, CHILD_BASELINE_BYTES + MIB, 8, 1)
                assert caught.value.baseline_bytes == CHILD_BASELINE_BYTES + MIB
                assert caught.value.descriptors == 8
                assert current.ready_count == 0
                assert (budget.used_bytes, budget.used_metadata_bytes, budget.used_descriptors) == before
            assert all(not lease.released for lease in current.baseline_leases)
        finally:
            ballast.release()
    assert_empty(budget)


def test_P13_T02_ready_descriptor_shortage_retains_previous_larger_rss_coefficient():
    budget = Budget(512 * MIB, descriptors=24)
    with admission(budget, max_process_slots=2) as current:
        current.observe_ready(0, 96 * MIB, 8, 1)
        before = budget.used_bytes
        with pytest.raises(ReadyAdmissionError) as caught:
            current.observe_ready(1, 65 * MIB, 9, 1)
        assert caught.value.baseline_bytes == 96 * MIB
        assert caught.value.descriptors == 9
        assert current.ready_count == 1
        assert current.observed_baseline_bytes == 96 * MIB
        assert budget.used_bytes == before and budget.used_descriptors == 24
    assert_empty(budget)


@pytest.mark.parametrize('options', [
    {'max_process_slots': -1}, {'max_process_slots': 65}, {'max_process_slots': True},
    {'baseline_bytes': CHILD_BASELINE_BYTES - 1}, {'baseline_bytes': True},
])
def test_P13_T02_invalid_retry_profiles_never_allocate(options):
    budget = Budget(512 * MIB)
    with pytest.raises(ConfigurationError):
        admission(budget, **options)
    assert_empty(budget)


def test_P13_T02_admitted_resize_commit_failure_is_not_retryable(monkeypatch):
    budget = Budget(512 * MIB)
    with admission(budget, max_process_slots=1) as current:
        owner = current.baseline_leases[0]
        original = Lease.resize
        failure = ResourceError('injected atomic commit failure')
        def fail(self, *args, **kwargs):
            if self is owner:
                raise failure
            return original(self, *args, **kwargs)
        monkeypatch.setattr(Lease, 'resize', fail)
        with pytest.raises(ResourceError) as caught:
            current.observe_ready(0, CHILD_BASELINE_BYTES + MIB, 8, 1)
        assert caught.value is failure and not isinstance(caught.value, ReadyAdmissionError)
        assert current.ready_count == 0
    assert_empty(budget)


@pytest.mark.parametrize('operation,count,compression', [('verify', 4, 0), ('mix', 5, 2)])
def test_P13_T02_real_ready_retry_reaps_entire_attempt_and_keeps_observed_profile(
        monkeypatch, operation, count, compression):
    budget = Budget(512 * MIB)
    original_ready = StageAdmission.observe_ready
    original_discard = scheduler_module.StageScheduler._discard_startup_attempt
    original_start = scheduler_module._start
    observed_partitions, retired_pids, started = [], [], []
    def enlarged(self, index, rss, descriptors, threads):
        if index == 0:
            observed_partitions.append(self.process_slots)
        return original_ready(self, index, 160 * MIB, descriptors, threads)
    def discard(self):
        pids = self.child_pids
        owners = (*self.admission.baseline_leases, *self.admission.channel_leases,
                  *self.admission.job_leases)
        assert pids and all(not lease.released for lease in owners)
        original_discard(self)
        assert all(not psutil.pid_exists(pid) for pid in pids)
        assert all(lease.released for lease in owners)
        retired_pids.extend(pids)
    def start(process):
        assert all(not psutil.pid_exists(pid) for pid in retired_pids)
        original_start(process)
        started.append(process.pid)
    monkeypatch.setattr(StageAdmission, 'observe_ready', enlarged)
    monkeypatch.setattr(scheduler_module.StageScheduler, '_discard_startup_attempt', discard)
    monkeypatch.setattr(scheduler_module, '_start', start)
    with scheduler_module.StageScheduler(budget, cpu(count), bytes(16), task_count=3,
                                        operation=operation, max_job_bytes=1024) as scheduler:
        assert observed_partitions == [3, 2]
        assert scheduler.startup_retries == 1 and scheduler.process_slots == 2
        assert len(retired_pids) == 2
        assert scheduler.startup_baseline_bytes == 160 * MIB
        assert scheduler.admission.compression_target == compression
        assert scheduler.workers_effective + compression <= count
        assert all(lease.bytes >= 160 * MIB for lease in scheduler.admission.baseline_leases)
        assert scheduler.admission.reduction_reasons == ('startup-baseline-admission',)
        assert scheduler.cancellation.error is None
        assert scheduler.pending_jobs == 0
        source = budget.acquire('test-ready-empty-source', 1)
        scheduler.submit(b'', source)
        with scheduler.take_next() as result:
            assert result.data == b'' and result.records == 0
        assert source.released
    assert all(not psutil.pid_exists(pid) for pid in started)
    assert_empty(budget)


def test_P13_T02_real_unaffordable_ready_falls_back_to_serial_without_restart(monkeypatch):
    # Keep this READY-refusal test independent of native arenas retained by
    # earlier full-suite tests; the injected child remains unaffordable.
    budget = Budget(512 * MIB)
    original = StageAdmission.observe_ready
    seen = []
    def enlarged(self, index, rss, descriptors, threads):
        seen.append(self.process_slots)
        return original(self, index, 1000 * MIB, descriptors, threads)
    monkeypatch.setattr(StageAdmission, 'observe_ready', enlarged)
    with scheduler_module.StageScheduler(budget, cpu(2), bytes(16), task_count=3,
                                        max_job_bytes=1024) as scheduler:
        assert seen == [1]
        assert scheduler.startup_retries == 1 and scheduler.process_slots == 0
        assert scheduler.capacity == 1 and scheduler.child_pids == ()
        assert scheduler._helper_lease is None
        assert scheduler.startup_baseline_bytes == 1000 * MIB
        assert scheduler.admission.compression_target == 0
        source = budget.acquire('test-serial-empty-source', 1)
        scheduler.submit(b'', source)
        with scheduler.take_next() as result:
            assert result.data == b''
    assert_empty(budget)


def test_P13_T02_real_descriptor_growth_retries_with_larger_descriptor_reservations(monkeypatch):
    budget = Budget(512 * MIB, descriptors=100)
    original = StageAdmission.observe_ready
    seen = []
    def enlarged(self, index, rss, descriptors, threads):
        if index == 0:
            seen.append(self.process_slots)
        return original(self, index, rss, 30, threads)
    monkeypatch.setattr(StageAdmission, 'observe_ready', enlarged)
    with scheduler_module.StageScheduler(budget, cpu(4), bytes(16), task_count=3,
                                        max_job_bytes=1024) as scheduler:
        assert seen == [3, 2]
        assert scheduler.startup_retries == 1 and scheduler.process_slots == 2
        assert scheduler.startup_worker_descriptors == 30
        assert all(lease.descriptors == 30 for lease in scheduler.admission.baseline_leases)
        assert budget.used_descriptors == 92
    assert_empty(budget)


def _multithreaded_ready(sock, run_id, max_job_bytes, workspace_bytes, *extra):
    from readmixer_py.worker_transport import FramedSocket
    channel = FramedSocket(sock)
    try:
        channel.send(Ready(100 * MIB, 8, 2))
        while True:
            channel.pump(0.01)
            if isinstance(channel.receive(), Stop):
                return
    finally:
        channel.close()


def test_P13_T02_real_native_thread_violation_is_fatal_without_retry(monkeypatch):
    budget = Budget(512 * MIB)
    original = scheduler_module._start
    started = []
    def start(process):
        original(process)
        started.append(process.pid)
    monkeypatch.setattr(scheduler_module, '_start', start)
    monkeypatch.setattr(scheduler_module, 'worker_main', _multithreaded_ready)
    with pytest.raises(ResourceError, match='native-thread'):
        scheduler_module.StageScheduler(budget, cpu(4), bytes(16), task_count=3,
                                        max_job_bytes=1024)
    assert len(started) == 1 and all(not psutil.pid_exists(pid) for pid in started)
    assert_empty(budget)


@pytest.mark.parametrize('failure_type', [IntegrityError, ResourceError, KeyboardInterrupt, CancelledError])
def test_P13_T02_ordinary_startup_failure_is_not_retried(monkeypatch, failure_type):
    budget = Budget(512 * MIB)
    failure = failure_type('injected startup failure')
    attempts = []
    def fail(self):
        attempts.append(self.process_slots)
        raise failure
    monkeypatch.setattr(scheduler_module.StageScheduler, '_launch_partition', fail)
    with pytest.raises(failure_type) as caught:
        scheduler_module.StageScheduler(budget, cpu(4), bytes(16), task_count=3,
                                        max_job_bytes=1024)
    assert caught.value is failure and attempts == [3]
    assert_empty(budget)


def test_P13_T02_failed_reap_retains_whole_attempt_and_never_retries(monkeypatch):
    budget = Budget(512 * MIB)
    captured = []
    original_stop = scheduler_module.StageScheduler._stop_workers
    failure = ResourceError('injected unreaped startup ownership')
    def shortage(self):
        captured.append(self)
        raise ReadyAdmissionError(160 * MIB, 8)
    def cannot_reap(self):
        raise failure
    monkeypatch.setattr(scheduler_module.StageScheduler, '_launch_partition', shortage)
    monkeypatch.setattr(scheduler_module.StageScheduler, '_stop_workers', cannot_reap)
    try:
        with pytest.raises(ResourceError) as caught:
            scheduler_module.StageScheduler(budget, cpu(4), bytes(16), task_count=3,
                                            max_job_bytes=1024)
        assert caught.value is failure and len(captured) == 1
        scheduler = captured[0]
        assert scheduler.startup_retries == 0 and not scheduler._closed
        assert budget.used_bytes > 0 and budget.used_descriptors > 0
        assert all(not lease.released for lease in scheduler.admission.baseline_leases)
        assert budget._stage_scheduler is scheduler
    finally:
        monkeypatch.setattr(scheduler_module.StageScheduler, '_stop_workers', original_stop)
        for scheduler in captured:
            scheduler.abort()
    assert_empty(budget)


def test_P13_T02_cancellation_during_retry_cleanup_preserves_first_cause(monkeypatch):
    budget = Budget(512 * MIB)
    captured = []
    original_stop = scheduler_module.StageScheduler._stop_workers
    interrupted = KeyboardInterrupt('injected startup shutdown interruption')
    def shortage(self):
        captured.append(self)
        raise ReadyAdmissionError(160 * MIB, 8)
    calls = []
    def stop(self):
        calls.append(True)
        outcome = original_stop(self)
        return interrupted if len(calls) == 1 else outcome
    monkeypatch.setattr(scheduler_module.StageScheduler, '_launch_partition', shortage)
    monkeypatch.setattr(scheduler_module.StageScheduler, '_stop_workers', stop)
    with pytest.raises(KeyboardInterrupt) as caught:
        scheduler_module.StageScheduler(budget, cpu(4), bytes(16), task_count=3,
                                        max_job_bytes=1024)
    assert caught.value is interrupted and len(captured) == 1
    assert captured[0]._closed and captured[0].startup_retries == 0
    assert captured[0].cancellation.error is interrupted
    assert_empty(budget)
