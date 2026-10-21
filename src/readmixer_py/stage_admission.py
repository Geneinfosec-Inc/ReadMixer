"""Private fixed process admission; production pipelines still use Plan 14 paths.

The versioned 64 MiB child baseline is an unqualified reservation, not an RSS
limit. This owner reserves idle children, transport overlap, and two complete
job grants per worker before a scheduler may start processes. It never launches,
terminates, or reaps a process, and must outlive every child and retained result.
"""
from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass
import math
import os
from threading import get_ident

from .errors import ConfigurationError, ResourceError
from .execution import CpuPlan
from .resources import Budget, checked_add, checked_multiply

MIB = 1 << 20
MAX_WORKERS = 64
MAX_OBSERVED_CHILDREN = MAX_WORKERS + 1  # One interpreter-owned resource tracker.
BASELINE_VERSION = 1
BASELINE_QUALIFIED = False
CHILD_BASELINE_BYTES = 64 * MIB
CHANNEL_BYTES = 4 * MIB + 256 * 1024
CHANNEL_METADATA_BYTES = 256 * 1024
CONTROL_BYTES = 16 * 1024
WORKER_CONTROL_BYTES = 4096
MAX_COMPRESSION_SLOTS = 32
_U64 = (1 << 64) - 1


def _integer(value, name, *, minimum=0, maximum=_U64):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ConfigurationError(f'{name} must be a bounded integer')
    return value


class ReadyAdmissionError(ResourceError):
    """A valid READY observation needs a smaller, freshly admitted partition.

    Only admission shortages produce this private retry signal. Protocol faults,
    thread violations, cancellation and failures committing a lease remain fatal.
    Existing reservations stay live until the scheduler has reaped the attempt.
    """

    def __init__(self, baseline_bytes, descriptors):
        super().__init__('worker startup baseline exceeds the admitted partition')
        self.baseline_bytes = baseline_bytes
        self.descriptors = descriptors


@dataclass(frozen=True, slots=True)
class ProcessIdentity:
    """Exact PID plus optional observed birth time; never a process-tree query."""

    pid: int
    created: float | None = None

    def __post_init__(self):
        _integer(self.pid, 'process identity', minimum=1)
        if (self.created is not None and
                (type(self.created) not in (int, float) or not 0 <= self.created <= _U64
                 or not math.isfinite(self.created))):
            raise ConfigurationError('process birth time is invalid')


@dataclass(frozen=True, slots=True)
class ProcessResources:
    identity: ProcessIdentity
    rss: int
    descriptors: int
    threads: int

    def __post_init__(self):
        if not isinstance(self.identity, ProcessIdentity):
            raise ConfigurationError('process observation identity is invalid')
        _integer(self.rss, 'process RSS')
        _integer(self.descriptors, 'process descriptors')
        _integer(self.threads, 'process threads', minimum=1)


@dataclass(frozen=True, slots=True)
class AggregateResources:
    coordinator: ProcessResources
    workers: tuple[ProcessResources, ...] = ()

    def __post_init__(self):
        if (not isinstance(self.coordinator, ProcessResources)
                or type(self.workers) is not tuple or len(self.workers) > MAX_OBSERVED_CHILDREN
                or any(not isinstance(worker, ProcessResources) for worker in self.workers)):
            raise ConfigurationError('aggregate process observations are invalid')
        identities = (self.coordinator.identity.pid, *(worker.identity.pid for worker in self.workers))
        if len(set(identities)) != len(identities):
            raise ConfigurationError('aggregate observations contain duplicate process identities')
        # Validate aggregate widths before a caller uses these facts for admission.
        self.rss, self.descriptors, self.threads

    def _total(self, field):
        result = getattr(self.coordinator, field)
        for worker in self.workers:
            result = checked_add(result, getattr(worker, field))
        return result

    @property
    def rss(self):
        return self._total('rss')

    @property
    def descriptors(self):
        return self._total('descriptors')

    @property
    def threads(self):
        return self._total('threads')


def _process_resources(identity):
    import psutil

    try:
        process = psutil.Process(identity.pid)
        if not process.is_running():
            raise ResourceError('a registered process disappeared during observation')
        created = process.create_time()
        if identity.created is not None and identity.created != created:
            raise ResourceError('a registered process identity was replaced')
        with process.oneshot():
            if process.status() == psutil.STATUS_ZOMBIE:
                raise ResourceError('a registered process exited before observation')
            rss = process.memory_info().rss
            descriptors = process.num_fds()
            threads = process.num_threads()
        if not process.is_running():
            raise ResourceError('a registered process disappeared during observation')
        return ProcessResources(ProcessIdentity(identity.pid, created), rss, descriptors, threads)
    except (OSError, AttributeError, OverflowError, ValueError, psutil.Error) as error:
        raise ResourceError('registered process resource observations are unavailable') from error


def observe_processes(worker_pids=(), *, coordinator_pid=None):
    """Read only exact registered PIDs; missing workers fail instead of counting zero.

    Pass ProcessIdentity values after the first observation to detect PID reuse.
    The bounded returned snapshot must remain within its caller's admitted control
    metadata; StageAdmission reserves room for one complete snapshot and overlap.
    """
    if type(worker_pids) is not tuple or len(worker_pids) > MAX_OBSERVED_CHILDREN:
        raise ConfigurationError('registered workers must be a bounded tuple')
    coordinator = os.getpid() if coordinator_pid is None else coordinator_pid
    identities = tuple(value if isinstance(value, ProcessIdentity) else ProcessIdentity(value)
                       for value in (coordinator, *worker_pids))
    if len({identity.pid for identity in identities}) != len(identities):
        raise ConfigurationError('registered process identities must be unique')
    rows = tuple(_process_resources(identity) for identity in identities)
    return AggregateResources(rows[0], rows[1:])


class StageAdmission(AbstractContextManager):
    """One coordinator-owned fixed partition and its complete reserved windows.

    ``per_job_bytes`` is the complete incremental job workspace, including its
    ``per_job_metadata_bytes`` subset and all input/result/copy overlap. It does
    not contain the separately reserved child import baseline or channels.
    Returned job leases remain owned here until close; a scheduler borrows them
    while retaining at most two jobs/results per worker. Close only after reaping
    every child and disposing every result. This class cannot prove that lifecycle.
    """

    def __init__(self, budget, cpu_plan, *, operation, task_count, per_job_bytes,
                 per_job_metadata_bytes=16 * 1024, per_job_descriptors=0,
                 channel_bytes=CHANNEL_BYTES, channel_metadata_bytes=CHANNEL_METADATA_BYTES,
                 channel_descriptors=4, worker_metadata_bytes=32 * 1024,
                 worker_descriptors=8, coordinator_descriptors=0,
                 max_process_slots=None, baseline_bytes=CHILD_BASELINE_BYTES):
        if not isinstance(budget, Budget) or not isinstance(cpu_plan, CpuPlan):
            raise ConfigurationError('stage admission requires shared RAM and CPU budgets')
        if operation not in ('mix', 'recover', 'verify'):
            raise ConfigurationError('stage admission operation is invalid')
        cpu = _integer(cpu_plan.effective_limit, 'CPU budget', minimum=1)
        _integer(task_count, 'task availability')
        if max_process_slots is not None:
            _integer(max_process_slots, 'startup process ceiling', maximum=MAX_WORKERS)
        _integer(baseline_bytes, 'child baseline', minimum=CHILD_BASELINE_BYTES)
        _integer(per_job_bytes, 'job workspace', minimum=1)
        for name, value in (('job metadata', per_job_metadata_bytes),
                            ('job descriptors', per_job_descriptors),
                            ('channel bytes', channel_bytes),
                            ('channel metadata', channel_metadata_bytes),
                            ('channel descriptors', channel_descriptors),
                            ('worker metadata', worker_metadata_bytes),
                            ('worker descriptors', worker_descriptors),
                            ('coordinator descriptors', coordinator_descriptors)):
            _integer(value, name)
        if per_job_metadata_bytes > per_job_bytes or channel_metadata_bytes > channel_bytes:
            raise ConfigurationError('stage metadata must be a subset of its workspace')
        if channel_bytes == channel_descriptors == 0:
            raise ConfigurationError('worker channels need a complete admitted workspace')
        baseline_reservation = checked_add(baseline_bytes, worker_metadata_bytes)
        self.observed_baseline_bytes = baseline_bytes
        self.observed_worker_descriptors = worker_descriptors
        self.budget, self.cpu_plan, self.operation = budget, cpu_plan, operation
        self.per_job_bytes, self.per_job_metadata_bytes = per_job_bytes, per_job_metadata_bytes
        self.baseline_version, self.baseline_qualified = BASELINE_VERSION, BASELINE_QUALIFIED
        self._owner, self._closed, self._sealed = get_ident(), False, False
        self._worker_metadata = worker_metadata_bytes
        self._coordinator_descriptors = coordinator_descriptors
        self._control = self._pressure = None
        self._baselines = self._channels = self._jobs = ()
        self._ready = []
        self._registered = []
        self._helper = None
        raw_target = 0 if cpu == 1 else cpu - 1 if operation == 'verify' else max(1, cpu - 2)
        target = min(raw_target, task_count, MAX_WORKERS)
        reasons = []
        if max_process_slots is not None and target > max_process_slots:
            target = max_process_slots
            reasons.append('startup-baseline-admission')
        if cpu == 1:
            reasons.append('single-cpu')
        if target < raw_target and task_count < raw_target:
            reasons.append('task-availability')
        if raw_target > MAX_WORKERS:
            reasons.append('process-slot-cap')
        try:
            self._control = budget.acquire('stage-control', CONTROL_BYTES, metadata_bytes=CONTROL_BYTES)
            # Keep this handle registered at zero so subsequent pressure growth
            # adds only observed excess, with no late metadata/registry allocation.
            self._pressure = budget.acquire('stage-pressure', 1)
            self._pressure.resize(0)
            while True:
                leases = [None] * (4 * target)
                try:
                    self._control.resize(CONTROL_BYTES + WORKER_CONTROL_BYTES * target,
                                         metadata_bytes=CONTROL_BYTES + WORKER_CONTROL_BYTES * target)
                    for index in range(target):
                        start = 4 * index
                        leases[start] = budget.acquire('stage-child', baseline_reservation, worker_descriptors,
                                                       metadata_bytes=worker_metadata_bytes)
                        leases[start + 1] = budget.acquire('stage-channel', channel_bytes, channel_descriptors,
                                                           metadata_bytes=channel_metadata_bytes)
                        for slot in (2, 3):
                            leases[start + slot] = budget.acquire('stage-job', per_job_bytes, per_job_descriptors,
                                                                  metadata_bytes=per_job_metadata_bytes)
                    self._baselines = tuple(leases[0::4])
                    self._channels = tuple(leases[1::4])
                    self._jobs = tuple(lease for index, lease in enumerate(leases) if index % 4 in (2, 3))
                    break
                except ResourceError:
                    for lease in reversed(leases):
                        if lease is not None:
                            lease.release()
                    self._baselines = self._channels = self._jobs = ()
                    if not target:
                        raise
                    target -= 1
                    if 'working-set-admission' not in reasons:
                        reasons.append('working-set-admission')
                except BaseException:
                    for lease in reversed(leases):
                        if lease is not None:
                            lease.release()
                    self._baselines = self._channels = self._jobs = ()
                    raise
                finally:
                    leases = None
            self.process_slots = target
            self.workers_effective = 1 + target
            self.job_capacity = 2 * target
            self.window_bytes = checked_multiply(self.job_capacity, per_job_bytes)
            self.compression_target = 0 if operation == 'verify' else min(MAX_COMPRESSION_SLOTS, cpu - 1 - target)
            self.reduction_reasons = tuple(reasons)
            self._ready = [False] * target
            self._registered = [None] * target
        except BaseException:
            self.close()
            raise

    def _check(self):
        if get_ident() != self._owner:
            raise ResourceError('stage admission must remain on its coordinator thread')
        if self._closed:
            raise ResourceError('stage admission is closed')

    def _index(self, index):
        self._check()
        _integer(index, 'worker slot', maximum=self.process_slots - 1)

    @property
    def baseline_leases(self):
        return self._baselines

    @property
    def channel_leases(self):
        return self._channels

    @property
    def job_leases(self):
        return self._jobs

    @property
    def ready_count(self):
        return sum(self._ready)

    @property
    def pressure_bytes(self):
        return 0 if self._pressure is None else self._pressure.bytes

    @property
    def pressure_descriptors(self):
        return 0 if self._pressure is None else self._pressure.descriptors

    def observe_ready(self, index, rss, descriptors, threads):
        self._index(index)
        if self._sealed:
            raise ResourceError('worker baselines cannot change after job admission')
        _integer(rss, 'worker startup RSS')
        _integer(descriptors, 'worker startup descriptors')
        _integer(threads, 'worker startup threads', minimum=1)
        if threads != 1:
            raise ResourceError('worker startup exceeds its single native-thread slot')
        lease = self._baselines[index]
        required_bytes = max(lease.bytes, checked_add(rss, self._worker_metadata))
        required_descriptors = max(lease.descriptors, descriptors)
        self.observed_baseline_bytes = max(self.observed_baseline_bytes, rss)
        self.observed_worker_descriptors = max(self.observed_worker_descriptors, descriptors)
        # Classify only an actual budget shortage as retryable. An interruption,
        # closed lease or failed atomic state commit must not become a retry.
        with self.budget._lock:
            if lease.released:
                raise ResourceError('worker startup baseline ownership was released')
            if (required_bytes - lease.bytes > self.budget.remaining_bytes or
                    required_descriptors - lease.descriptors >
                    self.budget.descriptor_limit - self.budget.used_descriptors):
                raise ReadyAdmissionError(self.observed_baseline_bytes,
                                          self.observed_worker_descriptors)
            lease.resize(required_bytes, required_descriptors,
                         metadata_bytes=self._worker_metadata)
        self._ready[index] = True

    def seal_ready(self):
        """Freeze successful startup observations before any job is dispatched."""
        self._check()
        if self.ready_count != self.process_slots:
            raise ResourceError('all admitted workers must be ready before dispatch')
        self._sealed = True

    def register_process(self, index, pid, *, created=None):
        self._index(index)
        if self._registered[index] is not None:
            raise ResourceError('worker slot already owns a registered process')
        identity = ProcessIdentity(pid, created)
        if (identity.pid == os.getpid()
                or self._helper is not None and self._helper.pid == pid
                or any(other is not None and other.pid == pid for other in self._registered)):
            raise ConfigurationError('registered worker identities must be distinct')
        # Observe birth identity now, then require it on every later checkpoint.
        facts = _process_resources(identity)
        self._registered[index] = facts.identity
        return facts

    def unregister_process(self, index):
        """Scheduler-only bookkeeping after confirmed exit/reap; leases stay live."""
        self._index(index)
        self._registered[index] = None

    def register_helper(self, pid, *, created=None):
        """Register one interpreter-owned tracker; its separate lease belongs to the scheduler.

        Reserve its 64 MiB startup baseline and at least 4 KiB metadata before
        constructing this partition. The tracker may outlive this invocation;
        registration conveys observation authority, never termination ownership.
        """
        self._check()
        if self._helper is not None:
            raise ResourceError('an interpreter helper is already registered')
        identity = ProcessIdentity(pid, created)
        if identity.pid == os.getpid() or any(other is not None and other.pid == pid for other in self._registered):
            raise ConfigurationError('registered helper identity must be distinct')
        facts = _process_resources(identity)
        self._helper = facts.identity
        return facts

    def unregister_helper(self):
        """Forget the helper only after the scheduler has reaped/unregistered workers."""
        self._check()
        if any(identity is not None for identity in self._registered):
            raise ResourceError('worker processes must be unregistered before their helper')
        self._helper = None

    def _identities(self):
        workers = tuple(identity for identity in self._registered if identity is not None)
        return workers if self._helper is None else (*workers, self._helper)

    def observe(self):
        self._check()
        return observe_processes(self._identities())

    def observe_pressure(self, facts=None):
        """Reserve only observed excess, preserving the prior charge on failure.

        Missing registered PIDs raise before any adjustment. Shared physical pages
        may be counted in several RSS readings, deliberately preserving conservative
        aggregate accounting. System/cgroup headroom remains Budget.checkpoint's
        separate responsibility; this method never changes the resolved RAM cap.
        """
        self._check()
        facts = self.observe() if facts is None else facts
        if (not isinstance(facts, AggregateResources)
                or len(facts.workers) > self.process_slots + int(self._helper is not None)):
            raise ConfigurationError('aggregate observation exceeds its admitted worker count')
        expected = self._identities()
        if expected and tuple(worker.identity for worker in facts.workers) != expected:
            raise ResourceError('aggregate observation does not match registered worker identities')
        if any(worker.threads != 1 for worker in facts.workers):
            raise ResourceError('worker observation exceeds its single native-thread slot')
        with self.budget._lock:
            accounted_bytes = self.budget.used_bytes - self._pressure.bytes
            accounted_descriptors = self.budget.used_descriptors - self._pressure.descriptors
            excess_bytes = max(0, facts.rss - accounted_bytes)
            excess_descriptors = max(0, facts.descriptors - self._coordinator_descriptors - accounted_descriptors)
            self._pressure.resize(excess_bytes, excess_descriptors)
        return facts

    def close(self):
        if self._closed:
            return
        if get_ident() != self._owner:
            raise ResourceError('stage admission must remain on its coordinator thread')
        self._registered.clear()
        self._helper = None
        self._ready.clear()
        for group in (self._jobs, self._channels, self._baselines):
            for lease in reversed(group):
                lease.release()
        self._jobs = self._channels = self._baselines = ()
        if self._pressure is not None:
            self._pressure.release()
            self._pressure = None
        if self._control is not None:
            self._control.release()
            self._control = None
        self._closed = True

    def __exit__(self, exc_type, exc, traceback):
        try:
            self.close()
        except BaseException:
            if exc is None:
                raise
            exc.add_note('stage admission release also failed; primary error preserved')
