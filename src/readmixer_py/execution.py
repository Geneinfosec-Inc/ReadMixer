"""CPU admission and one bounded native-compression service per invocation.

The invocation scheduler admits a fixed process partition for production
parsing, shuffle terminals and supported sorting before this service starts.
Bounded codec calls use threads within the same CPU budget. Rendering and all
compression result consumers remain on the coordinator.
"""
from __future__ import annotations

from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractContextManager
from dataclasses import dataclass
import os
from pathlib import Path, PurePosixPath
import re
import sys
from threading import Event, Lock, get_ident

from .errors import CancelledError, ConfigurationError, IntegrityError, ReadMixerError, ResourceError
from .resources import _mount_path, _read_fact, checked_add

MAX_PAYLOAD_BYTES = 65280
MAX_RESULT_BYTES = 65536
JOB_BYTES = (2 << 20) + 3 * MAX_RESULT_BYTES + 2048
THREAD_BYTES = 8 << 20  # Initial conservative stack/native envelope, unqualified.
_MAX_SLOTS = 32


@dataclass(frozen=True)
class CpuQuota:
    quota: int | None
    period: int
    version: int = 2

    def __post_init__(self):
        if (type(self.version) is not int or self.version not in (1, 2) or type(self.period) is not int or self.period <= 0
                or self.quota is not None and (type(self.quota) is not int or self.quota <= 0)):
            raise ConfigurationError('CPU quota requires a positive period and positive or unlimited quota')


@dataclass(frozen=True)
class CpuObservations:
    logical_cpus: int
    affinity_cpus: int | None = None
    quotas: tuple[CpuQuota, ...] = ()

    def __post_init__(self):
        if (type(self.logical_cpus) is not int or self.logical_cpus <= 0
                or self.affinity_cpus is not None and (type(self.affinity_cpus) is not int or self.affinity_cpus <= 0)
                or type(self.quotas) is not tuple or len(self.quotas) > 1024
                or any(not isinstance(quota, CpuQuota) for quota in self.quotas)):
            raise ConfigurationError('CPU observations must be positive and bounded')


@dataclass(frozen=True)
class CpuPlan:
    requested_workers: int | None
    logical_cpus: int
    affinity_cpus: int | None
    quota_limit: int | None
    effective_limit: int
    detection_sources: tuple[str, ...]


def parse_cpu_quota(value, version=2, *, period=None):
    try:
        if version == 2:
            fields = value.split()
            if len(fields) != 2 or not re.fullmatch(r'[0-9]{1,20}', fields[1]):
                raise ValueError
            quota_text, period_text = fields
            quota = None if quota_text == 'max' else int(quota_text)
        elif version == 1:
            quota_text, period_text = value.strip(), period.strip()
            quota = None if quota_text == '-1' else int(quota_text)
        else:
            raise ValueError
        if (quota_text not in ('max', '-1') and not re.fullmatch(r'[0-9]{1,20}', quota_text)
                or not re.fullmatch(r'[0-9]{1,20}', period_text)):
            raise ValueError
        result = CpuQuota(quota, int(period_text), version)
        if result.period >= 1 << 64 or result.quota is not None and result.quota >= 1 << 64:
            raise ValueError
        return result
    except (TypeError, ValueError, AttributeError, ConfigurationError):
        raise ResourceError('cgroup CPU quota is malformed') from None


def _cgroup_cpu(proc_root=Path('/proc')):
    memberships = _read_fact(proc_root / 'self/cgroup', optional=True)
    if memberships is None:
        return ()
    mounts = _read_fact(proc_root / 'self/mountinfo')
    groups = []
    for line in memberships.splitlines():
        fields = line.split(':', 2)
        if len(fields) != 3:
            raise ResourceError('CPU cgroup membership is malformed')
        version = 2 if fields[:2] == ['0', ''] else 1 if 'cpu' in fields[1].split(',') else 0
        if not version:
            continue
        path = PurePosixPath(fields[2])
        if not path.is_absolute() or '..' in path.parts:
            raise ResourceError('CPU cgroup membership path is malformed')
        groups.append((version, path))
        if len(groups) > 1024:
            raise ResourceError('CPU cgroup membership exceeds its bound')
    observed, visited = [], set()
    for line in mounts.splitlines():
        before, separator, after = line.partition(' - ')
        left, right = before.split(), after.split()
        if not separator or len(left) < 6 or len(right) < 3:
            raise ResourceError('CPU cgroup mount information is malformed')
        version = 2 if right[0] == 'cgroup2' else 1 if right[0] == 'cgroup' and 'cpu' in right[2].split(',') else 0
        if not version:
            continue
        root, mount = PurePosixPath(_mount_path(left[3])), Path(_mount_path(left[4]))
        if not root.is_absolute() or not mount.is_absolute():
            raise ResourceError('CPU cgroup mount paths are malformed')
        for kind, group in groups:
            if kind != version:
                continue
            try:
                relative = group.relative_to(root)
            except ValueError:
                if group != PurePosixPath('/'):
                    continue
                relative = PurePosixPath('.')
            current = mount / relative
            for _ in range(256):
                identity = version, current
                if identity not in visited:
                    if len(visited) >= 1024:
                        raise ResourceError('CPU cgroup ancestors exceed their bound')
                    visited.add(identity)
                    value = _read_fact(current / ('cpu.max' if version == 2 else 'cpu.cfs_quota_us'), optional=True)
                    if value is not None:
                        period = _read_fact(current / 'cpu.cfs_period_us') if version == 1 else None
                        observed.append(parse_cpu_quota(value, version, period=period))
                if current == mount:
                    break
                current = current.parent
            else:
                raise ResourceError('CPU cgroup hierarchy exceeds its depth bound')
    return tuple(observed)


def observe_cpus():
    logical = os.cpu_count() or 1
    affinity = None
    if hasattr(os, 'sched_getaffinity'):
        try:
            affinity = len(os.sched_getaffinity(0))
        except OSError:
            pass
    quotas = _cgroup_cpu() if sys.platform.startswith('linux') else ()
    return CpuObservations(logical, affinity, quotas)


def resolve_cpu_budget(workers=None, *, observations=None):
    if workers is not None and (type(workers) is not int or not 1 <= workers < 1 << 64):
        raise ConfigurationError('worker CPU upper bound must be a positive unsigned integer')
    facts = observe_cpus() if observations is None else observations
    if not isinstance(facts, CpuObservations):
        raise ConfigurationError('CPU observations have the wrong type')
    limits = [facts.logical_cpus]
    sources = ['logical-cpus']
    if facts.affinity_cpus is not None:
        limits.append(facts.affinity_cpus)
        sources.append('cpu-affinity')
    quotas = []
    for quota in facts.quotas:
        source = f'cgroup-v{quota.version}:' + ('unlimited' if quota.quota is None else 'quota')
        if source not in sources:
            sources.append(source)
        if quota.quota is not None:
            # Conservative integer concurrency, with one runnable coordinator
            # retained for every positive fractional quota.
            quotas.append(max(1, quota.quota // quota.period))
    quota_limit = min(quotas) if quotas else None
    if quota_limit is not None:
        limits.append(quota_limit)
    if workers is not None:
        limits.append(workers)
        sources.append('requested-workers')
    return CpuPlan(workers, facts.logical_cpus, facts.affinity_cpus, quota_limit,
                   min(limits), tuple(sources))


class CancellationState:
    """Thread-safe first typed failure; subsequent diagnostics are bounded."""

    def __init__(self, cancel=None):
        if cancel is not None and not callable(cancel):
            raise ConfigurationError('cancellation callback must be callable')
        self._external = cancel
        self._event, self._lock = Event(), Lock()
        self._error = None
        self._notes = 0

    @property
    def cancelled(self):
        return self._event.is_set()

    @property
    def error(self):
        with self._lock:
            return self._error

    def fail(self, error, *, stage='compression', task=None, coordinator=True):
        if not re.fullmatch(r'[a-z][a-z0-9-]{0,63}', stage):
            raise ConfigurationError('failure stage must be a bounded safe identifier')
        if task is not None and (type(task) is not int or not 0 <= task < 1 << 64):
            raise ConfigurationError('failure task identity must be an unsigned integer')
        if coordinator and isinstance(error, (KeyboardInterrupt, SystemExit)):
            pass
        elif not isinstance(error, ReadMixerError):
            error = ResourceError('bounded execution job failed')
        elif len(str(error)) > 1024:
            error = ResourceError('bounded execution diagnostic exceeded its safe length')
        with self._lock:
            if self._error is None:
                self._error = error
            elif error is not self._error and self._notes < 8:
                self._error.add_note(f'Additional {stage} failure' + (f' at task {task}' if task is not None else ''))
                self._notes += 1
            self._event.set()
            return self._error

    def cancel(self, error=None):
        return self.fail(CancelledError() if error is None else error, stage='cancellation')

    def check(self):
        if self._external is not None and self._external():
            self.cancel()
        error = self.error
        if error is not None:
            raise error


@dataclass(slots=True)
class _Job:
    future: object
    lane: object
    ordinal: int
    lease: object


class CompressionLane:
    def __init__(self, service, identity, consume):
        self.service, self.identity, self.consume = service, identity, consume
        self.submitted = self.drained = 0
        self.closed = False

    @property
    def pending(self):
        return self.submitted - self.drained

    def submit(self, payload, level=6):
        return self.service._submit(self, payload, level)

    def finish(self):
        if not self.closed:
            self.service._finish_lane(self)

    def abort(self, primary=None):
        if not self.closed:
            self.service.abort(primary)

    close = abort


class CompressionService(AbstractContextManager):
    """One CPU-admitted pool and at most two ordered jobs per compression slot.

    Queue slots have reserved leases before threads start. A submitted/completed
    job retains its transferred lease until the coordinator consumes its output.
    The default compression share is at most one quarter of currently unleased
    memory, leaving room for coordinator stages; this is not an RSS guarantee.
    """

    def __init__(self, budget, cpu_plan, *, encoder, cancellation=None, cancel=None,
                 max_lanes=64, memory_allowance=None):
        if (not isinstance(cpu_plan, CpuPlan) or not callable(encoder)
                or type(max_lanes) is not int or not 1 <= max_lanes <= 1024
                or cancellation is not None and not isinstance(cancellation, CancellationState)
                or cancellation is not None and cancel is not None):
            raise ConfigurationError('compression service configuration is invalid')
        if memory_allowance is not None and (type(memory_allowance) is not int or memory_allowance <= 0):
            raise ConfigurationError('compression memory allowance must be a positive integer')
        previous = getattr(budget, '_compression_service', None)
        if previous is not None and not previous._closed:
            raise ConfigurationError('an invocation may own only one shared compression service')
        self.budget, self.cpu_plan, self.encoder = budget, cpu_plan, encoder
        self.cancellation = cancellation or CancellationState(cancel)
        self._owner = get_ident()
        self._max_lanes, self._next_lane = max_lanes, 0
        self._lanes, self._pending, self._available = {}, deque(), []
        self._inflight = self._thread_lease = self._executor = None
        self._closed = False
        self.submitted_jobs = self.completed_jobs = self.peak_pending = 0
        self._metadata = budget.acquire('compression-service', 8192 + 256 * max_lanes,
                                        metadata_bytes=8192 + 256 * max_lanes)
        try:
            allowance = budget.remaining_bytes // 4 if memory_allowance is None else min(memory_allowance, budget.remaining_bytes)
            stage = getattr(budget, '_stage_scheduler', None)
            self.process_slots = stage.process_slots if stage is not None else 0
            wanted = min(_MAX_SLOTS, max(0, cpu_plan.effective_limit - 1))
            if stage is not None:
                if stage.admission.cpu_plan != cpu_plan:
                    raise ConfigurationError('compression must share the stage CPU budget')
                wanted = min(wanted, stage.admission.compression_target)
            self.slots = min(wanted, allowance // (THREAD_BYTES + 2 * JOB_BYTES))
            if memory_allowance is not None and allowance < JOB_BYTES:
                raise ResourceError('compression allowance cannot admit one bounded codec job')
            self.admission_reason = ('single-cpu' if cpu_plan.effective_limit == 1 else
                                     'memory-admission' if self.slots < wanted else
                                     'service-slot-cap' if cpu_plan.effective_limit - 1 > _MAX_SLOTS else 'cpu-admission')
            while True:
                try:
                    if self.slots:
                        self._thread_lease = budget.acquire('compression-threads', self.slots * THREAD_BYTES)
                    self.capacity = 2 * self.slots if self.slots else 1
                    charge = 8192 + 256 * max_lanes + 2048 * self.capacity
                    self._metadata.resize(charge, metadata_bytes=charge)
                    for _ in range(self.capacity):
                        self._available.append(budget.acquire('compression-window', JOB_BYTES))
                    break
                except ResourceError:
                    for lease in self._available:
                        lease.release()
                    self._available.clear()
                    if self._thread_lease is not None:
                        self._thread_lease.release()
                        self._thread_lease = None
                    if not self.slots:
                        raise
                    self.slots -= 1
                    self.admission_reason = 'memory-admission'
            if self.slots:
                self._executor = ThreadPoolExecutor(max_workers=self.slots, thread_name_prefix='readmixer-zlib')
            budget._compression_service = self
        except BaseException:
            self._release()
            raise

    @property
    def pending_jobs(self):
        return len(self._pending)

    @property
    def actual_cpu_slots(self):
        return 1 + self.process_slots + self.slots

    def _check(self):
        if get_ident() != self._owner:
            raise ResourceError('compression coordination must remain on its owner thread')
        if self._closed:
            raise ResourceError('compression service is closed')
        self.cancellation.check()

    def open_lane(self, consume):
        self._check()
        if not callable(consume) or len(self._lanes) >= self._max_lanes:
            raise ResourceError('compression lane callback or admitted lane count is invalid')
        identity = self._next_lane
        self._next_lane = checked_add(identity, 1)
        lane = CompressionLane(self, identity, consume)
        self._lanes[identity] = lane
        return lane

    def _encode(self, payload, level, ordinal):
        if self.cancellation.cancelled:
            raise CancelledError()
        try:
            result = self.encoder(payload, level)
            if type(result) is not bytes or not 1 <= len(result) <= MAX_RESULT_BYTES:
                raise IntegrityError('compression worker returned an invalid bounded block')
            return result
        except BaseException as error:
            safe = self.cancellation.fail(error, task=ordinal, coordinator=False)
            raise safe from None

    def _submit(self, lane, payload, level):
        self._check()
        if lane.closed or self._lanes.get(lane.identity) is not lane:
            raise ResourceError('compression lane is closed or belongs to another service')
        if type(payload) is not bytes or not 1 <= len(payload) <= MAX_PAYLOAD_BYTES:
            raise ResourceError('compression payload exceeds its immutable bounded block size')
        if type(level) is not int or not 0 <= level <= 9:
            raise ConfigurationError('compression level must be in 0..9')
        try:
            if self.slots and len(self._pending) == self.capacity:
                self._drain_one()
            self._inflight = self._available[-1]
            self._available.pop()
            self._inflight = self._inflight.transfer('compression-job')
            ordinal = lane.submitted
            if self.slots:
                future = self._executor.submit(self._encode, payload, level, ordinal)
                self._pending.append(_Job(future, lane, ordinal, self._inflight))
                self._inflight = None
                lane.submitted = checked_add(lane.submitted, 1)
                self.peak_pending = max(self.peak_pending, len(self._pending))
            else:
                result = self._encode(payload, level, ordinal)
                self.cancellation.check()
                lane.consume(result)
                lane.submitted = checked_add(lane.submitted, 1)
                lane.drained = checked_add(lane.drained, 1)
                self.completed_jobs = checked_add(self.completed_jobs, 1)
                self._inflight = self._inflight.transfer('compression-window')
                self._available.append(self._inflight)
                self._inflight = None
                self.peak_pending = max(self.peak_pending, 1)
            self.submitted_jobs = checked_add(self.submitted_jobs, 1)
            return ordinal
        except BaseException as error:
            primary = self.cancellation.fail(error)
            self._abort_preserving(primary)
            raise primary from None

    def _drain_one(self):
        self._check()
        job = self._pending[0]
        try:
            result = job.future.result()
            self.cancellation.check()
            if job.ordinal != job.lane.drained:
                raise IntegrityError('compression lane completion order is inconsistent')
            job.lane.consume(result)
            job.lane.drained = checked_add(job.lane.drained, 1)
            self.completed_jobs = checked_add(self.completed_jobs, 1)
            self._inflight = job.lease
            self._pending.popleft()
            self._inflight = self._inflight.transfer('compression-window')
            self._available.append(self._inflight)
            self._inflight = None
        except BaseException as error:
            primary = self.cancellation.fail(error, task=job.ordinal)
            self._abort_preserving(primary)
            raise primary from None

    def _finish_lane(self, lane):
        self._check()
        while lane.pending:
            self._drain_one()
        self._lanes.pop(lane.identity)
        lane.closed = True

    def drain(self):
        self._check()
        while self._pending:
            self._drain_one()

    def _release(self):
        if self._inflight is not None:
            self._inflight.release()
            self._inflight = None
        while self._pending:
            self._inflight = self._pending[0].lease
            self._pending.popleft()
            self._inflight.release()
            self._inflight = None
        for lease in self._available:
            lease.release()
        self._available.clear()
        if self._thread_lease is not None:
            self._thread_lease.release()
            self._thread_lease = None
        for lane in self._lanes.values():
            lane.closed = True
        self._lanes.clear()
        self._metadata.release()
        if getattr(self.budget, '_compression_service', None) is self:
            self.budget._compression_service = None

    def _abort_preserving(self, primary):
        try:
            self.abort(primary)
        except BaseException as cleanup:
            # Retain ownership and the first cause if shutdown/cleanup itself
            # is interrupted. A later close/abort may finish the same cleanup.
            self.cancellation.fail(cleanup, stage='compression-cleanup')

    def abort(self, primary=None):
        if self._closed:
            return
        self.cancellation.cancel(primary)
        for job in self._pending:
            job.future.cancel()
        if self._executor is not None:
            # Every callable is a fixed-size native codec block; no producer
            # waits for a writer callback. Only the coordinator writes results.
            self._executor.shutdown(wait=True, cancel_futures=True)
            self._executor = None
        self._release()
        self._closed = True

    def close(self):
        if self._closed:
            return
        try:
            self.drain()
            if self._executor is not None:
                self._executor.shutdown(wait=True)
                self._executor = None
        except BaseException as error:
            primary = self.cancellation.fail(error)
            self._abort_preserving(primary)
            raise primary from None
        self._release()
        self._closed = True

    def __exit__(self, exc_type, exc, traceback):
        if exc is None:
            self.close()
        else:
            self._abort_preserving(exc)
