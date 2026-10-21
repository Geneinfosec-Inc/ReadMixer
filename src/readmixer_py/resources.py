"""Read-only resource detection and bounded admission, not an OS RSS limit.

The initial terminal envelope is versioned but remains unqualified. Execution
stages must reserve their entire working set before using these primitives.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path, PurePosixPath
import re
import sys
from threading import RLock

from .config import MAX_RECORD_BYTES, U64_MAX
from .errors import ConfigurationError, ResourceError

MIB = 1 << 20
GIB = 1 << 30
TERMINAL_OVERHEAD = 32 * MIB
MAX_TERMINAL_PAIRS = 32_768
MAX_TERMINAL_ARENA = 64 * MIB
ENVELOPE_VERSION = 2
PORTABLE_DESCRIPTORS = 1024
DESCRIPTOR_RESERVE = 32
FALLBACK_OPEN_DESCRIPTORS = 64
_OBSERVATION_FILE_LIMIT = 1 * MIB


def _integer(value: int, name: str, *, positive: bool = False) -> int:
    if type(value) is not int or not (int(positive) <= value <= U64_MAX):
        raise ConfigurationError(f"{name} must be a bounded {'positive' if positive else 'nonnegative'} integer")
    return value


def checked_add(left: int, right: int) -> int:
    """Add unsigned counters before allocation, rejecting u64 overflow."""
    _integer(left, "left counter")
    _integer(right, "right counter")
    if left > U64_MAX - right:
        raise ResourceError("resource counter addition exceeds u64")
    return left + right


def checked_multiply(left: int, right: int) -> int:
    _integer(left, "left counter")
    _integer(right, "right counter")
    if right and left > U64_MAX // right:
        raise ResourceError("resource counter multiplication exceeds u64")
    return left * right


@dataclass(frozen=True)
class CgroupMemory:
    limit_bytes: int | None
    current_bytes: int
    version: int = 2

    def __post_init__(self) -> None:
        if self.version not in (1, 2):
            raise ConfigurationError("unsupported cgroup version")
        if self.limit_bytes is not None:
            _integer(self.limit_bytes, "cgroup limit")
        _integer(self.current_bytes, "cgroup usage")


@dataclass(frozen=True)
class ResourceObservations:
    """Injectable facts; cgroups include every visible applicable ancestor."""

    available_memory: int
    coordinator_rss: int
    cgroups: tuple[CgroupMemory, ...] = ()
    cpu_count: int = 1
    descriptor_soft_limit: int | None = None
    open_descriptors: int | None = None
    platform: str = sys.platform

    def __post_init__(self) -> None:
        _integer(self.available_memory, "available memory")
        _integer(self.coordinator_rss, "coordinator RSS")
        _integer(self.cpu_count, "CPU count", positive=True)
        if not isinstance(self.cgroups, tuple) or len(self.cgroups) > 1024:
            raise ConfigurationError("cgroup observations must be a bounded tuple")
        if any(not isinstance(group, CgroupMemory) for group in self.cgroups):
            raise ConfigurationError("invalid cgroup observations")
        if self.descriptor_soft_limit is not None:
            _integer(self.descriptor_soft_limit, "descriptor soft limit")
        if self.open_descriptors is not None:
            _integer(self.open_descriptors, "open descriptor count")


@dataclass(frozen=True)
class ResourcePlan:
    requested_max_ram: int | None
    effective_max_ram: int
    terminal_bytes: int
    requested_workers: int | None
    worker_limit: int
    descriptor_allowance: int
    coordinator_rss: int
    available_headroom: int
    reserved_headroom: int
    metadata_bytes: int
    request_clamped: bool
    detection_sources: tuple[str, ...]
    envelope_version: int = ENVELOPE_VERSION
    envelope_qualified: bool = False

    def to_dict(self) -> dict[str, object]:
        """Bounded public facts; never expose cgroup paths or process details."""
        return {
            "requested_max_ram": self.requested_max_ram,
            "effective_max_ram": self.effective_max_ram,
            "terminal_bytes": self.terminal_bytes,
            "requested_workers": self.requested_workers,
            "worker_limit": self.worker_limit,
            "descriptor_allowance": self.descriptor_allowance,
            "coordinator_rss": self.coordinator_rss,
            "available_headroom": self.available_headroom,
            "reserved_headroom": self.reserved_headroom,
            "metadata_bytes": self.metadata_bytes,
            "request_clamped": self.request_clamped,
            "detection_sources": list(self.detection_sources),
            "envelope_version": self.envelope_version,
            "envelope_qualified": self.envelope_qualified,
        }

    def terminal_fits(self, pair_count: int, arena_bytes: int) -> bool:
        return terminal_fits(self.terminal_bytes, pair_count, arena_bytes)

    def require_execution(self, max_record_bytes: int = MIB) -> None:
        """Check the smallest execution path, separately from inspection."""
        _integer(max_record_bytes, "record limit", positive=True)
        if max_record_bytes > MAX_RECORD_BYTES:
            raise ConfigurationError("record limit exceeds the complete frame u32 field")
        frame_bytes = checked_add(40, checked_multiply(2, max_record_bytes))
        if self.terminal_bytes <= TERMINAL_OVERHEAD or not self.terminal_fits(1, frame_bytes):
            raise ResourceError("terminal allowance cannot admit one maximum-size pair")
        if self.effective_max_ram - self.coordinator_rss < frame_bytes + 128 + TERMINAL_OVERHEAD:
            raise ResourceError("aggregate allowance cannot admit baseline and one terminal")
        if self.descriptor_allowance < 8:
            raise ResourceError("descriptor allowance cannot admit one operation")


def terminal_fits(terminal_bytes: int, pair_count: int, arena_bytes: int) -> bool:
    _integer(terminal_bytes, "terminal allowance")
    _integer(pair_count, "pair count")
    _integer(arena_bytes, "arena bytes")
    if pair_count > MAX_TERMINAL_PAIRS or arena_bytes > MAX_TERMINAL_ARENA:
        return False
    working_bytes = checked_add(checked_add(arena_bytes, checked_multiply(128, pair_count)), TERMINAL_OVERHEAD)
    return working_bytes <= terminal_bytes


def _headroom(observations: ResourceObservations) -> tuple[int, tuple[str, ...]]:
    headroom = observations.available_memory
    sources = ["psutil:available", "psutil:coordinator-rss"]
    if observations.platform.startswith("linux"):
        for group in observations.cgroups:
            if group.limit_bytes is None:
                source = f"cgroup-v{group.version}:unlimited"
            else:
                headroom = min(headroom, max(0, group.limit_bytes - group.current_bytes))
                source = f"cgroup-v{group.version}:headroom"
            if source not in sources:
                sources.append(source)
        if not observations.cgroups:
            sources.append("cgroup:no-visible-memory-controller")
    else:
        sources.append("cgroup:not-applicable")
    return headroom, tuple(sources)


def resolve_resources(
    max_ram: int | None = None,
    workers: int | None = None,
    *,
    observations: ResourceObservations | None = None,
    require_execution: bool = False,
    max_record_bytes: int = MIB,
) -> ResourcePlan:
    if max_ram is not None:
        _integer(max_ram, "maximum RAM", positive=True)
    if workers is not None:
        _integer(workers, "workers", positive=True)
    facts = observations if observations is not None else observe_resources()
    headroom, sources = _headroom(facts)
    reserve = min(2 * GIB, headroom // 5)
    detected = checked_add(facts.coordinator_rss, headroom - reserve)
    effective = detected if max_ram is None else min(max_ram, detected)
    if effective < facts.coordinator_rss:
        raise ResourceError(f"coordinator baseline requires {facts.coordinator_rss} bytes; allowance is {effective}")
    soft = facts.descriptor_soft_limit
    opened = facts.open_descriptors
    if soft is None:
        soft = PORTABLE_DESCRIPTORS
        sources += ("descriptors:portable-limit",)
    else:
        sources += ("descriptors:soft-limit",)
    if opened is None:
        opened = FALLBACK_OPEN_DESCRIPTORS
        sources += ("descriptors:conservative-baseline",)
    else:
        sources += ("descriptors:measured",)
    descriptors = min(PORTABLE_DESCRIPTORS, max(0, soft - opened - DESCRIPTOR_RESERVE))
    plan = ResourcePlan(
        requested_max_ram=max_ram,
        effective_max_ram=effective,
        terminal_bytes=min(128 * MIB, effective // 4),
        requested_workers=workers,
        worker_limit=min(workers or facts.cpu_count, facts.cpu_count),
        descriptor_allowance=descriptors,
        coordinator_rss=facts.coordinator_rss,
        available_headroom=headroom,
        reserved_headroom=reserve,
        metadata_bytes=min(64 * MIB, effective // 16),
        request_clamped=max_ram is not None and max_ram > detected,
        detection_sources=sources,
    )
    if require_execution:
        plan.require_execution(max_record_bytes)
    return plan


def _read_fact(path: Path, *, optional: bool = False) -> str | None:
    try:
        with path.open("rb") as stream:
            data = stream.read(_OBSERVATION_FILE_LIMIT + 1)
    except FileNotFoundError:
        if optional:
            return None
        raise ResourceError("resource observation is unavailable") from None
    except OSError as exc:
        raise ResourceError("resource observation could not be read") from exc
    if len(data) > _OBSERVATION_FILE_LIMIT:
        raise ResourceError("resource observation exceeds its byte limit")
    try:
        return data.decode("ascii")
    except UnicodeError as exc:
        raise ResourceError("resource observation is malformed") from exc


def parse_cgroup_limit(value: str, version: int, *, page_size: int = 4096) -> int | None:
    """Recognize v2 'max' and v1's page-rounded signed-long sentinel."""
    value = value.strip()
    if version == 2 and value == "max":
        return None
    if version not in (1, 2) or not re.fullmatch(r"[0-9]{1,20}", value):
        raise ResourceError("cgroup memory limit is malformed")
    result = int(value)
    if result > U64_MAX:
        raise ResourceError("cgroup memory limit exceeds u64")
    if version == 1 and result >= ((1 << 63) - 1) // page_size * page_size:
        return None
    return result


def _mount_path(value: str) -> str:
    return re.sub(r"\\(040|011|012|134)", lambda match: chr(int(match.group(1), 8)), value)


def _cgroup_memory(proc_root: Path = Path("/proc")) -> tuple[CgroupMemory, ...]:
    memberships = _read_fact(proc_root / "self/cgroup", optional=True)
    if memberships is None:
        return ()
    mounts = _read_fact(proc_root / "self/mountinfo")
    assert mounts is not None
    groups: list[tuple[int, PurePosixPath]] = []
    for line in memberships.splitlines():
        fields = line.split(":", 2)
        if len(fields) != 3:
            raise ResourceError("cgroup membership is malformed")
        if fields[0] == "0" and fields[1] == "":
            version = 2
        elif "memory" in fields[1].split(","):
            version = 1
        else:
            continue
        group = PurePosixPath(fields[2])
        if not group.is_absolute() or ".." in group.parts:
            raise ResourceError("cgroup membership is malformed")
        groups.append((version, group))
        if len(groups) > 1024:
            raise ResourceError("too many cgroup memory memberships")
    observed: list[CgroupMemory] = []
    visited: set[tuple[int, Path]] = set()
    for line in mounts.splitlines():
        left, separator, right = line.partition(" - ")
        before, after = left.split(), right.split()
        if not separator or len(before) < 6 or len(after) < 3:
            raise ResourceError("cgroup mount information is malformed")
        version = 2 if after[0] == "cgroup2" else 1 if after[0] == "cgroup" and "memory" in after[2].split(",") else 0
        if not version:
            continue
        root = PurePosixPath(_mount_path(before[3]))
        mount = Path(_mount_path(before[4]))
        for group_version, group in groups:
            if group_version != version:
                continue
            try:
                relative = group.relative_to(root)
            except ValueError:
                # In a cgroup namespace, '/' identifies the namespace root.
                if group != PurePosixPath("/"):
                    continue
                relative = PurePosixPath(".")
            current = mount / relative
            for _depth in range(256):
                identity = (version, current)
                if identity not in visited:
                    if len(visited) >= 1024:
                        raise ResourceError("too many visible cgroup ancestors")
                    visited.add(identity)
                    limit_file = "memory.max" if version == 2 else "memory.limit_in_bytes"
                    usage_file = "memory.current" if version == 2 else "memory.usage_in_bytes"
                    limit = _read_fact(current / limit_file, optional=True)
                    if limit is not None:
                        page_size = os.sysconf("SC_PAGE_SIZE")
                        parsed_limit = parse_cgroup_limit(limit, version, page_size=page_size)
                        usage = _read_fact(current / usage_file)
                        assert usage is not None
                        parsed_usage = parse_cgroup_limit(usage, 2)
                        if parsed_usage is None:
                            raise ResourceError("cgroup memory usage is malformed")
                        observed.append(CgroupMemory(parsed_limit, parsed_usage, version))
                if current == mount:
                    break
                current = current.parent
            else:
                raise ResourceError("cgroup hierarchy exceeds its depth limit")
    return tuple(observed)


def observe_resources() -> ResourceObservations:
    """Detect resources lazily so CLI help/version import no runtime libraries."""
    import psutil

    try:
        process = psutil.Process()
        available = psutil.virtual_memory().available
        rss = process.memory_info().rss
    except (OSError, psutil.Error) as exc:
        raise ResourceError("system memory observation failed") from exc
    try:
        opened = process.num_fds()
    except (AttributeError, OSError, psutil.Error):
        opened = None
    try:
        import resource

        soft, _hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        soft = U64_MAX if soft == resource.RLIM_INFINITY else soft
    except (ImportError, OSError, ValueError):
        soft = None
    cgroups = _cgroup_memory() if sys.platform.startswith("linux") else ()
    return ResourceObservations(
        available_memory=available,
        coordinator_rss=rss,
        cgroups=cgroups,
        cpu_count=os.cpu_count() or 1,
        descriptor_soft_limit=soft,
        open_descriptors=opened,
        platform=sys.platform,
    )


# These admission envelopes cover the live registry and one replacement registry
# during an atomic update, including Lease/_Reservation objects. They are not
# calibrated RSS measurements. An empty Budget retains only baseline control
# state; its active registry and this reservation disappear together.
BUDGET_REGISTRY_BYTES = 4096
LEASE_REGISTRY_BYTES = 512


@dataclass(frozen=True, slots=True)
class _Reservation:
    owner: str
    bytes: int
    descriptors: int
    metadata_bytes: int


@dataclass(frozen=True, slots=True)
class _BudgetState:
    leases: dict
    used_bytes: int
    used_descriptors: int
    used_metadata_bytes: int
    peak_metadata_bytes: int


def _registry_bytes(count: int) -> int:
    return BUDGET_REGISTRY_BYTES + count * LEASE_REGISTRY_BYTES if count else 0


def _lease_amounts(bytes: int, descriptors: int, metadata_bytes: int, *, empty=False) -> None:
    _integer(bytes, "lease bytes")
    _integer(descriptors, "lease descriptors")
    _integer(metadata_bytes, "lease metadata bytes")
    if metadata_bytes > bytes:
        raise ConfigurationError("lease metadata bytes must be a subset of lease bytes")
    if not empty and bytes == descriptors == 0:
        raise ConfigurationError("a lease must reserve bytes or descriptors")


class Budget:
    """Atomic RAM/descriptor/shared-metadata admission with bounded bookkeeping.

    Each live registry is explicitly charged without recursively leasing its
    own accounting objects. State replacement is the sole ownership commit:
    handles derive their authority from the currently published registry.
    """

    def __init__(self, limit_bytes: int, descriptors: int = PORTABLE_DESCRIPTORS, *,
                 baseline_bytes: int = 0, max_leases: int = 1024,
                 metadata_limit_bytes: int | None = None):
        self.limit_bytes = _integer(limit_bytes, "budget", positive=True)
        self.descriptor_limit = _integer(descriptors, "descriptor allowance")
        self.baseline_bytes = _integer(baseline_bytes, "baseline")
        self.max_leases = _integer(max_leases, "lease registry limit", positive=True)
        if self.max_leases > 100_000:
            raise ConfigurationError("lease registry limit exceeds 100000")
        derived = min(64 * MIB, self.limit_bytes // 16)
        self.metadata_limit_bytes = (derived if metadata_limit_bytes is None else
                                     _integer(metadata_limit_bytes, "metadata allowance"))
        if self.metadata_limit_bytes > derived:
            raise ConfigurationError("metadata allowance exceeds the shared budget ceiling")
        if baseline_bytes > limit_bytes:
            raise ResourceError("baseline exceeds budget")
        self._state = _BudgetState({}, baseline_bytes, 0, 0, 0)
        self._admission_limit = limit_bytes
        self._untracked_bytes = 0
        self._lock = RLock()

    @property
    def used_bytes(self) -> int:
        with self._lock:
            return self._state.used_bytes

    @property
    def remaining_bytes(self) -> int:
        with self._lock:
            return max(0, self._admission_limit - self._state.used_bytes - self._untracked_bytes)

    @property
    def used_descriptors(self) -> int:
        with self._lock:
            return self._state.used_descriptors

    @property
    def used_metadata_bytes(self) -> int:
        with self._lock:
            return self._state.used_metadata_bytes

    @property
    def remaining_metadata_bytes(self) -> int:
        with self._lock:
            return self.metadata_limit_bytes - self._state.used_metadata_bytes

    @property
    def peak_metadata_bytes(self) -> int:
        with self._lock:
            return self._state.peak_metadata_bytes

    @property
    def registry_bytes(self) -> int:
        with self._lock:
            return _registry_bytes(len(self._state.leases))

    @property
    def active_leases(self) -> int:
        with self._lock:
            return len(self._state.leases)

    @property
    def _leases(self):
        """Read-only compatibility view for existing internal diagnostics/tests."""
        with self._lock:
            return self._state.leases.keys()

    def _admit(self, category: str, count: int, bytes_delta: int,
               descriptors_delta: int, metadata_delta: int) -> tuple[int, int, int, int]:
        """Check an update before allocating its handles or replacement registry."""
        try:
            previous = self._state
            if count > self.max_leases:
                raise ResourceError("active lease registry is full")
            registry_delta = _registry_bytes(count) - _registry_bytes(len(previous.leases))
            bytes_delta += registry_delta
            metadata_delta += registry_delta
            available = max(0, self._admission_limit - previous.used_bytes - self._untracked_bytes)
            available_fds = self.descriptor_limit - previous.used_descriptors
            available_metadata = self.metadata_limit_bytes - previous.used_metadata_bytes
            if bytes_delta > available:
                raise ResourceError(f"{category}: requested {bytes_delta} bytes; available {available}")
            if descriptors_delta > available_fds:
                raise ResourceError(f"{category}: requested {descriptors_delta} descriptors; available {available_fds}")
            if metadata_delta > available_metadata:
                raise ResourceError(f"{category}: requested {metadata_delta} metadata bytes; available {available_metadata}")
            used_bytes = previous.used_bytes + bytes_delta
            used_descriptors = previous.used_descriptors + descriptors_delta
            used_metadata = previous.used_metadata_bytes + metadata_delta
            # Deltas may be negative when an owner shrinks or releases. The public
            # amounts and retained reservation determine their nonnegative bounds.
            _integer(used_bytes, "admitted bytes")
            _integer(used_descriptors, "admitted descriptors")
            _integer(used_metadata, "admitted metadata")
            return used_bytes, used_descriptors, used_metadata, max(previous.peak_metadata_bytes, used_metadata)
        finally:
            previous = None

    def _publish_state(self, state: _BudgetState) -> None:
        """The single authority handoff; callers roll it back on interruption."""
        try:
            self._state = state
        finally:
            state = None

    def _commit(self, state: _BudgetState) -> None:
        previous = self._state
        try:
            self._publish_state(state)
        except BaseException:
            # A failure immediately after assignment must not orphan a handle
            # which was never returned. No other thread can enter under this lock.
            if self._state is state:
                self._state = previous
            raise
        finally:
            previous = state = None

    def acquire(self, category: str, bytes: int, descriptors: int = 0, *,
                metadata_bytes: int = 0) -> Lease:
        _owner(category)
        _lease_amounts(bytes, descriptors, metadata_bytes)
        with self._lock:
            try:
                previous = self._state
                counters = self._admit(category, len(previous.leases) + 1, bytes, descriptors, metadata_bytes)
                lease = Lease(self, category, bytes, descriptors, metadata_bytes)
                leases = previous.leases.copy()
                leases[lease] = lease._retired
                self._commit(_BudgetState(leases, *counters))
                return lease
            finally:
                previous = leases = None

    def checkpoint(self, plan: ResourcePlan, observations: ResourceObservations | None = None) -> None:
        """Cancel on external pressure without changing B, M, or topology."""
        facts = observations if observations is not None else observe_resources()
        headroom, _sources = _headroom(facts)
        available = checked_add(facts.coordinator_rss, headroom - min(2 * GIB, headroom // 5))
        with self._lock:
            self._admission_limit = min(self.limit_bytes, plan.effective_max_ram, available)
            # RSS already includes some admitted objects. Only its excess is
            # untracked; future process stages must supply process-group facts.
            self._untracked_bytes = max(0, facts.coordinator_rss - self._state.used_bytes)
            if max(self._state.used_bytes, facts.coordinator_rss) > self._admission_limit:
                raise ResourceError("external memory pressure exceeds the admitted working set")


def _owner(value: str) -> None:
    if not isinstance(value, str) or not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", value):
        raise ConfigurationError("lease category must be a bounded safe identifier")


class Lease:
    """One explicit reservation; the budget registry is ownership authority."""

    __slots__ = ("_budget", "_retired")

    def __init__(self, budget: Budget, owner: str, bytes: int, descriptors: int,
                 metadata_bytes: int = 0):
        self._budget = budget
        self._retired = _Reservation(owner, bytes, descriptors, metadata_bytes)

    def _reservation(self) -> _Reservation:
        return self._budget._state.leases.get(self, self._retired)

    def _active(self) -> _Reservation:
        try:
            return self._budget._state.leases[self]
        except KeyError:
            raise ResourceError("a released lease cannot be used") from None

    @property
    def owner(self) -> str:
        with self._budget._lock:
            return self._reservation().owner

    @property
    def bytes(self) -> int:
        with self._budget._lock:
            return self._reservation().bytes

    @property
    def descriptors(self) -> int:
        with self._budget._lock:
            return self._reservation().descriptors

    @property
    def metadata_bytes(self) -> int:
        with self._budget._lock:
            return self._reservation().metadata_bytes

    @property
    def released(self) -> bool:
        with self._budget._lock:
            return self not in self._budget._state.leases

    def transfer(self, owner: str) -> Lease:
        """Return a new handle; this handle loses authority in the same commit."""
        _owner(owner)
        budget = self._budget
        with budget._lock:
            try:
                current = self._active()
                previous = budget._state
                replacement = Lease(budget, owner, current.bytes, current.descriptors, current.metadata_bytes)
                leases = previous.leases.copy()
                del leases[self]
                leases[replacement] = replacement._retired
                self._retired = current
                budget._commit(_BudgetState(leases, previous.used_bytes, previous.used_descriptors,
                                           previous.used_metadata_bytes, previous.peak_metadata_bytes))
                return replacement
            finally:
                previous = leases = None

    def resize(self, bytes: int, descriptors: int = 0, *, metadata_bytes: int = 0) -> Lease:
        """Reserve growth before allocation; callers discard objects before shrink."""
        _lease_amounts(bytes, descriptors, metadata_bytes, empty=True)
        budget = self._budget
        with budget._lock:
            try:
                current = self._active()
                previous = budget._state
                counters = budget._admit(current.owner, len(previous.leases), bytes - current.bytes,
                                        descriptors - current.descriptors, metadata_bytes - current.metadata_bytes)
                replacement = _Reservation(current.owner, bytes, descriptors, metadata_bytes)
                leases = previous.leases.copy()
                leases[self] = replacement
                budget._commit(_BudgetState(leases, *counters))
                return self
            finally:
                previous = leases = None

    def split(self, owner: str, bytes: int, descriptors: int = 0, *, metadata_bytes: int = 0) -> Lease:
        """Hand off part of a reservation, admitting the additional handle first."""
        _owner(owner)
        _lease_amounts(bytes, descriptors, metadata_bytes)
        budget = self._budget
        with budget._lock:
            try:
                current = self._active()
                if (bytes > current.bytes or descriptors > current.descriptors
                        or metadata_bytes > current.metadata_bytes
                        or current.metadata_bytes - metadata_bytes > current.bytes - bytes):
                    raise ConfigurationError("split amounts must leave valid subsets of the original lease")
                previous = budget._state
                counters = budget._admit(owner, len(previous.leases) + 1, 0, 0, 0)
                child = Lease(budget, owner, bytes, descriptors, metadata_bytes)
                remainder = _Reservation(current.owner, current.bytes - bytes,
                                         current.descriptors - descriptors, current.metadata_bytes - metadata_bytes)
                leases = previous.leases.copy()
                leases[self] = remainder
                leases[child] = child._retired
                budget._commit(_BudgetState(leases, *counters))
                return child
            finally:
                previous = leases = None

    def release(self) -> None:
        budget = self._budget
        with budget._lock:
            try:
                if self not in budget._state.leases:
                    return
                current = self._active()
                previous = budget._state
                counters = budget._admit(current.owner, len(previous.leases) - 1, -current.bytes,
                                        -current.descriptors, -current.metadata_bytes)
                leases = previous.leases.copy()
                del leases[self]
                self._retired = current
                budget._commit(_BudgetState(leases, *counters))
            finally:
                previous = leases = None

    def __enter__(self) -> Lease:
        with self._budget._lock:
            self._active()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.release()
