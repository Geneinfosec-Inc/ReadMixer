"""Private memory-transfer authority and coordinator-granted local subleases.

A MemoryRef owns a lease, rather than trusting a reusable caller-side handle.
Receipts are meaningful only after the channel owner validates the complete
receiver copy; they are not wire credentials or substitutes for authentication.
LocalWorkspace divides a reservation that the coordinator has already admitted.
Its metadata limit is the issued subset, not another global B/16 allowance.
"""
from dataclasses import dataclass, field
import hashlib
import hmac

from .config import U64_MAX
from .errors import ConfigurationError, IntegrityError, ResourceError
from .resources import Budget, Lease, MIB


def _uint(value):
    if type(value) is not int or not 0 <= value <= U64_MAX:
        raise ConfigurationError('worker ownership counters must be bounded unsigned integers')
    return value


def _run(value):
    if type(value) is not bytes or len(value) != 16:
        raise ConfigurationError('worker operation identity must have 16 bytes')
    return value


@dataclass(frozen=True, slots=True)
class TransferReceipt:
    """Exact, bounded identity of a validated receiver copy."""
    run_id: bytes = field(repr=False)
    owner_id: int
    generation: int
    task_id: int
    grant_id: int
    length: int
    digest: bytes = field(repr=False)

    def __post_init__(self):
        _run(self.run_id)
        for value in (self.owner_id, self.generation, self.task_id, self.grant_id, self.length):
            _uint(value)
        if type(self.digest) is not bytes or len(self.digest) != 32:
            raise ConfigurationError('worker transfer digest must have 32 bytes')


class MemoryRef:
    """Exclusive sender retention until a matching receiver acknowledgement.

    Construction transfers the supplied lease; that caller handle immediately
    loses authority. An unbound reference can be cancelled immediately. Once
    bound, cancel_after_reap may be called only after the consuming worker has
    stopped or its receiver copy has otherwise been safely discarded.
    """
    __slots__ = ('_run_id', '_owner_id', '_generation', '_length', '_digest',
                 '_payload', '_lease', '_reservation', '_binding', '_released')

    def __init__(self, payload, lease, run_id, owner_id, generation=0, *, budget=None):
        _run(run_id)
        _uint(owner_id)
        _uint(generation)
        if type(payload) is not bytes or not isinstance(lease, Lease):
            raise ConfigurationError('worker memory requires immutable bytes and an admitted lease')
        if budget is not None and lease._budget is not budget:
            raise ConfigurationError('worker memory lease belongs to another budget')
        self._run_id, self._owner_id, self._generation = run_id, owner_id, generation
        self._length, self._digest = len(payload), hashlib.sha256(payload).digest()
        self._payload, self._binding, self._released = payload, None, False
        with lease._budget._lock:
            previous = lease._budget._state
            try:
                current = lease._active()
                if current.bytes < len(payload):
                    raise ResourceError('worker memory exceeds its sender reservation')
                self._lease = lease.transfer('worker-memory')
                self._reservation = self._lease._active()
            except BaseException:
                # The receiver object has not escaped its constructor. Restore
                # sender authority if interrupted after Lease.transfer commits
                # but before this object can retain the returned handle.
                lease._budget._state = previous
                self._payload = None
                raise
            finally:
                previous = None

    @property
    def run_id(self):
        return self._run_id

    @property
    def owner_id(self):
        return self._owner_id

    @property
    def generation(self):
        return self._generation

    @property
    def length(self):
        return self._length

    @property
    def digest(self):
        return self._digest

    @property
    def lease(self):
        return self._lease

    def _live(self):
        if self._released:
            raise ResourceError('worker memory ownership has been released')
        if self._lease._active() is not self._reservation:
            raise ResourceError('worker memory reservation changed during transfer')

    @property
    def payload(self):
        with self._lease._budget._lock:
            self._live()
            if self._payload is None:
                raise ResourceError('worker memory sender retention has ended')
            return self._payload

    @property
    def released(self):
        with self._lease._budget._lock:
            return self._released

    @property
    def bound(self):
        with self._lease._budget._lock:
            return self._binding is not None

    def bind(self, task_id, grant_id):
        _uint(task_id)
        _uint(grant_id)
        with self._lease._budget._lock:
            self._live()
            if self._binding is not None:
                raise IntegrityError('worker memory is already assigned to a transfer')
            self._binding = (task_id, grant_id)
        return self

    def receipt(self):
        """Build the expected identity; caller must verify the receiver first."""
        with self._lease._budget._lock:
            self._live()
            if self._binding is None:
                raise IntegrityError('worker memory has no assigned transfer')
            return TransferReceipt(self.run_id, self.owner_id, self.generation,
                                   *self._binding, self.length, self.digest)

    def accept(self, receipt):
        with self._lease._budget._lock:
            self._live()
            if type(receipt) is not TransferReceipt or self._binding is None:
                raise IntegrityError('worker transfer acknowledgement is invalid')
            expected = (self.run_id, self.owner_id, self.generation, *self._binding, self.length)
            received = (receipt.run_id, receipt.owner_id, receipt.generation,
                        receipt.task_id, receipt.grant_id, receipt.length)
            if expected != received or not hmac.compare_digest(self.digest, receipt.digest):
                raise IntegrityError('worker transfer acknowledgement does not match ownership')
            self._release()

    def _release(self):
        # Discard the sender alias before refunding its reservation. A failed
        # release retains the lease and permits a later cleanup attempt.
        self._payload = None
        self._lease.release()
        self._released = True

    def cancel_after_reap(self):
        with self._lease._budget._lock:
            if not self._released:
                self._release()

    def __repr__(self):
        return f'MemoryRef(length={self.length}, bound={self.bound}, released={self.released})'


class _GrantBudget(Budget):
    """Reuse atomic Lease arithmetic with an already-admitted metadata subset."""
    def __init__(self, workspace_bytes, metadata_bytes, descriptors, max_leases):
        # The ordinary constructor enforces the invocation-wide B/16 policy.
        # A child grant can have a larger metadata fraction: the coordinator
        # already charged that fraction against the one shared ceiling. Use the
        # same registry/Lease machinery, then restrict both caps before exposing
        # or allocating any lease. There is no second baseline or RSS observer.
        super().__init__(max(workspace_bytes, metadata_bytes * 16), descriptors,
                         metadata_limit_bytes=metadata_bytes, max_leases=max_leases)
        self.limit_bytes = self._admission_limit = workspace_bytes
        self.metadata_limit_bytes = metadata_bytes


class LocalWorkspace:
    """Fixed worker-local subdivision of one coordinator-issued reservation.

    Registry charges are included in both byte and metadata limits. Returned
    Lease handles support the existing atomic split/transfer/resize/release
    operations, all bounded by this same local grant. Growing the grant itself
    is intentionally unsupported.
    """
    __slots__ = ('_budget', '_closed')

    def __init__(self, workspace_bytes, metadata_bytes, descriptors=0, *, max_leases=64):
        for value in (workspace_bytes, metadata_bytes, descriptors, max_leases):
            _uint(value)
        if (workspace_bytes == 0 or metadata_bytes > min(workspace_bytes, 64 * MIB)
                or not 1 <= max_leases <= 1024):
            raise ConfigurationError('worker workspace grant is invalid')
        self._budget = _GrantBudget(workspace_bytes, metadata_bytes, descriptors, max_leases)
        self._closed = False

    @property
    def limit_bytes(self):
        return self._budget.limit_bytes

    @property
    def metadata_limit_bytes(self):
        return self._budget.metadata_limit_bytes

    @property
    def descriptor_limit(self):
        return self._budget.descriptor_limit

    @property
    def used_bytes(self):
        return self._budget.used_bytes

    @property
    def used_metadata_bytes(self):
        return self._budget.used_metadata_bytes

    @property
    def used_descriptors(self):
        return self._budget.used_descriptors

    @property
    def active_leases(self):
        return self._budget.active_leases

    def acquire(self, category, bytes, descriptors=0, *, metadata_bytes=0):
        with self._budget._lock:
            if self._closed:
                raise ResourceError('worker workspace is closed')
            return self._budget.acquire(category, bytes, descriptors, metadata_bytes=metadata_bytes)

    def close(self):
        with self._budget._lock:
            if self._budget.active_leases:
                raise ResourceError('worker workspace still owns live subleases')
            self._closed = True

    def __enter__(self):
        if self._closed:
            raise ResourceError('worker workspace is closed')
        return self

    def __exit__(self, *_exc):
        self.close()

    def __repr__(self):
        return f'LocalWorkspace(closed={self._closed}, active_leases={self.active_leases})'
