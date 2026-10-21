"""Per-filesystem space reservations around actual writes, without size guesses."""

from contextlib import AbstractContextManager
from dataclasses import dataclass
import os
import stat
from pathlib import Path
from threading import RLock

from .errors import ConfigurationError, ResourceError
from .resources import checked_add

_RESERVE = 64 << 20
_MAX_DEVICES = 1024
_CONTROL_METADATA = 1024
_DEVICE_METADATA = 512
_TICKET_METADATA = 256


def _size(value):
    if type(value) is not int or not 0 <= value < 1 << 64:
        raise ConfigurationError("disk byte count must be an unsigned 64-bit integer")
    return value


@dataclass
class _Device:
    pending: int = 0
    ownership: tuple[int, int] = (0, 0)
    peak: int = 0
    tickets: int = 0


    @property
    def committed(self):
        return self.ownership[0]

    @committed.setter
    def committed(self, amount):
        self.ownership = (amount, self.ownership[1])


class DiskRefund:
    """Bounded caller-owned refund token; commit is safe after an interrupted return."""
    __slots__ = ('owner', 'device', 'amount', 'done')

    def __init__(self, owner, device, amount):
        self.owner, self.device, self.amount = owner, device, _size(amount)
        self.done = False

    def commit(self):
        self.owner._refund(self)


class DiskBudget(AbstractContextManager):
    """Coordinate this run's writes on shared devices.

    Current free space already reflects committed files. Only outstanding
    reservations are subtracted; committed counters track ownership and peaks,
    not a second charge against filesystem free space. Other processes can still
    consume space after admission: callers must propagate ENOSPC and short writes.
    """

    def __init__(self, *, budget=None, max_devices=64, max_reservations=1024):
        if type(max_devices) is not int or not 1 <= max_devices <= _MAX_DEVICES:
            raise ConfigurationError("disk device registry limit must be in 1..1024")
        if type(max_reservations) is not int or not 1 <= max_reservations <= 100_000:
            raise ConfigurationError("disk reservation limit must be in 1..100000")
        self.budget = budget
        self._metadata = None
        self._metadata_floor = 0
        self._closed = False
        self._closed_committed = self._closed_peak = 0
        self._max_devices = max_devices
        self._max_reservations = max_reservations
        self._devices = {}
        self._active = 0
        self._lock = RLock()
        self._pending_refund = None

    def _charge(self, devices, tickets):
        if self.budget is None:
            return
        amount = max(self._metadata_floor,
                     _CONTROL_METADATA + _DEVICE_METADATA * devices + _TICKET_METADATA * tickets)
        if self._metadata is None:
            self._metadata = self.budget.acquire("disk-registry", amount, metadata_bytes=amount)
        else:
            self._metadata.resize(amount, metadata_bytes=amount)

    def prepare(self):
        """Reserve one upcoming device/ticket before a spill exhausts RAM."""
        with self._lock:
            self._check_open()
            floor = (_CONTROL_METADATA + _DEVICE_METADATA * min(self._max_devices, len(self._devices) + 1)
                     + _TICKET_METADATA * min(self._max_reservations, self._active + 1))
            previous = self._metadata_floor
            self._metadata_floor = max(previous, floor)
            try:
                self._charge(len(self._devices), self._active)
            except BaseException:
                self._metadata_floor = previous
                raise

    def _check_open(self):
        if self._closed:
            raise ResourceError("disk budget is closed")

    def _facts(self, target):
        try:
            if type(target) is int:
                metadata = os.fstat(target)
                capacity = os.fstatvfs(target)
            elif isinstance(target, (str, Path)):
                metadata = os.stat(target, follow_symlinks=False)
                capacity = os.statvfs(target)
            else:
                raise ConfigurationError("disk target must be a directory descriptor or path")
        except OSError as exc:
            raise ResourceError("cannot query capacity on the target filesystem") from exc
        if not stat.S_ISDIR(metadata.st_mode):
            raise ResourceError("disk target must be a nonsymlink directory")
        free = capacity.f_bavail * capacity.f_frsize
        if free < 0:
            raise ResourceError("filesystem returned invalid available space")
        return metadata.st_dev, free

    def reserve(self, target, byte_increment, *, retained=None):
        """Reserve growth, optionally retaining the ticket before admission.

        A coordinator callback can install its cleanup owner before this method
        returns. Failure inside that callback leaves an inactive, uncommitted
        ticket and no disk debit. A later interrupted return leaves an active
        ticket with the retained owner for observed-growth reconciliation.
        """
        if retained is not None and not callable(retained):
            raise ConfigurationError("disk reservation retention must be callable")
        amount = _size(byte_increment)
        with self._lock:
            self._check_open()
            device, free = self._facts(target)
            state = self._devices.get(device)
            if state is None:
                if len(self._devices) >= self._max_devices:
                    raise ResourceError("disk device registry is full")
            if self._active >= self._max_reservations:
                raise ResourceError("disk reservation registry is full")
            pending = checked_add(state.pending if state is not None else 0, amount)
            required = checked_add(pending, max(_RESERVE, amount // 100))
            if free < required:
                raise ResourceError(f"disk growth needs {required} bytes including reserve; available {free}")
            peak = max(state.peak if state is not None else 0,
                       checked_add(state.committed if state is not None else 0, pending))
            new_device = state is None
            old_metadata = self._metadata.bytes if self._metadata is not None else None
            self._charge(len(self._devices) + int(state is None), self._active + 1)
            ticket = None
            if state is None:
                state = _Device()
            before = (state.pending, state.tickets, state.peak, self._active)
            try:
                ticket = DiskReservation(self, device, amount)
                ticket._active = False
                if retained is not None:
                    retained(ticket)
                # The retained caller owns cleanup before any counters change.
                self._devices[device] = state
                state.pending = pending
                state.tickets = before[1] + 1
                self._active = before[3] + 1
                state.peak = peak
                ticket._active = True
                return ticket
            except BaseException:
                state.pending, state.tickets, state.peak, self._active = before
                if ticket is not None:
                    ticket._active = False
                if new_device:
                    self._devices.pop(device, None)
                if self._metadata is not None:
                    if old_metadata is None:
                        self._metadata.release()
                        self._metadata = None
                    else:
                        self._metadata.resize(old_metadata, metadata_bytes=old_metadata)
                raise

    def release(self, target, byte_count):
        """Release ownership only after deletion; never requires a capacity query."""
        try:
            device = os.fstat(target).st_dev if type(target) is int else os.stat(target, follow_symlinks=False).st_dev
        except OSError as exc:
            raise ResourceError("cannot resolve released filesystem ownership") from exc
        self.release_device(device, byte_count)

    def _finish_refund(self):
        if self._pending_refund is None:
            return
        token, state, amount_after, serial = self._pending_refund
        if state.ownership[1] < serial:
            # Debit and commit marker change in one immutable state assignment.
            state.ownership = (amount_after, serial)
        token.done = True
        self._pending_refund = None

    def _refund(self, token):
        with self._lock:
            self._check_open()
            # Finish a previous interrupted handoff before any later refund.
            self._finish_refund()
            if token.done:
                return
            state = self._devices.get(token.device)
            if token.amount == 0 and state is None:
                token.done = True
                return
            if state is None or token.amount > state.committed:
                raise ResourceError("disk ownership release exceeds committed bytes")
            self._pending_refund = (token, state, state.committed - token.amount,
                                    state.ownership[1] + 1)
            self._finish_refund()

    def release_device(self, device, byte_count):
        amount = _size(byte_count)
        with self._lock:
            self._check_open()
            self._finish_refund()
            state = self._devices.get(device)
            if state is None or amount > state.committed:
                raise ResourceError("disk ownership release exceeds committed bytes")
            state.committed -= amount

    @property
    def pending_bytes(self):
        with self._lock:
            return sum(v.pending for v in self._devices.values())

    @property
    def committed_bytes(self):
        with self._lock:
            return self._closed_committed if self._closed else sum(v.committed for v in self._devices.values())

    @property
    def peak_bytes(self):
        with self._lock:
            return self._closed_peak if self._closed else sum(v.peak for v in self._devices.values())


    def close(self):
        with self._lock:
            if self._closed:
                return
            self._finish_refund()
            if self._active:
                raise ResourceError("disk budget still has active reservations")
            self._closed_committed = sum(v.committed for v in self._devices.values())
            self._closed_peak = sum(v.peak for v in self._devices.values())
            self._devices.clear()
            if self._metadata is not None:
                self._metadata.release()
                self._metadata = None
            self._closed = True

    def __enter__(self):
        self._check_open()
        return self

    def __exit__(self, exc_type, exc, traceback):
        try:
            self.close()
        except BaseException:
            if exc is None:
                raise
            exc.add_note("disk bookkeeping cleanup also failed; primary error preserved")


class DiskReservation:
    __slots__ = ("_owner", "device", "bytes", "_active", "committed", "committed_bytes")

    def __init__(self, owner, device, amount):
        self._owner, self.device, self.bytes = owner, device, amount
        self._active, self.committed = True, False
        self.committed_bytes = 0

    def commit(self, actual_bytes=None):
        """Account bytes written (including a partial failed write) without a new query."""
        amount = self.bytes if actual_bytes is None else _size(actual_bytes)
        if amount > self.bytes:
            raise ResourceError("disk write exceeds its reservation")
        with self._owner._lock:
            if not self._active:
                raise ResourceError("disk reservation is no longer active")
            self._owner._finish_refund()
            state = self._owner._devices[self.device]
            committed = checked_add(state.committed, amount)
            before = (state.committed, state.pending, state.tickets, self._owner._active)
            try:
                state.committed = committed
                state.pending = before[1] - self.bytes
                state.tickets = before[2] - 1
                self._owner._active = before[3] - 1
                # This final flag pair is the canonical commit boundary. Before
                # it, interruption rolls back all counters; afterwards the
                # retained ticket proves that a retry must not debit twice.
                self.committed_bytes = amount
                self._active, self.committed = False, True
            except BaseException:
                if not self.committed:
                    state.committed, state.pending, state.tickets, self._owner._active = before
                    self._active = True
                    self.committed_bytes = 0
                raise
            self._owner._charge(len(self._owner._devices), self._owner._active)
            return amount

    def release(self):
        with self._owner._lock:
            if self._active:
                state = self._owner._devices[self.device]
                state.pending -= self.bytes
                state.tickets -= 1
                self._owner._active -= 1
                self._active = False
                self._owner._charge(len(self._owner._devices), self._owner._active)

    def __enter__(self):
        if not self._active:
            raise ResourceError("disk reservation is no longer active")
        return self

    def __exit__(self, *_exception):
        self.release()
