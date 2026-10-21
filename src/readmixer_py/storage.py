"""Admitted RAM segments and authenticated, ephemeral encrypted scratch.

Disk ownership may be transferred to an authenticated caller ledger with
``detach``. Such callers must drop their segments before closing the store.
No keys, plaintext locators, or unbounded historical ownership map are kept.
"""
from __future__ import annotations

from contextlib import AbstractContextManager, contextmanager, nullcontext
from dataclasses import dataclass, replace
import hashlib
import os
from pathlib import Path
import stat
import struct
import tempfile
from threading import Lock
import zlib

from Crypto.Cipher import ChaCha20_Poly1305

from .crypto import OperationKeys, validate_logical_identity
from .disk import DiskRefund
from .errors import CancelledError, ConfigurationError, IntegrityError, ResourceError
from .resources import Budget, Lease, checked_add
from .paths import open_directory

MAGIC = b"RMPYTMP1"
BLOCK_BYTES = 1 << 20
HEADER = struct.Struct(">8s16s16s32sB3s")
BLOCK_HEADER = struct.Struct(">BB2sQII")
SEAL = struct.Struct(">QQQ32s")
_REF = struct.Struct(">16s16s32sB H QQQ32s QQQ")
_METADATA_BYTES = 8192
_WORKSPACE_BYTES = 8 * BLOCK_BYTES
_MAX_READ_BYTES = 2 * BLOCK_BYTES + 40
_U64_MAX = (1 << 64) - 1
_U128_MAX = (1 << 128) - 1


def _integer(value: int, maximum: int, label: str) -> int:
    if type(value) is not int or value < 0 or value > maximum:
        raise ConfigurationError(f"invalid {label}")
    return value


@dataclass(frozen=True, slots=True)
class SegmentRef:
    """Expected logical seal and physical identity; contains no secret key."""

    run_id: bytes
    segment_id: bytes
    generation_salt: bytes
    purpose: int
    logical_identity: bytes
    data_block_count: int
    item_count: int
    decoded_bytes: int
    digest: bytes
    device: int = 0
    inode: int = 0
    physical_bytes: int = 0

    @property
    def logical_item_count(self) -> int:
        return self.item_count

    @property
    def total_decoded_data_bytes(self) -> int:
        return self.decoded_bytes


def _validate_ref(ref: SegmentRef) -> None:
    if not isinstance(ref, SegmentRef):
        raise IntegrityError("invalid segment reference")
    for value, length in ((ref.run_id, 16), (ref.segment_id, 16), (ref.generation_salt, 32), (ref.digest, 32)):
        if not isinstance(value, bytes) or len(value) != length:
            raise IntegrityError("invalid segment reference identity")
    try:
        validate_logical_identity(ref.purpose, ref.logical_identity)
        for value in (ref.data_block_count, ref.item_count, ref.decoded_bytes, ref.device, ref.inode, ref.physical_bytes):
            _integer(value, _U64_MAX, "segment reference counter")
    except ConfigurationError as exc:
        raise IntegrityError("invalid segment reference fields") from exc
    if ref.decoded_bytes > ref.data_block_count * BLOCK_BYTES:
        raise IntegrityError("segment reference has impossible decoded length")
    if ref.physical_bytes and ref.physical_bytes < HEADER.size + BLOCK_HEADER.size + SEAL.size + 16:
        raise IntegrityError("segment reference has impossible physical length")


def serialize_segment_ref(ref: SegmentRef) -> bytes:
    _validate_ref(ref)
    return _REF.pack(ref.run_id, ref.segment_id, ref.generation_salt, ref.purpose,
                     len(ref.logical_identity), ref.data_block_count, ref.item_count,
                     ref.decoded_bytes, ref.digest, ref.device, ref.inode,
                     ref.physical_bytes) + ref.logical_identity


def deserialize_segment_ref(data: bytes) -> SegmentRef:
    if not isinstance(data, bytes) or len(data) < _REF.size or len(data) > _REF.size + 4096:
        raise IntegrityError("invalid encoded segment reference length")
    fields = _REF.unpack_from(data)
    if len(data) != _REF.size + fields[4]:
        raise IntegrityError("invalid encoded segment reference fields")
    ref = SegmentRef(*fields[:4], data[_REF.size:], *fields[5:])
    _validate_ref(ref)
    return ref


def _same_logical(left: SegmentRef, right: SegmentRef) -> bool:
    return (left.run_id, left.segment_id, left.generation_salt, left.purpose,
            left.logical_identity, left.data_block_count, left.item_count,
            left.decoded_bytes, left.digest) == (
            right.run_id, right.segment_id, right.generation_salt, right.purpose,
            right.logical_identity, right.data_block_count, right.item_count,
            right.decoded_bytes, right.digest)


@dataclass(slots=True)
class _Entry:
    lease: Lease
    writer: SegmentWriter | None = None
    ref: SegmentRef | None = None
    blocks: list[tuple[bytes, Lease]] | None = None
    pinned: int = 0


class SegmentStore(AbstractContextManager):
    """One operation's bounded segment registry and protected scratch owner."""

    def __init__(self, mode: str, budget: Budget, keys: OperationKeys, *,
                 temp_dir: str | Path | None = None, disk_budget=None,
                 input_root: str | Path | None = None, max_segments: int = 1024, cancel=None):
        if mode not in ("memory", "external", "auto"):
            raise ConfigurationError("invalid segment storage mode")
        _integer(max_segments, 100_000, "segment registry limit")
        if not max_segments:
            raise ConfigurationError("segment registry limit must be positive")
        if cancel is not None and not callable(cancel):
            raise ConfigurationError("cancellation callback must be callable")
        self.mode, self.budget, self.keys = mode, budget, keys
        self.cancel = cancel
        self._reader_pins: dict[bytes, int] = {}
        self._ledger_roots = 0
        self.max_segments = min(max_segments, min(64 * BLOCK_BYTES, budget.limit_bytes // 16) // _METADATA_BYTES)
        self.temp_dir = Path(temp_dir if temp_dir is not None else tempfile.gettempdir()).absolute()
        self.input_root = Path(input_root).resolve() if input_root is not None else None
        if self.input_root is not None and self.temp_dir.resolve().is_relative_to(self.input_root):
            raise ConfigurationError("temporary directory must be outside the input root")
        owns_disk_budget = disk_budget is None
        if owns_disk_budget:
            from .disk import DiskBudget
            disk_budget = DiskBudget(budget=budget)
        self.disk_budget = disk_budget
        self._entries: dict[bytes, _Entry] = {}
        self._next_id = 1
        self._directory: Path | None = None
        self._directory_fd: int | None = None
        self._directory_identity: tuple[int, int] | None = None
        self._directory_lease: Lease | None = None
        self._anchor = None
        self._parent_anchor = None
        control_bytes = 4096 + 8 * (len(str(self.temp_dir)) + len(str(self.input_root or '')))
        self._metadata = budget.acquire("segment-store", control_bytes, metadata_bytes=control_bytes)
        self._owns_disk_budget = owns_disk_budget
        self._spill_lease = None
        try:
            if getattr(self.disk_budget, 'budget', None) not in (None, budget):
                raise ConfigurationError("disk budget belongs to another RAM budget")
            if mode == "auto":
                self._spill_lease = budget.acquire("spill-overlap", 2 * _WORKSPACE_BYTES + 12288,
                                                  descriptors=6, metadata_bytes=12288)
                self.disk_budget.prepare()
        except BaseException:
            if self._spill_lease is not None:
                self._spill_lease.release()
            if self._owns_disk_budget:
                self.disk_budget.close()
            self._metadata.release()
            raise
        self._closed = False
        self._failed_spill = False
        self._delete_state = (0, None)
        self._pending_allocation = None

    @property
    def _detached_count(self):
        return self._delete_state[0]

    @_detached_count.setter
    def _detached_count(self, count):
        self._delete_state = (count, self._delete_state[1])

    @property
    def _pending_delete(self):
        return self._delete_state[1]

    @_pending_delete.setter
    def _pending_delete(self, pending):
        self._delete_state = (self._delete_state[0], pending)

    @property
    def resident_segments(self) -> int:
        return len(self._entries)

    @property
    def scratch_directory(self) -> Path | None:
        return self._directory

    def allocate_id(self) -> bytes:
        self._check_open()
        self._checkpoint()
        if self._next_id > _U128_MAX:
            raise ResourceError("segment identity namespace exhausted")
        result = self._next_id.to_bytes(16, "big")
        self._next_id += 1
        return result

    def _checkpoint(self) -> None:
        if self.cancel is not None and self.cancel():
            raise CancelledError()

    def _check_open(self) -> None:
        if self._closed:
            raise ResourceError("segment store is closed")

    def _ensure_directory(self) -> int:
        if self._directory_fd is not None:
            self._anchor.check()
            return self._directory_fd
        if self.mode == "memory":
            raise ResourceError("memory storage cannot create disk scratch")
        # Auto reserves directory metadata and handles alongside codec overlap,
        # so migration can start even when other admitted work fills RAM.
        lease = (None if self._spill_lease is not None else
                 self.budget.acquire("scratch-directory", 4096, descriptors=4, metadata_bytes=4096))
        parent = child = None
        try:
            parent = open_directory(self.temp_dir, input_root=self.input_root)
            child = parent.new_private_child("readmixer-scratch-")
            info = os.fstat(child.fd)
            if stat.S_IMODE(info.st_mode) != 0o700:
                raise ResourceError("private scratch directory has unsafe permissions")
            self._anchor, self._parent_anchor = child, parent
            self._directory, self._directory_fd = child.path, child.fd
            self._directory_identity = (info.st_dev, info.st_ino)
            self._directory_lease = lease
            return child.fd
        except BaseException:
            if child is not None:
                try:
                    child.remove_if_empty()
                except BaseException:
                    child.close()
            if parent is not None:
                parent.close()
            if lease is not None:
                lease.release()
            raise

    def new_segment(self, purpose: int, logical_identity: bytes, lease: Lease | None = None, *,
                    flush_workspace=None, output_lease: Lease | None = None,
                    block_reservation: Lease | None = None, external: bool = False) -> SegmentWriter:
        self._check_open()
        if type(external) is not bool:
            raise ConfigurationError("external segment output must be boolean")
        if external and self.mode == "memory":
            raise ResourceError("memory storage cannot create disk scratch")
        if self._failed_spill:
            raise ResourceError("scratch admission stopped after a failed spill")
        if self._pending_allocation is not None:
            raise ResourceError("scratch admission stopped after incomplete allocation cleanup")
        self._checkpoint()
        validate_logical_identity(purpose, logical_identity)
        if len(self._entries) >= self.max_segments:
            raise ResourceError("resident segment registry is full; page sealed external references")
        metadata = self.budget.acquire("segment-metadata", _METADATA_BYTES, metadata_bytes=_METADATA_BYTES)
        entry = writer = None
        try:
            segment_id = self.allocate_id()
            salt = os.urandom(32)
            if not isinstance(salt, bytes) or len(salt) != 32:
                raise OSError("entropy unavailable")
            entry = _Entry(metadata, blocks=[])
            writer = SegmentWriter(self, entry, segment_id, salt, purpose, logical_identity, lease,
                                   flush_workspace=flush_workspace, output_lease=output_lease,
                                   block_reservation=block_reservation, external=external)
            entry.writer = writer
            self._entries[segment_id] = entry
            return writer
        except BaseException as exc:
            pending = writer if writer is not None else entry.writer if entry is not None else None
            if pending is not None:
                try:
                    pending.abort()
                except BaseException as cleanup:
                    self._pending_allocation = pending
                    exc.add_note("pending segment allocation cleanup also failed")
                    raise exc from cleanup
            metadata.release()
            if isinstance(exc, OSError):
                raise ResourceError("segment entropy allocation failed") from exc
            raise

    def _entry(self, ref: SegmentRef) -> _Entry | None:
        self._check_open()
        _validate_ref(ref)
        if (ref.run_id != self.keys.run_id or not 0 < int.from_bytes(ref.segment_id, "big") < self._next_id):
            raise IntegrityError("segment reference belongs to another operation or allocation")
        entry = self._entries.get(ref.segment_id)
        if entry is not None and (entry.ref is None or not _same_logical(ref, entry.ref)):
            raise IntegrityError("segment reference does not match its trusted seal")
        if entry is None and (not ref.physical_bytes or not ref.inode):
            raise IntegrityError("unknown resident segment reference")
        return entry

    def is_external(self, ref: SegmentRef) -> bool:
        entry = self._entry(ref)
        return bool((entry.ref if entry else ref).physical_bytes)

    def detach(self, ref: SegmentRef) -> SegmentRef:
        """Transfer sealed disk ownership to a caller's authenticated ledger."""
        entry = self._entry(ref)
        if entry is None:
            return ref
        if self._reader_pins.get(ref.segment_id, 0):
            raise ResourceError("cannot detach a segment with active readers")
        assert entry.ref is not None
        if not entry.ref.physical_bytes:
            raise ResourceError("resident memory segments cannot leave their registry")
        result = entry.ref
        del self._entries[ref.segment_id]
        entry.ref = entry.writer = entry.blocks = None
        entry.lease.release()
        self._detached_count += 1
        return result

    def _admit(self, category: str, amount: int, *, exclude: bytes | None = None, descriptors: int = 0, metadata_bytes: int = 0) -> Lease:
        try:
            return self.budget.acquire(category, amount, descriptors, metadata_bytes=metadata_bytes)
        except ResourceError:
            if self.mode != "auto":
                raise
        # Iteration is bounded by the charged resident registry; no frontier copy.
        for segment_id, entry in self._entries.items():
            if segment_id != exclude and entry.ref is not None and entry.blocks and not entry.pinned:
                self.spill(entry.ref)
                try:
                    return self.budget.acquire(category, amount, descriptors, metadata_bytes=metadata_bytes)
                except ResourceError:
                    pass
        raise ResourceError(f"{category}: cannot admit the requested working set")

    def open_verified(self, ref: SegmentRef, *, cleanup: bool = False) -> _VerifiedReader:
        if not cleanup:
            self._checkpoint()
        entry = self._entry(ref)
        trusted = entry.ref if entry is not None else ref
        assert trusted is not None
        # A bounded read includes decoded, ciphertext, codec, and return copies.
        lease = self._admit("segment-reader", _WORKSPACE_BYTES, exclude=ref.segment_id,
                            descriptors=1 if trusted.physical_bytes else 0,
                            metadata_bytes=8192 + (min(BLOCK_BYTES, trusted.decoded_bytes)
                                                  if trusted.purpose == 4 and trusted.physical_bytes else 0))
        previous_pins = self._reader_pins.get(ref.segment_id, 0)
        previous_entry_pins = entry.pinned if entry is not None else 0
        try:
            self._reader_pins[ref.segment_id] = previous_pins + 1
            if entry is not None:
                entry.pinned = previous_entry_pins + 1
            return _VerifiedReader(self, trusted, entry, lease, cleanup=cleanup)
        except BaseException:
            if entry is not None:
                entry.pinned = previous_entry_pins
            if previous_pins:
                self._reader_pins[ref.segment_id] = previous_pins
            else:
                self._reader_pins.pop(ref.segment_id, None)
            lease.release()
            raise

    def _unpin(self, segment_id: bytes) -> None:
        count = self._reader_pins[segment_id] - 1
        if count:
            self._reader_pins[segment_id] = count
        else:
            del self._reader_pins[segment_id]

    @staticmethod
    def _name(ref: SegmentRef) -> str:
        return ref.segment_id.hex() + ".segment"

    def _open_owned(self, ref: SegmentRef) -> int:
        if self._directory_fd is None:
            raise IntegrityError("segment scratch directory is unavailable")
        self._anchor.check()
        try:
            fd = os.open(self._name(ref), os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_NONBLOCK", 0), dir_fd=self._directory_fd)
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                    or stat.S_IMODE(info.st_mode) != 0o600
                    or (info.st_dev, info.st_ino) != (ref.device, ref.inode)
                    or info.st_size != ref.physical_bytes):
                os.close(fd)
                raise IntegrityError("segment file identity, mode, or length changed")
            return fd
        except OSError as exc:
            raise IntegrityError("owned segment cannot be opened safely") from exc

    def spill(self, ref: SegmentRef) -> SegmentRef:
        self._checkpoint()
        if self._failed_spill:
            raise ResourceError("scratch admission stopped after a failed spill")
        entry = self._entry(ref)
        if self.mode == "memory":
            raise ResourceError("memory storage cannot spill to disk")
        if entry is None or (entry.ref is not None and entry.ref.physical_bytes):
            return entry.ref if entry is not None else ref
        if entry.pinned:
            raise ResourceError("cannot spill a segment with active readers")
        assert entry.ref is not None and entry.blocks is not None
        source = entry.ref
        workspace = self._spill_lease or self.budget.acquire("segment-spill", 2 * _WORKSPACE_BYTES, descriptors=2, metadata_bytes=8192)
        sink = _DiskSink(self, source)
        try:
            sink.begin()
            for data, _lease in entry.blocks:
                self._checkpoint()
                sink.write_data(data)
            result = sink.finish(source.item_count, source.decoded_bytes, source.digest)
            # Verify the complete durable replacement while the source remains owned.
            with _VerifiedReader(self, result, None, workspace, release_lease=False):
                pass
        except BaseException as primary:
            self._failed_spill = True
            try:
                sink.abort()
            except BaseException:
                primary.add_note("scratch spill cleanup also failed")
            raise
        finally:
            if workspace is not self._spill_lease:
                workspace.release()
        self._release_blocks(entry)
        entry.ref = result
        return result

    def _finish_delete(self):
        ref, refund = self._pending_delete
        self._anchor.check()
        try:
            info = os.stat(self._name(ref), dir_fd=self._directory_fd, follow_symlinks=False)
        except FileNotFoundError:
            info = None
        if info is not None:
            if (info.st_dev, info.st_ino) != (ref.device, ref.inode) or not stat.S_ISREG(info.st_mode):
                raise IntegrityError("refusing to remove a replaced scratch file")
            try:
                os.unlink(self._name(ref), dir_fd=self._directory_fd)
            except OSError as exc:
                raise ResourceError("owned scratch could not be removed") from exc
        refund.commit()
        count = self._detached_count
        if ref.segment_id not in self._entries:
            count = max(0, count - 1)
        # Retire the intent and detached-file debit together, including a retry
        # that reached deletion through another segment's cleanup.
        self._delete_state = (count, None)

    def _unlink(self, ref: SegmentRef) -> bool:
        assert self._directory_fd is not None
        if self._pending_delete is not None:
            previous = self._pending_delete[0]
            self._finish_delete()
            if previous == ref:
                return True
        self._anchor.check()
        try:
            info = os.stat(self._name(ref), dir_fd=self._directory_fd, follow_symlinks=False)
        except FileNotFoundError:
            return False
        except OSError as exc:
            raise ResourceError("owned scratch identity cannot be checked") from exc
        if (info.st_dev, info.st_ino) != (ref.device, ref.inode) or not stat.S_ISREG(info.st_mode):
            raise IntegrityError("refusing to remove a replaced scratch file")
        # One bounded intent is charged to the store control envelope. Retain it
        # before unlink so a missing-file retry still completes exactly one debit.
        self._pending_delete = (ref, DiskRefund(self.disk_budget, ref.device, ref.physical_bytes))
        self._finish_delete()
        return True

    def drop(self, ref: SegmentRef, *, missing_ok: bool = False) -> None:
        if missing_ok:
            self._check_open()
            _validate_ref(ref)
            if (ref.run_id != self.keys.run_id or not 0 < int.from_bytes(ref.segment_id, "big") < self._next_id):
                raise IntegrityError("segment reference belongs to another operation or allocation")
            if ref.segment_id not in self._entries and not ref.physical_bytes:
                return
        entry = self._entry(ref)
        if self._reader_pins.get(ref.segment_id, 0):
            raise ResourceError("cannot drop a segment with active readers")
        trusted = entry.ref if entry is not None else ref
        assert trusted is not None
        if trusted.physical_bytes:
            self._unlink(trusted)
        if entry is not None:
            self._release_blocks(entry)
            del self._entries[ref.segment_id]
            entry.ref = entry.writer = None
            entry.lease.release()

    @staticmethod
    def _release_blocks(entry):
        while entry.blocks:
            data, lease = entry.blocks.pop()
            del data
            lease.release()
        entry.blocks = None

    def close(self) -> None:
        if self._closed:
            return
        errors: list[BaseException] = []
        # Never unlink while a reader still owns data or a file descriptor.
        if self._reader_pins:
            raise ResourceError("segment store still has active readers")
        if self._ledger_roots:
            raise ResourceError("segment store still has active ledger roots")
        if self._pending_delete is not None:
            try:
                self._finish_delete()
            except BaseException as exc:
                errors.append(exc)
        if self._pending_allocation is not None:
            try:
                self._pending_allocation.abort()
                self._pending_allocation = None
            except BaseException as exc:
                errors.append(exc)
        for segment_id in tuple(self._entries):
            entry = self._entries[segment_id]
            try:
                if entry.writer is not None:
                    entry.writer.abort()
                elif entry.ref is not None:
                    self.drop(entry.ref)
            except BaseException as exc:
                errors.append(exc)
        if self._directory_fd is not None and not errors:
            assert self._anchor is not None and self._parent_anchor is not None
            try:
                # Check emptiness without closing the anchor on a failed attempt:
                # callers may still need to drop transferred ledger pages.
                self._anchor.check()
                os.rmdir(self._anchor.name, dir_fd=self._parent_anchor.fd)
                os.fsync(self._parent_anchor.fd)
            except BaseException as exc:
                errors.append(exc)
            if not errors:
                self._anchor.close()
                self._parent_anchor.close()
                self._directory_fd = None
                if self._directory_lease is not None:
                    self._directory_lease.release()

        if errors:
            raise ResourceError("scratch cleanup incomplete; owned or transferred files remain") from errors[0]
        if self._spill_lease is not None:
            self._spill_lease.release()
            self._spill_lease = None
        if self._owns_disk_budget:
            self.disk_budget.close()
        self._reader_pins.clear()
        self._entries.clear()
        self._anchor = self._parent_anchor = None
        self._directory_identity = None
        self._metadata.release()
        self._closed = True

    def abandon(self) -> None:
        """Terminal handle/RAM release without deleting uncertain scratch residue.

        Call only after all feasible authenticated cleanup and ledger abandonment.
        Keys are not persisted, and abandoned residue cannot be resumed.
        """
        if self._closed:
            return
        if self._reader_pins or self._ledger_roots:
            raise ResourceError("cannot abandon storage with active readers or ledger roots")
        first = None
        def attempt(action):
            nonlocal first
            try:
                action()
            except BaseException as exc:
                if first is None:
                    first = exc
        def dispose(entry):
            writer = entry.writer
            if writer is not None:
                if writer._sink is not None and writer._sink.fd is not None:
                    fd, writer._sink.fd = writer._sink.fd, None
                    attempt(lambda: os.close(fd))
                attempt(writer._release_work)
                writer._closed = True
            self._release_blocks(entry)
            entry.writer = entry.ref = None
            entry.lease.release()
        for entry in self._entries.values():
            dispose(entry)
        if self._pending_allocation is not None:
            # Allocation can fail before registry insertion. That writer still
            # owns its work buffers, provided lease and entry metadata.
            dispose(self._pending_allocation.entry)
        self._entries.clear()
        for anchor in (self._anchor, self._parent_anchor):
            if anchor is not None:
                attempt(anchor.close)
        self._anchor = self._parent_anchor = None
        self._directory_fd = None
        for lease in (self._directory_lease, self._spill_lease, self._metadata):
            if lease is not None:
                lease.release()
        self._directory_lease = self._spill_lease = None
        self._pending_allocation = self._pending_delete = None
        self.keys = None
        self._closed = True
        if self._owns_disk_budget:
            attempt(self.disk_budget.close)
        if first is not None:
            raise first

    def __exit__(self, exc_type, exc, traceback) -> None:
        try:
            self.close()
        except BaseException:
            if exc is None:
                raise
            exc.add_note("scratch cleanup also failed; primary error preserved")


class _DiskSink:
    def __init__(self, store: SegmentStore, ref: SegmentRef):
        self.store, self.ref = store, ref
        self.fd: int | None = None
        self.physical = 0
        self.sequence = 0
        self.header = HEADER.pack(MAGIC, ref.run_id, ref.segment_id,
                                  ref.generation_salt, ref.purpose, b"\0" * 3)
        self._key = store.keys.segment_key(ref.segment_id, ref.generation_salt, ref.purpose, ref.logical_identity)

    def begin(self) -> None:
        parent = self.store._ensure_directory()
        try:
            self.fd = os.open(self.store._name(self.ref), os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                              0o600, dir_fd=parent)
            info = os.fstat(self.fd)
            self.ref = replace(self.ref, device=info.st_dev, inode=info.st_ino)
            if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600 or not stat.S_ISREG(info.st_mode):
                raise ResourceError("scratch file was not created privately")
            self._write(self.header)
        except OSError as exc:
            raise ResourceError("private scratch creation failed") from exc

    def _write(self, data: bytes) -> None:
        assert self.fd is not None and self.store._directory_fd is not None
        with self.store.disk_budget.reserve(self.store._directory_fd, len(data)) as reservation:
            view = memoryview(data)
            written = 0
            try:
                while written < len(data):
                    count = os.write(self.fd, view[written:])
                    if count <= 0:
                        raise OSError("short scratch write")
                    written += count
            except OSError as exc:
                if written:
                    reservation.commit(written)
                    self.physical += written
                raise ResourceError("encrypted scratch write failed") from exc
            reservation.commit()
            self.physical += len(data)

    def _block(self, kind: int, codec: int, decoded: int, payload: bytes) -> None:
        if self.sequence > _U64_MAX:
            raise ResourceError("segment block sequence exhausted")
        header = BLOCK_HEADER.pack(kind, codec, b"\0\0", self.sequence, decoded, len(payload))
        cipher = ChaCha20_Poly1305.new(key=self._key, nonce=b"\0" * 4 + self.sequence.to_bytes(8, "big"))
        cipher.update(self.header + header)
        ciphertext, tag = cipher.encrypt_and_digest(payload)
        self._write(header + ciphertext + tag)
        self.sequence += 1

    def write_data(self, data: bytes) -> None:
        if len(data) > BLOCK_BYTES or self.sequence == _U64_MAX:
            raise ResourceError("segment block exceeds its protocol limit")
        compressed = zlib.compress(data, level=1)
        codec = int(len(compressed) < len(data))
        self._block(0, codec, len(data), compressed if codec else data)

    def finish(self, items: int, decoded: int, digest: bytes) -> SegmentRef:
        blocks = self.sequence
        self._block(1, 0, SEAL.size, SEAL.pack(blocks, items, decoded, digest))
        assert self.fd is not None
        try:
            os.fsync(self.fd)
            os.close(self.fd)
            self.fd = None
            assert self.store._directory_fd is not None
            os.fsync(self.store._directory_fd)
        except OSError as exc:
            raise ResourceError("scratch durability sync failed") from exc
        self._key = None
        self.ref = replace(self.ref, data_block_count=blocks, item_count=items,
                           decoded_bytes=decoded, digest=digest, physical_bytes=self.physical)
        return self.ref

    def abort(self) -> None:
        self._key = None
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        if self.ref.inode:
            self.store._unlink(replace(self.ref, physical_bytes=self.physical))


class SharedFlushWorkspace(AbstractContextManager):
    """One admitted synchronous codec/AEAD workspace for a complete writer set.

    Writers retain independent buffers, keys, descriptors and nonce counters.
    Only transient flush allocations borrow this reservation. The nonblocking
    lock rejects overlap, including reentrant calls, before any flush work.
    """

    def __init__(self, budget):
        self.budget = budget
        self._lease = budget.acquire("shared-segment-flush", _WORKSPACE_BYTES,
                                     metadata_bytes=8192)
        self._lock = Lock()
        self._writers = 0
        self._closed = False

    def _attach(self):
        if self._closed:
            raise ResourceError("shared flush workspace is closed")
        self._writers += 1

    def _detach(self):
        self._writers -= 1

    @contextmanager
    def borrow(self):
        if self._closed:
            raise ResourceError("shared flush workspace is closed")
        if not self._lock.acquire(blocking=False):
            raise ResourceError("shared flush workspace is already borrowed")
        try:
            yield self
        finally:
            self._lock.release()

    def close(self):
        if self._closed:
            return
        if self._writers or self._lock.locked():
            raise ResourceError("shared flush workspace still has live borrowers")
        self._lease.release()
        self._closed = True

    def __exit__(self, exc_type, exc, traceback):
        try:
            self.close()
        except BaseException:
            if exc is None:
                raise
            exc.add_note("shared flush workspace cleanup also failed")


class SegmentWriter(AbstractContextManager):
    def __init__(self, store, entry, segment_id, salt, purpose, logical_identity, lease, *,
                 flush_workspace=None, output_lease=None, block_reservation=None, external=False):
        if flush_workspace is not None and (not isinstance(flush_workspace, SharedFlushWorkspace)
                or flush_workspace.budget is not store.budget or flush_workspace._closed):
            raise ConfigurationError("writer workspace belongs to another budget or is closed")
        for supplied in (output_lease, block_reservation):
            if supplied is not None and (supplied._budget is not store.budget or supplied.released):
                raise ConfigurationError("writer reservation belongs to another budget or is closed")
        if output_lease is not None and (flush_workspace is None or output_lease.bytes < 8192
                or output_lease.metadata_bytes < 8192
                or (store.mode != "memory" and output_lease.descriptors < 1)):
            raise ConfigurationError("shared writer output reservation is incomplete")
        self.store, self.entry = store, entry
        # Callers with an already-external lifecycle (notably sort runs) can
        # stream encrypted output immediately. Waiting for an unbounded merge
        # to seal before spilling would retain that entire run in auto RAM.
        self._external_output = external or store.mode == "external"
        self._base = SegmentRef(store.keys.run_id, segment_id, salt, purpose,
                                logical_identity, 0, 0, 0, hashlib.sha256(b"").digest())
        self._buffer = bytearray()
        # Registry pages are small metadata. Avoid a one-MiB resident buffer
        # for every page writer; the wire format permits smaller data blocks.
        self._buffer_capacity = (64 << 10) if purpose == 4 else BLOCK_BYTES
        self._buffer_length = 0
        self._buffer_lease: Lease | None = None
        if lease is not None and lease._budget is not store.budget:
            raise ConfigurationError("writer lease belongs to another budget")
        self._provided_lease = None
        self._flush_workspace = None
        self._block_reservation = block_reservation
        self._digest = hashlib.sha256()
        self._items = self._decoded = self._blocks = 0
        self._sink: _DiskSink | None = None
        self._sink_lease: Lease | None = None
        self._closed = False
        # Register a fully initialized cleanup owner before any supplied lease
        # can transfer, including if the store's dictionary insertion then fails.
        entry.writer = self
        self._provided_lease = lease.transfer("segment-writer") if lease is not None else None
        if output_lease is not None:
            self._sink_lease = output_lease.transfer("shared-segment-output")
        if flush_workspace is not None:
            flush_workspace._attach()
            self._flush_workspace = flush_workspace

    def append(self, data, *, item_count: int = 0) -> None:
        if self._closed:
            raise ResourceError("segment writer is closed")
        self.store._checkpoint()
        if self.store._failed_spill:
            raise ResourceError("scratch admission stopped after a failed spill")
        _integer(item_count, _U64_MAX, "segment item increment")
        view = memoryview(data)
        if view.ndim != 1 or not view.contiguous or view.itemsize != 1:
            raise ConfigurationError("segment data must be contiguous bytes")
        next_bytes = checked_add(self._decoded, len(view))
        next_items = checked_add(self._items, item_count)
        if not self._buffer_lease and self._provided_lease is not None and self._provided_lease.bytes >= self._buffer_capacity + 256:
            self._buffer_lease, self._provided_lease = self._provided_lease, None
            required_metadata = self._buffer_capacity + 256 if self._base.purpose == 4 else 256
            self._buffer_lease.resize(self._buffer_lease.bytes, self._buffer_lease.descriptors,
                                      metadata_bytes=max(required_metadata, self._buffer_lease.metadata_bytes))
        if not self._buffer_lease:
            amount = self._buffer_capacity + 256
            self._buffer_lease = self.store._admit("segment-buffer", amount, exclude=self._base.segment_id,
                                                  metadata_bytes=amount if self._base.purpose == 4 else 256)
        if view and not self._buffer:
            self._buffer = bytearray(self._buffer_capacity)
        # Counts become final only if all bytes append successfully. Any failure
        # leaves an unsealed owned writer which can only be aborted.
        try:
            while view:
                self.store._checkpoint()
                length = min(self._buffer_capacity - self._buffer_length, len(view))
                part = view[:length]
                self._buffer[self._buffer_length:self._buffer_length + length] = part
                self._buffer_length += length
                self._digest.update(part)
                view = view[length:]
                if self._buffer_length == self._buffer_capacity:
                    self._flush()
        except BaseException:
            self.abort()
            raise
        self._decoded, self._items = next_bytes, next_items

    def append_pair(self, frame, *, max_record_bytes: int = BLOCK_BYTES) -> None:
        """Append a complete encoded frame as one logical item (no splitting segments)."""
        from .paircodec import decode_pair_view
        decode_pair_view(frame, max_record_bytes=max_record_bytes)
        self.append(frame, item_count=1)

    def _external(self) -> None:
        if self._sink is None:
            if self._sink_lease is None:
                self._sink_lease = self.store._admit(
                    "segment-output", 8192 if self._flush_workspace is not None else _WORKSPACE_BYTES,
                    exclude=self._base.segment_id, descriptors=1,
                    metadata_bytes=8192 if self._flush_workspace is not None else 4096)
            self._sink = _DiskSink(self.store, self._base)
            self._sink.begin()

    def _flush(self) -> None:
        with self._flush_workspace.borrow() if self._flush_workspace is not None else nullcontext():
            self._flush_borrowed()

    def _flush_borrowed(self) -> None:
        if not self._buffer_length:
            return
        if self._external_output:
            self._external()
            assert self._sink is not None
            self._sink.write_data(bytes(memoryview(self._buffer)[:self._buffer_length]))
        else:
            amount = self._buffer_length + 256
            metadata = amount if self._base.purpose == 4 else 256
            if self._block_reservation is not None:
                block_lease = self._block_reservation.split("segment-memory", amount, metadata_bytes=metadata)
            else:
                block_lease = self.store._admit("segment-memory", amount, exclude=self._base.segment_id,
                                               metadata_bytes=metadata)
            try:
                data = bytes(memoryview(self._buffer)[:self._buffer_length])
                assert self.entry.blocks is not None
                self.entry.blocks.append((data, block_lease))
            except BaseException:
                block_lease.release()
                raise
        self._blocks += 1
        self._buffer_length = 0

    def seal(self) -> SegmentRef:
        if self._closed:
            raise ResourceError("segment writer is closed")
        try:
            self._flush()
            digest = self._digest.digest()
            if self._external_output:
                with self._flush_workspace.borrow() if self._flush_workspace is not None else nullcontext():
                    self._external()
                    assert self._sink is not None
                    result = self._sink.finish(self._items, self._decoded, digest)
                self.entry.blocks = None
            else:
                result = replace(self._base, data_block_count=self._blocks, item_count=self._items,
                                 decoded_bytes=self._decoded, digest=digest)
            self.entry.ref = result
            self.entry.writer = None
            self._closed = True
            self._release_work()
            return result
        except BaseException as primary:
            try:
                self.abort()
            except BaseException:
                primary.add_note("segment writer cleanup also failed")
            raise

    def _release_work(self) -> None:
        self._buffer.clear()
        self._buffer_length = 0
        for lease in (self._buffer_lease, self._sink_lease, self._provided_lease):
            if lease is not None:
                lease.release()
        self._buffer_lease = self._sink_lease = self._provided_lease = None
        if self._flush_workspace is not None:
            self._flush_workspace._detach()
            self._flush_workspace = None
        self._block_reservation = None

    def abort(self) -> None:
        if self._closed:
            return
        if self._sink is not None:
            self._sink.abort()
        self.store._release_blocks(self.entry)
        self._release_work()
        self.store._entries.pop(self._base.segment_id, None)
        self.entry.writer = self.entry.ref = None
        self.entry.lease.release()
        self._closed = True

    def __exit__(self, exc_type, exc, traceback) -> None:
        if not self._closed:
            try:
                self.abort()
            except BaseException:
                if exc is None:
                    raise
                exc.add_note("segment writer cleanup also failed")


class _VerifiedReader(AbstractContextManager):
    def __init__(self, store, ref, entry, lease, *, release_lease=True, cleanup=False):
        self.store, self.ref, self.entry, self.lease = store, ref, entry, lease
        self._release_lease = release_lease
        self._cleanup = cleanup
        self._file = None
        self._closed = self._verified = False
        self._chunk = b""
        self._offset = self._sequence = self._decoded = 0
        self._digest = hashlib.sha256()
        self._header = HEADER.pack(MAGIC, ref.run_id, ref.segment_id,
                                   ref.generation_salt, ref.purpose, b"\0" * 3)
        self._key = None
        if ref.physical_bytes:
            fd = store._open_owned(ref)
            try:
                self._file = os.fdopen(fd, "rb", buffering=0)
                if self._exact(HEADER.size) != self._header:
                    raise IntegrityError("segment header does not match its expected identity")
                self._key = store.keys.segment_key(ref.segment_id, ref.generation_salt,
                                                   ref.purpose, ref.logical_identity)
            except BaseException:
                if self._file is not None:
                    self._file.close()
                else:
                    os.close(fd)
                raise
        elif entry is None or entry.blocks is None:
            raise IntegrityError("resident segment has no owned blocks")

    @property
    def verified(self) -> bool:
        return self._verified

    def _exact(self, length: int) -> bytes:
        assert self._file is not None
        try:
            result = self._file.read(length)
        except OSError as exc:
            raise IntegrityError("protected segment read failed") from exc
        if len(result) != length:
            raise IntegrityError("truncated protected segment")
        return result

    def _finish(self, blocks: int, items: int, decoded: int, digest: bytes) -> None:
        if (blocks, items, decoded, digest) != (self.ref.data_block_count, self.ref.item_count,
                                                self.ref.decoded_bytes, self.ref.digest):
            raise IntegrityError("segment seal differs from its expected reference")
        if (blocks, decoded, digest) != (self._sequence, self._decoded, self._digest.digest()):
            raise IntegrityError("segment seal differs from observed data")
        if self._file is not None:
            try:
                trailing = self._file.read(1)
            except OSError as exc:
                raise IntegrityError("protected segment end-of-file check failed") from exc
            if trailing:
                raise IntegrityError("protected segment has trailing bytes")
        self._verified = True

    def _next(self) -> bytes:
        if self._verified:
            return b""
        if self._file is None:
            assert self.entry is not None and self.entry.blocks is not None
            if self._sequence == len(self.entry.blocks):
                self._finish(self.ref.data_block_count, self.ref.item_count,
                             self.ref.decoded_bytes, self.ref.digest)
                return b""
            data = self.entry.blocks[self._sequence][0]
        else:
            block = self._exact(BLOCK_HEADER.size)
            kind, codec, reserved, sequence, decoded, stored = BLOCK_HEADER.unpack(block)
            if reserved != b"\0\0" or sequence != self._sequence or kind not in (0, 1) or codec not in (0, 1):
                raise IntegrityError("protected segment block flags or sequence are invalid")
            if kind == 1:
                if codec != 0 or decoded != SEAL.size or stored != SEAL.size:
                    raise IntegrityError("protected segment seal framing is invalid")
            elif (decoded > BLOCK_BYTES or stored > BLOCK_BYTES or self._sequence >= self.ref.data_block_count
                  or (codec == 0 and decoded != stored) or (codec == 1 and stored >= decoded)):
                raise IntegrityError("protected segment data block lengths are invalid")
            ciphertext = self._exact(stored)
            tag = self._exact(16)
            cipher = ChaCha20_Poly1305.new(key=self._key, nonce=b"\0" * 4 + sequence.to_bytes(8, "big"))
            cipher.update(self._header + block)
            try:
                payload = cipher.decrypt_and_verify(ciphertext, tag)
            except ValueError as exc:
                raise IntegrityError("protected segment authentication failed") from exc
            if kind == 1:
                self._finish(*SEAL.unpack(payload))
                return b""
            if codec:
                try:
                    decoder = zlib.decompressobj()
                    data = decoder.decompress(payload, decoded + 1)
                except zlib.error as exc:
                    raise IntegrityError("authenticated segment compression is invalid") from exc
                if (len(data) != decoded or not decoder.eof or decoder.unconsumed_tail or decoder.unused_data):
                    raise IntegrityError("authenticated segment compression length or end is invalid")
            else:
                data = payload
        self._decoded = checked_add(self._decoded, len(data))
        if self._decoded > self.ref.decoded_bytes:
            raise IntegrityError("segment decoded data exceeds its expected length")
        self._digest.update(data)
        self._sequence += 1
        return data

    def read(self, size: int = -1) -> bytes:
        if self._closed:
            raise ValueError("segment reader is closed")
        if not self._cleanup:
            self.store._checkpoint()
        if size == -1:
            size = self.ref.decoded_bytes - (self._decoded - len(self._chunk) + self._offset)
        if type(size) is not int or size < 0 or size > _MAX_READ_BYTES:
            raise ResourceError("segment reads require a bounded size of at most 2097192 bytes")
        result = bytearray()
        while len(result) < size:
            if self._offset == len(self._chunk):
                if not self._cleanup:
                    self.store._checkpoint()
                self._chunk, self._offset = self._next(), 0
                if not self._chunk:
                    if self._verified:
                        break
                    continue
            length = min(size - len(result), len(self._chunk) - self._offset)
            result.extend(memoryview(self._chunk)[self._offset:self._offset + length])
            self._offset += length
        return bytes(result)

    def readinto(self, buffer) -> int:
        view = memoryview(buffer)
        data = self.read(len(view))
        view[:len(data)] = data
        return len(data)

    def close(self) -> None:
        if self._closed:
            return
        primary = None
        try:
            self._chunk = b""
            while not self._verified:
                self._next()
        except BaseException as exc:
            primary = exc
            raise
        finally:
            try:
                if self._file is not None:
                    try:
                        self._file.close()
                    except OSError as exc:
                        if primary is None:
                            raise ResourceError("protected segment reader close failed") from exc
                        primary.add_note("protected segment reader close also failed")
            finally:
                if self.entry is not None:
                    self.entry.pinned -= 1
                if self._release_lease:
                    self.store._unpin(self.ref.segment_id)
                    self.lease.release()
                self._closed = True
                self._chunk = b""
                self._key = None

    def __exit__(self, exc_type, exc, traceback) -> None:
        try:
            self.close()
        except BaseException:
            if exc is None:
                raise
            exc.add_note("segment drain validation also failed; primary error preserved")
