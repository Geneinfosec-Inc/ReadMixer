"""Coordinator-issued encrypted scratch capabilities for spawned workers.

Only exact segment keys cross the bounded worker channel. The coordinator owns
allocation, disk reservations and an authenticated scratch ownership journal.
No application key roots, independent disk budget or publication authority is
constructed by the worker adapters in this module.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
import hashlib
import os
from pathlib import Path
import stat
import struct

from .crypto import validate_logical_identity
from .disk import DiskRefund
from .errors import ConfigurationError, IntegrityError, ResourceError
from ._cli_cancellation import resource_transition
from .ledger import PagedLedger
from .paths import open_directory
from .storage import (BLOCK_BYTES, BLOCK_HEADER, HEADER, SEAL, SegmentRef, _DiskSink,
                      _Entry, _VerifiedReader, deserialize_segment_ref,
                      serialize_segment_ref)

_MAX_JOB = 64 << 20
_MAX_GRANT = 16 << 10
_MAX_PATH = 4096
_MAX_INCREMENT = BLOCK_BYTES + BLOCK_HEADER.size + 16
_GRANT = struct.Struct('>8sBQQQQQQ32sHH')
_MAGIC = b'RMPYWSC1'
_RECORD = struct.Struct('>8sBQQ')
_RECORD_MAGIC = b'RMPYTSK1'


def _uint(value, label, *, positive=False):
    if type(value) is not int or not int(positive) <= value < 1 << 64:
        raise IntegrityError('invalid worker storage ' + label)
    return value


@dataclass(frozen=True, slots=True, repr=False)
class SegmentReadGrant:
    ref: SegmentRef
    key: bytes = field(repr=False)
    directory: str = field(repr=False)
    directory_device: int
    directory_inode: int
    max_decoded: int
    max_items: int

    def __repr__(self):
        return 'SegmentReadGrant(<protected>)'


@dataclass(frozen=True, slots=True, repr=False)
class SegmentWriteGrant:
    ref: SegmentRef
    key: bytes = field(repr=False)
    directory: str = field(repr=False)
    directory_device: int
    directory_inode: int
    max_decoded: int
    max_items: int
    task_id: int
    grant_id: int

    def __repr__(self):
        return 'SegmentWriteGrant(<protected>)'


def _validate_grant(grant):
    if type(grant) not in (SegmentReadGrant, SegmentWriteGrant):
        raise IntegrityError('unknown worker storage capability')
    serialize_segment_ref(grant.ref)
    if not isinstance(grant.key, bytes) or len(grant.key) != 32:
        raise IntegrityError('invalid worker storage key width')
    if (not isinstance(grant.directory, str) or '\0' in grant.directory
            or not Path(grant.directory).is_absolute() or '..' in Path(grant.directory).parts):
        raise IntegrityError('invalid worker storage directory')
    try:
        path = os.fsencode(grant.directory)
    except (ValueError, UnicodeError):
        raise IntegrityError('invalid worker storage directory') from None
    if not 1 <= len(path) <= _MAX_PATH:
        raise IntegrityError('worker storage directory exceeds its bound')
    _uint(grant.directory_device, 'directory device')
    _uint(grant.directory_inode, 'directory inode', positive=True)
    _uint(grant.max_items, 'item bound')
    if type(grant.max_decoded) is not int or not 0 <= grant.max_decoded <= _MAX_JOB:
        raise IntegrityError('worker storage decoded bound exceeds its admission')
    if (not grant.ref.inode or grant.ref.device != grant.directory_device
            or grant.ref.decoded_bytes > grant.max_decoded or grant.ref.item_count > grant.max_items):
        raise IntegrityError('worker storage reference exceeds its capability')
    if type(grant) is SegmentWriteGrant:
        _uint(grant.task_id, 'task identity')
        _uint(grant.grant_id, 'grant identity', positive=True)
        if (grant.ref.physical_bytes or grant.ref.decoded_bytes or grant.ref.item_count
                or grant.ref.data_block_count or grant.ref.digest != hashlib.sha256(b'').digest()):
            raise IntegrityError('worker write capability is not fresh')
    elif not grant.ref.physical_bytes:
        raise IntegrityError('worker read capability is not external')
    return path


def encode_grant(grant):
    path = _validate_grant(grant)
    ref = serialize_segment_ref(grant.ref)
    write = type(grant) is SegmentWriteGrant
    data = _GRANT.pack(_MAGIC, 2 if write else 1,
                       grant.task_id if write else 0, grant.grant_id if write else 0,
                       grant.directory_device, grant.directory_inode,
                       grant.max_decoded, grant.max_items, grant.key, len(path), len(ref)) + path + ref
    if len(data) > _MAX_GRANT:
        raise IntegrityError('worker storage capability exceeds its encoded bound')
    return data


def _decode_grant(data, expected):
    if not isinstance(data, bytes) or not _GRANT.size <= len(data) <= _MAX_GRANT:
        raise IntegrityError('invalid worker storage capability length')
    magic, kind, task, grant_id, dev, ino, decoded, items, key, path_len, ref_len = _GRANT.unpack_from(data)
    if (magic != _MAGIC or kind != expected or path_len > _MAX_PATH
            or _GRANT.size + path_len + ref_len != len(data) or expected == 1 and (task or grant_id)):
        raise IntegrityError('invalid worker storage capability envelope')
    path = os.fsdecode(data[_GRANT.size:_GRANT.size + path_len])
    ref = deserialize_segment_ref(data[_GRANT.size + path_len:])
    fields = (ref, key, path, dev, ino, decoded, items)
    result = SegmentReadGrant(*fields) if expected == 1 else SegmentWriteGrant(*fields, task, grant_id)
    _validate_grant(result)
    return result


def decode_read_grant(data):
    return _decode_grant(data, 1)


def decode_write_grant(data):
    return _decode_grant(data, 2)


def _physical_bound(decoded):
    return HEADER.size + decoded + ((decoded + BLOCK_BYTES - 1) // BLOCK_BYTES) * (BLOCK_HEADER.size + 16) + BLOCK_HEADER.size + SEAL.size + 16


def _same_identity(left, right):
    return (left.run_id, left.segment_id, left.generation_salt, left.purpose,
            left.logical_identity, left.device, left.inode) == (
            right.run_id, right.segment_id, right.generation_salt, right.purpose,
            right.logical_identity, right.device, right.inode)


class _CapabilityKeys:
    __slots__ = ('_grant',)

    def __init__(self, grant):
        self._grant = grant

    def segment_key(self, segment_id, salt, purpose, logical):
        ref = self._grant.ref
        if (segment_id, salt, purpose, logical) != (ref.segment_id, ref.generation_salt, ref.purpose, ref.logical_identity):
            raise IntegrityError('worker attempted forbidden key scope')
        return self._grant.key


class _CapabilityStore:
    """Narrow existing-codec adapter; holds no operation roots or disk budget."""
    def __init__(self, grant, checkpoint):
        _validate_grant(grant)
        self.grant, self.keys, self.checkpoint = grant, _CapabilityKeys(grant), checkpoint
        self.anchor = open_directory(grant.directory)
        self._directory_fd = self.anchor.fd
        try:
            self._check_directory()
        except BaseException:
            self.anchor.close()
            raise

    def _check_directory(self):
        self.anchor.check()
        info = os.fstat(self._directory_fd)
        if ((info.st_dev, info.st_ino) != (self.grant.directory_device, self.grant.directory_inode)
                or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700):
            raise IntegrityError('worker scratch directory identity or mode changed')

    def _checkpoint(self):
        if self.checkpoint is not None:
            self.checkpoint()

    def _open(self, ref, *, write=False):
        self._check_directory()
        flags = (os.O_WRONLY if write else os.O_RDONLY) | os.O_NOFOLLOW | getattr(os, 'O_NONBLOCK', 0)
        fd = None
        try:
            fd = os.open(ref.segment_id.hex() + '.segment', flags, dir_fd=self._directory_fd)
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                    or stat.S_IMODE(info.st_mode) != 0o600
                    or (info.st_dev, info.st_ino) != (ref.device, ref.inode)
                    or info.st_size != ref.physical_bytes):
                raise IntegrityError('worker scratch file identity, mode or length changed')
            return fd
        except BaseException:
            if fd is not None:
                os.close(fd)
            raise

    def _open_owned(self, ref):
        return self._open(ref)

    def close(self):
        self.anchor.close()
        self.keys = None


def read_granted(grant, checkpoint=None):
    if type(grant) is not SegmentReadGrant:
        raise IntegrityError('worker read requires a read capability')
    adapter = _CapabilityStore(grant, checkpoint)
    primary = None
    try:
        output = bytearray()
        with _VerifiedReader(adapter, grant.ref, None, None, release_lease=False) as reader:
            while True:
                adapter._checkpoint()
                chunk = reader.read(BLOCK_BYTES)
                if not chunk:
                    break
                output.extend(chunk)
                if len(output) > grant.max_decoded:
                    raise IntegrityError('worker read exceeds its capability')
        return bytes(output)
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            adapter.close()
        except BaseException:
            if primary is None:
                raise
            primary.add_note('worker capability descriptor cleanup also failed')


class _GrantedSink(_DiskSink):
    def __init__(self, adapter, grant, credit, commit):
        super().__init__(adapter, grant.ref)
        self.credit, self.commit = credit, commit

    def begin(self):
        self.fd = self.store._open(self.ref, write=True)
        self._write(self.header)

    def _write(self, data):
        self.store._checkpoint()
        offset = self.physical
        self.credit(offset, len(data))
        written = 0
        try:
            view = memoryview(data)
            while written < len(data):
                self.store._checkpoint()
                count = os.write(self.fd, view[written:])
                if count <= 0:
                    raise ResourceError('worker encrypted scratch write was short')
                written += count
        except BaseException as primary:
            try:
                self.commit(offset, written)
            except BaseException:
                primary.add_note('worker disk reconciliation remains coordinator-owned')
            raise
        self.commit(offset, written)
        self.physical += written


def write_granted(grant, data, item_count, credit_callback, commit_callback, checkpoint=None):
    if type(grant) is not SegmentWriteGrant:
        raise IntegrityError('worker write requires a write capability')
    _validate_grant(grant)
    _uint(item_count, 'result item count')
    if not isinstance(data, bytes) or len(data) > grant.max_decoded or item_count > grant.max_items:
        raise IntegrityError('worker result exceeds its storage capability')
    adapter = _CapabilityStore(grant, checkpoint)
    sink = _GrantedSink(adapter, grant, credit_callback, commit_callback)
    primary = None
    try:
        sink.begin()
        digest = hashlib.sha256()
        for offset in range(0, len(data), BLOCK_BYTES):
            adapter._checkpoint()
            chunk = data[offset:offset + BLOCK_BYTES]
            digest.update(chunk)
            sink.write_data(chunk)
        return sink.finish(item_count, len(data), digest.digest())
    except BaseException as error:
        primary = error
        raise
    finally:
        # Only the coordinator can delete or refund a granted file, including
        # failures after partial writes or after an acknowledgment is lost.
        sink._key = None
        cleanup = None
        if sink.fd is not None:
            try:
                os.close(sink.fd)
                sink.fd = None
            except BaseException as error:
                cleanup = error
        try:
            adapter.close()
        except BaseException as error:
            if cleanup is None:
                cleanup = error
        if cleanup is not None:
            if primary is None:
                raise cleanup
            primary.add_note('worker capability descriptor cleanup also failed')


@dataclass(slots=True, repr=False)
class _PendingWrite:
    grant: SegmentWriteGrant
    committed: int = 0
    ticket: object = None
    credit_offset: int = 0
    imported: SegmentRef | None = None


class WorkerStorage:
    """One invocation's bounded task capabilities and separate owned ledger.

    ``cleanup(workers_reaped=True)`` may run only after every granted writer has
    stopped. Accepted references remain manager-owned until ``take_write`` explicitly
    transfers them to the caller; only taken references survive cleanup. Unaccepted files stay in the task journal until removed.
    """
    def __init__(self, store, *, max_pending=128, max_job_bytes=_MAX_JOB, scatter=False):
        if store.mode == 'memory':
            raise ResourceError('strict memory cannot create worker disk capabilities')
        if type(max_pending) is not int or not 1 <= max_pending <= (4096 if scatter else 128):
            raise ConfigurationError('worker storage pending limit must be in 1..128')
        if type(max_job_bytes) is not int or not 0 <= max_job_bytes <= _MAX_JOB:
            raise ConfigurationError('worker storage batch exceeds its bound')
        self.store, self.max_pending, self.max_job_bytes = store, max_pending, max_job_bytes
        self._lease = store.budget.acquire('worker-storage', 16384 + 32768 * max_pending,
                                           metadata_bytes=16384 + 32768 * max_pending, descriptors=2)
        self._writes, self._reads, self._transferred = {}, {}, {}
        self._closed = self._cleaning = False
        self._last_grant_id = 0
        self._creation = None
        try:
            self.ledger = PagedLedger(store, owns_records=True)
        except BaseException:
            self._lease.release()
            raise

    def _check(self):
        if self._closed or self._cleaning:
            raise ResourceError('worker storage is closed or cleaning')
        self.store._check_open()
        self.store._checkpoint()

    def _grant(self, ref, max_decoded, max_items, task=None, grant_id=None):
        self.store._ensure_directory()
        device, inode = self.store._directory_identity
        args = (ref, self.store.keys.segment_key(ref.segment_id, ref.generation_salt,
                ref.purpose, ref.logical_identity), str(self.store.scratch_directory),
                device, inode, max_decoded, max_items)
        return SegmentReadGrant(*args) if task is None else SegmentWriteGrant(*args, task, grant_id)

    def issue_read(self, ref):
        self._check()
        if len(self._reads) >= self.max_pending or ref.segment_id in self._reads:
            raise ResourceError('worker read capability window is full or already pinned')
        if ref.decoded_bytes > self.max_job_bytes:
            raise ResourceError('worker input exceeds its admitted batch')
        if not self.store.is_external(ref):
            ref = self.store.spill(ref)
        entry = self.store._entry(ref)
        trusted = entry.ref if entry is not None else ref
        grant = self._grant(trusted, trusted.decoded_bytes, trusted.item_count)
        _validate_grant(grant)
        previous = self.store._reader_pins.get(ref.segment_id, 0)
        entry_pins = entry.pinned if entry is not None else 0
        try:
            self._reads[ref.segment_id] = grant
            self.store._reader_pins[ref.segment_id] = previous + 1
            if entry is not None:
                entry.pinned = entry_pins + 1
        except BaseException:
            if entry is not None:
                entry.pinned = entry_pins
            if previous:
                self.store._reader_pins[ref.segment_id] = previous
            else:
                self.store._reader_pins.pop(ref.segment_id, None)
            self._reads.pop(ref.segment_id, None)
            raise
        return grant

    def release_read(self, grant):
        current = self._reads.get(grant.ref.segment_id)
        if current != grant:
            raise IntegrityError('unknown or already released worker read capability')
        entry = self.store._entries.get(grant.ref.segment_id)
        prior = self.store._reader_pins[grant.ref.segment_id]
        entry_pins = entry.pinned if entry is not None else 0
        try:
            if entry is not None:
                entry.pinned = entry_pins - 1
            self.store._unpin(grant.ref.segment_id)
            # Removal is the canonical acknowledgment; until it commits an
            # interrupted unpin rolls back to one fully retained read owner.
            del self._reads[grant.ref.segment_id]
        except BaseException:
            if grant.ref.segment_id in self._reads:
                self.store._reader_pins[grant.ref.segment_id] = prior
                if entry is not None:
                    entry.pinned = entry_pins
            raise

    @staticmethod
    def _record(kind, grant, ref=None):
        return _RECORD.pack(_RECORD_MAGIC, kind, grant.task_id, grant.grant_id) + serialize_segment_ref(ref or grant.ref)

    def next_grant_id(self):
        if self._last_grant_id >= (1 << 64) - 1:
            raise ResourceError('worker storage grant namespace is exhausted')
        return self._last_grant_id + 1

    def issue_write(self, task_id, grant_id, purpose, logical_identity, max_decoded, max_items):
        self._check()
        _uint(task_id, 'task identity')
        _uint(grant_id, 'grant identity', positive=True)
        if grant_id <= self._last_grant_id:
            raise IntegrityError('worker write grant generation was reused or regressed')
        _uint(max_items, 'item bound')
        validate_logical_identity(purpose, logical_identity)
        if type(max_decoded) is not int or not 0 <= max_decoded <= self.max_job_bytes:
            raise ResourceError('worker output exceeds its admitted batch')
        if len(self._writes) >= self.max_pending or (task_id, grant_id) in self._writes or self._creation is not None:
            raise ResourceError('worker write capability window is full or duplicated')
        # Burn an issued generation even if allocation later fails. Historical
        # task-journal records must never alias a new live task's grant pair.
        self._last_grant_id = grant_id
        directory_fd = self.store._ensure_directory()
        ref = SegmentRef(self.store.keys.run_id, self.store.allocate_id(), os.urandom(32), purpose,
                         logical_identity, 0, 0, 0, hashlib.sha256(b'').digest())
        if len(ref.generation_salt) != 32:
            raise ResourceError('worker segment entropy allocation failed')
        fd = None
        try:
            # A CLI signal can arrive after the exclusive open returns but
            # before Python assigns its FD. Retain both descriptor and pathname
            # ownership before cancellation reaches the existing cleanup path.
            with resource_transition():
                fd = os.open(self.store._name(ref), os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                             0o600, dir_fd=directory_fd)
                info = os.fstat(fd)
                ref = replace(ref, device=info.st_dev, inode=info.st_ino)
                self._creation = ref
                if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600:
                    raise IntegrityError('worker scratch creation failed private ownership checks')
                os.close(fd)
                fd = None
            grant = self._grant(ref, max_decoded, max_items, task_id, grant_id)
            _validate_grant(grant)
            state = _PendingWrite(grant)
            self._writes[(task_id, grant_id)] = state
            # append's owned boundary is the authority even if return interrupts.
            self.ledger.append(self._record(1, grant))
            self._creation = None
            return grant
        except BaseException:
            if fd is not None:
                os.close(fd)
            raise

    def _pending(self, grant):
        self._check()
        state = self._writes.get((grant.task_id, grant.grant_id))
        if state is None or state.grant != grant or state.imported is not None:
            raise IntegrityError('unknown, stale or transferred worker write capability')
        return state

    def _info(self, ref):
        self.store._anchor.check()
        info = os.stat(self.store._name(ref), dir_fd=self.store._directory_fd, follow_symlinks=False)
        if (not stat.S_ISREG(info.st_mode) or (info.st_dev, info.st_ino) != (ref.device, ref.inode)
                or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600):
            raise IntegrityError('worker scratch file was replaced or changed mode')
        return info

    def authorize_write(self, grant, offset, amount):
        state = self._pending(grant)
        _uint(offset, 'write offset')
        if type(amount) is not int or not 0 < amount <= _MAX_INCREMENT:
            raise IntegrityError('worker disk increment exceeds its bound')
        if (state.ticket is not None or offset != state.committed
                or offset + amount > _physical_bound(grant.max_decoded)
                or self._info(grant.ref).st_size != offset):
            raise IntegrityError('worker disk increment is outside its granted state')
        state.credit_offset = offset
        self.store.disk_budget.reserve(self.store._directory_fd, amount,
                                       retained=lambda ticket: setattr(state, 'ticket', ticket))

    def commit_write(self, grant, offset, actual):
        state = self._pending(grant)
        _uint(offset, 'write offset')
        _uint(actual, 'write increment')
        ticket = state.ticket
        if ticket is None or offset != state.committed or actual > ticket.bytes:
            raise IntegrityError('worker disk commit has no matching credit')
        if self._info(grant.ref).st_size != offset + actual:
            raise IntegrityError('worker disk commit differs from observed growth')
        ticket.commit(actual)
        state.committed += actual
        state.ticket = None

    def accept_write(self, grant, ref):
        state = self._pending(grant)
        serialize_segment_ref(ref)
        if (state.ticket is not None or not _same_identity(ref, grant.ref)
                or ref.decoded_bytes > grant.max_decoded or ref.item_count > grant.max_items
                or ref.physical_bytes != state.committed or ref.physical_bytes > _physical_bound(grant.max_decoded)):
            raise IntegrityError('worker result exceeds its storage grant or committed growth')
        if len(self.store._entries) >= self.store.max_segments:
            raise ResourceError('resident segment registry is full')
        # Same full authenticated reader used for all other operation scratch.
        with self.store.open_verified(ref):
            pass
        metadata = self.store.budget.acquire('segment-metadata', 8192, metadata_bytes=8192)
        try:
            entry = _Entry(metadata, ref=ref)
            state.imported = ref
            self.store._entries[ref.segment_id] = entry
        except BaseException:
            if ref.segment_id not in self.store._entries:
                metadata.release()
            raise
        return ref

    def take_write(self, grant):
        """Transfer an accepted result to a pre-registered caller cleanup owner.

        Caller installs its result owner before invoking this method, and closes
        that owner on any exception. Thus an interruption after journal append
        cannot orphan output or cause a guessed deletion during manager cleanup.
        """
        self._check()
        state = self._writes.get((grant.task_id, grant.grant_id))
        if state is None or state.grant != grant or state.imported is None:
            raise IntegrityError('worker result is not accepted or already transferred')
        ref = state.imported
        self.ledger.append(self._record(2, grant, ref))
        del self._writes[(grant.task_id, grant.grant_id)]
        return ref

    def discard_empty_write(self, grant):
        """Retire a worker-acknowledged empty writer without a fake data segment.

        The worker must have closed this generation before calling. An owned
        cancellation record is appended only after identity-checked removal;
        interrupted removal remains covered by the original pending record.
        """
        state = self._pending(grant)
        if state.ticket is not None or state.committed:
            raise IntegrityError('only an unwritten worker capability can be retired empty')
        try:
            info = self._info(grant.ref)
        except FileNotFoundError:
            info = None
        if info is not None:
            if info.st_size:
                raise IntegrityError('empty worker capability has unexpected bytes')
            os.unlink(self.store._name(grant.ref), dir_fd=self.store._directory_fd)
        self.ledger.append(self._record(3, grant))
        self._writes.pop((grant.task_id, grant.grant_id), None)

    def _reconcile(self, state):
        ticket = state.ticket
        if ticket is None:
            return
        if not ticket._active:
            # reserve may be interrupted before admission, or commit can finish
            # its canonical debit before housekeeping raises. Both owners are
            # retained, but only a committed ticket justifies observed growth.
            observed = self._info(state.grant.ref).st_size
            if ticket.committed:
                expected = state.credit_offset + ticket.committed_bytes
                if observed != expected or state.committed not in (state.credit_offset, expected):
                    raise IntegrityError('worker scratch growth differs from exact committed credit')
                state.committed = expected
            elif observed != state.committed:
                raise IntegrityError('worker scratch grew before credit admission')
            state.ticket = None
            return
        observed = self._info(state.grant.ref).st_size - state.committed
        if observed < 0 or observed > ticket.bytes:
            raise IntegrityError('worker scratch growth exceeds reserved disk credit')
        ticket.commit(observed)
        state.committed += observed
        state.ticket = None

    def _parse_record(self, record, _ordinal=None):
        if not isinstance(record, bytes) or len(record) < _RECORD.size:
            raise IntegrityError('invalid task ownership record')
        magic, kind, task, grant = _RECORD.unpack_from(record)
        if magic != _RECORD_MAGIC or kind not in (1, 2, 3) or not grant:
            raise IntegrityError('invalid task ownership record envelope')
        ref = deserialize_segment_ref(record[_RECORD.size:])
        if (ref.run_id != self.store.keys.run_id or not ref.inode
                or not 0 < int.from_bytes(ref.segment_id, 'big') < self.store._next_id):
            raise IntegrityError('invalid task ownership record identity')
        if kind in (1, 3) and (ref.physical_bytes or ref.decoded_bytes or ref.data_block_count
                          or ref.item_count or ref.digest != hashlib.sha256(b'').digest()):
            raise IntegrityError('pending task record is not a fresh file identity')
        if kind == 2 and not ref.physical_bytes:
            raise IntegrityError('task transfer record has no sealed physical extent')
        return kind, task, grant, ref

    def _delete(self, ref, progress, committed):
        if not progress.flags & 1:
            try:
                info = self._info(ref)
            except FileNotFoundError:
                info = None
            if info is not None and info.st_size != committed:
                raise IntegrityError('worker scratch cleanup differs from reconciled growth')
            if progress.refund is None:
                progress.refund = DiskRefund(self.store.disk_budget, ref.device, committed)
            if info is not None:
                os.unlink(self.store._name(ref), dir_fd=self.store._directory_fd)
            progress.mark(1)
        if progress.refund is not None:
            progress.refund.commit()

    def _drain(self, record, progress):
        kind, task, grant_id, ref = self._parse_record(record)
        if kind in (2, 3):
            previous = self._transferred.get(ref.segment_id)
            if previous is not None and previous != ref:
                raise IntegrityError('duplicate task transfer identity')
            if previous is None and len(self._transferred) >= self.max_pending:
                raise IntegrityError('task transfer history exceeds its live window')
            self._transferred[ref.segment_id] = ref
            return
        transferred = self._transferred.get(ref.segment_id)
        active = self._writes.get((task, grant_id))
        if not progress.flags & (2 | 4 | 8):
            if transferred is not None:
                if not _same_identity(transferred, ref):
                    raise IntegrityError('task transfer does not match pending ownership')
                progress.mark(2)
            elif active is not None and active.imported is not None:
                if not _same_identity(active.imported, ref):
                    raise IntegrityError('pending imported task identity is inconsistent')
                self.store.drop(active.imported, missing_ok=True)
                progress.mark(4)
            else:
                if active is None:
                    raise IntegrityError('pending task has no retained coordinator ownership')
                self._delete(ref, progress, active.committed)
                progress.mark(8)
        # Progress survives even if dictionary retirement itself is interrupted.
        if progress.flags & 2:
            self._transferred.pop(ref.segment_id, None)
        self._writes.pop((task, grant_id), None)

    def cleanup(self, *, workers_reaped):
        if self._closed:
            return
        if workers_reaped is not True:
            raise ResourceError('worker scratch cleanup requires reaped writers')
        self._cleaning = True
        for state in self._writes.values():
            self._reconcile(state)
        for grant in tuple(self._reads.values()):
            self.release_read(grant)
        if self._creation is not None:
            # A pre-dispatch interruption can precede journal registration.
            # Delete only the identity retained at exclusive creation.
            ref = self._creation
            if not any(state.grant.ref == ref for state in self._writes.values()):
                try:
                    self._info(ref)
                except FileNotFoundError:
                    pass
                else:
                    os.unlink(self.store._name(ref), dir_fd=self.store._directory_fd)
            self._creation = None
        self.ledger.drain_owned(self._drain, validate=self._parse_record)
        if self._transferred:
            raise IntegrityError('task ownership chain has unmatched transfers')
        # Registration can fail before append's owned boundary. Such entries
        # have never escaped to a worker and remain directly coordinator-owned.
        for key, state in tuple(self._writes.items()):
            if state.imported is None:
                try:
                    info = self._info(state.grant.ref)
                except FileNotFoundError:
                    info = None
                if info is not None:
                    if info.st_size != 0:
                        raise IntegrityError('unregistered worker scratch is not empty')
                    os.unlink(self.store._name(state.grant.ref), dir_fd=self.store._directory_fd)
            del self._writes[key]
        self.ledger.close()
        self._lease.release()
        self._closed = True
