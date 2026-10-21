"""Private output staging with no-replace links and a durable completion marker.

Ownership records use authenticated, admitted pages in the operation store.
Only active handles and pending handoffs remain in coordinator RAM. The protocol
is a last-marker lifecycle, not atomic visibility of a multi-file dataset.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import re
import stat
from typing import BinaryIO, Callable

from .config import MAX_ARTIFACT_BYTES, MIN_ARTIFACT_BYTES
from .disk import DiskBudget, DiskRefund
from .crypto import OperationKeys
from .ownership import OutputOwnershipJournal, OwnershipRecord
from .storage import SegmentStore
from .errors import CancelledError, ConfigurationError, IntegrityError, PublicationError, ReadMixerError, ResourceError
from .paths import AnchoredDirectory, open_directory
from .resources import Budget, Lease

_BASENAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,254}", re.ASCII)
_IO_BYTES = 128 * 1024
_METADATA_MAX = 64 * 1024 * 1024
_DEFAULT_MARKERS = {"manifest": "manifest.json", "recovery-receipt": "recovery-receipt.json"}


def _basename(value: str) -> str:
    if not isinstance(value, str) or _BASENAME.fullmatch(value) is None:
        raise ConfigurationError("artifact name must be a bounded safe basename")
    return value


def _identity(facts: os.stat_result) -> tuple[int, int]:
    return facts.st_dev, facts.st_ino


def _private_file(facts: os.stat_result, expected: tuple[int, int]) -> None:
    if (_identity(facts) != expected or not stat.S_ISREG(facts.st_mode)
            or facts.st_uid != os.getuid() or stat.S_IMODE(facts.st_mode) != 0o600):
        raise PublicationError("owned partial identity or private permissions changed")


@dataclass(frozen=True)
class PublicationReport:
    committed: bool
    cleanup_complete: bool
    completion_basename: str
    warnings: tuple[str, ...] = ()


class Artifact:
    """An admitted sequential writer owned by exactly one transaction.

    Close the writer before registration. All growth passes through write(),
    which checks the physical cap and reserves disk capacity before each write.
    """

    __slots__ = ("_transaction", "name", "_identity", "_fd", "_lease", "_written",
                 "_registered", "_digest", "_linked", "_staged", "_completion", "_snapshot", "_ordinal", "_ticket", "_charge", "_refund")

    def __init__(self, transaction: OutputTransaction, name: str, descriptor: int,
                 identity: tuple[int, int], lease: Lease, completion: bool):
        self._transaction = transaction
        self.name = name
        self._identity = identity
        self._fd = descriptor
        self._lease = lease
        self._written = 0
        self._registered = False
        self._digest: bytes | None = None
        self._linked = False
        self._staged = True
        self._completion = completion
        self._snapshot: tuple[int, int, int] | None = None
        self._ordinal = transaction._created
        self._ticket = None
        self._charge = 2048 + 2 * len(name)
        self._refund = None

    @property
    def closed(self) -> bool:
        return self._fd < 0

    @property
    def bytes_written(self) -> int:
        return self._written

    def writable(self) -> bool:
        return not self.closed

    def write(self, data: bytes | bytearray | memoryview) -> int:
        transaction = self._transaction
        transaction._ensure_writable()
        transaction._poll()
        if self.closed:
            raise PublicationError("partial writer is closed")
        view = memoryview(data)
        if not view.c_contiguous:
            raise ConfigurationError("partial write requires contiguous bytes")
        view = view.cast("B")
        length = view.nbytes
        if length > transaction.max_artifact_bytes - self._written:
            raise ResourceError("final artifact exceeds its physical byte cap")
        transaction._check_directories()
        _private_file(os.fstat(self._fd), self._identity)
        if not length:
            return 0
        written = 0
        try:
            with transaction.disk_budget.reserve(transaction._stage.fd, length) as reservation:
                try:
                    while written < length:
                        transaction._poll()
                        count = os.write(self._fd, view[written:])
                        if count <= 0:
                            raise OSError("output write made no progress")
                        written += count
                        self._written += count
                finally:
                    # Even a failed write may have consumed physical capacity.
                    reservation.commit(written)
        except OSError as exc:
            raise PublicationError("partial output write failed") from exc
        return written

    def flush(self) -> None:
        if self.closed:
            raise PublicationError("partial writer is closed")
        try:
            os.fsync(self._fd)
        except OSError as exc:
            raise PublicationError("partial output sync failed") from exc

    def close(self) -> None:
        if not self.closed:
            descriptor, self._fd = self._fd, -1
            primary: BaseException | None = None
            try:
                os.fsync(descriptor)
            except BaseException as exc:
                if isinstance(exc, OSError):
                    primary = PublicationError("partial output sync failed")
                    primary.__cause__ = exc
                else:
                    primary = exc
            try:
                os.close(descriptor)
            except BaseException as exc:
                if primary is not None:
                    primary.add_note("partial output descriptor cleanup failed")
                elif isinstance(exc, OSError):
                    primary = PublicationError("partial output close failed")
                    primary.__cause__ = exc
                else:
                    primary = exc
            finally:
                self._lease.release()
            if primary is not None:
                raise primary

    def _close_for_cleanup(self) -> None:
        if not self.closed:
            descriptor, self._fd = self._fd, -1
            try:
                os.close(descriptor)
            finally:
                self._lease.release()

    def __enter__(self) -> Artifact:
        return self

    def __exit__(self, exc_type: object, exc: BaseException | None, traceback: object) -> None:
        if exc is None:
            self.close()
        else:
            try:
                self._close_for_cleanup()
            except OSError:
                exc.add_note("partial writer cleanup failed")


class _OwnedReader:
    """Close its descriptor lease with the verified file descriptor."""

    def __init__(self, stream: BinaryIO, lease: Lease):
        self._stream = stream
        self._lease = lease

    def __getattr__(self, name: str):
        return getattr(self._stream, name)

    def close(self) -> None:
        try:
            self._stream.close()
        finally:
            self._lease.release()

    def __enter__(self) -> _OwnedReader:
        return self

    def __exit__(self, exc_type: object, exc: BaseException | None, traceback: object) -> None:
        try:
            self.close()
        except OSError:
            if exc is None:
                raise
            exc.add_note("owned partial reader cleanup failed")


class OutputTransaction:
    """A single-coordinator publication transaction; destination must exist."""

    @classmethod
    @contextmanager
    def operation(cls, destination, completion_kind, budget, *, keys, storage_mode,
                  temp_dir=None, disk_budget=None, input_root=None, cancel=None, **limits):
        """Own construction and the begin-to-caller handoff without a cleanup gap."""
        store = transaction = primary = None
        try:
            store = SegmentStore(storage_mode, budget, keys, temp_dir=temp_dir or destination,
                                 disk_budget=disk_budget, input_root=input_root, cancel=cancel)
            transaction = cls.begin(destination, completion_kind, budget, store=store,
                                    disk_budget=store.disk_budget, input_root=input_root,
                                    cancel=cancel, **limits)
            yield transaction
        except BaseException as error:
            primary = error
            raise
        finally:
            if transaction is not None:
                transaction.abort(primary)
            elif store is not None:
                store.__exit__(type(primary), primary,
                               primary.__traceback__ if primary is not None else None)
            store = transaction = primary = None

    @classmethod
    def begin(
        cls, destination: Path, completion_kind: str, budget: Budget, *,
        disk_budget: DiskBudget | None = None, input_root: Path | None = None,
        max_artifacts: int = 100_000, max_artifact_bytes: int = MAX_ARTIFACT_BYTES,
        metadata_bytes: int = 1 << 20, completion_basename: str | None = None,
        cancel: Callable[[], bool] | None = None, store: SegmentStore | None = None,
    ) -> OutputTransaction:
        if completion_kind not in _DEFAULT_MARKERS:
            raise ConfigurationError("completion kind must be manifest or recovery-receipt")
        if type(max_artifacts) is not int or not 1 <= max_artifacts <= 100_000:
            raise ConfigurationError("artifact inventory limit must be in 1..100000")
        if type(max_artifact_bytes) is not int or not MIN_ARTIFACT_BYTES <= max_artifact_bytes <= MAX_ARTIFACT_BYTES:
            raise ConfigurationError("artifact byte cap must be in 65564..5000000000")
        if type(metadata_bytes) is not int or not 4096 <= metadata_bytes <= _METADATA_MAX:
            raise ConfigurationError("transaction metadata allowance must be in 4096..67108864")
        if cancel is not None and not callable(cancel):
            raise ConfigurationError("cancellation must be callable")
        marker = _basename(completion_basename or _DEFAULT_MARKERS[completion_kind])
        obj = cls.__new__(cls)
        obj.budget = budget
        if store is not None and (store.budget is not budget or
                (disk_budget is not None and store.disk_budget is not disk_budget)):
            raise ConfigurationError("transaction store must share RAM and disk budgets")
        obj._owns_disk = store is None and disk_budget is None
        obj.disk_budget = (store.disk_budget if store is not None else
                           disk_budget if disk_budget is not None else DiskBudget(budget=budget))
        obj.max_artifacts = max_artifacts
        obj.max_artifact_bytes = max_artifact_bytes
        obj.metadata_bytes = metadata_bytes
        obj.completion_basename = marker
        obj._cancel = cancel
        obj._active: dict[int, Artifact] = {}
        obj._created = 0
        obj._pending_link = None
        obj._journal = None
        obj.store = store
        obj._metadata_used = 1024
        obj._completion: Artifact | None = None
        obj._state = "open"
        obj._warnings: list[str] = []
        obj._resources_closed = False
        obj._poll()
        obj._lease = budget.acquire("output-transaction", obj._metadata_used + _IO_BYTES, 4,
                                    metadata_bytes=obj._metadata_used)
        destination_directory: AnchoredDirectory | None = None
        try:
            destination_directory = open_directory(destination, private_destination=True, input_root=input_root)
            obj._destination = destination_directory
            obj._poll()
            obj._stage = destination_directory.new_private_child(prefix="rmpy-stage-")
            if obj.store is None:
                obj.store = SegmentStore("memory", budget, OperationKeys.new(),
                                         disk_budget=obj.disk_budget, temp_dir=destination,
                                         input_root=input_root, cancel=cancel)
            obj._journal = OutputOwnershipJournal(obj.store, cancel=cancel)
            return obj
        except BaseException as exc:
            if obj._journal is not None:
                obj._journal.close()
            if store is None and obj.store is not None:
                obj.store.close()
            if hasattr(obj, '_stage'):
                try:
                    obj._stage.remove_if_empty()
                finally:
                    obj._stage.close()
            if obj._owns_disk:
                obj.disk_budget.close()
            if destination_directory is not None:
                try:
                    destination_directory.close()
                except OSError:
                    exc.add_note("transaction directory cleanup failed")
            obj._lease.release()
            raise

    def _poll(self) -> None:
        if self._cancel is not None and self._cancel():
            raise CancelledError()

    def _check_directories(self) -> None:
        self._destination.check()
        self._stage.check()

    def _ensure_writable(self) -> None:
        if self._state not in ("open", "publishing"):
            raise PublicationError("output transaction no longer accepts writes")

    def _owns(self, artifact: Artifact) -> None:
        if not isinstance(artifact, Artifact) or artifact._transaction is not self:
            raise PublicationError("artifact does not belong to this transaction")

    def create_partial(self, relative_basename: str) -> Artifact:
        if self._state != "open":
            raise PublicationError("output transaction no longer accepts artifacts")
        return self._create(relative_basename, completion=False)

    def _create(self, name: str, *, completion: bool) -> Artifact:
        _basename(name)
        if not completion and name == self.completion_basename:
            raise PublicationError("duplicate or reserved artifact basename")
        # Inventory includes the last completion document, not just data files.
        if self._created >= self.max_artifacts - int(not completion):
            raise ResourceError("final artifact inventory exceeds its count limit")
        if not completion and len(self._active) >= 64:
            raise ResourceError("active output handle limit reached")
        charge = 2048 + 2 * len(name)
        if charge > self.metadata_bytes - self._metadata_used:
            raise ResourceError("output ownership metadata allowance exhausted")
        self._poll()
        self._check_directories()
        next_metadata = self._metadata_used + charge
        self._lease.resize(_IO_BYTES + next_metadata, 4, metadata_bytes=next_metadata)
        lease = None
        descriptor = -1
        artifact: Artifact | None = None
        try:
            lease = self.budget.acquire("output-writer", 512, 1)
            descriptor = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                                 0o600, dir_fd=self._stage.fd)
            facts = os.fstat(descriptor)
            artifact = Artifact(self, name, descriptor, _identity(facts), lease, completion)
            # Active ownership is bounded to 64 handles plus the marker.
            self._active[artifact._ordinal] = artifact
            self._metadata_used += charge
            if not completion:
                self._created += 1
            _private_file(facts, artifact._identity)
            if completion:
                self._completion = artifact
            return artifact
        except BaseException as exc:
            if artifact is None or self._active.get(artifact._ordinal) is not artifact:
                if descriptor >= 0:
                    try:
                        facts = os.fstat(descriptor)
                        found = os.stat(name, dir_fd=self._stage.fd, follow_symlinks=False)
                        if _identity(found) == _identity(facts):
                            os.unlink(name, dir_fd=self._stage.fd)
                    except OSError:
                        exc.add_note("pending partial cleanup failed")
                    finally:
                        try:
                            os.close(descriptor)
                        except OSError:
                            exc.add_note("pending partial descriptor cleanup failed")
                if lease is not None:
                    lease.release()
                self._lease.resize(_IO_BYTES + self._metadata_used, 4, metadata_bytes=self._metadata_used)
            if isinstance(exc, OSError):
                message = "duplicate artifact basename" if isinstance(exc, FileExistsError) else "exclusive partial creation failed"
                raise PublicationError(message) from exc
            raise

    def open_owned_partial(self, artifact: Artifact) -> _OwnedReader:
        self._ensure_writable()
        self._owns(artifact)
        if not artifact.closed or not artifact._staged:
            raise PublicationError("partial must be closed and staged before reading")
        self._poll()
        self._check_directories()
        lease = self.budget.acquire("output-reader", 512, 1)
        descriptor = -1
        try:
            descriptor = os.open(artifact.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | getattr(os, "O_NONBLOCK", 0),
                                 dir_fd=self._stage.fd)
            _private_file(os.fstat(descriptor), artifact._identity)
            stream = os.fdopen(descriptor, "rb", buffering=0)
            descriptor = -1
            return _OwnedReader(stream, lease)
        except BaseException as exc:
            if descriptor >= 0:
                try:
                    os.close(descriptor)
                except OSError:
                    exc.add_note("owned partial descriptor cleanup failed")
            lease.release()
            if isinstance(exc, OSError):
                raise PublicationError("owned partial could not be opened") from exc
            raise

    def register_closed(self, artifact: Artifact, length: int, digest: bytes | str) -> None:
        try:
            self._register_closed(artifact, length, digest)
        except OSError as exc:
            raise PublicationError("closed partial validation could not complete") from exc

    def _register_closed(self, artifact: Artifact, length: int, digest: bytes | str) -> None:
        self._owns(artifact)
        if artifact._registered:
            raise PublicationError("artifact is already registered")
        if type(length) is not int or not 0 <= length <= self.max_artifact_bytes:
            raise ResourceError("registered artifact length exceeds its physical cap")
        if isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest):
            digest = bytes.fromhex(digest)
        if not isinstance(digest, bytes) or len(digest) != 32:
            raise ConfigurationError("artifact digest must be a SHA-256 value")
        with self.open_owned_partial(artifact) as stream:
            facts = os.fstat(stream.fileno())
            if facts.st_size != length or length != artifact._written:
                raise IntegrityError("closed artifact length does not match its declared size")
            observed = hashlib.sha256()
            count = 0
            while True:
                self._poll()
                chunk = stream.read(64 * 1024)
                if not chunk:
                    break
                count += len(chunk)
                if count > length:
                    raise IntegrityError("closed artifact grew during validation")
                observed.update(chunk)
            after = os.fstat(stream.fileno())
            if (count != length or observed.digest() != digest
                    or (facts.st_size, facts.st_mtime_ns, facts.st_ctime_ns)
                    != (after.st_size, after.st_mtime_ns, after.st_ctime_ns)):
                raise IntegrityError("closed artifact digest or modification state does not match")
            try:
                self._poll()
                os.fsync(stream.fileno())
            except OSError as exc:
                raise PublicationError("registered partial sync failed") from exc
        artifact._snapshot = (after.st_size, after.st_mtime_ns, after.st_ctime_ns)
        artifact._digest = digest
        if artifact._completion:
            artifact._registered = True
            return
        ticket = self._journal.prepare_inventory(self._record(artifact))
        artifact._ticket = ticket
        try:
            self._journal.commit(ticket)
        finally:
            # The journal's canonical boundary owns cleanup even if append did
            # not return. A stale active handle can never become a second owner.
            if ticket.committed:
                artifact._registered = True
                self._retire_active(artifact)
            else:
                self._journal.discard(ticket)
                artifact._ticket = None

    def _retire_active(self, artifact):
        if self._active.pop(artifact._ordinal, None) is not None:
            self._metadata_used -= artifact._charge
            self._lease.resize(_IO_BYTES + self._metadata_used, 4,
                               metadata_bytes=self._metadata_used)

    @staticmethod
    def _record(artifact):
        return OwnershipRecord(artifact._ordinal, artifact.name, *artifact._identity,
                               artifact._written, artifact._digest,
                               artifact._snapshot[1], artifact._snapshot[2])

    def _token(self, record):
        # A traversal retains one read token, never the complete inventory.
        artifact = Artifact.__new__(Artifact)
        artifact._transaction, artifact.name = self, record.name
        artifact._identity = (record.device, record.inode)
        artifact._fd, artifact._lease = -1, None
        artifact._written, artifact._digest = record.length, record.digest
        artifact._registered, artifact._linked, artifact._staged = True, False, True
        artifact._completion, artifact._ordinal = False, record.ordinal
        artifact._snapshot = (record.length, record.mtime_ns, record.ctime_ns)
        artifact._ticket, artifact._charge, artifact._refund = None, 0, None
        return artifact

    def _link(self, artifact: Artifact) -> None:
        self._poll()
        self._check_directories()
        facts = os.stat(artifact.name, dir_fd=self._stage.fd, follow_symlinks=False)
        _private_file(facts, artifact._identity)
        if (facts.st_size, facts.st_mtime_ns, facts.st_ctime_ns) != artifact._snapshot:
            raise IntegrityError("registered partial changed before publication")
        ticket = None if artifact._completion else self._journal.prepare_link(self._record(artifact))
        artifact._ticket = ticket
        self._pending_link = artifact
        artifact._linked = True
        try:
            os.link(artifact.name, artifact.name, src_dir_fd=self._stage.fd,
                    dst_dir_fd=self._destination.fd, follow_symlinks=False)
        except OSError:
            # EEXIST never confers ownership, even for the same inode.
            artifact._linked = False
            if ticket is not None:
                self._journal.discard(ticket)
            self._pending_link = None
            raise
        if ticket is not None:
            self._journal.commit(ticket)
        self._pending_link = None
        final = os.stat(artifact.name, dir_fd=self._destination.fd, follow_symlinks=False)
        _private_file(final, artifact._identity)

    def _unlink_record(self, record, *, final):
        directory = self._destination if final else self._stage
        directory.check()
        try:
            facts = os.stat(record.name, dir_fd=directory.fd, follow_symlinks=False)
        except FileNotFoundError:
            return
        if _identity(facts) != (record.device, record.inode):
            raise PublicationError("cleanup preserved a replaced output name")
        os.unlink(record.name, dir_fd=directory.fd)

    def _data_scratch_closed(self):
        # Only the two ownership roots/pages may remain at the commit boundary.
        roots = (self._journal._inventory.root, self._journal._links.root)
        pages = sum(root.page_count for root in roots)
        if (self.store._reader_pins or self.store._ledger_roots != 2 or
                self.store._pending_allocation is not None or
                self.store._detached_count + len(self.store._entries) != pages or
                any(entry.writer is not None for entry in self.store._entries.values())):
            raise ResourceError("data-processing scratch must close before publication")

    def _unlink_owned(self, artifact: Artifact, *, final: bool) -> None:
        present = artifact._linked if final else artifact._staged
        directory = self._destination if final else self._stage
        if present:
            try:
                facts = os.stat(artifact.name, dir_fd=directory.fd, follow_symlinks=False)
            except FileNotFoundError:
                facts = None
            if facts is not None:
                if _identity(facts) != artifact._identity:
                    raise PublicationError("cleanup preserved a replaced output name")
                os.unlink(artifact.name, dir_fd=directory.fd)
            if final:
                artifact._linked = False
            else:
                artifact._staged = False
        # Retry the refund even if the last unlink completed before interruption.
        # Final and staged hard links share one physical byte charge.
        if not artifact._linked and not artifact._staged and artifact._written:
            if artifact._refund is None:
                artifact._refund = DiskRefund(self.disk_budget, artifact._identity[0], artifact._written)
            artifact._refund.commit()
            artifact._written = 0

    def publish_all(self, completion_bytes: bytes) -> PublicationReport:
        if self._state != "open":
            raise PublicationError("output transaction cannot be published twice")
        if not isinstance(completion_bytes, bytes):
            raise ConfigurationError("completion document must be immutable bytes")
        if len(completion_bytes) > min(_METADATA_MAX, self.max_artifact_bytes, self.metadata_bytes):
            raise ResourceError("completion document exceeds its admitted metadata cap")
        if self._active or self._created != self._journal.inventory_count:
            raise PublicationError("all artifacts must be closed and registered before publication")
        # Allocate the ordinary success result before making any final link.
        # Immutable completion bytes remain under their caller's serialization
        # lease; this reservation covers only publication/report bookkeeping.
        completion_lease = self.budget.acquire("completion-state", 4096, metadata_bytes=4096)
        success_report = PublicationReport(True, True, self.completion_basename)
        self._state = "publishing"
        try:
            self._journal.freeze_inventory()
            self._data_scratch_closed()
            with self._journal.iter_inventory() as records:
                for record in records:
                    self._link(self._token(record))
            self._journal.validate_links()
            self._poll()
            os.fsync(self._destination.fd)
            with self._journal.iter_inventory() as records:
                for record in records:
                    self._poll()
                    self._unlink_record(record, final=False)
            self._poll()
            os.fsync(self._stage.fd)
            marker = self._create(self.completion_basename, completion=True)
            marker.write(completion_bytes)
            marker.close()
            self.register_closed(marker, len(completion_bytes), hashlib.sha256(completion_bytes).digest())
            self._link(marker)
            self._poll()
            # A failure here means indeterminate durability until rollback
            # succeeds. Only this final successful sync establishes commit.
            os.fsync(self._destination.fd)
            self._state = "committed"
        except BaseException as exc:
            primary: BaseException = exc
            marker_visible = (self._state != "committed" and self._completion is not None
                              and self._completion._linked)
            if isinstance(exc, OSError):
                if self._state == "committed":
                    message = "postcommit output operation failed"
                elif marker_visible:
                    message = "completion durability is indeterminate; ownership-aware rollback attempted"
                else:
                    message = "output publication failed before durable completion"
                primary = PublicationError(message)
            elif marker_visible:
                primary.add_note("completion durability is indeterminate; ownership-aware rollback attempted")
            self.abort(primary)
            if primary is not exc:
                raise primary from exc
            raise
        finally:
            completion_lease.release()
        try:
            self._cleanup(committed=True)
            if not self._warnings:
                return success_report
            return PublicationReport(True, False, self.completion_basename, tuple(self._warnings))
        except BaseException as exc:
            self._note_committed(exc)
            self.abort(exc)
            raise

    def _note_committed(self, primary: BaseException) -> None:
        note = f"outputs are durably committed; completion marker: {self.completion_basename}"
        if note not in getattr(primary, "__notes__", ()):
            primary.add_note(note)

    def _warn(self, warning: str) -> None:
        if warning not in self._warnings and len(self._warnings) < 4:
            self._warnings.append(warning)

    def _cleanup(self, *, committed: bool) -> None:
        if self._resources_closed:
            return
        interruption: BaseException | None = None

        def attempt(action, warning: str) -> None:
            nonlocal interruption
            # An interrupt can arrive after an unlink already succeeded. Every
            # action here is ownership checked and safe to retry once. Preserve
            # the first interrupt, finish available cleanup, then propagate it.
            for retry in range(2):
                try:
                    action()
                    return True
                except (CancelledError, KeyboardInterrupt) as exc:
                    if interruption is None:
                        interruption = exc
                    if retry == 0:
                        continue
                    self._warn(warning)
                except (OSError, ReadMixerError):
                    self._warn(warning)
                    return False
                except BaseException as exc:
                    if interruption is None:
                        interruption = exc
                    self._warn(warning)
                    return False
            return False

        # Do not poll cancellation during rollback; actual signal exceptions
        # still propagate after cleanup. External writers must already be joined.
        try:
            for artifact in self._active.values():
                attempt(artifact._close_for_cleanup, "an output descriptor could not be closed")
            if not committed and self._completion is not None:
                attempt(lambda: self._unlink_owned(self._completion, final=True),
                        "completion rollback incomplete; durability is indeterminate")
            pending_clean = True
            pending = self._pending_link
            if pending is not None and not pending._completion:
                if pending._ticket is None or not pending._ticket.committed:
                    if not committed and pending._linked:
                        pending_clean = attempt(lambda: self._unlink_record(self._record(pending), final=True),
                                "output rollback incomplete; owned or replaced names remain")
                    if pending._ticket is not None:
                        self._journal.discard(pending._ticket)
            self._pending_link = None
            links_done = False

            def clean_link(record, progress):
                if not committed and not progress.flags & 1:
                    self._unlink_record(record, final=True)
                    progress.flags |= 1

            def drain_links():
                nonlocal links_done
                self._journal.drain_links(clean_link)
                links_done = pending_clean

            attempt(drain_links, "output rollback incomplete; owned or replaced names remain")
            for artifact in self._active.values():
                if artifact._ticket is not None and artifact._ticket.committed:
                    continue  # interrupted registration already handed ownership off
                attempt(lambda: self._unlink_owned(artifact, final=False), "private staging cleanup incomplete")

            def clean_inventory(record, progress):
                if not progress.flags & 1:
                    self._unlink_record(record, final=False)
                    progress.flags |= 1
                if not committed and not links_done:
                    raise PublicationError("unresolved final links retain inventory ownership")
                if not committed and not progress.flags & 2:
                    if progress.refund is None:
                        progress.refund = DiskRefund(self.disk_budget, record.device, record.length)
                    progress.refund.commit()
                    progress.flags |= 2

            attempt(lambda: self._journal.drain_inventory(clean_inventory), "private staging cleanup incomplete")
            attempt(self._journal.close, "ownership journal cleanup incomplete")
            # A failed authenticated traversal retains residue. Abandon releases
            # only RAM/handles; it must never infer paths or delete unknown pages.
            attempt(self._journal.abandon, "ownership journal handle cleanup incomplete")
            attempt(self.store.close, "protected ownership scratch cleanup incomplete")
            if not self.store._closed:
                attempt(self.store.abandon, "protected scratch descriptor cleanup incomplete")
            for directory in (self._stage, self._destination):
                attempt(lambda: os.fsync(directory.fd), "cleanup directory sync failed")
            attempt(self._stage.remove_if_empty, "private staging directory cleanup incomplete")
            attempt(lambda: os.fsync(self._destination.fd), "cleanup directory sync failed")
        finally:
            for directory in (self._stage, self._destination):
                attempt(directory.close, "an output directory descriptor could not be closed")
            self._active.clear()
            self._completion = None
            self._lease.release()
            if self._owns_disk:
                attempt(self.disk_budget.close, "disk accounting cleanup failed")
            self._resources_closed = True
        if interruption is not None:
            if committed:
                self._note_committed(interruption)
            for warning in self._warnings:
                interruption.add_note(warning)
            raise interruption

    def abort(self, primary: BaseException | None = None) -> tuple[str, ...]:
        committed = self._state == "committed"
        try:
            self._cleanup(committed=committed)
        except BaseException as cleanup_error:
            if committed:
                self._note_committed(cleanup_error)
            if primary is None:
                raise
            if cleanup_error is not primary:
                primary.add_note("output cleanup also failed; primary error preserved")
        finally:
            if not committed:
                self._state = "aborted"
            if primary is not None:
                if committed:
                    self._note_committed(primary)
                for warning in self._warnings:
                    if warning not in getattr(primary, "__notes__", ()):
                        primary.add_note(warning)
        return tuple(self._warnings)

    def __enter__(self) -> OutputTransaction:
        return self

    def __exit__(self, exc_type: object, exc: BaseException | None, traceback: object) -> None:
        try:
            self.abort(exc)
        except BaseException:
            if exc is None:
                raise
            exc.add_note("output cleanup also failed; primary error preserved")
