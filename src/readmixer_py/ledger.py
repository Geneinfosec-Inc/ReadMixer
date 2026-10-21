"""Immutable authenticated registry pages with one trusted root per ledger.

Pages contain a previous expected SegmentRef, previous aggregate digest, and
bounded length-prefixed opaque records. Only the last reference and aggregate
counters stay in RAM. Auto/external pages are sealed and spilled before root
handoff, so serialized physical references never change after publication.
"""

from contextlib import AbstractContextManager
from dataclasses import dataclass
import hashlib
import struct

from .crypto import registry_identity
from .errors import CancelledError, ConfigurationError, IntegrityError, ResourceError
from .resources import checked_add
from .storage import deserialize_segment_ref, serialize_segment_ref

_MAGIC = b"RMPYLDG1"
_HEADER = struct.Struct(">8sQIQI32s")  # ordinal, records, record bytes, previous-ref length, Dprev
_LENGTH = struct.Struct(">I")
_EMPTY = hashlib.sha256(b"RMPY1-ledger-empty").digest()
_MAX_REF = 4608
_MAX_RECORDS = 1024


@dataclass(frozen=True)
class LedgerRoot:
    ledger_id: bytes
    last: object | None
    page_count: int
    record_count: int
    decoded_bytes: int  # Sum of opaque record payloads, excluding page framing.
    aggregate_digest: bytes


class PagedLedger(AbstractContextManager):
    def __init__(self, store, *, page_bytes=64 << 10, max_record_bytes=16 << 10, cancel=None, owns_records=False):
        if type(page_bytes) is not int or not 8192 <= page_bytes <= 1 << 20:
            raise ConfigurationError("ledger page bytes must be in 8192..1048576")
        if type(max_record_bytes) is not int or not 1 <= max_record_bytes <= page_bytes - _HEADER.size - _MAX_REF - 4:
            raise ConfigurationError("ledger record limit cannot fit its page")
        if cancel is not None and not callable(cancel):
            raise ConfigurationError("ledger cancellation must be callable")
        if type(owns_records) is not bool:
            raise ConfigurationError("ledger ownership mode must be boolean")
        previous_roots = getattr(store, "_ledger_roots", 0)
        if previous_roots >= 1024:
            raise ResourceError("trusted ledger root limit reached")
        self.store, self.page_bytes, self.max_record_bytes, self.cancel = store, page_bytes, max_record_bytes, cancel
        metadata_bytes = page_bytes * 4 + 128 * _MAX_RECORDS + 8192 + (256 * _MAX_RECORDS if owns_records else 0)
        self._lease = store.budget.acquire("ledger-root", metadata_bytes, metadata_bytes=metadata_bytes)
        self._cleanup_lease = None
        try:
            self._cleanup_lease = store.budget.acquire("ledger-cleanup", 8 << 20,
                                                       descriptors=int(store.mode != "memory"),
                                                       metadata_bytes=8192 + page_bytes)
            self.root = LedgerRoot(store.allocate_id(), None, 0, 0, 0, _EMPTY)
            self._pending = bytearray()
            self._pending_count = self._pending_bytes = 0
            self._cursor_active = self._closed = self._cleaning = False
            self._cursor = None
            self._cleanup_step = None
            self._owns_records = owns_records
            # end/count/payload bytes and lifetime append count; one assignment
            # transfers ownership even if append never returns to its caller.
            self._owned_boundary = (0, 0, 0, 0)
            self._owned_drain = None
            store._ledger_roots = previous_roots + 1
        except BaseException as primary:
            # Construction has not handed this root to a caller. If the final
            # coordinator-owned registration committed before interruption,
            # restore its previous count rather than leave a phantom root.
            if store._ledger_roots == previous_roots + 1:
                try:
                    store._ledger_roots = previous_roots
                except BaseException as cleanup:
                    primary.add_note(f"ledger registration rollback also failed: {type(cleanup).__name__}")
            for lease in (self._cleanup_lease, self._lease):
                if lease is not None:
                    try:
                        lease.release()
                    except BaseException as cleanup:
                        primary.add_note(f"ledger construction cleanup also failed: {type(cleanup).__name__}")
            raise

    def _check(self):
        if self._closed or self._cleaning:
            raise ResourceError("ledger is closed or cleaning up")
        if self.cancel is not None and self.cancel():
            raise CancelledError()

    @property
    def owned_count(self):
        return self._owned_boundary[3]

    @property
    def owned_remaining(self):
        return self.root.record_count + self._owned_boundary[1]

    def _commit_owned_boundary(self, boundary):
        self._owned_boundary = boundary

    def _append_owned(self, record):
        self._check()
        if self._cursor_active:
            raise ResourceError("cannot append while a ledger reader is active")
        if not isinstance(record, bytes) or len(record) > self.max_record_bytes:
            raise ResourceError("ledger record exceeds its admitted bound")
        before = self._owned_boundary
        del self._pending[before[0]:]
        if (_HEADER.size + _MAX_REF + before[0] + 4 + len(record) > self.page_bytes
                or before[1] >= _MAX_RECORDS):
            self.flush()
            before = self._owned_boundary
        checked_add(self.root.record_count, before[1] + 1)
        checked_add(self.root.decoded_bytes, before[2] + len(record))
        boundary = (before[0] + 4 + len(record), before[1] + 1,
                    before[2] + len(record), checked_add(before[3], 1))
        try:
            self._pending.extend(_LENGTH.pack(len(record)) + record)
            self._commit_owned_boundary(boundary)
        except BaseException:
            if self._owned_boundary is not boundary:
                del self._pending[before[0]:]
            raise
        finally:
            self._pending_count, self._pending_bytes = self._owned_boundary[1:3]

    def append(self, record):
        if self._owns_records:
            return self._append_owned(record)
        self._check()
        if self._cursor_active:
            raise ResourceError("cannot append while a ledger reader is active")
        if not isinstance(record, bytes) or len(record) > self.max_record_bytes:
            raise ResourceError("ledger record exceeds its admitted bound")
        footprint = _HEADER.size + _MAX_REF + len(self._pending) + 4 + len(record)
        if footprint > self.page_bytes or self._pending_count >= _MAX_RECORDS:
            self.flush()
        # Check cumulative widths before any allocation or root update.
        checked_add(self.root.record_count, self._pending_count + 1)
        checked_add(self.root.decoded_bytes, self._pending_bytes + len(record))
        before = len(self._pending)
        try:
            self._pending.extend(_LENGTH.pack(len(record)) + record)
        except BaseException:
            del self._pending[before:]
            raise
        self._pending_count += 1
        self._pending_bytes += len(record)

    def flush(self):
        self._check()
        if not self._pending_count:
            return
        if self._cursor_active:
            raise ResourceError("cannot flush while a ledger reader is active")
        root = self.root
        next_pages = checked_add(root.page_count, 1)
        previous = serialize_segment_ref(root.last) if root.last is not None else b""
        header = _HEADER.pack(_MAGIC, root.page_count, self._pending_count,
                              self._pending_bytes, len(previous), root.aggregate_digest)
        ref = None
        next_root = None
        next_pending = bytearray()
        next_boundary = (0, 0, 0, self.owned_count)
        try:
            with self.store.new_segment(4, registry_identity(root.ledger_id, root.page_count, 0)) as writer:
                writer.append(header)
                writer.append(previous)
                writer.append(self._pending, item_count=self._pending_count)
                ref = writer.seal()
            if self.store.mode != "memory":
                ref = self.store.spill(ref)
            digest = hashlib.sha256(b"RMPY1-ledger-page" + root.aggregate_digest
                                     + root.page_count.to_bytes(8, "big") + ref.digest).digest()
            next_root = LedgerRoot(root.ledger_id, ref, next_pages,
                                   checked_add(root.record_count, self._pending_count),
                                   checked_add(root.decoded_bytes, self._pending_bytes), digest)
            # Detach only after all fallible validation/allocation is complete.
            # The assignment is the atomic trusted-root ownership handoff.
            if self.store.mode != "memory":
                self.store.detach(ref)
            self.root = next_root
            self._pending = next_pending
            self._pending_count = self._pending_bytes = 0
            if self._owns_records:
                self._owned_boundary = next_boundary
        except BaseException as primary:
            if next_root is not None and self.root is next_root:
                # The immutable page is committed to the trusted root. Even an
                # interrupt during housekeeping must not remove or append it twice.
                self._pending = next_pending
                self._pending_count = self._pending_bytes = 0
                if self._owns_records:
                    self._owned_boundary = next_boundary
            elif ref is not None:
                try:
                    self.store.drop(ref)
                except BaseException:
                    primary.add_note("pending ledger page cleanup also failed")
            raise

    def open_reverse(self, *, flush=True, check_cancel=True):
        """Yield records newest first; normal exit proves the complete chain."""
        if self._closed:
            raise ResourceError("ledger is closed")
        if flush:
            self.flush()
        if self._cursor_active:
            raise ResourceError("ledger already has an active reader")
        if not check_cancel:
            self._begin_cleanup()
        cursor = _Cursor(self, check_cancel=check_cancel)
        self._cursor = cursor
        self._cursor_active = True
        return cursor

    def _page(self, ref, ordinal, expected_digest, *, cleanup=False):
        data = ranges = previous = None
        try:
            if ref.purpose != 4 or ref.logical_identity != registry_identity(self.root.ledger_id, ordinal, 0):
                raise IntegrityError("ledger page identity or ordinal is invalid")
            if not _HEADER.size <= ref.decoded_bytes <= self.page_bytes:
                raise IntegrityError("ledger page exceeds its admitted decoded bound")
            with self.store.open_verified(ref, cleanup=cleanup) as stream:
                data = stream.read(ref.decoded_bytes)
                if len(data) != ref.decoded_bytes:
                    raise IntegrityError("ledger page is truncated")
            magic, found_ordinal, count, record_bytes, ref_bytes, previous_digest = _HEADER.unpack_from(data)
            if (magic != _MAGIC or found_ordinal != ordinal or not 1 <= count <= _MAX_RECORDS
                    or count != ref.item_count or ref_bytes > _MAX_REF):
                raise IntegrityError("ledger page header is invalid")
            observed = hashlib.sha256(b"RMPY1-ledger-page" + previous_digest
                                      + ordinal.to_bytes(8, "big") + ref.digest).digest()
            if observed != expected_digest:
                raise IntegrityError("ledger aggregate digest is inconsistent")
            cursor = _HEADER.size
            if cursor + ref_bytes > len(data) or (ordinal == 0) != (ref_bytes == 0):
                raise IntegrityError("ledger backward reference is invalid")
            previous = deserialize_segment_ref(data[cursor:cursor + ref_bytes]) if ref_bytes else None
            cursor += ref_bytes
            ranges = []
            total = 0
            for _ in range(count):
                if cursor + 4 > len(data):
                    raise IntegrityError("ledger record prefix is truncated")
                length, = _LENGTH.unpack_from(data, cursor)
                cursor += 4
                if length > self.max_record_bytes or cursor + length > len(data):
                    raise IntegrityError("ledger record exceeds its admitted length")
                ranges.append((cursor, cursor + length))
                cursor += length
                total = checked_add(total, length)
            if cursor != len(data) or total != record_bytes:
                raise IntegrityError("ledger page payload totals or trailing bytes are invalid")
            if ordinal == 0 and previous_digest != _EMPTY:
                raise IntegrityError("ledger chain has an invalid initial digest")
            return data, ranges, previous, previous_digest, total
        finally:
            # A retained failure traceback must not keep a decoded page/range
            # graph after its ledger has subsequently been disposed.
            data = ranges = previous = None

    def _begin_cleanup(self):
        # Release a dedicated overlap reservation immediately before the serial
        # authenticated reader acquires it. The ledger cannot accept new work
        # after this point; metadata remains fully leased until pages are gone.
        self._cleaning = True
        if self._cleanup_lease is not None:
            self._cleanup_lease.release()
            self._cleanup_lease = None

    def _retire_root(self):
        """Finish closed-state housekeeping if the root debit already committed."""
        before = self.store._ledger_roots
        if before < 1:
            raise IntegrityError("trusted ledger root accounting is inconsistent")
        try:
            # This coordinator-owned count is the canonical retirement boundary.
            # A signal after its assignment must not permit another debit.
            self.store._ledger_roots = before - 1
            self._closed = True
        except BaseException:
            if self.store._ledger_roots == before - 1:
                self._closed = True
            raise

    def _owned_page(self):
        """Admit one complete, authenticated cleanup page before any callback."""
        end, count, payload_bytes, _total = self._owned_boundary
        data = ranges = None
        try:
            if count:
                data = bytes(self._pending[:end])
                ranges, cursor, observed = [], 0, 0
                for _ in range(count):
                    if cursor + 4 > len(data):
                        raise IntegrityError("owned pending record prefix is truncated")
                    length, = _LENGTH.unpack_from(data, cursor)
                    cursor += 4
                    if length > self.max_record_bytes or cursor + length > len(data):
                        raise IntegrityError("owned pending record length is invalid")
                    ranges.append((cursor, cursor + length))
                    cursor += length
                    observed += length
                if cursor != len(data) or observed != payload_bytes:
                    raise IntegrityError("owned pending boundary is inconsistent")
                return _OwnedDrainPage(True, None, None, data, ranges, bytearray(count), [None] * count)
            root = self.root
            if root.last is None:
                if root.page_count or root.record_count or root.decoded_bytes or root.aggregate_digest != _EMPTY:
                    raise IntegrityError("owned ledger root does not end at the empty chain")
                return None
            data, ranges, previous, digest, observed = self._page(
                root.last, root.page_count - 1, root.aggregate_digest, cleanup=True)
            if len(ranges) > root.record_count or observed > root.decoded_bytes:
                raise IntegrityError("owned ledger page exceeds root totals")
            successor = LedgerRoot(root.ledger_id, previous, root.page_count - 1,
                                   root.record_count - len(ranges), root.decoded_bytes - observed, digest)
            return _OwnedDrainPage(False, root, successor, data, ranges, bytearray(len(ranges)), [None] * len(ranges))
        finally:
            data = ranges = None

    def _retire_owned_page(self, page):
        try:
            if page.pending:
                empty = bytearray()
                self._owned_boundary = (0, 0, 0, self.owned_count)
                self._pending = empty
                self._pending_count = self._pending_bytes = 0
            elif self.root is not page.successor:
                if self.root is not page.root:
                    raise IntegrityError("owned cleanup root transition is inconsistent")
                # Both roots and all record progress remain retained if drop or the
                # following assignment is interrupted. Retrying cannot repeat callbacks.
                self.store.drop(page.root.last, missing_ok=True)
                self.root = page.successor
            self._owned_drain = None
        finally:
            page = None

    def drain_owned(self, callback, *, validate=None):
        """Consume external ownership before removing its bookkeeping pages.

        Callback receives (record_bytes, CleanupProgress). Its seven flag bits
        persist across retry; bit seven records a successfully returned callback.
        One pending/decoded page is retained on failure, with no historical table.
        The optional validator receives each record and its chronological append
        position; all positions in the current page are checked before callbacks.
        """
        if not self._owns_records:
            raise ConfigurationError("ordinary ledger records do not own external objects")
        if not callable(callback) or validate is not None and not callable(validate):
            raise ConfigurationError("owned cleanup callbacks must be callable")
        if self._closed:
            return
        if self._cursor_active:
            raise ResourceError("cannot drain ownership with an active ledger reader")
        self._begin_cleanup()
        first = page = progress = record = None
        try:
            while True:
                if self._owned_drain is None:
                    self._owned_drain = self._owned_page()
                page = self._owned_drain
                if page is None:
                    break
                # Validate every opaque ownership record in the authenticated page
                # before any one of those records may authorize a deletion.
                if not page.validated:
                    if validate is not None:
                        base = self.owned_remaining - len(page.ranges)
                        for index, (start, end) in enumerate(page.ranges):
                            validate(page.data[start:end], base + index)
                    page.validated = True
                failed = False
                for index in range(len(page.ranges) - 1, -1, -1):
                    if page.flags[index] & 128:
                        continue
                    start, end = page.ranges[index]
                    record = page.data[start:end]
                    progress = CleanupProgress(page.flags, page.refunds, index)
                    for attempt in range(2):
                        try:
                            callback(record, progress)
                            page.flags[index] |= 128
                            break
                        except BaseException as error:
                            if first is None:
                                first = error
                            if isinstance(error, (CancelledError, KeyboardInterrupt)) and attempt == 0:
                                continue
                            failed = True
                            break
                if failed:
                    raise first
                self._retire_owned_page(page)
                page = progress = record = None
            if first is not None:
                raise first
        except BaseException as error:
            # A retried callback may have completed before a later page fails
            # authentication, validation or retirement. Cleanup still stops at
            # that page, but the first interruption remains the causal error.
            if first is not None and error is not first:
                first.add_note(f"owned ledger cleanup also failed: {type(error).__name__}")
                raise first
            raise
        finally:
            # Retained tracebacks must not keep a second page representation once
            # the live ledger has been drained/disposed by a later cleanup attempt.
            first = page = progress = record = None

    def abandon(self):
        """Forget owned cleanup authority without deleting uncertain physical pages.

        The operation coordinator must separately abandon the store's remaining
        entries/anchors. No record callback or filesystem unlink runs here.
        """
        if not self._owns_records:
            raise ConfigurationError("only an ownership ledger can abandon its records")
        if self._closed:
            return
        empty = LedgerRoot(self.root.ledger_id, None, 0, 0, 0, _EMPTY)
        if self._cursor is not None:
            self._cursor.discard()
        self._pending = bytearray()
        self._pending_count = self._pending_bytes = 0
        self._owned_boundary = (0, 0, 0, self.owned_count)
        self._owned_drain = self._cleanup_step = None
        self.root = empty
        if self._cleanup_lease is not None:
            self._cleanup_lease.release()
            self._cleanup_lease = None
        self._lease.release()
        self._cleaning = True
        self._retire_root()

    def close(self):
        if self._closed:
            return
        if self._cursor_active:
            raise ResourceError("cannot close a ledger with an active reader")
        if self._owns_records and (self.owned_remaining or self._owned_drain is not None):
            raise ResourceError("ownership ledger must drain its records before close")
        self._begin_cleanup()
        # Pending RAM records were never committed; cleanup never flushes them.
        self._pending.clear()
        self._pending_count = self._pending_bytes = 0
        if self._cleanup_step is not None and self.root is self._cleanup_step[1]:
            self._cleanup_step = None
        while self.root.last is not None:
            root = self.root
            if self._cleanup_step is None:
                data, ranges, previous, previous_digest, total = self._page(
                    root.last, root.page_count - 1, root.aggregate_digest, cleanup=True)
                if len(ranges) > root.record_count or total > root.decoded_bytes:
                    raise IntegrityError("ledger root totals are inconsistent")
                successor = LedgerRoot(root.ledger_id, previous, root.page_count - 1,
                                       root.record_count - len(ranges), root.decoded_bytes - total,
                                       previous_digest)
                # Allocate/retain the authenticated successor before deleting the
                # only on-disk copy of its reference. Retain this intent on failure.
                self._cleanup_step = (root, successor)
                del data, ranges
            else:
                predecessor, successor = self._cleanup_step
                if root is not predecessor:
                    raise IntegrityError("ledger cleanup ownership state is inconsistent")
            self.store.drop(root.last, missing_ok=True)
            self.root = successor
            self._cleanup_step = None
        root = self.root
        if root.page_count or root.record_count or root.decoded_bytes or root.aggregate_digest != _EMPTY:
            raise IntegrityError("ledger root does not describe a complete chain")
        self._pending = bytearray()
        self._cleanup_step = None
        self._lease.release()
        self._retire_root()

    def __exit__(self, exc_type, exc, traceback):
        try:
            self.close()
        except BaseException:
            if exc is None:
                raise
            exc.add_note("ledger cleanup also failed; primary error preserved")


class _Cursor(AbstractContextManager):
    def __init__(self, ledger, *, check_cancel=True):
        self.ledger = ledger
        self._check_cancel = check_cancel
        self._ref = ledger.root.last
        self._ordinal = ledger.root.page_count
        self._digest = ledger.root.aggregate_digest
        self._records, self._bytes = ledger.root.record_count, ledger.root.decoded_bytes
        self._data, self._ranges = b"", []
        self._closed = False

    def __iter__(self):
        return self

    def __next__(self):
        if self._closed:
            raise ResourceError("ledger cursor is closed")
        if self._check_cancel:
            self.ledger._check()
        elif self.ledger._closed:
            raise ResourceError("ledger is closed")
        while not self._ranges:
            if self._ref is None:
                if self._ordinal or self._records or self._bytes or self._digest != _EMPTY:
                    raise IntegrityError("ledger cursor did not reach its expected complete root")
                raise StopIteration
            if self._ordinal == 0:
                raise IntegrityError("ledger contains an extra backward page")
            self._ordinal -= 1
            self._data, self._ranges, self._ref, self._digest, total = self.ledger._page(self._ref, self._ordinal, self._digest, cleanup=not self._check_cancel)
            if len(self._ranges) > self._records or total > self._bytes:
                raise IntegrityError("ledger cursor exceeds root totals")
            self._records -= len(self._ranges)
            self._bytes -= total
        start, end = self._ranges.pop()
        return self._data[start:end]

    def discard(self):
        self._data, self._ranges = b"", []
        self._ref = None
        self.ledger._cursor_active = False
        if self.ledger._cursor is self:
            self.ledger._cursor = None
        self._closed = True

    def __exit__(self, exc_type, exc, traceback):
        try:
            if exc is None and not self._closed:
                for _ in self:
                    pass
        finally:
            self.discard()


@dataclass(slots=True)
class _OwnedDrainPage:
    pending: bool
    root: object
    successor: object
    data: bytes
    ranges: list
    flags: bytearray
    refunds: list
    validated: bool = False


class CleanupProgress:
    """A bounded current-record view of caller-owned idempotency phase bits."""

    __slots__ = ('_flags', '_refunds', '_index')

    def __init__(self, flags, refunds, index):
        self._flags, self._refunds, self._index = flags, refunds, index

    @property
    def refund(self):
        return self._refunds[self._index]

    @refund.setter
    def refund(self, token):
        self._refunds[self._index] = token

    @property
    def flags(self):
        return self._flags[self._index] & 127

    @flags.setter
    def flags(self, value):
        if type(value) is not int or not 0 <= value <= 127:
            raise ConfigurationError("cleanup progress flags must fit seven bits")
        self._flags[self._index] = (self._flags[self._index] & 128) | value

    def mark(self, flag):
        if type(flag) is not int or not 0 <= flag <= 127:
            raise ConfigurationError("cleanup progress flag must fit seven bits")
        self.flags = self.flags | flag
