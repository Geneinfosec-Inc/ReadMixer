"""Bounded protected run sorting and exact canonical identity coverage.

Run registries use authenticated ledger pages, with two roots during a merge
pass. A consumed-prefix counter identifies removed parents without tombstones
or a dataset-sized ownership map. Memory mode never creates disk scratch.
"""
from __future__ import annotations

from collections.abc import Sequence
from contextlib import AbstractContextManager, ExitStack, contextmanager
import heapq
import struct
import sys

from .crypto import sort_identity
from .errors import CancelledError, ConfigurationError, IntegrityError, ResourceError
from .ledger import PagedLedger
from .resources import checked_add
from .storage import deserialize_segment_ref, serialize_segment_ref
from .stage_policy import StagePolicy, SortPolicy

_LENGTH = struct.Struct('>I')
_IDENTITY = struct.Struct('>IQ')
_STREAM_WORK = 8 << 20  # Versioned storage reader/native workspace allowance.
_WRITER_WORK = 10 << 20  # Storage output workspace, block and metadata overlap.
_KEY_BYTES = 4096
_MAX_RUN_RECORDS = 32768
_ROW_OVERHEAD = 256  # tuple/list slots, native sort pointer workspace, scalars.


def _positive(value, label, maximum=(1 << 63) - 1):
    if type(value) is not int or not 1 <= value <= maximum:
        raise ConfigurationError(f'{label} must be a positive bounded integer')
    return value


def _key_footprint(value, depth=0):
    """Reject arbitrary objects, unbounded integers and recursive key trees."""
    if type(value) is int:
        if not -(1 << 63) <= value <= (1 << 64) - 1:
            raise ConfigurationError('sort integer key exceeds its bounded width')
    elif type(value) is bytes:
        if len(value) > _KEY_BYTES:
            raise ConfigurationError('sort byte key exceeds its admitted bound')
    elif type(value) is tuple:
        if depth >= 4 or len(value) > 16:
            raise ConfigurationError('sort tuple key exceeds its admitted bound')
        size = sys.getsizeof(value)
        for component in value:
            size += _key_footprint(component, depth + 1)
            if size > _KEY_BYTES:
                raise ConfigurationError('sort key representation exceeds its admitted bound')
        return size
    else:
        raise ConfigurationError('sort keys must be bounded integers, bytes or tuples')
    size = sys.getsizeof(value)
    if size > _KEY_BYTES:
        raise ConfigurationError('sort key representation exceeds its admitted bound')
    return size


class ExternalSorter(AbstractContextManager):
    """Sort byte records through bounded RAM runs and protected merge segments.

    ``run_bytes`` covers retained records, keys, Python metadata and native sort
    pointer workspace. Record payloads have an independent per-record bound.
    The key callable is trusted code; its returned representation is validated.
    Sorted iteration is single-use and its normal context exit drains and proves
    all source seals even when the consumer stops reading early.
    """

    def __init__(self, store, *, key=None, policy=None, run_bytes, max_record_bytes, fan_in=32,
                 sort_purpose='records', cancel=None):
        _positive(run_bytes, 'sort run budget')
        _positive(max_record_bytes, 'sort record bound', (1 << 32) - 1)
        if type(fan_in) is not int or not 2 <= fan_in <= 32:
            raise ConfigurationError('sort merge fan-in must be in 2..32')
        if isinstance(policy, SortPolicy):
            policy = StagePolicy(policy)
        if policy is not None:
            if type(policy) is not StagePolicy or policy.kind is SortPolicy.TERMINAL_RANK:
                raise ConfigurationError('sorter requires a closed record policy')
            if key is not None:
                raise ConfigurationError('sorter policy and custom callable are mutually exclusive')
            key = policy.key
        if not callable(key) or (cancel is not None and not callable(cancel)):
            raise ConfigurationError('sort key and cancellation must be callable')
        # Validate the scheduler context before allocating or admitting records.
        sort_identity(sort_purpose, 0, 0)
        self.store, self.key, self.policy = store, key, policy
        self.scheduler = getattr(store.budget, '_stage_scheduler', None) if policy is not None else None
        if self.scheduler is not None and (not self.scheduler.production or not self.scheduler.process_slots):
            self.scheduler = None
        self._submitted = {}
        self._run_order_reversed = False
        self._wire_bytes = 0
        self.run_bytes, self.max_record_bytes = run_bytes, max_record_bytes
        self.fan_in, self.sort_purpose, self.cancel = fan_in, sort_purpose, cancel
        self._metadata_lease = store.budget.acquire('sort-metadata', 65536, metadata_bytes=65536)
        try:
            self._lease = store.budget.acquire('sort-run', run_bytes)
        except BaseException:
            self._metadata_lease.release()
            raise
        self._runs = self._next_runs = None
        try:
            self._runs = PagedLedger(store, max_record_bytes=4608, cancel=cancel)
        except BaseException:
            self._lease.release()
            self._metadata_lease.release()
            raise
        self._rows = []
        self._row_bytes = self._row_metadata = self._consumed = self._next_consumed = 0
        self._pending = None
        self._ordinal = self._level = 0
        self._closed = self._started = self._active = self._failed = False
        self.records = self.merge_passes = 0
        self.effective_fan_in = None

    @property
    def run_count(self):
        return self._runs.root.record_count if self._runs is not None else 0

    def _check(self):
        if self._closed or self._failed:
            raise ResourceError('sorter is closed or has already failed')
        if self.cancel is not None and self.cancel():
            raise CancelledError()

    def _key(self, record):
        try:
            value = self.key(record)
        except (CancelledError, ResourceError, IntegrityError):
            raise
        except Exception:
            raise ConfigurationError('sort key callable failed') from None
        return value, _key_footprint(value)

    def append(self, record):
        self._check()
        if self._started:
            raise ResourceError('cannot append after sorted iteration begins')
        if type(record) is not bytes or len(record) > self.max_record_bytes:
            raise ResourceError('sort record exceeds its admitted byte bound')
        value, key_bytes = self._key(record)
        footprint = len(record) + key_bytes + _ROW_OVERHEAD
        if footprint > self.run_bytes:
            raise ResourceError('one sort record and key cannot fit the run lease')
        if (self._row_bytes + footprint > self.run_bytes or len(self._rows) >= _MAX_RUN_RECORDS
                or (self.scheduler is not None and self._rows
                    and self._wire_bytes + 4 + len(record) > self.scheduler.max_job_bytes)):
            self._flush_run()
        next_count = checked_add(self.records, 1)
        row_metadata = _ROW_OVERHEAD + (0 if value is record else key_bytes)
        next_metadata = self._row_metadata + row_metadata
        self._lease.resize(self.run_bytes, metadata_bytes=next_metadata)
        try:
            self._rows.append((value, record))
        except BaseException:
            self._lease.resize(self.run_bytes, metadata_bytes=self._row_metadata)
            raise
        self._wire_bytes += 4 + len(record)
        self._row_bytes += footprint
        self._row_metadata = next_metadata
        self.records = next_count

    def _publish_run(self, writer, ledger, expected_items, expected_bytes):
        ref = writer.seal()
        self._pending = ref
        if (ref.item_count, ref.decoded_bytes) != (expected_items, expected_bytes):
            raise IntegrityError('sorted run seal disagrees with its expected complete record totals')
        if self.store.mode != 'memory':
            ref = self.store.spill(ref)
            self._pending = ref
        ledger.append(serialize_segment_ref(ref))
        ledger.flush()
        # The authenticated root owns the run now, including if detach fails.
        self._pending = None
        if self.store.mode != 'memory':
            self.store.detach(ref)
        return ref

    def _new_writer(self):
        ordinal = self._ordinal
        self._ordinal = checked_add(ordinal, 1)
        # Non-memory runs already require encrypted spill before registration.
        # Stream them to that destination from the start: a merge result may
        # exceed the entire RAM budget while its reader/head window stays bounded.
        return self.store.new_segment(3, sort_identity(self.sort_purpose, self._level, ordinal),
                                      external=self.store.mode != 'memory')

    def _parallel_available(self):
        scheduler = self.scheduler
        if scheduler is None or scheduler._closed:
            return False
        owners = getattr(scheduler, '_sort_owners', {})
        return all(task in owners for task in scheduler._jobs)

    def _drain_one(self):
        scheduler = self.scheduler
        owners = getattr(scheduler, '_sort_owners', {})
        if scheduler._drain_task not in owners:
            raise ResourceError('sort result is blocked by another production stage')
        task = scheduler._drain_task
        owner, source, items, size, target = owners[task]
        result = scheduler.take_next()
        try:
            if result.records != items:
                raise IntegrityError('parallel sorted run changed its item count')
            if hasattr(result, 'ref'):
                ref = result.ref
                if ref.decoded_bytes != size:
                    raise IntegrityError('parallel sorted run changed its byte count')
                owner._pending = ref
                target.append(serialize_segment_ref(ref))
                target.flush()
                result.take_ref()  # authenticated run inventory now owns it
                owner._pending = None
                if owner.store.mode != 'memory':
                    owner.store.detach(ref)
            else:
                if len(result.data) != size:
                    raise IntegrityError('parallel sorted run changed its byte count')
                with owner._new_writer() as writer:
                    writer.append(result.data, item_count=items)
                    owner._publish_run(writer, target, items, size)
            owner.store.drop(source)
            del owner._submitted[task]
            del owners[task]
        finally:
            result.close()

    def _submit_run(self, source, items, size, target):
        scheduler = self.scheduler
        if not hasattr(scheduler, '_sort_owners'):
            scheduler._sort_owners = {}
        while scheduler.pending_jobs >= scheduler.capacity:
            self._drain_one()
        task = scheduler.submit_segment(source, policy=self.policy)
        entry = (self, source, items, size, target)
        self._submitted[task] = source
        scheduler._sort_owners[task] = entry
        self._pending = None

    def _drain_submitted(self):
        while self._submitted:
            self._drain_one()

    def _flush_run(self):
        self._check()
        if not self._rows:
            return
        try:
            parallel = self._parallel_available() and self._wire_bytes <= self.scheduler.max_job_bytes
            if parallel:
                while self.scheduler.pending_jobs >= self.scheduler.capacity:
                    self._drain_one()
            try:
                if not parallel:
                    self._rows.sort(key=lambda row: row[0])
            except TypeError:
                raise ConfigurationError('sort keys are not mutually comparable') from None
            except MemoryError as exc:
                raise ResourceError('native sort exceeded its admitted workspace') from exc
            with self._new_writer() as writer:
                for _value, record in self._rows:
                    self._check()
                    writer.append(_LENGTH.pack(len(record)))
                    writer.append(record, item_count=1)
                if parallel:
                    ref = writer.seal()
                    self._pending = ref
                    self._submit_run(ref, len(self._rows), self._wire_bytes, self._runs)
                else:
                    self._publish_run(writer, self._runs, len(self._rows), self._wire_bytes)
            self._rows.clear()
            self._row_bytes = self._row_metadata = self._wire_bytes = 0
            self._lease.resize(self.run_bytes, metadata_bytes=0)
        except BaseException:
            self._failed = True
            raise

    def _read_record(self, stream, length):
        # Records may span many authenticated blocks. Only each contributing
        # read is bounded to one MiB; the admitted carry remains record-sized.
        try:
            arena = bytearray(length)
            view = memoryview(arena)
            offset = 0
            while offset < length:
                self._check()
                count = stream.readinto(view[offset:offset + min(1 << 20, length - offset)])
                if not count:
                    raise IntegrityError('sort run record payload is truncated')
                offset += count
            return bytes(arena)
        except (MemoryError, OverflowError) as exc:
            raise ResourceError('cannot allocate the admitted sorted-record carry') from exc

    def _records_from(self, ref):
        if ref.purpose != 3:
            raise IntegrityError('sort run has an invalid segment purpose')
        previous = None
        has_previous = False
        with self.store.open_verified(ref) as stream:
            for _ in range(ref.item_count):
                self._check()
                prefix = stream.read(4)
                if len(prefix) != 4:
                    raise IntegrityError('sort run record prefix is truncated')
                length, = _LENGTH.unpack(prefix)
                if length > self.max_record_bytes:
                    raise IntegrityError('sort run record exceeds its admitted bound')
                record = self._read_record(stream, length)
                value, _size = self._key(record)
                try:
                    decreasing = has_previous and value < previous
                except TypeError:
                    raise IntegrityError('sort run keys are not mutually comparable') from None
                if decreasing:
                    raise IntegrityError('sort run keys are not nondecreasing')
                previous, has_previous = value, True
                yield value, record
            if stream.read(1):
                raise IntegrityError('sort run has uncounted or trailing records')

    def _admitted_fan_in(self):
        # The storage layer acquires native stream leases independently. Reserve
        # their simultaneous envelope when choosing fan-in, then acquire the
        # sorter-owned head copies before opening the streams themselves.
        head_bytes = 3 * self.max_record_bytes + 2 * _KEY_BYTES + 2048
        available = self.store.budget.remaining_bytes - _WRITER_WORK
        admitted = min(self.fan_in, max(0, available // (_STREAM_WORK + head_bytes)))
        if self.store.mode != 'memory':
            descriptors = self.store.budget.descriptor_limit - self.store.budget.used_descriptors
            admitted = min(admitted, max(0, descriptors - 1))
        if admitted < 2:
            raise ResourceError('cannot admit two merge readers and their output overlap')
        self.effective_fan_in = admitted
        return admitted, head_bytes

    @staticmethod
    def _push(heap, item):
        try:
            heapq.heappush(heap, item)
        except TypeError:
            raise IntegrityError('sort run keys are not mutually comparable') from None

    def _merge_group(self, refs, heads_bytes):
        expected_items = expected_bytes = 0
        for ref in refs:
            expected_items = checked_add(expected_items, ref.item_count)
            expected_bytes = checked_add(expected_bytes, ref.decoded_bytes)
        with self.store.budget.acquire('sort-merge-heads', heads_bytes * len(refs),
                                       metadata_bytes=(2 * _KEY_BYTES + 2048) * len(refs)):
            with self._new_writer() as writer:
                with ExitStack() as stack:
                    readers = []
                    heap = []
                    # Reverse registry traversal alternates run-group orientation
                    # on each pass. Preserve original input-run order for ties.
                    ordered_refs = refs if self._run_order_reversed else reversed(refs)
                    for number, ref in enumerate(ordered_refs):
                        reader = self._records_from(ref)
                        stack.callback(reader.close)
                        readers.append(reader)
                        try:
                            value, record = next(reader)
                        except StopIteration:
                            continue
                        self._push(heap, (value, number, record))
                    while heap:
                        self._check()
                        try:
                            _value, number, record = heapq.heappop(heap)
                        except TypeError:
                            raise IntegrityError('sort run keys are not mutually comparable') from None
                        writer.append(_LENGTH.pack(len(record)))
                        writer.append(record, item_count=1)
                        try:
                            value, record = next(readers[number])
                        except StopIteration:
                            continue
                        self._push(heap, (value, number, record))
                self._publish_run(writer, self._next_runs, expected_items, expected_bytes)
        # Source contexts authenticated to EOF before publishing this child.
        # Increment only after each successful deletion: cleanup can resume at
        # precisely the first still-owned parent without a resident run map.
        for ref in refs:
            self.store.drop(ref)
            self._consumed += 1

    def _parallel_merge_group(self, refs):
        items = size = 0
        for ref in refs:
            items = checked_add(items, ref.item_count)
            size = checked_add(size, ref.decoded_bytes)
        if not self._parallel_available() or size > self.scheduler.max_job_bytes:
            return False
        while self.scheduler.pending_jobs >= self.scheduler.capacity:
            self._drain_one()
        head = 3 * min(self.max_record_bytes, size) + 2 * _KEY_BYTES + 2048
        with self.store.budget.acquire('sort-worker-merge-head', head,
                metadata_bytes=2 * _KEY_BYTES + 2048), self._new_writer() as writer:
            ordered_refs = refs if self._run_order_reversed else reversed(refs)
            for ref in ordered_refs:
                for _value, record in self._records_from(ref):
                    writer.append(_LENGTH.pack(len(record)))
                    writer.append(record, item_count=1)
            source = writer.seal()
            self._pending = source
            self._submit_run(source, items, size, self._next_runs)
        # Source copies are fully authenticated and the submitted temporary is
        # tracked. Parent deletion follows original registry traversal order.
        for ref in refs:
            self.store.drop(ref)
            self._consumed += 1
        return True

    def _merge_pass(self):
        self._next_runs = PagedLedger(self.store, max_record_bytes=4608, cancel=self.cancel)
        fan_in, heads_bytes = self._admitted_fan_in()
        self._level += 1
        with self._runs.open_reverse() as cursor:
            group = []
            for encoded in cursor:
                self._check()
                group.append(deserialize_segment_ref(encoded))
                if len(group) == fan_in:
                    if not self._parallel_merge_group(group):
                        self._drain_submitted()
                        self._merge_group(group, heads_bytes)
                    group.clear()
            if group:
                if not self._parallel_merge_group(group):
                    self._drain_submitted()
                    self._merge_group(group, heads_bytes)
        self._drain_submitted()
        self._run_order_reversed = not self._run_order_reversed
        self._runs.close()
        self._runs, self._next_runs = self._next_runs, None
        self._consumed = self._next_consumed = 0
        self.merge_passes += 1

    def _final_records(self, reader):
        observed = 0
        for _value, record in reader:
            observed = checked_add(observed, 1)
            if observed > self.records:
                raise IntegrityError('sorted result exceeds the admitted input record count')
            yield record
        if observed != self.records:
            raise IntegrityError('sorted result omits admitted input records')

    @contextmanager
    def iter_sorted(self):
        self._check()
        if self._started:
            raise ResourceError('sorted iteration is single-use')
        self._started = self._active = True
        reader = None
        try:
            self._flush_run()
            self._drain_submitted()
            # The row arena is no longer live. Native stream and merge leases
            # replace its reservation without changing the configured topology.
            self._lease.release()
            while self.run_count > 1:
                self._merge_pass()
            if self.run_count:
                with self._runs.open_reverse() as cursor:
                    encoded = next(cursor)
                ref = deserialize_segment_ref(encoded)
                if ref.item_count != self.records:
                    raise IntegrityError('sorted run seal differs from the admitted input record count')
                with self.store.budget.acquire('sort-result-head',
                                               3 * self.max_record_bytes + 2 * _KEY_BYTES + 2048,
                                               metadata_bytes=2 * _KEY_BYTES + 2048):
                    reader = self._records_from(ref)
                    iterator = self._final_records(reader)
                    yield iterator
                    for _ in iterator:
                        pass
            else:
                if self.records:
                    raise IntegrityError('sorted result has no runs for admitted input records')
                yield iter(())
        except BaseException:
            self._failed = True
            raise
        finally:
            try:
                if reader is not None:
                    reader.close()
            finally:
                self._active = False

    def _clean_ledger(self, ledger, consumed):
        with ledger.open_reverse(flush=False, check_cancel=False) as cursor:
            for position, encoded in enumerate(cursor):
                if position < consumed:
                    continue
                self.store.drop(deserialize_segment_ref(encoded), missing_ok=True)
                consumed += 1
                if ledger is self._runs:
                    self._consumed = consumed
                else:
                    self._next_consumed = consumed
        ledger.close()

    def close(self):
        if self._closed:
            return
        if self._active:
            raise ResourceError('cannot close a sorter with an active result iterator')
        if self._submitted:
            # Reap first on failed work; never drop a source still in a child.
            if self._failed or (self.scheduler is not None and self.scheduler._closed):
                self.scheduler.abort()
                for task, ref in tuple(self._submitted.items()):
                    self.store.drop(ref, missing_ok=True)
                    getattr(self.scheduler, '_sort_owners', {}).pop(task, None)
                    del self._submitted[task]
            else:
                self._drain_submitted()
        self._rows.clear()
        self._row_bytes = self._row_metadata = self._wire_bytes = 0
        self._lease.release()
        if self._pending is not None:
            self.store.drop(self._pending, missing_ok=True)
            self._pending = None
        if self._runs is not None:
            self._clean_ledger(self._runs, self._consumed)
            self._runs = None
        if self._next_runs is not None:
            self._clean_ledger(self._next_runs, self._next_consumed)
            self._next_runs = None
        self._metadata_lease.release()
        self._closed = True

    def __exit__(self, exc_type, exc, traceback):
        try:
            self.close()
        except BaseException:
            if exc is None:
                raise
            exc.add_note('sorter cleanup failed; primary error preserved and protected scratch remains owned')


class IdentityCoverage(AbstractContextManager):
    """Prove dense per-sample identity coverage using an admitted bitmap or sort."""

    def __init__(self, store, sample_counts, *, bitset_limit_bytes=None,
                 run_bytes=1 << 20, fan_in=32, cancel=None):
        if (not isinstance(sample_counts, Sequence) or isinstance(sample_counts, (bytes, str))
                or len(sample_counts) > 10000):
            raise ConfigurationError('coverage requires at most 10000 sample counts')
        if bitset_limit_bytes is not None and (type(bitset_limit_bytes) is not int or bitset_limit_bytes < 0):
            raise ConfigurationError('coverage bitmap limit must be a nonnegative integer')
        if cancel is not None and not callable(cancel):
            raise ConfigurationError('coverage cancellation must be callable')
        self.store, self.cancel = store, cancel
        metadata_bytes = 4096 + 128 * len(sample_counts)
        self._metadata = store.budget.acquire('coverage-metadata', metadata_bytes, metadata_bytes=metadata_bytes)
        self._bitmap_lease = self._sorter = None
        self._bitmap = None
        self._closed = self._finished = False
        self.observed = 0
        try:
            self.counts = tuple(sample_counts)
            self.offsets = []
            total = 0
            for count in self.counts:
                if type(count) is not int or not 0 <= count <= (1 << 64) - 1:
                    raise ConfigurationError('coverage sample count must be an unsigned 64-bit integer')
                self.offsets.append(total)
                total = checked_add(total, count)
            self.total = total
            bitmap_bytes = (total + 7) // 8
            if bitset_limit_bytes is None or bitmap_bytes <= bitset_limit_bytes:
                try:
                    self._bitmap_lease = store.budget.acquire('coverage-bitmap', bitmap_bytes + 4096)
                except ResourceError:
                    if store.mode == 'memory':
                        raise
            if self._bitmap_lease is not None:
                try:
                    self._bitmap = bytearray(bitmap_bytes)
                except (MemoryError, OverflowError) as exc:
                    raise ResourceError('cannot allocate the admitted coverage bitmap') from exc
                self.mode = 'bitset'
            else:
                if store.mode == 'memory':
                    raise ResourceError('memory storage cannot admit the exact coverage bitmap')
                self._sorter = ExternalSorter(store, policy=SortPolicy.COVERAGE_IDENTITIES, run_bytes=run_bytes,
                                              max_record_bytes=12, fan_in=fan_in,
                                              sort_purpose='identity-coverage', cancel=cancel)
                self.mode = 'external-sort'
        except BaseException:
            if self._bitmap_lease is not None:
                self._bitmap_lease.release()
            self._metadata.release()
            raise

    def _check(self):
        if self._closed or self._finished:
            raise ResourceError('identity coverage is closed or already complete')
        if self.cancel is not None and self.cancel():
            raise CancelledError()

    def add(self, sample_index, original_pair_index):
        self._check()
        if (type(sample_index) is not int or not 0 <= sample_index < len(self.counts)
                or type(original_pair_index) is not int
                or not 0 <= original_pair_index < self.counts[sample_index]):
            raise IntegrityError('observed identity is outside its expected sample range')
        if self.observed >= self.total:
            raise IntegrityError('observed identities exceed the expected total')
        if self._bitmap is not None:
            position = self.offsets[sample_index] + original_pair_index
            byte, bit = divmod(position, 8)
            mask = 1 << bit
            if self._bitmap[byte] & mask:
                raise IntegrityError('duplicate canonical identity')
            self._bitmap[byte] |= mask
        else:
            self._sorter.append(_IDENTITY.pack(sample_index, original_pair_index))
        self.observed = checked_add(self.observed, 1)

    def finish(self):
        self._check()
        if self._sorter is not None:
            sample = ordinal = observed = 0
            with self._sorter.iter_sorted() as records:
                for record in records:
                    self._check()
                    while sample < len(self.counts) and ordinal == self.counts[sample]:
                        sample += 1
                        ordinal = 0
                    if sample >= len(self.counts) or record != _IDENTITY.pack(sample, ordinal):
                        raise IntegrityError('canonical identity coverage has a duplicate, gap or invalid range')
                    observed += 1
                    ordinal += 1
                if observed != self.total:
                    raise IntegrityError('canonical identity coverage has missing identities')
            self._sorter.close()
            self._sorter = None
        if self.observed != self.total:
            raise IntegrityError('canonical identity coverage has missing identities')
        self._finished = True

    def close(self):
        if self._closed:
            return
        if self._sorter is not None:
            self._sorter.close()
            self._sorter = None
        self._bitmap = None
        self.counts = ()
        self.offsets = []
        if self._bitmap_lease is not None:
            self._bitmap_lease.release()
        self._metadata.release()
        self._closed = True

    def __exit__(self, exc_type, exc, traceback):
        try:
            self.close()
        except BaseException:
            if exc is None:
                raise
            exc.add_note('coverage cleanup failed; primary error preserved and protected scratch remains owned')
