"""Whole-set 64-bucket scatter with a shared flush workspace and paged ownership.

Only bounded writer arrays and one ownership-page buffer reside in Python.
Batch generations and producer ordinals affect scratch identity only. Canonical
output is descending root bucket then ascending original identity, independent
of input order, producer identity, and complete-set rotation boundaries.
"""
from contextlib import AbstractContextManager, contextmanager
import hashlib
import struct

from .crypto import pairs_identity
from .errors import CancelledError, ConfigurationError, IntegrityError, ResourceError
from .ledger import PagedLedger
from .paircodec import decode_pair_view, maximum_frame_bytes
from .resources import checked_add
from .sorting import ExternalSorter
from .stage_policy import SortPolicy, StagePolicy
from .storage import (BLOCK_BYTES, SharedFlushWorkspace, deserialize_segment_ref,
                      serialize_segment_ref)

_IDENTITY = struct.Struct('<IQ')
_BUCKET_IDENTITY = struct.Struct('>IQ')
_LENGTH = struct.Struct('<I')
_INVENTORY = struct.Struct('>BIQQI')  # bucket, producer, batch, position, maximum frame
_WRITERS = 64
_MAX_BATCH = 64 << 20
_CONTROL = 65536
_LEDGER_META = (64 << 10) * 4 + 128 * 1024 + 8192 + 256 * 1024
_CLEANUP_META = 8192 + (64 << 10)


def _bucket(key, sample, ordinal):
    return int.from_bytes(hashlib.blake2b(_BUCKET_IDENTITY.pack(sample, ordinal),
                                        key=key, digest_size=8).digest(), 'little') & 63


def _identity(frame):
    return _IDENTITY.unpack_from(frame, 4)


class DirectScatter(AbstractContextManager):
    """Private synchronous producer primitive, compatible with sorter iteration.

    All 64 writers, potential output handles, frame carry, batch retention and
    one flush workspace are admitted before the caller may submit input. Auto
    always spills completed batches, bounding unsealed resident input even for
    one huge source. Memory mode keeps sealed segments charged until consumed;
    exhaustion refuses further input without creating scratch.
    """

    @staticmethod
    def _configuration(max_record_bytes, batch_bytes, producer, sort_run_bytes):
        maximum = maximum_frame_bytes(max_record_bytes)
        if type(batch_bytes) is not int or not maximum <= batch_bytes <= _MAX_BATCH:
            raise ConfigurationError('direct scatter batch must fit one maximum frame and be at most 64 MiB')
        if type(producer) is not int or not 0 <= producer < 1 << 32:
            raise ConfigurationError('direct scatter producer must fit unsigned 32 bits')
        if type(sort_run_bytes) is not int or sort_run_bytes < maximum + 512:
            raise ResourceError('direct scatter sort run cannot admit a maximum frame and key')
        return maximum

    @classmethod
    def can_admit(cls, store, *, max_record_bytes=1 << 20, batch_bytes=_MAX_BATCH,
                  producer=0, sort_run_bytes=8 << 20):
        """Read-only resource probe; construction remains final admission authority.

        No IDs, entropy, disk space or source descriptors are consumed. This is
        a conservative sufficient condition, including page creation overlap.
        A caller must choose its strategy before decoding any source.
        """
        maximum = cls._configuration(max_record_bytes, batch_bytes, producer, sort_run_bytes)
        if store.max_segments - store.resident_segments < _WRITERS + 2:
            return False
        block_count = _WRITERS + (batch_bytes + BLOCK_BYTES - 1) // BLOCK_BYTES
        if store.budget.max_leases - store.budget.active_leases < 3 * _WRITERS + 16 + (block_count if store.mode != 'external' else 0):
            return False
        block_meta = 256 * block_count
        retained = batch_bytes + block_meta if store.mode != 'external' else 0
        retained_meta = block_meta if store.mode != 'external' else 0
        total = (_CONTROL + 2 * maximum + 4096 + (8 << 20) + _LEDGER_META + (8 << 20)
                 + _WRITERS * (BLOCK_BYTES + 256 + 8192 + 8192) + retained
                 + (10 << 20))
        metadata = (_CONTROL + 8192 + _LEDGER_META + _CLEANUP_META
                    + _WRITERS * (256 + 8192 + 8192) + retained_meta + (128 << 10))
        descriptors = 2 + (_WRITERS + 7 if store.mode != 'memory' else 0)
        try:
            lease = store.budget.acquire('direct-scatter-probe', total, descriptors,
                                         metadata_bytes=metadata)
        except ResourceError:
            return False
        lease.release()
        return True

    def __init__(self, store, *, max_record_bytes=1 << 20, batch_bytes=_MAX_BATCH,
                 producer=0, sort_run_bytes=8 << 20, cancel=None, inventory_only=False, producer_count=1):
        self.max_frame_bytes = self._configuration(max_record_bytes, batch_bytes, producer, sort_run_bytes)
        if cancel is not None and not callable(cancel):
            raise ConfigurationError('direct scatter cancellation must be callable')
        if type(inventory_only) is not bool or type(producer_count) is not int or not 1 <= producer_count <= 64:
            raise ConfigurationError('direct scatter producer inventory is outside its bound')
        self.inventory_only, self.producer_count = inventory_only, producer_count
        self._producer_batches = [-1] * producer_count
        self._producer_masks = [0] * producer_count
        self.store, self.max_record_bytes, self.batch_bytes = store, max_record_bytes, batch_bytes
        self.producer, self.sort_run_bytes, self.cancel = producer, sort_run_bytes, cancel
        self._key = store.keys.bucket_key(())
        self._control = self._workspace = self._retention = self._inventory = None
        self._writers = [None] * _WRITERS
        self._counts = [0] * _WRITERS
        self._bytes = [0] * _WRITERS
        self._maxima = [0] * _WRITERS
        self._pending = self._sorter = None
        self._batch_used = self._batch = self._published = 0
        self.records = self.frame_bytes = self.batches = 0
        self._started = self._active = self._failed = self._closed = self._sealed = False
        self._retention_meta = (256 * (_WRITERS + (batch_bytes + BLOCK_BYTES - 1) // BLOCK_BYTES)
                                if store.mode != 'external' else 256)
        self._retention_bytes = batch_bytes + self._retention_meta if store.mode != 'external' else 256
        try:
            # Keep two producer input slots and complete frame/copy carry visible
            # even though this primitive receives already framed records.
            self._control = store.budget.acquire('direct-scatter-control',
                                                 _CONTROL + 2 * self.max_frame_bytes + 4096,
                                                 descriptors=2, metadata_bytes=_CONTROL)
            self._inventory = PagedLedger(store, max_record_bytes=8192, cancel=cancel, owns_records=True)
            if not inventory_only:
                self._workspace = SharedFlushWorkspace(store.budget)
                self._retention = store.budget.acquire('direct-scatter-batch', self._retention_bytes,
                                                       metadata_bytes=self._retention_meta)
                self._open_set()
        except BaseException as primary:
            try:
                self.close()
            except BaseException:
                primary.add_note('direct scatter construction cleanup also failed')
            raise

    def _check(self):
        if self._closed or self._failed:
            raise ResourceError('direct scatter is closed or has already failed')
        if self.cancel is not None and self.cancel():
            raise CancelledError()

    def _open_set(self):
        if self.store.max_segments - self.store.resident_segments < _WRITERS + 2:
            raise ResourceError('cannot admit the complete direct scatter writer registry')
        self._retention.resize(self._retention_bytes, metadata_bytes=self._retention_meta)
        for bucket in range(_WRITERS):
            buffer = output = None
            try:
                buffer = self.store.budget.acquire('direct-scatter-buffer', BLOCK_BYTES + 256,
                                                   metadata_bytes=256)
                output = self.store.budget.acquire('direct-scatter-output', 8192,
                                                   descriptors=int(self.store.mode != 'memory'),
                                                   metadata_bytes=8192)
                self._writers[bucket] = self.store.new_segment(
                    1, pairs_identity((bucket,), self.producer, self._batch), buffer,
                    flush_workspace=self._workspace, output_lease=output,
                    block_reservation=self._retention if self.store.mode != 'external' else None)
            finally:
                # Transferred handles are already retired; failed allocations
                # leave the untransferred reservation with this local owner.
                if buffer is not None:
                    buffer.release()
                if output is not None:
                    output.release()

    def append(self, frame):
        self._check()
        if self._started or self._sealed or self.inventory_only:
            raise ResourceError('cannot append frames to a sealed or inventory-only scatter')
        decoded = decode_pair_view(frame, max_record_bytes=self.max_record_bytes)
        size = decoded.frame_bytes
        bucket = _bucket(self._key, decoded.sample_index, decoded.original_pair_index)
        count = checked_add(self.records, 1)
        total = checked_add(self.frame_bytes, size)
        try:
            if self._batch_used and self._batch_used + size > self.batch_bytes:
                self._seal_set()
                self._open_set()
            writer = self._writers[bucket]
            if writer is None:
                raise IntegrityError('direct scatter writer set is incomplete')
            writer.append(decoded.frame, item_count=1)
            self._counts[bucket] = checked_add(self._counts[bucket], 1)
            self._bytes[bucket] = checked_add(self._bytes[bucket], size)
            self._maxima[bucket] = max(self._maxima[bucket], size)
            self._batch_used += size
            self.records, self.frame_bytes = count, total
        except BaseException:
            self._failed = True
            raise

    append_frame = append

    def import_batch_reference(self, bucket, producer, batch, max_frame, ref):
        """Register a verified worker result before its sender relinquishes it.

        The caller retains the handoff owner across this call and invokes its
        coordinator storage ``take_write`` only after registration. If append
        commits then raises, both cleanup paths are idempotent; the invocation
        must abort, never resume admission from an uncertain handoff.
        """
        self._check()
        if not self.inventory_only or self._started or self._sealed:
            raise ResourceError('scatter does not accept worker batch references')
        if (type(bucket) is not int or not 0 <= bucket < 64
                or type(producer) is not int or not 0 <= producer < self.producer_count
                or type(batch) is not int or not 0 <= batch < 1 << 64
                or batch < self._producer_batches[producer]
                or type(max_frame) is not int or not 48 <= max_frame <= self.max_frame_bytes
                or ref.purpose != 1 or ref.logical_identity != pairs_identity((bucket,), producer, batch)
                or not ref.item_count or not max_frame <= ref.decoded_bytes <= self.batch_bytes):
            raise IntegrityError('worker scatter reference is outside its assigned generation')
        mask = self._producer_masks[producer] if batch == self._producer_batches[producer] else 0
        if mask & (1 << bucket):
            raise IntegrityError('worker scatter generation repeated a bucket')
        records = checked_add(self.records, ref.item_count)
        frame_bytes = checked_add(self.frame_bytes, ref.decoded_bytes)
        ordinal = self._published
        encoded = _INVENTORY.pack(bucket, producer, batch, ordinal, max_frame) + serialize_segment_ref(ref)
        before = self._inventory.owned_count
        try:
            self._inventory.append(encoded)
        except BaseException:
            self._failed = True
            raise
        finally:
            if self._inventory.owned_count == before + 1:
                self._published += 1
                self._producer_batches[producer] = batch
                self._producer_masks[producer] = mask | (1 << bucket)
                self.records, self.frame_bytes = records, frame_bytes
        if self.store.mode != 'memory':
            self.store.detach(ref)
        return ref

    def _seal_set(self):
        for bucket, writer in enumerate(self._writers):
            self._check()
            if writer is None:
                continue
            if not self._counts[bucket]:
                writer.abort()
                self._writers[bucket] = None
                continue
            ref = writer.seal()
            self._pending = ref
            self._writers[bucket] = None
            if (ref.item_count, ref.decoded_bytes) != (self._counts[bucket], self._bytes[bucket]):
                raise IntegrityError('direct scatter seal disagrees with complete pair totals')
            if self.store.mode != 'memory':
                ref = self.store.spill(ref)
                self._pending = ref
            record = (_INVENTORY.pack(bucket, self.producer, self._batch, self._published,
                                      self._maxima[bucket]) + serialize_segment_ref(ref))
            before = self._inventory.owned_count
            try:
                self._inventory.append(record)
            finally:
                if self._inventory.owned_count == before + 1:
                    self._pending = None
                    self._published += 1
            if self.store.mode != 'memory':
                self.store.detach(ref)
        self._inventory.flush()
        if self._batch_used:
            self.batches = checked_add(self.batches, 1)
            self._batch = checked_add(self._batch, 1)
        self._batch_used = 0
        self._counts[:] = [0] * _WRITERS
        self._bytes[:] = [0] * _WRITERS
        self._maxima[:] = [0] * _WRITERS

    def _decode_inventory(self, record, position=None):
        if len(record) < _INVENTORY.size:
            raise IntegrityError('direct scatter inventory record is truncated')
        bucket, producer, batch, ordinal, maximum = _INVENTORY.unpack_from(record)
        ref = deserialize_segment_ref(record[_INVENTORY.size:])
        invalid_producer = (producer >= self.producer_count or batch > self._producer_batches[producer]
                            if self.inventory_only else producer != self.producer or batch >= self._batch)
        if (bucket >= _WRITERS or invalid_producer
                or (position is not None and ordinal != position)
                or ordinal >= self._published or not 48 <= maximum <= self.max_frame_bytes
                or ref.purpose != 1 or ref.logical_identity != pairs_identity((bucket,), producer, batch)
                or not ref.item_count or ref.decoded_bytes < maximum
                or ref.decoded_bytes > self.batch_bytes):
            raise IntegrityError('direct scatter inventory disagrees with its protected segment')
        return bucket, maximum, ref

    def _frames(self, ref, bucket, maximum):
        observed = largest = 0
        with self.store.open_verified(ref) as stream:
            for _ in range(ref.item_count):
                self._check()
                prefix = stream.read(4)
                if len(prefix) != 4:
                    raise IntegrityError('direct scatter frame length is truncated')
                length, = _LENGTH.unpack(prefix)
                if not 48 <= length <= maximum:
                    raise IntegrityError('direct scatter frame length exceeds its admitted bound')
                arena = bytearray(length)
                arena[:4] = prefix
                view = memoryview(arena)
                offset = 4
                while offset < length:
                    self._check()
                    size = min(BLOCK_BYTES, length - offset)
                    read = stream.readinto(view[offset:offset + size])
                    if not read:
                        raise IntegrityError('direct scatter frame is truncated')
                    offset += read
                decoded = decode_pair_view(arena, max_record_bytes=self.max_record_bytes)
                if _bucket(self._key, decoded.sample_index, decoded.original_pair_index) != bucket:
                    raise IntegrityError('direct scatter frame is routed to the wrong bucket')
                observed = checked_add(observed, length)
                largest = max(largest, length)
                yield bytes(arena)
            if stream.read(1) or observed != ref.decoded_bytes or largest != maximum:
                raise IntegrityError('direct scatter segment totals disagree with its inventory')

    def _results(self):
        observed = encoded_bytes = 0
        # One bounded root is rescanned for each fixed bucket. Only reference
        # metadata is reread; every payload frame is decoded exactly once here.
        # No resident list grows with the number of producer batches.
        for bucket in range(_WRITERS - 1, -1, -1):
            self._check()
            sorter = ExternalSorter(self.store,
                                    policy=StagePolicy(SortPolicy.ORIGINAL_FRAMES, self.max_record_bytes),
                                    run_bytes=self.sort_run_bytes,
                                    max_record_bytes=self.max_frame_bytes,
                                    sort_purpose='direct-scatter-identity', cancel=self.cancel)
            self._sorter = sorter
            try:
                with self._inventory.open_reverse() as cursor:
                    for encoded in cursor:
                        found, maximum, ref = self._decode_inventory(encoded)
                        if found != bucket:
                            continue
                        for frame in self._frames(ref, bucket, maximum):
                            sorter.append(frame)
                        self.store.drop(ref, missing_ok=True)
                previous = None
                with sorter.iter_sorted() as frames:
                    for frame in frames:
                        identity = _identity(frame)
                        if previous is not None and identity <= previous:
                            raise IntegrityError('direct scatter identities are repeated or not increasing')
                        previous = identity
                        observed = checked_add(observed, 1)
                        encoded_bytes = checked_add(encoded_bytes, len(frame))
                        yield frame
            finally:
                sorter.close()
                self._sorter = None
        if (observed, encoded_bytes) != (self.records, self.frame_bytes):
            raise IntegrityError('direct scatter output differs from admitted source totals')

    def finish_ingress(self):
        """Seal the final complete writer set before admitting bundle emission.

        Source ownership stays in its paged inventory; parser working buffers
        and unused batch capacity retire before the emitter overlaps this stage.
        Calling this more than once is safe and never decodes original input.
        """
        self._check()
        if self._sealed:
            return
        try:
            if not self.inventory_only:
                self._seal_set()
                self._retention.release()
                self._workspace.close()
            self._inventory.flush()
            self._sealed = True
        except BaseException:
            self._failed = True
            raise

    @contextmanager
    def iter_sorted(self):
        self._check()
        if self._started:
            raise ResourceError('direct scatter sorted iteration is single-use')
        self._started = self._active = True
        iterator = None
        try:
            self.finish_ingress()
            iterator = self._results()
            yield iterator
            for _ in iterator:
                pass
        except BaseException:
            self._failed = True
            raise
        finally:
            try:
                if iterator is not None:
                    iterator.close()
            finally:
                self._active = False

    def close(self):
        if self._closed:
            return
        if self._active:
            raise ResourceError('cannot close direct scatter with an active result iterator')
        first = None
        for bucket, writer in enumerate(self._writers):
            if writer is not None:
                try:
                    if writer._closed and writer.entry.ref is not None:
                        self.store.drop(writer.entry.ref, missing_ok=True)
                    else:
                        writer.abort()
                    self._writers[bucket] = None
                except BaseException as error:
                    if first is None:
                        first = error
        if first is not None:
            raise first
        if self._sorter is not None:
            self._sorter.close()
            self._sorter = None
        if self._pending is not None:
            self.store.drop(self._pending, missing_ok=True)
            self._pending = None
        if self._inventory is not None:
            # The final batch may be only partly registered when sealing fails.
            # Its ordinal is still a valid trusted cleanup identity.
            self._batch = max(self._batch, self.batches + 1)
            self._inventory.drain_owned(
                lambda record, progress: self.store.drop(self._decode_inventory(record)[2], missing_ok=True),
                validate=lambda record, position: self._decode_inventory(record, position))
            self._inventory.close()
        if self._workspace is not None:
            self._workspace.close()
        for lease in (self._retention, self._control):
            if lease is not None:
                lease.release()
        self._key = None
        self._closed = True

    def __exit__(self, exc_type, exc, traceback):
        try:
            self.close()
        except BaseException:
            if exc is None:
                raise
            exc.add_note('direct scatter cleanup also failed; protected ownership is retained')
