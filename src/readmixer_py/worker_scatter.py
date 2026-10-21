"""Worker-owned complete scatter generations under coordinator capabilities.

No root key, identity allocator, disk budget, unlink authority or publication
handle exists here. Exactly 64 logical writers share one transient flush
workspace and one anchored scratch directory. The coordinator must grant
normalized-byte credit before appending a previously admitted parser batch.
"""
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
import hashlib
import os
import struct

from .crypto import pairs_identity
from .errors import ConfigurationError, IntegrityError, ResourceError
from .paircodec import decode_pair_view, maximum_frame_bytes
from .resources import Budget, checked_add
from .storage import BLOCK_BYTES, SharedFlushWorkspace
from .worker_storage import SegmentWriteGrant, _CapabilityStore, _GrantedSink, _validate_grant

_IDENTITY = struct.Struct('>IQ')
_METADATA = 2 << 20


class _GenerationKeys:
    def __init__(self, grants):
        self.grants = grants

    def segment_key(self, segment_id, salt, purpose, logical):
        for grant in self.grants:
            ref = grant.ref
            if (segment_id, salt, purpose, logical) == (ref.segment_id, ref.generation_salt,
                                                       ref.purpose, ref.logical_identity):
                return grant.key
        raise IntegrityError('worker scatter attempted a key outside its generation')


@dataclass(slots=True, repr=False)
class _Bucket:
    buffer: bytearray = field(default_factory=bytearray)
    blocks: list = field(default_factory=list)
    length: int = 0
    count: int = 0
    decoded: int = 0
    maximum: int = 0
    digest: object = field(default_factory=hashlib.sha256)
    sink: object = None
    result: object = None
    delivered: bool = False


class WorkerScatterGeneration(AbstractContextManager):
    """One pre-admitted worker's whole 64-writer generation.

    ``finish(callback)`` invokes callback for every bucket in numeric order.
    Nonempty callbacks receive ``(bucket, maximum_frame, ref_or_bytes, count)``;
    empty callbacks receive ``(bucket, 0, None, 0)``. Callback returns only after
    coordinator ownership acknowledgment. Sender buffers remain charged until
    then. On any failure the caller aborts the invocation; ``close`` closes
    descriptors but never removes coordinator-owned files or refunds disk.
    """

    @staticmethod
    def required_bytes(batch_bytes, max_record_bytes, external):
        maximum = maximum_frame_bytes(max_record_bytes)
        # Buffer arrays, scalar/grant metadata, single flush workspace, two
        # memory-retention copies, and frame/transport overlap. The original
        # parser batch and interpreter baseline are admitted separately.
        return (64 * (BLOCK_BYTES + 256) + _METADATA + (8 << 20)
                + (0 if external else 2 * batch_bytes + 256 * (64 + (batch_bytes + BLOCK_BYTES - 1) // BLOCK_BYTES))
                + 2 * maximum + (1 << 20))

    def __init__(self, grants=None, *, producer, batch, bucket_key, max_record_bytes,
                 batch_bytes, credit=None, commit=None, checkpoint=None):
        maximum = maximum_frame_bytes(max_record_bytes)
        if (type(producer) is not int or not 0 <= producer < 1 << 32
                or type(batch) is not int or not 0 <= batch < 1 << 64
                or type(bucket_key) is not bytes or len(bucket_key) != 32
                or type(batch_bytes) is not int or not maximum <= batch_bytes <= 64 << 20
                or checkpoint is not None and not callable(checkpoint)):
            raise ConfigurationError('worker scatter generation exceeds its admitted bounds')
        external = grants is not None
        if external and (type(grants) is not tuple or len(grants) != 64
                         or not callable(credit) or not callable(commit)):
            raise ConfigurationError('worker scatter requires exactly64 coordinator write capabilities')
        self.producer, self.batch, self.key = producer, batch, bucket_key
        self.max_record_bytes, self.batch_bytes = max_record_bytes, batch_bytes
        self.grants, self.checkpoint, self.credit, self.commit = grants, checkpoint, credit, commit
        self._budget = Budget(self.required_bytes(batch_bytes, max_record_bytes, external), descriptors=80)
        self._lease = self._retention = self._workspace = self._adapter = None
        self._buckets = []
        self._closed = self._started = self._failed = False
        self.pairs = self.encoded_bytes = self.maximum = 0
        try:
            self._lease = self._budget.acquire('worker-scatter-writers',
                                               64 * (BLOCK_BYTES + 256) + _METADATA,
                                               descriptors=68 if external else 0,
                                               metadata_bytes=_METADATA + 64 * 256)
            self._workspace = SharedFlushWorkspace(self._budget)
            if not external:
                self._retention = self._budget.acquire(
                    'worker-scatter-retention', 2 * batch_bytes + 256 * (64 + (batch_bytes + BLOCK_BYTES - 1) // BLOCK_BYTES),
                    metadata_bytes=256 * (64 + (batch_bytes + BLOCK_BYTES - 1) // BLOCK_BYTES))
            if external:
                first = grants[0]
                seen = set()
                for bucket, grant in enumerate(grants):
                    if type(grant) is not SegmentWriteGrant:
                        raise IntegrityError('worker scatter received a non-write capability')
                    _validate_grant(grant)
                    if (grant.ref.purpose != 1 or grant.ref.logical_identity != pairs_identity((bucket,), producer, batch)
                            or (grant.directory, grant.directory_device, grant.directory_inode, grant.ref.run_id, grant.task_id)
                            != (first.directory, first.directory_device, first.directory_inode, first.ref.run_id, first.task_id)
                            or grant.max_decoded != batch_bytes or grant.max_items < batch_bytes // 48
                            or grant.ref.segment_id in seen):
                        raise IntegrityError('worker scatter capability is outside its assigned generation')
                    seen.add(grant.ref.segment_id)
                if len({grant.grant_id for grant in grants}) != 64:
                    raise IntegrityError('worker scatter grant identity was repeated')
                self._adapter = _CapabilityStore(first, checkpoint)
                self._adapter.keys = _GenerationKeys(grants)
            self._buckets = [_Bucket() for _ in range(64)]
        except BaseException as primary:
            try:
                self.close()
            except BaseException:
                primary.add_note('worker scatter constructor cleanup also failed')
            raise

    def _check(self):
        if self._closed or self._failed:
            raise ResourceError('worker scatter generation is closed or failed')
        if self.checkpoint is not None:
            self.checkpoint()

    def append(self, frame):
        self._check()
        if self._started:
            raise ResourceError('worker scatter generation has already sealed')
        pair = decode_pair_view(frame, max_record_bytes=self.max_record_bytes)
        if pair.frame_bytes > self.batch_bytes - self.encoded_bytes:
            raise ResourceError('worker scatter generation exceeds its whole-pair batch grant')
        digest = hashlib.blake2b(_IDENTITY.pack(pair.sample_index, pair.original_pair_index),
                                 key=self.key, digest_size=8).digest()
        bucket = int.from_bytes(digest, 'little') & 63
        writer = self._buckets[bucket]
        try:
            if not writer.buffer:
                writer.buffer = bytearray(BLOCK_BYTES)
            view = pair.frame
            while view:
                self._check()
                size = min(BLOCK_BYTES - writer.length, len(view))
                part = view[:size]
                writer.buffer[writer.length:writer.length + size] = part
                writer.digest.update(part)
                writer.length += size
                view = view[size:]
                if writer.length == BLOCK_BYTES:
                    self._flush(bucket)
            writer.count = checked_add(writer.count, 1)
            writer.decoded = checked_add(writer.decoded, pair.frame_bytes)
            writer.maximum = max(writer.maximum, pair.frame_bytes)
            self.pairs = checked_add(self.pairs, 1)
            self.encoded_bytes = checked_add(self.encoded_bytes, pair.frame_bytes)
            self.maximum = max(self.maximum, pair.frame_bytes)
        except BaseException:
            self._failed = True
            raise

    def _flush(self, bucket):
        writer = self._buckets[bucket]
        if not writer.length:
            return
        with self._workspace.borrow():
            data = bytes(memoryview(writer.buffer)[:writer.length])
            if self.grants is None:
                owner = self._retention.split('worker-scatter-block', len(data) + 256, metadata_bytes=256)
                try:
                    writer.blocks.append((data, owner))
                except BaseException:
                    owner.release()
                    raise
            else:
                if writer.sink is None:
                    writer.sink = _GrantedSink(self._adapter, self.grants[bucket],
                        lambda offset, amount: self.credit(bucket, offset, amount),
                        lambda offset, actual: self.commit(bucket, offset, actual))
                    writer.sink.begin()
                writer.sink.write_data(data)
            writer.length = 0

    def finish(self, callback):
        self._check()
        if self._started or not callable(callback):
            raise ResourceError('worker scatter completion is single-use and requires an ownership callback')
        self._started = True
        try:
            for bucket, writer in enumerate(self._buckets):
                self._check()
                if writer.count:
                    self._flush(bucket)
                    if self.grants is None:
                        writer.result = b''.join(data for data, _owner in writer.blocks)
                    else:
                        with self._workspace.borrow():
                            writer.result = writer.sink.finish(writer.count, writer.decoded, writer.digest.digest())
                    callback(bucket, writer.maximum, writer.result, writer.count)
                else:
                    callback(bucket, 0, None, 0)
                # Acknowledgment returned: receiver now owns the data. Retire
                # the sender's bytes and leases before proceeding to next bucket.
                writer.result = None
                writer.buffer.clear()
                while writer.blocks:
                    data, owner = writer.blocks.pop()
                    del data
                    owner.release()
                writer.delivered = True
            return self.pairs, self.encoded_bytes, self.maximum
        except BaseException:
            self._failed = True
            raise

    def close(self):
        if self._closed:
            return
        first = None
        for writer in self._buckets:
            if writer.sink is not None:
                writer.sink._key = None
                if writer.sink.fd is not None:
                    try:
                        os.close(writer.sink.fd)
                        writer.sink.fd = None
                    except BaseException as error:
                        if first is None:
                            first = error
            writer.result = None
            writer.buffer.clear()
            while writer.blocks:
                data, owner = writer.blocks.pop()
                del data
                owner.release()
        if first is not None:
            raise first
        if self._adapter is not None:
            self._adapter.close()
            self._adapter = None
        if self._workspace is not None:
            self._workspace.close()
        for lease in (self._retention, self._lease):
            if lease is not None:
                lease.release()
        self.grants = self.key = self.credit = self.commit = None
        self._buckets.clear()
        self._closed = True

    def __exit__(self, exc_type, exc, traceback):
        try:
            self.close()
        except BaseException:
            if exc is None:
                raise
            exc.add_note('worker scatter descriptor cleanup also failed')
