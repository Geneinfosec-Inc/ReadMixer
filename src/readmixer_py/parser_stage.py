"""One-pass source jobs, closed source capabilities, and exact ingress credits.

Workers receive two admitted snapshots, never a catalog or root cryptographic
key. A whole pair is retained until its bounded batch has been acknowledged.
"""
from dataclasses import dataclass, field
import hashlib
import os
from pathlib import Path
import struct

from .catalog import FileSnapshot, InputFile, SampleCatalog, _flags
from .errors import ConfigurationError, IntegrityError, ResourceError
from .fastq import SampleReadReport, PairedReader, RecordFramer, open_decoded_stream
from .paircodec import maximum_frame_bytes, encode_pair_into, decode_pair_view
from .resources import checked_add

MAX_SOURCE_BYTES = 16 << 10
_HEAD = struct.Struct('>BIIQBI32s')
_SNAPSHOT = struct.Struct('>QQQqq')
_REPORT = struct.Struct('>I10Q32s32sB')


@dataclass(frozen=True, slots=True)
class SourceCapability:
    sample: int
    max_record_bytes: int
    batch_bytes: int
    root: Path = field(repr=False)
    root_snapshot: FileSnapshot = field(repr=False)
    r1: InputFile = field(repr=False)
    r2: InputFile = field(repr=False)
    scatter_mode: str = 'stream'
    producer: int = 0
    bucket_key: bytes = field(default=bytes(32), repr=False)


def encode_source(catalog, sample, max_record_bytes, batch_bytes, *, scatter_mode='stream', producer=0, bucket_key=bytes(32)):
    maximum = maximum_frame_bytes(max_record_bytes, single_frame_limit=64 << 20)
    if not maximum <= batch_bytes <= 64 << 20:
        raise ConfigurationError('parser batch cannot hold its maximum complete pair')
    if scatter_mode not in ('stream', 'memory', 'external') or type(producer) is not int or not 0 <= producer < 64 or type(bucket_key) is not bytes or len(bucket_key) != 32:
        raise ConfigurationError('source scatter capability is invalid')
    result = bytearray(_HEAD.pack(1, sample.index, max_record_bytes, batch_bytes,
        ('stream', 'memory', 'external').index(scatter_mode), producer, bucket_key))
    for path, snapshot, codec in ((catalog.root, catalog.root_snapshot, 0),
                                 (sample.r1.relative_path, sample.r1.snapshot, sample.r1.codec == 'gzip'),
                                 (sample.r2.relative_path, sample.r2.snapshot, sample.r2.codec == 'gzip')):
        encoded = os.fsencode(path)
        if not 1 <= len(encoded) <= 4096 or b'\0' in encoded:
            raise ResourceError('source path exceeds its worker capability bound')
        result.extend(struct.pack('>HB', len(encoded), codec))
        result.extend(encoded)
        result.extend(_SNAPSHOT.pack(snapshot.device, snapshot.inode, snapshot.size, snapshot.mtime_ns, snapshot.ctime_ns))
    if len(result) > MAX_SOURCE_BYTES:
        raise ResourceError('source capability exceeds its bounded control envelope')
    return bytes(result)


def decode_source(encoded):
    if type(encoded) is not bytes or not _HEAD.size <= len(encoded) <= MAX_SOURCE_BYTES:
        raise IntegrityError('source capability has an invalid width')
    try:
        version, sample, maximum, batch, mode, producer, key = _HEAD.unpack_from(encoded)
        if (version != 1 or not maximum_frame_bytes(maximum) <= batch <= 64 << 20 or mode > 2 or producer >= 64
                or mode == 0 and (producer or key != bytes(32))):
            raise ValueError
        offset = _HEAD.size
        rows = []
        for index in range(3):
            size, codec = struct.unpack_from('>HB', encoded, offset)
            offset += 3
            if not 1 <= size <= 4096 or codec not in (0, 1) or index == 0 and codec:
                raise ValueError
            path = encoded[offset:offset + size]
            if len(path) != size or b'\0' in path:
                raise ValueError
            path = Path(os.fsdecode(path))
            if (index == 0 and not path.is_absolute() or index and
                    (path.is_absolute() or '..' in path.parts or not path.parts)):
                raise ValueError
            offset += size
            snapshot = FileSnapshot(*_SNAPSHOT.unpack_from(encoded, offset))
            offset += _SNAPSHOT.size
            rows.append((path, snapshot, 'gzip' if codec else 'plain'))
        if offset != len(encoded):
            raise ValueError
        root, root_snapshot, _ = rows[0]
        inputs = tuple(InputFile(root / path, path, snap, codec) for path, snap, codec in rows[1:])
        return SourceCapability(sample, maximum, batch, root, root_snapshot, *inputs, ('stream', 'memory', 'external')[mode], producer, key)
    except (ValueError, struct.error, ConfigurationError):
        raise IntegrityError('source capability contains invalid fields') from None


def encode_report(report):
    return _REPORT.pack(report.sample_index, report.pair_count, report.r1_records, report.r2_records,
        report.r1_normalized_bytes, report.r2_normalized_bytes, report.r1_decoded_bytes,
        report.r2_decoded_bytes, report.r1_physical_bytes, report.r2_physical_bytes,
        report.max_frame_bytes, bytes.fromhex(report.r1_sha256), bytes.fromhex(report.r2_sha256), report.complete)


def decode_report(encoded):
    if type(encoded) is not bytes or len(encoded) != _REPORT.size:
        raise IntegrityError('parser report has an invalid width')
    values = _REPORT.unpack(encoded)
    if values[-1] != 1:
        raise IntegrityError('parser did not finish both admitted sources')
    return SampleReadReport(*values[:-3], values[-3].hex(), values[-2].hex(), True)


def lpt_assignments(catalog, producers):
    """Deterministic assignment from admitted compressed sizes, then source order."""
    if type(producers) is not int or not 1 <= producers <= 64:
        raise ConfigurationError('parser producer count is outside its bound')
    loads, groups = [0] * producers, [[] for _ in range(producers)]
    for sample in sorted(catalog, key=lambda s: (-(s.r1.snapshot.size + s.r2.snapshot.size), s.index)):
        producer = min(range(producers), key=lambda i: (loads[i], i))
        groups[producer].append(sample.index)
        loads[producer] = checked_add(loads[producer], sample.r1.snapshot.size + sample.r2.snapshot.size)
    return tuple(tuple(sorted(group)) for group in groups)


class IngressCredits:
    """Exact grants consumed at batch admission; there are no idle reservations."""
    def __init__(self, maximum=None):
        self.maximum, self.used = maximum, 0

    def admit(self, amount):
        value = checked_add(self.used, amount)
        if self.maximum is not None and value > self.maximum:
            raise ResourceError('normalized input exceeds max_total_uncompressed')
        self.used = value


class SourceReceiver:
    def __init__(self, capability, append, credits):
        self.capability, self.append, self.credits = capability, append, credits
        self.batches = self.pairs = self.maximum = 0
        self.normalized = [0, 0]
        self.hashes = [hashlib.sha256(), hashlib.sha256()]
        self.header = None
        self.scatter = None
        self._seen = None
        self._accepted = 0

    def begin(self, header):
        if (self.header is not None or header.batch != self.batches or not 48 <= header.output_bytes <= self.capability.batch_bytes
                or not 1 <= header.records <= header.output_bytes // 48
                or header.normalized_bytes != header.output_bytes - 40 * header.records):
            raise IntegrityError('parser batch is outside its exact grant')
        if self.capability.scatter_mode != 'stream' and header.digest != bytes(32):
            raise IntegrityError('scatter admission contains an unrelated raw-stream digest')
        self.credits.admit(header.normalized_bytes)
        self.header = header
        if self.capability.scatter_mode != 'stream':
            self._seen = bytearray((header.records + 7) // 8)
            self._accepted = 0

    def accept_scatter_frame(self, frame, bucket):
        from .shuffle import bucket_index
        pair = decode_pair_view(frame, max_record_bytes=self.capability.max_record_bytes)
        index = pair.original_pair_index - self.pairs
        if (self.header is None or pair.sample_index != self.capability.sample or not 0 <= index < self.header.records or
                bucket_index(self.capability.bucket_key, pair.sample_index, pair.original_pair_index, 64) != bucket):
            raise IntegrityError('scatter frame violates its source or bucket identity')
        byte, bit = divmod(index, 8)
        if self._seen[byte] & (1 << bit):
            raise IntegrityError('scatter returned a duplicate pair identity')
        self._seen[byte] |= 1 << bit
        self._accepted += 1
        for number, record in enumerate((pair.r1, pair.r2)):
            self.normalized[number] = checked_add(self.normalized[number], record.normalized_bytes)
        self.maximum = max(self.maximum, len(frame))

    def finish_scatter(self):
        if self.header is None or self._accepted != self.header.records:
            raise IntegrityError('scatter generation omitted admitted source identities')
        self.pairs = checked_add(self.pairs, self.header.records)
        self.batches = checked_add(self.batches, 1)
        self.header = self._seen = None

    def accept(self, payload):
        header = self.header
        if header is None or len(payload) != header.output_bytes or hashlib.sha256(payload).digest() != header.digest:
            raise IntegrityError('parser batch did not authenticate')
        view = memoryview(payload)
        cursor = count = 0
        while cursor < len(view):
            if len(view) - cursor < 4:
                raise IntegrityError('parser batch has a truncated pair prefix')
            length, = struct.unpack_from('<I', view, cursor)
            if not 48 <= length <= len(view) - cursor:
                raise IntegrityError('parser batch has a truncated pair')
            frame = view[cursor:cursor + length]
            pair = decode_pair_view(frame, max_record_bytes=self.capability.max_record_bytes)
            if (pair.sample_index, pair.original_pair_index) != (self.capability.sample, self.pairs):
                raise IntegrityError('parser returned an unexpected or duplicate identity')
            for index, record in enumerate((pair.r1, pair.r2)):
                self.normalized[index] = checked_add(self.normalized[index], record.normalized_bytes)
                for line in record.lines:
                    self.hashes[index].update(line)
                    self.hashes[index].update(b'\n')
            self.append(frame)
            self.pairs = checked_add(self.pairs, 1)
            self.maximum = max(self.maximum, length)
            count += 1
            cursor += length
        if count != header.records:
            raise IntegrityError('parser batch count differs from its complete payload')
        self.batches = checked_add(self.batches, 1)
        self.header = None

    def discard(self):
        """Drop job-owned receiver graphs only after writers have been reaped."""
        if self.scatter is not None:
            self.scatter.output = self.scatter.result = None
            self.scatter.grants.clear()
            self.scatter.source = self.scatter.inventory = self.scatter.completed = None
            self.scatter = None
        self.header = self._seen = None
        self.append = None
        self.hashes.clear()
        self.capability = None
        self.scatter_inventory = self.scatter_completed = None

    def finish(self, encoded):
        report = decode_report(encoded)
        cap = self.capability
        if (self.header is not None or report.sample_index != cap.sample or
                (report.pair_count, report.r1_records, report.r2_records) != (self.pairs,) * 3 or
                (report.r1_normalized_bytes, report.r2_normalized_bytes) != tuple(self.normalized) or
                (report.r1_physical_bytes, report.r2_physical_bytes) != (cap.r1.snapshot.size, cap.r2.snapshot.size) or
                report.max_frame_bytes != self.maximum or
                (cap.scatter_mode == 'stream' and
                 (report.r1_sha256, report.r2_sha256) != tuple(value.hexdigest() for value in self.hashes)) or
                report.r1_decoded_bytes < report.r1_normalized_bytes - self.pairs or
                report.r2_decoded_bytes < report.r2_normalized_bytes - self.pairs):
            raise IntegrityError('parser completion report differs from its accepted source stream')
        return report


def run_source(message, send, receive, check_stop):
    from .worker_protocol import ParserBatch, ParserDone, Chunk, ChunkAck, ResultAck, MAX_CHUNK_BYTES
    cap = decode_source(message.capability)
    task, grant = message.job.task_id, message.job.grant_id
    batch = bytearray(cap.batch_bytes)
    carry = bytearray(maximum_frame_bytes(cap.max_record_bytes))
    root_fd = os.open(cap.root, _flags(directory=True))
    catalog = SampleCatalog(cap.root, (), 0, 0, cap.root_snapshot, root_fd)
    used = count = number = 0

    def flush():
        nonlocal used, count, number
        if not count:
            return
        view = memoryview(batch)[:used]
        send(ParserBatch(task, grant, number, used, count, used - 40 * count, hashlib.sha256(view).digest() if cap.scatter_mode == 'stream' else bytes(32)))
        # Admission precedes any receiver payload or scatter storage allocation.
        ack = receive()
        if cap.scatter_mode != 'stream':
            from .scatter_stage import send_generation
            send_generation(cap, task, grant, number, view, ack, send, receive, check_stop, run_id=message.job.run_id)
            used = count = 0
            number += 1
            return
        if not isinstance(ack, ChunkAck) or (ack.task_id, ack.grant_id, ack.end) != (task, grant, 0):
            raise IntegrityError('parser batch credit was not acknowledged')
        for offset in range(0, used, MAX_CHUNK_BYTES):
            part = bytes(view[offset:offset + MAX_CHUNK_BYTES])
            send(Chunk(task, grant, offset, part))
            ack = receive()
            if not isinstance(ack, ChunkAck) or (ack.task_id, ack.grant_id, ack.end) != (task, grant, offset + len(part)):
                raise IntegrityError('parser batch transfer was not acknowledged')
        ack = receive()
        if not isinstance(ack, ResultAck) or (ack.task_id, ack.grant_id) != (task, grant):
            raise IntegrityError('parser batch ownership was not acknowledged')
        used = count = 0
        number += 1

    try:
        with catalog.open_input(cap.r1) as raw1, catalog.open_input(cap.r2) as raw2:
            with open_decoded_stream(raw1, advertised_gzip=cap.r1.advertised_gzip) as stream1, \
                 open_decoded_stream(raw2, advertised_gzip=cap.r2.advertised_gzip) as stream2:
                reader = PairedReader(RecordFramer(stream1, line_mode='ingress', max_record_bytes=cap.max_record_bytes),
                    RecordFramer(stream2, line_mode='ingress', max_record_bytes=cap.max_record_bytes), sample_index=cap.sample)
                def cancel():
                    check_stop()
                    return False
                while (pair := reader.next_pair(cancel)) is not None:
                    length = encode_pair_into(pair, carry, max_record_bytes=cap.max_record_bytes)
                    if used + length > cap.batch_bytes:
                        flush()
                    batch[used:used + length] = memoryview(carry)[:length]
                    used += length
                    count += 1
                    del pair
                report = reader.report()
                del reader
        flush()
        send(ParserDone(task, grant, encode_report(report)))
        ack = receive()
        if not isinstance(ack, ResultAck) or (ack.task_id, ack.grant_id) != (task, grant):
            raise IntegrityError('parser completion was not acknowledged')
    finally:
        catalog.dispose()
