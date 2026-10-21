"""Canonical bounded BGZF streams, sidecars and monotonic virtual addresses.

The caller owns input/output handles and any retained returned records. Optional
Budget/Lease admission covers codec state; callers without either must already
admit the exported workspace envelope. Indexed reads validate selected payloads
and the member chain, and deliberately do not claim a whole-stream digest.
"""
from __future__ import annotations
from contextlib import AbstractContextManager
from dataclasses import dataclass
import hashlib
import struct
import zlib

from ..errors import CancelledError, ConfigurationError, IntegrityError, ResourceError
from ..resources import checked_add

PAYLOAD_BYTES = 65_280
MAX_MEMBER_BYTES = 65_536
EOF_BYTES = 28
WORKSPACE_BYTES = 2 << 20
MAX_READ_BYTES = 1 << 20
_PREFIX = bytes.fromhex('1f8b08040000000000ff060042430200')
EOF_MEMBER = bytes.fromhex('1f8b08040000000000ff0600424302001b0003000000000000000000')
_U64 = struct.Struct('<Q')
_ENTRY = struct.Struct('<QQ')
_FOOTER = struct.Struct('<II')
_U64_MAX = (1 << 64) - 1


def _uint(value, maximum, label):
    if type(value) is not int or not 0 <= value <= maximum:
        raise ConfigurationError(f'{label} must be an unsigned bounded integer')
    return value


def _check(cancel):
    if cancel is not None and cancel():
        raise CancelledError()


def _lease(budget, lease, category):
    if lease is not None:
        if budget is not None and lease._budget is not budget:
            raise ConfigurationError("BGZF lease belongs to another budget")
        if lease.bytes < WORKSPACE_BYTES or lease.released:
            raise ResourceError('BGZF codec lease is too small or released')
        return lease.transfer(category)
    return budget.acquire(category, WORKSPACE_BYTES) if budget is not None else None


def _write(target, data):
    view = memoryview(data)
    written = 0
    try:
        while written < len(view):
            count = target.write(view[written:])
            if type(count) is not int or count <= 0 or count > len(view) - written:
                raise ResourceError('artifact sink returned an invalid write count')
            written += count
    except OSError as exc:
        raise ResourceError('BGZF artifact write failed') from exc


def encode_member(payload, compression_level=6):
    """Encode one nonempty canonical data member (bounded standalone primitive)."""
    _uint(compression_level, 9, 'compression level')
    try:
        payload = memoryview(payload).cast('B')
    except (TypeError, ValueError) as exc:
        raise ConfigurationError('BGZF payload requires contiguous bytes') from exc
    if not 1 <= len(payload) <= PAYLOAD_BYTES:
        raise ConfigurationError('BGZF payload must contain 1..65280 bytes')
    for level in (compression_level, 0):
        try:
            encoder = zlib.compressobj(level, zlib.DEFLATED, -15)
            compressed = encoder.compress(payload) + encoder.flush()
        except zlib.error as exc:
            raise ResourceError('BGZF compression failed') from exc
        size = 18 + len(compressed) + 8
        if size <= MAX_MEMBER_BYTES:
            return _PREFIX + struct.pack('<H', size - 1) + compressed + _FOOTER.pack(zlib.crc32(payload), len(payload))
    raise ResourceError('BGZF fallback compression exceeded its member invariant')


@dataclass(frozen=True, slots=True)
class MemberInfo:
    compressed_start: int
    uncompressed_start: int
    decoded_bytes: int
    physical_bytes: int


@dataclass(frozen=True, slots=True)
class BgzfStats:
    physical_bytes: int
    uncompressed_bytes: int
    data_members: int
    sha256: str | None

    @property
    def bytes(self):
        return self.physical_bytes


@dataclass(frozen=True, slots=True)
class GziStats:
    bytes: int
    entries: int
    sha256: str

    @property
    def physical_bytes(self):
        return self.bytes


class BgzfWriter(AbstractContextManager):
    def __init__(self, output, *, compression_level=6, budget=None, lease=None, cancel=None, on_member=None, compression_service=None):
        _uint(compression_level, 9, 'compression level')
        if cancel is not None and not callable(cancel) or on_member is not None and not callable(on_member):
            raise ConfigurationError('BGZF callbacks must be callable')
        self.output, self.compression_level, self.cancel, self.on_member = output, compression_level, cancel, on_member
        self._lease = _lease(budget, lease, 'bgzf-writer')
        try:
            self._buffer = bytearray(PAYLOAD_BYTES)
        except BaseException:
            if self._lease is not None:
                self._lease.release()
            raise
        self._used = self._physical = self._decoded = self._members = self._submitted = 0
        self._digest = hashlib.sha256()
        self._closed = self._failed = False
        self._result = None
        self._lane = None
        if compression_service is not None:
            try:
                self._lane = compression_service.open_lane(self._consume_member)
            except BaseException:
                self.close()
                raise

    def write(self, data):
        if self._closed or self._failed:
            raise ResourceError('BGZF writer is closed or failed')
        try:
            view = memoryview(data).cast('B')
        except (TypeError, ValueError) as exc:
            raise ConfigurationError('BGZF writes require contiguous bytes') from exc
        length = len(view)
        checked_add(self._submitted, self._used + length)
        try:
            while view:
                _check(self.cancel)
                count = min(PAYLOAD_BYTES - self._used, len(view))
                self._buffer[self._used:self._used+count] = view[:count]
                self._used += count
                view = view[count:]
                if self._used == PAYLOAD_BYTES:
                    self._flush()
        except BaseException:
            self._failed = True
            raise
        return length

    def _flush(self):
        if not self._used:
            return
        payload = bytes(memoryview(self._buffer)[:self._used])
        self._submitted = checked_add(self._submitted, self._used)
        self._used = 0
        if self._lane is None:
            self._consume_member(encode_member(payload, self.compression_level))
        else:
            self._lane.submit(payload, self.compression_level)

    def _consume_member(self, member):
        # Only the coordinator invokes this callback. The shared service retains
        # each job's lease through this write and orders callbacks per lane.
        if (not isinstance(member, bytes) or not 29 <= len(member) <= MAX_MEMBER_BYTES
                or member[:16] != _PREFIX
                or struct.unpack_from('<H', member, 16)[0] + 1 != len(member)):
            raise IntegrityError('compression service returned invalid BGZF framing')
        decoded = _FOOTER.unpack(member[-8:])[1]
        if not 1 <= decoded <= PAYLOAD_BYTES or self._decoded + decoded > self._submitted:
            raise IntegrityError('compression service returned invalid decoded BGZF length')
        if self._physical >= 1 << 48:
            raise ResourceError('BGZF member start exceeds virtual-offset capacity')
        info = MemberInfo(self._physical, self._decoded, decoded, len(member))
        _write(self.output, member)
        self._digest.update(member)
        self._physical = checked_add(self._physical, len(member))
        self._decoded = checked_add(self._decoded, decoded)
        self._members += 1
        if self.on_member is not None:
            self.on_member(info)

    def finish(self):
        if self._result is not None:
            return self._result
        if self._closed or self._failed:
            raise ResourceError('BGZF writer cannot finish after failure')
        primary = None
        try:
            _check(self.cancel)
            self._flush()
            if self._lane is not None:
                self._lane.finish()
                self._lane = None
            if self._decoded != self._submitted:
                raise IntegrityError('completed compression lane lost decoded bytes')
            _write(self.output, EOF_MEMBER)
            self._digest.update(EOF_MEMBER)
            self._physical = checked_add(self._physical, EOF_BYTES)
            self._result = BgzfStats(self._physical, self._decoded, self._members, self._digest.hexdigest())
            return self._result
        except BaseException as exc:
            self._failed = True
            primary = exc
            raise
        finally:
            self.close(primary)

    @property
    def uncompressed_bytes(self):
        return self._submitted + self._used

    def close(self, primary=None):
        if not self._closed:
            try:
                if self._lane is not None:
                    lane, self._lane = self._lane, None
                    lane.abort(primary=primary)
            finally:
                self._buffer = bytearray()
                if self._lease is not None:
                    self._lease.release()
                self._closed = True

    def __exit__(self, exc_type, exc, traceback):
        if exc is None:
            self.finish()
        else:
            self.close(exc)


def virtual_offset(compressed_start, within_member):
    _uint(compressed_start, (1 << 48) - 1, 'compressed member start')
    _uint(within_member, 65535, 'within-member offset')
    return compressed_start << 16 | within_member


def split_virtual_offset(value):
    _uint(value, _U64_MAX, 'virtual offset')
    return value >> 16, value & 65535


class BgzfReader(AbstractContextManager):
    def __init__(self, source, *, uncompressed_limit, physical_limit, budget=None, lease=None,
                 cancel=None, canonical=True, indexed=False, on_member=None):
        _uint(uncompressed_limit, _U64_MAX, 'BGZF decoded limit')
        _uint(physical_limit, _U64_MAX, 'BGZF physical limit')
        if not canonical:
            raise ConfigurationError('pool BGZF readers require canonical framing')
        if cancel is not None and not callable(cancel) or on_member is not None and not callable(on_member):
            raise ConfigurationError('BGZF callbacks must be callable')
        if indexed and not hasattr(source, 'seek'):
            raise ConfigurationError('indexed BGZF access requires a seekable source')
        self.source, self.cancel, self.indexed, self.on_member = source, cancel, bool(indexed), on_member
        self.uncompressed_limit, self.physical_limit = uncompressed_limit, physical_limit
        self._lease = _lease(budget, lease, 'bgzf-reader')
        self._physical = self._decoded = self._members = self.bytes_read = 0
        self._digest = hashlib.sha256()
        self._data = b''
        self._position = 0
        self.member = None
        self._previous_tail = None
        self._short = self._eof = self._closed = False

    def _exact(self, count):
        if self._physical + count > self.physical_limit:
            raise IntegrityError('BGZF stream exceeds its declared physical bound')
        parts = bytearray()
        try:
            while len(parts) < count:
                part = self.source.read(count - len(parts))
                if not isinstance(part, bytes) or not part or len(part) > count - len(parts):
                    raise IntegrityError('BGZF member is truncated or source read is invalid')
                parts.extend(part)
        except OSError as exc:
            raise IntegrityError('BGZF artifact read failed') from exc
        self._physical += count
        self.bytes_read += count
        result = bytes(parts)
        if not self.indexed:
            self._digest.update(result)
        return result

    def _skip(self, count):
        if self._physical + count > self.physical_limit:
            raise IntegrityError('BGZF member exceeds its physical bound')
        try:
            self.source.seek(count, 1)
        except (OSError, ValueError) as exc:
            raise IntegrityError('BGZF indexed payload skip failed') from exc
        self._physical += count

    @staticmethod
    def _inflate(payload, footer):
        expected_crc, decoded_size = _FOOTER.unpack(footer)
        if decoded_size > PAYLOAD_BYTES:
            raise IntegrityError('BGZF decoded member exceeds canonical payload bound')
        try:
            decoder = zlib.decompressobj(-15)
            decoded = decoder.decompress(payload, PAYLOAD_BYTES + 1)
        except zlib.error as exc:
            raise IntegrityError('BGZF DEFLATE payload is malformed') from exc
        if (len(decoded) != decoded_size or not decoder.eof or decoder.unused_data
                or decoder.unconsumed_tail or zlib.crc32(decoded) != expected_crc):
            raise IntegrityError('BGZF DEFLATE termination, CRC or ISIZE is invalid')
        return decoded

    def _next(self, target=None):
        if self._eof:
            return False
        _check(self.cancel)
        start = self._physical
        if start >= 1 << 48:
            raise IntegrityError('BGZF member address exceeds virtual-offset capacity')
        header = self._exact(18)
        if header[:16] != _PREFIX:
            raise IntegrityError('BGZF header is not canonical')
        size = struct.unpack_from('<H', header, 16)[0] + 1
        if not 28 <= size <= MAX_MEMBER_BYTES:
            raise IntegrityError('BGZF member physical length is invalid')
        end = start + size
        if target is not None and start < target < end:
            raise IntegrityError('virtual address is not a BGZF member boundary')
        decode = target is None or start == target or end == target or size == EOF_BYTES
        if decode:
            body = self._exact(size - 18)
            footer = body[-8:]
            decoded = self._inflate(body[:-8], footer)
        else:
            self._skip(size - 26)
            footer = self._exact(8)
            decoded = None
        decoded_length = _FOOTER.unpack(footer)[1]
        if decoded_length == 0:
            if not decode or header + body != EOF_MEMBER:
                raise IntegrityError('BGZF zero-length member is not the canonical EOF')
            try:
                trailing = self.source.read(1)
            except OSError as exc:
                raise IntegrityError('BGZF trailing-byte check failed') from exc
            self.bytes_read += len(trailing)
            if trailing:
                raise IntegrityError('BGZF EOF has trailing bytes or duplicate members')
            self._eof = True
            self._data, self._position = b'', 0
            self.member = None
            return False
        if decoded_length > PAYLOAD_BYTES or self._short:
            raise IntegrityError('BGZF nonterminal data member has noncanonical payload length')
        if self._decoded + decoded_length > self.uncompressed_limit:
            raise IntegrityError('BGZF decoded stream exceeds its admitted bound')
        self._previous_tail = self._data[-1:] if self._data else None
        info = MemberInfo(start, self._decoded, decoded_length, size)
        self._decoded += decoded_length
        self._members += 1
        self._short = decoded_length < PAYLOAD_BYTES
        self.member, self._data, self._position = info, decoded if decoded is not None else b'', 0
        if self.on_member is not None:
            self.on_member(info)
        return True

    def _ensure(self):
        while self._position == len(self._data):
            if not self._next():
                return False
        return True

    def tell_virtual(self):
        if self._closed:
            raise ResourceError('BGZF reader is closed')
        if not self._ensure():
            raise IntegrityError('EOF is not a valid record virtual address')
        return virtual_offset(self.member.compressed_start, self._position)

    def tell_uncompressed(self):
        if self.member is None:
            return self._decoded
        return self.member.uncompressed_start + self._position

    def read(self, size=MAX_READ_BYTES):
        if self._closed:
            raise ResourceError('BGZF reader is closed')
        if type(size) is not int or not 0 <= size <= MAX_READ_BYTES:
            raise ResourceError('BGZF read requires a bounded size of at most 1 MiB')
        result = bytearray()
        while len(result) < size and self._ensure():
            _check(self.cancel)
            count = min(size - len(result), len(self._data) - self._position)
            result.extend(memoryview(self._data)[self._position:self._position+count])
            self._position += count
        return bytes(result)

    def readline(self, limit):
        if self._closed:
            raise ResourceError("BGZF reader is closed")
        _uint(limit, (1 << 32) - 1, 'BGZF line limit')
        if not limit:
            raise ResourceError('BGZF line limit must be positive')
        result = bytearray()
        while self._ensure():
            _check(self.cancel)
            end = self._data.find(b'\n', self._position)
            stop = len(self._data) if end < 0 else end + 1
            count = stop - self._position
            if len(result) + count > limit:
                raise ResourceError('BGZF line exceeds its admitted byte limit')
            result.extend(memoryview(self._data)[self._position:stop])
            self._position = stop
            if end >= 0:
                return bytes(result)
        if result:
            raise IntegrityError('BGZF text stream has an unterminated final line')
        return b''

    def seek_virtual(self, offset):
        if self._closed:
            raise ResourceError("BGZF reader is closed")
        if not self.indexed:
            raise ConfigurationError('virtual seeking requires indexed mode')
        target, within = split_virtual_offset(offset)
        if self.member is not None:
            if target < self.member.compressed_start or target == self.member.compressed_start and within < self._position:
                raise IntegrityError('indexed BGZF addresses must be monotonic and nonoverlapping')
        while self.member is None or self.member.compressed_start < target:
            if not self._next(target):
                raise IntegrityError('indexed BGZF address targets EOF or an absent member')
        if self.member.compressed_start != target or within >= self.member.decoded_bytes:
            raise IntegrityError('indexed BGZF address is not a canonical data position')
        if not self._data:
            raise IntegrityError('selected BGZF member was not decoded')
        preceding = self._data[within-1:within] if within else self._previous_tail
        if (target or within) and preceding != b'\n':
            raise IntegrityError('indexed BGZF address does not begin a text line')
        self._position = within
        return self.member.uncompressed_start + within

    def readline_at(self, offset, limit):
        self.seek_virtual(offset)
        return self.readline(limit)

    def read_record_at(self, offset, limit):
        self.seek_virtual(offset)
        return self.read_record(limit)

    def read_record(self, limit):
        _uint(limit, (1 << 32) - 1, 'BGZF record limit')
        lines = []
        length = 0
        for _ in range(4):
            line = self.readline(limit - length)
            if not line:
                if not lines:
                    return b''
                raise IntegrityError('BGZF FASTQ record is truncated')
            lines.append(line)
            length += len(line)
        if not lines[0].startswith(b'@') or not lines[2].startswith(b'+') or len(lines[1]) != len(lines[3]):
            raise IntegrityError('BGZF FASTQ record shape is invalid')
        return b''.join(lines)

    @property
    def stats(self):
        return BgzfStats(self._physical, self._decoded, self._members,
                         None if self.indexed else self._digest.hexdigest())

    def finish(self):
        if self._closed:
            raise ResourceError('BGZF reader is closed')
        self._position = len(self._data)
        while not self._eof:
            self._next()
        return self.stats

    def close(self):
        if not self._closed:
            self._data = b''
            if self._lease is not None:
                self._lease.release()
            self._closed = True

    def __exit__(self, exc_type, exc, traceback):
        try:
            if exc is None and not self.indexed:
                self.finish()
        finally:
            self.close()


class GziCursor(AbstractContextManager):
    """O(1)-cursor validation/conversion; no member table is retained."""
    def __init__(self, source, *, uncompressed_bytes, physical_bytes, expected_entries=None,
                 budget=None, cancel=None):
        _uint(uncompressed_bytes, _U64_MAX, 'sidecar decoded length')
        _uint(physical_bytes, (1 << 48) - 1, 'sidecar compressed length')
        expected_members = (uncompressed_bytes + PAYLOAD_BYTES - 1) // PAYLOAD_BYTES
        if (physical_bytes < 29 * expected_members + EOF_BYTES
                or physical_bytes > MAX_MEMBER_BYTES * expected_members + EOF_BYTES):
            raise IntegrityError('sidecar stream bounds cannot describe canonical BGZF')
        if expected_entries is not None:
            _uint(expected_entries, _U64_MAX, 'sidecar expected count')
        if cancel is not None and not callable(cancel):
            raise ConfigurationError('sidecar cancellation callback must be callable')
        self.source, self.cancel = source, cancel
        self.uncompressed_bytes, self.physical_bytes = uncompressed_bytes, physical_bytes
        self._lease = budget.acquire('gzi-cursor', 4096, metadata_bytes=4096) if budget is not None else None
        self._digest = hashlib.sha256()
        self._bytes = self._read_count = self._observed = 0
        self._current = (0, 0)
        self._last_requested = -1
        self._mode = None
        self._finished = self._closed = False
        try:
            self.entries, = _U64.unpack(self._read(8))
            canonical = max((uncompressed_bytes + PAYLOAD_BYTES - 1) // PAYLOAD_BYTES - 1, 0)
            if self.entries != canonical or expected_entries is not None and self.entries != expected_entries:
                raise IntegrityError('BGZF sidecar entry count disagrees with canonical stream bounds')
            self._next_entry = self._entry()
        except BaseException:
            self.close()
            raise

    def _read(self, count):
        _check(self.cancel)
        data = bytearray()
        try:
            while len(data) < count:
                part = self.source.read(count - len(data))
                if not isinstance(part, bytes) or not part or len(part) > count - len(data):
                    raise IntegrityError('BGZF sidecar is truncated or source read is invalid')
                data.extend(part)
        except OSError as exc:
            raise IntegrityError('BGZF sidecar read failed') from exc
        self._digest.update(data)
        self._bytes += count
        return bytes(data)

    def _entry(self):
        if self._read_count == self.entries:
            return None
        compressed, uncompressed = _ENTRY.unpack(self._read(16))
        self._read_count += 1
        if (compressed <= self._current[0] or compressed >= self.physical_bytes - EOF_BYTES
                or compressed >= 1 << 48 or uncompressed != self._read_count * PAYLOAD_BYTES
                or uncompressed >= self.uncompressed_bytes):
            raise IntegrityError('BGZF sidecar member offsets are not canonical or in range')
        return compressed, uncompressed

    def virtual_offset(self, uncompressed_offset):
        if self._closed or self._finished:
            raise ResourceError("sidecar cursor is closed or finished")
        _uint(uncompressed_offset, _U64_MAX, 'uncompressed record offset')
        if self._mode not in (None, 'convert'):
            raise ConfigurationError('sidecar cursor cannot mix conversion and observation')
        self._mode = 'convert'
        if not self._last_requested < uncompressed_offset < self.uncompressed_bytes:
            raise IntegrityError('sidecar record offsets must increase and remain inside data')
        while self._next_entry is not None and self._next_entry[1] <= uncompressed_offset:
            self._current = self._next_entry
            self._next_entry = self._entry()
        self._last_requested = uncompressed_offset
        within = uncompressed_offset - self._current[1]
        if within >= min(PAYLOAD_BYTES, self.uncompressed_bytes - self._current[1]):
            raise IntegrityError('sidecar offset is not inside its canonical data member')
        return virtual_offset(self._current[0], within)

    def observe_member(self, info):
        if self._closed or self._finished:
            raise ResourceError("sidecar cursor is closed or finished")
        if self._mode not in (None, 'observe'):
            raise ConfigurationError('sidecar cursor cannot mix conversion and observation')
        self._mode = 'observe'
        if self._observed == 0:
            expected = (0, 0)
        else:
            if self._next_entry is None:
                raise IntegrityError('BGZF has more data members than its sidecar')
            expected = self._next_entry
        if (info.compressed_start, info.uncompressed_start) != expected:
            raise IntegrityError('BGZF sidecar differs from observed member boundaries')
        self._observed += 1
        if self._observed > 1:
            self._current = self._next_entry
            self._next_entry = self._entry()

    def finish(self):
        if self._closed:
            raise ResourceError("sidecar cursor is closed")
        if self._finished:
            return GziStats(self._bytes, self.entries, self._digest.hexdigest())
        if self._mode == 'observe' and self._observed != (self.uncompressed_bytes + PAYLOAD_BYTES - 1) // PAYLOAD_BYTES:
            raise IntegrityError('BGZF sidecar did not observe every expected data member')
        while self._next_entry is not None:
            self._current = self._next_entry
            self._next_entry = self._entry()
        try:
            if self.source.read(1):
                raise IntegrityError('BGZF sidecar has trailing bytes')
        except OSError as exc:
            raise IntegrityError('BGZF sidecar trailing-byte check failed') from exc
        self._finished = True
        return GziStats(self._bytes, self.entries, self._digest.hexdigest())

    def close(self):
        if not self._closed:
            if self._lease is not None:
                self._lease.release()
            self._closed = True

    def __exit__(self, exc_type, exc, traceback):
        try:
            if exc is None:
                self.finish()
        finally:
            self.close()


def build_gzi(output, source, *, uncompressed_bytes, physical_bytes, budget=None, cancel=None):
    """Write a standard sidecar using one bounded verified pass over closed BGZF."""
    _uint(uncompressed_bytes, _U64_MAX, 'BGZF decoded length')
    members = (uncompressed_bytes + PAYLOAD_BYTES - 1) // PAYLOAD_BYTES
    entries = max(members - 1, 0)
    digest = hashlib.sha256()
    count = 0
    def emit(data):
        nonlocal count
        _write(output, data)
        digest.update(data)
        count += len(data)
    emit(_U64.pack(entries))
    def observe(info):
        if info.compressed_start:
            emit(_ENTRY.pack(info.compressed_start, info.uncompressed_start))
    with BgzfReader(source, uncompressed_limit=uncompressed_bytes, physical_limit=physical_bytes,
                    budget=budget, cancel=cancel, on_member=observe) as reader:
        stats = reader.finish()
        if (stats.uncompressed_bytes, stats.physical_bytes, stats.data_members) != (uncompressed_bytes, physical_bytes, members):
            raise IntegrityError('BGZF observed totals disagree with sidecar source declarations')
    if count != 8 + 16 * entries:
        raise IntegrityError('BGZF sidecar observed entry count differs from its header')
    return GziStats(count, entries, digest.hexdigest())
