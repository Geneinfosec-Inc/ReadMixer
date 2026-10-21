"""Bounded byte FASTQ framing and lockstep, lossless paired input validation.

Record views borrow their framer's allocation and expire at its next read. A
caller must consume a pair before requesting another. Compression wrappers never
close the caller-owned input descriptor and never reopen or seek their inputs.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import re
from typing import BinaryIO, Callable, Literal
import zlib

from .errors import CancelledError, ConfigurationError, InputError, ResourceError

LineMode = Literal["ingress", "canonical_lf"]
Cancel = Callable[[], bool] | None
_UINT32_MAX = (1 << 32) - 1
_UINT64_MAX = (1 << 64) - 1
_CHUNK_BYTES = 64 * 1024
_ASCII_WHITESPACE = frozenset(b" \t\n\r\v\f")
_HEADER_TOKENS = re.compile(rb"[^ \t\n\r\v\f]+")
_GZIP_MAGIC = b"\x1f\x8b"


def _check_cancel(cancel: Cancel) -> None:
    if cancel is not None and cancel():
        raise CancelledError()


def _add_counter(value: int, increment: int) -> int:
    if increment < 0 or value > _UINT64_MAX - increment:
        raise ResourceError("input byte or record counter exceeds unsigned 64-bit range")
    return value + increment


class DecodedStream:
    """One forward plain/gzip stream; gzip trailing bytes must be full members.

    Unlike ``gzip.GzipFile``, this reader rejects zero padding after a member.
    zlib checks each member's header, CRC and uncompressed-size trailer. Both
    compressed input and decoded output are chunk bounded, including empty or
    highly compressible member sequences.
    """

    def __init__(self, raw: BinaryIO, *, advertised_gzip: bool = False):
        self._raw = raw
        self.physical_bytes = 0
        self.decoded_bytes = 0
        self._pending = b""
        self._decompressor = None
        self._members = 0
        self._finished = False
        self._closed = False
        while len(self._pending) < 2:
            chunk = self._read_raw(2 - len(self._pending))
            if not chunk:
                break
            self._pending += chunk
        self.codec = "gzip" if self._pending == _GZIP_MAGIC else "plain"
        if advertised_gzip and self.codec != "gzip":
            raise InputError("input advertised as gzip does not contain a gzip header")

    def __enter__(self) -> DecodedStream:
        if self._closed:
            raise ValueError("decoded stream is closed")
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def close(self) -> None:
        """Release decoder state, preserving the caller-owned descriptor."""
        self._closed = True
        self._pending = b""
        self._decompressor = None

    def _read_raw(self, size: int) -> bytes:
        try:
            result = self._raw.read(size)
        except OSError as exc:
            raise InputError("cannot read input descriptor") from exc
        if not isinstance(result, bytes) or len(result) > size:
            raise InputError("input descriptor did not provide a bounded byte read")
        self.physical_bytes = _add_counter(self.physical_bytes, len(result))
        return result

    def readinto(self, destination: bytearray | memoryview, cancel: Cancel = None) -> int:
        if self._closed:
            raise ValueError("decoded stream is closed")
        target = memoryview(destination).cast("B")
        if target.readonly:
            raise TypeError("decoded destination must be writable")
        wanted = min(len(target), _CHUNK_BYTES)
        _check_cancel(cancel)
        if wanted == 0 or self._finished:
            return 0
        if self.codec == "plain":
            if self._pending:
                piece, self._pending = self._pending[:wanted], self._pending[wanted:]
            else:
                piece = self._read_raw(wanted)
            _check_cancel(cancel)
            target[:len(piece)] = piece
            if not piece:
                self._finished = True
            self.decoded_bytes = _add_counter(self.decoded_bytes, len(piece))
            return len(piece)

        while True:
            _check_cancel(cancel)
            if self._decompressor is None:
                while len(self._pending) < 2:
                    chunk = self._read_raw(_CHUNK_BYTES - len(self._pending))
                    if not chunk:
                        if self._pending or self._members == 0:
                            raise InputError("truncated gzip member header")
                        self._finished = True
                        return 0
                    self._pending += chunk
                    _check_cancel(cancel)
                if self._pending[:2] != _GZIP_MAGIC:
                    raise InputError("gzip input contains nonmember trailing bytes")
                self._decompressor = zlib.decompressobj(wbits=31)
            if not self._pending:
                self._pending = self._read_raw(_CHUNK_BYTES)
                if not self._pending:
                    raise InputError("truncated gzip member")
                _check_cancel(cancel)
            try:
                piece = self._decompressor.decompress(self._pending, wanted)
            except zlib.error as exc:
                raise InputError("gzip member failed decoding or checksum validation") from exc
            _check_cancel(cancel)
            if self._decompressor.eof:
                self._pending = self._decompressor.unused_data
                self._decompressor = None
                self._members = _add_counter(self._members, 1)
            else:
                self._pending = self._decompressor.unconsumed_tail
            if piece:
                target[:len(piece)] = piece
                self.decoded_bytes = _add_counter(self.decoded_bytes, len(piece))
                return len(piece)

    def read(self, size: int) -> bytes:
        """Read at most one bounded chunk; an unbounded read is unsupported."""
        if not isinstance(size, int) or size < 0:
            raise ValueError("decoded reads require a nonnegative byte bound")
        destination = bytearray(min(size, _CHUNK_BYTES))
        count = self.readinto(destination)
        return bytes(memoryview(destination)[:count])


def open_decoded_stream(raw: BinaryIO, *, advertised_gzip: bool = False) -> DecodedStream:
    return DecodedStream(raw, advertised_gzip=advertised_gzip)


@dataclass(frozen=True, slots=True)
class RecordView:
    """Four borrowed payload ranges; the next framer read invalidates this view."""

    _owner: RecordFramer
    _generation: int
    _ranges: tuple[tuple[int, int], ...]
    _normalized_bytes: int

    def _line(self, index: int) -> memoryview:
        self._check_live()
        start, end = self._ranges[index]
        return memoryview(self._owner._buffer)[start:end].toreadonly()

    def _check_live(self) -> None:
        if self._owner._generation != self._generation:
            raise RuntimeError("FASTQ record view expired on the next framer read")

    @property
    def header(self) -> memoryview:
        return self._line(0)

    @property
    def sequence(self) -> memoryview:
        return self._line(1)

    @property
    def plus(self) -> memoryview:
        return self._line(2)

    @property
    def quality(self) -> memoryview:
        return self._line(3)

    @property
    def lines(self) -> tuple[memoryview, memoryview, memoryview, memoryview]:
        return self.header, self.sequence, self.plus, self.quality

    @property
    def normalized_bytes(self) -> int:
        self._check_live()
        return self._normalized_bytes


class RecordFramer:
    """Parse four physical lines in one reusable ``max_record_bytes + 5`` arena."""

    def __init__(
        self,
        stream: BinaryIO | DecodedStream,
        *,
        line_mode: LineMode,
        max_record_bytes: int = 1024 * 1024,
        read_chunk_bytes: int = _CHUNK_BYTES,
    ):
        if line_mode not in ("ingress", "canonical_lf"):
            raise ConfigurationError("FASTQ line mode must be ingress or canonical_lf")
        if (isinstance(max_record_bytes, bool) or not isinstance(max_record_bytes, int)
                or max_record_bytes <= 0 or 40 + 2 * max_record_bytes > _UINT32_MAX):
            raise ConfigurationError("record byte limit cannot fit a paired unsigned-32-bit frame")
        if (isinstance(read_chunk_bytes, bool) or not isinstance(read_chunk_bytes, int)
                or not 1 <= read_chunk_bytes <= _CHUNK_BYTES):
            raise ConfigurationError("parser read chunk must be between 1 and 65536 bytes")
        self.stream = stream
        self.line_mode = line_mode
        self.max_record_bytes = max_record_bytes
        self._read_chunk_bytes = read_chunk_bytes
        try:
            self._buffer = bytearray(max_record_bytes + 5)
        except (MemoryError, OverflowError) as exc:
            raise ResourceError("cannot allocate admitted FASTQ record buffer") from exc
        self._cursor = self._end = self._generation = 0
        self._eof = False
        self.record_count = self.normalized_bytes = self.decoded_bytes = 0
        self._digest = hashlib.sha256()

    @property
    def sha256(self) -> str:
        return self._digest.hexdigest()

    @property
    def physical_bytes(self) -> int:
        return getattr(self.stream, "physical_bytes", self.decoded_bytes)

    def _fill(self, cancel: Cancel) -> None:
        _check_cancel(cancel)
        available = len(self._buffer) - self._end
        if available == 0:
            raise ResourceError("FASTQ record exceeds normalized byte limit")
        size = min(available, self._read_chunk_bytes)
        target = memoryview(self._buffer)[self._end:self._end + size]
        try:
            if isinstance(self.stream, DecodedStream):
                count = self.stream.readinto(target, cancel)
            elif hasattr(self.stream, "readinto"):
                count = self.stream.readinto(target)
            else:
                chunk = self.stream.read(size)
                if not isinstance(chunk, bytes) or len(chunk) > size:
                    raise InputError("FASTQ stream did not provide a bounded byte read")
                count = len(chunk)
                target[:count] = chunk
        except OSError as exc:
            raise InputError("cannot read FASTQ stream") from exc
        _check_cancel(cancel)
        if not isinstance(count, int) or not 0 <= count <= size:
            raise InputError("FASTQ stream did not provide a bounded byte read")
        if count == 0:
            self._eof = True
        self.decoded_bytes = _add_counter(self.decoded_bytes, count)
        self._end += count

    def next_record(self, cancel: Cancel = None) -> RecordView | None:
        self._generation += 1
        _check_cancel(cancel)
        remaining = self._end - self._cursor
        if self._cursor and remaining:
            arena = memoryview(self._buffer)
            arena[:remaining] = arena[self._cursor:self._end]
        self._cursor, self._end = 0, remaining
        ranges: list[tuple[int, int]] = []
        normalized_bytes = 4
        start = 0
        for line_number in range(4):
            scan_start = start
            while True:
                _check_cancel(cancel)
                end = self._buffer.find(b"\n", scan_start, self._end)
                if end >= 0:
                    payload_end = end
                    if (self.line_mode == "ingress" and payload_end > start
                            and self._buffer[payload_end - 1] == 13):
                        payload_end -= 1
                    self._cursor = end + 1
                    break
                scan_start = self._end
                if self._eof:
                    if line_number == 0 and start == self._end:
                        return None
                    if (line_number == 3 and self.line_mode == "ingress"
                            and self._end > start):
                        payload_end = self._end
                        self._cursor = self._end
                        break
                    raise InputError("FASTQ record is missing a required physical line terminator")
                # A pending CR may still be the CRLF delimiter. Reserve exactly
                # that one byte until its following LF or EOF is observed.
                pending = self._end - start
                if (self.line_mode == "ingress" and pending
                        and self._buffer[self._end - 1] == 13):
                    pending -= 1
                if (normalized_bytes + pending > self.max_record_bytes
                        and (line_number != 0 or self._end != 0)):
                    raise ResourceError("FASTQ record exceeds normalized byte limit")
                self._fill(cancel)
            normalized_bytes += payload_end - start
            if normalized_bytes > self.max_record_bytes:
                raise ResourceError("FASTQ record exceeds normalized byte limit")
            ranges.append((start, payload_end))
            start = self._cursor
        header, sequence, plus, quality = ranges
        if header[0] == header[1] or self._buffer[header[0]] != 64:
            raise InputError("FASTQ header must begin with the header marker")
        if plus[0] == plus[1] or self._buffer[plus[0]] != 43:
            raise InputError("FASTQ plus line must begin with the plus marker")
        if sequence[1] - sequence[0] != quality[1] - quality[0]:
            raise InputError("FASTQ sequence and quality lengths differ")
        record_count = _add_counter(self.record_count, 1)
        normalized_total = _add_counter(self.normalized_bytes, normalized_bytes)
        view = RecordView(self, self._generation, tuple(ranges), normalized_bytes)
        for line in view.lines:
            self._digest.update(line)
            self._digest.update(b"\n")
        self.record_count, self.normalized_bytes = record_count, normalized_total
        return view


def _terminal_role(token: memoryview) -> int | None:
    if len(token) >= 2 and token[-2] == 47 and token[-1] in (49, 50):
        return token[-1] - 48
    return None


def header_identity(header: memoryview | bytes) -> tuple[memoryview, int | None]:
    """Borrow a base identifier and reconcile only the specified role grammar."""
    header = memoryview(header).cast("B")
    if not header or header[0] != 64:
        raise InputError("mate header is missing its header marker")
    # The bytes matcher scans the existing contiguous buffer without copying
    # opaque metadata. Retain only the identifier and first/last metadata spans;
    # a token list would grow with the admitted header length.
    identifier = _HEADER_TOKENS.search(header, 1)
    if identifier is None:
        raise InputError("mate header has an empty base identifier")
    token = header[identifier.start():identifier.end()]
    role = _terminal_role(token)
    base = token[:-2] if role is not None else token
    if not base:
        raise InputError("mate header has an empty base identifier")
    if identifier.end() == len(header):
        return base, role
    first_metadata = None
    last_metadata = None
    tokens = _HEADER_TOKENS.finditer(header, identifier.end())
    first = next(tokens, None)
    if first is not None:
        last = first
        for last in tokens:
            pass
        first_metadata = header[first.start():first.end()]
        last_metadata = header[last.start():last.end()]
    candidates = [role]
    if first_metadata is not None and len(first_metadata) >= 2:
        if first_metadata[0] in (49, 50) and first_metadata[1] == 58:
            candidates.append(first_metadata[0] - 48)
    if last_metadata is not None:
        candidates.append(_terminal_role(last_metadata))
        if len(last_metadata) == 1 and last_metadata[0] in (49, 50):
            candidates.append(last_metadata[0] - 48)
    for candidate in candidates:
        if candidate is not None:
            if role is not None and candidate != role:
                raise InputError("mate header contains conflicting role markers")
            role = candidate
    return base, role


@dataclass(frozen=True, slots=True)
class PairView:
    sample_index: int
    original_pair_index: int
    r1: RecordView
    r2: RecordView

    @property
    def normalized_bytes(self) -> int:
        return self.r1.normalized_bytes + self.r2.normalized_bytes

    @property
    def frame_bytes(self) -> int:
        return 40 + self.normalized_bytes


@dataclass(frozen=True, slots=True)
class SampleReadReport:
    sample_index: int
    pair_count: int
    r1_records: int
    r2_records: int
    r1_normalized_bytes: int
    r2_normalized_bytes: int
    r1_decoded_bytes: int
    r2_decoded_bytes: int
    r1_physical_bytes: int
    r2_physical_bytes: int
    max_frame_bytes: int
    r1_sha256: str
    r2_sha256: str
    complete: bool


class PairedReader:
    """Consume exactly one record from each mate before admitting a whole pair."""

    def __init__(self, r1: RecordFramer, r2: RecordFramer, *, sample_index: int):
        if (isinstance(sample_index, bool) or not isinstance(sample_index, int)
                or not 0 <= sample_index <= _UINT32_MAX):
            raise ConfigurationError("sample index must be an unsigned 32-bit integer")
        if r1 is r2:
            raise ConfigurationError("paired reader requires two distinct mate framers")
        self.r1, self.r2 = r1, r2
        self.sample_index = sample_index
        self.pair_count = 0
        self.max_frame_bytes = 0
        self.complete = False

    def _read_mate(self, framer: RecordFramer, mate: int, cancel: Cancel) -> RecordView | None:
        try:
            return framer.next_record(cancel)
        except (InputError, ResourceError) as exc:
            raise type(exc)(f"sample {self.sample_index}, mate {mate}, pair {self.pair_count}: {exc}") from exc

    def _identity(self, view: RecordView, mate: int) -> tuple[memoryview, int | None]:
        try:
            return header_identity(view.header)
        except InputError as exc:
            raise InputError(f"sample {self.sample_index}, mate {mate}, pair {self.pair_count}: {exc}") from exc

    def next_pair(self, cancel: Cancel = None) -> PairView | None:
        _check_cancel(cancel)
        if self.complete:
            return None
        left = self._read_mate(self.r1, 1, cancel)
        right = self._read_mate(self.r2, 2, cancel)
        if left is None or right is None:
            if left is not None or right is not None:
                raise InputError(f"sample {self.sample_index}, pair {self.pair_count}: mate streams end at different ordinals")
            self.complete = True
            return None
        base1, role1 = self._identity(left, 1)
        base2, role2 = self._identity(right, 2)
        if base1 != base2:
            raise InputError(f"sample {self.sample_index}, pair {self.pair_count}: mate base identifiers differ")
        if role1 not in (None, 1) or role2 not in (None, 2):
            raise InputError(f"sample {self.sample_index}, pair {self.pair_count}: mate header role disagrees with its input role")
        count = _add_counter(self.pair_count, 1)
        pair = PairView(self.sample_index, self.pair_count, left, right)
        self.max_frame_bytes = max(self.max_frame_bytes, pair.frame_bytes)
        self.pair_count = count
        return pair

    def report(self) -> SampleReadReport:
        return SampleReadReport(
            sample_index=self.sample_index, pair_count=self.pair_count,
            r1_records=self.r1.record_count, r2_records=self.r2.record_count,
            r1_normalized_bytes=self.r1.normalized_bytes,
            r2_normalized_bytes=self.r2.normalized_bytes,
            r1_decoded_bytes=self.r1.decoded_bytes, r2_decoded_bytes=self.r2.decoded_bytes,
            r1_physical_bytes=self.r1.physical_bytes, r2_physical_bytes=self.r2.physical_bytes,
            max_frame_bytes=self.max_frame_bytes, r1_sha256=self.r1.sha256,
            r2_sha256=self.r2.sha256, complete=self.complete,
        )
