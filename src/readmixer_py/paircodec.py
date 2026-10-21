"""PYPAIR1 private atomic frames: checked lengths and borrowed opaque lines."""
from __future__ import annotations

from dataclasses import dataclass
import re
import struct
from typing import Protocol

from .errors import ConfigurationError, InputError, IntegrityError, ResourceError
from .fastq import header_identity

FRAME_HEADER_BYTES = 48
DEFAULT_MAX_RECORD_BYTES = 1024 * 1024
_HEADER = struct.Struct("<IIQ8I")
_LINE_FEED = re.compile(b"\n")


class _Record(Protocol):
    @property
    def lines(self) -> tuple[memoryview, memoryview, memoryview, memoryview]: ...


class _Pair(Protocol):
    sample_index: int
    original_pair_index: int
    r1: _Record
    r2: _Record


def maximum_frame_bytes(max_record_bytes: int, *, single_frame_limit: int | None = None) -> int:
    """Validate the configured per-mate limit before allocating a frame arena."""
    if (isinstance(max_record_bytes, bool) or not isinstance(max_record_bytes, int)
            or max_record_bytes <= 0 or 40 + 2 * max_record_bytes > 0xFFFFFFFF):
        raise ConfigurationError("record byte limit cannot fit a paired unsigned-32-bit frame")
    maximum = 40 + 2 * max_record_bytes
    if single_frame_limit is not None:
        if (isinstance(single_frame_limit, bool) or not isinstance(single_frame_limit, int)
                or single_frame_limit <= 0):
            raise ConfigurationError("single-frame resource limit must be a positive integer")
        if maximum > single_frame_limit:
            raise ResourceError("configured pair frame exceeds its single-frame resource limit")
    return maximum


def _byte_view(value: bytes | bytearray | memoryview) -> memoryview:
    try:
        return memoryview(value).cast("B")
    except (TypeError, ValueError) as exc:
        raise ConfigurationError("pair codec requires a contiguous byte buffer") from exc


def _validate_lines(lines: tuple[memoryview, ...], limit: int, error: type[InputError] | type[IntegrityError]) -> None:
    if len(lines) != 8:
        raise error("pair frame must contain exactly eight lines")
    # LF cannot be a physical line payload. CR and all other opaque bytes remain
    # untouched, including a final ingress quality payload ending in CR.
    # Input views are already contiguous bytes. Search their buffers directly
    # in native code without materializing a payload-sized bytes copy.
    if any(_LINE_FEED.search(line) is not None for line in lines):
        raise error("pair frame line payload contains an LF terminator")
    for start in (0, 4):
        header, sequence, plus, quality = lines[start:start + 4]
        if not header or header[0] != 64 or not plus or plus[0] != 43:
            raise error("pair frame contains an invalid FASTQ marker")
        if len(sequence) != len(quality):
            raise error("pair frame sequence and quality lengths differ")
        if 4 + sum(len(line) for line in lines[start:start + 4]) > limit:
            raise error("pair frame exceeds the normalized per-mate byte limit")
    try:
        base1, role1 = header_identity(lines[0])
        base2, role2 = header_identity(lines[4])
    except InputError as exc:
        raise error("pair frame has an invalid mate identity") from exc
    if base1 != base2 or role1 not in (None, 1) or role2 not in (None, 2):
        raise error("pair frame mate identities or roles disagree")


@dataclass(frozen=True, slots=True)
class DecodedRecordView:
    """Readonly borrowed ranges; validity follows the caller-owned frame arena."""

    _frame: memoryview
    _ranges: tuple[tuple[int, int], ...]

    @property
    def lines(self) -> tuple[memoryview, memoryview, memoryview, memoryview]:
        return tuple(self._frame[start:end] for start, end in self._ranges)

    @property
    def header(self) -> memoryview:
        start, end = self._ranges[0]
        return self._frame[start:end]

    @property
    def sequence(self) -> memoryview:
        start, end = self._ranges[1]
        return self._frame[start:end]

    @property
    def plus(self) -> memoryview:
        start, end = self._ranges[2]
        return self._frame[start:end]

    @property
    def quality(self) -> memoryview:
        start, end = self._ranges[3]
        return self._frame[start:end]

    @property
    def normalized_bytes(self) -> int:
        return 4 + sum(end - start for start, end in self._ranges)


@dataclass(frozen=True, slots=True)
class DecodedPairView:
    sample_index: int
    original_pair_index: int
    r1: DecodedRecordView
    r2: DecodedRecordView
    frame: memoryview

    @property
    def frame_bytes(self) -> int:
        return len(self.frame)

    @property
    def normalized_bytes(self) -> int:
        return self.frame_bytes - 40


def encode_pair_into(pair: _Pair, destination: bytearray | memoryview, *,
                     max_record_bytes: int = DEFAULT_MAX_RECORD_BYTES) -> int:
    """Validate then copy a complete pair into a preallocated charged buffer.

    Returns bytes written. No resize or allocation of a full frame occurs; all
    structural checks finish before any destination byte is changed.
    """
    maximum = maximum_frame_bytes(max_record_bytes)
    for value, largest in ((pair.sample_index, 0xFFFFFFFF),
                           (pair.original_pair_index, 0xFFFFFFFFFFFFFFFF)):
        if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= largest:
            raise InputError("pair identity exceeds its unsigned integer width")
    lines = tuple(_byte_view(line) for line in (*pair.r1.lines, *pair.r2.lines))
    lengths = tuple(len(line) for line in lines)
    frame_bytes = FRAME_HEADER_BYTES + sum(lengths)
    if frame_bytes > maximum:
        raise InputError("pair frame exceeds the configured maximum frame size")
    _validate_lines(lines, max_record_bytes, InputError)
    target = _byte_view(destination)
    if target.readonly:
        raise ConfigurationError("pair frame destination must be writable")
    if len(target) < frame_bytes:
        raise ResourceError("pair frame destination is smaller than the complete frame")
    _HEADER.pack_into(target, 0, frame_bytes, pair.sample_index, pair.original_pair_index, *lengths)
    offset = FRAME_HEADER_BYTES
    for line in lines:
        target[offset:offset + len(line)] = line
        offset += len(line)
    return frame_bytes


def decode_pair_view(frame: bytes | bytearray | memoryview, *,
                     max_record_bytes: int = DEFAULT_MAX_RECORD_BYTES) -> DecodedPairView:
    """Validate one complete PYPAIR1 frame, then borrow its eight line payloads."""
    maximum = maximum_frame_bytes(max_record_bytes)
    arena = _byte_view(frame).toreadonly()
    if len(arena) < FRAME_HEADER_BYTES:
        raise IntegrityError("pair frame header is truncated")
    if len(arena) > maximum:
        raise IntegrityError("pair frame exceeds the configured maximum frame size")
    frame_bytes, sample_index, ordinal, *lengths = _HEADER.unpack_from(arena)
    if frame_bytes != len(arena):
        raise IntegrityError("pair frame declared length disagrees with its bounded payload")
    offset = FRAME_HEADER_BYTES
    ranges = []
    for length in lengths:
        if length > frame_bytes - offset:
            raise IntegrityError("pair frame line length exceeds its remaining payload")
        ranges.append((offset, offset + length))
        offset += length
    if offset != frame_bytes:
        raise IntegrityError("pair frame contains trailing body bytes")
    lines = tuple(arena[start:end] for start, end in ranges)
    _validate_lines(lines, max_record_bytes, IntegrityError)
    return DecodedPairView(sample_index, ordinal,
                           DecodedRecordView(arena, tuple(ranges[:4])),
                           DecodedRecordView(arena, tuple(ranges[4:])), arena)
