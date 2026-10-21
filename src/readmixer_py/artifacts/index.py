"""Frozen 64-byte RMPYIDX1 headers and physical-order 40-byte entries."""

from contextlib import AbstractContextManager
from dataclasses import dataclass
import struct

from ..config import U64_MAX
from ..errors import ConfigurationError, IntegrityError, ResourceError, UnsupportedFormatError

HEADER_STRUCT = struct.Struct("<8sII16sQQQQ")
ENTRY_STRUCT = struct.Struct("<IIQQQQ")
HEADER_BYTES, ENTRY_BYTES = 64, 40
MAGIC = b"RMPYIDX1"


@dataclass(frozen=True, slots=True)
class IndexHeader:
    run_id: bytes
    bundle_index: int
    first_secure_pair_index: int
    entry_count: int


@dataclass(frozen=True, slots=True)
class IndexEntry:
    sample_index: int
    original_pair_index: int
    r1_virtual_offset: int
    r2_virtual_offset: int
    key_virtual_offset: int


def _uint(value, maximum=U64_MAX, *, error=IntegrityError):
    if type(value) is not int or not 0 <= value <= maximum:
        raise error("mapping index integer exceeds its unsigned field width")
    return value


def index_bytes(entry_count: int) -> int:
    _uint(entry_count)
    if not entry_count or entry_count > (U64_MAX - HEADER_BYTES) // ENTRY_BYTES:
        raise IntegrityError("mapping index count or physical length is impossible")
    return HEADER_BYTES + ENTRY_BYTES * entry_count


def encode_header(header: IndexHeader) -> bytes:
    if not isinstance(header, IndexHeader) or not isinstance(header.run_id, bytes) or len(header.run_id) != 16:
        raise ConfigurationError("mapping index requires a sixteen-byte run identifier")
    for value in (header.bundle_index, header.first_secure_pair_index, header.entry_count):
        _uint(value, error=ConfigurationError)
    index_bytes(header.entry_count)
    if header.first_secure_pair_index > U64_MAX - header.entry_count:
        raise ConfigurationError("mapping index secured range exceeds u64")
    return HEADER_STRUCT.pack(MAGIC, 1, ENTRY_BYTES, header.run_id, header.bundle_index,
                              header.first_secure_pair_index, header.entry_count, 0)


def encode_entry(entry: IndexEntry) -> bytes:
    if not isinstance(entry, IndexEntry):
        raise ConfigurationError("mapping index entry has the wrong type")
    _uint(entry.sample_index, (1 << 32) - 1, error=ConfigurationError)
    for value in (entry.original_pair_index, entry.r1_virtual_offset, entry.r2_virtual_offset, entry.key_virtual_offset):
        _uint(value, error=ConfigurationError)
    return ENTRY_STRUCT.pack(entry.sample_index, 0, entry.original_pair_index,
                             entry.r1_virtual_offset, entry.r2_virtual_offset, entry.key_virtual_offset)


def decode_header(data: bytes) -> IndexHeader:
    if len(data) != HEADER_BYTES:
        raise IntegrityError("mapping index header is truncated")
    magic, version, width, run_id, bundle, first, count, reserved = HEADER_STRUCT.unpack(data)
    if magic != MAGIC or version != 1:
        raise UnsupportedFormatError("unsupported mapping index format or version")
    if width != ENTRY_BYTES or reserved:
        raise IntegrityError("mapping index width or reserved field is invalid")
    index_bytes(count)
    if first > U64_MAX - count:
        raise IntegrityError("mapping index secured range exceeds u64")
    return IndexHeader(run_id, bundle, first, count)


def decode_entry(data: bytes, *, sample_counts=None) -> IndexEntry:
    if len(data) != ENTRY_BYTES:
        raise IntegrityError("mapping index entry is truncated")
    sample, reserved, original, r1, r2, key = ENTRY_STRUCT.unpack(data)
    if reserved:
        raise IntegrityError("mapping index entry reserved field is nonzero")
    if sample_counts is not None and (sample >= len(sample_counts) or original >= sample_counts[sample]):
        raise IntegrityError("mapping index identity is outside the declared source domain")
    return IndexEntry(sample, original, r1, r2, key)


class IndexReader(AbstractContextManager):
    """One bounded forward pass; successful context exit checks all entries."""

    def __init__(self, stream, *, run_id=None, bundle_index=None, first_secure_pair_index=None,
                 entry_count=None, sample_counts=None, max_bytes=5_000_000_000):
        self.stream = stream
        self.header = decode_header(self._read_exact(HEADER_BYTES))
        for name, expected in (("run_id", run_id), ("bundle_index", bundle_index),
                               ("first_secure_pair_index", first_secure_pair_index), ("entry_count", entry_count)):
            if expected is not None and getattr(self.header, name) != expected:
                raise IntegrityError("mapping index header disagrees with the manifest")
        if index_bytes(self.header.entry_count) > max_bytes:
            raise ResourceError("mapping index exceeds its physical byte allowance")
        self.sample_counts = sample_counts
        self.position = 0
        self._previous = None
        self._finished = False
        self._failed = False

    def _read_exact(self, size):
        data = bytearray()
        while len(data) < size:
            chunk = self.stream.read(size - len(data))
            if not chunk:
                raise IntegrityError("mapping index is truncated")
            if len(chunk) > size - len(data):
                raise IntegrityError("mapping index reader exceeded its bounded request")
            data.extend(chunk)
        return bytes(data)

    def __iter__(self):
        return self

    def __next__(self):
        if self._failed:
            raise IntegrityError("mapping index reader previously failed validation")
        try:
            return self._next()
        except StopIteration:
            raise
        except BaseException:
            self._failed = True
            raise

    def _next(self):
        if self.position == self.header.entry_count:
            if not self._finished:
                if self.stream.read(1):
                    raise IntegrityError("mapping index has trailing bytes")
                self._finished = True
            raise StopIteration
        entry = decode_entry(self._read_exact(ENTRY_BYTES), sample_counts=self.sample_counts)
        offsets = (entry.r1_virtual_offset, entry.r2_virtual_offset, entry.key_virtual_offset)
        if self._previous is not None and any(value <= previous for value, previous in zip(offsets, self._previous)):
            raise IntegrityError("mapping index lane starts do not increase strictly")
        self._previous = offsets
        self.position += 1
        return entry

    def finish(self):
        for _ in self:
            pass

    def __exit__(self, exc_type, exc, traceback):
        if exc is None:
            self.finish()
