"""Canonical twelve-field private mapping rows; no generic CSV coercions."""

from dataclasses import dataclass
import re

from ..config import MAX_RECORD_BYTES, U64_MAX
from ..errors import ConfigurationError, InputError, IntegrityError, ResourceError
from ..fastq import header_identity

KEY_HEADER = (b"sample_index,original_pair_index,secure_pair_index,pair_tag,"
              b"r1_uncompressed_offset,r1_record_bytes,r2_uncompressed_offset,r2_record_bytes,"
              b"r1_header_hex,r1_plus_hex,r2_header_hex,r2_plus_hex\n")
HEADER = KEY_HEADER
_DECIMAL = re.compile(rb"(?:0|[1-9][0-9]*)")
_HEX = re.compile(rb"[0-9a-f]*")
_TAG = re.compile(r"[0-9a-f]{32}", re.ASCII)
SECURED_FRAMING_BYTES = 83  # 78-byte secured header, bare plus, four LFs.


@dataclass(frozen=True, slots=True)
class KeyRow:
    sample_index: int
    original_pair_index: int
    secure_pair_index: int
    pair_tag: str
    r1_uncompressed_offset: int
    r1_record_bytes: int
    r2_uncompressed_offset: int
    r2_record_bytes: int
    r1_header: bytes
    r1_plus: bytes
    r2_header: bytes
    r2_plus: bytes


def maximum_key_row_bytes(max_record_bytes: int = 1 << 20) -> int:
    if type(max_record_bytes) is not int or not 1 <= max_record_bytes <= MAX_RECORD_BYTES:
        raise ConfigurationError("invalid original record limit")
    # Metadata occupies at most M-4 bytes per mate. Decimal fields are bounded
    # by one u32 and six u64 widths, plus tag, eleven commas and final LF.
    return 4 * max_record_bytes + 158


def secured_record_limit(max_record_bytes: int = 1 << 20) -> int:
    maximum_key_row_bytes(max_record_bytes)
    return max_record_bytes + SECURED_FRAMING_BYTES - 7


def _validate(row: KeyRow, maximum: int, error=IntegrityError) -> None:
    maximum_key_row_bytes(maximum)
    if not isinstance(row, KeyRow):
        raise error("invalid mapping row type")
    for name in ("sample_index", "original_pair_index", "secure_pair_index", "r1_uncompressed_offset",
                 "r1_record_bytes", "r2_uncompressed_offset", "r2_record_bytes"):
        value = getattr(row, name)
        upper = (1 << 32) - 1 if name == "sample_index" else U64_MAX
        if type(value) is not int or not 0 <= value <= upper:
            raise error("mapping integer exceeds its unsigned field width")
    if not isinstance(row.pair_tag, str) or _TAG.fullmatch(row.pair_tag) is None:
        raise error("mapping pair tag is not canonical lowercase hexadecimal")
    for role in (1, 2):
        header, plus = getattr(row, f"r{role}_header"), getattr(row, f"r{role}_plus")
        if not isinstance(header, bytes) or not isinstance(plus, bytes):
            raise error("mapping metadata must be opaque bytes")
        if not header.startswith(b"@") or not plus.startswith(b"+") or b"\n" in header or b"\n" in plus:
            raise error("mapping metadata contains invalid markers or line endings")
        record_bytes = getattr(row, f"r{role}_record_bytes")
        if record_bytes < SECURED_FRAMING_BYTES or (record_bytes - SECURED_FRAMING_BYTES) % 2:
            raise error("mapping secured record length is impossible")
        restored = record_bytes - SECURED_FRAMING_BYTES + len(header) + len(plus) + 4
        if restored > maximum:
            raise error("mapping metadata and body exceed the original record limit")
        if getattr(row, f"r{role}_uncompressed_offset") > U64_MAX - record_bytes:
            raise error("mapping record endpoint exceeds u64")
    try:
        base1, role1 = header_identity(row.r1_header)
        base2, role2 = header_identity(row.r2_header)
    except InputError as exc:
        raise error("mapping original headers have invalid mate identities") from exc
    if base1 != base2 or role1 not in (None, 1) or role2 not in (None, 2):
        raise error("mapping original mate identities disagree")


def _fields(row):
    return (row.sample_index, row.original_pair_index, row.secure_pair_index, row.pair_tag,
            row.r1_uncompressed_offset, row.r1_record_bytes, row.r2_uncompressed_offset, row.r2_record_bytes)


def key_row_size(row: KeyRow, *, max_record_bytes: int = 1 << 20) -> int:
    _validate(row, max_record_bytes, ConfigurationError)
    return (sum(len(str(value)) for value in _fields(row)) + 12
            + 2 * sum(len(value) for value in (row.r1_header, row.r1_plus, row.r2_header, row.r2_plus)))


def encode_key_row(row: KeyRow, *, max_record_bytes: int = 1 << 20) -> bytes:
    size = key_row_size(row, max_record_bytes=max_record_bytes)
    encoded = (",".join([*(str(value) for value in _fields(row)), row.r1_header.hex(),
                         row.r1_plus.hex(), row.r2_header.hex(), row.r2_plus.hex()]) + "\n").encode("ascii")
    if len(encoded) != size:
        raise IntegrityError("mapping row sizing invariant failed")
    return encoded


def parse_key_row(line: bytes, *, max_record_bytes: int = 1 << 20,
                  sample_counts=None, expected_secure_pair_index=None) -> KeyRow:
    maximum = maximum_key_row_bytes(max_record_bytes)
    if not isinstance(line, bytes) or len(line) > maximum:
        raise ResourceError("mapping row exceeds its encoded bound")
    if not line.endswith(b"\n") or line.count(b"\n") != 1:
        raise IntegrityError("mapping row requires exactly one terminating LF")
    fields = line[:-1].split(b",")
    if len(fields) != 12:
        raise IntegrityError("mapping row must have exactly twelve fields")
    integers = []
    for position in (0, 1, 2, 4, 5, 6, 7):
        field = fields[position]
        if len(field) > (10 if position == 0 else 20) or _DECIMAL.fullmatch(field) is None:
            raise IntegrityError("mapping integer is not canonical unsigned decimal")
        integers.append(int(field))
    if len(fields[3]) != 32 or re.fullmatch(rb"[0-9a-f]{32}", fields[3]) is None:
        raise IntegrityError("mapping tag is invalid")
    metadata = []
    for field in fields[8:]:
        if len(field) > 2 * max_record_bytes or len(field) % 2 or _HEX.fullmatch(field) is None:
            raise IntegrityError("mapping hexadecimal metadata is malformed or oversized")
        metadata.append(bytes.fromhex(field.decode("ascii")))
    row = KeyRow(*integers[:3], fields[3].decode("ascii"), *integers[3:], *metadata)
    _validate(row, max_record_bytes)
    if sample_counts is not None:
        if row.sample_index >= len(sample_counts) or row.original_pair_index >= sample_counts[row.sample_index]:
            raise IntegrityError("mapping identity is outside the declared source domain")
    if expected_secure_pair_index is not None and row.secure_pair_index != expected_secure_pair_index:
        raise IntegrityError("mapping secured ordinal is not the expected physical position")
    return row


def row_from_pair(pair, tag, secure_index, r1_offset, r1_length, r2_offset, r2_length) -> KeyRow:
    return KeyRow(pair.sample_index, pair.original_pair_index, secure_index, tag,
                  r1_offset, r1_length, r2_offset, r2_length,
                  bytes(pair.r1.header), bytes(pair.r1.plus), bytes(pair.r2.header), bytes(pair.r2.plus))
