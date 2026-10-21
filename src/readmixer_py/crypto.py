"""Schema-1 cryptographic transcripts and ephemeral operation keys.

Only ``OperationKeys.new`` is a production constructor. Deterministic roots are
confined to the explicitly named test seam; keys have no persistence format.
"""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
import os
import struct

from Crypto.Cipher import AES
from Crypto.Hash import SHA256
from Crypto.Protocol.KDF import HKDF

from .errors import ConfigurationError, ResourceError

_PREFIX = b"RMPY1\x00"
_SALT = b"readmixer-python/schema1/hkdf-sha256"
_ROOT_DOMAINS = frozenset(("root/shuffle", "root/rank", "root/tag", "root/scratch"))
_MAX_LOGICAL_IDENTITY = 4096


def _uint(value: int, width: int, label: str) -> bytes:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value < 1 << (8 * width):
        raise ConfigurationError(f"{label} must fit its unsigned integer width")
    return value.to_bytes(width, "big")


def _bytes(value: bytes, width: int | None, label: str) -> bytes:
    if not isinstance(value, bytes) or (width is not None and len(value) != width):
        raise ConfigurationError(f"{label} has an invalid byte width or type")
    return value


def _ascii(value: str, label: str) -> bytes:
    if not isinstance(value, str):
        raise ConfigurationError(f"{label} must be ASCII text")
    try:
        return value.encode("ascii")
    except UnicodeEncodeError as exc:
        raise ConfigurationError(f"{label} must be ASCII text") from exc


def context(domain: str, fields: Sequence[bytes]) -> bytes:
    """Encode the length-delimited Plan 05 Context transcript exactly."""
    domain_bytes = _ascii(domain, "context domain")
    if (not isinstance(fields, Sequence) or isinstance(fields, (bytes, bytearray, str))
            or len(fields) > 65535 or len(domain_bytes) > 65535):
        raise ConfigurationError("context domain or field count exceeds unsigned-16-bit width")
    chunks = [_PREFIX, struct.pack(">H", len(domain_bytes)), domain_bytes,
              struct.pack(">H", len(fields))]
    for field in fields:
        _bytes(field, None, "context field")
        if len(field) > 0xFFFFFFFF:
            raise ConfigurationError("context field exceeds unsigned-32-bit width")
        chunks.extend((struct.pack(">I", len(field)), field))
    return b"".join(chunks)


def encode_path(path: tuple[int, ...]) -> bytes:
    """Encode the virtual root or a bounded numeric bucket path."""
    if not isinstance(path, tuple) or len(path) > 32:
        raise ConfigurationError("bucket path must be a tuple with at most 32 components")
    if any(isinstance(item, bool) or not isinstance(item, int) or not 0 <= item <= 63
           for item in path):
        raise ConfigurationError("bucket path component must be in 0 through 63")
    return bytes((len(path), *path))


def _validate_path(encoded: bytes) -> None:
    if not encoded or encoded[0] > 32 or len(encoded) != encoded[0] + 1 or any(c > 63 for c in encoded[1:]):
        raise ConfigurationError("encoded bucket path is malformed")


def _decode_context(encoded: bytes) -> tuple[str, tuple[bytes, ...]]:
    _bytes(encoded, None, "logical identity")
    if len(encoded) > _MAX_LOGICAL_IDENTITY or not encoded.startswith(_PREFIX):
        raise ConfigurationError("logical identity is oversized or has an invalid context prefix")
    offset = len(_PREFIX)

    def take(width: int) -> bytes:
        nonlocal offset
        if width > len(encoded) - offset:
            raise ConfigurationError("logical identity context is truncated")
        result = encoded[offset:offset + width]
        offset += width
        return result

    domain_length = int.from_bytes(take(2), "big")
    try:
        domain = take(domain_length).decode("ascii")
    except UnicodeDecodeError as exc:
        raise ConfigurationError("logical identity domain must be ASCII") from exc
    count = int.from_bytes(take(2), "big")
    # Every field needs a four-byte length, even an empty field. Check before
    # constructing the bounded tuple, rather than trusting an attacker count.
    if count > (len(encoded) - offset) // 4:
        raise ConfigurationError("logical identity context has an invalid field count")
    fields = tuple(take(int.from_bytes(take(4), "big")) for _ in range(count))
    if offset != len(encoded):
        raise ConfigurationError("logical identity context has trailing bytes")
    return domain, fields


def validate_logical_identity(purpose: int, logical_identity: bytes) -> None:
    """Reject unknown purposes and malformed scheduler-owned context fields."""
    _uint(purpose, 1, "segment purpose")
    domains = {1: ("segment/pairs", (None, 4, 8)),
               2: ("segment/observations", (None, 8)),
               3: ("segment/sort", (None, 4, 8)),
               4: ("segment/registry", (16, 8, 8)),
               5: ("segment/observations", (None, 8))}
    if purpose not in domains:
        raise ConfigurationError("unknown segment purpose")
    domain, fields = _decode_context(logical_identity)
    expected_domain, widths = domains[purpose]
    if domain != expected_domain or len(fields) != len(widths):
        raise ConfigurationError("segment purpose and logical identity context disagree")
    for field, width in zip(fields, widths):
        _bytes(field, width, "logical identity field")
    if purpose == 1:
        _validate_path(fields[0])
    elif purpose in (2, 3, 5):
        try:
            fields[0].decode("ascii")
        except UnicodeDecodeError as exc:
            raise ConfigurationError("logical identity label must be ASCII") from exc


def derive(key32: bytes, domain: str, fields: Sequence[bytes]) -> bytes:
    """Derive a key only for an exactly validated schema-1 purpose transcript."""
    _bytes(key32, 32, "derivation key")
    _ascii(domain, "context domain")
    if (not isinstance(fields, Sequence) or isinstance(fields, (bytes, bytearray, str))
            or len(fields) > 65535):
        raise ConfigurationError("key derivation fields must be a bounded byte sequence")
    for field in fields:
        _bytes(field, None, "key derivation field")
    if domain in _ROOT_DOMAINS:
        valid = len(fields) == 0
    elif domain == "shuffle/bucket":
        valid = len(fields) == 1
        if valid:
            _validate_path(fields[0])
    elif domain == "shuffle/rank":
        valid = len(fields) == 3
        if valid:
            _validate_path(fields[0])
            _bytes(fields[1], 8, "rank pair count")
            _bytes(fields[2], 1, "rank attempt")
            valid = fields[2][0] <= 7
    elif domain == "scratch/segment":
        valid = len(fields) == 5
        if valid:
            for field, width in zip(fields[:4], (16, 16, 32, 1)):
                _bytes(field, width, "segment key field")
            validate_logical_identity(fields[3][0], fields[4])
    else:
        valid = False
    if not valid:
        raise ConfigurationError("unknown key derivation domain or malformed fields")
    return HKDF(key32, 32, _SALT, SHA256, context=context(domain, fields))


def pairs_identity(path: tuple[int, ...], producer: int, ordinal: int) -> bytes:
    return context("segment/pairs", [encode_path(path), _uint(producer, 4, "producer"),
                                     _uint(ordinal, 8, "segment ordinal")])


def sort_identity(sort_purpose: str, level: int, ordinal: int) -> bytes:
    encoded = context("segment/sort", [_ascii(sort_purpose, "sort purpose"),
                                      _uint(level, 4, "sort level"), _uint(ordinal, 8, "run ordinal")])
    validate_logical_identity(3, encoded)
    return encoded


def registry_identity(ledger_id: bytes, page: int, generation: int) -> bytes:
    return context("segment/registry", [_bytes(ledger_id, 16, "ledger identity"),
                                        _uint(page, 8, "page"), _uint(generation, 8, "generation")])


def observations_identity(stage: str, ordinal: int) -> bytes:
    encoded = context("segment/observations", [_ascii(stage, "observation stage"),
                                             _uint(ordinal, 8, "observation ordinal")])
    validate_logical_identity(5, encoded)
    return encoded


@dataclass(frozen=True, slots=True, repr=False, init=False)
class OperationKeys:
    """Four independent ephemeral roots and a separately drawn public run ID."""

    run_id: bytes
    shuffle_root: bytes
    rank_root: bytes
    tag_root: bytes
    scratch_root: bytes

    def __init__(self) -> None:
        raise ConfigurationError("operation keys must be created with OperationKeys.new")

    def __repr__(self) -> str:
        return "OperationKeys(<ephemeral keys redacted>)"

    @classmethod
    def new(cls) -> OperationKeys:
        try:
            root, run_id = os.urandom(32), os.urandom(16)
        except (OSError, RuntimeError) as exc:
            raise ResourceError("operating-system entropy is unavailable") from exc
        if not isinstance(root, bytes) or len(root) != 32 or not isinstance(run_id, bytes) or len(run_id) != 16:
            raise ResourceError("operating-system entropy returned an invalid draw")
        return cls._from_test(root, run_id)

    @classmethod
    def _from_test(cls, root: bytes, run_id: bytes) -> OperationKeys:
        """Test-only deterministic seam; never expose through config, CLI or env."""
        _bytes(root, 32, "operation root")
        _bytes(run_id, 16, "run identifier")
        instance = object.__new__(cls)
        object.__setattr__(instance, "run_id", run_id)
        for name in ("shuffle", "rank", "tag", "scratch"):
            object.__setattr__(instance, f"{name}_root", derive(root, f"root/{name}", []))
        return instance

    def bucket_key(self, path: tuple[int, ...]) -> bytes:
        return derive(self.shuffle_root, "shuffle/bucket", [encode_path(path)])

    def rank_key(self, path: tuple[int, ...], pair_count: int, attempt: int) -> bytes:
        return derive(self.rank_root, "shuffle/rank", [encode_path(path),
                      _uint(pair_count, 8, "rank pair count"), _uint(attempt, 1, "rank attempt")])

    def tag(self, sample_index: int, original_pair_index: int) -> str:
        message = b"RMP1" + _uint(sample_index, 4, "sample index") + _uint(original_pair_index, 8, "pair ordinal")
        return AES.new(self.tag_root, AES.MODE_ECB).encrypt(message).hex()

    def segment_key(self, segment_id: bytes, generation_salt: bytes, purpose: int,
                    logical_identity: bytes) -> bytes:
        return derive(self.scratch_root, "scratch/segment", [self.run_id, segment_id,
                      generation_salt, _uint(purpose, 1, "segment purpose"), logical_identity])


class BulkTagger:
    """One admitted coordinator-owned AES tag window.

    ``tags`` borrows an already admitted list of integer identity tuples and
    returns this object's reusable list of hexadecimal tags. Consume that list
    before the next call or ``close``: both invalidate the previous window.
    Neither identity tuples nor views into a terminal arena are retained.

    The lease covers native AES state, both packed identity/ciphertext buffers,
    the bounded identity-list representation and hexadecimal rendering metadata.
    Record bodies remain in the caller's terminal and bundle-pair leases.
    """

    MAX_BATCH_SIZE = 4096
    _NATIVE_BYTES = 16 << 10
    _METADATA_PER_PAIR = 384
    __slots__ = ('_batch_size', '_lease', '_cipher', '_plain', '_encrypted', '_tags', '_busy')

    def __init__(self, keys: OperationKeys, budget, *, batch_size: int = MAX_BATCH_SIZE):
        if type(batch_size) is not int or not 1 <= batch_size <= self.MAX_BATCH_SIZE:
            raise ConfigurationError('tag batch size must be in 1 through 4096')
        if not isinstance(keys, OperationKeys):
            raise ConfigurationError('bulk tags require operation keys')
        self._batch_size = batch_size
        self._cipher = self._plain = self._encrypted = None
        self._tags = []
        self._busy = False
        metadata = 4096 + self._METADATA_PER_PAIR * batch_size
        self._lease = budget.acquire('bulk-tags', self._NATIVE_BYTES + 32 * batch_size + metadata,
                                     metadata_bytes=metadata)
        try:
            self._plain = bytearray(16 * batch_size)
            self._encrypted = bytearray(16 * batch_size)
            self._cipher = AES.new(keys.tag_root, AES.MODE_ECB)
        except BaseException:
            self.close()
            raise

    @property
    def batch_size(self) -> int:
        return self._batch_size

    def __repr__(self) -> str:
        return f'BulkTagger(batch_size={self._batch_size}, keys=<redacted>)'

    def tags(self, identities: list[tuple[int, int]]) -> list[str]:
        if self._lease.released:
            raise ResourceError('bulk tag generator is closed')
        if self._busy:
            raise ResourceError('bulk tag workspace is already borrowed')
        if type(identities) is not list or len(identities) > self._batch_size:
            raise ConfigurationError('tag identities must be a bounded list')
        # Validate the entire window before overwriting any previous result.
        for identity in identities:
            if type(identity) is not tuple or len(identity) != 2:
                raise ConfigurationError('tag identity must be an integer pair')
            _uint(identity[0], 4, 'sample index')
            _uint(identity[1], 8, 'pair ordinal')
        self._busy = True
        self._tags.clear()
        try:
            length = 16 * len(identities)
            for position, (sample, ordinal) in enumerate(identities):
                struct.pack_into('>4sIQ', self._plain, 16 * position, b'RMP1', sample, ordinal)
            if length:
                # Explicit output avoids another ciphertext allocation, and each
                # view is at most 64 KiB even for the maximum-size window.
                with memoryview(self._plain)[:length] as source, memoryview(self._encrypted)[:length] as target:
                    self._cipher.encrypt(source, output=target)
                    for offset in range(0, length, 16):
                        self._tags.append(target[offset:offset + 16].hex())
            return self._tags
        except BaseException:
            self._tags.clear()
            raise
        finally:
            self._busy = False

    def close(self) -> None:
        self._tags.clear()
        self._cipher = self._plain = self._encrypted = None
        self._lease.release()

    def __enter__(self):
        if self._lease.released:
            raise ResourceError('bulk tag generator is closed')
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()
