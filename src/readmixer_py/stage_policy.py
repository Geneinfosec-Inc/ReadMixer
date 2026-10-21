"""Closed production policies for bounded sort and terminal worker jobs.

Only fixed scalars and task-specific derived keys cross a worker channel. No
callable, root key, path on disk, or publication capability is represented here.
"""
from dataclasses import dataclass, field
from enum import IntEnum
import struct

from .errors import ConfigurationError, IntegrityError
from .paircodec import decode_pair_view

MAX_POLICY_BYTES = 1024
_HEADER = struct.Struct('>BBIIQBB')
_RECORD = struct.Struct('>I')


class SortPolicy(IntEnum):
    ORIGINAL_FRAMES = 1
    TAG_OBSERVATIONS = 2
    COVERAGE_IDENTITIES = 3
    BUCKET_FRAMES = 4
    TERMINAL_RANK = 5


@dataclass(frozen=True, slots=True)
class StagePolicy:
    kind: SortPolicy
    max_record_bytes: int = 1 << 20
    max_frame_bytes: int = 0
    count: int = 0
    path: tuple = ()
    fanout: int = 0
    keys: tuple = field(default=(), repr=False)

    def __post_init__(self):
        if not any(self.kind is kind for kind in SortPolicy):
            raise ConfigurationError('worker sort policy is unsupported')
        if (type(self.max_record_bytes) is not int or not 1 <= self.max_record_bytes <= (0xFFFFFFFF - 40) // 2
                or type(self.max_frame_bytes) is not int or not 0 <= self.max_frame_bytes <= 0xFFFFFFFF
                or type(self.count) is not int or not 0 <= self.count <= 32768
                or type(self.path) is not tuple or len(self.path) > 32
                or any(type(part) is not int or not 0 <= part < 64 for part in self.path)
                or type(self.keys) is not tuple
                or any(type(key) is not bytes or len(key) != 32 for key in self.keys)):
            raise ConfigurationError('worker sort policy exceeds its closed bounds')
        if self.kind is SortPolicy.BUCKET_FRAMES:
            if type(self.fanout) is not int or self.fanout not in (2, 4, 8, 16, 32, 64) or len(self.keys) != 1 or self.count or self.max_frame_bytes:
                raise ConfigurationError('bucket policy requires one exact routing key and fanout')
        elif self.kind is SortPolicy.TERMINAL_RANK:
            if self.fanout or len(self.keys) != 8 or not self.count or not 48 <= self.max_frame_bytes <= 2 * self.max_record_bytes + 40:
                raise ConfigurationError('terminal policy requires eight rank keys and exact statistics')
        elif self.fanout or self.keys or self.path or self.count or self.max_frame_bytes:
            raise ConfigurationError('ordinary sort policy contains unrelated authority')

    @classmethod
    def terminal(cls, path, count, max_record_bytes, max_frame_bytes, rank_keys):
        return cls(SortPolicy.TERMINAL_RANK, max_record_bytes, max_frame_bytes, count, path, 0, rank_keys)

    @classmethod
    def bucket(cls, path, fanout, key, max_record_bytes):
        return cls(SortPolicy.BUCKET_FRAMES, max_record_bytes, path=path, fanout=fanout, keys=(key,))

    def key(self, record):
        if self.kind is SortPolicy.TAG_OBSERVATIONS:
            if len(record) != 24:
                raise IntegrityError('tag observation has an invalid width')
            return bytes(record[:16])
        if self.kind is SortPolicy.COVERAGE_IDENTITIES:
            if len(record) != 12:
                raise IntegrityError('coverage identity has an invalid width')
            return bytes(record)
        if self.kind not in (SortPolicy.ORIGINAL_FRAMES, SortPolicy.BUCKET_FRAMES):
            raise ConfigurationError('terminal policy is not a record comparison')
        pair = decode_pair_view(record, max_record_bytes=self.max_record_bytes)
        identity = pair.sample_index, pair.original_pair_index
        if self.kind is SortPolicy.BUCKET_FRAMES:
            from .shuffle import bucket_index
            return (-bucket_index(self.keys[0], *identity, self.fanout), *identity)
        return identity


def encode_policy(policy):
    if type(policy) is not StagePolicy:
        raise ConfigurationError('worker needs an explicit closed stage policy')
    policy.__post_init__()
    return (_HEADER.pack(1, policy.kind, policy.max_record_bytes, policy.max_frame_bytes,
                         policy.count, len(policy.path), policy.fanout)
            + bytes(policy.path) + b''.join(policy.keys))


def decode_policy(data):
    if type(data) is not bytes or not _HEADER.size <= len(data) <= MAX_POLICY_BYTES:
        raise IntegrityError('worker policy envelope has an invalid width')
    try:
        version, number, record, frame, count, depth, fanout = _HEADER.unpack_from(data)
        if version != 1 or depth > 32:
            raise ValueError
        kind = SortPolicy(number)
        key_count = 8 if kind is SortPolicy.TERMINAL_RANK else 1 if kind is SortPolicy.BUCKET_FRAMES else 0
        if len(data) != _HEADER.size + depth + 32 * key_count:
            raise ValueError
        offset = _HEADER.size + depth
        return StagePolicy(kind, record, frame, count, tuple(data[_HEADER.size:offset]), fanout,
                           tuple(data[pos:pos + 32] for pos in range(offset, len(data), 32)))
    except (ValueError, ConfigurationError):
        raise IntegrityError('worker policy contains invalid fields') from None


def record_count_limit(policy, byte_count):
    if policy is None:
        return byte_count // 48
    if policy.kind is SortPolicy.TERMINAL_RANK:
        return policy.count
    minimum = {SortPolicy.ORIGINAL_FRAMES: 52, SortPolicy.BUCKET_FRAMES: 52,
               SortPolicy.TAG_OBSERVATIONS: 28, SortPolicy.COVERAGE_IDENTITIES: 16}[policy.kind]
    return byte_count // minimum


def execute_policy(payload, policy, *, checkpoint=lambda: None):
    if policy is None:
        from .worker_bootstrap import sort_identity_frames
        return sort_identity_frames(payload, checkpoint=checkpoint)
    if policy.kind is SortPolicy.TERMINAL_RANK:
        return _terminal(payload, policy, checkpoint)
    view = memoryview(payload)
    rows = []
    offset = 0
    while offset < len(view):
        if len(rows) % 256 == 0:
            checkpoint()
        if len(view) - offset < 4:
            raise IntegrityError('worker sorted record prefix is truncated')
        length, = _RECORD.unpack_from(view, offset)
        end = offset + 4 + length
        if end > len(view):
            raise IntegrityError('worker sorted record payload is truncated')
        value = policy.key(view[offset + 4:end])
        rows.append((value, offset, end))
        offset = end
    checkpoint()
    # Stable native sort preserves equal keys in canonical source/run order.
    rows.sort(key=lambda row: row[0])
    checkpoint()
    result = bytearray(len(payload))
    offset = 0
    for index, (_value, begin, end) in enumerate(rows):
        if index % 256 == 0:
            checkpoint()
        result[offset:offset + end - begin] = view[begin:end]
        offset += end - begin
    return bytes(result), len(rows)


def _terminal(payload, policy, checkpoint):
    import numpy as np
    from .shuffle import terminal_permutation
    from .resources import MAX_TERMINAL_ARENA
    if len(payload) > MAX_TERMINAL_ARENA:
        raise IntegrityError('terminal arena exceeds its canonical bound')
    view = memoryview(payload)
    offsets = np.empty(policy.count + 1, dtype='<u8')
    cursor = maximum = 0
    previous = None
    for index in range(policy.count):
        checkpoint()
        if len(view) - cursor < 4:
            raise IntegrityError('terminal frame prefix is truncated')
        length = struct.unpack_from('<I', view, cursor)[0]
        if not 48 <= length <= min(policy.max_frame_bytes, len(view) - cursor):
            raise IntegrityError('terminal frame exceeds its admitted bound')
        pair = decode_pair_view(view[cursor:cursor + length], max_record_bytes=policy.max_record_bytes)
        identity = pair.sample_index, pair.original_pair_index
        if previous is not None and identity <= previous:
            raise IntegrityError('terminal identities are not strictly canonical')
        del pair
        previous = identity
        offsets[index] = cursor
        cursor += length
        maximum = max(maximum, length)
    offsets[policy.count] = cursor
    if cursor != len(view) or maximum != policy.max_frame_bytes:
        raise IntegrityError('terminal statistics disagree with the complete input')
    class RankKeys:
        def rank_key(self, path, count, attempt):
            if path != policy.path or count != policy.count:
                raise IntegrityError('terminal rank context changed')
            return policy.keys[attempt]
    def cancelled():
        checkpoint()
        return False
    permutation = terminal_permutation(RankKeys(), policy.path, policy.count, cancel=cancelled)
    result = bytearray(len(payload))
    cursor = 0
    for index in permutation:
        checkpoint()
        begin, end = int(offsets[index]), int(offsets[index + 1])
        result[cursor:cursor + end - begin] = view[begin:end]
        cursor += end - begin
    return bytes(result), policy.count
