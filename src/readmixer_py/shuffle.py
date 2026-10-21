"""Canonical whole-pair shuffle with protected, bounded scratch.

Direct scatter admits complete writer sets; sorter scatter remains the smaller
working-set fallback. An invocation's shared workers rank admitted terminals,
while the coordinator retains canonical leaf order, exact identity coverage,
secured ordinals, and bounded AES tag windows. Serial execution uses the same
bucket topology, rank transcript, and terminal limits.
"""
from __future__ import annotations

from contextlib import AbstractContextManager, contextmanager
from collections import deque
from dataclasses import dataclass
import hashlib
import itertools
import struct

from Crypto.Cipher import ChaCha20
import numpy as np

from .crypto import BulkTagger, encode_path, pairs_identity
from .errors import CancelledError, ConfigurationError, IntegrityError, RankCollisionError, ResourceError
from .ledger import PagedLedger
from .direct_scatter import DirectScatter
from .paircodec import decode_pair_view, encode_pair_into, maximum_frame_bytes
from .resources import checked_add, checked_multiply, MAX_TERMINAL_ARENA, MAX_TERMINAL_PAIRS, TERMINAL_OVERHEAD
from .sorting import ExternalSorter, IdentityCoverage
from .stage_policy import StagePolicy
from .storage import deserialize_segment_ref, serialize_segment_ref

_IDENTITY = struct.Struct('>IQ')
_FRAME_ID = struct.Struct('<IQ')
_LENGTH = struct.Struct('<I')
_STAT = struct.Struct('>I')


def _check(cancel):
    if cancel is not None and cancel():
        raise CancelledError()


def bucket_index(bucket_key: bytes, sample_index: int, original_pair_index: int, fanout: int) -> int:
    """Plan 04's exact eight-byte BLAKE2b identity routing transcript."""
    if type(bucket_key) is not bytes or len(bucket_key) != 32:
        raise ConfigurationError('bucket key must contain exactly 32 bytes')
    if type(fanout) is not int or not 2 <= fanout <= 64 or fanout & (fanout - 1):
        raise ConfigurationError('bucket fanout must be a power of two in 2..64')
    if (type(sample_index) is not int or not 0 <= sample_index < 1 << 32
            or type(original_pair_index) is not int or not 0 <= original_pair_index < 1 << 64):
        raise ConfigurationError('bucket identity exceeds its unsigned integer width')
    digest = hashlib.blake2b(_IDENTITY.pack(sample_index, original_pair_index),
                             key=bucket_key, digest_size=8).digest()
    return int.from_bytes(digest, 'little') & (fanout - 1)


@dataclass(frozen=True)
class _Limits:
    pairs: int = MAX_TERMINAL_PAIRS
    arena: int = MAX_TERMINAL_ARENA
    overhead: int = TERMINAL_OVERHEAD
    depth: int = 32


def refinement_fanout(pair_count, arena_bytes, terminal_bytes, *, _limits=_Limits()):
    if terminal_bytes <= _limits.overhead:
        raise ResourceError('terminal allowance cannot cover its fixed overhead')
    if type(pair_count) is not int or pair_count < 0 or type(arena_bytes) is not int or arena_bytes < 0:
        raise ConfigurationError('refinement statistics must be unsigned integers')
    work = checked_add(arena_bytes, checked_multiply(128, pair_count))
    requirements = (2, (pair_count + _limits.pairs - 1) // _limits.pairs,
                    (arena_bytes + _limits.arena - 1) // _limits.arena,
                    (work + terminal_bytes - _limits.overhead - 1) // (terminal_bytes - _limits.overhead))
    desired = max(requirements)
    return min(64, 1 << (desired - 1).bit_length())


def _fill_rank_bytes(destination, key, *, chunk_bytes=65536, cancel=None):
    cipher = ChaCha20.new(key=key, nonce=bytes(12))
    zeros = bytes(chunk_bytes)
    for offset in range(0, len(destination), chunk_bytes):
        _check(cancel)
        size = min(chunk_bytes, len(destination) - offset)
        cipher.encrypt(memoryview(zeros)[:size], output=destination[offset:offset + size])


def _has_collision(high, low, permutation, *, window=4096, cancel=None):
    previous = None
    for start in range(0, len(permutation), window):
        _check(cancel)
        selected = permutation[start:start + window]
        high_window, low_window = high[selected], low[selected]
        if previous is not None and previous == (int(high_window[0]), int(low_window[0])):
            return True
        if len(selected) > 1 and np.any((high_window[1:] == high_window[:-1])
                                        & (low_window[1:] == low_window[:-1])):
            return True
        previous = int(high_window[-1]), int(low_window[-1])
    return False


def terminal_permutation(keys, path, pair_count, *, cancel=None, _chunk_bytes=65536, _collision_window=4096):
    """Native 128-bit rank sorting; caller owns the terminal working-set lease."""
    encode_path(path)
    if type(pair_count) is not int or not 0 <= pair_count <= MAX_TERMINAL_PAIRS:
        raise ConfigurationError('terminal pair count exceeds the canonical capacity')
    if (type(_chunk_bytes) is not int or not 1 <= _chunk_bytes <= 65536
            or type(_collision_window) is not int or not 1 <= _collision_window <= 4096):
        raise ConfigurationError('rank processing chunk sizes exceed their fixed bounds')
    try:
        for attempt in range(8):
            _check(cancel)
            storage = np.empty(16 * pair_count, dtype=np.uint8)
            _fill_rank_bytes(memoryview(storage), keys.rank_key(path, pair_count, attempt),
                             chunk_bytes=_chunk_bytes, cancel=cancel)
            words = storage.view('<u8')
            high, low = words[:pair_count], words[pair_count:]
            _check(cancel)
            permutation = np.lexsort((low, high))
            _check(cancel)
            collision = _has_collision(high, low, permutation, window=_collision_window, cancel=cancel)
            del high, low, words, storage
            if not collision:
                return permutation
            del permutation
    except MemoryError as exc:
        raise ResourceError('terminal rank sort exceeded its admitted working set') from exc
    raise RankCollisionError('all eight terminal rank batches contain a repeated 128-bit rank')


@dataclass(frozen=True, slots=True)
class OrderedPair:
    secure_pair_index: int
    sample_index: int
    original_pair_index: int
    frame: memoryview
    tag: str | None = None


@dataclass(frozen=True, slots=True)
class _Shard:
    path: tuple[int, ...]
    ref: object
    max_frame_bytes: int


def _encode_shard(shard):
    return encode_path(shard.path) + _STAT.pack(shard.max_frame_bytes) + serialize_segment_ref(shard.ref)


def _decode_shard(encoded):
    if not encoded or not 1 <= encoded[0] <= 32 or len(encoded) < 1 + encoded[0] + 4:
        raise IntegrityError('shuffle shard descriptor is malformed')
    depth = encoded[0]
    path = tuple(encoded[1:depth + 1])
    try:
        encode_path(path)
    except ConfigurationError as exc:
        raise IntegrityError('shuffle shard path is malformed') from exc
    maximum, = _STAT.unpack_from(encoded, depth + 1)
    ref = deserialize_segment_ref(encoded[depth + 5:])
    if (ref.purpose != 1 or ref.logical_identity != pairs_identity(path, 0, 0)
            or not ref.item_count or not 48 <= maximum <= ref.decoded_bytes):
        raise IntegrityError('shuffle shard descriptor disagrees with its protected segment')
    return _Shard(path, ref, maximum)


class _Frontier:
    def __init__(self, store, cancel):
        self.store = store
        self.ledger = PagedLedger(store, max_record_bytes=8192, cancel=cancel)
        self.consumed = 0
        self.pending = None

    def publish(self, path, writer, maximum, count, encoded_bytes):
        ref = writer.seal()
        self.pending = ref
        if (ref.item_count, ref.decoded_bytes) != (count, encoded_bytes):
            raise IntegrityError('shuffle shard seal differs from its complete frame totals')
        if self.store.mode != 'memory':
            ref = self.store.spill(ref)
            self.pending = ref
        self.ledger.append(_encode_shard(_Shard(path, ref, maximum)))
        self.ledger.flush()
        self.pending = None
        if self.store.mode != 'memory':
            self.store.detach(ref)

    def drop(self, shard):
        self.store.drop(shard.ref)
        self.consumed += 1

    def close(self):
        if self.ledger._closed:
            return
        if self.pending is not None:
            self.store.drop(self.pending, missing_ok=True)
            self.pending = None
        with self.ledger.open_reverse(flush=False, check_cancel=False) as records:
            for position, encoded in enumerate(records):
                if position < self.consumed:
                    continue
                self.store.drop(_decode_shard(encoded).ref, missing_ok=True)
                self.consumed += 1
        self.ledger.close()


class SerialShuffler(AbstractContextManager):
    """Coordinate a canonical permutation with an optional admitted scheduler.

    The historical class name preserves the synchronous reference interface.
    A yielded frame is borrowed from its terminal arena and must be consumed
    before requesting the next pair. Production topology uses fixed Plan 03
    limits and the coordinator's canonical ``terminal_bytes`` allowance.
    """

    def __init__(self, store, *, terminal_bytes, max_record_bytes=1 << 20,
                 sort_run_bytes=8 << 20, cancel=None, scheduler=None, _tag_batch_size=4096,
                 scatter_strategy="auto", scatter_batch_bytes=64 << 20):
        if type(terminal_bytes) is not int or not 0 < terminal_bytes <= 128 << 20:
            raise ConfigurationError('canonical terminal allowance must be in 1..128 MiB')
        if cancel is not None and not callable(cancel):
            raise ConfigurationError('shuffle cancellation must be callable')
        if scatter_strategy not in ('auto', 'direct', 'sorter'):
            raise ConfigurationError('invalid private scatter strategy')
        DirectScatter._configuration(max_record_bytes, scatter_batch_bytes, 0, sort_run_bytes)
        self._limits = _Limits()
        self.max_frame_bytes = maximum_frame_bytes(max_record_bytes)
        if (self.max_frame_bytes > self._limits.arena
                or self.max_frame_bytes + 128 + self._limits.overhead > terminal_bytes):
            raise ResourceError('canonical terminal allowance cannot admit one maximum-size pair')
        if type(sort_run_bytes) is not int or sort_run_bytes < self.max_frame_bytes + 512:
            raise ResourceError('shuffle sort run cannot admit one maximum-size pair and its key')
        self.store, self.keys, self.cancel = store, store.keys, cancel
        self.scheduler, self._tag_batch_size, self._tagger = scheduler, _tag_batch_size, None
        self.terminal_bytes, self.max_record_bytes = terminal_bytes, max_record_bytes
        self.sort_run_bytes = sort_run_bytes
        self.scatter_strategy = "sorter"
        self.scatter_batch_bytes = scatter_batch_bytes
        self._metadata = store.budget.acquire('shuffle-metadata', 65536, metadata_bytes=65536)
        self._carry_lease = self._scatter = None
        try:
            self._carry_lease = store.budget.acquire('shuffle-frame-carry', 2 * self.max_frame_bytes + 4096)
            self._carry = bytearray(self.max_frame_bytes)
            if (scatter_strategy != "sorter" and scheduler is not None and scheduler.process_slots
                    and scheduler.parser_scatter):
                self._scatter = DirectScatter(store, max_record_bytes=max_record_bytes,
                    batch_bytes=scheduler.parser_batch_bytes, sort_run_bytes=sort_run_bytes,
                    cancel=cancel, inventory_only=True, producer_count=scheduler.process_slots)
                self.scatter_strategy, self.scatter_batch_bytes = "direct", scheduler.parser_batch_bytes
            if self._scatter is None and scatter_strategy != "sorter":
                target = scatter_batch_bytes
                while True:
                    if DirectScatter.can_admit(store, max_record_bytes=max_record_bytes,
                                               batch_bytes=target, sort_run_bytes=sort_run_bytes):
                        self._scatter = DirectScatter(store, max_record_bytes=max_record_bytes,
                                                      batch_bytes=target, sort_run_bytes=sort_run_bytes,
                                                      cancel=cancel)
                        self.scatter_strategy, self.scatter_batch_bytes = "direct", target
                        break
                    if scatter_strategy == "direct" or target == self.max_frame_bytes:
                        break
                    target = max(self.max_frame_bytes, target // 2)
                if self._scatter is None and scatter_strategy == "direct":
                    raise ResourceError('cannot admit the complete direct scatter producer set')
            if self._scatter is None:
                self._scatter = self._sorter((), 64)
        except BaseException as exc:
            if self._carry_lease is not None:
                self._carry_lease.release()
            self._metadata.release()
            if isinstance(exc, (MemoryError, OverflowError)):
                raise ResourceError('cannot allocate the admitted shuffle frame carry') from exc
            raise
        self._frontiers = []
        self._refining_sorter = None
        self._closed = self._started = self._active = self._failed = False
        self.pairs = self.frame_bytes = self.emitted_pairs = self.emitted_mates = 0
        self.terminal_count = self.refinements = self.max_depth = 0
        self._last_path = None
        self._terminal_result = None
        self._coverage = None

    def _check(self):
        if self._closed or self._failed:
            raise ResourceError('shuffle is closed or has already failed')
        _check(self.cancel)

    def finish_ingress(self):
        """Retire a complete producer set before emission working-set admission."""
        self._check()
        if self._started:
            raise ResourceError('shuffle output has already started')
        if isinstance(self._scatter, DirectScatter):
            self._scatter.finish_ingress()

    def _sorter(self, path, fanout):
        policy = StagePolicy.bucket(path, fanout, self.keys.bucket_key(path), self.max_record_bytes)
        return ExternalSorter(self.store, policy=policy, run_bytes=self.sort_run_bytes,
                              max_record_bytes=self.max_frame_bytes, sort_purpose='shuffle-partition',
                              cancel=self.cancel)

    def append_pair(self, pair):
        self._check()
        if self._started:
            raise ResourceError('cannot append after shuffle output begins')
        size = encode_pair_into(pair, self._carry, max_record_bytes=self.max_record_bytes)
        self.append_frame(memoryview(self._carry)[:size])

    def append_frame(self, frame):
        self._check()
        if self._started:
            raise ResourceError('cannot append after shuffle output begins')
        decoded = decode_pair_view(frame, max_record_bytes=self.max_record_bytes)
        count = checked_add(self.pairs, 1)
        encoded_bytes = checked_add(self.frame_bytes, decoded.frame_bytes)
        try:
            self._scatter.append(bytes(decoded.frame))
        except MemoryError as exc:
            self._failed = True
            raise ResourceError('cannot allocate an admitted shuffle frame') from exc
        self.pairs, self.frame_bytes = count, encoded_bytes

    def accept_scatter_stats(self, count, frame_bytes):
        self._check()
        if self._started or not isinstance(self._scatter, DirectScatter) or not self._scatter.inventory_only:
            raise ResourceError('shuffle cannot accept remote scatter statistics')
        self.pairs = checked_add(self.pairs, count)
        self.frame_bytes = checked_add(self.frame_bytes, frame_bytes)

    def _new_frontier(self):
        if len(self._frontiers) >= 33:
            raise ResourceError('shuffle frontier depth exceeds its bounded metadata allowance')
        result = _Frontier(self.store, self.cancel)
        self._frontiers.append(result)
        return result

    def _partition(self, sorter, path, fanout, expected_count, expected_bytes):
        frontier = self._new_frontier()
        key = self.keys.bucket_key(path)
        def child_number(frame):
            sample, ordinal = _FRAME_ID.unpack_from(frame, 4)
            return bucket_index(key, sample, ordinal, fanout)
        count = encoded_bytes = 0
        with sorter.iter_sorted() as frames:
            previous_bucket = fanout
            for child, group in itertools.groupby(frames, child_number):
                self._check()
                if child >= previous_bucket:
                    raise IntegrityError('shuffle partition bucket order is not canonical')
                previous_bucket = child
                child_path = (*path, child)
                previous_identity = None
                shard_count = shard_bytes = maximum = 0
                with self.store.new_segment(1, pairs_identity(child_path, 0, 0)) as writer:
                    for frame in group:
                        self._check()
                        decoded = decode_pair_view(frame, max_record_bytes=self.max_record_bytes)
                        identity = decoded.sample_index, decoded.original_pair_index
                        if previous_identity is not None and identity <= previous_identity:
                            raise IntegrityError('shuffle shard has duplicate or nonincreasing identities')
                        previous_identity = identity
                        writer.append(decoded.frame, item_count=1)
                        shard_count = checked_add(shard_count, 1)
                        shard_bytes = checked_add(shard_bytes, decoded.frame_bytes)
                        maximum = max(maximum, decoded.frame_bytes)
                    frontier.publish(child_path, writer, maximum, shard_count, shard_bytes)
                count = checked_add(count, shard_count)
                encoded_bytes = checked_add(encoded_bytes, shard_bytes)
        if (count, encoded_bytes) != (expected_count, expected_bytes):
            raise IntegrityError('shuffle partition totals differ from admitted source frames')
        return frontier

    def _source_frames(self, shard):
        count = encoded_bytes = maximum = 0
        previous = None
        with self.store.open_verified(shard.ref) as stream:
            while True:
                self._check()
                prefix = stream.read(4)
                if not prefix:
                    break
                if len(prefix) != 4:
                    raise IntegrityError('shuffle source frame length is truncated')
                length, = _LENGTH.unpack(prefix)
                if not 48 <= length <= self.max_frame_bytes:
                    raise IntegrityError('shuffle source frame exceeds its admitted length')
                self._carry[:4] = prefix
                offset = 4
                while offset < length:
                    self._check()
                    size = min(1 << 20, length - offset)
                    got = stream.readinto(memoryview(self._carry)[offset:offset + size])
                    if not got:
                        raise IntegrityError('shuffle source frame payload is truncated')
                    offset += got
                frame = bytes(memoryview(self._carry)[:length])
                decoded = decode_pair_view(frame, max_record_bytes=self.max_record_bytes)
                identity = decoded.sample_index, decoded.original_pair_index
                if previous is not None and identity <= previous:
                    raise IntegrityError('shuffle source identities are not canonical')
                previous = identity
                count = checked_add(count, 1)
                encoded_bytes = checked_add(encoded_bytes, length)
                maximum = max(maximum, length)
                yield frame
        if (count, encoded_bytes, maximum) != (shard.ref.item_count, shard.ref.decoded_bytes, shard.max_frame_bytes):
            raise IntegrityError('shuffle source statistics disagree with its validated frames')

    def _fits(self, shard):
        n, size = shard.ref.item_count, shard.ref.decoded_bytes
        return (n <= self._limits.pairs and size <= self._limits.arena
                and checked_add(checked_add(size, checked_multiply(128, n)), self._limits.overhead) <= self.terminal_bytes)

    def _refine(self, shard):
        if len(shard.path) >= self._limits.depth:
            raise ResourceError('shuffle bucket depth limit reached before terminal admission')
        fanout = refinement_fanout(shard.ref.item_count, shard.ref.decoded_bytes,
                                  self.terminal_bytes, _limits=self._limits)
        self._refining_sorter = self._sorter(shard.path, fanout)
        for frame in self._source_frames(shard):
            self._refining_sorter.append(frame)
        result = self._partition(self._refining_sorter, shard.path, fanout,
                                 shard.ref.item_count, shard.ref.decoded_bytes)
        self._refining_sorter.close()
        self._refining_sorter = None
        self.refinements += 1
        return result

    def _terminal_started(self, shard):
        self._check()
        if self._last_path is not None and shard.path <= self._last_path:
            raise IntegrityError('shuffle terminal paths are not in canonical numeric order')
        self._last_path = shard.path
        self.terminal_count += 1
        self.max_depth = max(self.max_depth, len(shard.path))

    def _emit_terminal(self, view, offsets, permutation):
        # Every window belongs to this one retained terminal. Only numeric
        # identities are batched; no list of frames can outlive its arena.
        frame = identities = tags = indices = None
        try:
            for start in range(0, len(permutation), self._tagger.batch_size):
                self._check()
                indices = permutation[start:start + self._tagger.batch_size]
                identities = [_FRAME_ID.unpack_from(view, int(offsets[index]) + 4) for index in indices]
                tags = self._tagger.tags(identities)
                for index, (sample, ordinal), tag in zip(indices, identities, tags):
                    self._check()
                    begin, end = int(offsets[index]), int(offsets[index + 1])
                    frame = view[begin:end]
                    self._coverage.add(sample, ordinal)
                    secure_index = self.emitted_pairs
                    self.emitted_pairs = checked_add(secure_index, 1)
                    self.emitted_mates = checked_add(self.emitted_mates, 2)
                    yield OrderedPair(secure_index, sample, ordinal, frame, tag)
                    frame = None
                identities = tags = indices = None
        finally:
            frame = identities = tags = indices = view = offsets = permutation = None

    def _terminal(self, shard):
        self._check()
        n, size = shard.ref.item_count, shard.ref.decoded_bytes
        with self.store.budget.acquire('shuffle-terminal', self.terminal_bytes):
            arena = offsets = decoded = view = permutation = None
            try:
                arena = bytearray(size)
                offsets = np.empty(n + 1, dtype='<u8')
                with self.store.open_verified(shard.ref) as stream:
                    read = 0
                    while read < size:
                        self._check()
                        got = stream.readinto(memoryview(arena)[read:read + min(1 << 20, size - read)])
                        if not got:
                            raise IntegrityError('shuffle terminal arena is truncated')
                        read += got
                    if stream.read(1):
                        raise IntegrityError('shuffle terminal has extra decoded bytes')
                cursor = maximum = 0
                previous = None
                view = memoryview(arena).toreadonly()
                for index in range(n):
                    self._check()
                    if size - cursor < 4:
                        raise IntegrityError('shuffle terminal frame prefix is truncated')
                    length, = _LENGTH.unpack_from(view, cursor)
                    if not 48 <= length <= min(self.max_frame_bytes, size - cursor):
                        raise IntegrityError('shuffle terminal frame length is invalid')
                    decoded = decode_pair_view(view[cursor:cursor + length], max_record_bytes=self.max_record_bytes)
                    identity = decoded.sample_index, decoded.original_pair_index
                    if previous is not None and identity <= previous:
                        raise IntegrityError('shuffle terminal identities are not canonical')
                    previous = identity
                    offsets[index] = cursor
                    cursor += length
                    maximum = max(maximum, length)
                decoded = None
                offsets[n] = cursor
                if cursor != size or maximum != shard.max_frame_bytes:
                    raise IntegrityError('shuffle terminal statistics differ from its complete frame payload')
                permutation = terminal_permutation(self.keys, shard.path, n, cancel=self.cancel)
                self._terminal_started(shard)
                yield from self._emit_terminal(view, offsets, permutation)
            except (MemoryError, OverflowError) as exc:
                raise ResourceError('cannot allocate the admitted terminal arena and offsets') from exc
            finally:
                # Drop views before the arena reservation, including generator
                # cancellation during an in-flight rendering window.
                decoded = view = permutation = offsets = arena = None

    def _parallel_terminal(self, shard, task):
        n, size = shard.ref.item_count, shard.ref.decoded_bytes
        self._terminal_result = self.scheduler.take_next()
        result = self._terminal_result
        try:
            if result.task_id != task or result.records != n:
                raise IntegrityError('terminal result does not match its canonical task')
            external = hasattr(result, 'ref')
            charge = self.terminal_bytes if external else self.terminal_bytes - size
            with self.store.budget.acquire('shuffle-terminal-result', charge):
                arena = offsets = decoded = view = permutation = None
                try:
                    if external:
                        if result.ref.decoded_bytes != size:
                            raise IntegrityError('terminal result has invalid byte totals')
                        arena = bytearray(size)
                        with self.store.open_verified(result.ref) as stream:
                            read = 0
                            while read < size:
                                self._check()
                                got = stream.readinto(memoryview(arena)[read:read + min(1 << 20, size - read)])
                                if not got:
                                    raise IntegrityError('terminal result is truncated')
                                read += got
                            if stream.read(1):
                                raise IntegrityError('terminal result has extra decoded bytes')
                    else:
                        arena = result.data
                        if len(arena) != size:
                            raise IntegrityError('terminal result has invalid byte totals')
                    view = memoryview(arena).toreadonly()
                    offsets = np.empty(n + 1, dtype='<u8')
                    cursor = maximum = 0
                    for index in range(n):
                        self._check()
                        if size - cursor < 4:
                            raise IntegrityError('terminal result frame prefix is truncated')
                        length, = _LENGTH.unpack_from(view, cursor)
                        if not 48 <= length <= min(self.max_frame_bytes, size - cursor):
                            raise IntegrityError('terminal result frame length is invalid')
                        decoded = decode_pair_view(view[cursor:cursor + length], max_record_bytes=self.max_record_bytes)
                        decoded = None
                        offsets[index] = cursor
                        cursor += length
                        maximum = max(maximum, length)
                    offsets[n] = cursor
                    if cursor != size or maximum != shard.max_frame_bytes:
                        raise IntegrityError('terminal result statistics differ from its complete frames')
                    permutation = range(n)  # worker already returned rank order
                    self._terminal_started(shard)
                    yield from self._emit_terminal(view, offsets, permutation)
                finally:
                    decoded = view = permutation = offsets = arena = None
        except BaseException as error:
            try:
                result.close()
                self._terminal_result = None
            except BaseException:
                error.add_note('terminal result cleanup also failed; ownership retained')
            raise
        else:
            result.close()
            self._terminal_result = None

    def _walk(self, frontier):
        parallel = self.scheduler is not None and self.scheduler.process_slots > 0
        pending = deque()
        try:
            with frontier.ledger.open_reverse() as records:
                for encoded in records:
                    self._check()
                    shard = _decode_shard(encoded)
                    if self._fits(shard):
                        if not parallel:
                            yield from self._terminal(shard)
                            frontier.drop(shard)
                            continue
                        from .stage_policy import StagePolicy
                        policy = StagePolicy.terminal(shard.path, shard.ref.item_count,
                            self.max_record_bytes, shard.max_frame_bytes,
                            tuple(self.keys.rank_key(shard.path, shard.ref.item_count, attempt) for attempt in range(8)))
                        task = self.scheduler.submit_segment(shard.ref, policy=policy)
                        pending.append((shard, task))
                        if len(pending) < self.scheduler.capacity:
                            continue
                    # One next refinement can run on the coordinator while
                    # older terminal workers finish. Its sorter uses local runs
                    # while foreign tasks occupy the shared scheduler. Keep both
                    # parent and authenticated child frontier owned until older
                    # leaves drain: consumed-prefix cleanup must stay canonical.
                    children = self._refine(shard) if not self._fits(shard) else None
                    while pending:
                        leaf, task = pending[0]
                        yield from self._parallel_terminal(leaf, task)
                        frontier.drop(leaf)
                        pending.popleft()
                    if children is not None:
                        frontier.drop(shard)
                        yield from self._walk(children)
                while pending:
                    leaf, task = pending[0]
                    yield from self._parallel_terminal(leaf, task)
                    frontier.drop(leaf)
                    pending.popleft()
            frontier.close()
            self._frontiers.remove(frontier)
        except BaseException as error:
            # The frontier cannot discard a source still borrowed by a worker.
            # Reap and revoke all task grants before its owning cleanup runs.
            if parallel and not self.scheduler._closed:
                try:
                    self.scheduler.abort(error)
                except BaseException:
                    error.add_note('terminal worker cleanup also failed; ownership retained')
            raise
        finally:
            pending.clear()

    @contextmanager
    def iter_ordered(self, sample_counts):
        self._check()
        if self._started:
            raise ResourceError('shuffle ordered iteration is single-use')
        self._started = self._active = True
        iterator = None
        tagger = queue_lease = None
        try:
            tagger = BulkTagger(self.keys, self.store.budget, batch_size=self._tag_batch_size)
            self._tagger = tagger
            if self.scheduler is not None and self.scheduler.process_slots:
                charge = 4096 + 2048 * self.scheduler.capacity
                queue_lease = self.store.budget.acquire(
                    'shuffle-terminal-window', charge, metadata_bytes=charge)
            self._coverage = IdentityCoverage(self.store, sample_counts, cancel=self.cancel)
            if self._coverage.total != self.pairs:
                raise IntegrityError('shuffle input pairs differ from validated sample counts')
            expected_mates = checked_multiply(self.pairs, 2)
            root = self._partition(self._scatter, (), 64, self.pairs, self.frame_bytes)
            self._scatter.close()
            self._scatter = None
            iterator = self._walk(root)
            yield iterator
            for unused_pair in iterator:
                # Auto-draining an early consumer obeys the same borrowed-view
                # contract as normal emission before advancing terminal leases.
                del unused_pair
            self._coverage.finish()
            if (self.emitted_pairs, self.emitted_mates) != (self.pairs, expected_mates):
                raise IntegrityError('shuffle emitted totals differ from validated inputs')
            self._coverage.close()
            self._coverage = None
        except MemoryError as exc:
            self._failed = True
            raise ResourceError('shuffle exceeded its admitted working set') from exc
        except BaseException:
            self._failed = True
            raise
        finally:
            try:
                if iterator is not None:
                    iterator.close()
            finally:
                self._active = False
                self._tagger = None
                if tagger is not None:
                    tagger.close()
                if queue_lease is not None:
                    queue_lease.release()

    def close(self):
        if self._closed:
            return
        if self._active:
            raise ResourceError('cannot close shuffle while a borrowed output iterator is active')
        if self._terminal_result is not None:
            self._terminal_result.close()
            self._terminal_result = None
        if self._coverage is not None:
            self._coverage.close()
            self._coverage = None
        for name in ('_refining_sorter', '_scatter'):
            sorter = getattr(self, name)
            if sorter is not None:
                sorter.close()
                setattr(self, name, None)
        while self._frontiers:
            self._frontiers[-1].close()
            self._frontiers.pop()
        self._carry = None
        self._carry_lease.release()
        self._metadata.release()
        self._closed = True

    def __exit__(self, exc_type, exc, traceback):
        try:
            self.close()
        except BaseException:
            if exc is None:
                raise
            exc.add_note('shuffle cleanup failed; primary error preserved and protected scratch remains owned')
