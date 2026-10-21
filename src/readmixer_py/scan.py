"""Independent physical joins for complete verification and union recovery."""

from contextlib import ExitStack
from dataclasses import dataclass

from .artifacts.bgzf import BgzfReader, GziCursor
from .artifacts.index import IndexReader
from .artifacts.key import KEY_HEADER, maximum_key_row_bytes, parse_key_row, secured_record_limit
from .errors import InputError, IntegrityError
from .paircodec import encode_pair_into, maximum_frame_bytes


@dataclass(frozen=True, slots=True)
class _Record:
    lines: tuple


@dataclass(frozen=True, slots=True)
class _Pair:
    sample_index: int
    original_pair_index: int
    r1: _Record
    r2: _Record


def _original(row, r1, r2, run_id, arena, record_limit):
    records = []
    for role, raw in ((1, r1), (2, r2)):
        lines = raw.split(b'\n')
        expected = f'@ReadMixer.{run_id}.{row.pair_tag}/{role}'.encode('ascii')
        if (len(lines) != 5 or lines[-1] or lines[0] != expected
                or lines[2] != b'+' or len(lines[1]) != len(lines[3])):
            raise IntegrityError('secured FASTQ header, role, plus, or line framing is invalid')
        records.append(_Record((getattr(row, f'r{role}_header'), lines[1],
                                getattr(row, f'r{role}_plus'), lines[3])))
    pair = _Pair(row.sample_index, row.original_pair_index, *records)
    try:
        size = encode_pair_into(pair, arena, max_record_bytes=record_limit)
    except InputError as exc:
        raise IntegrityError('reconstructed original pair is invalid') from exc
    return bytes(memoryview(arena)[:size])


def _fits(cap, pairs, r1, r2, key):
    # Deliberately independent of producer admission/placement helpers.
    safe = ((cap - 28) // 65536) * 65280
    if max(r1, r2, key) > safe or 64 + 40 * pairs > cap:
        return False
    for length in (r1, r2):
        members = (length + 65279) // 65280
        if 8 + 16 * max(members - 1, 0) > cap:
            return False
    return True


class PoolScanner:
    def __init__(self, pool, *, max_record_bytes, cancel=None):
        self.pool, self.manifest = pool, pool.manifest
        self.max_record_bytes, self.cancel = max_record_bytes, cancel
        metadata = 4096 + 128 * len(self.manifest['samples'])
        self._lease = pool.budget.acquire('pool-pair-join', 16 * max_record_bytes + (1 << 20) + metadata,
                                          metadata_bytes=metadata)
        try:
            self.counts = tuple(sample['pairs'] for sample in self.manifest['samples'])
            self.sample_observations = [0] * len(self.counts)
            self._arena = bytearray(maximum_frame_bytes(max_record_bytes))
        except BaseException:
            self._lease.release()
            raise

    def _index(self, stack, bundle):
        source = stack.enter_context(self.pool.open(bundle['index']))
        return stack.enter_context(IndexReader(source, run_id=bytes.fromhex(self.manifest['run_id']),
            bundle_index=bundle['bundle_index'], first_secure_pair_index=bundle['first_secure_pair_index'],
            entry_count=bundle['pairs'], sample_counts=self.counts, max_bytes=bundle['index']['bytes']))

    def _sidecars(self, stack, bundle):
        cursors = []
        for role in ('r1', 'r2'):
            source = stack.enter_context(self.pool.open(bundle[role + '_gzi']))
            cursors.append(stack.enter_context(GziCursor(source,
                uncompressed_bytes=bundle[role]['uncompressed_bytes'], physical_bytes=bundle[role]['bytes'],
                expected_entries=bundle[role + '_gzi']['entries'], budget=self.pool.budget)))
        return cursors

    def _readers(self, stack, bundle, cursors, *, indexed):
        readers = []
        for number, role in enumerate(('r1', 'r2', 'key')):
            source = stack.enter_context(self.pool.open(bundle[role], full_hash=not indexed))
            readers.append(stack.enter_context(BgzfReader(source,
                uncompressed_limit=bundle[role]['uncompressed_bytes'], physical_limit=bundle[role]['bytes'],
                budget=self.pool.budget, cancel=self.cancel, indexed=indexed,
                on_member=cursors[number].observe_member if number < 2 and not indexed else None)))
        return readers

    def sequential(self, *, indexes=True, selected=None, prove_greedy=False):
        """Every consumed lane is fully parsed and hashed, even unselected rows."""
        previous = None
        for bundle in self.manifest['bundles']:
            self.pool.checkpoint()
            with ExitStack() as stack:
                cursors = self._sidecars(stack, bundle)
                r1, r2, key = self._readers(stack, bundle, cursors, indexed=False)
                index = iter(self._index(stack, bundle)) if indexes else None
                if key.readline(len(KEY_HEADER)) != KEY_HEADER:
                    raise IntegrityError('mapping key header is not canonical')
                totals = [0, 0, len(KEY_HEADER)]
                for position in range(bundle['pairs']):
                    if position % 4096 == 0:
                        self.pool.checkpoint()
                    starts = (r1.tell_virtual(), r2.tell_virtual(), key.tell_virtual())
                    offsets = (r1.tell_uncompressed(), r2.tell_uncompressed())
                    line = key.readline(maximum_key_row_bytes(self.max_record_bytes))
                    row = parse_key_row(line, max_record_bytes=self.max_record_bytes, sample_counts=self.counts,
                        expected_secure_pair_index=bundle['first_secure_pair_index'] + position)
                    raw1, raw2 = r1.read_record(secured_record_limit(self.max_record_bytes)), r2.read_record(secured_record_limit(self.max_record_bytes))
                    if (row.r1_uncompressed_offset, row.r2_uncompressed_offset) != offsets:
                        raise IntegrityError('key mate offsets differ from physical record starts')
                    if (row.r1_record_bytes, row.r2_record_bytes) != (len(raw1), len(raw2)):
                        raise IntegrityError('key mate lengths differ from complete secured records')
                    if index is not None:
                        entry = next(index, None)
                        if entry is None or (entry.sample_index, entry.original_pair_index,
                            entry.r1_virtual_offset, entry.r2_virtual_offset, entry.key_virtual_offset) != (
                            row.sample_index, row.original_pair_index, *starts):
                            raise IntegrityError('mapping index disagrees with observed physical pair boundaries')
                    self.sample_observations[row.sample_index] += 1
                    frame = _original(row, raw1, raw2, self.manifest['run_id'], self._arena, self.max_record_bytes)
                    if prove_greedy:
                        cap = self.manifest['limits']['max_artifact_bytes']
                        if not _fits(cap, 1, len(raw1), len(raw2), len(KEY_HEADER) + len(line)):
                            raise IntegrityError('a complete pair cannot fit the declared bundle cap')
                        if position == 0 and previous is not None:
                            old_pairs, old_r1, old_r2, old_key = previous
                            hypothetical = (len(line) - len(str(row.r1_uncompressed_offset))
                                - len(str(row.r2_uncompressed_offset)) + len(str(old_r1)) + len(str(old_r2)))
                            if _fits(cap, old_pairs + 1, old_r1 + len(raw1), old_r2 + len(raw2), old_key + hypothetical):
                                raise IntegrityError('pool bundle boundary is earlier than canonical greedy admission')
                    totals[0] += len(raw1)
                    totals[1] += len(raw2)
                    totals[2] += len(line)
                    if prove_greedy and not _fits(cap, position + 1, *totals):
                        raise IntegrityError('pool bundle exceeds canonical admission bounds')
                    if selected is None or row.sample_index in selected:
                        yield frame, row.pair_tag
                if index is not None and next(index, None) is not None:
                    raise IntegrityError('mapping index has excess entries')
                for number, reader in enumerate((r1, r2, key)):
                    if reader.read(1):
                        raise IntegrityError('pool lane contains excess records')
                    stats = reader.finish()
                    role = ('r1', 'r2', 'key')[number]
                    if stats.uncompressed_bytes != totals[number] or stats.uncompressed_bytes != bundle[role]['uncompressed_bytes']:
                        raise IntegrityError('pool lane uncompressed totals disagree')
                for cursor in cursors:
                    cursor.finish()
                previous = (bundle['pairs'], *totals)
        if tuple(self.sample_observations) != self.counts:
            raise IntegrityError('observed per-sample pair counts differ from the manifest')

    def indexed(self, selected):
        """One physical index traversal gathers the union; unread lanes stay unhashed."""
        for bundle in self.manifest['bundles']:
            self.pool.checkpoint()
            with ExitStack() as stack:
                index = self._index(stack, bundle)
                cursors = self._sidecars(stack, bundle)
                r1, r2, key = self._readers(stack, bundle, cursors, indexed=True)
                key_started = False
                for position, entry in enumerate(index):
                    self.sample_observations[entry.sample_index] += 1
                    if position % 4096 == 0:
                        self.pool.checkpoint()
                    if entry.sample_index not in selected:
                        continue
                    if not key_started:
                        if key.readline(len(KEY_HEADER)) != KEY_HEADER:
                            raise IntegrityError('mapping key header is not canonical')
                        key_started = True
                    line = key.readline_at(entry.key_virtual_offset, maximum_key_row_bytes(self.max_record_bytes))
                    row = parse_key_row(line, max_record_bytes=self.max_record_bytes, sample_counts=self.counts,
                        expected_secure_pair_index=bundle['first_secure_pair_index'] + position)
                    if (row.sample_index, row.original_pair_index) != (entry.sample_index, entry.original_pair_index):
                        raise IntegrityError('selected key identity differs from its mapping index')
                    if (cursors[0].virtual_offset(row.r1_uncompressed_offset),
                        cursors[1].virtual_offset(row.r2_uncompressed_offset)) != (entry.r1_virtual_offset, entry.r2_virtual_offset):
                        raise IntegrityError('selected key offset differs from its sidecar/index location')
                    raw1 = r1.read_record_at(entry.r1_virtual_offset, secured_record_limit(self.max_record_bytes))
                    raw2 = r2.read_record_at(entry.r2_virtual_offset, secured_record_limit(self.max_record_bytes))
                    if (len(raw1), len(raw2)) != (row.r1_record_bytes, row.r2_record_bytes):
                        raise IntegrityError('selected key record lengths disagree with observed records')
                    # Header-chain decoded offsets independently corroborate the sidecars.
                    if (r1.tell_uncompressed() - len(raw1), r2.tell_uncompressed() - len(raw2)) != (
                        row.r1_uncompressed_offset, row.r2_uncompressed_offset):
                        raise IntegrityError('selected sidecar offset disagrees with the member chain')
                    yield _original(row, raw1, raw2, self.manifest['run_id'], self._arena, self.max_record_bytes), row.pair_tag
                for cursor in cursors:
                    cursor.finish()
        if tuple(self.sample_observations) != self.counts:
            raise IntegrityError('complete index sample counts differ from the manifest')

    def close(self):
        self._arena = None
        self.counts = ()
        self.sample_observations = []
        self.manifest = None
        self._lease.release()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
