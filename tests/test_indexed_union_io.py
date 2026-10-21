"""P15-T04: observe physical I/O during a real multi-sample public recovery.

The byte oracle uses stdlib gzip/zlib and literal wire structs, independently of
the production index, key, BGZF and placement helpers. Instrumentation stores
offsets and sizes, never borrowed record buffers or decoded member payloads.
"""

from collections import Counter, defaultdict
from dataclasses import dataclass, field
import builtins
import gzip
import io
import json
import os
from pathlib import Path
import shutil
import struct
import zlib

import pytest

from readmixer_py.artifacts.bgzf import BgzfReader
from readmixer_py.artifacts.index import IndexReader
from readmixer_py.config import MixConfig, RecoverConfig, RecoveryMode, StorageMode
from readmixer_py.crypto import OperationKeys
from readmixer_py.errors import IntegrityError
from readmixer_py.mixing import _mix
from readmixer_py.pool import _Input
from readmixer_py.recovery import recover


SELECTED = frozenset((0, 1, 3))
RECORD_LIMIT = 262_144


@dataclass(frozen=True)
class Member:
    physical: int
    size: int
    decoded: int
    length: int

    @property
    def payload(self):
        return self.physical + 18, self.physical + self.size - 8


def members(raw):
    """Read canonical members without the production BGZF implementation."""
    result = []
    physical = decoded = 0
    while physical < len(raw):
        assert raw[physical:physical + 16] == bytes.fromhex('1f8b08040000000000ff060042430200')
        size = struct.unpack_from('<H', raw, physical + 16)[0] + 1
        body = zlib.decompress(raw[physical + 18:physical + size - 8], -15)
        crc, length = struct.unpack_from('<II', raw, physical + size - 8)
        assert length == len(body) and crc == zlib.crc32(body)
        result.append(Member(physical, size, decoded, length))
        physical += size
        decoded += length
    assert physical == len(raw) and result[-1].length == 0
    assert all(member.length == 65280 for member in result[:-2])
    return result


def overlap(first, second):
    return max(first[0], second[0]) < min(first[1], second[1])


def member_cover(lane, start, length):
    return {member.physical for member in lane
            if overlap((start, start + length), (member.decoded, member.decoded + member.length))}


def pool_oracle(path):
    document = json.loads(path.read_bytes())
    lanes, entries, ranges = {}, {}, {}
    for bundle in document['bundles']:
        for role in ('r1', 'r2', 'key'):
            name = bundle[role]['name']
            lanes[name] = members((path.parent / name).read_bytes())
            assert sum(member.length for member in lanes[name]) == bundle[role]['uncompressed_bytes']
        raw_index = (path.parent / bundle['index']['name']).read_bytes()
        header = struct.unpack_from('<8sII16sQQQQ', raw_index)
        assert header[:3] == (b'RMPYIDX1', 1, 40)
        assert header[6] == bundle['pairs'] and header[7] == 0
        rows = list(struct.iter_unpack('<IIQQQQ', raw_index[64:]))
        assert len(rows) == bundle['pairs'] and all(row[1] == 0 for row in rows)
        entries[bundle['index']['name']] = rows
        key_lines = gzip.decompress((path.parent / bundle['key']['name']).read_bytes()).splitlines(keepends=True)
        assert len(key_lines) == len(rows) + 1
        key_position = len(key_lines[0])
        for row, line in zip(rows, key_lines[1:]):
            fields = line.rstrip(b'\n').split(b',')
            sample, original = int(fields[0]), int(fields[1])
            assert (sample, original) == (row[0], row[2])
            for role, position, length in (
                ('r1', int(fields[4]), int(fields[5])),
                ('r2', int(fields[6]), int(fields[7])),
                ('key', key_position, len(line)),
            ):
                name = bundle[role]['name']
                ranges.setdefault(name, []).append((sample, original, position, length))
            key_position += len(line)
    return document, lanes, entries, ranges


@pytest.fixture(scope='module')
def union_pool(tmp_path_factory):
    root = tmp_path_factory.mktemp('union-io')
    original = root / 'original'
    original.mkdir(mode=0o700)
    expected = {}
    for sample, count in enumerate((16, 16, 80, 0)):
        label = chr(65 + sample)
        expected[label] = {}
        for role in (1, 2):
            records = []
            for ordinal in range(count):
                length = (90_000 if ordinal == 0 else 7) if sample < 2 else 30_000
                length += role - 1
                padding = b'h' * 12_000 if sample < 2 and ordinal == 0 else b''
                header = f'@{label}-{ordinal}/{role} '.encode() + padding
                plus = b'+original' + padding
                sequence = (b'ACGT' * ((length + 3) // 4))[:length]
                record = b'\n'.join((header, sequence, plus, b'!' * length)) + b'\n'
                records.append(record)
            expected[label][f'r{role}'] = data = b''.join(records)
            (original / f'{label}_R{role}.fastq').write_bytes(data)
    report = _mix(MixConfig(input_dir=original, output=root / 'pool', workers=1,
                           max_ram=1 << 30, storage=StorageMode.EXTERNAL,
                           max_record_bytes=RECORD_LIMIT, max_artifact_bytes=393_244),
                  _keys=OperationKeys._from_test(b'K' * 32, b'R' * 16))
    manifest = root / 'pool' / report.manifest
    oracle = pool_oracle(manifest)
    shutil.rmtree(original)
    return manifest, expected, oracle


def recover_config(manifest, destination, mode=RecoveryMode.INDEXED):
    return RecoverConfig(manifest=manifest, output=destination, sample=('D', 'B', 'A'),
                         io=mode, workers=1, max_ram=1 << 30, storage=StorageMode.EXTERNAL,
                         max_record_bytes=RECORD_LIMIT)


def assert_restored(destination, report, expected):
    receipt = json.loads((destination / report.receipt).read_bytes())
    actual = {sample['sample_id']: {
        role: b''.join(gzip.decompress((destination / part[role]['name']).read_bytes())
                       for part in sample['parts'])
        for role in ('r1', 'r2')} for sample in receipt['samples']}
    assert actual == {label: expected[label] for label in ('A', 'B', 'D')}
    assert report.samples == 3 and report.pairs == 32 and report.cleanup_complete
    assert not report.warnings
    assert receipt['samples'][-1]['pairs'] == 0
    return receipt


def test_fixture_has_required_union_witnesses(union_pool):
    _manifest, _expected, (document, lanes, entries, ranges) = union_pool
    assert len(document['bundles']) > 1
    assert any(all(row[0] not in SELECTED for row in rows) for rows in entries.values())
    assert any(len(member_cover(lanes[name], start, length)) > 2
               for name, records in ranges.items() for sample, _ordinal, start, length in records
               if sample in SELECTED and name.endswith('.fastq.gz'))
    assert any(len(member_cover(lanes[name], start, length)) > 1
               for name, records in ranges.items() for sample, _ordinal, start, length in records
               if sample in SELECTED and name.endswith('.key.csv.gz'))
    shared = False
    for name, records in ranges.items():
        occupants = defaultdict(set)
        for sample, _ordinal, start, length in records:
            if sample in SELECTED:
                for member in member_cover(lanes[name], start, length):
                    occupants[member].add(sample)
        shared |= any(len(samples) > 1 for samples in occupants.values())
    assert shared


@dataclass
class InputTrace:
    full_hash: bool
    reads: list = field(default_factory=list)
    seeks: list = field(default_factory=list)
    hashed_bytes: int = 0
    hash_updates: int = 0
    finished: list = field(default_factory=list)


class DigestWitness:
    def __init__(self, digest, observe):
        self.digest, self.observe = digest, observe

    def update(self, data):
        self.observe(len(data))
        return self.digest.update(data)

    def hexdigest(self):
        return self.digest.hexdigest()


class IOTrace:
    """Observe real readers while keeping their validation and hash code active."""

    def __init__(self, monkeypatch):
        self.inputs = defaultdict(list)
        self.visits = defaultdict(list)
        self.inflations = defaultdict(list)
        self.index_entries = defaultdict(list)
        self.cache_accesses = defaultdict(list)
        self.codec_hashed = Counter()
        self.codec_updates = Counter()
        self.closed_stats = {}
        self.live = {}
        self.max_retained = self.max_readers = 0
        active = []
        input_init, read, seek, finish = _Input.__init__, _Input.read, _Input.seek, _Input.finish
        bgzf_init, next_member = BgzfReader.__init__, BgzfReader._next
        inflate, seek_virtual, close = BgzfReader._inflate, BgzfReader.seek_virtual, BgzfReader.close
        index_next = IndexReader.__next__

        def init_input(source, *args, **kwargs):
            input_init(source, *args, **kwargs)
            trace = InputTrace(source.full_hash)
            self.inputs[source.descriptor['name']].append(trace)
            source._io_test_trace = trace
            if source.digest is not None:
                def observe(size):
                    trace.hashed_bytes += size
                    trace.hash_updates += 1
                source.digest = DigestWitness(source.digest, observe)

        def traced_read(source, size=-1):
            start = source.tell()
            data = read(source, size)
            if data:
                source._io_test_trace.reads.append((start, start + len(data)))
            return data

        def traced_seek(source, offset, whence=0):
            start = source.tell()
            result = seek(source, offset, whence)
            source._io_test_trace.seeks.append((start, source.tell(), whence))
            return result

        def traced_finish(source):
            result = finish(source)
            source._io_test_trace.finished.append((source.bytes_read,
                None if source.digest is None else source.digest.hexdigest()))
            return result

        def init_bgzf(reader, source, *args, **kwargs):
            bgzf_init(reader, source, *args, **kwargs)
            if isinstance(source, _Input):
                name = source.descriptor['name']
                self.live[id(reader)] = reader
                self.max_readers = max(self.max_readers, len(self.live))
                def observe(size):
                    self.codec_hashed[name] += size
                    self.codec_updates[name] += 1
                reader._digest = DigestWitness(reader._digest, observe)

        def traced_next(reader, target=None):
            if not isinstance(reader.source, _Input):
                return next_member(reader, target)
            name, start = reader.source.descriptor['name'], reader._physical
            active.append((name, start))
            try:
                result = next_member(reader, target)
                if reader._physical != start:
                    self.visits[name].append(start)
                for retained in self.live.values():
                    assert not retained._lease.released
                    assert len(retained._data) <= min(65280, retained._lease.bytes)
                self.max_retained = max(self.max_retained,
                    sum(len(retained._data) for retained in self.live.values()))
                return result
            finally:
                active.pop()

        def traced_inflate(payload, footer):
            data = inflate(payload, footer)
            if active:
                name, start = active[-1]
                self.inflations[name].append((start, len(payload), len(data)))
            return data

        def traced_virtual(reader, offset):
            if not isinstance(reader.source, _Input):
                return seek_virtual(reader, offset)
            hit = (reader.member is not None and reader.member.compressed_start == offset >> 16
                   and bool(reader._data))
            result = seek_virtual(reader, offset)
            self.cache_accesses[reader.source.descriptor['name']].append((offset, hit))
            return result

        def traced_close(reader):
            if isinstance(reader.source, _Input) and not reader._closed:
                self.closed_stats[reader.source.descriptor['name']] = reader.stats
                close(reader)
                assert reader._data == b'' and reader._lease.released
                self.live.pop(id(reader))
            else:
                close(reader)

        def traced_index(reader):
            entry = index_next(reader)
            if isinstance(reader.stream, _Input):
                self.index_entries[reader.stream.descriptor['name']].append((entry.sample_index,
                    0, entry.original_pair_index, entry.r1_virtual_offset,
                    entry.r2_virtual_offset, entry.key_virtual_offset))
            return entry

        monkeypatch.setattr(_Input, '__init__', init_input)
        monkeypatch.setattr(_Input, 'read', traced_read)
        monkeypatch.setattr(_Input, 'seek', traced_seek)
        monkeypatch.setattr(_Input, 'finish', traced_finish)
        monkeypatch.setattr(BgzfReader, '__init__', init_bgzf)
        monkeypatch.setattr(BgzfReader, '_next', traced_next)
        monkeypatch.setattr(BgzfReader, '_inflate', staticmethod(traced_inflate))
        monkeypatch.setattr(BgzfReader, 'seek_virtual', traced_virtual)
        monkeypatch.setattr(BgzfReader, 'close', traced_close)
        monkeypatch.setattr(IndexReader, '__next__', traced_index)

    def sole_input(self, name):
        assert len(self.inputs[name]) == 1, 'each artifact must have one union reader'
        return self.inputs[name][0]

    def assert_full_file(self, descriptor):
        trace = self.sole_input(descriptor['name'])
        assert trace.full_hash and not trace.seeks
        cursor = 0
        for start, end in trace.reads:
            assert start == cursor
            cursor = end
        assert cursor == trace.hashed_bytes == descriptor['bytes']
        assert trace.hash_updates > 0
        assert trace.finished == [(descriptor['bytes'], descriptor['sha256'])]


def test_indexed_union_reuses_members_and_skips_unneeded_compressed_payload(
        union_pool, tmp_path, monkeypatch):
    manifest, expected, (document, lanes, entries, ranges) = union_pool
    trace = IOTrace(monkeypatch)
    destination = tmp_path / 'indexed'
    report = recover(recover_config(manifest, destination))
    receipt = assert_restored(destination, report, expected)
    assert report.validation_scope == receipt['validation_scope'] == 'selected_records'
    assert report.index_validation == receipt['index_validation'] == 'validated'
    assert dict(trace.index_entries) == entries
    assert trace.max_readers == 3 and 0 < trace.max_retained <= 3 * 65280
    assert not trace.live
    skipped_payloads = unused_bundles = long_records = 0
    shared_cache_hits = Counter()
    observed_bytes = 0
    for bundle in document['bundles']:
        for role in ('index', 'r1_gzi', 'r2_gzi'):
            trace.assert_full_file(bundle[role])
        selected_rows = [row for row in entries[bundle['index']['name']] if row[0] in SELECTED]
        unused_bundles += not selected_rows
        for role, offset_column in (('r1', 3), ('r2', 4), ('key', 5)):
            name = bundle[role]['name']
            lane, source = lanes[name], trace.sole_input(name)
            assert not source.full_hash and source.hash_updates == source.hashed_bytes == 0
            assert trace.codec_hashed[name] == trace.codec_updates[name] == 0
            assert trace.closed_stats[name].sha256 is None
            assert source.finished == [(sum(end - start for start, end in source.reads), None)]
            assert all(end > start and whence == 1 for start, end, whence in source.seeks)
            if not selected_rows:
                assert not source.reads and not source.seeks
                assert not trace.visits[name] and not trace.inflations[name]
                continue

            required, allowed = set(), set()
            selected_ranges = [row for row in ranges[name] if row[0] in SELECTED]
            for _sample, _ordinal, start, length in selected_ranges:
                cover = member_cover(lane, start, length)
                required.update(cover)
                long_records += role != 'key' and len(cover) > 2
            if role == 'key':
                required.add(lane[0].physical)  # The canonical key header is also required.
            allowed.update(required)
            for row in selected_rows:
                target = row[offset_column] >> 16
                number = next(number for number, member in enumerate(lane) if member.physical == target)
                if number:
                    # A skipped member immediately before a target may be decoded
                    # to prove a line start. Continuations are in `required` above.
                    allowed.add(lane[number - 1].physical)

            decoded = [start for start, _compressed, size in trace.inflations[name] if size]
            assert len(decoded) == len(set(decoded)), 'a shared member was decompressed again'
            assert required <= set(decoded) <= allowed
            visited = trace.visits[name]
            assert visited == [member.physical for member in lane if member.physical <= visited[-1]]
            assert set(visited) >= required
            for member in lane:
                payload_reads = sum(min(end, member.payload[1]) - max(start, member.payload[0])
                    for start, end in source.reads if overlap((start, end), member.payload))
                assert payload_reads == (member.size - 26 if member.physical in decoded else 0)
                if member.physical in visited and member.physical not in decoded:
                    skipped_payloads += 1
                    assert (member.physical + 18, member.physical + member.size - 8, 1) in source.seeks
                    assert (member.physical, member.physical + 18) in source.reads
                    assert (member.physical + member.size - 8, member.physical + member.size) in source.reads

            accesses = trace.cache_accesses[name]
            assert [offset for offset, _hit in accesses] == [row[offset_column] for row in selected_rows]
            prior_samples = defaultdict(set)
            for row, (offset, hit) in zip(selected_rows, accesses):
                if hit and prior_samples[offset >> 16] - {row[0]}:
                    shared_cache_hits[role] += 1
                matching = next(record for record in selected_ranges if record[:2] == (row[0], row[2]))
                for physical in member_cover(lane, matching[2], matching[3]):
                    prior_samples[physical].add(row[0])
    for sources in trace.inputs.values():
        observed_bytes += sum(end - start for source in sources for start, end in source.reads)
    assert skipped_payloads > 0 and unused_bundles > 0 and long_records > 0
    assert all(shared_cache_hits[role] > 0 for role in ('r1', 'r2', 'key'))
    assert report.consumed_artifact_bytes == manifest.stat().st_size + observed_bytes
    assert observed_bytes < sum(bundle[role]['bytes'] for bundle in document['bundles']
                                for role in ('index', 'r1_gzi', 'r2_gzi', 'r1', 'r2', 'key'))


def test_sequential_union_bypasses_even_resolution_of_missing_indexes(
        union_pool, tmp_path, monkeypatch):
    manifest, expected, (document, lanes, _entries, _ranges) = union_pool
    source_root = tmp_path / 'source'
    shutil.copytree(manifest.parent, source_root)
    source_manifest = source_root / manifest.name
    # Remove all indexes before instrumentation: success must not depend on one.
    index_names = {bundle['index']['name'] for bundle in document['bundles']}
    for name in index_names:
        (source_root / name).unlink()

    def checked(path):
        if isinstance(path, (str, bytes, os.PathLike)):
            assert os.fsdecode(os.fspath(path)).rsplit('/', 1)[-1] not in index_names, \
                'explicitly bypassed index reached filesystem resolution/access'

    for operation in ('stat', 'lstat', 'open'):
        original = getattr(os, operation)
        def guarded(path, *args, _original=original, **kwargs):
            checked(path)
            return _original(path, *args, **kwargs)
        monkeypatch.setattr(os, operation, guarded)
    for module in (builtins, io):
        original = module.open
        def guarded_open(path, *args, _original=original, **kwargs):
            checked(path)
            return _original(path, *args, **kwargs)
        monkeypatch.setattr(module, 'open', guarded_open)
    resolve = Path.resolve
    def guarded_resolve(path, *args, **kwargs):
        checked(path)
        return resolve(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'resolve', guarded_resolve)
    trace = IOTrace(monkeypatch)
    destination = tmp_path / 'sequential'
    report = recover(recover_config(source_manifest, destination, RecoveryMode.SEQUENTIAL))
    receipt = assert_restored(destination, report, expected)
    assert report.index_validation == receipt['index_validation'] == 'explicitly_bypassed'
    assert report.validation_scope == receipt['validation_scope'] == 'complete_bundle_streams'
    assert not trace.index_entries and not (set(trace.inputs) & index_names)
    assert trace.max_readers == 3 and 0 < trace.max_retained <= 3 * 65280 and not trace.live
    expected_names = set()
    for bundle in document['bundles']:
        for role in ('r1', 'r2', 'key', 'r1_gzi', 'r2_gzi'):
            descriptor = bundle[role]
            expected_names.add(descriptor['name'])
            trace.assert_full_file(descriptor)
            if role in ('r1', 'r2', 'key'):
                name = descriptor['name']
                assert trace.visits[name] == [member.physical for member in lanes[name]]
                assert [start for start, _compressed, _decoded in trace.inflations[name]] == trace.visits[name]
                assert trace.codec_hashed[name] == descriptor['bytes'] and trace.codec_updates[name] > 0
                assert trace.closed_stats[name].sha256 == descriptor['sha256']
    assert set(trace.inputs) == expected_names
    assert report.consumed_artifact_bytes == source_manifest.stat().st_size + sum(
        bundle[role]['bytes'] for bundle in document['bundles']
        for role in ('r1', 'r2', 'key', 'r1_gzi', 'r2_gzi'))


@pytest.mark.parametrize('role', ('index', 'r1_gzi', 'r2_gzi'))
def test_indexed_union_validates_required_metadata_in_unselected_bundle(
        union_pool, tmp_path, monkeypatch, role):
    manifest, _expected, (_document, _lanes, entries, _ranges) = union_pool
    source_root = tmp_path / 'source'
    shutil.copytree(manifest.parent, source_root)
    source_manifest = source_root / manifest.name
    document = json.loads(source_manifest.read_bytes())
    bundle = next(bundle for bundle in document['bundles']
                  if all(row[0] not in SELECTED for row in entries[bundle['index']['name']]))
    descriptor = bundle[role]
    original_digest = descriptor['sha256']
    descriptor['sha256'] = '00' * 32
    # Only the outer declared digest changes. Structural parsing must reach the
    # complete required file in a bundle that has no requested original pairs.
    source_manifest.write_text(json.dumps(document, separators=(',', ':')) + '\n')
    trace = IOTrace(monkeypatch)
    destination = tmp_path / 'rejected'
    with pytest.raises(IntegrityError, match='physical length or digest mismatch'):
        recover(recover_config(source_manifest, destination))
    assert trace.index_entries[bundle['index']['name']] == entries[bundle['index']['name']]
    source = trace.sole_input(descriptor['name'])
    assert source.full_hash and source.hash_updates > 0 and source.hashed_bytes == descriptor['bytes']
    assert source.reads[-1][1] == descriptor['bytes'] and not source.finished
    assert original_digest != descriptor['sha256']
    for lane in ('r1', 'r2', 'key'):
        assert not trace.sole_input(bundle[lane]['name']).reads
    assert not trace.live
    assert not list(destination.iterdir())
