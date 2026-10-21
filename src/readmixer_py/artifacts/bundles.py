"""Greedy synchronized six-file bundles, with bounded current-pair state."""

from contextlib import ExitStack
import hashlib

from .bgzf import BgzfReader, BgzfWriter, GziCursor, build_gzi
from .index import IndexEntry, IndexHeader, encode_entry, encode_header
from .key import KEY_HEADER, encode_key_row, maximum_key_row_bytes, parse_key_row, row_from_pair
from .manifest import artifact_names
from ..errors import IntegrityError, ResourceError
from ..paircodec import decode_pair_view
from ..resources import checked_add


def secured_header(run_id, tag, role):
    return f'@ReadMixer.{run_id}.{tag}/{role}'.encode('ascii')


def secured_record_bytes(pair, run_id, tag, role):
    mate = pair.r1 if role == 1 else pair.r2
    return len(secured_header(run_id, tag, role)) + len(mate.sequence) + len(mate.quality) + 5


def lane_fits(uncompressed_bytes, cap):
    members = (uncompressed_bytes + 65279) // 65280
    return (uncompressed_bytes <= ((cap - 28) // 65536) * 65280
            and 8 + 16 * max(members - 1, 0) <= cap)


def pair_footprint(pair, run_id, tag, secure_index, r1_offset, r2_offset, *, max_record_bytes=1 << 20):
    r1 = secured_record_bytes(pair, run_id, tag, 1)
    r2 = secured_record_bytes(pair, run_id, tag, 2)
    row = row_from_pair(pair, tag, secure_index, r1_offset, r1, r2_offset, r2)
    key = encode_key_row(row, max_record_bytes=max_record_bytes)
    return r1, r2, key


def register_artifact(transaction, artifact, **facts):
    """Independently bind descriptor bytes to the closed, owned file."""
    artifact.close()
    digest = hashlib.sha256()
    size = 0
    with transaction.open_owned_partial(artifact) as reader:
        while chunk := reader.read(1 << 20):
            digest.update(chunk)
            size += len(chunk)
    transaction.register_closed(artifact, size, digest.digest())
    return dict(name=artifact.name, bytes=size, sha256=digest.hexdigest(), **facts)


class BundleEmitter:
    def __init__(self, transaction, keys, sample_counts, *, max_record_bytes,
                 compression_level=6, metadata_bytes=16 << 20, cancel=None, compression_service=None, metadata_lease=None):
        self.transaction, self.keys = transaction, keys
        self.sample_counts = sample_counts
        self.run_id = keys.run_id.hex()
        self.max_record_bytes, self.compression_level = max_record_bytes, compression_level
        self.metadata_bytes, self.cancel = metadata_bytes, cancel
        self.compression_service = compression_service
        self.budget = transaction.budget
        self.cap = transaction.max_artifact_bytes
        self.bundles = []
        self.pairs = 0
        self._active = None
        self._metadata_lease = metadata_lease
        self._owns_metadata = metadata_lease is None
        self._lease = self.budget.acquire('bundle-pair', 16 * max_record_bytes + (1 << 20))
        try:
            if self._owns_metadata:
                self._metadata_lease = self.budget.acquire('bundle-descriptors', 4096, metadata_bytes=4096)
        except BaseException:
            self._lease.release()
            raise

    def _start(self):
        if (len(self.bundles) + 1) * 8192 > self.metadata_bytes:
            raise ResourceError('bundle descriptor metadata allowance exhausted')
        charge = 4096 + (len(self.bundles) + 1) * 8192
        self._metadata_lease.resize(charge, metadata_bytes=charge)
        names = artifact_names(self.run_id, len(self.bundles))
        active = dict(names=names, first=self.pairs, pairs=0, r1_bytes=0, r2_bytes=0,
                      key_bytes=len(KEY_HEADER), artifacts={}, writers={})
        self._active = active
        for role in ('r1', 'r2', 'key'):
            artifact = self.transaction.create_partial(names[role])
            active['artifacts'][role] = artifact
            active['writers'][role] = BgzfWriter(artifact, compression_level=self.compression_level,
                                                 budget=self.budget, cancel=self.cancel, compression_service=self.compression_service)
        active['writers']['key'].write(KEY_HEADER)

    def _fits(self, r1, r2, key):
        active = self._active
        return (lane_fits(active['r1_bytes'] + r1, self.cap)
                and lane_fits(active['r2_bytes'] + r2, self.cap)
                and active['key_bytes'] + len(key) <= ((self.cap - 28) // 65536) * 65280
                and 64 + 40 * (active['pairs'] + 1) <= self.cap)

    def append(self, ordered, *, tag=None):
        if ordered.secure_pair_index != self.pairs:
            raise IntegrityError('emitter received a nondense secured ordinal')
        pair = decode_pair_view(ordered.frame, max_record_bytes=self.max_record_bytes)
        if (pair.sample_index >= len(self.sample_counts)
                or pair.original_pair_index >= self.sample_counts[pair.sample_index]):
            raise IntegrityError('emitter received an out-of-range original identity')
        if tag is None:
            tag = self.keys.tag(pair.sample_index, pair.original_pair_index)
        elif (type(tag) is not str or len(tag) != 32
              or any(character not in '0123456789abcdef' for character in tag)):
            raise IntegrityError('emitter received a malformed coordinator tag')
        if self._active is None:
            self._start()
        r1, r2, key = pair_footprint(pair, self.run_id, tag, self.pairs,
                                    self._active['r1_bytes'], self._active['r2_bytes'], max_record_bytes=self.max_record_bytes)
        if not self._fits(r1, r2, key):
            if not self._active['pairs']:
                raise ResourceError('one complete pair cannot fit an empty bundle')
            self._finish_bundle()
            self._start()
            # Offset decimal widths change at rollover; regenerate all fields.
            r1, r2, key = pair_footprint(pair, self.run_id, tag, self.pairs, 0, 0, max_record_bytes=self.max_record_bytes)
            if not self._fits(r1, r2, key):
                raise ResourceError('one complete pair cannot fit an empty bundle')
        active = self._active
        for role, mate in ((1, pair.r1), (2, pair.r2)):
            writer = active['writers'][f'r{role}']
            for data in (secured_header(self.run_id, tag, role), b'\n', mate.sequence,
                         b'\n+\n', mate.quality, b'\n'):
                writer.write(data)
        active['writers']['key'].write(key)
        active['r1_bytes'] = checked_add(active['r1_bytes'], r1)
        active['r2_bytes'] = checked_add(active['r2_bytes'], r2)
        active['key_bytes'] = checked_add(active['key_bytes'], len(key))
        active['pairs'] = checked_add(active['pairs'], 1)
        self.pairs = checked_add(self.pairs, 1)

    def _finish_bundle(self):
        active = self._active
        if active is None:
            return
        if not active['pairs']:
            raise IntegrityError('pool bundles cannot be empty')
        descriptor = dict(bundle_index=len(self.bundles), first_secure_pair_index=active['first'], pairs=active['pairs'])
        for role in ('r1', 'r2', 'key'):
            stats = active['writers'][role].finish()
            if stats.uncompressed_bytes != active[role + '_bytes']:
                raise IntegrityError('BGZF writer bytes disagree with pair admission')
            descriptor[role] = register_artifact(self.transaction, active['artifacts'][role],
                                                  uncompressed_bytes=stats.uncompressed_bytes,
                                                  records=active['pairs'])
        for role in ('r1', 'r2'):
            sidecar_role = role + '_gzi'
            sidecar = self.transaction.create_partial(active['names'][sidecar_role])
            active['artifacts'][sidecar_role] = sidecar
            with self.transaction.open_owned_partial(active['artifacts'][role]) as source:
                stats = build_gzi(sidecar, source, uncompressed_bytes=descriptor[role]['uncompressed_bytes'],
                                  physical_bytes=descriptor[role]['bytes'], budget=self.budget, cancel=self.cancel)
            descriptor[sidecar_role] = register_artifact(self.transaction, sidecar, entries=stats.entries)
        index = self.transaction.create_partial(active['names']['index'])
        index.write(encode_header(IndexHeader(self.keys.run_id, len(self.bundles), active['first'], active['pairs'])))
        with ExitStack() as stack:
            key_source = stack.enter_context(self.transaction.open_owned_partial(active['artifacts']['key']))
            key_reader = stack.enter_context(BgzfReader(key_source,
                uncompressed_limit=descriptor['key']['uncompressed_bytes'], physical_limit=descriptor['key']['bytes'],
                budget=self.budget, cancel=self.cancel))
            cursors = []
            for role in ('r1', 'r2'):
                source = stack.enter_context(self.transaction.open_owned_partial(active['artifacts'][role + '_gzi']))
                cursors.append(stack.enter_context(GziCursor(source,
                    uncompressed_bytes=descriptor[role]['uncompressed_bytes'], physical_bytes=descriptor[role]['bytes'],
                    expected_entries=descriptor[role + '_gzi']['entries'], budget=self.budget)))
            if key_reader.readline(len(KEY_HEADER)) != KEY_HEADER:
                raise IntegrityError('owned key partial has an invalid header')
            for position in range(active['pairs']):
                start = key_reader.tell_virtual()
                line = key_reader.readline(maximum_key_row_bytes(self.max_record_bytes))
                row = parse_key_row(line, max_record_bytes=self.max_record_bytes, sample_counts=self.sample_counts,
                                    expected_secure_pair_index=active['first'] + position)
                index.write(encode_entry(IndexEntry(row.sample_index, row.original_pair_index,
                    cursors[0].virtual_offset(row.r1_uncompressed_offset),
                    cursors[1].virtual_offset(row.r2_uncompressed_offset), start)))
            if key_reader.read(1):
                raise IntegrityError('owned key partial has extra rows')
            key_reader.finish()
            for cursor in cursors:
                cursor.finish()
        descriptor['index'] = register_artifact(self.transaction, index, entries=active['pairs'])
        self.bundles.append(descriptor)
        self._active = None

    def finish(self):
        self._finish_bundle()
        return self.bundles

    def close(self):
        primary = None
        try:
            if self._active is not None:
                for writer in self._active['writers'].values():
                    try:
                        writer.close()
                    except BaseException as error:
                        if primary is None:
                            primary = error
                        else:
                            primary.add_note('another bundle codec cleanup also failed')
                self._active = None
        finally:
            self._lease.release()
        if primary is not None:
            raise primary

    def dispose(self):
        try:
            self.close()
        finally:
            self.bundles.clear()
            self.sample_counts = ()
            if self._owns_metadata:
                self._metadata_lease.release()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        try:
            if self._owns_metadata:
                self.dispose()
            else:
                self.close()
        except BaseException:
            if exc is None:
                raise
            exc.add_note('bundle codec cleanup also failed')
