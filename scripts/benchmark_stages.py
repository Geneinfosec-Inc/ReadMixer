#!/usr/bin/env python3
"""Correctness-checked, bounded component timings; not an end-to-end qualification.

The public artificial payload and private deterministic key seam are confined to
this harness. No production seed/configuration option is added. One warm-up and
at least five measured observations are retained, including failed observations.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
import gzip
import hashlib
import io
import json
from pathlib import Path
import struct
import sys
import time
from types import SimpleNamespace

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_HERE.parent / 'src'))

import numpy as np
from Crypto.Cipher import AES, ChaCha20
from benchmark_pipeline import (_cpu_result, _reaped_child_cpu, _versions,
                                _workspace, startup_benchmark, summarize)
from generate_fixtures import FixtureConfig, generate_fixtures
from readmixer_py.artifacts.bgzf import BgzfReader, BgzfWriter, GziCursor
from readmixer_py.crypto import BulkTagger, OperationKeys, observations_identity
from readmixer_py.fastq import DecodedStream, PairedReader, RecordFramer
from readmixer_py.paircodec import decode_pair_view, encode_pair_into
from readmixer_py.resources import Budget
from readmixer_py.shuffle import _fill_rank_bytes, bucket_index
from readmixer_py.sorting import ExternalSorter
from readmixer_py.stage_policy import SortPolicy
from readmixer_py.storage import SegmentStore

STAGES = ('parser', 'frame_codec', 'bucket_hash', 'ranks', 'lexsort',
          'tag_rendering', 'scratch_aead_codec', 'bgzf', 'indexed_gather', 'sort_merge')


def _hash(data):
    return hashlib.sha256(data).hexdigest()


def _result(data, items, **witness):
    return dict(sha256=_hash(data), logical_bytes=len(data), items=items, witness=witness)


class _Suite:
    """Small bounded prepared inputs and independent expected wire transcripts."""
    def __init__(self, root, pairs, max_ram):
        self.root, self.pairs, self.max_ram = root, pairs, max_ram
        self.keys = OperationKeys._from_test(bytes(range(32)), bytes(range(16)))
        self.fields = tuple(tuple((f'@artificial-{n:08d}/{mate} opaque'.encode(),
            bytes((33 + n % 80,)) * (n % 101), b'+opaque\x00',
            bytes((120 - n % 80,)) * (n % 101)) for mate in (1, 2)) for n in range(pairs))
        self.record_limit = max(sum(map(len, fields)) + 4 for pair in self.fields for fields in pair)
        self.mates = tuple(b''.join(b'\n'.join(pair[mate]) + b'\n' for pair in self.fields)
                           for mate in (0, 1))
        self.gzip_mates = tuple(gzip.compress(data, mtime=0) for data in self.mates)
        self.normalized = b''.join(b'\n'.join(fields) + b'\n' for pair in self.fields for fields in pair)
        self.frames = tuple(struct.pack('<IIQ8I', 48 + sum(map(len, (*pair[0], *pair[1]))),
            0, n, *(len(line) for line in (*pair[0], *pair[1]))) + b''.join((*pair[0], *pair[1]))
            for n, pair in enumerate(self.fields))
        self.bucket_key = self.keys.bucket_key(())
        self.rank_key = self.keys.rank_key((), pairs, 0)
        self.rank_bytes = ChaCha20.new(key=self.rank_key, nonce=bytes(12)).encrypt(bytes(16 * pairs))
        high = struct.unpack('<' + 'Q' * pairs, self.rank_bytes[:8 * pairs])
        low = struct.unpack('<' + 'Q' * pairs, self.rank_bytes[8 * pairs:])
        permutation = sorted(range(pairs), key=lambda n: (high[n], low[n]))
        cipher = AES.new(self.keys.tag_root, AES.MODE_ECB)
        tags = b''.join(cipher.encrypt(b'RMP1' + struct.pack('>IQ', 0, n)).hex().encode() for n in range(pairs))
        routes = bytes(int.from_bytes(hashlib.blake2b(struct.pack('>IQ', 0, n),
            key=self.bucket_key, digest_size=8).digest(), 'little') % 64 for n in range(pairs))
        self.identities = tuple(struct.pack('>IQ', 0, n) for n in range(pairs))
        # Canonical BGZF preparation uses stdlib zlib, a literal header and
        # literal GZI layout; the measured reader cannot validate itself.
        import zlib
        self.bgzf_wire = bytearray()
        self.gzi_entries = []
        for start in range(0, len(self.mates[0]), 65280):
            payload = self.mates[0][start:start + 65280]
            if start:
                self.gzi_entries.append((len(self.bgzf_wire), start))
            encoder = zlib.compressobj(6, zlib.DEFLATED, -15)
            body = encoder.compress(payload) + encoder.flush()
            self.bgzf_wire.extend(bytes.fromhex('1f8b08040000000000ff060042430200') +
                struct.pack('<H', len(body) + 25) + body + struct.pack('<II', zlib.crc32(payload), len(payload)))
        self.bgzf_wire.extend(bytes.fromhex('1f8b08040000000000ff0600424302001b0003000000000000000000'))
        self.bgzf_wire = bytes(self.bgzf_wire)
        self.gzi = struct.pack('<Q', len(self.gzi_entries)) + b''.join(struct.pack('<QQ', *item) for item in self.gzi_entries)
        self.selected = tuple(range(0, pairs, 3))
        offsets, offset = [], 0
        for pair in self.fields:
            offsets.append(offset)
            offset += sum(map(len, pair[0])) + 4
        self.offsets = offsets
        self.expected = dict(parser=_result(self.normalized, pairs),
            frame_codec=_result(b''.join(self.frames), pairs), bucket_hash=_result(routes, pairs),
            ranks=_result(self.rank_bytes, pairs),
            lexsort=_result(b''.join(struct.pack('<Q', n) for n in permutation), pairs),
            tag_rendering=_result(tags, pairs), scratch_aead_codec=_result(b''.join(self.frames), pairs),
            bgzf=_result(self.mates[0], pairs),
            indexed_gather=_result(b''.join(b'\n'.join(self.fields[n][0]) + b'\n' for n in self.selected), len(self.selected)),
            sort_merge=_result(b''.join(self.identities), pairs))

    def parser(self):
        digest, size, count = hashlib.sha256(), 0, 0
        with ExitStack() as stack:
            framers = []
            for data in self.gzip_mates:
                source = stack.enter_context(DecodedStream(io.BytesIO(data), advertised_gzip=True))
                framers.append(RecordFramer(source, line_mode="ingress", max_record_bytes=self.record_limit))
            reader = PairedReader(*framers, sample_index=0)
            while pair := reader.next_pair():
                for record in (pair.r1, pair.r2):
                    for line in record.lines:
                        digest.update(line)
                        digest.update(b'\n')
                        size += len(line) + 1
                count += 1
            assert reader.report().complete
        return dict(sha256=digest.hexdigest(), logical_bytes=size, items=count, witness={'gzip_trailers_consumed': True})

    def frame_codec(self):
        digest, size = hashlib.sha256(), 0
        arena = bytearray(40 + 2 * self.record_limit)
        for ordinal, fields in enumerate(self.fields):
            pair = SimpleNamespace(sample_index=0, original_pair_index=ordinal,
                r1=SimpleNamespace(lines=fields[0]), r2=SimpleNamespace(lines=fields[1]))
            length = encode_pair_into(pair, arena, max_record_bytes=self.record_limit)
            view = decode_pair_view(memoryview(arena)[:length], max_record_bytes=self.record_limit)
            assert (view.sample_index, view.original_pair_index) == (0, ordinal)
            digest.update(view.frame)
            size += length
        return dict(sha256=digest.hexdigest(), logical_bytes=size, items=self.pairs, witness={})

    def bucket_hash(self):
        return _result(bytes(bucket_index(self.bucket_key, 0, n, 64) for n in range(self.pairs)), self.pairs)

    def ranks(self):
        buffer = bytearray(16 * self.pairs)
        _fill_rank_bytes(memoryview(buffer), self.rank_key)
        return _result(buffer, self.pairs)

    def lexsort(self):
        words = np.frombuffer(self.rank_bytes, dtype='<u8')
        order = np.lexsort((words[self.pairs:], words[:self.pairs]))
        return _result(order.astype('<u8', copy=False).tobytes(), self.pairs)

    def tag_rendering(self):
        budget, digest = Budget(self.max_ram), hashlib.sha256()
        with BulkTagger(self.keys, budget) as tagger:
            for start in range(0, self.pairs, tagger.batch_size):
                for tag in tagger.tags([(0, n) for n in range(start, min(start + tagger.batch_size, self.pairs))]):
                    digest.update(tag.encode('ascii'))
        assert budget.active_leases == 0
        return dict(sha256=digest.hexdigest(), logical_bytes=32 * self.pairs, items=self.pairs, witness={'all_leases_released': True})

    def scratch_aead_codec(self):
        budget, digest, size = Budget(self.max_ram), hashlib.sha256(), 0
        with SegmentStore('external', budget, self.keys, temp_dir=self.root) as store:
            with store.new_segment(5, observations_identity('verify', 0)) as writer:
                for frame in self.frames:
                    writer.append(frame, item_count=1)
                ref = writer.seal()
            physical = (store.scratch_directory / (ref.segment_id.hex() + '.segment')).stat().st_size
            with store.open_verified(ref) as source:
                while block := source.read(65536):
                    digest.update(block)
                    size += len(block)
                assert source.verified
            store.drop(ref)
        assert budget.active_leases == 0
        return dict(sha256=digest.hexdigest(), logical_bytes=size, items=self.pairs,
                    witness=dict(ciphertext_physical_bytes=physical, authenticated_eof=True, all_leases_released=True))

    def bgzf(self):
        output = io.BytesIO()
        with BgzfWriter(output) as writer:
            writer.write(self.mates[0])
            writer.finish()
        raw = output.getvalue()
        # The independent decoder validates the actual measured encoder output.
        return _result(gzip.decompress(raw), self.pairs, physical_bytes=len(raw))

    def indexed_gather(self):
        digest, size, members = hashlib.sha256(), 0, []
        decoded = 0
        class ObservedReader(BgzfReader):
            @staticmethod
            def _inflate(payload, footer):
                nonlocal decoded
                decoded += 1
                return BgzfReader._inflate(payload, footer)
        with GziCursor(io.BytesIO(self.gzi), uncompressed_bytes=len(self.mates[0]),
                physical_bytes=len(self.bgzf_wire)) as cursor:
            with ObservedReader(io.BytesIO(self.bgzf_wire), uncompressed_limit=len(self.mates[0]),
                    physical_limit=len(self.bgzf_wire), indexed=True,
                    on_member=lambda info: members.append(info.compressed_start)) as reader:
                for n in self.selected:
                    record = reader.read_record_at(cursor.virtual_offset(self.offsets[n]), self.record_limit)
                    digest.update(record)
                    size += len(record)
                payload_read = reader.bytes_read
            cursor.finish()
        assert len(set(members)) == len(members)
        return dict(sha256=digest.hexdigest(), logical_bytes=size, items=len(self.selected),
                    witness=dict(scanned_members=len(members), decoded_members=decoded, physical_bytes_read=payload_read,
                                 selected_records=len(self.selected), full_sidecar_consumed=True))

    def sort_merge(self):
        budget, digest, count = Budget(self.max_ram), hashlib.sha256(), 0
        with SegmentStore('external', budget, self.keys, temp_dir=self.root) as store:
            with ExternalSorter(store, policy=SortPolicy.COVERAGE_IDENTITIES,
                    run_bytes=512, max_record_bytes=12, fan_in=2) as sorter:
                for record in reversed(self.identities):
                    sorter.append(record)
                with sorter.iter_sorted() as records:
                    for record in records:
                        digest.update(record)
                        count += 1
                passes = sorter.merge_passes
        assert budget.active_leases == 0
        return dict(sha256=digest.hexdigest(), logical_bytes=12 * count, items=count,
                    witness=dict(merge_passes=passes, fan_in=2, all_leases_released=True))


def _observe(action, expected):
    before = _reaped_child_cpu()
    wall, cpu = time.perf_counter(), time.process_time()
    actual, error = None, None
    try:
        actual = action()
    except Exception as exc:
        # Do not serialize exception messages, paths, tracebacks or payloads.
        error = type(exc).__name__
    elapsed, used = time.perf_counter() - wall, time.process_time() - cpu
    completed = error is None and all(actual.get(key) == expected[key] for key in ('sha256', 'logical_bytes', 'items'))
    if not completed and error is None:
        error = 'independent_digest_or_count_mismatch'
    return dict(completed=completed, error=error, wall_seconds=elapsed,
                **_cpu_result(used, before, _reaped_child_cpu()), result=actual)


def benchmark_stages(*, pairs=256, repetitions=5, work_dir=None, startup=False, max_ram=512 << 20):
    if type(pairs) is not int or not 16 <= pairs <= 32768:
        raise ValueError('component fixture requires 16..32768 pairs')
    if type(repetitions) is not int or not 5 <= repetitions <= 1000:
        raise ValueError('component benchmark requires 5..1000 measured repetitions')
    if type(startup) is not bool or type(max_ram) is not int or max_ram < 64 << 20:
        raise ValueError('invalid component startup or RAM configuration')
    with _workspace(work_dir) as root:
        suite = _Suite(root, pairs, max_ram)
        lanes = {}
        for name in STAGES:
            action, expected = getattr(suite, name), suite.expected[name]
            warmup = _observe(action, expected)
            measured = [_observe(action, expected) for _ in range(repetitions)]
            completed = all(row['completed'] for row in (warmup, *measured))
            summary = None if not completed else dict(wall_seconds=summarize(row['wall_seconds'] for row in measured),
                process_cpu_seconds=summarize(row['process_cpu_seconds'] for row in measured))
            lanes[name] = dict(completed=completed, warmup=warmup, measured_runs=measured,
                               summary=summary, expected=expected)
        startup_report = None
        if startup:
            fixture = generate_fixtures(root / 'startup-fixture', FixtureConfig(counts=(1, 1, 0)))
            startup_report = startup_benchmark(root / 'startup-fixture' / 'input', fixture,
                                               repetitions=repetitions, max_ram=max_ram)
        residue = [path for path in root.iterdir() if path.name != 'startup-fixture']
        report = dict(stage_benchmark_version=1, completed=not residue and all(lane['completed'] for lane in lanes.values())
            and (startup_report is None or startup_report['completed']), versions=_versions(),
            tooling_sha256=_hash(Path(__file__).read_bytes()), pairs=pairs, stages=lanes, startup=startup_report,
            cache_policy='one warm-up per lane; prepared inputs and OS caches reused; no cache clearing',
            timing_scope='component initialization, operation, streaming digest/count checks and component cleanup included; fixture/oracle preparation and final comparison excluded',
            parser_scope='gzip paired framing from prepared in-memory compressed streams; no catalog, original-file I/O or worker transport',
            indexed_gather_scope='canonical indexed BGZF plus GZI component; full multi-sample union scanning belongs to integrated workflows',
            sort_scope='closed identity policy, external protected run generation and fan-in-two merge with witnessed passes',
            startup_scope='separate existing fresh interpreter/API import/module CLI-inspect lanes when requested',
            limitations=['bounded component timings are not integrated throughput or resource qualification',
                         'component budgets do not include harness-prepared arrays and independent oracle buffers',
                         'fixed context is a private harness seam, never a production seed',
                         'component timings include checksum and allocation overhead'],
            scratch_cleanup_complete=not residue, artifacts_retained=work_dir is not None)
    report['temporary_workspace_removed'] = work_dir is None
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pairs', type=int, default=256)
    parser.add_argument('--repetitions', type=int, default=5)
    parser.add_argument('--work-dir', type=Path)
    parser.add_argument('--startup', action='store_true')
    args = parser.parse_args(argv)
    try:
        report = benchmark_stages(**vars(args))
    except KeyboardInterrupt:
        print(json.dumps(dict(completed=False, error='interrupted')))
        return 130
    except Exception as exc:
        print(json.dumps(dict(completed=False, error=type(exc).__name__)))
        return 1
    print(json.dumps(report, sort_keys=True))
    return 0 if report['completed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
