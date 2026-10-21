#!/usr/bin/env python3
"""Fresh-process component observations against versioned ReadMixer charges.

This observes representative workloads, not a universal RSS ceiling. Each case
runs in a fresh interpreter, including the identified warm-up and every retained
measurement. The five-run rule applies to these probes too. No private data is
read. Full pipeline contention/host lanes remain separate required evidence.
"""
from __future__ import annotations

import argparse
import gc
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
if (_HERE.parent / 'src').is_dir():
    sys.path.insert(0, str(_HERE.parent / 'src'))

from resource_telemetry import _process_snapshot

CASES = ('parser', 'packed_terminal', 'bulk_tags', 'bgzf', 'scratch_aead', 'metadata', 'resident_catalog', 'memory_handoff')
_NATIVE_LIMITS = ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS',
                  'VECLIB_MAXIMUM_THREADS', 'NUMEXPR_NUM_THREADS')


class _Sampler:
    def __init__(self, interval=0.002):
        self.interval = interval
        self.stop = threading.Event()
        self.peak_rss = self.peak_threads = self.samples = self.errors = 0
        self.observer_seconds = 0.0
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def sample(self):
        with self.lock:
            self._sample_locked()

    def _sample_locked(self):
        before = time.perf_counter()
        facts = _process_snapshot()
        self.samples += 1
        self.errors += facts['error']
        self.peak_rss = max(self.peak_rss, facts['rss_bytes'] or 0)
        self.peak_threads = max(self.peak_threads, facts['threads'] or 0)
        self.observer_seconds += time.perf_counter() - before

    def _run(self):
        while not self.stop.wait(self.interval):
            self.sample()

    def __enter__(self):
        self.thread.start()
        self.sample()
        return self

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join()
        self.sample()


def _prepare(case, scale_bytes, pair_count, scratch, checkpoint):
    """Prepare public fixture inputs before baseline; allocate working sets later."""
    from readmixer_py.crypto import OperationKeys, BulkTagger, pairs_identity
    from readmixer_py.resources import Budget, TERMINAL_OVERHEAD, _registry_bytes
    from readmixer_py import fastq, shuffle, storage, execution, worker_protocol, worker_memory, stage_admission
    from readmixer_py.artifacts import bgzf
    import numpy as np
    import zlib
    keys = OperationKeys.new()
    budget = Budget(512 << 20)
    if case == 'parser':
        maximum = min(scale_bytes, 16 << 20)
        sequence = max(1, (maximum - 9) // 2)
        records = tuple(b'@x/' + str(role).encode() + b'\n' + b'X' * sequence + b'\n+\n' + b'!' * sequence + b'\n' for role in (1, 2))
        originals = tuple(gzip.compress(record, mtime=0) for record in records)
        expected = sum(map(len, records))
        del records
        charge = 2 * (maximum + 5) + (2 << 20)
        def action():
            with fastq.DecodedStream(io.BytesIO(originals[0])) as first, fastq.DecodedStream(io.BytesIO(originals[1])) as second:
                reader = fastq.PairedReader(fastq.RecordFramer(first, line_mode='ingress', max_record_bytes=maximum),
                    fastq.RecordFramer(second, line_mode='ingress', max_record_bytes=maximum), sample_index=0)
                pair = reader.next_pair()
                assert pair is not None and pair.normalized_bytes == expected
                checkpoint()
                del pair
                assert reader.next_pair() is None and reader.report().pair_count == 1
                return dict(pairs=1, normalized_bytes=expected, record_limit=maximum)
        formula = 'mix-parser: 2 * (record_limit + 5) + 2 MiB'
    elif case == 'packed_terminal':
        arena_bytes = min(64 << 20, max(scale_bytes, pair_count * 48))
        count = min(pair_count, arena_bytes // 48)
        charge = TERMINAL_OVERHEAD + arena_bytes + 128 * count
        def action():
            # Touch all arena pages and keep offsets live across the actual
            # production ChaCha ranks/NumPy lexsort. This is a representation
            # probe, not a claim that these artificial arena bytes are frames.
            arena = bytearray(b'~') * arena_bytes
            offsets = np.arange(count + 1, dtype='<u8')
            checkpoint()
            permutation = shuffle.terminal_permutation(keys, (), count)
            assert len(permutation) == count and sorted(map(int, permutation)) == list(range(count))
            assert arena[-1] == 126 and int(offsets[-1]) == count
            checkpoint()
            return dict(pairs=count, arena_bytes=arena_bytes, offsets_bytes=offsets.nbytes)
        formula = 'terminal envelope: TERMINAL_OVERHEAD + arena_bytes + 128 * pairs'
    elif case == 'bulk_tags':
        count = min(pair_count, BulkTagger.MAX_BATCH_SIZE)
        charge = BulkTagger._NATIVE_BYTES + 32 * count + 4096 + BulkTagger._METADATA_PER_PAIR * count
        def action():
            with BulkTagger(keys, budget, batch_size=count) as tagger:
                identities = [(0, number) for number in range(count)]
                tags = tagger.tags(identities)
                assert len(tags) == count and all(len(tag) == 32 for tag in tags)
                assert tags[0] == keys.tag(0, 0) and tags[-1] == keys.tag(0, count - 1)
                checkpoint()
                return dict(pairs=count, tag_bytes=32 * count)
        formula = 'BulkTagger native + packed buffers + 4096 + per-pair metadata'
    elif case == 'bgzf':
        payload = hashlib.shake_256(b'readmixer-python/calibration/bgzf/v1').digest(bgzf.PAYLOAD_BYTES)
        charge = execution.JOB_BYTES
        def action():
            encoded = bgzf.encode_member(payload, 6)
            decoded = zlib.decompress(encoded, wbits=31)
            assert decoded == payload
            checkpoint()
            return dict(decoded_bytes=len(payload), physical_bytes=len(encoded))
        formula = 'CompressionService JOB_BYTES; fixed prepared source payload excluded from RSS baseline delta'
    elif case == 'scratch_aead':
        payload = hashlib.shake_256(b'readmixer-python/calibration/scratch/v1').digest(min(scale_bytes, storage.BLOCK_BYTES))
        # Writer codec work, its 1 MiB block, reader work, directory/store and
        # metadata coexist under actual leases; the explicit bound includes all.
        charge = 2 * storage._WORKSPACE_BYTES + 2 * storage.BLOCK_BYTES + (1 << 20)
        def action():
            with storage.SegmentStore('external', budget, keys, temp_dir=scratch) as store:
                with store.new_segment(1, pairs_identity((0,), 0, 0)) as writer:
                    writer.append(payload, item_count=1)
                    ref = writer.seal()
                checkpoint()
                with store.open_verified(ref) as source:
                    assert source.read(len(payload)) == payload and source.read(1) == b''
                    checkpoint()
                store.drop(ref)
            assert budget.used_bytes == 0
            return dict(decoded_bytes=len(payload), physical_bytes=ref.physical_bytes)
        formula = '2 * segment workspace + 2 * BLOCK_BYTES + 1 MiB metadata/control allowance'
    elif case == 'metadata':
        count = min(pair_count, budget.max_leases)
        charge = 4096 * count + _registry_bytes(count)
        def action():
            owners = [budget.acquire('calibration-metadata', 4096, metadata_bytes=4096) for _ in range(count)]
            try:
                retained_charge = budget.used_metadata_bytes
                checkpoint()
                for index, owner in enumerate(owners):
                    owners[index] = owner.transfer('calibration-metadata-retained')
                checkpoint()
            finally:
                for owner in owners:
                    owner.release()
            assert budget.used_bytes == 0
            return dict(owners=count, charged_resident_metadata_bytes=retained_charge)
        formula = '4096 per live metadata lease plus the actual shared Budget registry formula'
    elif case == 'resident_catalog':
        from readmixer_py.catalog import build_catalog
        # Actual pathname/snapshot/source/sample representation and construction
        # overlap, bounded to 1024 real files. Their creation precedes baseline.
        count = min(pair_count, 512)
        inputs = scratch / 'synthetic-inputs'
        inputs.mkdir(mode=0o700)
        for index in range(count):
            label = f'synthetic{index:06d}' + 'x' * 96
            for role in (1, 2):
                (inputs / f'{label}_R{role}.fastq').write_bytes(f'@x/{role}\nX\n+\n!\n'.encode())
        charge = None  # replace with the actual production-admitted peak below
        def action():
            with build_catalog(input_dir=inputs, budget=budget, max_metadata_bytes=24 << 20) as catalog:
                assert len(catalog) == count
                catalog.close()
                retained = budget.used_metadata_bytes
                peak = budget.peak_metadata_bytes
                assert retained > 0 and budget.used_descriptors == 0
                checkpoint()
                borrowed = catalog.samples[0]
            # Returned aliases keep their real representation reservation after
            # source descriptor and catalog teardown, until final owner release.
            assert borrowed.index == 0 and budget.used_metadata_bytes == retained
            checkpoint()
            del borrowed
            assert budget.used_bytes == 0
            shutil.rmtree(inputs)
            return dict(samples=count, source_files=2 * count,
                        charged_peak_metadata_bytes=peak, charged_retained_metadata_bytes=retained,
                        source_descriptors_closed_before_alias_release=True)
        formula = 'actual shared Budget peak metadata reservation during production build_catalog, including registry overlap'
    else:
        size = min(scale_bytes, worker_protocol.MAX_CHUNK_BYTES)
        charge = stage_admission.CHANNEL_BYTES + 4 * size + (64 << 10)
        def action():
            payload = hashlib.shake_256(b'readmixer-python/calibration/ipc/v1').digest(size)
            owner = worker_memory.MemoryRef(payload, budget.acquire('calibration-sender', size + 4096), bytes(16), 1).bind(2, 3)
            with budget.acquire('calibration-receiver', 3 * size + (64 << 10)):
                encoded = worker_protocol.encode_message(worker_protocol.Chunk(2, 3, 0, owner.payload))
                decoded = worker_protocol.decode_message(encoded)
                receiver = bytearray(decoded.payload)
                assert receiver == payload and not owner.released
                checkpoint()  # sender, receiver, transport frame/copy all coexist
                owner.accept(owner.receipt())
                assert owner.released and bytes(receiver) == payload
            assert budget.used_bytes == 0
            return dict(raw_bytes=size, encoded_bytes=len(encoded), sender_released_after_ack=True)
        formula = 'CHANNEL_BYTES + four bounded raw copies + 64 KiB bookkeeping allowance'
    return action, charge, formula


def observe_case(case, *, scale_bytes=1 << 20, pair_count=32768):
    if case not in CASES or type(scale_bytes) is not int or not 64 <= scale_bytes <= 64 << 20 or type(pair_count) is not int or not 1 <= pair_count <= 32768:
        raise ValueError('calibration parameters exceed bounded probe definitions')
    from readmixer_py import __version__
    from readmixer_py.resources import ENVELOPE_VERSION
    from readmixer_py.stage_admission import BASELINE_VERSION
    with tempfile.TemporaryDirectory(prefix='readmixer-calibration-') as root:
        root = Path(root).resolve()
        sampler = _Sampler()
        action, charge, formula = _prepare(case, scale_bytes, pair_count, Path(root), sampler.sample)
        gc.collect()
        with sampler:
            before = _process_snapshot()
            wall = time.perf_counter()
            cpu = time.process_time()
            facts = action()
            seconds = time.perf_counter() - wall
            cpu_seconds = time.process_time() - cpu
            after = _process_snapshot()
        cleaned = not any(Path(root).iterdir())
    if charge is None:
        charge = facts['charged_peak_metadata_bytes']
    observed_growth = max(0, sampler.peak_rss - (before['rss_bytes'] or sampler.peak_rss))
    return dict(completed=cleaned and not sampler.errors, case=case, package_version=__version__,
        envelope_version=ENVELOPE_VERSION, child_baseline_version=BASELINE_VERSION,
        charged_bytes=charge, charge_formula=formula, observations=facts,
        wall_seconds=seconds, process_cpu_seconds=cpu_seconds,
        before=before, after=after, sampled_peak_rss_bytes=sampler.peak_rss,
        sampled_positive_rss_growth_bytes=observed_growth,
        sampled_growth_exceeds_charge=observed_growth > charge,
        sampled_peak_threads=sampler.peak_threads, samples=sampler.samples,
        sample_errors=sampler.errors, sample_interval_seconds=sampler.interval,
        observer_seconds=sampler.observer_seconds, cleanup_complete=cleaned,
        envelope_qualified=False,
        interpretation='sampled lower bound with explicit live-allocation checkpoints; a no-excess result cannot prove a universal envelope; observer/allocator effects and unmeasured overlap require host/pipeline qualification')


def _child_observation(case, scale_bytes, pair_count, timeout):
    command = [sys.executable, str(Path(__file__).resolve()), '--child', case,
               '--scale-bytes', str(scale_bytes), '--pair-count', str(pair_count)]
    environment = dict(os.environ)
    environment.update({name: '1' for name in _NATIVE_LIMITS})
    # Files bound resident diagnostic memory; no diagnostic bytes are published.
    with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors:
        started = time.perf_counter()
        child = subprocess.Popen(command, stdout=output, stderr=errors, env=environment)
        try:
            child.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait()
            return dict(completed=False, case=case, error='TimeoutExpired', child_reaped=True,
                        elapsed_launch_seconds=time.perf_counter() - started)
        elapsed = time.perf_counter() - started
        output.seek(0)
        data = output.read((1 << 20) + 1)
        if child.returncode or len(data) > 1 << 20:
            return dict(completed=False, case=case, error='ChildFailure', child_reaped=True,
                        returncode=child.returncode, elapsed_launch_seconds=elapsed)
        try:
            result = json.loads(data)
        except (ValueError, TypeError):
            return dict(completed=False, case=case, error='MalformedObservation', child_reaped=True,
                        elapsed_launch_seconds=elapsed)
        result.update(elapsed_launch_seconds=elapsed, child_reaped=True)
        return result


def calibrate(*, repetitions=5, scale_bytes=1 << 20, pair_count=32768, timeout=60, cases=CASES):
    if type(repetitions) is not int or repetitions < 5 or repetitions > 100:
        raise ValueError('calibration requires five through one hundred measured runs')
    if (type(scale_bytes) is not int or not 64 <= scale_bytes <= 64 << 20
            or type(pair_count) is not int or not 1 <= pair_count <= 32768
            or type(timeout) not in (int, float) or not 0 < timeout <= 3600
            or not cases or len(cases) > len(CASES) or len(set(cases)) != len(cases)
            or any(case not in CASES for case in cases)):
        raise ValueError('calibration parameters exceed bounded probe definitions')
    results = []
    for case in cases:
        warmup = _child_observation(case, scale_bytes, pair_count, timeout)
        observations = [_child_observation(case, scale_bytes, pair_count, timeout) for _ in range(repetitions)]
        valid = warmup.get('completed') and all(item.get('completed') for item in observations)
        values = [item['wall_seconds'] for item in observations] if valid else []
        results.append(dict(case=case, completed=bool(valid), warmup=warmup, measured=observations,
            summary=None if not valid else dict(median_seconds=statistics.median(values),
                minimum_seconds=min(values), maximum_seconds=max(values), range_seconds=max(values) - min(values)),
            observed_charge_excess=any(item.get('sampled_growth_exceeds_charge', False) for item in (warmup, *observations))))
    return dict(calibration_version=1, completed=all(item['completed'] for item in results),
        envelope_qualified=False, platform=platform.system(), architecture=platform.machine(),
        python=platform.python_version(), native_limits={name: '1' for name in _NATIVE_LIMITS},
        tool_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        scale_bytes=scale_bytes, pair_count=pair_count, measured_runs=repetitions,
        cache_treatment='fresh interpreter for every observation; OS caches reused; no cache clearing',
        scope='prepared public fixture bytes and imports precede RSS/time baseline; live working sets, validation and owned scratch cleanup are inside action; launch timing includes interpreter/import/observer/cleanup',
        limitations=['sampling can miss peaks', 'allocator/lazy-native state can change observed RSS',
                     'blocked ordered results, process/thread stacks, maximum catalogs and constrained hosts require separate pipeline lanes'],
        cases=results)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--child', choices=CASES, help=argparse.SUPPRESS)
    parser.add_argument('--case', choices=CASES, action='append', dest='cases')
    parser.add_argument('--runs', type=int, default=5)
    parser.add_argument('--scale-bytes', type=int, default=1 << 20)
    parser.add_argument('--pair-count', type=int, default=32768)
    parser.add_argument('--timeout', type=float, default=60)
    args = parser.parse_args(argv)
    try:
        report = (observe_case(args.child, scale_bytes=args.scale_bytes, pair_count=args.pair_count)
                  if args.child else calibrate(repetitions=args.runs, scale_bytes=args.scale_bytes,
                    pair_count=args.pair_count, timeout=args.timeout, cases=tuple(args.cases or CASES)))
    except Exception as error:
        report = dict(completed=False, error=type(error).__name__)
    print(json.dumps(report, sort_keys=True))
    return 0 if report['completed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
