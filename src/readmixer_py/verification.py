"""Complete independent pool verification, including original-domain bijections."""

from dataclasses import dataclass
import hashlib
from pathlib import Path
import struct
import tempfile
import time

from .catalog import build_catalog
from .config import VerifyConfig
from .crypto import OperationKeys
from .errors import ConfigurationError, IntegrityError, ResourceError
from .fastq import PairedReader, RecordFramer, open_decoded_stream
from .metadata import discard_failed_frames
from .paircodec import decode_pair_view, maximum_frame_bytes
from .pool import Pool
from .resources import ResourcePlan, checked_add
from .runtime import checkpoint, operation_resources, stage_execution
from .execution import resolve_cpu_budget
from .stage_policy import StagePolicy, SortPolicy
from .scan import PoolScanner
from .sorting import ExternalSorter
from .storage import SegmentStore


@dataclass(frozen=True)
class VerifyReport:
    report_version: int
    operation: str
    completed: bool
    run_id: str
    source_manifest_sha256: str
    validation_scope: str
    samples: int
    pairs: int
    reads: int
    bundles: int
    artifacts: int
    normalized_bytes: int
    consumed_artifact_bytes: int
    resources: ResourcePlan
    workers_effective: int
    elapsed_seconds: float


def frame_identity(frame):
    return struct.unpack_from('<IQ', frame, 4)


def _compare_originals(config, manifest, budget, plan, record_limit):
    metadata = min(plan.metadata_bytes, 16 << 20)
    with build_catalog(samples=config.originals, max_metadata_bytes=metadata, budget=budget, cancel=config.cancel) as catalog:
        if len(catalog) != len(manifest['samples']):
            raise IntegrityError('original sample catalog differs from the pool')
        with budget.acquire('verify-original-parser', 2 * (record_limit + 5) + (2 << 20), descriptors=2):
            for sample, expected in zip(catalog, manifest['samples']):
                if sample.sample_id != expected['sample_id'] or sample.index != expected['sample_index']:
                    raise IntegrityError('original sample identities differ from the pool')
                checkpoint(config, budget, plan)
                with catalog.open_input(sample.r1) as raw1, catalog.open_input(sample.r2) as raw2:
                    with open_decoded_stream(raw1, advertised_gzip=sample.r1.advertised_gzip) as stream1, \
                         open_decoded_stream(raw2, advertised_gzip=sample.r2.advertised_gzip) as stream2:
                        reader = PairedReader(
                            RecordFramer(stream1, line_mode='ingress', max_record_bytes=record_limit),
                            RecordFramer(stream2, line_mode='ingress', max_record_bytes=record_limit), sample_index=sample.index)
                        while reader.next_pair(config.cancel) is not None:
                            if reader.pair_count % 4096 == 0:
                                checkpoint(config, budget, plan)
                        report = reader.report()
                        del reader
                if not report.complete or report.pair_count != expected['pairs']:
                    raise IntegrityError('original pair count differs from the verified pool')
                for role in (1, 2):
                    source = expected[f'r{role}']
                    if (getattr(report, f'r{role}_normalized_bytes') != source['normalized_bytes']
                            or getattr(report, f'r{role}_sha256') != source['normalized_sha256']):
                        raise IntegrityError('normalized originals differ from the verified pool')


@discard_failed_frames
def verify(config):
    if not isinstance(config, VerifyConfig):
        raise ConfigurationError('verify requires VerifyConfig')
    started = time.monotonic()
    plan, budget = operation_resources(config)
    cpu = resolve_cpu_budget(config.workers)
    workers_effective = 1
    metadata = min(plan.metadata_bytes, 16 << 20)
    with budget.acquire('verify-control-and-report', 16384, metadata_bytes=16384), \
         Pool(config.manifest, budget, metadata_bytes=metadata, cancel=config.cancel) as pool:
        manifest = pool.manifest
        limit = min(config.max_record_bytes or manifest['limits']['max_input_record_bytes'],
                    manifest['limits']['max_input_record_bytes'])
        plan.require_execution(limit)
        pool.preflight(indexes=True)
        normalized = pairs = 0
        with SegmentStore(config.storage.value, budget, OperationKeys.new(),
                          temp_dir=config.temp_dir or Path(tempfile.gettempdir()).resolve(),
                          input_root=pool.root.path, cancel=config.cancel) as store, \
             stage_execution(config, plan, budget, store, cpu, operation='verify',
                 task_count=manifest['totals']['pairs'], record_bytes=limit) as stage:
            workers_effective = stage.workers_effective if stage is not None else 1
            with ExternalSorter(store, policy=StagePolicy(SortPolicy.ORIGINAL_FRAMES, limit), run_bytes=max(8 << 20, 2 * maximum_frame_bytes(limit)),
                                max_record_bytes=maximum_frame_bytes(limit), sort_purpose='verify-originals',
                                cancel=config.cancel) as bodies, \
                 ExternalSorter(store, policy=SortPolicy.TAG_OBSERVATIONS, run_bytes=1 << 20,
                                max_record_bytes=24, sort_purpose='verify-tags', cancel=config.cancel) as tags:
                with PoolScanner(pool, max_record_bytes=limit, cancel=config.cancel) as scanner:
                    for frame, tag in scanner.sequential(indexes=True, prove_greedy=True):
                        normalized = checked_add(normalized, len(frame) - 40)
                        if config.max_total_uncompressed is not None and normalized > config.max_total_uncompressed:
                            raise ResourceError('verified normalized bytes exceed max_total_uncompressed')
                        bodies.append(frame)
                        tags.append(bytes.fromhex(tag) + pairs.to_bytes(8, 'big'))
                        pairs = checked_add(pairs, 1)
                        del frame, tag
                        if pairs % 4096 == 0:
                            checkpoint(config, budget, plan)
                if pairs != manifest['totals']['pairs']:
                    raise IntegrityError('complete pool pair count disagrees with the manifest')
                with bodies.iter_sorted() as frames:
                    for sample in manifest['samples']:
                        digests = [hashlib.sha256(), hashlib.sha256()]
                        sizes = [0, 0]
                        for ordinal in range(sample['pairs']):
                            frame = next(frames, None)
                            if frame is None or frame_identity(frame) != (sample['sample_index'], ordinal):
                                raise IntegrityError('original pair domain has a gap, duplicate, or substitution')
                            pair = decode_pair_view(frame, max_record_bytes=limit)
                            for role, record in enumerate((pair.r1, pair.r2)):
                                for line in record.lines:
                                    digests[role].update(line)
                                    digests[role].update(b'\n')
                                    sizes[role] += len(line) + 1
                            # Every field is a borrowed view into this frame.
                            # Release all views before advancing or closing the sort.
                            del line, record, pair, frame
                            if ordinal % 4096 == 0:
                                checkpoint(config, budget, plan)
                        for role in (1, 2):
                            source = sample[f'r{role}']
                            if sizes[role - 1] != source['normalized_bytes'] or digests[role - 1].hexdigest() != source['normalized_sha256']:
                                raise IntegrityError('reconstructed original digest or byte total disagrees with the manifest')
                    if next(frames, None) is not None:
                        raise IntegrityError('original pair domain has excess records')
                bodies.close()
                with tags.iter_sorted() as records:
                    previous = None
                    observed = 0
                    for row in records:
                        if row[:16] == previous:
                            raise IntegrityError('pool contains a duplicated secured pair tag')
                        previous = row[:16]
                        observed += 1
                    if observed != pairs:
                        raise IntegrityError('tag domain does not cover all secured pairs')
        if config.originals is not None:
            _compare_originals(config, manifest, budget, plan, limit)
        checkpoint(config, budget, plan)
        return VerifyReport(1, 'verify', True, manifest['run_id'], pool.source_manifest_sha256,
                            'pool_and_originals' if config.originals is not None else 'pool_complete',
                            manifest['totals']['samples'], pairs, 2 * pairs, manifest['totals']['bundles'],
                            manifest['totals']['artifacts'], normalized, pool.consumed_bytes, plan, workers_effective,
                            time.monotonic() - started)
