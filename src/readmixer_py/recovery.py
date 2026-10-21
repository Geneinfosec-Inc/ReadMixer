"""One union gather, original-order reconstruction, and receipt-last recovery."""

from dataclasses import dataclass, replace
import hashlib
import struct
import time

from .artifacts.bgzf import BgzfWriter, build_gzi, encode_member
from .artifacts.bundles import lane_fits, register_artifact
from .artifacts.key import secured_record_limit
from .artifacts.manifest import bounded_json_bytes
from .config import RecoverConfig, RecoveryMode
from .crypto import OperationKeys
from .disk import DiskBudget
from .errors import ConfigurationError, IntegrityError, ResourceError
from .execution import CompressionService, resolve_cpu_budget
from .metadata import discard_failed_frames, serialization_lease
from .paircodec import decode_pair_view, maximum_frame_bytes
from .pool import Pool
from .resources import ResourcePlan, checked_add
from .runtime import checkpoint, codec_description, operation_resources, prepare_destination
from .scan import PoolScanner
from .sorting import ExternalSorter
from .stage_policy import StagePolicy, SortPolicy
from .runtime import stage_execution
from .transaction import OutputTransaction


@dataclass(frozen=True)
class RecoverReport:
    report_version: int
    operation: str
    completed: bool
    pool_run_id: str
    recovery_id: str
    source_manifest_sha256: str
    receipt: str
    io_mode: str
    index_validation: str
    validation_scope: str
    samples: int
    pairs: int
    reads: int
    parts: int
    artifacts: int
    normalized_bytes: int
    physical_bytes: int
    consumed_artifact_bytes: int
    outputs: tuple[dict, ...]
    resources: ResourcePlan
    workers_effective: int
    cpu_budget: int
    compression_slots: int
    cleanup_complete: bool
    warnings: tuple[str, ...]
    elapsed_seconds: float


def _identity(frame):
    return struct.unpack_from('<IQ', frame, 4)


class _RecoveryEmitter:
    """Only two mate writers live at once; descriptors have a bounded RAM owner."""

    def __init__(self, transaction, config, pool_run_id, recovery_id, metadata_bytes, compression_service,
                 *, metadata_lease=None):
        self.transaction, self.config = transaction, config
        self.compression_service = compression_service
        self.pool_run_id, self.recovery_id = pool_run_id, recovery_id
        self.budget = transaction.budget
        self.cap = transaction.max_artifact_bytes
        self.metadata_bytes, self._metadata_used = metadata_bytes, 4096
        self._owns_metadata = metadata_lease is None
        self._lease = metadata_lease if metadata_lease is not None else self.budget.acquire(
            'recovery-output-metadata', 4096, metadata_bytes=4096)
        self.samples, self.outputs = [], []
        self.parts = self.pairs = self.normalized_bytes = self.physical_bytes = 0
        self._sample = self._active = None

    def _admit_metadata(self, amount):
        if amount > self.metadata_bytes - self._metadata_used:
            raise ResourceError('recovery descriptor metadata allowance exhausted')
        charge = self._metadata_used + amount
        self._lease.resize(charge, metadata_bytes=charge)
        self._metadata_used = charge

    def start_sample(self, expected):
        if self._sample is not None:
            raise IntegrityError('recovery sample output overlaps an unfinished sample')
        self._admit_metadata(2048 + len(expected['sample_id']))
        self._expected = expected
        self._sample = dict(sample_index=expected['sample_index'], sample_id=expected['sample_id'],
                            pairs=0, reads=0, parts=[])
        self._hashes = [hashlib.sha256(), hashlib.sha256()]
        self._bytes = [0, 0]
        self._start_part()  # Selected empty samples deliberately receive four files.

    def _start_part(self):
        self._admit_metadata(8192)
        if 4 * (self.parts + 1) + 1 > self.transaction.max_artifacts:
            raise ResourceError('recovery artifact inventory exceeds its admitted ceiling')
        sample_index = self._sample['sample_index']
        part_index = len(self._sample['parts'])
        prefix = f'recover.{self.pool_run_id}.{self.recovery_id}.s{sample_index:06d}.p{part_index:06d}'
        active = dict(part_index=part_index, pairs=0, bytes=[0, 0], artifacts={}, writers={}, names={})
        self._active = active
        for role in ('r1', 'r2'):
            name = prefix + f'.{role.upper()}.fastq.gz'
            active['names'][role] = name
            active['names'][role + '_gzi'] = name + '.gzi'
            artifact = self.transaction.create_partial(name)
            active['artifacts'][role] = artifact
            active['writers'][role] = BgzfWriter(artifact, compression_level=self.config.compression_level,
                                                budget=self.budget, cancel=self.config.cancel,
                                                compression_service=self.compression_service)

    def append(self, pair):
        checkpoint(self.config)
        if pair.sample_index != self._sample['sample_index'] or pair.original_pair_index != self._sample['pairs']:
            raise IntegrityError('selected original identities have a gap, duplicate, or wrong sample')
        sizes = (pair.r1.normalized_bytes, pair.r2.normalized_bytes)
        if not all(lane_fits(previous + size, self.cap) for previous, size in zip(self._active['bytes'], sizes)):
            if not self._active['pairs']:
                raise ResourceError('one restored pair cannot fit an empty recovery part')
            self._finish_part()
            self._start_part()
            if not all(lane_fits(size, self.cap) for size in sizes):
                raise ResourceError('one restored pair cannot fit an empty recovery part')
        for role_index, mate in enumerate((pair.r1, pair.r2)):
            writer = self._active['writers'][f'r{role_index + 1}']
            for line in mate.lines:
                writer.write(line)
                writer.write(b'\n')
                self._hashes[role_index].update(line)
                self._hashes[role_index].update(b'\n')
            self._active['bytes'][role_index] = checked_add(self._active['bytes'][role_index], sizes[role_index])
            self._bytes[role_index] = checked_add(self._bytes[role_index], sizes[role_index])
        self._active['pairs'] += 1
        self._sample['pairs'] += 1
        self._sample['reads'] += 2
        self.pairs = checked_add(self.pairs, 1)
        self.normalized_bytes = checked_add(self.normalized_bytes, sum(sizes))

    def _finish_part(self):
        active = self._active
        descriptor = dict(part_index=active['part_index'], pairs=active['pairs'])
        for role_index, role in enumerate(('r1', 'r2')):
            stats = active['writers'][role].finish()
            if stats.uncompressed_bytes != active['bytes'][role_index]:
                raise IntegrityError('restored writer bytes disagree with admitted pair lengths')
            descriptor[role] = register_artifact(self.transaction, active['artifacts'][role],
                                                  uncompressed_bytes=stats.uncompressed_bytes,
                                                  records=active['pairs'])
        for role in ('r1', 'r2'):
            sidecar = self.transaction.create_partial(active['names'][role + '_gzi'])
            active['artifacts'][role + '_gzi'] = sidecar
            with self.transaction.open_owned_partial(active['artifacts'][role]) as source:
                stats = build_gzi(sidecar, source, uncompressed_bytes=descriptor[role]['uncompressed_bytes'],
                                  physical_bytes=descriptor[role]['bytes'], budget=self.budget, cancel=self.config.cancel)
            descriptor[role + '_gzi'] = register_artifact(self.transaction, sidecar, entries=stats.entries)
        self._sample['parts'].append(descriptor)
        # Public report descriptors include only numeric/generated identifiers and
        # physical artifacts; private source IDs/digests stay in the receipt.
        self.outputs.append(dict(sample_index=self._sample['sample_index'], **descriptor))
        self.physical_bytes = checked_add(self.physical_bytes, sum(descriptor[role]['bytes'] for role in ('r1', 'r1_gzi', 'r2', 'r2_gzi')))
        self.parts += 1
        self._active = None

    def finish_sample(self):
        if self._sample['pairs'] != self._expected['pairs']:
            raise IntegrityError('recovery did not reconstruct every selected original ordinal')
        for role_index, role in enumerate(('r1', 'r2')):
            digest = self._hashes[role_index].hexdigest()
            source = self._expected[role]
            if self._bytes[role_index] != source['normalized_bytes'] or digest != source['normalized_sha256']:
                raise IntegrityError('restored selected source length or normalized digest disagrees')
            self._sample[role] = dict(normalized_bytes=self._bytes[role_index], normalized_sha256=digest)
        self._finish_part()
        self.samples.append(self._sample)
        self._sample = None

    def close_codecs(self):
        primary = None
        try:
            if self._active is not None:
                for writer in self._active['writers'].values():
                    try:
                        writer.close(primary)
                    except BaseException as exc:
                        if primary is None:
                            primary = exc
                self._active = None
        finally:
            self._active = None
        if primary is not None:
            raise primary

    def close(self):
        try:
            self.close_codecs()
        finally:
            # Shared descriptor dictionaries may have transferred to the caller
            # through the return value; drop only our own references.
            self.samples.clear()
            self.outputs.clear()
            self._sample = self._expected = None
            if getattr(self, '_owns_metadata', True):
                self._lease.release()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        try:
            self.close()
        except BaseException:
            if exc is None:
                raise
            exc.add_note('recovery codec cleanup also failed; primary error preserved')


@discard_failed_frames
def recover(config):
    if not isinstance(config, RecoverConfig):
        raise ConfigurationError('recover requires RecoverConfig')
    started = time.monotonic()
    plan, budget = operation_resources(config)
    cpu = resolve_cpu_budget(config.workers)
    metadata = min(plan.metadata_bytes, 16 << 20)
    # Report descriptors remain admitted through emitter, transaction, pool and
    # disk cleanup. The outermost exit hands them to the returning caller.
    with budget.acquire('recovery-output-metadata', 4096, metadata_bytes=4096) as output_metadata, \
         DiskBudget(budget=budget) as disk, \
         Pool(config.manifest, budget, metadata_bytes=metadata, cancel=config.cancel) as pool:
        manifest = pool.manifest
        limit = min(config.max_record_bytes or manifest['limits']['max_input_record_bytes'],
                    manifest['limits']['max_input_record_bytes'])
        plan.require_execution(limit)
        cap = config.max_artifact_bytes or manifest['limits']['max_artifact_bytes']
        if cap > manifest['limits']['max_artifact_bytes']:
            raise ConfigurationError('recovery artifact cap cannot exceed the source pool cap')
        metadata_cap = min(metadata, cap, 64 << 20)
        indexed = config.io is RecoveryMode.INDEXED
        mode = config.io.value
        index_validation = 'validated' if indexed else 'explicitly_bypassed'
        scope = 'selected_records' if indexed else 'complete_bundle_streams'
        selection_charge = 16384 + 512 * len(manifest['samples'])
        with budget.acquire('recovery-selection-and-report', selection_charge, metadata_bytes=selection_charge):
            requested = set(config.sample)
            selected = tuple(sample['sample_index'] for sample in manifest['samples'] if sample['sample_id'] in requested)
            if len(selected) != len(requested):
                raise ConfigurationError('one or more selected samples are absent from the pool')
            selected_set = set(selected)
            expected_pairs = sum(manifest['samples'][index]['pairs'] for index in selected)
            pool.preflight(indexes=indexed)
            keys = OperationKeys.new()
            recovery_id = keys.run_id.hex()
            marker = f'recover.{manifest["run_id"]}.{recovery_id}.receipt.json'
            prepare_destination(config.output, input_root=pool.root.path)
            with OutputTransaction.operation(config.output, 'recovery-receipt', budget,
                    keys=keys, storage_mode=config.storage.value, temp_dir=config.temp_dir,
                    disk_budget=disk, input_root=pool.root.path, max_artifact_bytes=cap,
                    metadata_bytes=metadata, completion_basename=marker,
                    cancel=config.cancel) as transaction:
                store = transaction.store
                with stage_execution(config, plan, budget, store, cpu, operation='recover',
                         task_count=expected_pairs, record_bytes=limit) as stage, \
                     CompressionService(budget, cpu, encoder=encode_member, cancel=config.cancel) as compression, \
                     _RecoveryEmitter(transaction, config, manifest['run_id'], recovery_id, metadata, compression,
                                      metadata_lease=output_metadata) as emitter:
                    with ExternalSorter(store, policy=StagePolicy(SortPolicy.ORIGINAL_FRAMES, limit), run_bytes=max(8 << 20, 2 * maximum_frame_bytes(limit)),
                                        max_record_bytes=maximum_frame_bytes(limit), sort_purpose='recover-originals',
                                        cancel=config.cancel) as sorter:
                        normalized = gathered = 0
                        with PoolScanner(pool, max_record_bytes=limit, cancel=config.cancel) as scanner:
                            source = scanner.indexed(selected_set) if indexed else scanner.sequential(indexes=False, selected=selected_set)
                            primary = None
                            try:
                                for frame, _tag in source:
                                    try:
                                        normalized = checked_add(normalized, len(frame) - 40)
                                        if config.max_total_uncompressed is not None and normalized > config.max_total_uncompressed:
                                            raise ResourceError('selected restored bytes exceed max_total_uncompressed')
                                        sorter.append(frame)
                                        gathered = checked_add(gathered, 1)
                                        if gathered % 4096 == 0:
                                            checkpoint(config, budget, plan)
                                    finally:
                                        del frame
                            except BaseException as error:
                                primary = error
                                raise
                            finally:
                                # A retained exception traceback must not keep
                                # the suspended scanner and its input leases alive.
                                try:
                                    source.close()
                                except BaseException:
                                    if primary is None:
                                        raise
                                    primary.add_note('recovery source cleanup also failed; primary error preserved')
                        if gathered != expected_pairs:
                            raise IntegrityError('gathered selected pair counts differ from the manifest')
                        with sorter.iter_sorted() as records:
                            current = next(records, None)
                            try:
                                for sample_index in selected:
                                    emitter.start_sample(manifest['samples'][sample_index])
                                    while current is not None and _identity(current)[0] == sample_index:
                                        pair = decode_pair_view(current, max_record_bytes=limit)
                                        try:
                                            emitter.append(pair)
                                        finally:
                                            del pair
                                        current = next(records, None)
                                    emitter.finish_sample()
                                if current is not None:
                                    raise IntegrityError('sorted recovery contains an unexpected selected domain')
                            finally:
                                current = None
                        if emitter.pairs != gathered or emitter.normalized_bytes != normalized:
                            raise IntegrityError('recovery emission disagrees with gathered paired content')
                    compression.close()
                    if stage is not None:
                        stage.close()
                    # Every source seal and selected digest is proved, and all
                    # data scratch is removed, before the receipt can commit.
                    # Authenticated ownership pages survive through publication.
                    receipt = dict(format='readmixer-python-recovery', schema_version=1, normalization_version=1,
                        pool_run_id=manifest['run_id'], recovery_id=recovery_id,
                        source_manifest_sha256=pool.source_manifest_sha256, io_mode=mode,
                        index_validation=index_validation, validation_scope=scope,
                        limits=dict(max_artifact_bytes=cap, max_artifacts=transaction.max_artifacts,
                                    max_input_record_bytes=limit, max_secured_record_bytes=secured_record_limit(limit),
                                    max_metadata_bytes=metadata_cap),
                        codec=codec_description(config.compression_level), samples=emitter.samples,
                        totals=dict(samples=len(selected), pairs=emitter.pairs, reads=2 * emitter.pairs,
                                    parts=emitter.parts, artifacts=4 * emitter.parts + 1))
                    with serialization_lease(budget, receipt, max_bytes=metadata_cap):
                        encoded = bounded_json_bytes(receipt, max_bytes=metadata_cap, max_parsed_bytes=budget.metadata_limit_bytes)
                        try:
                            report = RecoverReport(1, 'recover', True, manifest['run_id'], recovery_id,
                                pool.source_manifest_sha256, marker, mode, index_validation, scope,
                                len(selected), emitter.pairs, 2 * emitter.pairs, emitter.parts, 4 * emitter.parts + 1,
                                emitter.normalized_bytes, emitter.physical_bytes + len(encoded), pool.consumed_bytes,
                                tuple(emitter.outputs), plan, stage.workers_effective if stage is not None else 1, compression.actual_cpu_slots, compression.slots,
                                True, (), time.monotonic() - started)
                            checkpoint(config, budget, plan)
                            pool.close_sources()
                            publication = transaction.publish_all(encoded)
                            return replace(report, cleanup_complete=publication.cleanup_complete,
                                           warnings=publication.warnings, elapsed_seconds=time.monotonic() - started)
                        finally:
                            del encoded
