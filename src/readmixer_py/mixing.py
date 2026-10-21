"""Single-pass paired ingress, canonical shuffle, and manifest-last emission."""

from contextlib import ExitStack
from dataclasses import dataclass, replace
import os
import time

from . import __version__
from .artifacts.bgzf import encode_member
from .artifacts.bundles import BundleEmitter
from .artifacts.manifest import encode_manifest, manifest_name
from .catalog import build_catalog
from .config import MixConfig
from .crypto import OperationKeys
from .disk import DiskBudget
from .errors import ConfigurationError, InputError, ResourceError
from .execution import CompressionService, resolve_cpu_budget
from .fastq import PairedReader, RecordFramer, open_decoded_stream
from .metadata import discard_failed_frames, serialization_lease
from .resources import ResourcePlan, checked_add, checked_multiply
from .runtime import checkpoint, codec_description, execution_description, operation_resources, prepare_destination, stage_execution
from .shuffle import SerialShuffler
from .transaction import OutputTransaction


@dataclass(frozen=True)
class MixReport:
    report_version: int
    operation: str
    completed: bool
    run_id: str
    manifest: str
    samples: int
    pairs: int
    reads: int
    normalized_bytes: int
    physical_bytes: int
    bundles: int
    artifacts: int
    resources: ResourcePlan
    workers_effective: int
    cpu_budget: int
    compression_slots: int
    cleanup_complete: bool
    warnings: tuple[str, ...]
    elapsed_seconds: float


def mix(config):
    if not isinstance(config, MixConfig):
        raise ConfigurationError('mix requires MixConfig')
    return _mix(config)


@discard_failed_frames
def _mix(config, *, _keys=None, _scatter_strategy="auto", _scatter_batch_bytes=64 << 20, _tag_batch_size=4096):
    """Private test harness may inject public fixture entropy; CLI cannot."""
    started = time.monotonic()
    plan, budget = operation_resources(config, record_bytes=config.max_record_bytes, execution=True)
    metadata = min(plan.metadata_bytes, 16 << 20)
    keys = OperationKeys.new() if _keys is None else _keys
    marker = manifest_name(keys.run_id.hex())
    # Owners stay live through serialization and marker-last publication.
    with DiskBudget(budget=budget) as disk, \
         budget.acquire('mix-source-metadata', 4096, metadata_bytes=4096) as source_metadata, \
         budget.acquire('mix-bundle-descriptors', 4096, metadata_bytes=4096) as bundle_metadata, \
         budget.acquire('mix-control-and-report', 16384, metadata_bytes=16384), \
         ExitStack() as retained:
        with build_catalog(input_dir=config.input_dir, samples=config.samples,
                           max_metadata_bytes=metadata, budget=budget, cancel=config.cancel) as catalog:
            if len(catalog) < 2:
                raise InputError('mixing requires at least two nonempty samples')
            prepare_destination(config.output, input_root=catalog.root)
            samples = []
            retained.callback(samples.clear)
            normalized = nonempty = 0
            with OutputTransaction.operation(config.output, 'manifest', budget,
                    keys=keys, storage_mode=config.storage.value, temp_dir=config.temp_dir,
                    disk_budget=disk, input_root=catalog.root,
                    max_artifact_bytes=config.max_artifact_bytes, metadata_bytes=metadata,
                    completion_basename=marker, cancel=config.cancel) as transaction:
                store = transaction.store
                cpu = resolve_cpu_budget(config.workers)
                with stage_execution(config, plan, budget, store, cpu, operation='mix',
                                     task_count=len(catalog), record_bytes=config.max_record_bytes,
                                     scatter=_scatter_strategy != 'sorter', scatter_batch_bytes=_scatter_batch_bytes) as stage, \
                     SerialShuffler(store, terminal_bytes=plan.terminal_bytes,
                                    max_record_bytes=config.max_record_bytes,
                                    sort_run_bytes=max(8 << 20, 40 + 2 * config.max_record_bytes + 512),
                                    cancel=config.cancel, scheduler=stage,
                                    scatter_strategy=_scatter_strategy, scatter_batch_bytes=_scatter_batch_bytes,
                                    _tag_batch_size=_tag_batch_size) as shuffle:
                    if stage is not None and stage.process_slots:
                        normalized = _parallel_ingress(catalog, shuffle, stage, config, samples, source_metadata)
                        nonempty = sum(record['pairs'] > 0 for record in samples)
                    else:
                        parser_bytes = 2 * (config.max_record_bytes + 5) + (2 << 20)
                        with budget.acquire('mix-parser', parser_bytes, descriptors=2):
                            for sample in catalog:
                                checkpoint(config, budget, plan)
                                with catalog.open_input(sample.r1) as raw1, catalog.open_input(sample.r2) as raw2:
                                    with open_decoded_stream(raw1, advertised_gzip=sample.r1.advertised_gzip) as stream1, \
                                         open_decoded_stream(raw2, advertised_gzip=sample.r2.advertised_gzip) as stream2:
                                        reader = PairedReader(
                                            RecordFramer(stream1, line_mode='ingress', max_record_bytes=config.max_record_bytes),
                                            RecordFramer(stream2, line_mode='ingress', max_record_bytes=config.max_record_bytes),
                                            sample_index=sample.index)
                                        while (pair := reader.next_pair(config.cancel)) is not None:
                                            normalized = checked_add(normalized, pair.normalized_bytes)
                                            if config.max_total_uncompressed is not None and normalized > config.max_total_uncompressed:
                                                raise ResourceError('normalized input exceeds max_total_uncompressed')
                                            shuffle.append_pair(pair)
                                            if reader.pair_count % 4096 == 0:
                                                checkpoint(config, budget, plan)
                                        report = reader.report()
                                        if not report.complete:
                                            raise InputError('input parser did not complete both mates')
                                        del reader
                                samples.append(_source_record(sample, report, source_metadata))
                                nonempty += report.pair_count > 0
                    if nonempty < 2:
                        raise InputError('mixing requires at least two nonempty samples')
                    catalog.dispose()
                    shuffle.finish_ingress()
                    counts = tuple(sample['pairs'] for sample in samples)
                    with CompressionService(budget, cpu, encoder=encode_member,
                                            cancel=config.cancel) as compression, \
                         BundleEmitter(transaction, keys, counts, max_record_bytes=config.max_record_bytes,
                                       compression_level=config.compression_level, metadata_bytes=metadata,
                                       cancel=config.cancel, compression_service=compression,
                                       metadata_lease=bundle_metadata) as emitter:
                        retained.callback(emitter.dispose)
                        with shuffle.iter_ordered(counts) as ordered:
                            for pair in ordered:
                                try:
                                    emitter.append(pair, tag=pair.tag)
                                finally:
                                    # The frame borrows the whole terminal arena.
                                    # Drop it before advancing to another terminal.
                                    del pair
                                if emitter.pairs % 4096 == 0:
                                    checkpoint(config, budget, plan)
                        bundles = emitter.finish()
                    total_pairs = shuffle.pairs
                manifest = dict(
                    format='readmixer-python-pool', schema_version=1, normalization_version=1,
                    run_id=keys.run_id.hex(), producer=dict(name='readmixer-py', version=__version__),
                    shuffle=dict(algorithm_version=1, tag_algorithm='aes256-identity-prp-v1'),
                    limits=dict(max_artifact_bytes=config.max_artifact_bytes,
                                max_input_record_bytes=config.max_record_bytes, max_metadata_bytes=min(metadata, config.max_artifact_bytes),
                                max_artifacts=100_000),
                    codec=codec_description(config.compression_level), execution=execution_description(config, plan, compression, stage),
                    samples=samples, bundles=bundles,
                    totals=dict(samples=len(samples), pairs=total_pairs, reads=checked_multiply(2, total_pairs),
                                bundles=len(bundles), artifacts=6 * len(bundles) + 1,
                                **{role + '_uncompressed_bytes':sum(bundle[role]['uncompressed_bytes'] for bundle in bundles)
                                   for role in ('r1', 'r2', 'key')}))
                with serialization_lease(budget, manifest, max_bytes=min(metadata, config.max_artifact_bytes)):
                    encoded = encode_manifest(manifest, max_metadata_bytes=min(metadata, config.max_artifact_bytes),
                                              max_parsed_bytes=budget.metadata_limit_bytes)
                    try:
                        physical = sum(bundle[role]['bytes'] for bundle in bundles
                                       for role in ('r1', 'r1_gzi', 'r2', 'r2_gzi', 'key', 'index')) + len(encoded)
                        result = MixReport(1, 'mix', True, keys.run_id.hex(), marker, len(samples), total_pairs,
                                           2 * total_pairs, normalized, physical, len(bundles), 6 * len(bundles) + 1,
                                           plan, stage.workers_effective if stage is not None else 1, compression.actual_cpu_slots, compression.slots,
                                           True, (), time.monotonic() - started)
                        checkpoint(config, budget, plan)
                        published = transaction.publish_all(encoded)
                    finally:
                        del encoded
                return replace(result, cleanup_complete=published.cleanup_complete, warnings=published.warnings,
                               elapsed_seconds=time.monotonic() - started)


def _source_record(sample, report, source_metadata):
    charge = source_metadata.bytes + 8192 + 16 * (len(os.fspath(sample.r1.relative_path)) + len(os.fspath(sample.r2.relative_path)))
    source_metadata.resize(charge, metadata_bytes=charge)
    record = dict(sample_index=sample.index, sample_id=sample.sample_id, pairs=report.pair_count)
    for role, source in ((1, sample.r1), (2, sample.r2)):
        record[f'r{role}'] = dict(source_file_id=2 * sample.index + role - 1,
            relative_path_hex=os.fsencode(source.relative_path).hex(), input_codec=source.codec,
            physical_bytes=source.snapshot.size, decoded_bytes=getattr(report, f'r{role}_decoded_bytes'),
            normalized_bytes=getattr(report, f'r{role}_normalized_bytes'),
            normalized_sha256=getattr(report, f'r{role}_sha256'))
    return record


def _parallel_ingress(catalog, shuffle, stage, config, samples, source_metadata):
    from collections import deque
    from .parser_stage import (lpt_assignments, encode_source, decode_source, decode_report,
                               SourceReceiver, IngressCredits)
    # Catalog-sized schedules and report slots remain charged independently of
    # source descriptors and are discarded before catalog disposal.
    with stage.budget.acquire('parser-assignment', 16384 + 256 * len(catalog) + 4096 * stage.process_slots,
            metadata_bytes=16384 + 256 * len(catalog) + 4096 * stage.process_slots):
        assignments = lpt_assignments(catalog, stage.process_slots)
        positions = [0] * stage.process_slots
        credits = IngressCredits(config.max_total_uncompressed)
        samples.extend([None] * len(catalog))
        pending = deque()
        def submit(producer):
            position = positions[producer]
            if position == len(assignments[producer]):
                return
            index = assignments[producer][position]
            sample = catalog.samples[index]
            direct = getattr(shuffle._scatter, 'inventory_only', False)
            mode = ('memory' if stage.store.mode == 'memory' else 'external') if direct else 'stream'
            capability = encode_source(catalog, sample, config.max_record_bytes, stage.parser_batch_bytes,
                scatter_mode=mode, producer=producer if direct else 0,
                bucket_key=stage.store.keys.bucket_key(()) if direct else bytes(32))
            receiver = SourceReceiver(decode_source(capability), shuffle.append_frame, credits)
            if direct:
                receiver.scatter_inventory = shuffle._scatter
                receiver.scatter_completed = shuffle.accept_scatter_stats
            task = stage.submit_parser(capability, receiver, producer=producer)
            pending.append((task, producer, index))
            positions[producer] += 1
        for _ in range(2):
            for producer in range(stage.process_slots):
                submit(producer)
        while pending:
            task, producer, index = pending[0]
            with stage.take_next() as result:
                if result.task_id != task:
                    raise InputError('parser results violated canonical source ownership')
                report = decode_report(result.data)
                if samples[index] is not None or report.sample_index != index:
                    raise InputError('parser source completion was duplicated')
                samples[index] = _source_record(catalog.samples[index], report, source_metadata)
            pending.popleft()
            submit(producer)
        if any(record is None for record in samples):
            raise InputError('parser did not complete every admitted source')
        return credits.used
