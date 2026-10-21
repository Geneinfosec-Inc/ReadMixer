"""Read-only catalog and exact paired-stream inspection."""

from dataclasses import dataclass
import struct
import sys
import time
import traceback

from .catalog import _MetadataOwner, build_catalog
from .config import InspectConfig
from .errors import CancelledError, ConfigurationError, InputError, ResourceError, UnsupportedFormatError
from .fastq import PairedReader, RecordFramer, open_decoded_stream
from .resources import ResourcePlan, checked_add, checked_multiply
from .runtime import operation_resources


@dataclass(frozen=True)
class InspectReport:
    report_version: int
    operation: str
    completed: bool
    deep: bool
    samples: int
    physical_bytes: int
    pairs: int | None
    reads: int | None
    normalized_bytes: int | None
    decoded_bytes: int | None
    nonempty_samples: int | None
    empty_samples: int | None
    mix_eligible: bool | None
    resources: ResourcePlan
    workers_effective: int
    elapsed_seconds: float


def inspect(config: InspectConfig) -> InspectReport:
    if not isinstance(config, InspectConfig):
        raise ConfigurationError("inspect requires InspectConfig")
    if sys.implementation.name != "cpython" or sys.platform not in ("linux", "darwin") or struct.calcsize("P") != 8:
        raise UnsupportedFormatError("this build requires 64-bit CPython on Linux or macOS")
    started = time.monotonic()
    if config.cancel is not None and config.cancel():
        raise CancelledError()
    plan, budget = operation_resources(config, record_bytes=config.max_record_bytes)
    # Parsing is serial. The catalog independently owns its shared metadata
    # reservation; a parser lease only covers reusable payload/native work.
    parser_bytes = checked_add(checked_multiply(2, config.max_record_bytes + 5), 2 << 20) if config.deep else 0
    metadata_limit = min(plan.metadata_bytes, budget.remaining_bytes - parser_bytes)
    if metadata_limit <= 0:
        raise ResourceError("inspection cannot admit catalog and parser working memory")
    catalog = sample = pair = reader = source_report = result = result_owner = None
    raw1 = raw2 = stream1 = stream2 = None
    try:
        # Report/ResourcePlan scalars and bounded detection-source strings coexist
        # with the catalog and parsers through success construction and disposal.
        report_lease = budget.acquire("inspection-report", 8192, metadata_bytes=8192)
        try:
            result_owner = _MetadataOwner(report_lease)
        except BaseException:
            report_lease.release()
            raise
        with build_catalog(input_dir=config.input_dir, samples=config.samples, budget=budget,
                           max_metadata_bytes=metadata_limit, cancel=config.cancel) as catalog:
            budget.checkpoint(plan)
            physical = 0
            for sample in catalog:
                physical = checked_add(physical, checked_add(sample.r1.snapshot.size, sample.r2.snapshot.size))
            sample = None
            pairs = normalized = decoded = nonempty = 0
            if config.deep:
                with budget.acquire("inspection-parser", parser_bytes, descriptors=2):
                    try:
                        for sample in catalog:
                            budget.checkpoint(plan)
                            if config.cancel is not None and config.cancel():
                                raise CancelledError()
                            with catalog.open_input(sample.r1) as raw1, catalog.open_input(sample.r2) as raw2:
                                with open_decoded_stream(raw1, advertised_gzip=sample.r1.advertised_gzip) as stream1, \
                                     open_decoded_stream(raw2, advertised_gzip=sample.r2.advertised_gzip) as stream2:
                                    reader = PairedReader(
                                        RecordFramer(stream1, line_mode="ingress", max_record_bytes=config.max_record_bytes),
                                        RecordFramer(stream2, line_mode="ingress", max_record_bytes=config.max_record_bytes),
                                        sample_index=sample.index,
                                    )
                                    while (pair := reader.next_pair(config.cancel)) is not None:
                                        total = checked_add(normalized, pair.normalized_bytes)
                                        if config.max_total_uncompressed is not None and total > config.max_total_uncompressed:
                                            raise ResourceError("normalized input bytes exceed max_total_uncompressed")
                                        normalized = total
                                        if reader.pair_count % 4096 == 0:
                                            budget.checkpoint(plan)
                                        pair = None
                                    source_report = reader.report()
                                    pairs = checked_add(pairs, source_report.pair_count)
                                    decoded = checked_add(decoded, checked_add(source_report.r1_decoded_bytes,
                                                                              source_report.r2_decoded_bytes))
                                    nonempty += source_report.pair_count > 0
                            # Views/reader metadata end before the next arenas.
                            sample = pair = reader = source_report = None
                            raw1 = raw2 = stream1 = stream2 = None
                    finally:
                        sample = pair = reader = source_report = None
                        raw1 = raw2 = stream1 = stream2 = None
            budget.checkpoint(plan)
            if config.cancel is not None and config.cancel():
                raise CancelledError()
            result = InspectReport(
                report_version=1, operation="inspect", completed=True, deep=config.deep,
                samples=len(catalog), physical_bytes=physical,
                pairs=pairs if config.deep else None,
                reads=checked_multiply(2, pairs) if config.deep else None,
                normalized_bytes=normalized if config.deep else None,
                decoded_bytes=decoded if config.deep else None,
                nonempty_samples=nonempty if config.deep else None,
                empty_samples=len(catalog) - nonempty if config.deep else None,
                mix_eligible=nonempty >= 2 if config.deep else (False if len(catalog) < 2 else None),
                resources=plan, workers_effective=1,
                elapsed_seconds=time.monotonic() - started,
            )
            # Not a dataclass field: public asdict/JSON/repr do not expose an
            # internal lease or acquire a public report.close() obligation.
            object.__setattr__(result, "_metadata_owner", result_owner)
        result_owner = None  # Returned result now owns its bounded reservation.
        return result
    except BaseException as error:
        traceback.clear_frames(error.__traceback__)
        sample = pair = reader = source_report = result = None
        raw1 = raw2 = stream1 = stream2 = None
        if catalog is not None:
            catalog.dispose()
            catalog = None
        result_owner = None
        if isinstance(error, OSError):
            raise InputError("input inspection failed during descriptor I/O") from error
        if isinstance(error, MemoryError):
            raise ResourceError("input inspection could not allocate its admitted buffers") from error
        raise
