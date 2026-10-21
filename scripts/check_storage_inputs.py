#!/usr/bin/env python3
"""Development-only bounded prefix parser/codec/encrypted-storage validation.

This checks at most --pairs-per-sample pairs from each admitted sample. It is
not a complete input scan and does not validate mixing or recovery. Only
aggregate counts and success flags are printed; private names, reads and
fingerprints remain in memory. Run directly from a checkout, for example:

    python scripts/check_storage_inputs.py --input /path/to/paired-inputs
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import struct
import sys
import tempfile

# This utility is deliberately outside the installable package/production CLI.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from readmixer_py.catalog import build_catalog
from readmixer_py.config import parse_size, positive_integer
from readmixer_py.crypto import OperationKeys, pairs_identity
from readmixer_py.disk import DiskBudget
from readmixer_py.errors import IntegrityError, ReadMixerError
from readmixer_py.fastq import PairedReader, RecordFramer, open_decoded_stream
from readmixer_py.paircodec import decode_pair_view, encode_pair_into, maximum_frame_bytes
from readmixer_py.resources import Budget, checked_add, resolve_resources
from readmixer_py.storage import SegmentStore

_SCOPE = "bounded_prefix_storage_check"
_RECORD_BYTES = 1 << 20
_HASH_DOMAIN = b"RMPY1-development-storage-prefix\x00"


class _SafeArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        # argparse's default diagnostics can echo a supplied private path.
        self.exit(2, "invalid validation arguments; use --help\n")


def _size_argument(value):
    try:
        return parse_size(value)
    except ReadMixerError as exc:
        raise argparse.ArgumentTypeError("invalid byte allowance") from exc


def validate_prefix(input_dir: Path, *, pairs_per_sample: int = 128,
                    max_ram: int = 128 << 20) -> dict[str, object]:
    """Check bounded sample prefixes without emitting source identifiers."""
    positive_integer(pairs_per_sample, "pairs per sample")
    plan = resolve_resources(max_ram=max_ram, workers=1)
    budget = Budget(plan.effective_max_ram, descriptors=plan.descriptor_allowance,
                    baseline_bytes=plan.coordinator_rss)
    disk = DiskBudget()
    frame_capacity = maximum_frame_bytes(_RECORD_BYTES)
    parser_bytes = 2 * (_RECORD_BYTES + 5) + (2 << 20)
    metadata_bytes = min(plan.metadata_bytes, 16 << 20)
    source_digest, decoded_digest = hashlib.sha256(_HASH_DOMAIN), hashlib.sha256(_HASH_DOMAIN)
    total_pairs = verified_pairs = normalized_bytes = sample_count = nonempty = 0
    scratch_path = None
    scratch_parent = Path(tempfile.gettempdir()).resolve()

    with budget.acquire("prefix-catalog", metadata_bytes, descriptors=6):
        with build_catalog(input_dir=input_dir, max_metadata_bytes=metadata_bytes) as catalog:
            sample_count = len(catalog)
            with budget.acquire("prefix-parser", parser_bytes, descriptors=2), \
                 budget.acquire("prefix-frame", frame_capacity + 4096):
                arena = bytearray(frame_capacity)
                with SegmentStore("external", budget, OperationKeys.new(),
                                  temp_dir=scratch_parent, input_root=catalog.root,
                                  disk_budget=disk) as store:
                    for sample in catalog:
                        budget.checkpoint(plan)
                        sampled = 0
                        with store.new_segment(1, pairs_identity((), sample.index, 0)) as writer:
                            with catalog.open_input(sample.r1) as raw1, catalog.open_input(sample.r2) as raw2:
                                with open_decoded_stream(raw1, advertised_gzip=sample.r1.advertised_gzip) as stream1, \
                                     open_decoded_stream(raw2, advertised_gzip=sample.r2.advertised_gzip) as stream2:
                                    reader = PairedReader(
                                        RecordFramer(stream1, line_mode="ingress", max_record_bytes=_RECORD_BYTES),
                                        RecordFramer(stream2, line_mode="ingress", max_record_bytes=_RECORD_BYTES),
                                        sample_index=sample.index,
                                    )
                                    for _ in range(pairs_per_sample):
                                        pair = reader.next_pair()
                                        if pair is None:
                                            break
                                        # Independently hash parser identities and normalized
                                        # fields before passing any bytes through the codec.
                                        source_digest.update(struct.pack(">IQ", pair.sample_index, pair.original_pair_index))
                                        for mate in (pair.r1, pair.r2):
                                            for field in (mate.header, mate.sequence, mate.plus, mate.quality):
                                                source_digest.update(field)
                                                source_digest.update(b"\n")
                                        del mate, field
                                        normalized_bytes = checked_add(normalized_bytes, pair.normalized_bytes)
                                        size = encode_pair_into(pair, arena, max_record_bytes=_RECORD_BYTES)
                                        writer.append_pair(memoryview(arena)[:size], max_record_bytes=_RECORD_BYTES)
                                        sampled += 1
                                        del pair
                                    del reader
                            ref = writer.seal()
                        total_pairs = checked_add(total_pairs, sampled)
                        nonempty += int(sampled > 0)
                        restored = 0
                        with store.open_verified(ref) as stream:
                            while True:
                                prefix = stream.read(4)
                                if not prefix:
                                    break
                                if len(prefix) != 4:
                                    raise IntegrityError("prefix check found a truncated frame length")
                                size = int.from_bytes(prefix, "little")
                                if not 48 <= size <= frame_capacity:
                                    raise IntegrityError("prefix check found an invalid bounded frame length")
                                arena[:4] = prefix
                                if stream.readinto(memoryview(arena)[4:size]) != size - 4:
                                    raise IntegrityError("prefix check found a truncated frame payload")
                                decoded = decode_pair_view(memoryview(arena)[:size], max_record_bytes=_RECORD_BYTES)
                                if decoded.sample_index != sample.index or decoded.original_pair_index != restored:
                                    raise IntegrityError("prefix check found changed numeric pair identities")
                                # This transcript comes from independently decoded lines,
                                # not from an encoder-produced frame checksum.
                                decoded_digest.update(decoded.sample_index.to_bytes(4, "big"))
                                decoded_digest.update(decoded.original_pair_index.to_bytes(8, "big"))
                                for mate in (decoded.r1, decoded.r2):
                                    for line in mate.lines:
                                        decoded_digest.update(line)
                                        decoded_digest.update(b"\n")
                                restored += 1
                                del mate, line, decoded
                        if restored != sampled or ref.item_count != sampled:
                            raise IntegrityError("prefix check found changed paired item counts")
                        verified_pairs = checked_add(verified_pairs, restored)
                        scratch_path = store.scratch_directory
                        store.drop(ref)
                    del arena
            if source_digest.digest() != decoded_digest.digest():
                raise IntegrityError("prefix check found changed canonical pair content")
    cleanup_complete = (scratch_path is None or not scratch_path.exists()) and disk.committed_bytes == 0
    if not cleanup_complete or budget.used_bytes != budget.baseline_bytes or budget.used_descriptors:
        raise IntegrityError("prefix check did not release all owned resources")
    return {
        "completed": True,
        "scope": _SCOPE,
        "full_input_validation": False,
        "mix_recovery_validation": False,
        "pairs_per_sample_limit": pairs_per_sample,
        "samples": sample_count,
        "nonempty_prefixes": nonempty,
        "pairs": total_pairs,
        "verified_pairs": verified_pairs,
        "normalized_bytes": normalized_bytes,
        "canonical_sha256_equal": True,
        "numeric_identities_equal": True,
        "encrypted_external_storage": True,
        "all_segment_seals_verified": True,
        "scratch_cleanup_complete": True,
        "peak_scratch_bytes": disk.peak_bytes,
    }


def main(argv=None) -> int:
    parser = _SafeArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", required=True, type=Path, help="directory containing paired inputs")
    parser.add_argument("--pairs-per-sample", type=int, default=128, help="prefix pair limit per sample (default: 128)")
    parser.add_argument("--max-ram", type=_size_argument, default=128 << 20, help="exact byte budget (default: 128MiB)")
    arguments = parser.parse_args(argv)
    try:
        report = validate_prefix(arguments.input, pairs_per_sample=arguments.pairs_per_sample,
                                 max_ram=arguments.max_ram)
    except KeyboardInterrupt:
        print(json.dumps({"completed": False, "scope": _SCOPE, "error": "interrupted"}, sort_keys=True))
        return 130
    except (ReadMixerError, OSError, MemoryError) as exc:
        # Do not print exception messages, chained OS paths, source fingerprints,
        # sample identifiers, or any record bytes in this development report.
        print(json.dumps({"completed": False, "scope": _SCOPE, "error": type(exc).__name__}, sort_keys=True))
        return 1
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
