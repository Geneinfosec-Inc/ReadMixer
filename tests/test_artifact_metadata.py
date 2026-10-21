"""Independent Plan06 mapping/index bytes and hostile manifest fixtures."""

from copy import deepcopy
from dataclasses import replace
import hashlib
import io
import json
import struct

import pytest

from readmixer_py.artifacts.key import KEY_HEADER, KeyRow, encode_key_row, key_row_size, maximum_key_row_bytes, parse_key_row, secured_record_limit
from readmixer_py.artifacts.index import IndexEntry, IndexHeader, IndexReader, decode_entry, decode_header, encode_entry, encode_header
from readmixer_py.artifacts.manifest import artifact_names, bounded_json_bytes, encode_manifest, load_manifest, manifest_name, parse_json, parse_manifest, validate_manifest
from readmixer_py.errors import ConfigurationError, IntegrityError, ReadMixerError, ResourceError, UnsupportedFormatError


def key_row(**changes):
    return replace(KeyRow(0, 9, 12, "0123456789abcdef" * 2, 0, 87, 100, 89,
                          b"@read/1 note\xff", b"+old\t\r", b"@read/2 note\xfe", b"+other"), **changes)


def test_exact_twelve_field_key_bytes_and_borrowed_metadata_preservation():
    row = key_row()
    expected = (b"0,9,12,0123456789abcdef0123456789abcdef,0,87,100,89,"
                b"40726561642f31206e6f7465ff,2b6f6c64090d,40726561642f32206e6f7465fe,2b6f74686572\n")
    assert encode_key_row(row) == expected
    assert key_row_size(row) == len(expected)
    assert parse_key_row(expected) == row
    assert KEY_HEADER.count(b",") == 11 and KEY_HEADER.endswith(b"\n")
    assert len(expected.split(b",")) == 12
    assert secured_record_limit(1024) == 1100


@pytest.mark.parametrize("field,replacement", [(0, b"00"), (0, b"+1"), (0, b" 0"), (1, b"1e2"),
    (2, b"18446744073709551616"), (0, b"4294967296"), (3, b"A" * 32), (8, b"40FF"),
    (8, b"4"), (9, b"2b0a"), (9, b"2c"), (10, b"4077726f6e672f32"), (5, b"84")])
def test_key_row_rejects_noncanonical_fields_and_injections(field, replacement):
    fields = encode_key_row(key_row()).rstrip(b"\n").split(b",")
    fields[field] = replacement
    with pytest.raises(IntegrityError):
        parse_key_row(b",".join(fields) + b"\n")


@pytest.mark.parametrize("suffix", [b"", b"\r\n", b"\n\n"])
def test_key_row_requires_one_canonical_final_lf(suffix):
    with pytest.raises(IntegrityError):
        parse_key_row(encode_key_row(key_row())[:-1] + suffix)


def test_key_limits_counts_boolean_and_large_lossless_metadata():
    row = key_row(r1_header=b"@read/1 " + b"x" * 100_000, r2_header=b"@read/2 " + b"y" * 100_000)
    encoded = encode_key_row(row)
    assert len(encoded) > 100_000 and len(encoded) <= maximum_key_row_bytes()
    assert parse_key_row(encoded) == row
    with pytest.raises(IntegrityError):
        parse_key_row(encoded, max_record_bytes=100_000)
    with pytest.raises(ResourceError):
        parse_key_row(b"x" * (maximum_key_row_bytes(100) + 1), max_record_bytes=100)
    with pytest.raises(ConfigurationError):
        encode_key_row(key_row(sample_index=True))
    with pytest.raises(IntegrityError):
        parse_key_row(encode_key_row(key_row()), sample_counts=[9])
    with pytest.raises(IntegrityError):
        parse_key_row(encode_key_row(key_row()), expected_secure_pair_index=11)


def index_fixture():
    run = bytes(range(16))
    header = struct.pack("<8sII16sQQQQ", b"RMPYIDX1", 1, 40, run, 2, 8, 2, 0)
    first = struct.pack("<IIQQQQ", 1, 0, 2, 0, 0, 177)
    second = struct.pack("<IIQQQQ", 0, 0, 3, 85, 89, 256)
    return run, header, first, second


def test_independent_index_struct_fixture_exact_width_and_physical_order():
    run, header, first, second = index_fixture()
    assert len(header) == 64 and len(first) == 40
    assert encode_header(IndexHeader(run, 2, 8, 2)) == header
    assert encode_entry(IndexEntry(1, 2, 0, 0, 177)) == first
    with IndexReader(io.BytesIO(header + first + second), run_id=run, bundle_index=2,
                     first_secure_pair_index=8, entry_count=2, sample_counts=[4, 3]) as reader:
        entries = list(reader)
    assert [entry.sample_index for entry in entries] == [1, 0]
    assert reader.position == 2


@pytest.mark.parametrize("offset,value", [(0, 0), (8, 2), (12, 41), (56, 1)])
def test_index_header_mutations(offset, value):
    _run, header, _first, _second = index_fixture()
    changed = bytearray(header)
    changed[offset] = value
    with pytest.raises((IntegrityError, UnsupportedFormatError)):
        decode_header(changed)


@pytest.mark.parametrize("failure", ["reserved", "sample", "ordinal", "decreasing", "trailing", "truncated", "run", "count", "cap"])
def test_index_reader_rejects_hostile_streams_and_manifest_disagreement(failure):
    run, header, first, second = index_fixture()
    options = dict(run_id=run, bundle_index=2, first_secure_pair_index=8, entry_count=2, sample_counts=[4, 3])
    if failure == "reserved":
        first = first[:4] + b"\x01\0\0\0" + first[8:]
    elif failure == "sample":
        first = struct.pack("<IIQQQQ", 2, 0, 0, 0, 0, 177)
    elif failure == "ordinal":
        first = struct.pack("<IIQQQQ", 1, 0, 3, 0, 0, 177)
    elif failure == "decreasing":
        second = first
    elif failure == "run":
        options["run_id"] = b"x" * 16
    elif failure == "count":
        options["entry_count"] = 3
    elif failure == "cap":
        options["max_bytes"] = 143
    data = header + first + second
    if failure == "trailing":
        data += b"x"
    elif failure == "truncated":
        data = data[:-1]
    with pytest.raises((IntegrityError, ResourceError)):
        with IndexReader(io.BytesIO(data), **options):
            pass  # Successful context exit must validate even unread entries.


def test_failed_index_cannot_be_reused_after_consuming_corrupt_tail():
    run, header, first, second = index_fixture()
    reader = IndexReader(io.BytesIO(header + first + second + b"x"))
    next(reader)
    next(reader)
    with pytest.raises(IntegrityError):
        next(reader)
    with pytest.raises(IntegrityError, match="previously failed"):
        next(reader)
    with pytest.raises(ConfigurationError):
        encode_entry(IndexEntry(True, 0, 0, 0, 0))


def manifest_fixture():
    run = "01" * 16
    digest = "ab" * 32
    names = artifact_names(run, 0)
    bundle = {"bundle_index": 0, "first_secure_pair_index": 0, "pairs": 2}
    for role in ("r1", "r2", "key"):
        bundle[role] = {"name": names[role], "bytes": 100, "sha256": digest,
                        "uncompressed_bytes": 170 if role != "key" else len(KEY_HEADER) + 200, "records": 2}
    for role in ("r1_gzi", "r2_gzi", "index"):
        bundle[role] = {"name": names[role], "bytes": 144 if role == "index" else 8,
                        "sha256": digest, "entries": 2 if role == "index" else 0}
    samples = []
    for index, name in enumerate(("A", "B")):
        sample = {"sample_index": index, "sample_id": name, "pairs": 1}
        for role, lane in enumerate(("r1", "r2")):
            sample[lane] = {"source_file_id": index * 2 + role,
                            "relative_path_hex": f"{name}_{lane}.fq".encode().hex(),
                            "input_codec": "plain", "physical_bytes": 11, "decoded_bytes": 11,
                            "normalized_bytes": 11, "normalized_sha256": digest}
        samples.append(sample)
    return {"format": "readmixer-python-pool", "schema_version": 1, "normalization_version": 1,
            "run_id": run, "producer": {"name": "readmixer-py", "version": "2.0.0.dev1"},
            "shuffle": {"algorithm_version": 1, "tag_algorithm": "aes256-identity-prp-v1"},
            "limits": {"max_artifact_bytes": 65564, "max_input_record_bytes": 1024,
                       "max_metadata_bytes": 65564, "max_artifacts": 100000},
            "codec": {"format": "bgzf", "compression_level": 6, "payload_bytes": 65280,
                      "max_member_bytes": 65536, "eof_bytes": 28, "digest_algorithm": "sha256"},
            "execution": {"profile_version": 1, "memory_budget_bytes": 128 << 20,
                          "terminal_slice_bytes": 32 << 20, "storage_mode": "external", "workers_effective": 1,
                          "cpu_budget": 1, "descriptor_limit": 64, "compression_slots": 0},
            "samples": samples, "bundles": [bundle],
            "totals": {"samples": 2, "pairs": 2, "reads": 4, "bundles": 1, "artifacts": 7,
                       "r1_uncompressed_bytes": 170, "r2_uncompressed_bytes": 170,
                       "key_uncompressed_bytes": len(KEY_HEADER) + 200}}


def test_manifest_roundtrip_bounded_streaming_and_canonical_names():
    document = manifest_fixture()
    encoded = encode_manifest(document)
    assert encoded.endswith(b"\n") and not encoded.endswith(b"\n\n")
    assert parse_manifest(encoded) == document
    assert load_manifest(io.BytesIO(encoded)) == document
    assert encoded == encode_manifest(dict(reversed(list(document.items()))))
    assert manifest_name(document["run_id"]) == "pool." + "01" * 16 + ".manifest.json"
    assert parsed_json(encoded) == document


def parsed_json(data):
    # Independent standard-library reader for valid golden schema data.
    return json.loads(data.decode("utf-8"))


@pytest.mark.parametrize("path,value", [
    (("schema_version",), 2), (("schema_version",), True), (("format",), "readmixer-python-recovery"),
    (("normalization_version",), 2), (("run_id",), "AA" * 16), (("unknown",), 1),
    (("shuffle", "algorithm_version"), 2), (("shuffle", "tag_algorithm"), "random"),
    (("codec", "payload_bytes"), 65536), (("codec", "compression_level"), True),
    (("execution", "storage_mode"), "plaintext"), (("execution", "terminal_slice_bytes"), 1),
    (("execution", "workers_effective"), 2), (("limits", "max_artifacts"), 6),
    (("limits", "max_metadata_bytes"), 65565), (("totals", "artifacts"), 6), (("totals", "reads"), 3),
    (("samples", 1, "sample_index"), 0), (("samples", 1, "sample_id"), "A"),
    (("samples", 0, "r1", "source_file_id"), 1), (("samples", 0, "r1", "relative_path_hex"), b"../x".hex()),
    (("samples", 0, "r1", "relative_path_hex"), b"/abs".hex()), (("samples", 0, "r1", "decoded_bytes"), 100),
    (("samples", 0, "r1", "normalized_sha256"), "AA" * 32), (("samples", 0, "r1", "normalized_bytes"), 0),
    (("bundles", 0, "first_secure_pair_index"), 1), (("bundles", 0, "pairs"), 0),
    (("bundles", 0, "r1", "name"), "../escape"), (("bundles", 0, "r1", "records"), True),
    (("bundles", 0, "r1", "bytes"), 65565), (("bundles", 0, "r1_gzi", "entries"), 1),
    (("bundles", 0, "index", "bytes"), 143), (("bundles", 0, "key", "uncompressed_bytes"), 1),
])
def test_manifest_rejects_independent_schema_relationship_mutations(path, value):
    document = manifest_fixture()
    target = document
    for component in path[:-1]:
        target = target[component]
    target[path[-1]] = value
    with pytest.raises(ReadMixerError):
        validate_manifest(document)


def test_manifest_empty_sample_requires_zero_content_and_empty_digest():
    document = manifest_fixture()
    empty = deepcopy(document["samples"][-1])
    empty.update(sample_index=2, sample_id="C", pairs=0)
    for role, lane in enumerate(("r1", "r2")):
        empty[lane].update(source_file_id=4 + role, relative_path_hex=f"C_{lane}.fq".encode().hex(),
                           physical_bytes=0, decoded_bytes=0, normalized_bytes=0,
                           normalized_sha256=hashlib.sha256(b"").hexdigest())
    document["samples"].append(empty)
    document["totals"]["samples"] = 3
    assert parse_manifest(encode_manifest(document)) == document
    empty["r1"]["normalized_sha256"] = "00" * 32
    with pytest.raises(IntegrityError, match="empty"):
        validate_manifest(document)


@pytest.mark.parametrize("data", [b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":Infinity}', b'{"x":1.5}',
                                       b'{"x":"\xff"}', b'{"x":"\\ud800"}', b'{} trailing'])
def test_strict_json_rejects_duplicate_nonfinite_invalid_utf8_and_surrogates(data):
    with pytest.raises(IntegrityError):
        parse_json(data)


def test_manifest_encoded_and_parsed_representation_limits_precede_success():
    document = manifest_fixture()
    encoded = encode_manifest(document)
    with pytest.raises(ResourceError):
        parse_manifest(encoded, max_metadata_bytes=len(encoded) - 1)
    with pytest.raises(ResourceError):
        parse_manifest(encoded, max_parsed_bytes=100)
    with pytest.raises(ResourceError):
        encode_manifest(document, max_metadata_bytes=100)
    with pytest.raises(ResourceError):
        encode_manifest(document, max_parsed_bytes=100)
    with pytest.raises(ResourceError):
        bounded_json_bytes({"data": "\x01" * 4096}, max_bytes=128)
    with pytest.raises(ResourceError):
        parse_json(b"[" * 34 + b"0" + b"]" * 34)
    document["samples"][1]["r2"]["relative_path_hex"] = document["samples"][0]["r1"]["relative_path_hex"]
    with pytest.raises(IntegrityError, match="reused"):
        validate_manifest(document)
