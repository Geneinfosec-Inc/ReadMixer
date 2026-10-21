"""Strict bounded pool metadata, deterministic JSON, and generated artifact names.

Callers own leases for input/output byte buffers as well as the separately
bounded parsed object representation. No source path is automatically reopened.
"""

import hashlib
import json
import re
import sys

from ..config import MAX_ARTIFACT_BYTES, MAX_RECORD_BYTES, MIN_ARTIFACT_BYTES, U64_MAX
from ..errors import IntegrityError, ResourceError, UnsupportedFormatError

MAX_METADATA_BYTES = 64 << 20
MAX_PARSED_BYTES = 256 << 20
_ROLES = ("r1", "r1_gzi", "r2", "r2_gzi", "key", "index")
_SUFFIXES = (".R1.fastq.gz", ".R1.fastq.gz.gzi", ".R2.fastq.gz", ".R2.fastq.gz.gzi", ".key.csv.gz", ".index.rmpyidx")
_EMPTY_SHA = hashlib.sha256(b"").hexdigest()
_HEX32 = re.compile(r"[0-9a-f]{32}", re.ASCII)
_HEX64 = re.compile(r"[0-9a-f]{64}", re.ASCII)
_SAMPLE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", re.ASCII)


def _uint(value, maximum=U64_MAX, minimum=0):
    if type(value) is not int or not minimum <= value <= maximum:
        raise IntegrityError("manifest integer is outside its permitted unsigned range")
    return value


def _keys(value, expected):
    if type(value) is not dict or set(value) != set(expected):
        raise IntegrityError("manifest object has missing or unexpected fields")
    return value


def _constant(value, expected):
    if type(value) is not type(expected) or value != expected:
        raise IntegrityError("manifest policy or codec constant is unsupported")


def _digest(value):
    if not isinstance(value, str) or _HEX64.fullmatch(value) is None:
        raise IntegrityError("manifest SHA-256 is not canonical lowercase hexadecimal")


def _run_id(value):
    if not isinstance(value, str) or _HEX32.fullmatch(value) is None:
        raise IntegrityError("manifest run identifier is invalid")
    return value


def manifest_name(run_id: str) -> str:
    return f"pool.{_run_id(run_id)}.manifest.json"


def artifact_names(run_id: str, bundle_index: int) -> dict[str, str]:
    _run_id(run_id)
    _uint(bundle_index, 16_665)
    prefix = f"pool.{run_id}.b{bundle_index:06d}"
    return {role: prefix + suffix for role, suffix in zip(_ROLES, _SUFFIXES)}


def parsed_object_bytes(value, *, maximum=MAX_PARSED_BYTES, _depth=0, _ancestors=()) -> int:
    """Conservative recursive representation cost; cycles/deep trees fail closed."""
    if _depth > 32:
        raise ResourceError("metadata nesting exceeds its admitted bound")
    kind = type(value)
    if kind not in (dict, list, str, int, bool, type(None)):
        raise IntegrityError("metadata contains a non-JSON or unsupported numeric value")
    total = sys.getsizeof(value)
    if kind in (dict, list):
        if id(value) in _ancestors:
            raise IntegrityError("metadata contains a reference cycle")
        ancestors = (*_ancestors, id(value))
        if kind is dict:
            for key, child in value.items():
                if type(key) is not str:
                    raise IntegrityError("metadata object keys must be text")
                total += parsed_object_bytes(key, maximum=maximum, _depth=_depth + 1, _ancestors=ancestors)
                total += parsed_object_bytes(child, maximum=maximum, _depth=_depth + 1, _ancestors=ancestors)
                if total > maximum:
                    raise ResourceError("parsed metadata representation exceeds its allowance")
        else:
            for child in value:
                total += parsed_object_bytes(child, maximum=maximum, _depth=_depth + 1, _ancestors=ancestors)
                if total > maximum:
                    raise ResourceError("parsed metadata representation exceeds its allowance")
    elif kind is str and any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        raise IntegrityError("metadata text contains invalid Unicode scalar values")
    elif kind is int and not -U64_MAX <= value <= U64_MAX:
        raise IntegrityError("metadata integer exceeds its bounded width")
    if total > maximum:
        raise ResourceError("parsed metadata representation exceeds its allowance")
    return total


def _preflight_json(data, maximum):
    # Charge the decoded JSON text, parsed strings, temporary pair-hook entries,
    # container slots and scalar objects before invoking the materializing parser.
    charge = 8 * len(data) + 1024
    depth = 0
    quoted = escaped = False
    for byte in data:
        if quoted:
            if escaped:
                escaped = False
            elif byte == 92:
                escaped = True
            elif byte == 34:
                quoted = False
            continue
        if byte == 34:
            quoted = True
            charge += 256
        elif byte in (123, 91):
            depth += 1
            charge += 512
            if depth > 32:
                raise ResourceError("metadata nesting exceeds its admitted bound")
        elif byte in (125, 93):
            depth -= 1
        elif byte in (58, 44):
            charge += 256
        if charge > maximum:
            raise ResourceError("JSON parsing workspace exceeds its admitted allowance")
    if charge > maximum:
        raise ResourceError("JSON parsing workspace exceeds its admitted allowance")

    return charge


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise IntegrityError("JSON metadata contains duplicate object keys")
        result[key] = value
    return result


def _integer_token(value):
    if len(value) > 21:
        raise IntegrityError("JSON integer exceeds its bounded width")
    return int(value)


def _reject_number(value):
    raise IntegrityError("JSON metadata contains a noninteger or nonfinite number")


def parse_json(data: bytes, *, max_bytes=MAX_METADATA_BYTES, max_parsed_bytes=MAX_PARSED_BYTES):
    _uint(max_bytes, MAX_METADATA_BYTES, 1)
    _uint(max_parsed_bytes, U64_MAX, 1)
    if not isinstance(data, bytes) or len(data) > min(MAX_METADATA_BYTES, max_bytes):
        raise ResourceError("encoded metadata exceeds its admitted allowance")
    _preflight_json(data, max_parsed_bytes)
    try:
        text = data.decode("utf-8", "strict")
        document = json.loads(text, object_pairs_hook=_pairs, parse_int=_integer_token,
                              parse_float=_reject_number, parse_constant=_reject_number)
    except (UnicodeError, json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise IntegrityError("metadata is not strict bounded UTF-8 JSON") from exc
    parsed_object_bytes(document, maximum=max_parsed_bytes)
    return document


def _json_chunks(value):
    kind = type(value)
    if kind is dict:
        yield b"{"
        for position, key in enumerate(sorted(value)):
            if position:
                yield b","
            yield from _json_chunks(key)
            yield b":"
            yield from _json_chunks(value[key])
        yield b"}"
    elif kind is list:
        yield b"["
        for position, item in enumerate(value):
            if position:
                yield b","
            yield from _json_chunks(item)
        yield b"]"
    elif kind is str:
        yield b'"'
        # Escape bounded substrings; never materialize an entire encoded string.
        for position in range(0, len(value), 1024):
            try:
                yield json.encoder.encode_basestring(value[position:position + 1024])[1:-1].encode("utf-8")
            except UnicodeError as exc:
                raise IntegrityError("metadata contains invalid Unicode scalar values") from exc
        yield b'"'
    elif kind is bool:
        yield b"true" if value else b"false"
    elif value is None:
        yield b"null"
    else:
        yield str(value).encode("ascii")


def bounded_json_bytes(document, *, max_bytes=MAX_METADATA_BYTES, max_parsed_bytes=MAX_PARSED_BYTES) -> bytes:
    _uint(max_bytes, MAX_METADATA_BYTES, 1)
    parsed_object_bytes(document, maximum=max_parsed_bytes)
    output = bytearray()
    for chunk in _json_chunks(document):
        if len(chunk) > max_bytes - len(output) - 1:
            raise ResourceError("encoded metadata exceeds its admitted allowance")
        output.extend(chunk)
    output.append(10)
    return bytes(output)


def _source(value, *, sample_index, role, pairs, record_limit, paths):
    _keys(value, ("source_file_id", "relative_path_hex", "input_codec", "physical_bytes",
                  "decoded_bytes", "normalized_bytes", "normalized_sha256"))
    _constant(value["source_file_id"], 2 * sample_index + role)
    encoded = value["relative_path_hex"]
    if (type(encoded) is not str or not 2 <= len(encoded) <= 32_768 or len(encoded) % 2
            or re.fullmatch(r"[0-9a-f]+", encoded, re.ASCII) is None):
        raise IntegrityError("manifest source path encoding is invalid")
    path = bytes.fromhex(encoded)
    if path.startswith(b"/") or b"\0" in path or any(part in (b"", b".", b"..") for part in path.split(b"/")):
        raise IntegrityError("manifest source path is not a safe descriptive relative path")
    if path in paths:
        raise IntegrityError("manifest source paths are reused across roles or samples")
    paths.add(path)
    if value["input_codec"] not in ("plain", "gzip"):
        raise IntegrityError("manifest input container codec is unsupported")
    physical = _uint(value["physical_bytes"])
    decoded = _uint(value["decoded_bytes"])
    normalized = _uint(value["normalized_bytes"])
    _digest(value["normalized_sha256"])
    if not 7 * pairs <= normalized <= record_limit * pairs:
        raise IntegrityError("manifest normalized source length is inconsistent with record counts")
    if not max(0, normalized - int(pairs > 0)) <= decoded <= normalized + 4 * pairs:
        raise IntegrityError("manifest decoded source length is inconsistent with normalization")
    if value["input_codec"] == "plain" and physical != decoded:
        raise IntegrityError("manifest plain source lengths disagree")
    if value["input_codec"] == "gzip" and physical < 20:
        raise IntegrityError("manifest gzip source is shorter than a complete member")
    if not pairs and (normalized or decoded or value["normalized_sha256"] != _EMPTY_SHA):
        raise IntegrityError("manifest empty sample source facts are invalid")


def validate_manifest(document, *, max_metadata_bytes=MAX_METADATA_BYTES,
                      max_parsed_bytes=MAX_PARSED_BYTES):
    parsed_object_bytes(document, maximum=max_parsed_bytes)
    if type(document) is not dict:
        raise IntegrityError("pool manifest must be a JSON object")
    if document.get("format") != "readmixer-python-pool" or type(document.get("schema_version")) is not int or document.get("schema_version") != 1:
        raise UnsupportedFormatError("unsupported pool format or schema version")
    if type(document.get("normalization_version")) is not int or document.get("normalization_version") != 1:
        raise UnsupportedFormatError("unsupported pool normalization version")
    _keys(document, ("format", "schema_version", "normalization_version", "run_id", "producer", "shuffle",
                     "limits", "codec", "execution", "samples", "bundles", "totals"))
    run = _run_id(document["run_id"])
    producer = _keys(document["producer"], ("name", "version"))
    _constant(producer["name"], "readmixer-py")
    if type(producer["version"]) is not str or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.+_-]{0,127}", producer["version"], re.ASCII) is None:
        raise IntegrityError("manifest producer version is invalid")
    shuffle = _keys(document["shuffle"], ("algorithm_version", "tag_algorithm"))
    _constant(shuffle["algorithm_version"], 1)
    _constant(shuffle["tag_algorithm"], "aes256-identity-prp-v1")
    limits = _keys(document["limits"], ("max_artifact_bytes", "max_input_record_bytes", "max_metadata_bytes", "max_artifacts"))
    cap = _uint(limits["max_artifact_bytes"], MAX_ARTIFACT_BYTES, MIN_ARTIFACT_BYTES)
    record_limit = _uint(limits["max_input_record_bytes"], MAX_RECORD_BYTES, 1)
    metadata_cap = _uint(limits["max_metadata_bytes"], min(MAX_METADATA_BYTES, cap), 1)
    inventory_cap = _uint(limits["max_artifacts"], 100_000, 1)
    _uint(max_metadata_bytes, MAX_METADATA_BYTES, 1)
    codec = _keys(document["codec"], ("format", "compression_level", "payload_bytes", "max_member_bytes", "eof_bytes", "digest_algorithm"))
    for name, value in (("format", "bgzf"), ("payload_bytes", 65280), ("max_member_bytes", 65536), ("eof_bytes", 28), ("digest_algorithm", "sha256")):
        _constant(codec[name], value)
    _uint(codec["compression_level"], 9)
    execution = _keys(document["execution"], ("profile_version", "memory_budget_bytes", "terminal_slice_bytes", "storage_mode",
                       "workers_effective", "cpu_budget", "descriptor_limit", "compression_slots"))
    _constant(execution["profile_version"], 1)
    memory = _uint(execution["memory_budget_bytes"], U64_MAX, 1)
    terminal = _uint(execution["terminal_slice_bytes"], 128 << 20, 1)
    if terminal != min(128 << 20, memory // 4):
        raise IntegrityError("manifest terminal allowance does not follow its memory budget")
    if execution["storage_mode"] not in ("memory", "external", "auto"):
        raise IntegrityError("manifest storage policy is unsupported")
    workers = _uint(execution["workers_effective"], U64_MAX, 1)
    cpus = _uint(execution["cpu_budget"], U64_MAX, 1)
    _uint(execution["descriptor_limit"], U64_MAX, 1)
    slots = _uint(execution["compression_slots"], U64_MAX)
    if workers + slots > cpus:
        raise IntegrityError("manifest execution exceeds its admitted CPU budget")
    samples = document["samples"]
    if type(samples) is not list or not 2 <= len(samples) <= 10_000:
        raise IntegrityError("manifest sample catalog exceeds its count policy")
    paths, previous_id = set(), None
    source_pairs = nonempty = 0
    catalog_encoded = 64
    for sample_index, sample in enumerate(samples):
        _keys(sample, ("sample_index", "sample_id", "pairs", "r1", "r2"))
        _constant(sample["sample_index"], sample_index)
        sample_id = sample["sample_id"]
        if type(sample_id) is not str or _SAMPLE_ID.fullmatch(sample_id) is None or (previous_id is not None and sample_id <= previous_id):
            raise IntegrityError("manifest sample IDs are invalid, duplicated, or noncanonical")
        previous_id = sample_id
        catalog_encoded += 8 + len(sample_id)
        pairs = _uint(sample["pairs"])
        source_pairs += pairs
        _uint(source_pairs)
        nonempty += int(pairs > 0)
        for role, name in enumerate(("r1", "r2")):
            _source(sample[name], sample_index=sample_index, role=role, pairs=pairs, record_limit=record_limit, paths=paths)
            catalog_encoded += 69 + len(sample[name]["relative_path_hex"]) // 2
            if catalog_encoded > 16 << 20:
                raise ResourceError("manifest source catalog exceeds its encoded metadata bound")
    if nonempty < 2:
        raise IntegrityError("pool manifest requires at least two nonempty samples")
    bundles = document["bundles"]
    if type(bundles) is not list or not 1 <= len(bundles) <= (inventory_cap - 1) // 6:
        raise IntegrityError("manifest bundle inventory exceeds its admitted capacity")
    first = lane_r1 = lane_r2 = lane_key = 0
    safe_payload = ((cap - 28) // 65536) * 65280
    for bundle_index, bundle in enumerate(bundles):
        _keys(bundle, ("bundle_index", "first_secure_pair_index", "pairs", *_ROLES))
        _constant(bundle["bundle_index"], bundle_index)
        _constant(bundle["first_secure_pair_index"], first)
        pairs = _uint(bundle["pairs"], U64_MAX, 1)
        first += pairs
        _uint(first)
        names = artifact_names(run, bundle_index)
        for role in _ROLES:
            data_lane = role in ("r1", "r2", "key")
            fields = ("name", "bytes", "sha256", "uncompressed_bytes", "records") if data_lane else ("name", "bytes", "sha256", "entries")
            descriptor = _keys(bundle[role], fields)
            _constant(descriptor["name"], names[role])
            physical = _uint(descriptor["bytes"], cap, 1)
            _digest(descriptor["sha256"])
            if data_lane:
                _constant(descriptor["records"], pairs)
                decoded = _uint(descriptor["uncompressed_bytes"], safe_payload, 1)
                if role in ("r1", "r2") and not 83 * pairs <= decoded <= (record_limit + 76) * pairs:
                    raise IntegrityError("manifest secured stream lengths contradict record counts")
                if role == "key":
                    from .key import KEY_HEADER, maximum_key_row_bytes
                    if not len(KEY_HEADER) + 65 * pairs <= decoded <= len(KEY_HEADER) + maximum_key_row_bytes(record_limit) * pairs:
                        raise IntegrityError("manifest key stream length is impossible")
                members = (decoded + 65279) // 65280
                if not 28 + 29 * members <= physical <= 28 + 65536 * members:
                    raise IntegrityError("manifest BGZF physical length contradicts its member count")
            else:
                entries = _uint(descriptor["entries"])
                if role == "index":
                    if entries != pairs or physical != 64 + 40 * pairs:
                        raise IntegrityError("manifest mapping index length or count disagrees")
                else:
                    lane = bundle[role[:2]]
                    decoded = _uint(lane["uncompressed_bytes"])
                    if entries != max((decoded + 65279) // 65280 - 1, 0) or physical != 8 + 16 * entries:
                        raise IntegrityError("manifest sidecar length or member count disagrees")
        lane_r1 += bundle["r1"]["uncompressed_bytes"]
        lane_r2 += bundle["r2"]["uncompressed_bytes"]
        lane_key += bundle["key"]["uncompressed_bytes"]
    if first != source_pairs:
        raise IntegrityError("manifest original and secured pair totals disagree")
    totals = _keys(document["totals"], ("samples", "pairs", "reads", "bundles", "artifacts",
                                      "r1_uncompressed_bytes", "r2_uncompressed_bytes", "key_uncompressed_bytes"))
    expected = {"samples": len(samples), "pairs": first, "reads": 2 * first, "bundles": len(bundles),
                "artifacts": 6 * len(bundles) + 1, "r1_uncompressed_bytes": lane_r1,
                "r2_uncompressed_bytes": lane_r2, "key_uncompressed_bytes": lane_key}
    for name, value in expected.items():
        _uint(value)
        _uint(totals[name])
        _constant(totals[name], value)
    return document


def parse_manifest(data: bytes, *, max_metadata_bytes=MAX_METADATA_BYTES, max_parsed_bytes=MAX_PARSED_BYTES):
    document = parse_json(data, max_bytes=max_metadata_bytes, max_parsed_bytes=max_parsed_bytes)
    validate_manifest(document, max_metadata_bytes=max_metadata_bytes, max_parsed_bytes=max_parsed_bytes)
    if len(data) > min(document["limits"]["max_metadata_bytes"], document["limits"]["max_artifact_bytes"]):
        raise ResourceError("manifest exceeds its declared metadata or artifact cap")
    return document


def encode_manifest(document, *, max_metadata_bytes=MAX_METADATA_BYTES, max_parsed_bytes=MAX_PARSED_BYTES):
    validate_manifest(document, max_metadata_bytes=max_metadata_bytes, max_parsed_bytes=max_parsed_bytes)
    return bounded_json_bytes(document, max_bytes=min(max_metadata_bytes, document["limits"]["max_metadata_bytes"],
                              document["limits"]["max_artifact_bytes"]), max_parsed_bytes=max_parsed_bytes)


def load_manifest(stream, *, max_metadata_bytes=MAX_METADATA_BYTES, max_parsed_bytes=MAX_PARSED_BYTES):
    _uint(max_metadata_bytes, MAX_METADATA_BYTES, 1)
    data = bytearray()
    while True:
        chunk = stream.read(min(65536, max_metadata_bytes + 1 - len(data)))
        if not chunk:
            break
        data.extend(chunk)
        if len(data) > max_metadata_bytes:
            raise ResourceError("manifest input exceeds its metadata allowance")
    return parse_manifest(bytes(data), max_metadata_bytes=max_metadata_bytes, max_parsed_bytes=max_parsed_bytes)
