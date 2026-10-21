#!/usr/bin/env python3
"""Generate versioned public artificial paired FASTQ fixtures, one record at a time.

The output directory must not exist. It receives input/ and fixtures.json; no
biological references, downloaded accessions, or user datasets are read. The
public generator key is NOT a production shuffle key. Metadata length applies
to each header and each plus field. Newline conversion affects files only;
reported SHA-256 values always describe normalized four-line LF records.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
from dataclasses import asdict, dataclass
import gzip
import hashlib
import hmac
import json
import os
from pathlib import Path
import struct

GENERATOR_VERSION = 1
GENERATOR_LABEL = 'readmixer-python/fixtures/v1'
GENERATOR_KEY = hashlib.sha256(GENERATOR_LABEL.encode('utf-8')).digest()
PROFILE_LABEL_V2 = 'readmixer-python/fixtures/v2'
PROFILE_KEY_V2 = hashlib.sha256(PROFILE_LABEL_V2.encode('utf-8')).digest()
PROFILE_LABEL_V3 = 'readmixer-python/fixtures/v3'
PROFILE_KEY_V3 = hashlib.sha256(PROFILE_LABEL_V3.encode('utf-8')).digest()
PROFILES = ('v1', 'high-compressibility-v2', 'low-compressibility-v2', 'low-compressibility-v3')
# These are artificial symbols, deliberately not a nucleotide alphabet.
_SYMBOLS = b'XYZ0123456789!?%'
_QUALITY = bytes(range(33, 97))
_HEX = b'0123456789abcdef'
_MAX_FIELD = 4 << 20
_U64_MAX = (1 << 64) - 1


@dataclass(frozen=True)
class FixtureConfig:
    counts: tuple[int, ...] = (1000, 1000)
    sequence_bytes: int = 150
    metadata_bytes: int = 24
    newline: str = 'lf'
    compression: str = 'gzip'
    payload: str = 'varied'
    gzip_level: int = 6
    profile: str = 'v1'
    length_mode: str = 'fixed'

    def __post_init__(self):
        if type(self.counts) is not tuple or not 1 <= len(self.counts) <= 10_000:
            raise ValueError('counts must contain 1..10000 sample counts')
        if any(type(n) is not int or not 0 <= n <= _U64_MAX for n in self.counts):
            raise ValueError('sample counts must be unsigned 64-bit integers')
        if sum(self.counts) > _U64_MAX:
            raise ValueError('total pair count exceeds the fixture counter bound')
        for value in (self.sequence_bytes, self.metadata_bytes):
            if type(value) is not int or not 0 <= value <= _MAX_FIELD:
                raise ValueError('sequence and metadata lengths must be in 0..4194304 bytes')
        if self.newline not in ('lf', 'crlf'):
            raise ValueError('newline must be lf or crlf')
        if self.compression not in ('plain', 'gzip', 'mixed'):
            raise ValueError('compression must be plain, gzip, or mixed')
        if self.payload not in ('varied', 'repeated'):
            raise ValueError('payload must be varied or repeated')
        if self.profile not in PROFILES:
            raise ValueError('unknown versioned fixture profile')
        if self.length_mode not in ('fixed', 'varied-v2'):
            raise ValueError('length mode must be fixed or varied-v2')
        if self.profile == 'v1' and self.length_mode != 'fixed':
            raise ValueError('version-1 fixture lengths are fixed')
        if self.profile != 'v1' and self.payload != 'varied':
            raise ValueError('payload applies only to the version-1 profile')
        if type(self.gzip_level) is not int or not 0 <= self.gzip_level <= 9:
            raise ValueError('gzip level must be in 0..9')

    @property
    def max_record_bytes(self):
        # Exact for fixed lengths; an admitted maximum for varied-v2 lengths.
        return 75 + int(self.profile == 'low-compressibility-v3') + 2 * self.metadata_bytes + 2 * self.sequence_bytes


def parse_counts(value):
    fields = value.split(',')
    if len(fields) > 10_000 or any(not s.isascii() or not s.isdecimal() or len(s) > 20 for s in fields):
        raise argparse.ArgumentTypeError('counts requires comma-separated unsigned decimal integers')
    values = tuple(int(s) for s in fields)
    if any(n > _U64_MAX for n in values) or sum(values) > _U64_MAX:
        raise argparse.ArgumentTypeError('counts exceeds the unsigned 64-bit bound')
    return values


def symbolic_field(sample, ordinal, mate, field, length, alphabet, *, repeated=False):
    """Version-1 byte recipe: HMAC-SHA256(identity/field), then SHAKE256 expansion.

    Identity/field is >IQBB. Power-of-two alphabets permit exact low-bit mapping.
    Only the current field is allocated; memory does not depend on pair count.
    """
    seed = hmac.new(GENERATOR_KEY, struct.pack('>IQBB', sample, ordinal, mate, field), hashlib.sha256).digest()
    if repeated:
        return bytes((alphabet[seed[0] & (len(alphabet) - 1)],)) * length
    random_bytes = hashlib.shake_256(seed).digest(length)
    table = bytes(alphabet[value & (len(alphabet) - 1)] for value in range(256))
    return random_bytes.translate(table)


def _v2_seed(sample, ordinal, mate, field):
    return hmac.digest(PROFILE_KEY_V2, struct.pack('>IQBB', sample, ordinal, mate, field), 'sha256')


def _profile_seed(config, sample, ordinal, mate, field):
    # V3 intentionally uses a new public domain/key. The v2 field expansion and
    # length algorithms are retained, but none of v1/v2's bytes are changed.
    if config.profile == 'low-compressibility-v3':
        return hmac.digest(PROFILE_KEY_V3, struct.pack('>IQBB', sample, ordinal, mate, field), 'sha256')
    return _v2_seed(sample, ordinal, mate, field)


def record_lengths(config, sample, ordinal, mate):
    if config.length_mode == 'fixed':
        return config.sequence_bytes, config.metadata_bytes
    # Length fields use distinct domains from payload fields. Zero and the
    # configured maxima are admitted; no dataset-size length array is retained.
    return tuple(int.from_bytes(_profile_seed(config, sample, ordinal, mate, field)[:8], 'big') % (maximum + 1)
                 for field, maximum in ((3, config.sequence_bytes), (4, config.metadata_bytes)))


def _v2_field(config, sample, ordinal, mate, field, length):
    """Original v2 expansion, with a separate seed domain for the v3 profile."""
    if config.profile == 'high-compressibility-v2':
        return b'~' * length
    # Uniform opaque bytes from the 254 values excluding CR/LF. These are valid
    # under the byte-preserving input grammar, not an ASCII quality alphabet.
    # Rejection (not modulo mapping) avoids bias. Only one bounded chunk and the
    # current field are live; expansion does not depend on total pair count.
    seed = _profile_seed(config, sample, ordinal, mate, field)
    output = bytearray()
    counter = 0
    while len(output) < length:
        block = hashlib.shake_256(seed + struct.pack('>I', counter)).digest(min(65536, length - len(output) + 256))
        block = block.translate(None, b'\n\r')
        output.extend(block[:length - len(output)])
        counter += 1
    return bytes(output)


def record_fields(config, sample, ordinal, mate):
    """Return one complete artificial record, never using production FASTQ code."""
    sequence_length, metadata_length = record_lengths(config, sample, ordinal, mate)
    if config.profile == 'v1':
        metadata = symbolic_field(sample, ordinal, mate, 0, metadata_length, _HEX,
                                  repeated=config.payload == 'repeated')
        sequence = symbolic_field(sample, ordinal, mate, 1, sequence_length, _SYMBOLS,
                                  repeated=config.payload == 'repeated')
        quality = symbolic_field(sample, ordinal, mate, 2, sequence_length, _QUALITY,
                                 repeated=config.payload == 'repeated')
        plus_metadata = metadata
    else:
        metadata = _v2_field(config, sample, ordinal, mate, 0, metadata_length)
        sequence = _v2_field(config, sample, ordinal, mate, 1, sequence_length)
        quality = _v2_field(config, sample, ordinal, mate, 2, sequence_length)
        plus_metadata = _v2_field(config, sample, ordinal, mate, 5, metadata_length)
    header = f'@synthetic-s{sample:06d}-p{ordinal:020d}/{mate} meta='.encode('ascii') + metadata
    if config.profile == 'low-compressibility-v3':
        # Opaque metadata can contain ASCII whitespace and accidental trailing
        # role markers. First metadata always begins 'meta='; appending '~'
        # makes the final token neither 1/2 nor a token ending /1 or /2. This
        # fixed syntax byte is separate from configured random metadata length.
        header += b'~'
    plus = f'+synthetic mate={mate} meta='.encode('ascii') + plus_metadata
    return header, sequence, plus, quality


def _private_binary(path):
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0)
    return os.fdopen(os.open(path, flags, 0o600), 'wb')


def _write_json(path, value):
    with _private_binary(path) as output:
        output.write((json.dumps(value, sort_keys=True, indent=2) + '\n').encode('utf-8'))


def generate_fixtures(output: Path, config: FixtureConfig = FixtureConfig()):
    """Create a new fixture root; an interrupted run has no completion JSON."""
    if not isinstance(config, FixtureConfig):
        raise TypeError('config must be FixtureConfig')
    output = Path(output)
    output.mkdir(mode=0o700, exist_ok=False)
    inputs = output / 'input'
    inputs.mkdir(mode=0o700)
    samples = []
    newline = b'\n' if config.newline == 'lf' else b'\r\n'
    physical_total = normalized_total = decoded_total = 0
    observed = {name: dict(minimum=None, maximum=None, total=0)
                for name in ('record_bytes', 'sequence_bytes', 'metadata_bytes')}
    def observe(name, value):
        stats = observed[name]
        stats['minimum'] = value if stats['minimum'] is None else min(stats['minimum'], value)
        stats['maximum'] = value if stats['maximum'] is None else max(stats['maximum'], value)
        stats['total'] += value
    for sample_index, count in enumerate(config.counts):
        sample_id = f'sample{sample_index:06d}'
        entry = dict(sample_index=sample_index, sample_id=sample_id, pairs=count)
        with ExitStack() as stack:
            mates = []
            for mate in (1, 2):
                compressed = config.compression == 'gzip' or (
                    config.compression == 'mixed' and (sample_index + mate) % 2 == 0)
                name = f'{sample_id}_R{mate}.fastq' + ('.gz' if compressed else '')
                raw = stack.enter_context(_private_binary(inputs / name))
                stream = stack.enter_context(gzip.GzipFile(filename='', fileobj=raw, mode='wb',
                    compresslevel=config.gzip_level, mtime=0)) if compressed else raw
                mates.append((stream, hashlib.sha256(), name, compressed))
            sizes = [0, 0]
            for ordinal in range(count):
                for mate, (stream, digest, _name, _compressed) in enumerate(mates, 1):
                    fields = record_fields(config, sample_index, ordinal, mate)
                    record_bytes = sum(len(field) + 1 for field in fields)
                    sizes[mate - 1] += record_bytes
                    observe('record_bytes', record_bytes)
                    observe('sequence_bytes', len(fields[1]))
                    observe('metadata_bytes', (record_bytes - 75 - int(config.profile == 'low-compressibility-v3') - 2 * len(fields[1])) // 2)
                    for field in fields:
                        digest.update(field)
                        digest.update(b'\n')
                        stream.write(field)
                        stream.write(newline)
            for mate, (_stream, digest, name, compressed) in enumerate(mates, 1):
                size = sizes[mate - 1]
                decoded = size + (4 * count if config.newline == 'crlf' else 0)
                entry[f'r{mate}'] = dict(name=name, gzip=compressed, normalized_bytes=size,
                                         decoded_bytes=decoded, normalized_sha256=digest.hexdigest())
                normalized_total += size
                decoded_total += decoded
        for mate in (1, 2):
            physical = (inputs / entry[f'r{mate}']['name']).stat().st_size
            entry[f'r{mate}']['physical_bytes'] = physical
            decoded = entry[f'r{mate}']['decoded_bytes']
            entry[f'r{mate}']['physical_to_decoded_ratio'] = physical / decoded if decoded else None
            physical_total += physical
        samples.append(entry)
    version = 1 if config.profile == 'v1' else 3 if config.profile == 'low-compressibility-v3' else 2
    label = {1: GENERATOR_LABEL, 2: PROFILE_LABEL_V2, 3: PROFILE_LABEL_V3}[version]
    key = {1: GENERATOR_KEY, 2: PROFILE_KEY_V2, 3: PROFILE_KEY_V3}[version]
    report = dict(completed=True, fixture_version=version,
                  generator_label=label,
                  public_generator_key_sha256=key.hex(),
                  profile=config.profile, observed_lengths=observed,
                  decoded_bytes=decoded_total,
                  physical_to_decoded_ratio=physical_total / decoded_total if decoded_total else None,
                  compressibility_scope='observed physical/decoded ratio; no incompressibility guarantee',
                  public_artificial_test_material=True, biological_reference_used=False,
                  configuration=asdict(config), max_record_bytes=config.max_record_bytes,
                  pairs=sum(config.counts), reads=2 * sum(config.counts),
                  normalized_bytes=normalized_total, physical_bytes=physical_total, samples=samples)
    if version == 3:
        report.update(header_guard_bytes=1, record_fixed_overhead_bytes=76,
            field_recipe='v2 rejection expansion using SHA-256(readmixer-python/fixtures/v3) as the HMAC key',
            length_recipe='fixed or v2 modulo lengths with the version-3 field domains',
            header_recipe='literal meta= prefix and final tilde syntax byte protect mate-role grammar')
    # Publication of this marker follows successful closure of every fixture.
    _write_json(output / 'fixtures.json', report)
    return report


def add_fixture_arguments(parser, *, default_counts=(1000, 1000)):
    parser.add_argument('--counts', type=parse_counts, default=default_counts,
                        help='comma-separated pair counts, one per sample (default: 1000,1000)')
    parser.add_argument('--sequence-bytes', type=int, default=150)
    parser.add_argument('--metadata-bytes', type=int, default=24,
                        help='bytes added to each header and plus field')
    parser.add_argument('--newline', choices=('lf', 'crlf'), default='lf')
    parser.add_argument('--compression', choices=('plain', 'gzip', 'mixed'), default='gzip')
    parser.add_argument('--payload', choices=('varied', 'repeated'), default='varied')
    parser.add_argument('--gzip-level', type=int, default=6)
    parser.add_argument('--profile', choices=PROFILES, default='v1',
                        help='v1/v2 preserve historical bytes; low-compressibility-v3 guards opaque header metadata')
    parser.add_argument('--length-mode', choices=('fixed', 'varied-v2'), default='fixed',
                        help='v2 lengths vary from zero through the configured maxima')


def config_from_arguments(arguments):
    return FixtureConfig(**{field: getattr(arguments, field) for field in FixtureConfig.__dataclass_fields__})


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--output-dir', type=Path, required=True, help='new directory; must not already exist')
    add_fixture_arguments(parser)
    arguments = parser.parse_args(argv)
    try:
        report = generate_fixtures(arguments.output_dir, config_from_arguments(arguments))
    except (OSError, ValueError) as exc:
        parser.exit(1, f'fixture generation failed: {type(exc).__name__}\n')
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
