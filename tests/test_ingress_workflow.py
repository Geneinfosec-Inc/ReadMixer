"""Production ingress is one forward pass; sample identity is not a basename."""

from collections import Counter
from contextlib import contextmanager
import gzip
import hashlib
import json

import pytest

from readmixer_py import mix, recover, verify
from readmixer_py.catalog import SampleCatalog
from readmixer_py.config import MixConfig, RecoverConfig, RecoveryMode, StorageMode, VerifyConfig
from readmixer_py.fastq import DecodedStream


def _fixtures(root, *, codec='mixed', repeated_basenames=False):
    """An independent symbolic oracle; no production parser/renderer defines it."""
    root.mkdir()
    expected, decoded_inputs, physical_inputs, paths = {}, {}, {}, {}
    counts = {'alpha': 171, 'beta': 83, 'empty': 0}
    for sample_index, (sample, count) in enumerate(counts.items()):
        parent = root / sample if repeated_basenames else root
        if repeated_basenames:
            parent.mkdir()
        for role in (1, 2):
            normalized, decoded = bytearray(), bytearray()
            for ordinal in range(count):
                fields = (
                    f'@repeated/{role} sample={sample} opaque='.encode() + b'\xff',
                    f'{sample_index}:{ordinal}:{role}:'.encode() + b'X0!' * 93,
                    b'+complete original annotation\xfe',
                )
                quality = bytes((33 + ordinal % 40,)) * (len(fields[1]) - 1)
                quality += b'\r' if ordinal == count - 1 else b'!'
                lines = (*fields, quality)
                normalized.extend(b'\n'.join(lines) + b'\n')
                decoded.extend(b'\r\n'.join(lines))
                if ordinal != count - 1:
                    decoded.extend(b'\r\n')
            expected[sample, role] = bytes(normalized)
            is_gzip = codec == 'gzip' or codec == 'mixed' and role == 1
            leaf = ('forward' if role == 1 else 'reverse') if repeated_basenames else f'{sample}_R{role}'
            path = parent / (leaf + '.fastq' + ('.gz' if is_gzip else ''))
            relative = path.relative_to(root).as_posix()
            decoded_inputs[relative] = bytes(decoded)
            if is_gzip:
                cut = len(decoded) // 2
                physical = (gzip.compress(decoded[:cut], mtime=0)
                            + gzip.compress(b'', mtime=0)
                            + gzip.compress(decoded[cut:], mtime=0))
            else:
                physical = bytes(decoded)
            path.write_bytes(physical)
            physical_inputs[relative] = physical
            paths[sample, role] = relative
    return counts, expected, decoded_inputs, physical_inputs, paths


def _assert_both_recoveries(manifest, destination, counts, expected):
    result = verify(VerifyConfig(manifest=manifest, max_ram=512 << 20, storage=StorageMode.EXTERNAL))
    assert result.completed and result.validation_scope == 'pool_complete'
    assert result.pairs == sum(counts.values())
    assert result.normalized_bytes == sum(map(len, expected.values()))
    for mode in RecoveryMode:
        output = destination / mode.value
        report = recover(RecoverConfig(manifest=manifest, output=output, sample=tuple(reversed(counts)),
                                        io=mode, max_ram=512 << 20, workers=1,
                                        storage=StorageMode.EXTERNAL))
        assert report.completed and report.pairs == sum(counts.values())
        receipt = json.loads((output / report.receipt).read_bytes())
        assert [sample['sample_id'] for sample in receipt['samples']] == sorted(counts)
        actual = {}
        for sample in receipt['samples']:
            assert sample['pairs'] == counts[sample['sample_id']]
            for role in (1, 2):
                # stdlib gzip is independent of production BGZF/parser code.
                actual[sample['sample_id'], role] = b''.join(
                    gzip.decompress((output / part[f'r{role}']['name']).read_bytes())
                    for part in sample['parts'])
        assert actual == expected


@pytest.mark.parametrize('codec', ['plain', 'gzip', 'mixed'])
def test_PY02_IO_002_P09_T03_public_mix_decodes_each_original_once(tmp_path, monkeypatch, codec):
    counts, expected, decoded_inputs, physical_inputs, _paths = _fixtures(tmp_path / 'inputs', codec=codec)
    opens, decoders, raw_bytes, decoded_bytes = Counter(), Counter(), Counter(), Counter()
    raw_hashes = {name: hashlib.sha256() for name in physical_inputs}
    decoded_hashes = {name: hashlib.sha256() for name in decoded_inputs}
    completed = set()
    real_open = SampleCatalog.open_input

    class ForwardOnlyRaw:
        def __init__(self, stream, name):
            self.stream, self.name = stream, name

        def read(self, size):
            assert type(size) is int and size >= 0
            data = self.stream.read(size)
            raw_bytes[self.name] += len(data)
            raw_hashes[self.name].update(data)
            return data

        def seek(self, *args):
            raise AssertionError('production mix replayed an original input prefix')

        def __getattr__(self, name):
            return getattr(self.stream, name)

    @contextmanager
    def open_input(catalog, source):
        name = source.relative_path.as_posix()
        opens[name] += 1
        assert opens[name] == 1, 'original input was reopened for decoding'
        with real_open(catalog, source) as stream:
            yield ForwardOnlyRaw(stream, name)

    class CountedDecodedStream(DecodedStream):
        def __init__(self, raw, **kwargs):
            self.source_name = raw.name
            decoders[self.source_name] += 1
            assert decoders[self.source_name] == 1, 'original input acquired a second decoder'
            super().__init__(raw, **kwargs)

        def readinto(self, destination, cancel=None):
            size = super().readinto(destination, cancel)
            decoded_bytes[self.source_name] += size
            decoded_hashes[self.source_name].update(memoryview(destination)[:size])
            return size

        def close(self):
            assert self._finished, 'successful mix failed to validate the entire original stream'
            completed.add(self.source_name)
            super().close()

    import readmixer_py.mixing as mixing_module
    monkeypatch.setattr(SampleCatalog, 'open_input', open_input)
    monkeypatch.setattr(mixing_module, 'open_decoded_stream', CountedDecodedStream)
    output = tmp_path / 'pool'
    report = mix(MixConfig(input_dir=tmp_path / 'inputs', output=output,
                           max_ram=512 << 20, workers=1, storage=StorageMode.EXTERNAL,
                           max_artifact_bytes=65_564))
    assert report.completed and report.pairs == sum(counts.values())
    assert report.bundles > 1  # A later output part must not trigger input replay.
    assert opens == decoders == Counter({name: 1 for name in physical_inputs})
    assert completed == set(physical_inputs)
    for name, data in physical_inputs.items():
        assert raw_bytes[name] == len(data)
        assert raw_hashes[name].digest() == hashlib.sha256(data).digest()
    for name, data in decoded_inputs.items():
        assert decoded_bytes[name] == len(data)
        assert decoded_hashes[name].digest() == hashlib.sha256(data).digest()
    _assert_both_recoveries(output / report.manifest, tmp_path, counts, expected)
    # Leave the original-open spy active: verification/recovery cannot reread them.
    assert opens == decoders == Counter({name: 1 for name in physical_inputs})


def test_AUD09_repeated_leaf_basenames_in_tsv_public_roundtrip(tmp_path):
    inputs = tmp_path / 'inputs'
    counts, expected, _decoded, _physical, paths = _fixtures(inputs, repeated_basenames=True)
    sheet = inputs / 'samples.tsv'
    # Noncanonical sheet order must not change dense catalog/sample identities.
    sheet.write_text('sample_id\tr1\tr2\n' + ''.join(
        f'{sample}\t{paths[sample, 1]}\t{paths[sample, 2]}\n' for sample in reversed(counts)))
    assert len({path.rsplit('/', 1)[-1] for path in paths.values()}) == 2
    output = tmp_path / 'pool'
    report = mix(MixConfig(samples=sheet, output=output, max_ram=512 << 20, workers=1,
                           storage=StorageMode.EXTERNAL, max_artifact_bytes=65_564))
    manifest = output / report.manifest
    document = json.loads(manifest.read_bytes())
    assert [sample['sample_id'] for sample in document['samples']] == sorted(counts)
    assert [sample['sample_index'] for sample in document['samples']] == list(range(len(counts)))
    assert len({sample[f'r{role}']['source_file_id'] for sample in document['samples'] for role in (1, 2)}) == 6
    declared_paths = [bytes.fromhex(sample[f'r{role}']['relative_path_hex']).decode()
                      for sample in document['samples'] for role in (1, 2)]
    assert set(declared_paths) == set(paths.values()) and len(set(declared_paths)) == 6
    authoritative = verify(VerifyConfig(manifest=manifest, originals=sheet, max_ram=512 << 20,
                                       storage=StorageMode.EXTERNAL))
    assert authoritative.completed and authoritative.validation_scope == 'pool_and_originals'
    _assert_both_recoveries(manifest, tmp_path, counts, expected)
