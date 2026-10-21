"""Independent synthetic content oracles for the public serial pool workflow."""

from dataclasses import replace
import gzip
import hashlib
import json
from pathlib import Path

import pytest

from readmixer_py.config import MixConfig, StorageMode, VerifyConfig
from readmixer_py.crypto import OperationKeys
from readmixer_py.errors import CancelledError, InputError, IntegrityError, ResourceError
from readmixer_py.mixing import _mix
from readmixer_py.verification import verify


def fixtures(root, *, counts=(9, 5, 0), metadata=20, sequence=7):
    root.mkdir()
    expected = {}
    for sample, count in zip(('alpha', 'beta', 'empty'), counts):
        for mate in (1, 2):
            canonical, physical = bytearray(), bytearray()
            for ordinal in range(count):
                header = f'@repeat/{mate} '.encode() + b'opaque\xff' * metadata
                seq = b'' if ordinal == 0 else b'AC\x00' * sequence
                quality = b'' if not seq else b'!' * (len(seq) - 1) + b'\r'
                lines = (header, seq, b'+private\xfe' * metadata, quality)
                canonical.extend(b'\n'.join(lines) + b'\n')
                for number, line in enumerate(lines):
                    physical.extend(line)
                    if not (ordinal == count - 1 and number == 3):
                        physical.extend(b'\r\n')
            if count and not seq:
                # An empty final quality requires an actual physical empty line.
                physical.extend(b'\r\n')
            expected[(sample, mate)] = bytes(canonical)
            suffix = '.fq.gz' if mate == 1 else '.fq'
            payload = bytes(physical)
            if mate == 1:
                split = len(payload) // 2
                payload = gzip.compress(payload[:split], mtime=0) + gzip.compress(payload[split:], mtime=0)
            (root / f'{sample}_R{mate}{suffix}').write_bytes(payload)
    return expected


def mix_fixture(root, *, mode=StorageMode.EXTERNAL, cap=5_000_000_000, counts=(9, 5, 0), metadata=20, level=6):
    inputs = root / 'inputs'
    expected = fixtures(inputs, counts=counts, metadata=metadata)
    keys = OperationKeys._from_test(hashlib.sha256(b'readmixer-python/shuffle-test/v1/0').digest(), bytes(range(16)))
    report = _mix(MixConfig(input_dir=inputs, output=root / 'pool', storage=mode, max_ram=512 << 20, workers=1,
                            max_artifact_bytes=cap, compression_level=level), _keys=keys)
    manifest = root / 'pool' / report.manifest
    return inputs, expected, report, manifest


@pytest.mark.parametrize('mode', list(StorageMode))
@pytest.mark.parametrize('level', [0, 6])
def test_P07_001_complete_pool_and_originals_preserve_independent_bytes(tmp_path, mode, level):
    inputs, expected, produced, manifest = mix_fixture(tmp_path, mode=mode, level=level)
    document = json.loads(manifest.read_bytes())
    assert produced.pairs == 14 and produced.normalized_bytes == sum(map(len, expected.values()))
    for sample in document['samples']:
        for mate in (1, 2):
            original = expected[sample['sample_id'], mate]
            assert sample[f'r{mate}']['normalized_bytes'] == len(original)
            assert sample[f'r{mate}']['normalized_sha256'] == hashlib.sha256(original).hexdigest()
    verified = verify(VerifyConfig(manifest=manifest, max_ram=512 << 20, storage=mode))
    assert verified.validation_scope == 'pool_complete'
    assert verified.pairs == produced.pairs and verified.normalized_bytes == produced.normalized_bytes
    assert verified.consumed_artifact_bytes == produced.physical_bytes
    sheet = inputs / 'samples.tsv'
    sheet.write_text('sample_id\tr1\tr2\n' + ''.join(f'{sample}\t{sample}_R1.fq.gz\t{sample}_R2.fq\n'
        for sample in ('alpha', 'beta', 'empty')))
    authoritative = verify(VerifyConfig(manifest=manifest, originals=sheet, max_ram=512 << 20, storage=mode))
    assert authoritative.validation_scope == 'pool_and_originals'
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in manifest.parent.iterdir())


def test_P06_004_many_bundles_obey_physical_caps_and_greedy_verification(tmp_path):
    _, expected, report, manifest = mix_fixture(tmp_path, cap=65564, counts=(80, 39, 0), metadata=40)
    assert report.bundles > 2
    document = json.loads(manifest.read_bytes())
    for path in manifest.parent.iterdir():
        assert path.stat().st_size <= 65564
    verified = verify(VerifyConfig(manifest=manifest, max_ram=512 << 20))
    assert verified.normalized_bytes == sum(map(len, expected.values()))
    assert document['totals']['artifacts'] == 6 * report.bundles + 1


@pytest.mark.parametrize('counts', [(3, 0, 0), (0, 0, 0)])
def test_P04_two_nonempty_samples_required_after_validated_ingress(tmp_path, counts):
    inputs = tmp_path / 'inputs'
    fixtures(inputs, counts=counts)
    with pytest.raises(InputError, match='two nonempty'):
        _mix(MixConfig(input_dir=inputs, output=tmp_path / 'pool', max_ram=512 << 20))
    assert not list((tmp_path / 'pool').iterdir())


def test_P06_004_oversized_singleton_rolls_back(tmp_path):
    inputs = tmp_path / 'inputs'
    fixtures(inputs, metadata=2000)
    with pytest.raises(ResourceError, match='complete pair'):
        _mix(MixConfig(input_dir=inputs, output=tmp_path / 'pool', max_artifact_bytes=65564, max_ram=512 << 20, workers=1))
    assert not list((tmp_path / 'pool').iterdir())


def test_P07_006_changed_originals_never_downgrade_scope(tmp_path):
    inputs, _, _, manifest = mix_fixture(tmp_path)
    sheet = inputs / 'samples.tsv'
    sheet.write_text('sample_id\tr1\tr2\n' + ''.join(f'{sample}\t{sample}_R1.fq.gz\t{sample}_R2.fq\n'
        for sample in ('alpha', 'beta', 'empty')))
    p = inputs / 'alpha_R2.fq'
    p.write_bytes(p.read_bytes().replace(b'private', b'changed', 1))
    with pytest.raises(IntegrityError, match='originals differ'):
        verify(VerifyConfig(manifest=manifest, originals=sheet, max_ram=512 << 20))
    assert verify(VerifyConfig(manifest=manifest, max_ram=512 << 20)).validation_scope == 'pool_complete'


def test_P07_actual_normalized_guard_and_cancellation(tmp_path):
    _, _, produced, manifest = mix_fixture(tmp_path)
    config = VerifyConfig(manifest=manifest, max_ram=512 << 20, max_total_uncompressed=produced.normalized_bytes)
    assert verify(config).completed
    with pytest.raises(ResourceError, match='max_total_uncompressed'):
        verify(replace(config, max_total_uncompressed=produced.normalized_bytes - 1))
    with pytest.raises(CancelledError):
        verify(replace(config, cancel=lambda: True))


def test_pipeline_drops_borrowed_terminal_before_next_arena(tmp_path, monkeypatch):
    import weakref
    from readmixer_py import mix
    from readmixer_py.artifacts.bundles import BundleEmitter
    from readmixer_py.shuffle import SerialShuffler

    source = tmp_path / 'input'
    fixtures(source)
    original_append = BundleEmitter.append
    original_terminal = SerialShuffler._terminal
    retained = []
    terminals = []

    def observe_append(self, pair, **kwargs):
        retained.append(weakref.ref(pair.frame))
        return original_append(self, pair, **kwargs)

    def observe_terminal(self, shard):
        # A previous frame must not retain its entire already-unleased arena
        # when the next terminal starts allocating its workspace.
        assert all(reference() is None for reference in retained)
        terminals.append(shard.path)
        yield from original_terminal(self, shard)

    monkeypatch.setattr(BundleEmitter, 'append', observe_append)
    monkeypatch.setattr(SerialShuffler, '_terminal', observe_terminal)
    result = mix(MixConfig(input_dir=source, output=tmp_path / 'pool', max_ram=512 << 20, workers=1))
    assert result.completed and len(terminals) > 1
    assert all(reference() is None for reference in retained)
