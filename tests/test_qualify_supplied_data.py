"""P16-T06: independent full-data oracle, frozen candidate, and private ownership."""
import gzip
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tarfile
from types import SimpleNamespace
import zipfile

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / 'scripts'
sys.path.insert(0, str(_SCRIPTS))
_spec = importlib.util.spec_from_file_location('qualify_supplied_data', _SCRIPTS / 'qualify_supplied_data.py')
full = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = full
_spec.loader.exec_module(full)


def _inputs(root):
    root.mkdir()
    for index, count in enumerate((2, 1, 0)):
        for mate in (1, 2):
            chunks = []
            for ordinal in range(count):
                fields = (f'@private-s{index}-p{ordinal}/{mate} opaque='.encode() + b'\x00\rZ',
                          b'A\x00\r', b'+opaque\xfe', b'!#\r')
                chunks.append(b'\r\n'.join(fields) + (b'' if ordinal + 1 == count else b'\r\n'))
            raw = b''.join(chunks)
            compressed = mate == 1
            name = f'private-s{index}_R{mate}.fastq' + ('.gz' if compressed else '')
            if compressed:
                middle = len(raw) // 2
                raw = gzip.compress(raw[:middle], mtime=0) + gzip.compress(raw[middle:], mtime=0)
            (root / name).write_bytes(raw)
    return root


@pytest.mark.parametrize('raw,expected', [
    (b'@x\nA\n+\n!\n', (b'@x', b'A', b'+', b'!')),
    (b'@x\r\nA\r\n+\r\n!\r\n', (b'@x', b'A', b'+', b'!')),
    (b'@x\nA\n+\n!', (b'@x', b'A', b'+', b'!')),
    (b'@x\n\r\n+\n\r\n', (b'@x', b'', b'+', b'')),
    (b'@x\rZ\nA\x00\r\r\n+opaque\n!\x00\r', (b'@x\rZ', b'A\x00\r', b'+opaque', b'!\x00\r')),
    (b'@x\n\n+\n\n', (b'@x', b'', b'+', b'')),
])
def test_P16_T06_oracle_normalizes_only_ingress_crlf_and_preserves_opaque_final_bytes(raw, expected):
    stream = io.BytesIO(raw)
    assert full._record(stream, 1024) == expected
    assert full._record(stream, 1024) is None


@pytest.mark.parametrize('raw', [b'@x', b'@x\nA', b'@x\nA\n+', b'@x\n\n+\n',
                               b'x\nA\n+\n!\n', b'@x\nA\nx\n!\n', b'@x\nAA\n+\n!\n'])
def test_P16_T06_oracle_rejects_missing_lines_markers_and_length_mismatch(raw):
    with pytest.raises(full.IntegrityError):
        full._record(io.BytesIO(raw), 1024)


def test_P16_T06_canonical_recovery_preserves_cr_and_requires_every_lf():
    fields = full._record(io.BytesIO(b'@x\nA\r\n+\n!\r\n'), 32, canonical=True)
    assert fields == (b'@x', b'A\r', b'+', b'!\r')
    with pytest.raises(full.IntegrityError):
        full._record(io.BytesIO(b'@x\nA\n+\n!'), 32, canonical=True)


def test_P16_T06_record_read_and_retention_are_bounded_even_without_newline():
    class Bounded(io.BytesIO):
        def readline(self, size):
            assert size == 66
            return super().readline(size)
    with pytest.raises(full.IntegrityError):
        full._record(Bounded(b'@' + b'x' * 10000), 64)
    assert full._record(io.BytesIO(b'@x\nA\n+\n!\n'), 9) is not None
    with pytest.raises(full.ResourceError):
        full._record(io.BytesIO(b'@x\nA\n+\n!\n'), 8)


def test_P16_T06_independent_inventory_reads_all_members_to_trailer_and_retains_only_aggregates(tmp_path):
    inputs = _inputs(tmp_path / 'input')
    before = {path.name: path.read_bytes() for path in inputs.iterdir()}
    progress = []
    with full.build_catalog(input_dir=inputs) as catalog:
        expected, aggregate = full.scan_originals(catalog, 1024, lambda *items: progress.append(items))
    assert aggregate['samples'] == 3 and aggregate['pairs'] == 3 and aggregate['reads'] == 6
    assert aggregate['nonempty_samples'] == 2
    assert len(expected) == len(progress) == 3
    assert expected[2]['pairs'] == 0 and expected[2]['sizes'] == [0, 0]
    assert all(set(item) == {'sample_index', 'sample_id', 'pairs', 'sizes', 'digests'} for item in expected)
    assert before == {path.name: path.read_bytes() for path in inputs.iterdir()}
    path = inputs / 'private-s0_R1.fastq.gz'
    altered = bytearray(path.read_bytes())
    altered[-5] ^= 1
    path.write_bytes(altered)
    with full.build_catalog(input_dir=inputs) as catalog, pytest.raises((gzip.BadGzipFile, EOFError)):
        full.scan_originals(catalog, 1024)


def _restored(catalog, directory, maximum=1024):
    directory.mkdir()
    outputs = []
    for sample in catalog:
        part = dict(sample_index=sample.index, part_index=0, pairs=0)
        for role, source in zip(('r1', 'r2'), (sample.r1, sample.r2)):
            content = bytearray()
            count = 0
            with catalog.open_input(source) as raw, full._decoded(raw, source.advertised_gzip) as stream:
                while (fields := full._record(stream, maximum)) is not None:
                    content.extend(b'\n'.join(fields) + b'\n')
                    count += 1
            name = f'sample-{sample.index}-{role}.gz'
            (directory / name).write_bytes(gzip.compress(content))
            part[role] = dict(name=name)
            part['pairs'] = count
        outputs.append(part)
    return SimpleNamespace(outputs=outputs)


@pytest.mark.parametrize('corruption', ['field', 'ordinal', 'role', 'part_order', 'part_count', 'extra', 'missing', 'trailer'])
def test_P16_T06_direct_comparison_rejects_self_consistent_lengths_and_structural_corruptions(tmp_path, corruption):
    inputs = _inputs(tmp_path / 'input')
    directory = tmp_path / 'restored'
    with full.build_catalog(input_dir=inputs) as catalog:
        expected, _ = full.scan_originals(catalog, 1024)
        restored = _restored(catalog, directory)
        assert full.compare_recovered(catalog, restored, directory, expected, 1024)['pairs'] == 3
        path = directory / restored.outputs[0]['r1']['name']
        decoded = gzip.decompress(path.read_bytes())
        if corruption == 'field':
            path.write_bytes(gzip.compress(decoded.replace(b'opaque', b'OPaQUE')))
        elif corruption == 'ordinal':
            lines = decoded.split(b'\n')[:-1]
            path.write_bytes(gzip.compress(b'\n'.join(lines[4:] + lines[:4]) + b'\n'))
        elif corruption == 'role':
            restored.outputs[0]['r1'], restored.outputs[0]['r2'] = restored.outputs[0]['r2'], restored.outputs[0]['r1']
        elif corruption == 'part_order':
            restored.outputs[0]['part_index'] = 1
        elif corruption == 'part_count':
            restored.outputs[0]['pairs'] += 1
        elif corruption == 'extra':
            path.write_bytes(gzip.compress(decoded + decoded))
        elif corruption == 'missing':
            path.write_bytes(gzip.compress(b''))
        else:
            path.write_bytes(path.read_bytes()[:-1])
        with pytest.raises((full.IntegrityError, EOFError, gzip.BadGzipFile)):
            full.compare_recovered(catalog, restored, directory, expected, 1024)


def test_P16_T06_cleanup_preserves_foreign_replacements_unregistered_files_and_symlinks(tmp_path):
    root = tmp_path / 'owned'
    root.mkdir()
    (root / 'owned-file').write_bytes(b'owned')
    (root / 'replacement').write_bytes(b'original')
    owned = full.OwnedFiles(root)
    owned.add('owned-file')
    owned.add('replacement')
    foreign = tmp_path / 'replacement'
    foreign.write_bytes(b'foreign')
    foreign.replace(root / 'replacement')
    (root / 'unregistered').write_bytes(b'foreign')
    assert not owned.remove()
    assert not (root / 'owned-file').exists()
    assert (root / 'replacement').read_bytes() == (root / 'unregistered').read_bytes() == b'foreign'
    (root / 'link').symlink_to(tmp_path / 'replacement')
    with pytest.raises(full.IntegrityError):
        owned.add('link')


def test_P16_T06_cleanup_does_not_follow_replaced_directory(tmp_path):
    root = tmp_path / 'owned'
    root.mkdir()
    (root / 'artifact').write_bytes(b'owned')
    owned = full.OwnedFiles(root)
    owned.add('artifact')
    root.rename(tmp_path / 'original')
    root.symlink_to(tmp_path / 'original', target_is_directory=True)
    assert not owned.remove()
    assert (tmp_path / 'original' / 'artifact').read_bytes() == b'owned'


def test_P16_T06_space_admission_combines_real_devices_and_refuses_failed_or_insufficient_query(tmp_path, monkeypatch):
    inventory = dict(normalized_bytes=100, pairs=2)
    enough = SimpleNamespace(f_bavail=1 << 40, f_frsize=1)
    monkeypatch.setattr(full.os, 'statvfs', lambda _path: enough)
    result = full.space_admission(tmp_path, tmp_path, inventory, 'initial')
    assert result['shared_scratch_output_device']
    assert result['devices'][0]['roles'] == ['output', 'scratch']
    initial = result['devices'][0]['required_bytes']
    assert initial > full.space_admission(tmp_path, tmp_path, inventory, 'recover_indexed')['devices'][0]['required_bytes']
    monkeypatch.setattr(full.os, 'statvfs', lambda _path: SimpleNamespace(f_bavail=initial - 1, f_frsize=1))
    with pytest.raises(full.ResourceError):
        full.space_admission(tmp_path, tmp_path, inventory, 'initial')
    def failed(_path):
        raise OSError('private device path should not appear in report')
    monkeypatch.setattr(full.os, 'statvfs', failed)
    with pytest.raises(OSError):
        full.space_admission(tmp_path, tmp_path, inventory, 'initial')


def _candidate(tmp_path, monkeypatch):
    package = tmp_path / 'venv' / 'lib' / 'site-packages' / 'readmixer_py'
    package.mkdir(parents=True)
    (package / '__init__.py').write_bytes(b'__version__="test"\n')
    (package / 'module.py').write_bytes(b'answer=42\n')
    wheel = tmp_path / 'candidate.whl'
    with zipfile.ZipFile(wheel, 'w') as archive:
        for path in package.glob('*.py'):
            archive.writestr('readmixer_py/' + path.name, path.read_bytes())
    sdist = tmp_path / 'candidate.tar.gz'
    with tarfile.open(sdist, 'w:gz') as archive:
        archive.add(_SCRIPTS / 'qualify_supplied_data.py', arcname='candidate/scripts/qualify_supplied_data.py')
    monkeypatch.setattr(full.readmixer_py, '__file__', str(package / '__init__.py'))
    monkeypatch.setattr(full.metadata, 'version', lambda _name: 'test')
    return package, wheel, sdist


@pytest.mark.parametrize('change', ['none', 'archive', 'module', 'extra_module', 'harness'])
def test_P16_T06_candidate_validation_binds_installed_modules_harness_and_both_archive_hashes(tmp_path, monkeypatch, change):
    package, wheel, sdist = _candidate(tmp_path, monkeypatch)
    wheel_hash, source_hash = full._digest_file(wheel), full._digest_file(sdist)
    if change == 'archive':
        wheel.write_bytes(wheel.read_bytes() + b'changed')
    elif change == 'module':
        (package / 'module.py').write_bytes(b'changed')
    elif change == 'extra_module':
        (package / 'extra.py').write_bytes(b'changed')
    elif change == 'harness':
        with tarfile.open(sdist, 'w:gz') as archive:
            member = tarfile.TarInfo('candidate/scripts/qualify_supplied_data.py')
            member.size = len(b'changed')
            archive.addfile(member, io.BytesIO(b'changed'))
        source_hash = full._digest_file(sdist)
    if change != 'none':
        with pytest.raises(full.IntegrityError):
            full.candidate_identity(wheel, wheel_hash, sdist, source_hash)
    else:
        result = full.candidate_identity(wheel, wheel_hash, sdist, source_hash)
        assert result['installed_package_matches_wheel'] and result['outside_checkout']
        assert result['package_python_files'] == 2


def _run_arguments(tmp_path, monkeypatch, *, allow=True):
    monkeypatch.setattr(full, 'candidate_identity', lambda *_: {'version': 'test', 'installed_package_matches_wheel': True})
    inputs = _inputs(tmp_path / 'input')
    return dict(input_dir=inputs, workspace=tmp_path / 'private-run', wheel=tmp_path / 'wheel',
                wheel_sha256='0' * 64, sdist=tmp_path / 'sdist', sdist_sha256='0' * 64,
                max_ram=512 << 20, workers=1, allow_inventory_change=allow)


def test_P16_T06_historical_scope_change_requires_explicit_opt_in_before_mix(tmp_path, monkeypatch):
    arguments = _run_arguments(tmp_path, monkeypatch, allow=False)
    monkeypatch.setattr(full, 'mix', lambda *_: pytest.fail('mix must not run after changed inventory'))
    report = full.validate_supplied(**arguments)
    assert not report['completed'] and report['failed_phase'] == 'inventory'
    assert report['inventory_change']['pairs'] == {'historical': 30_042_389, 'observed': 3}
    assert report['original_files_unchanged'] and report['private_workspace_retained']
    assert arguments['workspace'].stat().st_mode & 0o777 == 0o700
    for path in arguments['workspace'].glob('*.json'):
        assert path.stat().st_mode & 0o777 == 0o600
    public = json.dumps(report)
    assert 'private-s' not in public and str(tmp_path) not in public and 'digests' not in public


def test_P16_T06_existing_workspace_and_input_tree_are_never_overwritten(tmp_path, monkeypatch):
    arguments = _run_arguments(tmp_path, monkeypatch)
    arguments['workspace'].mkdir()
    foreign = arguments['workspace'] / 'foreign'
    foreign.write_bytes(b'preserve')
    with pytest.raises(FileExistsError):
        full.validate_supplied(**arguments)
    assert foreign.read_bytes() == b'preserve'
    arguments['workspace'] = arguments['input_dir'] / 'unsafe'
    with pytest.raises(Exception):
        full.validate_supplied(**arguments)
    assert not arguments['workspace'].exists()


@pytest.mark.parametrize('failure', [OSError, KeyboardInterrupt])
def test_P16_T06_failed_operation_retains_private_journal_cause_and_unchanged_sources(tmp_path, monkeypatch, failure):
    arguments = _run_arguments(tmp_path, monkeypatch)
    def injected(*_):
        raise failure('private source and path must remain private')
    monkeypatch.setattr(full, '_measure', injected)
    report = full.validate_supplied(**arguments)
    assert not report['completed'] and report['failed_phase'] == 'mix'
    assert report['error'] == ('interrupted' if failure is KeyboardInterrupt else 'OSError')
    assert report['original_files_unchanged'] and report['private_workspace_retained']
    state = json.loads((arguments['workspace'] / 'private-progress.json').read_text())
    assert state['phase'] == 'mix' and not state['completed']
    assert 'private source and path' not in json.dumps(report)


@pytest.mark.parametrize('workers', [1, 4])
def test_P16_T06_complete_tiny_full_oracle_lane_runs_both_modes_and_cleans_only_owned_outputs(tmp_path, monkeypatch, workers):
    arguments = _run_arguments(tmp_path, monkeypatch)
    arguments['workers'] = workers
    arguments['max_ram'] = (512 if workers == 1 else 2048) << 20
    before = {path.name: path.read_bytes() for path in arguments['input_dir'].iterdir()}
    report = full.validate_supplied(**arguments)
    assert report['completed'], report
    assert report['indexed_recovery_equal'] and report['sequential_recovery_equal']
    assert report['pool_fully_verified'] and report['original_files_unchanged']
    assert report['mix_ingress']['exact_single_forward_pass']
    assert [item['operation'] for item in report['operations']] == ['mix', 'verify', 'recover_indexed', 'recover_sequential']
    assert all(item['actual_ram_budget_bytes'] == arguments['max_ram'] for item in report['operations'])
    assert report['workspace_cleanup_complete'] and not arguments['workspace'].exists()
    assert before == {path.name: path.read_bytes() for path in arguments['input_dir'].iterdir()}
    assert all(item['owned_recovery_removed_before_next_mode'] for item in report['comparisons'].values())
    public = json.dumps(report)
    assert 'private-s' not in public and str(tmp_path) not in public


def test_P16_T06_foreign_recovery_residue_is_preserved_and_blocks_next_recovery(tmp_path, monkeypatch):
    arguments = _run_arguments(tmp_path, monkeypatch)
    original = full.recover
    modes = []
    def foreign(config):
        result = original(config)
        modes.append(config.io.value)
        (config.output / 'foreign').write_bytes(b'preserve')
        return result
    monkeypatch.setattr(full, 'recover', foreign)
    report = full.validate_supplied(**arguments)
    assert not report['completed'] and report['failed_phase'] == 'cleanup_indexed'
    assert modes == ['indexed']
    assert report['original_files_unchanged'] and report['indexed_recovery_equal']
    assert (arguments['workspace'] / 'recover_indexed' / 'foreign').read_bytes() == b'preserve'
    assert (arguments['workspace'] / 'pool').is_dir()


def test_P16_T06_explicit_pool_retention_keeps_private_evidence_but_removes_both_restorations(tmp_path, monkeypatch):
    arguments = _run_arguments(tmp_path, monkeypatch)
    report = full.validate_supplied(**arguments, retain_pool=True)
    assert report['completed'] and report['pool_artifacts_retained']
    assert not report['workspace_cleanup_complete']
    assert {item.name for item in arguments['workspace'].iterdir()} == {
        'pool', 'private-inventory.json', 'private-progress.json'}
    state = json.loads((arguments['workspace'] / 'private-progress.json').read_text())
    assert state['completed'] and state['pool_retained']
