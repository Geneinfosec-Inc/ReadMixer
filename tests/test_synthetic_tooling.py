"""Public symbolic fixture vectors and complete benchmark correctness evidence."""
import gzip
import hashlib
import hmac
import importlib.util
import json
from pathlib import Path
import struct
import subprocess
import sys
from types import SimpleNamespace

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / 'scripts'


def load_script(name):
    spec = importlib.util.spec_from_file_location(name, _SCRIPTS / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


fixtures = load_script('generate_fixtures')
benchmark = load_script('benchmark_pipeline')


def independent_record(sample, ordinal, mate, sequence_bytes, metadata_bytes):
    key = hashlib.sha256(b'readmixer-python/fixtures/v1').digest()
    fields = []
    for number, size, symbols in ((0, metadata_bytes, b'0123456789abcdef'),
                                  (1, sequence_bytes, b'XYZ0123456789!?%'),
                                  (2, sequence_bytes, bytes(range(33, 97)))):
        seed = hmac.digest(key, struct.pack('>IQBB', sample, ordinal, mate, number), 'sha256')
        fields.append(bytes(symbols[n % len(symbols)] for n in hashlib.shake_256(seed).digest(size)))
    metadata, sequence, quality = fields
    return (f'@synthetic-s{sample:06d}-p{ordinal:020d}/{mate} meta='.encode() + metadata + b'\n' +
            sequence + b'\n' + f'+synthetic mate={mate} meta='.encode() + metadata + b'\n' + quality + b'\n')


def test_public_version_one_key_and_exact_symbolic_golden_records():
    assert fixtures.GENERATOR_KEY.hex() == 'f0a2af121098bd786cabe0af4f7ee952916dae900dbc2572c87821b7b0e1ff48'
    config = fixtures.FixtureConfig(counts=(1,), sequence_bytes=8, metadata_bytes=4)
    expected1 = b'@synthetic-s000000-p00000000000000000000/1 meta=f6f8\n6139326?\n+synthetic mate=1 meta=f6f8\n(\\/;F-!R\n'
    expected2 = b"@synthetic-s000000-p00000000000000000000/2 meta=6da7\n33%84%9%\n+synthetic mate=2 meta=6da7\n2-'QWM6N\n"
    for mate, expected in ((1, expected1), (2, expected2)):
        actual = b'\n'.join(fixtures.record_fields(config, 0, 0, mate)) + b'\n'
        assert actual == expected == independent_record(0, 0, mate, 8, 4)
        assert len(actual) == config.max_record_bytes
    assert set(fixtures._SYMBOLS).isdisjoint(b'ACGTNacgtn')
    assert len(fixtures._SYMBOLS) == 16


@pytest.mark.parametrize('compression', ['plain', 'gzip', 'mixed'])
@pytest.mark.parametrize('newline', ['lf', 'crlf'])
def test_streamed_files_match_independent_normalized_bytes_and_gzip_is_reproducible(tmp_path, compression, newline):
    config = fixtures.FixtureConfig(counts=(3, 0, 2), sequence_bytes=17, metadata_bytes=9,
                                    newline=newline, compression=compression)
    report = fixtures.generate_fixtures(tmp_path / 'first', config)
    second = fixtures.generate_fixtures(tmp_path / 'second', config)
    assert report == second
    assert report['pairs'] == 5 and report['reads'] == 10
    assert report['normalized_bytes'] == 10 * config.max_record_bytes
    assert (tmp_path / 'first').stat().st_mode & 0o777 == 0o700
    assert (tmp_path / 'first' / 'fixtures.json').stat().st_mode & 0o777 == 0o600
    for sample in report['samples']:
        for mate in (1, 2):
            entry = sample[f'r{mate}']
            path = tmp_path / 'first' / 'input' / entry['name']
            raw = path.read_bytes()
            assert raw == (tmp_path / 'second' / 'input' / entry['name']).read_bytes()
            assert path.stat().st_mode & 0o777 == 0o600
            decoded = gzip.decompress(raw) if entry['gzip'] else raw
            expected = b''.join(independent_record(sample['sample_index'], ordinal, mate, 17, 9)
                                for ordinal in range(sample['pairs']))
            assert decoded == (expected if newline == 'lf' else expected.replace(b'\n', b'\r\n'))
            assert entry['normalized_sha256'] == hashlib.sha256(expected).hexdigest()
            assert entry['normalized_bytes'] == len(expected)
            assert entry['physical_bytes'] == len(raw)
            if entry['gzip']:
                assert raw[4:8] == b'\0' * 4  # no wall-clock mtime
                assert raw[3] & 8 == 0        # no environment-dependent filename


def test_repeated_profile_and_empty_records_are_explicitly_artificial(tmp_path):
    config = fixtures.FixtureConfig(counts=(2, 0), sequence_bytes=0, metadata_bytes=5,
                                    payload='repeated', compression='plain')
    report = fixtures.generate_fixtures(tmp_path / 'fixture', config)
    assert report['biological_reference_used'] is False
    assert report['public_artificial_test_material'] is True
    for mate in (1, 2):
        fields = fixtures.record_fields(config, 0, 0, mate)
        assert fields[1] == fields[3] == b''
        assert len(set(fields[0].split(b'meta=')[1])) == 1
        assert sum(len(field) + 1 for field in fields) == config.max_record_bytes


@pytest.mark.parametrize('kwargs', [dict(counts=()), dict(counts=(True,)), dict(counts=(-1,)),
    dict(counts=((1 << 64) - 1, 1)), dict(sequence_bytes=-1), dict(sequence_bytes=(4 << 20) + 1),
    dict(metadata_bytes=True), dict(newline='native'), dict(compression='unknown'),
    dict(payload='private'), dict(gzip_level=10)])
def test_generator_validates_configuration_before_creation(kwargs):
    with pytest.raises(ValueError):
        fixtures.FixtureConfig(**kwargs)


def test_generator_never_overwrites_foreign_files_or_directories(tmp_path):
    directory = tmp_path / 'existing'
    directory.mkdir()
    foreign = directory / 'foreign'
    foreign.write_bytes(b'preserve')
    with pytest.raises(FileExistsError):
        fixtures.generate_fixtures(directory, fixtures.FixtureConfig(counts=(1,)))
    assert list(directory.iterdir()) == [foreign] and foreign.read_bytes() == b'preserve'


def test_failed_fixture_generation_has_no_completion_marker(tmp_path, monkeypatch):
    calls = 0
    original = fixtures.record_fields
    def fail_later(*args):
        nonlocal calls
        calls += 1
        if calls == 3:
            raise OSError('injected generator failure')
        return original(*args)
    monkeypatch.setattr(fixtures, 'record_fields', fail_later)
    with pytest.raises(OSError):
        fixtures.generate_fixtures(tmp_path / 'partial', fixtures.FixtureConfig(counts=(3,)))
    assert calls == 3 and not (tmp_path / 'partial' / 'fixtures.json').exists()


def test_benchmark_summary_retains_every_observation_and_reports_variability():
    assert benchmark.summarize([5, 2, 9, 1, 3]) == dict(
        observations=[5, 2, 9, 1, 3], median=3, minimum=1, maximum=9, range=8)
    with pytest.raises(ValueError):
        benchmark.summarize([])


@pytest.mark.parametrize('kwargs', [dict(repetitions=4), dict(repetitions=True),
    dict(repetitions=1001), dict(selected=(0, 0)), dict(selected=(3,)), dict(selected=())])
def test_benchmark_requires_five_timed_runs_and_valid_selection_before_outputs(tmp_path, kwargs):
    with pytest.raises(ValueError):
        benchmark.benchmark(work_dir=tmp_path / 'must-not-exist', **kwargs)
    assert not (tmp_path / 'must-not-exist').exists()


def test_independent_recovery_oracle_rejects_equal_length_modified_bytes(tmp_path):
    expected = independent_record(0, 0, 1, 8, 4)
    paths = []
    for mate in (1, 2):
        path = tmp_path / f'mate{mate}.gz'
        path.write_bytes(gzip.compress(expected.replace(b'6139326?', b'XXXXXXXX')))
        paths.append(path)
    report = SimpleNamespace(outputs=({'sample_index': 0, 'part_index': 0,
        'r1': {'name': paths[0].name}, 'r2': {'name': paths[1].name}},))
    entry = dict(normalized_sha256=hashlib.sha256(expected).hexdigest(), normalized_bytes=len(expected))
    with pytest.raises(benchmark.IntegrityError, match='independent fixture oracle'):
        benchmark.check_recovered(report, tmp_path, [dict(sample_index=0, pairs=1, r1=entry, r2=entry)])


def test_complete_benchmark_warmup_plus_five_runs_includes_empty_selection_and_cleans_temp():
    result = subprocess.run([sys.executable, str(_SCRIPTS / 'benchmark_pipeline.py'),
        '--counts', '1,1,0', '--sequence-bytes', '0', '--metadata-bytes', '0',
        '--select', '0,2', '--workers', '2', '--storage', 'external', '--startup'],
        capture_output=True, text=True, check=False, timeout=90)
    assert result.returncode == 0, result.stdout + result.stderr
    assert not result.stderr
    report = json.loads(result.stdout)
    assert report['completed'] and report['temporary_workspace_removed']
    assert not report['artifacts_retained']
    assert report['configuration']['selected_pair_fraction'] == 0.5
    assert report['fixture']['pairs'] == 2
    assert report['startup']['completed']
    assert len(report['startup']['lanes']['cli_inspect']['measured_runs']) == 5
    assert len(report['measured_runs']) == 5
    assert report['versions'] == benchmark._versions()
    assert len(report['versions']['source_python_sha256']) == 64
    for run in [report['warmup'], *report['measured_runs']]:
        assert all(run['correctness'].values())
        assert run['independent_recovery_check_seconds'] >= 0
        assert set(run['phases']) == {'mix', 'verify', 'recover_indexed', 'recover_sequential'}
        for phase, measured in run['phases'].items():
            assert measured['wall_seconds'] > 0 and measured['process_cpu_seconds'] >= 0
            assert measured['sampled']['observations'] >= 2
            assert measured['sampled']['process_tree_peak_rss_bytes'] > 0
            assert measured['sampled']['process_tree_peak_processes'] >= 1
            assert measured['cleanup_complete']
            assert measured['pairs'] == (1 if phase.startswith('recover') else 2)
            assert measured['resources']['envelope_qualified'] is False
    observations = [run['wall_seconds'] for run in report['measured_runs']]
    assert report['summary']['wall_seconds'] == benchmark.summarize(observations)
    assert any('not a release' in item for item in report['limitations'])


def test_benchmark_fingerprints_actual_imported_installed_package_when_checkout_src_is_absent(tmp_path, monkeypatch):
    scripts = tmp_path / 'outside-checkout' / 'scripts'
    scripts.mkdir(parents=True)
    installed = tmp_path / 'environment' / 'site-packages' / 'readmixer_py'
    (installed / 'artifacts').mkdir(parents=True)
    files = {'__init__.py': b'VERSION = "installed-test"\n',
             'artifacts/test.py': b'# installed candidate only\n'}
    expected = hashlib.sha256()
    for relative, data in sorted(files.items()):
        (installed / relative).write_bytes(data)
        expected.update(('readmixer_py/' + relative).encode() + b'\0')
        expected.update(data)
    monkeypatch.setattr(benchmark, '_HERE', scripts)
    monkeypatch.setattr(benchmark.readmixer_py, '__file__', str(installed / '__init__.py'))
    versions = benchmark._versions()
    assert not (scripts.parent / 'src').exists()
    assert versions['measured_candidate'] == 'installed-package'
    assert versions['source_python_sha256'] == expected.hexdigest()
    assert versions['fingerprinted_python_files'] == 2
    assert str(tmp_path) not in json.dumps(versions)


def test_invalid_benchmark_size_is_a_cli_argument_error_without_traceback():
    result = subprocess.run([sys.executable, str(_SCRIPTS / 'benchmark_pipeline.py'),
                             '--max-ram', 'wrong'], capture_output=True, text=True, check=False)
    assert result.returncode == 2
    assert 'Traceback' not in result.stdout + result.stderr
    assert 'SIZE requires' in result.stderr
