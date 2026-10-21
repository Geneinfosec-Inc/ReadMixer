"""Independent v2 fixture recipes and scoped process/startup observations."""

from contextlib import nullcontext
import gzip
import hashlib
import hmac
import json
from pathlib import Path
import struct
import subprocess
import sys
from types import SimpleNamespace

import pytest

# Reuse only script loading; the independent oracle never imports runtime codecs.
from test_synthetic_tooling import benchmark, fixtures


def independent_v2(config, sample, ordinal, mate, *, version=2):
    assert version in (2, 3)
    key = hashlib.sha256(f'readmixer-python/fixtures/v{version}'.encode()).digest()
    def seed(field):
        return hmac.digest(key, struct.pack('>IQBB', sample, ordinal, mate, field), 'sha256')
    sequence, metadata = config.sequence_bytes, config.metadata_bytes
    if config.length_mode == 'varied-v2':
        sequence = int.from_bytes(seed(3)[:8], 'big') % (sequence + 1)
        metadata = int.from_bytes(seed(4)[:8], 'big') % (metadata + 1)
    def field(number, length):
        if config.profile == 'high-compressibility-v2':
            return b'~' * length
        result = bytearray()
        counter = 0
        while len(result) < length:
            block = hashlib.shake_256(seed(number) + counter.to_bytes(4, 'big')).digest(
                min(65536, length - len(result) + 256))
            for byte in block:
                if byte not in (10, 13):
                    result.append(byte)
                    if len(result) == length:
                        break
            counter += 1
        return bytes(result)
    return (f'@synthetic-s{sample:06d}-p{ordinal:020d}/{mate} meta='.encode() + field(0, metadata) + (b'~' if version == 3 else b''),
            field(1, sequence), f'+synthetic mate={mate} meta='.encode() + field(5, metadata),
            field(2, sequence))


@pytest.mark.parametrize('profile', ('high-compressibility-v2', 'low-compressibility-v2'))
@pytest.mark.parametrize('length_mode', ['fixed', 'varied-v2'])
def test_v2_profiles_match_independent_fields_lengths_and_totals(tmp_path, profile, length_mode):
    config = fixtures.FixtureConfig(counts=(7, 0, 3), sequence_bytes=51, metadata_bytes=19,
                                    profile=profile, length_mode=length_mode,
                                    compression='mixed', newline='crlf')
    report = fixtures.generate_fixtures(tmp_path / 'fixture', config)
    sizes, sequences, metadatas = [], [], []
    for sample in report['samples']:
        for mate in (1, 2):
            records = []
            for ordinal in range(sample['pairs']):
                expected = independent_v2(config, sample['sample_index'], ordinal, mate)
                assert fixtures.record_fields(config, sample['sample_index'], ordinal, mate) == expected
                assert all(b'\n' not in value and b'\r' not in value for value in expected)
                assert len(expected[1]) == len(expected[3])
                raw = b'\n'.join(expected) + b'\n'
                sizes.append(len(raw))
                sequences.append(len(expected[1]))
                metadatas.append((len(raw) - 75 - 2 * len(expected[1])) // 2)
                records.append(raw)
            entry = sample[f'r{mate}']
            expected = b''.join(records)
            physical = (tmp_path / 'fixture' / 'input' / entry['name']).read_bytes()
            decoded = gzip.decompress(physical) if entry['gzip'] else physical
            assert decoded == expected.replace(b'\n', b'\r\n')
            assert entry['normalized_sha256'] == hashlib.sha256(expected).hexdigest()
            assert entry['normalized_bytes'] == len(expected)
            assert entry['decoded_bytes'] == len(decoded)
    assert report['fixture_version'] == 2
    assert report['generator_label'] == 'readmixer-python/fixtures/v2'
    assert report['normalized_bytes'] == sum(sizes)
    assert max(sizes) <= config.max_record_bytes
    for name, values in [('record_bytes', sizes), ('sequence_bytes', sequences), ('metadata_bytes', metadatas)]:
        assert report['observed_lengths'][name] == dict(minimum=min(values), maximum=max(values), total=sum(values))
    if length_mode == 'varied-v2':
        assert len(set(sizes)) > 3


def test_low_compressibility_stream_crosses_expansion_chunks_without_bias_mapping():
    config = fixtures.FixtureConfig(sequence_bytes=65570, metadata_bytes=0, profile='low-compressibility-v2')
    actual = fixtures.record_fields(config, 0, 1, 2)
    assert actual == independent_v2(config, 0, 1, 2)
    assert set(actual[1]) == set(range(256)) - {10, 13}


def test_profiles_report_observed_compressibility_without_relabeled_v1(tmp_path):
    reports = {}
    for profile in fixtures.PROFILES[1:]:
        reports[profile] = fixtures.generate_fixtures(tmp_path / profile, fixtures.FixtureConfig(
            counts=(8,), sequence_bytes=4096, metadata_bytes=1024, profile=profile))
    high = reports['high-compressibility-v2']
    low = reports['low-compressibility-v2']
    assert high['normalized_bytes'] == low['normalized_bytes']
    assert high['physical_to_decoded_ratio'] < 0.03
    assert low['physical_to_decoded_ratio'] > 0.9
    assert 'no incompressibility guarantee' in low['compressibility_scope']
    modern = reports['low-compressibility-v3']
    assert modern['normalized_bytes'] == low['normalized_bytes'] + low['reads']
    assert modern['physical_to_decoded_ratio'] > 0.9


@pytest.mark.parametrize('kwargs', [dict(profile='v3'), dict(length_mode='varied'),
    dict(length_mode='varied-v2'), dict(profile='low-compressibility-v2', payload='repeated')])
def test_invalid_profile_combinations_fail_before_fixture_creation(kwargs):
    with pytest.raises(ValueError):
        fixtures.FixtureConfig(**kwargs)


def test_empty_v2_fixture_has_unknown_observed_lengths_and_no_division_by_zero(tmp_path):
    report = fixtures.generate_fixtures(tmp_path / 'empty', fixtures.FixtureConfig(
        counts=(0,), compression='plain', profile='low-compressibility-v2'))
    assert report['physical_to_decoded_ratio'] is None
    assert all(value == dict(minimum=None, maximum=None, total=0)
               for value in report['observed_lengths'].values())


class ObservedProcess:
    def __init__(self, pid, rss, ticks):
        self.pid, self.rss, self.ticks = pid, rss, ticks
        self.descendants = []
    def children(self, recursive=True):
        return self.descendants
    def oneshot(self):
        return nullcontext()
    def memory_info(self):
        return SimpleNamespace(rss=self.rss)
    def num_threads(self):
        return len(self.ticks)
    def threads(self):
        return [SimpleNamespace(id=number, user_time=value, system_time=0)
                for number, value in enumerate(self.ticks)]
    def create_time(self):
        return 100.0
    def cpu_times(self):
        return SimpleNamespace(user=sum(self.ticks), system=0)
    def num_fds(self):
        return 3


def test_sampler_distinguishes_thread_census_activity_and_live_descendant_cpu(tmp_path):
    root = ObservedProcess(1, 200, [1.0, 0.0])
    child = ObservedProcess(2, 100, [0.4])
    root.descendants.append(child)
    sampler = benchmark.ProcessSampler(tmp_path, process=root)
    sampler._sample()
    root.ticks[0] += 0.2
    child.ticks[0] += 0.3
    sampler._sample()
    report = sampler.result
    assert report['process_tree_peak_threads'] == 3
    assert report['sampled_peak_active_os_threads'] == 2
    assert report['sampled_os_thread_cpu_delta_seconds'] == pytest.approx(0.5)
    assert report['sampled_live_descendant_cpu_seconds'] == pytest.approx(0.7)
    assert report['process_tree_peak_rss_bytes'] == 300
    assert report['process_tree_last_descendants'] == 1
    assert benchmark._cpu_result(3, 5, 7)['aggregate_accounted_cpu_seconds'] == 5
    assert benchmark._cpu_result(3, None, None)['aggregate_accounted_cpu_seconds'] is None


def test_phase_accounts_for_waited_child_that_exits_between_all_samples(tmp_path, monkeypatch):
    if benchmark._reaped_child_cpu() is None:
        pytest.skip('waited-child CPU accounting is unavailable')
    original = benchmark.ProcessSampler
    baseline_processes = 1 + len(benchmark.psutil.Process().children(recursive=True))
    monkeypatch.setattr(benchmark, 'ProcessSampler', lambda scratch: original(scratch, interval=10))
    def action():
        subprocess.run([sys.executable, '-c',
            'import time\nstart=time.process_time()\nwhile time.process_time()-start<0.04: pass'], check=True)
        return SimpleNamespace(completed=True, elapsed_seconds=0, samples=1, pairs=1, reads=2,
            normalized_bytes=10, workers_effective=1, resources=SimpleNamespace(to_dict=lambda: {}))
    _, measured = benchmark._timed_phase(action, tmp_path)
    assert measured['sampled']['process_tree_peak_processes'] == baseline_processes
    assert measured['reaped_child_cpu_seconds'] >= 0.035
    assert measured['aggregate_accounted_cpu_seconds'] == pytest.approx(
        measured['process_cpu_seconds'] + measured['reaped_child_cpu_seconds'])
    assert measured['sampled']['process_tree_last_descendants'] == baseline_processes - 1


def test_startup_lanes_are_fresh_processes_with_warmup_and_five_correct_observations(tmp_path):
    config = fixtures.FixtureConfig(counts=(1, 1, 0), sequence_bytes=0, metadata_bytes=0)
    fixture = fixtures.generate_fixtures(tmp_path / 'fixture', config)
    report = benchmark.startup_benchmark(tmp_path / 'fixture' / 'input', fixture, max_ram=256 << 20)
    assert report['completed']
    assert set(report['lanes']) == {'interpreter', 'api_import', 'cli_inspect'}
    for kind, lane in report['lanes'].items():
        assert len(lane['measured_runs']) == 5
        for observation in [lane['warmup'], *lane['measured_runs']]:
            assert observation['completed'] and observation['exit_code'] == 0
            assert observation['wall_seconds'] > 0
            assert observation['parent_monitor_cpu_seconds'] >= 0
            if benchmark._reaped_child_cpu() is not None:
                assert observation['child_cpu_seconds'] > 0
            if kind != 'interpreter':
                child = observation['child']
                assert child['candidate_matches']
                assert child['before_readmixer_import']['os_threads'] >= 1
                assert child['after_operation']['rss_bytes'] > 0
            if kind == 'cli_inspect':
                assert child['operation']['pairs'] == 2
                assert child['operation']['deep'] is True
        assert lane['summary']['wall_seconds'] == benchmark.summarize(
            run['wall_seconds'] for run in lane['measured_runs'])
    assert str(tmp_path) not in json.dumps(report)


def test_startup_failure_is_retained_and_has_no_performance_summary(tmp_path, monkeypatch):
    count = 0
    def failure(*args, **kwargs):
        nonlocal count
        count += 1
        return dict(completed=count != 3, wall_seconds=1.0, child_cpu_seconds=0.1)
    monkeypatch.setattr(benchmark, '_startup_observation', failure)
    report = benchmark.startup_benchmark(tmp_path, {}, repetitions=5)
    assert not report['completed'] and count == 18
    assert report['lanes']['interpreter']['summary'] is None
    assert report['lanes']['api_import']['summary'] is not None
    assert len(report['lanes']['interpreter']['measured_runs']) == 5


def test_startup_timeout_reaps_its_child_and_does_not_expose_diagnostics(tmp_path, monkeypatch):
    original = subprocess.Popen
    started = []
    def slow_child(command, **kwargs):
        process = original([sys.executable, '-c', 'import time; time.sleep(30)'], **kwargs)
        started.append(process)
        return process
    monkeypatch.setattr(benchmark.subprocess, 'Popen', slow_child)
    report = benchmark._startup_observation('interpreter', tmp_path, {'max_record_bytes': 1},
                                            max_ram=256 << 20, timeout=0.03)
    assert not report['completed'] and report['timed_out']
    assert report['error'] == 'timeout'
    assert started[0].poll() is not None
    assert str(tmp_path) not in json.dumps(report)


def test_v2_varied_binary_fixture_completes_both_recovery_modes():
    result = benchmark.benchmark(fixture_config=fixtures.FixtureConfig(counts=(2, 1, 0),
        sequence_bytes=257, metadata_bytes=33, profile='low-compressibility-v2', length_mode='varied-v2'),
        workers=1, storage=benchmark.StorageMode.EXTERNAL, max_ram=256 << 20)
    assert result['completed'] and result['temporary_workspace_removed']
    assert result['fixture']['fixture_version'] == 2
    assert all(all(run['correctness'].values()) for run in result['measured_runs'])
    assert result['startup'] is None


@pytest.mark.parametrize('compression', ['plain', 'gzip', 'mixed'])
@pytest.mark.parametrize('length_mode', ['fixed', 'varied-v2'])
def test_v3_independent_bytes_domains_guard_lengths_and_reports(tmp_path, compression, length_mode):
    config = fixtures.FixtureConfig(counts=(7, 0, 3), sequence_bytes=51, metadata_bytes=19,
        profile='low-compressibility-v3', length_mode=length_mode, compression=compression, newline='crlf')
    report = fixtures.generate_fixtures(tmp_path / 'v3', config)
    lengths, metadata_lengths = [], []
    for sample in report['samples']:
        for mate in (1, 2):
            expected = bytearray()
            for ordinal in range(sample['pairs']):
                fields = independent_v2(config, sample['sample_index'], ordinal, mate, version=3)
                assert fixtures.record_fields(config, sample['sample_index'], ordinal, mate) == fields
                assert fields[0].endswith(b'~') and b' meta=' in fields[0]
                assert all(b'\r' not in field and b'\n' not in field for field in fields)
                record = b'\n'.join(fields) + b'\n'
                lengths.append(len(record))
                metadata_lengths.append((len(record) - 76 - 2 * len(fields[1])) // 2)
                expected.extend(record)
            entry = sample[f'r{mate}']
            physical = (tmp_path / 'v3' / 'input' / entry['name']).read_bytes()
            decoded = gzip.decompress(physical) if entry['gzip'] else physical
            assert decoded == bytes(expected).replace(b'\n', b'\r\n')
            assert entry['normalized_bytes'] == len(expected)
            assert entry['normalized_sha256'] == hashlib.sha256(expected).hexdigest()
            assert entry['decoded_bytes'] == len(decoded)
    assert report['fixture_version'] == 3
    assert report['generator_label'] == 'readmixer-python/fixtures/v3'
    assert report['public_generator_key_sha256'] == hashlib.sha256(b'readmixer-python/fixtures/v3').hexdigest()
    assert report['header_guard_bytes'] == 1 and report['record_fixed_overhead_bytes'] == 76
    assert config.max_record_bytes == 76 + 2 * 19 + 2 * 51
    assert max(lengths) <= config.max_record_bytes
    assert report['normalized_bytes'] == sum(lengths)
    assert report['observed_lengths']['metadata_bytes'] == dict(minimum=min(metadata_lengths),
        maximum=max(metadata_lengths), total=sum(metadata_lengths))
    if length_mode == 'fixed':
        assert min(lengths) == max(lengths) == config.max_record_bytes
    else:
        assert len(set(lengths)) > 3


def test_v3_guard_fixes_proved_legacy_role_collision_without_changing_legacy_bytes():
    from readmixer_py.fastq import header_identity
    from readmixer_py.errors import InputError
    legacy = fixtures.FixtureConfig(sequence_bytes=0, metadata_bytes=32, profile='low-compressibility-v2')
    old = fixtures.record_fields(legacy, 0, 1198, 2)
    assert old == independent_v2(legacy, 0, 1198, 2)
    with pytest.raises(InputError, match='conflicting role'):
        header_identity(old[0])
    modern = fixtures.FixtureConfig(sequence_bytes=0, metadata_bytes=32, profile='low-compressibility-v3')
    new = fixtures.record_fields(modern, 0, 1198, 2)
    assert new == independent_v2(modern, 0, 1198, 2, version=3)
    base, role = header_identity(new[0])
    assert bytes(base) == b'synthetic-s000000-p00000000000000001198' and role == 2
    assert modern.max_record_bytes == legacy.max_record_bytes + 1


def test_v3_opaque_headers_preserve_pair_identity_through_large_lane_ranges():
    from readmixer_py.fastq import header_identity
    config = fixtures.FixtureConfig(sequence_bytes=0, metadata_bytes=32, profile='low-compressibility-v3')
    # All 40k pairs of the large physical lane plus the second sample of the
    # complete 4096+4096 ENOSPC workload. Retain only the current fields.
    for sample, count in ((0, 40000), (1, 4096)):
        for ordinal in range(count):
            expected = f'synthetic-s{sample:06d}-p{ordinal:020d}'.encode()
            for mate in (1, 2):
                header, sequence, plus, quality = fixtures.record_fields(config, sample, ordinal, mate)
                identity, role = header_identity(header)
                assert bytes(identity) == expected and role == mate
                assert header.endswith(b'~') and not sequence and not quality
                assert sum(map(len, (header, sequence, plus, quality))) + 4 == config.max_record_bytes


def test_v3_zero_metadata_and_expansion_boundaries_keep_explicit_syntax_guard():
    config = fixtures.FixtureConfig(sequence_bytes=65570, metadata_bytes=0, profile='low-compressibility-v3')
    fields = fixtures.record_fields(config, 0, 1, 2)
    assert fields == independent_v2(config, 0, 1, 2, version=3)
    assert fields[0].endswith(b' meta=~') and fields[2].endswith(b' meta=')
    assert set(fields[1]) == set(range(256)) - {10, 13}
    assert sum(map(len, fields)) + 4 == config.max_record_bytes
