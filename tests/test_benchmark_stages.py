"""P16-T01: real component timing, independent oracles and failure retention."""
import copy
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest

from test_synthetic_tooling import load_script

stages = load_script('benchmark_stages')


def test_P16_T01_complete_component_benchmark_and_independent_golden_counts(tmp_path):
    report = stages.benchmark_stages(pairs=16, work_dir=tmp_path / 'stages')
    assert report['completed'] and report['scratch_cleanup_complete']
    assert len(report['stages']) == 10
    assert list((tmp_path / 'stages').iterdir()) == []
    assert report['stages']['frame_codec']['expected']['sha256'] == '22bc294761e26b6b6d9588472db96c15dc0f3ae76756a7cea5feebe23fd6c56e'
    assert report['stages']['tag_rendering']['expected']['sha256'] == '380a141eb502fe69817fe12494dc338ee09bcc6a960351b3297233a708012394'
    for name, lane in report['stages'].items():
        assert lane['completed'] and len(lane['measured_runs']) == 5
        for observation in [lane['warmup'], *lane['measured_runs']]:
            assert observation['completed'] and observation['error'] is None
            assert observation['wall_seconds'] > 0
            assert observation['process_cpu_seconds'] >= 0
            assert observation['result']['logical_bytes'] == lane['expected']['logical_bytes']
            assert observation['result']['sha256'] == lane['expected']['sha256']
        assert lane['summary']['wall_seconds']['observations'] == [row['wall_seconds'] for row in lane['measured_runs']]
    assert report['stages']['sort_merge']['warmup']['result']['witness']['merge_passes'] >= 2
    assert report['stages']['scratch_aead_codec']['warmup']['result']['witness']['authenticated_eof']
    assert report['stages']['indexed_gather']['warmup']['result']['items'] == 6
    assert report['stages']['parser']['warmup']['result']['witness']['gzip_trailers_consumed']


def test_P16_T01_equal_count_wrong_hash_invalidates_entire_lane_and_retains_all_failures(tmp_path, monkeypatch):
    def corrupt(self):
        return dict(self.expected['bucket_hash'], sha256='0' * 64)
    monkeypatch.setattr(stages._Suite, 'bucket_hash', corrupt)
    report = stages.benchmark_stages(pairs=16)
    assert not report['completed'] and report['temporary_workspace_removed']
    lane = report['stages']['bucket_hash']
    assert not lane['completed'] and lane['summary'] is None
    assert len(lane['measured_runs']) == 5
    assert all(row['error'] == 'independent_digest_or_count_mismatch' for row in [lane['warmup'], *lane['measured_runs']])
    assert report['stages']['sort_merge']['completed']


def test_P16_T01_failed_warmup_is_not_replaced_or_hidden(tmp_path, monkeypatch):
    original = stages._Suite.ranks
    calls = 0
    def interrupted(self):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError('private-record /private/path')
        return original(self)
    monkeypatch.setattr(stages._Suite, 'ranks', interrupted)
    report = stages.benchmark_stages(pairs=16)
    assert calls == 6
    lane = report['stages']['ranks']
    assert not lane['completed'] and lane['summary'] is None
    assert lane['warmup']['error'] == 'OSError'
    assert all(row['completed'] for row in lane['measured_runs'])
    assert 'private-record' not in json.dumps(report) and '/private/path' not in json.dumps(report)


def test_P16_T01_parser_rejects_corrupt_gzip_trailer(tmp_path):
    suite = stages._Suite(tmp_path, 16, 512 << 20)
    broken = bytearray(suite.gzip_mates[0])
    broken[-8] ^= 1
    suite.gzip_mates = (bytes(broken), suite.gzip_mates[1])
    observed = stages._observe(suite.parser, suite.expected['parser'])
    assert not observed['completed'] and observed['error'] == 'InputError'


def test_P16_T01_indexed_component_crosses_members_and_compares_sparse_bytes(tmp_path):
    suite = stages._Suite(tmp_path, 1200, 512 << 20)
    observed = stages._observe(suite.indexed_gather, suite.expected['indexed_gather'])
    assert observed['completed']
    witness = observed['result']['witness']
    assert witness['decoded_members'] >= 2 and witness['full_sidecar_consumed']
    assert witness['decoded_members'] <= witness['scanned_members']
    assert observed['result']['items'] == 400


@pytest.mark.parametrize('options', [dict(pairs=15), dict(pairs=32769), dict(pairs=True),
    dict(repetitions=4), dict(repetitions=True), dict(repetitions=1001),
    dict(startup='yes'), dict(max_ram=True), dict(max_ram=(64 << 20)-1)])
def test_P16_T01_invalid_component_limits_create_nothing(tmp_path, options):
    with pytest.raises(ValueError):
        stages.benchmark_stages(work_dir=tmp_path / 'absent', **options)
    assert not (tmp_path / 'absent').exists()


def test_P16_T01_stage_cli_produces_json_without_paths_or_stderr():
    result = subprocess.run([sys.executable, str(Path(stages.__file__)), '--pairs', '16'],
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0 and not result.stderr
    report = json.loads(result.stdout)
    assert report['completed'] and report['temporary_workspace_removed']
    assert '/tmp/' not in result.stdout
