"""P16-T04 tooling: fresh-process observations, ownership, and honest limits."""
import json
from pathlib import Path
import sys

import pytest

from test_synthetic_tooling import load_script
calibration = load_script('calibrate_resources')


@pytest.mark.parametrize('case', calibration.CASES)
def test_component_probe_runs_actual_work_and_cleans_owned_resources(case):
    report = calibration.observe_case(case, scale_bytes=65536, pair_count=64)
    assert report['completed'] and report['cleanup_complete']
    assert report['case'] == case and report['charged_bytes'] > 0
    assert report['before']['rss_bytes'] > 0 and report['after']['rss_bytes'] > 0
    assert report['sampled_peak_rss_bytes'] >= report['before']['rss_bytes']
    assert report['samples'] >= 3 and report['sample_errors'] == 0
    assert not report['envelope_qualified']
    assert report['observations']
    encoded = json.dumps(report)
    assert '/tmp/' not in encoded
    if case == 'memory_handoff':
        assert report['observations']['sender_released_after_ack']
    if case == 'metadata':
        assert report['observations']['owners'] == 64
    if case == 'packed_terminal':
        assert report['observations']['offsets_bytes'] == 8 * 65


@pytest.mark.parametrize('kwargs', [dict(case='unknown'), dict(scale_bytes=63),
    dict(scale_bytes=(64 << 20) + 1), dict(pair_count=0), dict(pair_count=32769),
    dict(pair_count=True)])
def test_probe_bounds_reject_before_allocation(kwargs):
    with pytest.raises(ValueError):
        calibration.observe_case(**(dict(case='parser') | kwargs))


@pytest.mark.parametrize('kwargs', [dict(repetitions=4), dict(repetitions=101),
    dict(repetitions=True), dict(cases=()), dict(cases=('parser', 'parser')),
    dict(timeout=0)])
def test_matrix_requires_full_protocol_and_bounded_definitions(kwargs):
    with pytest.raises(ValueError):
        calibration.calibrate(**kwargs)


def test_fresh_children_retain_warmup_and_five_observations_with_native_limits():
    report = calibration.calibrate(cases=('bgzf',), scale_bytes=65536, pair_count=64)
    assert report['completed'] and not report['envelope_qualified']
    case = report['cases'][0]
    assert case['warmup']['completed'] and len(case['measured']) == 5
    for observation in (case['warmup'], *case['measured']):
        assert observation['child_reaped']
        assert observation['sampled_peak_threads'] <= 2  # coordinator + observer; no BLAS pool
        assert observation['elapsed_launch_seconds'] > observation['wall_seconds']
        assert observation['observations']['decoded_bytes'] == 65280
    assert case['summary']['minimum_seconds'] <= case['summary']['median_seconds'] <= case['summary']['maximum_seconds']
    assert report['native_limits']['OPENBLAS_NUM_THREADS'] == '1'


def test_failed_cell_keeps_every_observation_and_suppresses_summary(monkeypatch):
    observations = []
    def child(case, *_):
        index = len(observations)
        result = dict(completed=index != 2, case=case, wall_seconds=index + 1,
                      sampled_growth_exceeds_charge=index == 4)
        observations.append(result)
        return result
    monkeypatch.setattr(calibration, '_child_observation', child)
    report = calibration.calibrate(cases=('parser',))
    assert not report['completed']
    assert report['cases'][0]['summary'] is None
    assert report['cases'][0]['observed_charge_excess']
    assert report['cases'][0]['warmup'] == observations[0]
    assert report['cases'][0]['measured'] == observations[1:]


def test_timed_out_child_is_killed_reaped_and_diagnostics_are_not_returned(monkeypatch):
    import subprocess
    actual = calibration.subprocess.Popen
    children = []
    def slow(*_args, **kwargs):
        child = actual([sys.executable, '-c',
            'import sys,time; print("PRIVATE_DIAGNOSTIC", file=sys.stderr); time.sleep(30)'], **kwargs)
        children.append(child)
        return child
    monkeypatch.setattr(calibration.subprocess, 'Popen', slow)
    result = calibration._child_observation('parser', 65536, 64, 0.02)
    assert not result['completed'] and result['error'] == 'TimeoutExpired'
    assert result['child_reaped'] and children[0].poll() is not None
    assert 'PRIVATE_DIAGNOSTIC' not in json.dumps(result)
