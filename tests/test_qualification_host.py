"""Host requests only pass when actual kernel observations match."""
import importlib.util
from pathlib import Path
import sys
import pytest

_spec = importlib.util.spec_from_file_location('qualification_host', Path(__file__).parents[1] / 'scripts' / 'qualification_host.py')
host = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(host)

@pytest.mark.parametrize('controls', [{'unknown': 1}, {'memory_bytes': True}, {'cpu_quota': 0},
    {'affinity': []}, {'affinity': [-1]}, {'nofile': '96'}, {'scratch_mount': {'path': 'relative', 'bytes': 1024}}])
def test_invalid_host_requests_refused(controls):
    with pytest.raises(ValueError):
        host._validate(controls)


def test_requested_is_not_observed():
    controls = dict(memory_bytes=1024, cpu_quota=2, affinity=[1], nofile=96,
                    scratch_mount={'path': '/tmp/private', 'bytes': 1024})
    assert not host.verified_controls(controls, {})['verified']
    actual = dict(cgroup={'memory.max': '1024', 'memory.swap.max': '0', 'cpu.max': '200000 100000'},
                  nofile=[96, 1024], affinity=[1], private_scratch_mounted=True,
                  scratch_capacity_bytes=4096, scratch_block_bytes=4096)
    assert host.verified_controls(controls, actual)['verified']
    actual['cgroup']['memory.swap.max'] = 'max'
    assert not host.verified_controls(controls, actual)['verified']


@pytest.mark.skipif(sys.platform != 'linux', reason=(
    'Linux /proc namespace launcher; native portable nofile coverage runs in '
    'tools/release/tests/test_resource_lane.py'))
def test_owned_child_nofile_without_parent_mutation(tmp_path):
    before = host.host_snapshot()['nofile']
    report = tmp_path/'host.json'
    result = host.run_host([sys.executable, '-c', 'import resource; assert resource.getrlimit(resource.RLIMIT_NOFILE)[0] == 96'],
                           controls={'nofile': 96}, report=report, timeout=15)
    assert result['returncode'] == 0 and result['report_written']
    import json
    observation = json.loads(report.read_text())
    assert observation['controls']['verified']
    assert host.host_snapshot()['nofile'] == before
    with pytest.raises(FileExistsError):
        host.run_host([sys.executable, '-c', 'pass'], report=report)
