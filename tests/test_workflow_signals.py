"""P15-T07/T08 real public CLI signals and acknowledged SIGKILL residue.

Every helper imports the currently tested package (including an installed wheel)
from outside the checkout. Hooks expose concrete stage witnesses without changing
CLI handlers, scheduler deadlines, rollback, or commit decisions.
"""
import json
import os
from pathlib import Path
import shutil
import signal
import stat
import subprocess
import sys
import time

import psutil
import pytest
import readmixer_py
from readmixer_py import MixConfig, VerifyConfig, mix, verify
from test_pipeline import fixtures

pytestmark = pytest.mark.skipif(os.name != 'posix', reason='POSIX real signal and owned process-group acceptance')


def _snapshot(path):
    facts = path.stat()
    return (path.read_bytes(), facts.st_dev, facts.st_ino, facts.st_mode, facts.st_size,
            facts.st_mtime_ns, facts.st_ctime_ns)


def _prepare(tmp_path, phase):
    control = tmp_path / 'control'
    control.mkdir()
    shutil.copyfile(Path(__file__).with_name('workflow_signal_harness.py'),
                    control / 'workflow_signal_harness.py')
    inputs, destination, scratch = [tmp_path / name for name in ('PRIVATE_INPUT', 'output', 'scratch')]
    fixtures(inputs, counts=(37, 29, 0), metadata=13, sequence=40)
    destination.mkdir(mode=0o700)
    scratch.mkdir(mode=0o700)
    foreign = destination / 'foreign'
    foreign.write_bytes(b'foreign file must survive')
    foreign.chmod(0o600)
    original = {path: _snapshot(path) for path in inputs.iterdir()}
    original[foreign] = _snapshot(foreign)
    if phase in ('gather-indexed', 'gather-sequential', 'verify-scan', 'merge'):
        pool = tmp_path / 'source-pool'
        result = mix(MixConfig(input_dir=inputs, output=pool, workers=1, max_ram=512 << 20,
                               max_record_bytes=4096))
        original.update({path: _snapshot(path) for path in pool.iterdir()})
        manifest = pool / result.manifest
        if phase == 'verify-scan':
            args = ['verify', '--manifest', str(manifest)]
        else:
            args = ['recover', '--manifest', str(manifest), '--output', str(destination),
                    '--sample', 'alpha', '--sample', 'beta', '--io',
                    'indexed' if phase == 'merge' else phase.removeprefix('gather-')]
    else:
        args = ['mix', '--input', str(inputs), '--output', str(destination)]
    args += ['--workers', '1' if phase.startswith('crash-') else '4', '--max-ram', '2GiB',
             '--max-record-bytes', '4KiB', '--storage', 'external', '--temp-dir', str(scratch), '--json']
    environment = os.environ.copy()
    environment['PYTHONPATH'] = str(Path(readmixer_py.__file__).resolve().parent.parent)
    environment['P15_CONTROL'], environment['P15_PHASE'] = str(control), phase
    process = subprocess.Popen([sys.executable, '-m', 'workflow_signal_harness', *args],
        cwd=control, env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        start_new_session=True)
    return process, control, destination, scratch, original


def _ready(process, control):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        ready = control / 'ready.json'
        if ready.exists():
            return json.loads(ready.read_text())
        if process.poll() is not None:
            stdout, stderr = process.communicate(timeout=2)
            pytest.fail(f'workflow exited before its acknowledged stage: {process.returncode}; {stdout!r}; {stderr[-4000:]!r}')
        time.sleep(0.01)
    pytest.fail('workflow never acknowledged its intended stage')


def _reap(process, control):
    (control / 'release').touch()
    # Only this helper's fresh session/process group is signaled. Healthy runs
    # already joined their children; timeout cleanup cannot target user processes.
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.communicate(timeout=5)


def _preserved(original, destination):
    assert all(_snapshot(path) == snapshot for path, snapshot in original.items())
    assert (destination / 'foreign').read_bytes() == b'foreign file must survive'


@pytest.mark.parametrize('signum', [signal.SIGINT, signal.SIGTERM], ids=['SIGINT', 'SIGTERM'])
@pytest.mark.parametrize('phase', ['parser', 'native', 'gather-indexed', 'gather-sequential',
                                  'verify-scan', 'merge', 'compression', 'publication', 'report'])
def test_P15_T07_T08_real_cli_signal_at_acknowledged_workflow_stage(tmp_path, phase, signum):
    process, control, destination, scratch, original = _prepare(tmp_path, phase)
    try:
        witness = _ready(process, control)
        assert witness['phase'] == phase
        pids = json.loads((control / 'pids.json').read_text())
        assert pids, 'the production operation must actually admit process workers'
        if phase in ('parser', 'native'):
            assert witness['pid'] in pids
        elif phase not in ('publication', 'report'):
            assert all(psutil.pid_exists(pid) for pid in pids)
        if phase in ('parser', 'native', 'compression'):
            assert any(witness.get(name, 0) > 0 for name in ('decoded_pairs', 'ranked_pairs', 'compressed_bytes'))
        if phase == 'merge':
            assert witness['completed_merge_passes'] >= 1
            assert witness['sealed_children'] > 0 and witness['pending_jobs'] > 0
        process.send_signal(signum)
        (control / 'release').touch()
        stdout, stderr = process.communicate(timeout=25)
        assert process.returncode == 128 + signum, stderr[-4000:]
        assert stdout == b''
        assert b'operation cancelled' in stderr
        state = json.loads((control / 'result.json').read_text())
        assert state['status'] == 128 + signum and state['workers'] == pids
        assert state['reaped'] and state['descriptors_restored'] and state['handlers_restored']
        assert state['leases'] == 0
        assert state['errors'] == ([] if phase == 'report' else ['CancelledError'])
        assert state['signals'] == ([] if phase == 'report' else [signum])
        assert all(not psutil.pid_exists(pid) for pid in pids)
        assert all(len(notes) <= 8 for notes in state['notes'])
        for private in (str(tmp_path), 'PRIVATE_INPUT', 'alpha', 'beta', 'opaque', '+private'):
            assert private.encode() not in stdout + stderr
        _preserved(original, destination)
        assert not list(scratch.iterdir())
        if phase == 'report':
            marker = destination / witness['marker']
            assert marker.is_file() and witness['marker'].encode() in stderr
            assert b'durably committed' in stderr
            assert verify(VerifyConfig(manifest=marker, workers=1, max_ram=512 << 20)).completed
        else:
            assert list(destination.iterdir()) == [destination / 'foreign']
            assert b'durably committed' not in stderr
    finally:
        _reap(process, control)


@pytest.mark.parametrize('phase', ['crash-pre-marker', 'crash-marker-visible', 'crash-durable'])
def test_P15_T07_sigkill_residue_distinguishes_visibility_and_durability(tmp_path, phase):
    process, control, destination, scratch, original = _prepare(tmp_path, phase)
    try:
        witness = _ready(process, control)
        assert witness['phase'] == phase
        # Single-CPU public workflows have no worker or helper processes. The
        # test directly supervises and reaps the sole member of its owned group.
        assert psutil.Process(process.pid).children(recursive=True) == []
        assert os.getpgid(process.pid) == process.pid
        before = sorted([*destination.rglob('*'), *scratch.rglob('*')])
        private = [path for path in before if path.name != 'foreign']
        assert private and any(path.is_dir() for path in private), 'private staging must still exist at crash point'
        for path in private:
            facts = path.lstat()
            assert not stat.S_ISLNK(facts.st_mode)
            assert stat.S_IMODE(facts.st_mode) == (0o700 if path.is_dir() else 0o600)
        markers = list(destination.glob('*.manifest.json'))
        if phase == 'crash-pre-marker':
            assert markers == [] and witness['marker_visible'] is False
        else:
            assert len(markers) == 1
        assert witness['transaction_state'] == ('committed' if phase == 'crash-durable' else 'publishing')
        os.killpg(process.pid, signal.SIGKILL)
        stdout, stderr = process.communicate(timeout=5)
        assert process.returncode == -signal.SIGKILL and stdout == stderr == b''
        _preserved(original, destination)
        assert sorted([*destination.rglob('*'), *scratch.rglob('*')]) == before
        assert not (control / 'result.json').exists(), 'uncatchable termination cannot claim cleanup or success'
        if phase == 'crash-durable':
            assert verify(VerifyConfig(manifest=markers[0], workers=1, max_ram=512 << 20)).completed
        # Marker-visible/pre-sync is deliberately only an observed complete set;
        # a process kill cannot simulate or prove power-loss durability.
    finally:
        _reap(process, control)


def test_P15_T07_reached_native_broken_ipc_fails_cli_and_reaps_writers(tmp_path):
    process, control, destination, scratch, original = _prepare(tmp_path, 'broken-ipc')
    try:
        witness = _ready(process, control)
        pids = json.loads((control / 'pids.json').read_text())
        assert witness['phase'] == 'broken-ipc' and witness['ranked_pairs'] > 0
        assert witness['pid'] in pids and all(psutil.pid_exists(pid) for pid in pids)
        (control / 'release').touch()
        stdout, stderr = process.communicate(timeout=25)
        assert process.returncode == 1, stderr[-4000:]
        assert stdout == b'' and b'ResourceError' in stderr
        assert b'operation cancelled' not in stderr and b'durably committed' not in stderr
        state = json.loads((control / 'result.json').read_text())
        assert state['errors'] == ['ResourceError'] and state['signals'] == [None]
        assert state['workers'] == pids and state['reaped'] and state['descriptors_restored']
        assert state['leases'] == 0 and state['handlers_restored']
        assert all(not psutil.pid_exists(pid) for pid in pids)
        assert all(len(notes) <= 8 for notes in state['notes'])
        for private in (str(tmp_path), 'PRIVATE_INPUT', 'alpha', 'beta', 'opaque', '+private'):
            assert private.encode() not in stdout + stderr
        _preserved(original, destination)
        assert list(destination.iterdir()) == [destination / 'foreign']
        assert not list(scratch.iterdir())
    finally:
        _reap(process, control)
