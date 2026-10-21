"""Fixed-context artifact equivalence with real shared compression threads."""

import hashlib
import json

from readmixer_py.config import MixConfig, StorageMode
from readmixer_py.crypto import OperationKeys
from readmixer_py.execution import CpuObservations, resolve_cpu_budget
from readmixer_py.mixing import _mix
from readmixer_py.verification import verify
from readmixer_py.config import VerifyConfig
from test_pipeline import fixtures


def test_P09_six_artifacts_equal_with_admitted_one_two_four_cpus(tmp_path, monkeypatch):
    import readmixer_py.mixing as module
    monkeypatch.setattr(module, 'resolve_cpu_budget', lambda workers: resolve_cpu_budget(workers,
                        observations=CpuObservations(logical_cpus=4, affinity_cpus=4)))
    inputs = tmp_path / 'inputs'
    fixtures(inputs, counts=(120, 70, 0), metadata=40, sequence=300)
    results = []
    slots = []
    for workers in (1, 2, 4):
        target = tmp_path / f'workers{workers}'
        keys = OperationKeys._from_test(hashlib.sha256(b'readmixer-python/shuffle-test/v1/0').digest(), bytes(16))
        report = _mix(MixConfig(input_dir=inputs, output=target, max_ram=512 << 20,
                               max_artifact_bytes=65564, workers=workers, storage=StorageMode.EXTERNAL), _keys=keys)
        document = json.loads((target / report.manifest).read_bytes())
        profile = document.pop('execution')
        slots.append(profile['compression_slots'])
        assert profile['workers_effective'] == report.workers_effective == 1
        assert profile['cpu_budget'] == report.cpu_budget <= workers
        artifacts = {(bundle['bundle_index'], role): (target / bundle[role]['name']).read_bytes()
                     for bundle in document['bundles'] for role in ('r1', 'r1_gzi', 'r2', 'r2_gzi', 'key', 'index')}
        results.append((document, artifacts))
        assert verify(VerifyConfig(manifest=target / report.manifest, max_ram=512 << 20)).completed
    assert slots == [0, 1, 3]
    assert results[0] == results[1] == results[2]


# The representative matrix varies every production strategy dimension at fixed
# B/T/codec/entropy. It is deliberately bounded; scale belongs to Plan 16.
_MATRIX = (
    (1, 'memory', 'sorter', 'frame', 1),
    (2, 'memory', 'direct', 1 << 20, 4096),
    (4, 'memory', 'direct', 64 << 20, 1),
    (1, 'external', 'direct', 1 << 20, 1),
    (2, 'external', 'sorter', 64 << 20, 4096),
    (4, 'external', 'direct', 'frame', 4096),
    (1, 'auto', 'direct', 64 << 20, 4096),
    (2, 'auto', 'direct', 'frame', 1),
    (4, 'auto', 'sorter', 1 << 20, 4096),
)


def _fixed_observed_cpus(monkeypatch):
    from importlib import import_module
    def observed(workers):
        return resolve_cpu_budget(workers, observations=CpuObservations(logical_cpus=4, affinity_cpus=4))
    for name in ('mixing', 'verification', 'recovery'):
        monkeypatch.setattr(import_module('readmixer_py.' + name), 'resolve_cpu_budget', observed)


def _artifact_bytes(manifest):
    document = json.loads(manifest.read_bytes())
    execution = document.pop('execution')
    artifacts = {(bundle['bundle_index'], role): (manifest.parent / bundle[role]['name']).read_bytes()
                 for bundle in document['bundles']
                 for role in ('r1', 'r1_gzi', 'r2', 'r2_gzi', 'key', 'index')}
    return document, artifacts, execution



def _expected_generations(expected, sample, bound):
    # Independent framing accounting: eight LF bytes in normalized mates are
    # replaced by the PYPAIR1 forty-eight-byte fixed prefix.
    mates = [expected[sample, role].split(b'\n')[:-1] for role in (1, 2)]
    sizes = [sum(len(line) for lines in mates for line in lines[index:index + 4]) + 48
             for index in range(0, len(mates[0]), 4)]
    output = []
    size = count = 0
    for frame_bytes in sizes:
        if size + frame_bytes > bound:
            output.append((size, count))
            size = count = 0
        size += frame_bytes
        count += 1
    if count:
        output.append((size, count))
    return output

def test_P14_T03_T06_six_artifacts_equal_across_real_pipeline_strategies(tmp_path, monkeypatch):
    from readmixer_py.paircodec import maximum_frame_bytes
    from readmixer_py.storage import SegmentStore
    _fixed_observed_cpus(monkeypatch)
    inputs = tmp_path / 'inputs'
    expected = fixtures(inputs, counts=(17, 11, 0), metadata=40, sequence=300)
    before = {path.name: path.read_bytes() for path in inputs.iterdir()}
    normalized = sum(map(len, expected.values()))
    reference = None
    observed_auto_spills = []
    generations = []
    from readmixer_py.parser_stage import SourceReceiver
    original_generation = SourceReceiver.finish_scatter
    def generation(receiver):
        cap, header = receiver.capability, receiver.header
        generations.append((index, cap.sample, cap.batch_bytes, header.output_bytes, header.records))
        return original_generation(receiver)
    monkeypatch.setattr(SourceReceiver, 'finish_scatter', generation)
    original_spill = SegmentStore.spill
    def spill(store, ref):
        result = original_spill(store, ref)
        if store.mode == 'auto' and not ref.physical_bytes and result.physical_bytes:
            observed_auto_spills.append((ref.purpose, ref.decoded_bytes))
        return result
    monkeypatch.setattr(SegmentStore, 'spill', spill)
    for index, (workers, mode, strategy, batch, tags) in enumerate(_MATRIX):
        target = tmp_path / f'case{index}'
        temporary = tmp_path / f'temporary{index}'
        temporary.mkdir(mode=0o700)
        keys = OperationKeys._from_test(hashlib.sha256(b'readmixer-python/shuffle-test/v1/0').digest(), bytes(16))
        report = _mix(MixConfig(input_dir=inputs, output=target, max_ram=2 << 30,
                    max_record_bytes=4096, max_artifact_bytes=65564, workers=workers,
                    max_total_uncompressed=normalized, storage=StorageMode(mode), temp_dir=temporary),
                    _keys=keys, _scatter_strategy=strategy,
                    _scatter_batch_bytes=maximum_frame_bytes(4096) if batch == 'frame' else batch,
                    _tag_batch_size=tags)
        document, artifacts, execution = _artifact_bytes(target / report.manifest)
        assert report.completed and report.cleanup_complete and not report.warnings
        assert report.normalized_bytes == normalized and report.pairs == 28
        assert report.bundles >= 2  # exercise synchronized rollover, including key offsets
        assert execution['memory_budget_bytes'] == 2 << 30
        assert execution['terminal_slice_bytes'] == 128 << 20
        expected_processes = 0 if workers == 1 else max(1, workers - 2)
        assert report.workers_effective == execution['workers_effective']
        assert report.workers_effective == 1 if workers == 1 else 1 < report.workers_effective <= 1 + expected_processes
        assert report.cpu_budget == execution['cpu_budget'] <= workers
        assert report.compression_slots == execution['compression_slots']
        assert report.workers_effective + report.compression_slots == report.cpu_budget
        current = document, artifacts
        if reference is None:
            reference = current
        else:
            assert current == reference, (workers, mode, strategy, batch, tags)
        if workers > 1 and strategy == 'direct':
            selected_batch = maximum_frame_bytes(4096) if batch == 'frame' else batch
            for sample_index, sample in enumerate(('alpha', 'beta', 'empty')):
                actual_batches = [(amount, count) for case, source, bound, amount, count in generations
                                  if case == index and source == sample_index]
                assert all(bound == selected_batch for case, source, bound, amount, count in generations
                           if case == index and source == sample_index)
                assert actual_batches == _expected_generations(expected, sample, selected_batch)
        assert not list(temporary.iterdir())
    assert observed_auto_spills and any(purpose == 1 for purpose, _ in observed_auto_spills)
    assert {path.name: path.read_bytes() for path in inputs.iterdir()} == before


def test_P14_T07_spawned_complete_verification_and_both_union_recoveries(tmp_path, monkeypatch):
    from readmixer_py.config import RecoverConfig, RecoveryMode
    from readmixer_py.recovery import recover
    from readmixer_py.pool import Pool
    from test_recovery import restored_files
    _fixed_observed_cpus(monkeypatch)
    inputs = tmp_path / 'inputs'
    expected = fixtures(inputs, counts=(17, 11, 0), metadata=40, sequence=300)
    expected_nested = {sample: {f'r{role}': expected[sample, role] for role in (1, 2)}
                       for sample in ('alpha', 'beta', 'empty')}
    for mode in StorageMode:
        pool = tmp_path / ('pool-' + mode.value)
        report = _mix(MixConfig(input_dir=inputs, output=pool, max_ram=2 << 30,
                      workers=4, storage=mode, max_record_bytes=4096, max_artifact_bytes=65564))
        assert 1 < report.workers_effective <= 3
        manifest = pool / report.manifest
        document = json.loads(manifest.read_bytes())
        verification = verify(VerifyConfig(manifest=manifest, max_ram=2 << 30, workers=4, storage=mode))
        assert verification.completed and verification.workers_effective > 1
        assert verification.validation_scope == 'pool_complete'
        assert verification.normalized_bytes == sum(map(len, expected.values()))
        for recovery_mode in RecoveryMode:
            opened = []
            original = Pool.open
            def open_artifact(self, descriptor, **kwargs):
                opened.append(descriptor['name'])
                return original(self, descriptor, **kwargs)
            target = tmp_path / (mode.value + '-' + recovery_mode.value)
            with monkeypatch.context() as scope:
                scope.setattr(Pool, 'open', open_artifact)
                restored = recover(RecoverConfig(manifest=manifest, output=target,
                    sample=('empty', 'beta', 'alpha'), io=recovery_mode, storage=mode,
                    max_ram=2 << 30, workers=4))
            assert restored.completed and restored.workers_effective > 1
            assert restored.cleanup_complete and not restored.warnings
            assert restored_files(target, restored)[1] == expected_nested
            assert restored.normalized_bytes == verification.normalized_bytes
            for bundle in document['bundles']:
                for role in ('r1', 'r2', 'key', 'r1_gzi', 'r2_gzi'):
                    assert opened.count(bundle[role]['name']) == 1
                assert opened.count(bundle['index']['name']) == int(recovery_mode is RecoveryMode.INDEXED)


def _observed_input_worker(sock, run_id, max_job_bytes, workspace_bytes, *, counters, names, **kwargs):
    """Install instrumentation in the real child, before production bootstrap."""
    from contextlib import contextmanager
    import os
    from readmixer_py.catalog import SampleCatalog
    from readmixer_py import worker_bootstrap
    original = SampleCatalog.open_input
    class Reader:
        def __init__(self, raw, index):
            self.raw, self.index = raw, index
        def read(self, size):
            assert isinstance(size, int) and size >= 0
            with counters.get_lock():
                expected = counters[len(names) + self.index]
            assert self.raw.tell() == expected, 'child reread or sought within the source'
            data = self.raw.read(size)
            with counters.get_lock():
                counters[len(names) + self.index] += len(data)
            assert self.raw.tell() == expected + len(data)
            return data
        def readinto(self, target):
            data = self.read(len(target))
            target[:len(data)] = data
            return len(data)
        def seek(self, *args):
            raise AssertionError('child source decoding must not seek')
        def __getattr__(self, name):
            return getattr(self.raw, name)
    @contextmanager
    def open_input(catalog, source):
        index = names.index(source.relative_path.name)
        with original(catalog, source) as raw:
            with counters.get_lock():
                counters[index] += 1
                counters[2 * len(names) + index] = os.getpid()
            yield Reader(raw, index)
    SampleCatalog.open_input = open_input
    worker_bootstrap.worker_main(sock, run_id, max_job_bytes, workspace_bytes, **kwargs)


def test_P14_T01_real_children_open_each_mate_once_and_report_exact_credits(tmp_path, monkeypatch):
    from functools import partial
    import multiprocessing
    import os
    from readmixer_py import stage_scheduler
    from readmixer_py.parser_stage import SourceReceiver
    _fixed_observed_cpus(monkeypatch)
    inputs = tmp_path / 'inputs'
    expected = fixtures(inputs, counts=(31, 13, 0), metadata=4, sequence=31)
    paths = sorted(inputs.iterdir())
    names = tuple(path.name for path in paths)
    physical = [path.stat().st_size for path in paths]
    counters = multiprocessing.get_context('spawn').Array('q', 3 * len(names))
    monkeypatch.setattr(stage_scheduler, 'worker_main', partial(_observed_input_worker, counters=counters, names=names))
    reports = []
    original = SourceReceiver.finish
    def finish(receiver, encoded):
        report = original(receiver, encoded)
        reports.append(report)
        return report
    monkeypatch.setattr(SourceReceiver, 'finish', finish)
    normalized = sum(map(len, expected.values()))
    report = _mix(MixConfig(input_dir=inputs, output=tmp_path / 'pool', max_ram=2 << 30,
                  workers=4, storage=StorageMode.EXTERNAL, max_record_bytes=4096,
                  max_total_uncompressed=normalized), _scatter_strategy='direct', _scatter_batch_bytes=8232)
    assert report.completed and report.workers_effective == 3
    assert report.normalized_bytes == normalized
    assert list(counters[:len(names)]) == [1] * len(names)
    assert list(counters[len(names):2 * len(names)]) == physical
    assert all(pid > 0 and pid != os.getpid() for pid in counters[2 * len(names):])
    assert sorted(value.sample_index for value in reports) == [0, 1, 2]
    assert all(value.complete for value in reports)
    assert sum(value.r1_normalized_bytes + value.r2_normalized_bytes for value in reports) == normalized


def test_P14_T08_public_cli_spawn_guard_and_round_trip(tmp_path):
    import os
    from pathlib import Path
    import subprocess
    import sys
    import pytest
    import readmixer_py
    from test_recovery import restored_files
    if resolve_cpu_budget(2).effective_limit < 2:
        pytest.skip('public CLI spawn lane requires at least two observed CPU slots')
    inputs = tmp_path / 'private-inputs'
    expected = fixtures(inputs, counts=(3, 2, 0), metadata=2, sequence=5)
    env = os.environ.copy()
    env['PYTHONPATH'] = str(Path(readmixer_py.__file__).resolve().parent.parent)
    def command(*args):
        result = subprocess.run([sys.executable, '-m', 'readmixer_py', *map(str, args),
                                 '--max-ram', '2GiB', '--workers', '2', '--json'],
                                cwd=tmp_path, env=env, capture_output=True, text=True, timeout=60)
        assert result.returncode == 0, result.stderr
        assert not result.stderr
        assert 'private-inputs' not in result.stdout and str(tmp_path) not in result.stdout
        return json.loads(result.stdout)
    pool = tmp_path / 'pool'
    mixed = command('mix', '--input', inputs, '--output', pool, '--storage', 'external',
                    '--max-record-bytes', '4096B')
    assert mixed['completed'] and mixed['workers_effective'] == 2
    manifest = pool / mixed['manifest']
    verified = command('verify', '--manifest', manifest, '--storage', 'external')
    assert verified['completed'] and verified['workers_effective'] == 2
    target = tmp_path / 'restored'
    recovered = command('recover', '--manifest', manifest, '--output', target,
                        '--sample', 'alpha', '--sample', 'beta', '--sample', 'empty',
                        '--io', 'indexed', '--storage', 'external')
    assert recovered['completed'] and recovered['workers_effective'] == 2
    from types import SimpleNamespace
    _, actual = restored_files(target, SimpleNamespace(receipt=recovered['receipt']))
    assert actual == {sample: {f'r{role}': expected[sample, role] for role in (1, 2)}
                      for sample in ('alpha', 'beta', 'empty')}


def test_P14_T03_late_terminal_and_changed_producer_ownership_preserve_six_files(tmp_path, monkeypatch):
    from functools import partial
    import multiprocessing
    import time
    from readmixer_py import parser_stage, stage_scheduler
    from readmixer_py.stage_policy import SortPolicy
    from test_parallel_terminals import _gated_terminal_worker
    _fixed_observed_cpus(monkeypatch)
    inputs = tmp_path / 'inputs'
    fixtures(inputs, counts=(17, 11, 0), metadata=40, sequence=300)
    root = hashlib.sha256(b'readmixer-python/shuffle-test/v1/0').digest()
    def mixed(name):
        output = tmp_path / name
        report = _mix(MixConfig(input_dir=inputs, output=output, max_ram=2 << 30,
                       workers=4, storage=StorageMode.EXTERNAL, max_record_bytes=4096,
                       max_artifact_bytes=65564), _keys=OperationKeys._from_test(root, bytes(16)),
                       _scatter_strategy='direct', _scatter_batch_bytes=1 << 20)
        assert report.workers_effective == 3
        return _artifact_bytes(output / report.manifest)[:2]
    expected = mixed('normal')
    gate = multiprocessing.get_context('spawn').Event()
    monkeypatch.setattr(stage_scheduler, 'worker_main', partial(_gated_terminal_worker, gate=gate))
    original_assign = parser_stage.lpt_assignments
    assignments = []
    def assign(catalog, producers):
        normal = original_assign(catalog, producers)
        changed = tuple(reversed(normal))
        assert producers == 2 and changed != normal
        assignments.append(changed)
        return changed
    monkeypatch.setattr(parser_stage, 'lpt_assignments', assign)
    original_take = stage_scheduler.StageScheduler.take_next
    delayed = []
    def take(scheduler):
        jobs = list(scheduler._jobs.values())
        if (not delayed and jobs and jobs[0].policy is not None
                and jobs[0].policy.kind is SortPolicy.TERMINAL_RANK):
            assert len(jobs) == scheduler.capacity == 4
            deadline = time.monotonic() + 15
            while not all(job.result_ack for job in jobs[2:]):
                scheduler._service()
                assert time.monotonic() < deadline
            assert not jobs[0].complete
            delayed.append(tuple(job.task for job in jobs))
            gate.set()
        return original_take(scheduler)
    monkeypatch.setattr(stage_scheduler.StageScheduler, 'take_next', take)
    try:
        assert mixed('delayed-other-producers') == expected
        assert len(delayed) == len(assignments) == 1
    finally:
        gate.set()
