"""P15-T01: production workers meet the independent six-artifact byte oracle."""

import json

import psutil
import pytest

from readmixer_py import mixing, recovery, verification
from readmixer_py.config import MixConfig, RecoverConfig, RecoveryMode, StorageMode, VerifyConfig
from readmixer_py.crypto import OperationKeys
from readmixer_py.execution import CpuObservations, resolve_cpu_budget
from readmixer_py.stage_scheduler import StageScheduler
import test_serial_artifact_oracle as oracle


@pytest.mark.parametrize('profile,mode,cpus', [
    ('tiny', StorageMode.MEMORY, 2), ('tiny', StorageMode.EXTERNAL, 4),
    ('tiny', StorageMode.AUTO, 4), ('rollover', StorageMode.MEMORY, 4),
    ('rollover', StorageMode.EXTERNAL, 2), ('rollover', StorageMode.AUTO, 4),
])
def test_P15_T01_spawned_six_artifacts_match_independent_serial_reference(
        tmp_path, monkeypatch, profile, mode, cpus):
    # Fixed topology, roots, input fields and codec build. The reference itself
    # has pinned primitive, identity/order/tag and six-artifact literal goldens.
    pairs = oracle.fixtures(profile)
    samples, originals = oracle.write_inputs(tmp_path / 'originals', pairs)
    bundles, artifacts, _, _, leaves = oracle.reference(pairs, 6)
    expected = oracle.expected_manifest(samples, bundles, 6)
    for module in (mixing, recovery, verification):
        monkeypatch.setattr(module, 'resolve_cpu_budget', lambda workers: resolve_cpu_budget(
            workers, observations=CpuObservations(logical_cpus=cpus, affinity_cpus=cpus)))
    parser_tasks, pids = [], set()
    submit = StageScheduler.submit_parser

    def observe_source(scheduler, *args, **kwargs):
        assert scheduler.child_pids
        pids.update(scheduler.child_pids)
        task = submit(scheduler, *args, **kwargs)
        parser_tasks.append(task)
        return task

    monkeypatch.setattr(StageScheduler, 'submit_parser', observe_source)
    options = dict(workers=cpus, max_ram=2 << 30, storage=mode,
                   max_record_bytes=oracle.RECORD_LIMIT)
    destination = tmp_path / 'pool'
    report = mixing._mix(MixConfig(input_dir=tmp_path / 'originals', output=destination,
        compression_level=6, max_artifact_bytes=oracle.CAP, **options),
        _keys=OperationKeys._from_test(oracle.ROOT, oracle.RUN))
    assert report.completed and report.workers_effective >= 2
    assert len(parser_tasks) == len(samples) and pids
    assert all(not psutil.pid_exists(pid) for pid in pids)
    document = json.loads((destination / report.manifest).read_bytes())
    execution = document.pop('execution')
    assert document == expected
    assert all(leaf[4] + 128 * len(leaf[1]) + (32 << 20) <= execution['terminal_slice_bytes']
               for leaf in leaves)
    assert {p.name for p in destination.iterdir()} == {*artifacts, report.manifest}
    assert all((destination / name).read_bytes() == data for name, data in artifacts.items())
    checked = verification.verify(VerifyConfig(manifest=destination / report.manifest, **options))
    assert checked.completed and checked.workers_effective >= 2 and checked.pairs == 14
    for mode in RecoveryMode:
        output = tmp_path / mode.value
        restored = recovery.recover(RecoverConfig(manifest=destination / report.manifest,
            output=output, sample=('empty', 'beta', 'alpha'), io=mode, **options))
        assert restored.workers_effective >= 2
        oracle.check_restored(output, restored, pairs, mode)
    assert {name: (tmp_path / 'originals' / name).read_bytes() for name in originals} == originals
