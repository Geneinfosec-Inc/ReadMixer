"""Guarded child module for public CLI signal acceptance (never a runtime API).

The parent sends real OS signals only after the chosen operation acknowledged a
concrete stage. Release files unblock worker/native-thread hooks while the CLI's
ordinary cancellation path owns cleanup. No timing delay stands in for readiness.
"""
import json
import os
from pathlib import Path
import signal
import sys
import time

_DUMPS = json.dumps
_REACHED = False


def _write(name, value):
    target = Path(os.environ['P15_CONTROL']) / name
    temporary = target.with_suffix('.tmp.' + str(os.getpid()))
    temporary.write_text(_DUMPS(value))
    temporary.replace(target)


def checkpoint(witness):
    global _REACHED
    if _REACHED:
        return
    _REACHED = True
    _write('ready.json', {'phase': os.environ['P15_PHASE'], 'pid': os.getpid(), **witness})
    # Finite watchdog also prevents a lost parent from stranding a native thread.
    deadline = time.monotonic() + 35
    release = Path(os.environ['P15_CONTROL']) / 'release'
    while not release.exists():
        if time.monotonic() >= deadline:
            raise AssertionError('signal harness release watchdog expired')
        time.sleep(0.005)


def signal_worker(sock, run_id, max_job_bytes, workspace_bytes, **options):
    from readmixer_py import worker_bootstrap
    from readmixer_py.fastq import PairedReader
    from readmixer_py.stage_policy import SortPolicy
    phase = os.environ['P15_PHASE']
    if phase == 'parser':
        original = PairedReader.next_pair
        def parse(reader, *args, **kwargs):
            pair = original(reader, *args, **kwargs)
            if pair is not None:
                checkpoint({'decoded_pairs': reader.pair_count})
            return pair
        PairedReader.next_pair = parse
    elif phase in ('native', 'broken-ipc'):
        original = worker_bootstrap.execute_policy
        def execute(payload, policy, *, checkpoint):
            result = original(payload, policy, checkpoint=checkpoint)
            if policy.kind is SortPolicy.TERMINAL_RANK:
                assert result[1] > 0
                globals()['checkpoint']({'ranked_pairs': result[1]})
                if phase == 'broken-ipc':
                    import socket
                    sock.shutdown(socket.SHUT_RDWR)
            return result
        worker_bootstrap.execute_policy = execute
    worker_bootstrap.worker_main(sock, run_id, max_job_bytes, workspace_bytes, **options)


def main():
    import psutil
    from multiprocessing import resource_tracker
    from readmixer_py import cli, mixing, recovery, verification, stage_scheduler
    from readmixer_py.execution import CompressionService, CpuObservations, resolve_cpu_budget
    from readmixer_py.resources import Budget
    from readmixer_py.scan import PoolScanner
    from readmixer_py.transaction import OutputTransaction

    phase = os.environ['P15_PHASE']
    crash = phase.startswith('crash-')
    if not crash:
        # Interpreter helper persists across invocation cleanup; account for it
        # before measuring operation-owned descriptors.
        resource_tracker.ensure_running()
    before_fds = psutil.Process().num_fds()
    previous_handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    budgets, schedulers, caught, pids = [], [], [], set()
    original_budget = Budget.__init__
    def budget_init(budget, *args, **kwargs):
        original_budget(budget, *args, **kwargs)
        budgets.append(budget)
    Budget.__init__ = budget_init
    original_launch = stage_scheduler.StageScheduler._launch_partition
    def launch(scheduler):
        answer = original_launch(scheduler)
        schedulers.append(scheduler)
        pids.update(scheduler.child_pids)
        _write('pids.json', sorted(pids))
        return answer
    stage_scheduler.StageScheduler._launch_partition = launch
    stage_scheduler.worker_main = signal_worker
    for module in (mixing, recovery, verification):
        module.resolve_cpu_budget = lambda workers: resolve_cpu_budget(workers,
            observations=CpuObservations(logical_cpus=4, affinity_cpus=4))

    if phase in ('gather-indexed', 'gather-sequential', 'verify-scan'):
        method = 'indexed' if phase == 'gather-indexed' else 'sequential'
        original = getattr(PoolScanner, method)
        def gathered(scanner, *args, **kwargs):
            source = original(scanner, *args, **kwargs)
            try:
                for frame, tag in source:
                    assert len(frame) > 40
                    checkpoint({'gathered_frame_bytes': len(frame), 'method': method})
                    yield frame, tag
            finally:
                source.close()
        setattr(PoolScanner, method, gathered)
    if phase == 'merge':
        from readmixer_py.sorting import ExternalSorter
        original_init = ExternalSorter.__init__
        def small_runs(sorter, *args, **kwargs):
            if kwargs.get('sort_purpose') == 'recover-originals':
                kwargs.update(run_bytes=2048, fan_in=2)
            original_init(sorter, *args, **kwargs)
        ExternalSorter.__init__ = small_runs
        original_drain = ExternalSorter._drain_one
        def merged(sorter):
            scheduler = sorter.scheduler
            owner = scheduler._sort_owners[scheduler._drain_task][0]
            answer = original_drain(sorter)
            if owner.sort_purpose == 'recover-originals' and owner.merge_passes >= 1:
                assert owner._next_runs.root.record_count > 0
                assert scheduler.pending_jobs > 0
                checkpoint({'completed_merge_passes': owner.merge_passes,
                            'sealed_children': owner._next_runs.root.record_count,
                            'pending_jobs': scheduler.pending_jobs})
            return answer
        ExternalSorter._drain_one = merged
    if phase == 'compression':
        original_encode = CompressionService._encode
        def compressed(service, payload, level, ordinal):
            result = original_encode(service, payload, level, ordinal)
            assert service.slots > 0
            checkpoint({'compressed_bytes': len(result), 'native_thread': True})
            return result
        CompressionService._encode = compressed
    if phase in ('publication', 'crash-pre-marker', 'crash-marker-visible'):
        original_link = OutputTransaction._link
        def linked(transaction, artifact):
            answer = original_link(transaction, artifact)
            if artifact._completion == (phase == 'crash-marker-visible'):
                checkpoint({'linked': artifact.name, 'marker_visible': artifact._completion,
                            'transaction_state': transaction._state})
            return answer
        OutputTransaction._link = linked
    if phase == 'crash-durable':
        original_cleanup = OutputTransaction._cleanup
        def committed(transaction, *, committed):
            if committed:
                assert transaction._state == 'committed'
                checkpoint({'marker': transaction.completion_basename,
                            'transaction_state': transaction._state})
            return original_cleanup(transaction, committed=committed)
        OutputTransaction._cleanup = committed
    if phase == 'report':
        original_mix = cli.mix
        def mixed(config):
            report = original_mix(config)
            assert report.completed and report.cleanup_complete
            def render(value, *args, **kwargs):
                checkpoint({'marker': report.manifest, 'completed': True})
                return _DUMPS(value, *args, **kwargs)
            cli.json.dumps = render
            return report
        cli.mix = mixed

    for name in ('mix', 'verify', 'recover'):
        original_operation = getattr(cli, name)
        def observed(config, operation=original_operation):
            try:
                return operation(config)
            except BaseException as error:
                caught.append(error)  # Keep tracebacks live through resource checks.
                raise
        setattr(cli, name, observed)
    code = cli.main(sys.argv[1:])
    assert all(budget.active_leases == budget.used_metadata_bytes == budget.used_descriptors == 0
               and budget.used_bytes == budget.baseline_bytes for budget in budgets)
    assert all(scheduler._closed for scheduler in schedulers)
    assert all(not psutil.pid_exists(pid) for pid in pids)
    after_fds = psutil.Process().num_fds()
    assert after_fds == before_fds, (before_fds, after_fds)
    assert all(signal.getsignal(sig) == old for sig, old in previous_handlers.items())
    _write('result.json', {'status': code, 'workers': sorted(pids), 'reaped': True,
        'descriptors_restored': True, 'leases': 0, 'handlers_restored': True,
        'errors': [type(error).__name__ for error in caught],
        'signals': [getattr(error, 'signal', None) for error in caught],
        'notes': [list(getattr(error, '__notes__', ())) for error in caught]})
    return code


if __name__ == '__main__':
    raise SystemExit(main())
