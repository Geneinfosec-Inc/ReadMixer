"""Private benchmark-only, restored workload controls and reached-checkpoint witnesses.

No environment switch, public seed, production configuration or persistent patch
is introduced. Use only in a single harness thread with no concurrent operations.
Unsupported host controls must be applied and witnessed by the release-host runner.
"""
from __future__ import annotations

from dataclasses import replace
from functools import partial
import hashlib
import errno
import os
from pathlib import Path
import multiprocessing
import threading
import time
import weakref

_ACTIVE = threading.Lock()
SUPPORTED = frozenset(('fixed_test_context', 'terminal_pairs', 'sort_run_bytes', 'sort_fan_in',
    'scatter_strategy', 'require_normalized_over_four_actual_budgets',
    'storage_write_delay_seconds', 'block_first_terminal', 'fail_space_query',
    'defer_scratch_space_refusal', 'scratch_space_probe_path'))


def _gated_worker(target, gate, entered, delay, *args, **kwargs):
    """Importable spawn target; retain inherited coordinator/telemetry wrapper."""
    from readmixer_py import worker_bootstrap, storage
    from readmixer_py.errors import ResourceError
    from readmixer_py.stage_policy import SortPolicy
    original = worker_bootstrap.execute_policy
    blocked = False
    def execute(payload, policy, *, checkpoint):
        nonlocal blocked
        if (not blocked and policy is not None and policy.kind is SortPolicy.TERMINAL_RANK
                and multiprocessing.current_process().name.endswith('-0')):
            blocked = True
            entered.set()
            deadline = time.monotonic() + 20
            while not gate.wait(0.005):
                checkpoint()
                if time.monotonic() >= deadline:
                    raise ResourceError('qualification terminal gate watchdog expired')
        return original(payload, policy, checkpoint=checkpoint)
    worker_bootstrap.execute_policy = execute
    old_write = storage._DiskSink._write
    if delay:
        def write(sink, data):
            time.sleep(delay)
            return old_write(sink, data)
        storage._DiskSink._write = write
    try:
        return target(*args, **kwargs)
    finally:
        worker_bootstrap.execute_policy = original
        storage._DiskSink._write = old_write


class QualificationControls:
    def __init__(self, settings=None):
        self.settings = dict(settings or {})
        unknown = set(self.settings) - SUPPORTED
        if unknown:
            raise ValueError('unsupported private qualification control')
        for name in ('terminal_pairs', 'sort_run_bytes', 'sort_fan_in'):
            if name in self.settings and (type(self.settings[name]) is not int or self.settings[name] <= 0):
                raise ValueError('private numeric qualification control must be positive')
        if not 1 <= self.settings.get('terminal_pairs', 32768) <= 32768:
            raise ValueError('private terminal limit exceeds production maximum')
        if not 2 <= self.settings.get('sort_fan_in', 32) <= 32:
            raise ValueError('private merge fan-in must be 2..32')
        if self.settings.get('fixed_test_context', 'matrix-v1') != 'matrix-v1':
            raise ValueError('unknown artificial fixed benchmark context')
        if self.settings.get('scatter_strategy', 'auto') not in ('auto', 'direct', 'sorter'):
            raise ValueError('invalid private scatter implementation')
        delay = self.settings.get('storage_write_delay_seconds', 0)
        if type(delay) not in (int, float) or not 0 <= delay <= 0.1:
            raise ValueError('private write delay must be 0..0.1 seconds')
        for name in ('block_first_terminal', 'fail_space_query', 'require_normalized_over_four_actual_budgets', 'defer_scratch_space_refusal'):
            if name in self.settings and type(self.settings[name]) is not bool:
                raise ValueError('private boolean control must be a boolean')
        if bool(self.settings.get('defer_scratch_space_refusal')) != ('scratch_space_probe_path' in self.settings):
            raise ValueError('scratch admission bypass requires its explicit dedicated workspace path')
        if self.settings.get('defer_scratch_space_refusal') and self.settings.get('fail_space_query'):
            raise ValueError('scratch admission bypass and failed capacity query are incompatible')
        self.result = dict(controls_applied=False, witnesses=dict(refinements=0, sibling_refinement_groups=0,
            union_shared_selected_members=0, merge_passes=0, delayed_writes=0,
            late_results_retained=0, backpressure_events=0, space_query_failures=0,
            synthetic_admission_capacity_queries=0, os_enospc_observed=0),
            scope='private harness controls; application counts do not substitute for execution witnesses; shared selected members count distinct starting members per reader and are a lower bound for spanning-record overlap')
        self._patches, self._parents = [], {}
        self._selected, self._index_sample = None, None
        self._readers = weakref.WeakKeyDictionary()
        self._entered, self._gate, self._gated = None, None, False
        self._active = False
        self._used = False

    def _patch(self, target, name, replacement):
        original = getattr(target, name)
        self._patches.append((target, name, original))
        setattr(target, name, replacement)

    def __enter__(self):
        if self._used or self._active or not _ACTIVE.acquire(blocking=False):
            raise RuntimeError('qualification controls require one exclusive harness context')
        self._active = self._used = True
        try:
            self._install()
            self.result['controls_applied'] = True
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def _install(self):
        from readmixer_py import shuffle, sorting, storage, disk, scan, stage_scheduler
        from readmixer_py.artifacts.index import IndexReader
        from readmixer_py.artifacts.bgzf import BgzfReader
        from readmixer_py.errors import ResourceError
        from readmixer_py.stage_policy import SortPolicy
        counters, settings = self.result['witnesses'], self.settings
        original_shuffle = shuffle.SerialShuffler.__init__
        def shuffle_init(instance, *args, **kwargs):
            if 'sort_run_bytes' in settings:
                kwargs['sort_run_bytes'] = settings['sort_run_bytes']
            original_shuffle(instance, *args, **kwargs)
            if 'terminal_pairs' in settings:
                instance._limits = replace(instance._limits, pairs=settings['terminal_pairs'])
        self._patch(shuffle.SerialShuffler, '__init__', shuffle_init)
        original_sorter = sorting.ExternalSorter.__init__
        def sort_init(instance, *args, **kwargs):
            if 'sort_run_bytes' in settings:
                kwargs['run_bytes'] = settings['sort_run_bytes']
            if 'sort_fan_in' in settings:
                kwargs['fan_in'] = settings['sort_fan_in']
            original_sorter(instance, *args, **kwargs)
        self._patch(sorting.ExternalSorter, '__init__', sort_init)
        original_refine = shuffle.SerialShuffler._refine
        def refine(instance, shard):
            result = original_refine(instance, shard)
            counters['refinements'] += 1
            parent = shard.path[:-1]
            if parent not in self._parents:
                if len(self._parents) >= 4096:
                    raise ResourceError('qualification refinement witness capacity exhausted')
                self._parents[parent] = 0
            self._parents[parent] += 1
            if self._parents[parent] == 2:
                counters['sibling_refinement_groups'] += 1
            return result
        self._patch(shuffle.SerialShuffler, '_refine', refine)
        original_merge = sorting.ExternalSorter._merge_pass
        def merge(instance):
            result = original_merge(instance)
            counters['merge_passes'] += 1
            return result
        self._patch(sorting.ExternalSorter, '_merge_pass', merge)
        original_indexed = scan.PoolScanner.indexed
        def indexed(instance, selected):
            previous = self._selected
            self._selected = frozenset(selected)
            try:
                yield from original_indexed(instance, selected)
            finally:
                self._selected = previous
                self._index_sample = None
                self._readers.clear()
        self._patch(scan.PoolScanner, 'indexed', indexed)
        original_next = IndexReader.__next__
        def next_entry(instance):
            entry = original_next(instance)
            if self._selected is not None:
                self._index_sample = entry.sample_index
            return entry
        self._patch(IndexReader, '__next__', next_entry)
        original_record = BgzfReader.read_record_at
        def record(instance, offset, limit):
            data = original_record(instance, offset, limit)
            if self._selected is not None and self._index_sample in self._selected:
                member, sample = offset >> 16, self._index_sample
                previous = self._readers.get(instance)
                if previous is not None and previous[0] == member:
                    shared = previous[2]
                    if previous[1] != sample and not shared:
                        counters['union_shared_selected_members'] += 1
                        shared = True
                    self._readers[instance] = (member, previous[1], shared)
                else:
                    self._readers[instance] = (member, sample, False)
            return data
        self._patch(BgzfReader, 'read_record_at', record)
        delay = settings.get('storage_write_delay_seconds', 0)
        if delay:
            original_write = storage._DiskSink._write
            def write(instance, data):
                time.sleep(delay)
                counters['delayed_writes'] += 1
                return original_write(instance, data)
            self._patch(storage._DiskSink, '_write', write)
        if settings.get('fail_space_query'):
            def fail_query(target):
                counters['space_query_failures'] += 1
                raise OSError('qualification filesystem-capacity query unavailable')
            self._patch(disk.os, 'fstatvfs', fail_query)
            self._patch(disk.os, 'statvfs', fail_query)
        if settings.get('defer_scratch_space_refusal'):
            root = Path(settings['scratch_space_probe_path']).resolve(strict=True)
            snapshot = root.stat()
            if not root.is_dir():
                raise ValueError('scratch probe root must be a directory')
            identity = snapshot.st_dev, snapshot.st_ino
            self.result['admission_bypass_scope'] = ('dedicated scratch directories only: observed free capacity is replaced with 4 GiB to reach real OS ENOSPC; this is injected admission bypass, not ordinary admission behavior')
            def owned_target(target):
                current = root.stat()
                if (current.st_dev, current.st_ino) != identity:
                    raise ResourceError('qualification scratch probe root changed identity')
                try:
                    path = Path(os.readlink(f'/proc/self/fd/{target}')) if type(target) is int else Path(target)
                    return path.resolve(strict=True).is_relative_to(root)
                except (OSError, TypeError, ValueError):
                    return False
            original_facts = disk.DiskBudget._facts
            def facts(instance, target):
                device, free = original_facts(instance, target)
                if owned_target(target):
                    counters['synthetic_admission_capacity_queries'] += 1
                    return device, max(free, 4 << 30)
                return device, free
            self._patch(disk.DiskBudget, '_facts', facts)
            original_os_write = storage.os.write
            def observed_write(fd, data):
                try:
                    return original_os_write(fd, data)
                except OSError as exc:
                    if exc.errno == errno.ENOSPC and owned_target(fd):
                        counters['os_enospc_observed'] += 1
                    raise
            self._patch(storage.os, 'write', observed_write)
        if settings.get('block_first_terminal'):
            context = multiprocessing.get_context('spawn')
            self._gate, self._entered = context.Event(), context.Event()
            self._patch(stage_scheduler, 'worker_main', partial(_gated_worker,
                stage_scheduler.worker_main, self._gate, self._entered, delay))
            original_take = stage_scheduler.StageScheduler.take_next
            def take(instance):
                job = instance._jobs.get(instance._drain_task)
                if (not self._gated and instance.terminal_bytes > 0 and job is not None
                        and job.policy is not None and job.policy.kind is SortPolicy.TERMINAL_RANK):
                    self._gated = True
                    if instance.process_slots < 2:
                        self._gate.set()
                        return original_take(instance)
                    deadline = time.monotonic() + 15
                    try:
                        while True:
                            instance._service()
                            others = [other for other in instance._jobs.values() if other.slot // 2 != job.slot // 2]
                            expected = 2 * (instance.process_slots - 1)
                            late = sum(other.complete and other.result_ack for other in others)
                            if self._entered.is_set() and len(others) == expected and late == expected:
                                if job.complete or len(instance._jobs) != instance.capacity or any(lease.released for lease in instance._slots):
                                    raise ResourceError('qualification maximum retained window lost its charged earliest owner')
                                counters['late_result_window_expected'] = expected
                                counters['late_result_window_observed'] = late
                                counters['earliest_terminal_incomplete'] = True
                                counters['retained_job_workspace_bytes'] = sum(instance._slots[other.slot].bytes for other in others)
                                counters['late_results_retained'] = max(counters['late_results_retained'], late)
                                counters['backpressure_events'] += 1
                                break
                            if time.monotonic() >= deadline:
                                raise ResourceError('qualification delayed-result checkpoint was not reached')
                    finally:
                        self._gate.set()
                return original_take(instance)
            self._patch(stage_scheduler.StageScheduler, 'take_next', take)

    def mix(self, config):
        if not self._active:
            raise RuntimeError('qualification mix requires an active control context')
        from readmixer_py.crypto import OperationKeys
        from readmixer_py.mixing import _mix
        keys = None
        if 'fixed_test_context' in self.settings:
            keys = OperationKeys._from_test(hashlib.sha256(b'readmixer-qualification/matrix-v1/root').digest(),
                hashlib.sha256(b'readmixer-qualification/matrix-v1/run').digest()[:16])
        report = _mix(config, _keys=keys, _scatter_strategy=self.settings.get('scatter_strategy', 'auto'))
        self.result['witnesses']['bundles'] = report.bundles
        return report

    def __exit__(self, *_):
        if self._gate is not None:
            self._gate.set()
        for target, name, original in reversed(self._patches):
            setattr(target, name, original)
        self._patches.clear()
        self._parents.clear()
        self._readers.clear()
        if self._active:
            self._active = False
            _ACTIVE.release()
