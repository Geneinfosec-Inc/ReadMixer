"""Private, bounded instrumentation for candidate qualification, never runtime API.

All names in reports are a closed vocabulary. No arguments, paths, record bytes,
source identities, exception text, root keys or task capabilities are retained.
The observer changes timing and child import/RSS baselines; report that overhead.
"""
from __future__ import annotations

from contextlib import contextmanager
import functools
import inspect
import json
import math
import os
from pathlib import Path
import stat
import sys
import tempfile
import threading
import time

import psutil

_CLASSES = ('original', 'input_admission', 'catalog', 'encrypted_scratch', 'r1', 'r2', 'key', 'index', 'gzi',
            'manifest', 'receipt', 'other_output')
_IO_FIELDS = ('opens', 'read_calls', 'read_bytes', 'write_calls', 'write_bytes', 'seek_calls', 'errors')
_STAGES = ('parser', 'frame_encode', 'frame_decode', 'bucket_hash', 'ranks', 'lexsort',
           'tag_render', 'scratch_aead_encode', 'scratch_aead_decode', 'scratch_codec',
           'bgzf_encode', 'bgzf_decode', 'indexed_gather', 'sequential_gather',
           'sort_run', 'sort_merge', 'refinement', 'publication', 'worker_policy',
           'scheduler_drain', 'transport_pump', 'compression_drain', 'process_startup')
_COUNTERS = ('sort_runs', 'merge_passes', 'merge_groups', 'refinements', 'spills',
             'spill_decoded_bytes', 'scatter_generations', 'worker_policy_jobs',
             'scheduler_waits', 'transport_no_progress', 'compression_waits',
             'compression_full_window', 'source_pairs')
_PEAKS = ('scheduler_pending_jobs', 'scheduler_pending_bytes', 'scheduler_completed_retained_jobs',
          'scheduler_completed_retained_bytes', 'compression_pending_jobs',
          'admitted_ram_bytes', 'admitted_metadata_bytes', 'admitted_descriptors')
_NATIVE_STAGES = frozenset(('ranks', 'lexsort', 'tag_render', 'scratch_aead_encode',
                            'scratch_aead_decode', 'scratch_codec', 'bgzf_encode', 'bgzf_decode'))
_CONTEXT_LOCK = threading.Lock()


def artifact_class(name):
    """Classify privately; callers must never serialize the supplied name."""
    name = str(name).lower()
    if name.endswith('.gzi'):
        return 'gzi'
    if name.endswith('.key.csv.gz'):
        return 'key'
    if name.endswith('.idx') or name.endswith('.index') or name.endswith('.rmpyidx'):
        return 'index'
    if 'receipt' in name and name.endswith('.json'):
        return 'receipt'
    if name.endswith('.json'):
        return 'manifest'
    if ('_r1.' in name or '.r1.' in name or '-r1.' in name) and name.endswith(('.gz', '.fastq')):
        return 'r1'
    if ('_r2.' in name or '.r2.' in name or '-r2.' in name) and name.endswith(('.gz', '.fastq')):
        return 'r2'
    return 'other_output'


def _process_snapshot():
    """Observed process only, not additive to an independent tree sampler."""
    result = dict(rss_bytes=None, threads=None, descriptors=None, cpu_seconds=None,
                  lifetime_peak_rss_bytes=None, error=False)
    try:
        process = psutil.Process()
        with process.oneshot():
            result.update(rss_bytes=process.memory_info().rss, threads=process.num_threads(),
                          descriptors=process.num_fds() if hasattr(process, 'num_fds') else None)
            cpu = process.cpu_times()
            result['cpu_seconds'] = cpu.user + cpu.system
        try:
            import resource
            rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            result['lifetime_peak_rss_bytes'] = int(rss if sys.platform == 'darwin' else rss * 1024)
        except (ImportError, AttributeError, OSError):
            pass
    except (psutil.Error, OSError):
        result['error'] = True
    return result


class _ReadProxy:
    """Preserve the underlying stream and ownership while counting actual reads."""
    def __init__(self, stream, observer, category):
        self._stream, self._observer, self._category = stream, observer, category
        observer._io(category, 'opens', 1)

    def __getattr__(self, name):
        return getattr(self._stream, name)

    def read(self, *args, **kwargs):
        try:
            value = self._stream.read(*args, **kwargs)
        except BaseException:
            self._observer._io(self._category, 'errors', 1)
            raise
        self._observer._io_read(self._category, len(value or b''))
        return value

    def readline(self, *args, **kwargs):
        try:
            value = self._stream.readline(*args, **kwargs)
        except BaseException:
            self._observer._io(self._category, 'errors', 1)
            raise
        self._observer._io_read(self._category, len(value or b''))
        return value

    def readinto(self, buffer):
        try:
            count = self._stream.readinto(buffer)
        except BaseException:
            self._observer._io(self._category, 'errors', 1)
            raise
        self._observer._io_read(self._category, count or 0)
        return count

    def seek(self, *args, **kwargs):
        self._observer._io(self._category, 'seek_calls', 1)
        return self._stream.seek(*args, **kwargs)

    def __enter__(self):
        self._stream.__enter__()
        return self

    def __exit__(self, *args):
        return self._stream.__exit__(*args)


def _empty_result(interval, detail):
    return dict(telemetry_version=1, complete=True, errors=0,
        scope=dict(detail=detail, timing='inclusive integrated call time; nested/concurrent stages overlap and must not be summed',
            io='actual process reads/writes at intercepted file streams/syscalls; includes validation rereads; discovery/stat-only opens excluded',
            original='actual DecodedStream raw handles only; coordinator and reported spawned workers',
            native='first eight calls per native stage; process snapshots, not allocation or library-thread attribution',
            observer='harness patches, locks, clocks, optional sampler thread, and child imports add uncharged measurement overhead',
            storage='sampled path apparent-byte sums; publication hard links may count twice; not allocated device usage',
            sampling='short-lived peaks can be missed; RSS includes allocator retention and shared pages',
            worker_cpu='per-worker lifetime CPU snapshots; do not add to RUSAGE_CHILDREN accounting',
            waits='wait entry counts and no-progress pumps; idle polls are included, not pure storage stalls',
            admission_io='input_admission counts bounded codec probes separately from forward decoded source passes',
            retained_results='completed result bytes are logical bytes in RAM or authenticated storage, not resident RSS',
            admission_peaks='maximum charged counters of an observed Budget, not a sum of overlapping parent/child sub-budgets'),
        io={name: {field: 0 for field in _IO_FIELDS} for name in _CLASSES},
        stages={name: dict(calls=0, failures=0, wall_seconds=0.0, max_call_seconds=0.0) for name in _STAGES},
        counters={name: 0 for name in _COUNTERS}, peaks={name: 0 for name in _PEAKS},
        native={name: dict(observations=0, max_before_rss_bytes=0, max_after_rss_bytes=0,
                           max_positive_rss_delta_bytes=0, max_threads=0) for name in sorted(_NATIVE_STAGES)},
        worker_admission=dict(ready_observations=0, max_ready_rss_bytes=0, max_ready_descriptors=0,
            max_ready_threads=0, max_reserved_baseline_bytes=0, max_reserved_job_bytes=0,
            max_reserved_channel_bytes=0, max_process_slots=0, max_job_capacity=0),
        snapshots={}, workers=dict(expected=0, received=0, missing=0, complete=True,
                                  lifetime_cpu_seconds=0.0, peak_individual_rss_bytes=0),
        storage=dict(interval_seconds=interval, observations=0, observer_seconds=0.0,
                     peak_coexisting_apparent_bytes=0, peak_scratch_apparent_bytes=0,
                     peak_output_apparent_bytes=0, scratch_when_coexistence_peaked=0,
                     output_when_coexistence_peaked=0,
                     peak_by_class={name: 0 for name in _CLASSES if name != 'original'}))


def _instrumented_worker(report_directory, detail, target, *args, **kwargs):
    """Pickleable private spawn proxy; only bounded aggregates leave this child."""
    observer = ResourceTelemetry(None, detail=detail, _worker=True)
    try:
        with observer:
            return target(*args, **kwargs)
    finally:
        # A hard kill cannot execute finally. The parent records a missing
        # observation instead of inferring a clean worker or a measured peak.
        try:
            encoded = json.dumps(observer.result, sort_keys=True, separators=(',', ':')).encode()
            if len(encoded) <= 128 << 10:
                path = Path(report_directory) / f'worker-{os.getpid()}.json'
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                with os.fdopen(fd, 'wb') as sink:
                    sink.write(encoded)
        except OSError:
            pass


class ResourceTelemetry:
    """Scoped coordinator/worker hooks with constant-size aggregate reports.

    ``scratch`` and ``output`` must be disjoint observer-owned workload roots.
    Worker reports live in a separate private temporary directory and are
    consumed/removed on exit, after the operation has reaped its workers.
    ``detail=False`` retains I/O/witnesses but skips per-record codec timers.
    Overlapping observers within a process fail explicitly before patching.
    """
    def __init__(self, scratch, output=None, *, interval=0.02, detail=True, _worker=False):
        if not isinstance(interval, (int, float)) or isinstance(interval, bool) or not 0 < interval <= 60:
            raise ValueError('telemetry interval must be positive and at most sixty seconds')
        self.scratch = None if scratch is None else Path(scratch).absolute()
        self.output = None if output is None else Path(output).absolute()
        if self.scratch is not None and self.output is not None and (
                self.scratch.is_relative_to(self.output) or self.output.is_relative_to(self.scratch)):
            raise ValueError('telemetry storage roots must be disjoint')
        self.interval, self.detail, self._worker = interval, bool(detail), _worker
        self.result = _empty_result(interval, self.detail)
        self._patches = []
        self._function_aliases = []
        self._lock = threading.RLock()
        self._local = threading.local()
        self._stop = threading.Event()
        self._thread = self._reports = None
        self._entered = False

    def _io(self, category, field, value):
        with self._lock:
            self.result['io'][category][field] += value

    def _io_read(self, category, count):
        with self._lock:
            values = self.result['io'][category]
            values['read_calls'] += 1
            values['read_bytes'] += count

    def _count(self, name, value=1):
        with self._lock:
            self.result['counters'][name] += value

    def _peak(self, name, value):
        with self._lock:
            self.result['peaks'][name] = max(self.result['peaks'][name], value)

    def _error(self):
        with self._lock:
            self.result['errors'] += 1
            self.result['complete'] = False

    def _patch(self, owner, name, value):
        original = inspect.getattr_static(owner, name)
        self._patches.append((owner, name, original))
        setattr(owner, name, value)

    def _function(self, module, name, stage, **kwargs):
        original = getattr(module, name)
        wrapped = self._timer(original, stage, **kwargs)
        self._function_aliases.append((wrapped, original))
        # Production imports several codecs by value. Patch their existing
        # module aliases too, without inspecting any runtime object graphs.
        for candidate in tuple(sys.modules.values()):
            if candidate is not None and getattr(candidate, '__name__', '').startswith('readmixer_py'):
                for alias, value in tuple(vars(candidate).items()):
                    if value is original:
                        self._patch(candidate, alias, wrapped)

    def _timer(self, action, stage, *, counter=None, before=None, after=None):
        @functools.wraps(action)
        def measured(*args, **kwargs):
            if before is not None:
                before(*args, **kwargs)
            native_before = None
            with self._lock:
                stats = self.result['stages'][stage]
                stats['calls'] += 1
                if stage in _NATIVE_STAGES and self.result['native'][stage]['observations'] < 8:
                    self.result['native'][stage]['observations'] += 1
                    native_before = _process_snapshot()
            started = time.perf_counter()
            failed = False
            try:
                value = action(*args, **kwargs)
                if counter:
                    self._count(counter)
                if after is not None:
                    after(value, *args, **kwargs)
                return value
            except BaseException:
                failed = True
                raise
            finally:
                duration = time.perf_counter() - started
                with self._lock:
                    stats = self.result['stages'][stage]
                    stats['failures'] += failed
                    stats['wall_seconds'] += duration
                    stats['max_call_seconds'] = max(stats['max_call_seconds'], duration)
                    if native_before is not None:
                        native_after = _process_snapshot()
                        native = self.result['native'][stage]
                        first, last = native_before['rss_bytes'], native_after['rss_bytes']
                        if first is not None and last is not None:
                            native['max_before_rss_bytes'] = max(native['max_before_rss_bytes'], first)
                            native['max_after_rss_bytes'] = max(native['max_after_rss_bytes'], last)
                            native['max_positive_rss_delta_bytes'] = max(native['max_positive_rss_delta_bytes'], last - first)
                        native['max_threads'] = max(native['max_threads'], native_before['threads'] or 0, native_after['threads'] or 0)
        return measured

    @contextmanager
    def _scope(self, attribute, category):
        previous = getattr(self._local, attribute, None)
        setattr(self._local, attribute, category)
        try:
            yield
        finally:
            setattr(self._local, attribute, previous)

    def _install(self):
        from readmixer_py import (catalog, crypto, direct_scatter, execution, fastq, paircodec, pool,
            resources, scan, shuffle, sorting, stage_policy, stage_scheduler, storage,
            transaction, worker_bootstrap, worker_storage, worker_transport, parser_stage, stage_admission,
            worker_scatter)
        from readmixer_py.artifacts import bgzf

        original_fdopen, original_write, original_read = os.fdopen, os.write, os.read
        def fdopen(*args, **kwargs):
            stream = original_fdopen(*args, **kwargs)
            category = getattr(self._local, 'read_class', None)
            return _ReadProxy(stream, self, category) if category is not None else stream
        def write(fd, data):
            category = getattr(self._local, 'write_class', None)
            try:
                count = original_write(fd, data)
            except BaseException:
                if category is not None:
                    self._io(category, 'errors', 1)
                raise
            if category is not None:
                self._io(category, 'write_calls', 1)
                self._io(category, 'write_bytes', count)
            return count
        def read(fd, length):
            category = getattr(self._local, 'read_class', None)
            try:
                value = original_read(fd, length)
            except BaseException:
                if category is not None:
                    self._io(category, 'errors', 1)
                raise
            if category is not None:
                self._io_read(category, len(value))
            return value
        self._patch(os, 'fdopen', fdopen)
        self._patch(os, 'read', read)
        self._patch(os, 'write', write)

        def scope_method(owner, name, attribute, category):
            original = getattr(owner, name)
            @functools.wraps(original)
            def wrapped(instance, *args, **kwargs):
                value = category(instance, *args, **kwargs) if callable(category) else category
                with self._scope(attribute, value):
                    return original(instance, *args, **kwargs)
            self._patch(owner, name, wrapped)
        scope_method(catalog._Builder, 'source', 'read_class', 'input_admission')
        original_sheet = catalog._sample_sheet
        def sheet(*args, **kwargs):
            with self._scope('read_class', 'catalog'):
                return original_sheet(*args, **kwargs)
        self._patch(catalog, '_sample_sheet', sheet)
        scope_method(pool.Pool, '__init__', 'read_class', 'manifest')
        scope_method(storage._VerifiedReader, '__init__', 'read_class', 'encrypted_scratch')
        for owner in (storage._DiskSink, worker_storage._GrantedSink):
            scope_method(owner, '_write', 'write_class', 'encrypted_scratch')
        scope_method(transaction.Artifact, 'write', 'write_class', lambda artifact, *_: artifact_class(artifact.name))

        original_decoder = fastq.DecodedStream.__init__
        def decoder(instance, raw, **kwargs):
            return original_decoder(instance, _ReadProxy(raw, self, 'original'), **kwargs)
        self._patch(fastq.DecodedStream, '__init__', decoder)
        original_input = pool._Input.__init__
        def pool_input(instance, owner, stream, descriptor, *args):
            return original_input(instance, owner, _ReadProxy(stream, self, artifact_class(descriptor['name'])), descriptor, *args)
        self._patch(pool._Input, '__init__', pool_input)
        original_partial = transaction.OutputTransaction.open_owned_partial
        def partial(instance, artifact):
            return _ReadProxy(original_partial(instance, artifact), self, artifact_class(artifact.name))
        self._patch(transaction.OutputTransaction, 'open_owned_partial', partial)

        def wrap(owner, name, stage, **kwargs):
            self._patch(owner, name, self._timer(getattr(owner, name), stage, **kwargs))
        for owner, name, stage in (
            (fastq.RecordFramer, '_fill', 'parser'),
            (storage._DiskSink, '_block', 'scratch_aead_encode'),
            (storage._DiskSink, 'write_data', 'scratch_codec'),
            (storage._VerifiedReader, '_next', 'scratch_aead_decode'),
            (crypto.BulkTagger, 'tags', 'tag_render'),
            (transaction.OutputTransaction, 'publish_all', 'publication')):
            wrap(owner, name, stage)
        self._function(bgzf, 'encode_member', 'bgzf_encode')
        # _inflate is static: preserve its descriptor on install/restore.
        self._patch(bgzf.BgzfReader, '_inflate', staticmethod(self._timer(bgzf.BgzfReader._inflate, 'bgzf_decode')))
        self._function(shuffle, '_fill_rank_bytes', 'ranks')
        self._patch(shuffle.np, 'lexsort', self._timer(shuffle.np.lexsort, 'lexsort'))
        self._function(stage_policy, 'execute_policy', 'worker_policy', counter='worker_policy_jobs')
        if self.detail:
            self._function(paircodec, 'encode_pair_into', 'frame_encode')
            self._function(paircodec, 'decode_pair_view', 'frame_decode')
            self._function(shuffle, 'bucket_index', 'bucket_hash')
            self._function(direct_scatter, '_bucket', 'bucket_hash')
        original_flush = sorting.ExternalSorter._flush_run
        def flush(instance):
            had_rows = bool(instance._rows)
            value = original_flush(instance)
            if had_rows:
                self._count('sort_runs')
            return value
        self._patch(sorting.ExternalSorter, '_flush_run', self._timer(flush, 'sort_run'))
        wrap(sorting.ExternalSorter, '_merge_pass', 'sort_merge', counter='merge_passes')
        wrap(sorting.ExternalSorter, '_merge_group', 'sort_merge', counter='merge_groups')
        wrap(shuffle.SerialShuffler, '_refine', 'refinement', counter='refinements')

        # Advance generator timing excludes the consumer's work between yields.
        for name, stage in (('indexed', 'indexed_gather'), ('sequential', 'sequential_gather')):
            original = getattr(scan.PoolScanner, name)
            def generated(instance, *args, _original=original, _stage=stage, **kwargs):
                iterator = _original(instance, *args, **kwargs)
                advance = self._timer(lambda: next(iterator), _stage)
                try:
                    while True:
                        try:
                            value = advance()
                        except StopIteration:
                            # Exhaustion is normal, not a stage failure.
                            self.result['stages'][_stage]['failures'] -= 1
                            return
                        yield value
                        del value
                finally:
                    iterator.close()
            self._patch(scan.PoolScanner, name, generated)

        original_spill = storage.SegmentStore.spill
        def spill(instance, ref):
            was_external = instance.is_external(ref)
            value = original_spill(instance, ref)
            if not was_external and value.physical_bytes:
                self._count('spills')
                self._count('spill_decoded_bytes', value.decoded_bytes)
            return value
        self._patch(storage.SegmentStore, 'spill', spill)
        for owner, name in ((direct_scatter.DirectScatter, '_seal_set'), (worker_scatter.WorkerScatterGeneration, 'finish')):
            original = getattr(owner, name)
            def generation(instance, *args, _original=original, **kwargs):
                result = _original(instance, *args, **kwargs)
                self._count('scatter_generations')
                return result
            self._patch(owner, name, generation)

        original_pair = fastq.PairedReader.next_pair
        def pair(instance, *args, **kwargs):
            result = original_pair(instance, *args, **kwargs)
            if result is not None:
                self._count('source_pairs')
            return result
        self._patch(fastq.PairedReader, 'next_pair', pair)
        def scheduler_window(instance, *args, **kwargs):
            jobs = instance._jobs
            self._peak('scheduler_pending_jobs', len(jobs))
            self._peak('scheduler_pending_bytes', instance.pending_bytes)
            self._peak('scheduler_completed_retained_jobs', sum(job.complete for job in jobs.values()))
            self._peak('scheduler_completed_retained_bytes', sum(
                len(job.output) if isinstance(job.output, (bytes, bytearray)) else getattr(job.output, 'decoded_bytes', 0)
                for job in jobs.values() if job.complete))
        def drain_before(instance):
            scheduler_window(instance)
            job = instance._jobs.get(instance._drain_task)
            if job is not None and not (job.complete and job.result_ack):
                self._count('scheduler_waits')
        wrap(stage_scheduler.StageScheduler, 'take_next', 'scheduler_drain', before=drain_before)
        original_service = stage_scheduler.StageScheduler._service
        def service(instance):
            result = original_service(instance)
            scheduler_window(instance)
            return result
        self._patch(stage_scheduler.StageScheduler, '_service', service)
        def compression_before(instance, *_args):
            self._peak('compression_pending_jobs', instance.pending_jobs)
            if instance._pending and not instance._pending[0].future.done():
                self._count('compression_waits')
        wrap(execution.CompressionService, '_drain_one', 'compression_drain', before=compression_before)
        original_submit = execution.CompressionService._submit
        def submit(instance, *args, **kwargs):
            if instance.slots and instance.pending_jobs == instance.capacity:
                self._count('compression_full_window')
            return original_submit(instance, *args, **kwargs)
        self._patch(execution.CompressionService, '_submit', submit)
        original_pump = worker_transport.FramedSocket.pump
        def pump(instance, *args, **kwargs):
            before = (instance._sent, len(instance._header), len(instance._body or b''), instance._message is not None)
            value = original_pump(instance, *args, **kwargs)
            after = (instance._sent, len(instance._header), len(instance._body or b''), instance._message is not None)
            if before == after:
                self._count('transport_no_progress')
            return value
        self._patch(worker_transport.FramedSocket, 'pump', self._timer(pump, 'transport_pump'))
        original_commit = resources.Budget._commit
        def commit(instance, state):
            result = original_commit(instance, state)
            self._peak('admitted_ram_bytes', instance.used_bytes)
            self._peak('admitted_metadata_bytes', instance.used_metadata_bytes)
            self._peak('admitted_descriptors', instance.used_descriptors)
            return result
        self._patch(resources.Budget, '_commit', commit)
        original_ready = stage_admission.StageAdmission.observe_ready
        def ready(instance, index, rss, descriptors, threads):
            # Retain the actual authenticated READY values, including a valid
            # baseline too large for this attempt. Never publish PID/slot IDs.
            facts = self.result['worker_admission']
            if all(type(value) is int and value >= 0 for value in (rss, descriptors, threads)):
                facts['ready_observations'] += 1
                for key, value in (('max_ready_rss_bytes', rss), ('max_ready_descriptors', descriptors),
                                   ('max_ready_threads', threads), ('max_process_slots', instance.process_slots),
                                   ('max_job_capacity', len(instance.job_leases))):
                    facts[key] = max(facts[key], value)
            try:
                return original_ready(instance, index, rss, descriptors, threads)
            finally:
                for key, leases in (('max_reserved_baseline_bytes', instance.baseline_leases),
                                    ('max_reserved_job_bytes', instance.job_leases),
                                    ('max_reserved_channel_bytes', instance.channel_leases)):
                    for lease in leases:
                        if not lease.released:
                            facts[key] = max(facts[key], lease.bytes)
        self._patch(stage_admission.StageAdmission, 'observe_ready', ready)
        if not self._worker:
            self._reports = tempfile.TemporaryDirectory(prefix='readmixer-observer-')
            self._patch(stage_scheduler, 'worker_main', functools.partial(
                _instrumented_worker, self._reports.name, self.detail, stage_scheduler.worker_main))
            original_start = stage_scheduler._start
            def start(process):
                try:
                    return original_start(process)
                finally:
                    if process.pid is not None:
                        self.result['workers']['expected'] += 1
            self._patch(stage_scheduler, '_start', self._timer(start, 'process_startup'))

    def _sample_storage(self):
        started = time.perf_counter()
        totals = {name: 0 for name in _CLASSES if name != 'original'}
        def visit(path, scratch, depth=0):
            if path is None:
                return 0
            if depth > 64:
                self._error()
                return 0
            size = 0
            try:
                with os.scandir(path) as entries:
                    for entry in entries:
                        try:
                            info = entry.stat(follow_symlinks=False)
                            if stat.S_ISDIR(info.st_mode):
                                size += visit(entry.path, scratch, depth + 1)
                            elif stat.S_ISREG(info.st_mode):
                                category = 'encrypted_scratch' if scratch else artifact_class(entry.name)
                                totals[category] += info.st_size
                                size += info.st_size
                        except FileNotFoundError:
                            pass
            except FileNotFoundError:
                pass
            except OSError:
                self._error()
            return size
        scratch = visit(self.scratch, True)
        output = visit(self.output, False)
        with self._lock:
            result = self.result['storage']
            result['observations'] += 1
            result['observer_seconds'] += time.perf_counter() - started
            result['peak_scratch_apparent_bytes'] = max(result['peak_scratch_apparent_bytes'], scratch)
            result['peak_output_apparent_bytes'] = max(result['peak_output_apparent_bytes'], output)
            if scratch + output > result['peak_coexisting_apparent_bytes']:
                result['peak_coexisting_apparent_bytes'] = scratch + output
                result['scratch_when_coexistence_peaked'] = scratch
                result['output_when_coexistence_peaked'] = output
            for name, size in totals.items():
                result['peak_by_class'][name] = max(result['peak_by_class'][name], size)

    def _run(self):
        while not self._stop.wait(self.interval):
            self._sample_storage()

    def _collect_workers(self):
        if self._reports is None:
            return
        workers = self.result['workers']
        try:
            with os.scandir(self._reports.name) as entries:
                for entry in entries:
                    if workers['received'] >= workers['expected']:
                        self._error()
                        break
                    try:
                        if not entry.is_file(follow_symlinks=False):
                            raise ValueError
                        with open(entry.path, 'rb') as source:
                            data = source.read((128 << 10) + 1)
                        if len(data) > 128 << 10:
                            raise ValueError
                        report = json.loads(data)
                        # Closed data shape and numeric scalar validation: no
                        # worker can accidentally copy opaque diagnostics here.
                        if type(report) is not dict or type(report.get('complete')) is not bool:
                            raise ValueError
                        for group in ('io', 'stages', 'counters', 'peaks', 'native', 'worker_admission'):
                            if type(report.get(group)) is not dict or report[group].keys() != self.result[group].keys():
                                raise ValueError
                        for group in ('io', 'stages', 'native'):
                            for key, fields in self.result[group].items():
                                if type(report[group][key]) is not dict or report[group][key].keys() != fields.keys():
                                    raise ValueError
                                if any(type(value) not in (int, float) or not math.isfinite(value) or value < 0 for value in report[group][key].values()):
                                    raise ValueError
                        for group in ('counters', 'peaks', 'worker_admission'):
                            if any(type(value) not in (int, float) or not math.isfinite(value) or value < 0 for value in report[group].values()):
                                raise ValueError
                        for group in ('io', 'stages', 'native'):
                            for key, fields in self.result[group].items():
                                for field in fields:
                                    value = report[group][key][field]
                                    if field.startswith('max_'):
                                        fields[field] = max(fields[field], value)
                                    else:
                                        fields[field] += value
                        for key, value in report['counters'].items():
                            self.result['counters'][key] += value
                        for key, value in report['peaks'].items():
                            self.result['peaks'][key] = max(self.result['peaks'][key], value)
                        for key, value in report['worker_admission'].items():
                            if key.startswith('max_'):
                                self.result['worker_admission'][key] = max(self.result['worker_admission'][key], value)
                            else:
                                self.result['worker_admission'][key] += value
                        final = report['snapshots']['after_operation']
                        workers['lifetime_cpu_seconds'] += final['cpu_seconds'] or 0
                        workers['peak_individual_rss_bytes'] = max(workers['peak_individual_rss_bytes'], final['lifetime_peak_rss_bytes'] or 0)
                        workers['received'] += 1
                        if not report['complete']:
                            workers['complete'] = False
                            self._error()
                    except (OSError, ValueError, KeyError, TypeError, AttributeError, OverflowError):
                        self._error()
        finally:
            workers['missing'] = max(0, workers['expected'] - workers['received'])
            workers['complete'] = workers['complete'] and workers['missing'] == 0
            if not workers['complete']:
                self._error()
            self._reports.cleanup()
            self._reports = None

    def __enter__(self):
        if self._entered or not _CONTEXT_LOCK.acquire(blocking=False):
            raise RuntimeError('resource telemetry contexts must not overlap')
        self._entered = True
        self.result['snapshots']['before_hook_imports'] = _process_snapshot()
        try:
            self._install()
            self.result['snapshots']['before_operation'] = _process_snapshot()
            if not self._worker:
                self._sample_storage()
                self._thread = threading.Thread(target=self._run, name='readmixer-storage-observer', daemon=True)
                self._thread.start()
            return self
        except BaseException:
            self.__exit__(*sys.exc_info())
            raise

    def __exit__(self, *_exc):
        try:
            self._stop.set()
            if self._thread is not None:
                self._thread.join()
            if not self._worker:
                self._sample_storage()
            self.result['snapshots']['after_operation'] = _process_snapshot()
        finally:
            for owner, name, original in reversed(self._patches):
                setattr(owner, name, original)
            self._patches.clear()
            # A lazy production module may have imported a wrapped function
            # by value after installation. Restore those newly imported aliases
            # too, or they would retain this observer beyond its lifetime.
            for candidate in tuple(sys.modules.values()):
                if candidate is not None and getattr(candidate, '__name__', '').startswith('readmixer_py'):
                    for name, value in tuple(vars(candidate).items()):
                        for wrapped, original in self._function_aliases:
                            if value is wrapped:
                                setattr(candidate, name, original)
                                break
            self._function_aliases.clear()
            try:
                self._collect_workers()
            finally:
                self._entered = False
                _CONTEXT_LOCK.release()
