"""P15-T06: bounded public workflows reach faults before cleanup is asserted.

These use small artificial inputs and lower *internal sort batch sizes*, never
weaken public admission or artifact caps. Exceptions remain live during ownership
assertions. Process lifecycle faults are covered by the production-worker and
real-signal harnesses; these cases force CPU=1 to isolate storage/publication.
"""
from contextlib import contextmanager
import errno
import json
import os

import psutil
import pytest

from readmixer_py import mixing, recovery, verification
from readmixer_py.catalog import SampleCatalog
from readmixer_py.config import MixConfig, RecoverConfig, RecoveryMode, StorageMode, VerifyConfig
from readmixer_py.errors import IntegrityError, PublicationError, ResourceError, UnsupportedFormatError
from readmixer_py.execution import CompressionService
from readmixer_py.pool import Pool, _Input
from readmixer_py.resources import Budget, ResourceObservations
from readmixer_py.sorting import ExternalSorter
from readmixer_py.storage import SegmentStore, _DiskSink
from readmixer_py.transaction import Artifact, OutputTransaction
from test_pipeline import fixtures

_OPERATIONS = ('mix', 'verify', 'indexed', 'sequential')
_OUTPUTS = ('mix', 'indexed', 'sequential')
_PRIVATE = 'PRIVATE_FAULT_DETAIL_AND_PATH_MUST_NOT_BE_REPORTED'


class _Workflow:
    def __init__(self, root, operation):
        self.operation = operation
        self.inputs, self.destination, self.scratch = [root / name for name in ('inputs', 'destination', 'scratch')]
        self.expected = fixtures(self.inputs, counts=(60, 41, 0), metadata=1, sequence=7)
        self.destination.mkdir(mode=0o700)
        self.scratch.mkdir(mode=0o700)
        (self.destination / 'foreign').write_bytes(b'foreign output must survive')
        self.options = dict(max_ram=1 << 30, workers=1, storage=StorageMode.EXTERNAL,
                            max_record_bytes=1024, temp_dir=self.scratch)
        self.manifest = None
        if operation != 'mix':
            report = mixing._mix(MixConfig(input_dir=self.inputs, output=root / 'pool', **self.options),
                                 _scatter_strategy='sorter')
            self.manifest = root / 'pool' / report.manifest
        self.source = self.snapshot()
        self.budgets, self.streams, self.stores, self.services, self.transactions = [], [], [], [], []
        self.reached = []

    def snapshot(self):
        roots = [self.inputs] + ([self.manifest.parent] if self.manifest else [])
        return {str(path): path.read_bytes() for root in roots for path in root.iterdir() if path.is_file()}

    def observe(self, monkeypatch):
        module = {'mix': mixing, 'verify': verification}.get(self.operation, recovery)
        resources = module.operation_resources
        def acquire_resources(*args, **kwargs):
            plan, budget = resources(*args, **kwargs)
            self.budgets.append((plan, budget))
            return plan, budget
        monkeypatch.setattr(module, 'operation_resources', acquire_resources)
        input_init = _Input.__init__
        def input_reader(reader, *args, **kwargs):
            input_init(reader, *args, **kwargs)
            self.streams.append(reader.stream)
        monkeypatch.setattr(_Input, '__init__', input_reader)
        open_input = SampleCatalog.open_input
        @contextmanager
        def catalog_input(catalog, *args, **kwargs):
            with open_input(catalog, *args, **kwargs) as stream:
                self.streams.append(stream)
                yield stream
        monkeypatch.setattr(SampleCatalog, 'open_input', catalog_input)
        store_init = SegmentStore.__init__
        def store(instance, *args, **kwargs):
            store_init(instance, *args, **kwargs)
            self.stores.append(instance)
        monkeypatch.setattr(SegmentStore, '__init__', store)
        service_init = CompressionService.__init__
        def service(instance, *args, **kwargs):
            service_init(instance, *args, **kwargs)
            self.services.append(instance)
        monkeypatch.setattr(CompressionService, '__init__', service)
        begin = OutputTransaction.begin
        def transaction(cls, *args, **kwargs):
            instance = begin(*args, **kwargs)
            self.transactions.append(instance)
            return instance
        monkeypatch.setattr(OutputTransaction, 'begin', classmethod(transaction))
        # Enough independently sealed runs for multiple binary merge passes.
        init = ExternalSorter.__init__
        def sorter(instance, *args, **kwargs):
            kwargs.update(run_bytes=2048, fan_in=2)
            init(instance, *args, **kwargs)
        monkeypatch.setattr(ExternalSorter, '__init__', sorter)
        self.fd_count = psutil.Process().num_fds()
        self.children = {child.pid for child in psutil.Process().children(recursive=True)}

    def run(self, **options):
        settings = dict(self.options, **options)
        if self.operation == 'mix':
            return mixing._mix(MixConfig(input_dir=self.inputs, output=self.destination, **settings),
                               _scatter_strategy='sorter')
        if self.operation == 'verify':
            return verification.verify(VerifyConfig(manifest=self.manifest, **settings))
        return recovery.recover(RecoverConfig(manifest=self.manifest, output=self.destination,
            sample=('alpha', 'beta', 'empty'), io=RecoveryMode(self.operation), **settings))

    def assert_clean(self, error=None, *, committed=False, residue=False):
        assert self.reached, 'the intended stage was never reached'
        if error is not None:
            assert error.__traceback__ is not None  # Retain it throughout the checks.
            diagnostic = str(error) + ' '.join(getattr(error, '__notes__', ()))
            assert _PRIVATE not in diagnostic and str(self.inputs) not in diagnostic
            assert len(getattr(error, '__notes__', ())) <= 8
        assert self.snapshot() == self.source
        assert (self.destination / 'foreign').read_bytes() == b'foreign output must survive'
        if not committed and not residue:
            assert list(self.destination.iterdir()) == [self.destination / 'foreign']
        if not residue:
            assert not list(self.scratch.iterdir())
        assert self.streams and all(stream.closed for stream in self.streams)
        assert self.budgets and all(budget.active_leases == budget.used_descriptors == 0
                                   and budget.used_bytes == budget.baseline_bytes
                                   for _plan, budget in self.budgets)
        assert all(store._closed for store in self.stores)
        assert all(service._closed and service.pending_jobs == 0 and service._executor is None
                   for service in self.services)
        assert all(transaction._resources_closed for transaction in self.transactions)
        assert psutil.Process().num_fds() == self.fd_count
        assert {child.pid for child in psutil.Process().children(recursive=True)} == self.children


@pytest.mark.parametrize('operation', _OPERATIONS)
@pytest.mark.parametrize('fault', ('ram', 'descriptors', 'pressure'))
def test_P15_T06_resource_change_after_authenticated_run_seal(tmp_path, monkeypatch, operation, fault):
    case = _Workflow(tmp_path, operation)
    case.observe(monkeypatch)
    publish, admit = ExternalSorter._publish_run, Budget._admit
    reduced, failed_admission = [], []
    def actual_admission(budget, category, *args):
        try:
            return admit(budget, category, *args)
        except ResourceError:
            if reduced and budget is case.budgets[0][1]:
                failed_admission.append(category)
                # The transient contention ends only after a real workflow
                # allocation has failed. Cleanup retains its normal allowance.
                budget._admission_limit, budget.descriptor_limit = reduced.pop()
            raise
    monkeypatch.setattr(Budget, '_admit', actual_admission)
    def sealed(sorter, *args, **kwargs):
        ref = publish(sorter, *args, **kwargs)
        if not case.reached:
            assert ref.physical_bytes > 0 and ref.item_count > 0 and sorter.run_count > 0
            case.reached.append((sorter.sort_purpose, ref.item_count))
            plan, budget = case.budgets[0]
            if fault == 'pressure':
                budget.checkpoint(plan, ResourceObservations(
                    available_memory=budget.used_bytes * 5 // 4 - 5, coordinator_rss=0))
                pytest.fail('pressure did not reject the active working set')
            reduced.append((budget._admission_limit, budget.descriptor_limit))
            if fault == 'ram':
                budget._admission_limit = budget.used_bytes + 1024
            else:
                budget.descriptor_limit = budget.used_descriptors
        return ref
    monkeypatch.setattr(ExternalSorter, '_publish_run', sealed)
    with pytest.raises(ResourceError) as caught:
        case.run()
    assert {'ram': 'requested', 'descriptors': 'descriptor', 'pressure': 'pressure'}[fault] in str(caught.value)
    if fault != 'pressure':
        assert len(failed_admission) == 1 and not reduced
        assert 'injected' not in failed_admission[0]
    case.assert_clean(caught.value)


@pytest.mark.parametrize('operation', _OPERATIONS)
@pytest.mark.parametrize('fault', ('scratch_credit', 'enospc', 'zero_write'))
def test_P15_T06_scratch_failure_after_protected_prefix(tmp_path, monkeypatch, operation, fault):
    case = _Workflow(tmp_path, operation)
    case.observe(monkeypatch)
    write = _DiskSink._write
    real_write = os.write
    def protected_write(sink, data):
        if sink.ref.purpose == 3 and sink.physical and not case.reached:
            assert sink.fd is not None and os.fstat(sink.fd).st_size == sink.physical
            case.reached.append((sink.physical, sink.fd))
            if fault == 'scratch_credit':
                # Exercise the real disk-growth admission using a vanished free
                # space observation, after this writer already owns ciphertext.
                import readmixer_py.disk as disk
                statvfs = disk.os.fstatvfs
                facts = statvfs(sink.store._directory_fd)
                class Empty:
                    f_bavail = 0
                    f_frsize = facts.f_frsize
                    f_bsize = facts.f_bsize
                with monkeypatch.context() as scoped:
                    scoped.setattr(disk.os, 'fstatvfs', lambda fd: Empty())
                    return write(sink, data)
            calls = []
            def short(descriptor, payload):
                if descriptor != sink.fd:
                    return real_write(descriptor, payload)
                calls.append(len(payload))
                if len(calls) == 1:
                    return real_write(descriptor, payload[:2])
                if fault == 'enospc':
                    raise OSError(errno.ENOSPC, _PRIVATE)
                return 0
            with monkeypatch.context() as scoped:
                scoped.setattr(os, 'write', short)
                try:
                    return write(sink, data)
                finally:
                    assert len(calls) == 2
        return write(sink, data)
    monkeypatch.setattr(_DiskSink, '_write', protected_write)
    with pytest.raises(ResourceError) as caught:
        case.run()
    if fault == 'enospc':
        assert caught.value.__cause__.errno == errno.ENOSPC
    case.assert_clean(caught.value)


@pytest.mark.parametrize('operation', _OPERATIONS)
def test_P15_T06_second_merge_pass_failure_owns_sealed_child_and_parents(tmp_path, monkeypatch, operation):
    case = _Workflow(tmp_path, operation)
    case.observe(monkeypatch)
    publish = ExternalSorter._publish_run
    primary = ResourceError('injected merge child handoff failure')
    def publish_child(sorter, *args, **kwargs):
        ref = publish(sorter, *args, **kwargs)
        if sorter._level >= 2:
            assert sorter.merge_passes >= 1 and sorter._next_runs.root.record_count > 0
            assert ref.item_count > 1 and ref.physical_bytes > 0
            case.reached.append((sorter._level, sorter.merge_passes, ref.item_count))
            raise primary
        return ref
    monkeypatch.setattr(ExternalSorter, '_publish_run', publish_child)
    with pytest.raises(ResourceError) as caught:
        case.run()
    assert caught.value is primary
    case.assert_clean(caught.value)


@pytest.mark.parametrize('operation', _OUTPUTS)
@pytest.mark.parametrize('fault', ('enospc', 'zero_write'))
def test_P15_T06_staged_artifact_partial_write_failure(tmp_path, monkeypatch, operation, fault):
    case = _Workflow(tmp_path, operation)
    case.observe(monkeypatch)
    write, real_write = Artifact.write, os.write
    write_errors = []
    def artifact_write(artifact, data):
        if not artifact._completion and len(data) > 2 and not case.reached:
            assert artifact._transaction._state == 'open'
            calls = []
            def short(descriptor, payload):
                if descriptor != artifact._fd:
                    return real_write(descriptor, payload)
                calls.append(len(payload))
                if len(calls) == 1:
                    count = real_write(descriptor, payload[:2])
                    case.reached.append((artifact.name, count))
                    return count
                if fault == 'enospc':
                    raise OSError(errno.ENOSPC, _PRIVATE)
                return 0
            with monkeypatch.context() as scoped:
                scoped.setattr(os, 'write', short)
                try:
                    return write(artifact, data)
                except PublicationError as error:
                    write_errors.append(error.__cause__)
                    raise
                finally:
                    assert len(calls) == 2
        return write(artifact, data)
    monkeypatch.setattr(Artifact, 'write', artifact_write)
    with pytest.raises(PublicationError) as caught:
        case.run()
    if fault == 'enospc':
        assert write_errors[0].errno == errno.ENOSPC
    case.assert_clean(caught.value)


@pytest.mark.parametrize('operation', _OUTPUTS)
def test_P15_T06_compression_failure_after_an_earlier_lane_drained(tmp_path, monkeypatch, operation):
    case = _Workflow(tmp_path, operation)
    case.observe(monkeypatch)
    module = mixing if operation == 'mix' else recovery
    encode = module.encode_member
    encoded = []
    def fail_after_first(payload, level):
        if encoded:
            assert case.transactions and case.transactions[0]._state == 'open'
            assert case.services[0].completed_jobs >= 1
            stage = case.transactions[0]._stage.path
            assert any(path.stat().st_size > 0 for path in stage.iterdir() if path.is_file())
            case.reached.append(len(encoded))
            raise OSError(errno.EIO, _PRIVATE)
        result = encode(payload, level)
        encoded.append(len(result))
        return result
    monkeypatch.setattr(module, 'encode_member', fail_after_first)
    with pytest.raises(ResourceError) as caught:
        case.run()
    assert str(caught.value) == 'bounded execution job failed'
    case.assert_clean(caught.value)


@pytest.mark.parametrize('operation,boundary', [('mix', 'after')] + [
    (operation, boundary) for operation in ('verify', 'indexed', 'sequential') for boundary in ('before', 'after')])
def test_P15_T06_source_close_failure_prevents_success(tmp_path, monkeypatch, operation, boundary):
    case = _Workflow(tmp_path, operation)
    case.observe(monkeypatch)
    primary = ResourceError('source descriptor close failed after verified work')
    if operation == 'mix':
        close = SampleCatalog.close
        def close_sources(catalog):
            live = catalog._root_fd >= 0
            result = close(catalog)
            if live and not case.reached:
                assert case.streams and all(stream.closed for stream in case.streams)
                case.reached.append('all parsed source readers closed')
                raise primary
            return result
        monkeypatch.setattr(SampleCatalog, 'close', close_sources)
    else:
        close = Pool.close_sources
        def close_sources(pool):
            if boundary == 'before' and not case.reached:
                assert case.streams and all(stream.closed for stream in case.streams)
                case.reached.append('source directory still owned at interrupted close')
                raise primary
            result = close(pool)
            if not case.reached:
                assert case.streams and all(stream.closed for stream in case.streams)
                if operation != 'verify':
                    assert case.transactions[0]._journal.inventory_count >= 8
                    assert all(service._closed for service in case.services)
                case.reached.append('verified sources closed before return/publication')
                raise primary
            return result
        monkeypatch.setattr(Pool, 'close_sources', close_sources)
    with pytest.raises(ResourceError) as caught:
        case.run()
    assert caught.value is primary
    case.assert_clean(caught.value)


@pytest.mark.parametrize('operation', ('verify', 'indexed', 'sequential'))
def test_P15_T06_false_small_source_totals_do_not_authorize_actual_decoded_work(tmp_path, monkeypatch, operation):
    case = _Workflow(tmp_path, operation)
    document = json.loads(case.manifest.read_bytes())
    declared = 0
    for sample in document['samples']:
        for role in ('r1', 'r2'):
            source = sample[role]
            source['normalized_bytes'] = source['decoded_bytes'] = 7 * sample['pairs']
            if source['input_codec'] == 'plain':
                source['physical_bytes'] = source['decoded_bytes']
            declared += source['normalized_bytes']
    case.manifest.write_text(json.dumps(document) + '\n')
    case.source = case.snapshot()
    case.observe(monkeypatch)
    append = ExternalSorter.append
    accumulated = [0]
    def observe(sorter, frame):
        if sorter.sort_purpose in ('verify-originals', 'recover-originals'):
            accumulated[0] += len(frame) - 40
            case.reached.append(accumulated[0])
        return append(sorter, frame)
    monkeypatch.setattr(ExternalSorter, 'append', observe)
    with pytest.raises(ResourceError, match='max_total_uncompressed') as caught:
        case.run(max_total_uncompressed=declared + 1)
    assert accumulated[0] > 0 and accumulated[0] <= declared + 1
    case.assert_clean(caught.value)


@pytest.mark.parametrize('operation', ('indexed', 'sequential'))
def test_P15_T06_late_sample_digest_failure_rolls_back_earlier_staged_sample(tmp_path, monkeypatch, operation):
    case = _Workflow(tmp_path, operation)
    document = json.loads(case.manifest.read_bytes())
    document['samples'][1]['r2']['normalized_sha256'] = '0' * 64
    case.manifest.write_text(json.dumps(document) + '\n')
    case.source = case.snapshot()
    case.observe(monkeypatch)
    finish = recovery._RecoveryEmitter.finish_sample
    def complete_sample(emitter):
        sample = emitter._sample['sample_index']
        result = finish(emitter)
        if sample == 0:
            assert emitter.parts > 0 and emitter.transaction._journal.inventory_count >= 4
            assert len(emitter.samples) == 1 and emitter.samples[0]['pairs'] == 60
            case.reached.append('first sample completely staged and registered')
        return result
    monkeypatch.setattr(recovery._RecoveryEmitter, 'finish_sample', complete_sample)
    with pytest.raises(IntegrityError, match='normalized digest') as caught:
        case.run()
    case.assert_clean(caught.value)


@pytest.mark.parametrize('operation', _OUTPUTS)
@pytest.mark.parametrize('fault', ('artifact_link', 'artifact_sync', 'marker_link', 'marker_sync'))
def test_P15_T06_publication_checkpoints_roll_back_one_complete_operation(tmp_path, monkeypatch, operation, fault):
    case = _Workflow(tmp_path, operation)
    case.observe(monkeypatch)
    link, sync = OutputTransaction._link, os.fsync
    linked = []
    def link_then_fail(transaction, artifact):
        result = link(transaction, artifact)
        linked.append(artifact._completion)
        assert all(stream.closed for stream in case.streams)
        assert all(service._closed for service in case.services)
        assert transaction._state == 'publishing'
        if not case.reached and ((fault == 'artifact_link' and not artifact._completion)
                                 or (fault == 'marker_link' and artifact._completion)):
            assert (case.destination / artifact.name).exists()
            case.reached.append(fault)
            raise OSError(errno.EIO, _PRIVATE)
        return result
    def sync_then_fail(descriptor):
        if case.transactions:
            transaction = case.transactions[0]
            if descriptor == transaction._destination.fd and transaction._state == 'publishing':
                marker = bool(transaction._completion and transaction._completion._linked)
                if not case.reached and ((fault == 'artifact_sync' and not marker)
                                         or (fault == 'marker_sync' and marker)):
                    assert linked and any(not completion for completion in linked)
                    case.reached.append(fault)
                    raise OSError(errno.EIO, _PRIVATE)
        return sync(descriptor)
    monkeypatch.setattr(OutputTransaction, '_link', link_then_fail)
    monkeypatch.setattr(os, 'fsync', sync_then_fail)
    with pytest.raises(PublicationError) as caught:
        case.run()
    assert caught.value.__cause__.errno == errno.EIO
    assert ('indeterminate' in str(caught.value)) is fault.startswith('marker')
    case.assert_clean(caught.value)


@pytest.mark.parametrize('operation', _OUTPUTS)
def test_P15_T06_postcommit_housekeeping_warning_preserves_verifiable_complete_set(tmp_path, monkeypatch, operation):
    case = _Workflow(tmp_path, operation)
    case.observe(monkeypatch)
    sync = os.fsync
    def fail_cleanup_sync(descriptor):
        if case.transactions:
            transaction = case.transactions[0]
            if transaction._state == 'committed' and descriptor == transaction._destination.fd and not case.reached:
                assert (case.destination / transaction.completion_basename).exists()
                case.reached.append('marker directory sync completed before housekeeping')
                raise OSError(errno.EIO, _PRIVATE)
        return sync(descriptor)
    monkeypatch.setattr(os, 'fsync', fail_cleanup_sync)
    report = case.run()
    assert report.completed and not report.cleanup_complete
    assert report.warnings == ('cleanup directory sync failed',)
    case.assert_clean(committed=True)
    if operation == 'mix':
        # Independent complete verifier consumes the surviving committed set.
        monkeypatch.undo()
        verified = verification.verify(VerifyConfig(manifest=case.destination / report.manifest,
            max_ram=1 << 30, workers=1, storage=StorageMode.EXTERNAL, temp_dir=case.scratch))
        assert verified.completed and verified.pairs == 101
    else:
        import gzip
        receipt = json.loads((case.destination / report.receipt).read_bytes())
        for sample in receipt['samples']:
            for role in ('r1', 'r2'):
                actual = b''.join(gzip.decompress((case.destination / part[role]['name']).read_bytes())
                                  for part in sample['parts'])
                import hashlib
                assert hashlib.sha256(actual).hexdigest() == sample[role]['normalized_sha256']
                assert len(actual) == sample[role]['normalized_bytes']


@pytest.mark.parametrize('operation', _OPERATIONS)
def test_P15_T06_positive_short_writes_complete_every_public_workflow(tmp_path, monkeypatch, operation):
    case = _Workflow(tmp_path, operation)
    case.observe(monkeypatch)
    write = os.write
    calls = [0]
    def short_positive(descriptor, data):
        count = write(descriptor, memoryview(data)[:127])
        calls[0] += 1
        assert count > 0
        if count < len(data) and not case.reached:
            case.reached.append('positive short write requires a forward retry')
        return count
    monkeypatch.setattr(os, 'write', short_positive)
    report = case.run()
    assert report.completed and report.pairs == 101 and calls[0] > 10
    case.assert_clean(committed=operation != 'verify')
    if operation == 'mix':
        monkeypatch.undo()
        verified = verification.verify(VerifyConfig(manifest=case.destination / report.manifest,
            max_ram=1 << 30, workers=1, storage=StorageMode.EXTERNAL, temp_dir=case.scratch))
        assert verified.completed and verified.pairs == 101
    elif operation != 'verify':
        import gzip
        receipt = json.loads((case.destination / report.receipt).read_bytes())
        for sample in receipt['samples']:
            for role in (1, 2):
                actual = b''.join(gzip.decompress((case.destination / part[f'r{role}']['name']).read_bytes())
                                  for part in sample['parts'])
                assert actual == case.expected[sample['sample_id'], role]


@pytest.mark.parametrize('operation', _OPERATIONS)
def test_P15_T06_secondary_source_cleanup_preserves_first_workflow_error(tmp_path, monkeypatch, operation):
    case = _Workflow(tmp_path, operation)
    case.observe(monkeypatch)
    publish = ExternalSorter._publish_run
    primary = ResourceError('data-processing failure after authenticated run')
    def sealed(sorter, *args, **kwargs):
        ref = publish(sorter, *args, **kwargs)
        assert ref.item_count > 0 and ref.physical_bytes > 0
        case.reached.append('protected run sealed before primary error')
        raise primary
    monkeypatch.setattr(ExternalSorter, '_publish_run', sealed)
    owner = SampleCatalog if operation == 'mix' else Pool
    name = 'close' if operation == 'mix' else 'close_sources'
    close = getattr(owner, name)
    secondary = []
    def close_then_fail(instance):
        result = close(instance)
        if case.reached and not secondary:
            secondary.append(True)
            raise ResourceError(_PRIVATE)
        return result
    monkeypatch.setattr(owner, name, close_then_fail)
    with pytest.raises(ResourceError) as caught:
        case.run()
    assert caught.value is primary and secondary == [True]
    assert any('cleanup also failed' in note or 'disposal also failed' in note
               for note in caught.value.__notes__)
    case.assert_clean(caught.value)


def test_P15_T06_pool_admission_primary_survives_secondary_close_error(tmp_path, monkeypatch):
    case = _Workflow(tmp_path, 'verify')
    document = json.loads(case.manifest.read_bytes())
    document['schema_version'] = 999
    case.manifest.write_text(json.dumps(document) + '\n')
    before = case.snapshot()
    budget = Budget(512 << 20)
    close = Pool.close_sources
    reached = []
    def close_then_fail(pool):
        assert pool._metadata_lease is not None
        result = close(pool)
        reached.append('failed admitted manifest owner closed its directory')
        raise ResourceError(_PRIVATE)
    monkeypatch.setattr(Pool, 'close_sources', close_then_fail)
    descriptors = psutil.Process().num_fds()
    with pytest.raises(UnsupportedFormatError, match='schema version') as caught:
        Pool(case.manifest, budget, metadata_bytes=16 << 20)
    assert caught.value.__traceback__ is not None and reached
    assert _PRIVATE not in str(caught.value)
    assert 'pool admission cleanup also failed; primary error preserved' in caught.value.__notes__
    assert budget.active_leases == budget.used_bytes == budget.used_descriptors == 0
    assert psutil.Process().num_fds() == descriptors
    assert case.snapshot() == before


@pytest.mark.parametrize('operation', _OUTPUTS)
def test_P15_T06_foreign_replacement_after_link_is_preserved_with_bounded_residue_warning(tmp_path, monkeypatch, operation):
    case = _Workflow(tmp_path, operation)
    case.observe(monkeypatch)
    link = OutputTransaction._link
    replacements = []
    def replaced(transaction, artifact):
        result = link(transaction, artifact)
        if not case.reached and not artifact._completion:
            path = case.destination / artifact.name
            original_inode = path.stat().st_ino
            # Staging still owns the original inode, so replacement is distinct
            # even on filesystems that promptly recycle unlinked inode numbers.
            path.unlink()
            path.write_bytes(b'foreign replacement must survive failed publication')
            assert path.stat().st_ino != original_inode
            replacements.append(path)
            case.reached.append('first successful link replaced before next publication step')
            raise OSError(errno.EIO, _PRIVATE)
        return result
    monkeypatch.setattr(OutputTransaction, '_link', replaced)
    with pytest.raises(PublicationError) as caught:
        case.run()
    assert caught.value.__cause__.errno == errno.EIO
    assert any('replaced names remain' in note for note in caught.value.__notes__)
    assert replacements[0].read_bytes() == b'foreign replacement must survive failed publication'
    transaction = case.transactions[0]
    assert not (case.destination / transaction.completion_basename).exists()
    case.assert_clean(caught.value, residue=True)
    remaining = [path for directory in (case.scratch, case.destination)
                 for path in directory.rglob('*')
                 if path not in replacements and path != case.destination / 'foreign']
    # Uncertain authenticated ownership pages may remain. Every such file is
    # private; a warning is required instead of guessing which names to delete.
    for path in remaining:
        assert path.stat().st_mode & 0o777 == (0o700 if path.is_dir() else 0o600)
        if path.is_file():
            assert b'@repeat/' not in path.read_bytes()
    if remaining:
        assert any('cleanup incomplete' in note for note in caught.value.__notes__)
