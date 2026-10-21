"""CPU-quota admission and bounded, ordered shared compression ownership."""
from threading import Event, Timer, enumerate as threads, get_ident

import pytest

from readmixer_py import execution
from readmixer_py.artifacts.bgzf import encode_member
from readmixer_py.errors import CancelledError, ConfigurationError, IntegrityError, ResourceError
from readmixer_py.execution import (CancellationState, CompressionService, CpuObservations, CpuQuota,
                                   parse_cpu_quota, resolve_cpu_budget)
from readmixer_py.resources import Budget


def cpu(count):
    return resolve_cpu_budget(observations=CpuObservations(count))


@pytest.mark.parametrize('facts,workers,expected', [
    (CpuObservations(32,8,(CpuQuota(250000,100000),)),None,2),
    (CpuObservations(32,8,(CpuQuota(25000,100000),)),None,1),
    (CpuObservations(32,8,(CpuQuota(None,100000),)),4,4),
    (CpuObservations(32,8,(CpuQuota(400000,100000),CpuQuota(100000,100000,1))),8,1),
    (CpuObservations(2,8),None,2), (CpuObservations(8),1,1),
])
def test_cpu_intersects_logical_affinity_all_quotas_and_request(facts,workers,expected):
    plan=resolve_cpu_budget(workers,observations=facts)
    assert plan.effective_limit==expected
    assert 'logical-cpus' in plan.detection_sources


@pytest.mark.parametrize('value,version,period,expected', [
    ('max 100000',2,None,CpuQuota(None,100000)),
    ('25000 100000',2,None,CpuQuota(25000,100000)),
    ('-1',1,'100000',CpuQuota(None,100000,1)),
    ('300000',1,'100000',CpuQuota(300000,100000,1)),
])
def test_quota_encoding(value,version,period,expected):
    assert parse_cpu_quota(value,version,period=period)==expected


@pytest.mark.parametrize('value,version,period', [
    ('0 100000',2,None),('-1 100000',2,None),('max 0',2,None),('1',2,None),
    ('1 2 extra',2,None),('1.5 100000',2,None),('+1 100000',2,None),
    ('-2',1,'100000'),('1',1,'0'),('1',1,None),('max 100000',3,None),
])
def test_malformed_quotas_fail_closed(value,version,period):
    with pytest.raises(ResourceError):parse_cpu_quota(value,version,period=period)


def test_cgroup_v2_and_v1_ancestor_quotas_and_unlimited(tmp_path):
    proc=tmp_path/'proc';(proc/'self').mkdir(parents=True)
    v2=tmp_path/'v2';(v2/'team'/'job').mkdir(parents=True)
    v1=tmp_path/'v1';(v1/'team'/'job').mkdir(parents=True)
    (proc/'self'/'cgroup').write_text('0::/team/job\n2:cpu,cpuacct:/team/job\n')
    (proc/'self'/'mountinfo').write_text(
        f'1 0 0:1 / {v2} rw - cgroup2 cgroup rw\n'
        f'2 0 0:2 / {v1} rw - cgroup cgroup rw,cpu,cpuacct\n')
    (v2/'team'/'job'/'cpu.max').write_text('max 100000')
    (v2/'team'/'cpu.max').write_text('350000 100000')
    (v2/'cpu.max').write_text('800000 100000')
    (v1/'team'/'job'/'cpu.cfs_quota_us').write_text('150000')
    (v1/'team'/'job'/'cpu.cfs_period_us').write_text('100000')
    quotas=execution._cgroup_cpu(proc)
    assert len(quotas)==4
    plan=resolve_cpu_budget(observations=CpuObservations(16,8,quotas))
    assert plan.effective_limit==1
    assert 'cgroup-v1:quota' in plan.detection_sources
    assert 'cgroup-v2:unlimited' in plan.detection_sources


def test_one_cpu_is_synchronous_without_background_executor(monkeypatch):
    monkeypatch.setattr(execution,'ThreadPoolExecutor',lambda **kwargs:pytest.fail('unexpected background pool'))
    budget=Budget(16<<20)
    owner=get_ident();encoded=[];consumed=[]
    def encoder(payload,level):
        encoded.append(get_ident())
        return b'encoded-'+payload
    with CompressionService(budget,cpu(1),encoder=encoder) as service:
        lane=service.open_lane(lambda result:consumed.append((get_ident(),result)))
        lane.submit(b'one',6);lane.submit(b'two',0)
        assert service.slots==0 and service.actual_cpu_slots==1 and service.pending_jobs==0
        assert consumed==[(owner,b'encoded-one'),(owner,b'encoded-two')]
        assert encoded==[owner,owner]
        lane.finish()
    assert budget.used_bytes==0


def test_global_window_retains_completed_leases_and_drains_all_lanes_in_order():
    budget=Budget(256<<20)
    gate,other_done=Event(),Event()
    output=[[],[]];callback_threads=[];owner=get_ident()
    def encoder(payload,level):
        if payload==b'0':assert gate.wait(3)
        if payload==b'5':other_done.set()
        return b'encoded-'+payload
    with CompressionService(budget,cpu(4),encoder=encoder) as service:
        assert service.slots==3 and service.capacity==6
        def consumer(lane):
            def consume(result):
                callback_threads.append(get_ident());output[lane].append(result)
            return consume
        lanes=[service.open_lane(consumer(index)) for index in range(2)]
        for index in range(6):lanes[index%2].submit(str(index).encode())
        assert other_done.wait(3)
        assert service.pending_jobs==service.capacity==6 and output==[[],[]]
        assert sum(lease.owner=='compression-job' for lease in budget._leases)==6
        timer=Timer(0.02,gate.set);timer.start()
        try:lanes[0].submit(b'6')
        finally:gate.set();timer.join()
        assert service.pending_jobs==6 and service.peak_pending==6
        # Finishing a lane must drain earlier jobs on the other lane too.
        lanes[0].finish();lanes[1].finish()
        assert output==[[b'encoded-0',b'encoded-2',b'encoded-4',b'encoded-6'],
                        [b'encoded-1',b'encoded-3',b'encoded-5']]
        assert callback_threads==[owner]*7
        assert service.pending_jobs==0 and service.completed_jobs==service.submitted_jobs==7
        assert all(lease.owner!='compression-job' for lease in budget._leases)
    assert budget.used_bytes==0
    assert not any(thread.name.startswith('readmixer-zlib') for thread in threads())


def test_memory_admission_lowers_threads_and_one_service_per_invocation():
    budget=Budget(16<<20)
    with CompressionService(budget,cpu(32),encoder=encode_member) as service:
        assert service.slots==0 and service.admission_reason=='memory-admission'
        with pytest.raises(ConfigurationError):CompressionService(budget,cpu(4),encoder=encode_member)
        lane=service.open_lane(lambda result:None);lane.submit(b'x');lane.finish()
    assert budget.used_bytes==0
    with pytest.raises(ResourceError):CompressionService(budget,cpu(4),encoder=encode_member,memory_allowance=1)
    assert budget.used_bytes==0


def test_first_failure_and_secondary_notes_are_bounded_and_private():
    state=CancellationState()
    first=ResourceError('first safe failure')
    assert state.fail(first) is first
    for index in range(20):assert state.fail(ValueError('secret payload'),task=index) is first
    assert len(first.__notes__)==8
    assert all('secret payload' not in note for note in first.__notes__)
    with pytest.raises(ResourceError) as observed:state.check()
    assert observed.value is first


@pytest.mark.parametrize('kind',['encoder','callback','malformed-result','unknown-exception'])
def test_worker_and_callback_failure_join_and_preserve_safe_primary(kind):
    budget=Budget(256<<20)
    primary=ResourceError('injected compression failure')
    output=[]
    def encoder(payload,level):
        if kind=='encoder':raise primary
        if kind=='unknown-exception':raise ValueError('private record bytes')
        if kind=='malformed-result':return bytearray(b'bad')
        return b'encoded'
    def consume(result):
        if kind=='callback':raise primary
        output.append(result)
    with pytest.raises((ResourceError,IntegrityError)) as observed:
        with CompressionService(budget,cpu(3),encoder=encoder) as service:
            lane=service.open_lane(consume)
            lane.submit(b'x');lane.finish()
    if kind in ('encoder','callback'):assert observed.value is primary
    assert 'private record bytes' not in str(observed.value)
    assert output==[] and budget.used_bytes==0
    assert not any(thread.name.startswith('readmixer-zlib') for thread in threads())


def test_lane_abort_cancels_siblings_before_any_callback():
    budget=Budget(256<<20);output=[]
    with CompressionService(budget,cpu(3),encoder=lambda data,level:data) as service:
        first=service.open_lane(output.append);second=service.open_lane(output.append)
        first.submit(b'a');second.submit(b'b')
        cause=CancelledError('test cancellation')
        first.abort(cause)
        assert service.cancellation.error is cause
        assert first.closed and second.closed and output==[]
        with pytest.raises(ResourceError):second.submit(b'c')
    assert budget.used_bytes==0


def test_external_cancellation_prevents_completed_result_publication():
    budget=Budget(256<<20);cancelled=[False];output=[]
    with pytest.raises(CancelledError):
        with CompressionService(budget,cpu(3),encoder=lambda data,level:data,
                                cancel=lambda:cancelled[0]) as service:
            lane=service.open_lane(output.append)
            lane.submit(b'encoded')
            cancelled[0]=True
            lane.finish()
    assert output==[] and budget.used_bytes==0


def test_bounded_payloads_lane_registry_and_invalid_levels():
    budget=Budget(32<<20)
    with CompressionService(budget,cpu(1),encoder=lambda data,level:data,max_lanes=1) as service:
        lane=service.open_lane(lambda result:None)
        with pytest.raises(ResourceError):service.open_lane(lambda result:None)
        for payload in (b'',b'x'*65281,bytearray(b'x')):
            with pytest.raises(ResourceError):lane.submit(payload)
        with pytest.raises(ConfigurationError):lane.submit(b'x',10)
        lane.submit(b'x');lane.finish()
        another=service.open_lane(lambda result:None);another.finish()
    assert budget.used_bytes==0


def test_executor_submission_failure_releases_reserved_and_inflight_work(monkeypatch):
    budget=Budget(256<<20)
    with pytest.raises(ResourceError):
        with CompressionService(budget,cpu(3),encoder=encode_member) as service:
            def fail(*args,**kwargs):raise RuntimeError('injected thread creation failure')
            monkeypatch.setattr(service._executor,'submit',fail)
            service.open_lane(lambda result:None).submit(b'x')
    assert budget.used_bytes==0


@pytest.mark.parametrize('fault',['post-pop','pre-append','post-append'])
def test_result_lease_handoff_survives_queue_transition_faults(fault):
    from collections import deque
    budget=Budget(256<<20)
    failure=ResourceError('injected result ownership transition failure')
    class FaultQueue(deque):
        def popleft(self):
            result=super().popleft()
            raise failure
    class FaultList(list):
        def append(self,value):
            if fault=='post-append':super().append(value)
            raise failure
    with pytest.raises(ResourceError) as observed:
        with CompressionService(budget,cpu(2),encoder=lambda data,level:data) as service:
            lane=service.open_lane(lambda result:None)
            lane.submit(b'result')
            if fault=='post-pop':service._pending=FaultQueue(service._pending)
            else:service._available=FaultList(service._available)
            lane.finish()
    assert observed.value is failure
    assert budget.used_bytes==budget.active_leases==0


@pytest.mark.parametrize('exception_type',[KeyboardInterrupt,SystemExit])
def test_coordinator_interrupt_preserves_identity_while_joining_workers(exception_type):
    budget=Budget(256<<20)
    primary=exception_type()
    def consume(result):raise primary
    with pytest.raises(exception_type) as observed:
        with CompressionService(budget,cpu(2),encoder=lambda data,level:data) as service:
            lane=service.open_lane(consume)
            lane.submit(b'result');lane.finish()
    assert observed.value is primary
    assert service.cancellation.error is primary
    assert budget.used_bytes==0
    assert not any(thread.name.startswith('readmixer-zlib') for thread in threads())


def test_worker_interrupt_is_safe_typed_failure():
    budget=Budget(256<<20)
    def worker(data,level):raise KeyboardInterrupt()
    with pytest.raises(ResourceError):
        with CompressionService(budget,cpu(2),encoder=worker) as service:
            lane=service.open_lane(lambda result:None)
            lane.submit(b'result');lane.finish()
    assert budget.used_bytes==0


def test_shutdown_cleanup_failure_does_not_replace_primary_and_can_retry(monkeypatch):
    budget=Budget(256<<20)
    primary=ResourceError('original publication callback failure')
    def consume(result):raise primary
    with pytest.raises(ResourceError) as observed:
        with CompressionService(budget,cpu(2),encoder=lambda data,level:data) as service:
            original=service._executor.shutdown
            failed=[False]
            def shutdown_then_fail(*args,**kwargs):
                result=original(*args,**kwargs)
                if not failed[0]:
                    failed[0]=True
                    raise ResourceError('injected shutdown bookkeeping failure')
                return result
            monkeypatch.setattr(service._executor,'shutdown',shutdown_then_fail)
            lane=service.open_lane(consume)
            lane.submit(b'result');lane.finish()
    assert observed.value is primary
    assert any('compression-cleanup' in note for note in primary.__notes__)
    assert budget.used_bytes==0
