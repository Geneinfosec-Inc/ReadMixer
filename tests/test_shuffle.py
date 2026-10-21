"""Independent transcript/oracle checks for canonical serial paired shuffling."""
from contextlib import contextmanager
from dataclasses import replace
import hashlib
import hmac
import io
import struct

import numpy as np
import pytest

from readmixer_py import shuffle
from readmixer_py.crypto import OperationKeys
from readmixer_py.errors import CancelledError, ConfigurationError, IntegrityError, RankCollisionError, ResourceError
from readmixer_py.fastq import PairedReader, RecordFramer
from readmixer_py.paircodec import decode_pair_view
from readmixer_py.resources import Budget
from readmixer_py.shuffle import SerialShuffler, bucket_index, refinement_fanout, terminal_permutation
from readmixer_py.storage import SegmentStore
from readmixer_py.transaction import OutputTransaction

ROOT, RUN_ID = bytes(range(32)), bytes(range(16))


def oracle_derive(key, domain, fields):
    encoded = domain.encode('ascii')
    transcript = (b'RMPY1\0' + len(encoded).to_bytes(2, 'big') + encoded
                  + len(fields).to_bytes(2, 'big')
                  + b''.join(len(field).to_bytes(4, 'big') + field for field in fields))
    extracted = hmac.digest(b'readmixer-python/schema1/hkdf-sha256', key, 'sha256')
    return hmac.digest(extracted, transcript + b'\1', 'sha256')


def oracle_chacha(key, length):
    """Independent RFC-style 20-round implementation with initial counter zero."""
    def rotate(value, bits):
        return ((value << bits) | (value >> (32 - bits))) & 0xffffffff
    def quarter(state, a, b, c, d):
        state[a] = (state[a] + state[b]) & 0xffffffff
        state[d] = rotate(state[d] ^ state[a], 16)
        state[c] = (state[c] + state[d]) & 0xffffffff
        state[b] = rotate(state[b] ^ state[c], 12)
        state[a] = (state[a] + state[b]) & 0xffffffff
        state[d] = rotate(state[d] ^ state[a], 8)
        state[c] = (state[c] + state[d]) & 0xffffffff
        state[b] = rotate(state[b] ^ state[c], 7)
    result = bytearray()
    for counter in range((length + 63) // 64):
        initial = list(struct.unpack('<4I', b'expand 32-byte k')) + list(struct.unpack('<8I', key)) + [counter, 0, 0, 0]
        state = initial.copy()
        for _ in range(10):
            for indices in ((0,4,8,12), (1,5,9,13), (2,6,10,14), (3,7,11,15),
                            (0,5,10,15), (1,6,11,12), (2,7,8,13), (3,4,9,14)):
                quarter(state, *indices)
        result.extend(struct.pack('<16I', *((a+b) & 0xffffffff for a,b in zip(state, initial))))
    return bytes(result[:length])


def oracle_bucket(identity, path=(), fanout=64):
    root = oracle_derive(ROOT, 'root/shuffle', [])
    key = oracle_derive(root, 'shuffle/bucket', [bytes((len(path), *path))])
    sample, ordinal = identity
    digest = hashlib.blake2b(sample.to_bytes(4, 'big') + ordinal.to_bytes(8, 'big'), key=key, digest_size=8).digest()
    return int.from_bytes(digest, 'little') & (fanout - 1)


def oracle_terminal(identities, path):
    identities = sorted(identities)
    n = len(identities)
    rank_root = oracle_derive(ROOT, 'root/rank', [])
    key = oracle_derive(rank_root, 'shuffle/rank', [bytes((len(path), *path)), n.to_bytes(8, 'big'), b'\0'])
    data = oracle_chacha(key, 16 * n)
    high = struct.unpack('<' + 'Q' * n, data[:8*n])
    low = struct.unpack('<' + 'Q' * n, data[8*n:])
    return [identities[index] for index in sorted(range(n), key=lambda index: (high[index], low[index]))]


def oracle_order(counts, *, max_pairs=32768):
    groups = [[] for _ in range(64)]
    for sample, count in enumerate(counts):
        for ordinal in range(count):
            identity = sample, ordinal
            groups[oracle_bucket(identity)].append(identity)
    result = []
    def visit(identities, path):
        if len(identities) <= max_pairs:
            result.extend(oracle_terminal(identities, path))
            return
        desired = max(2, (len(identities) + max_pairs - 1) // max_pairs)
        fanout = min(64, 1 << (desired - 1).bit_length())
        children = [[] for _ in range(fanout)]
        for identity in identities:
            children[oracle_bucket(identity, path, fanout)].append(identity)
        for child, members in enumerate(children):
            if members:
                visit(members, (*path, child))
    for bucket, members in enumerate(groups):
        if members:
            visit(members, (bucket,))
    return result


def frame(sample, ordinal, *, annotation=b'opaque\xff'):
    base = f'{sample}.{ordinal}'.encode('ascii')
    lines = (b'@'+base+b'/1 '+annotation, b'A\0', b'+meta\xfe', b'!\r',
             b'@'+base+b'/2 '+annotation, b'G\0', b'+other\xff', b'#\r')
    return struct.pack('<IIQ8I', 48+sum(map(len,lines)), sample, ordinal, *map(len,lines)) + b''.join(lines)


@contextmanager
def store_at(path, mode='external', *, budget_bytes=512<<20, root_key=ROOT):
    budget = Budget(budget_bytes, descriptors=128)
    with SegmentStore(mode, budget, OperationKeys._from_test(root_key, RUN_ID), temp_dir=path) as store:
        yield store
    assert budget.used_bytes == budget.used_descriptors == 0
    assert not list(path.iterdir())


def make_shuffle(store, **kwargs):
    defaults = dict(terminal_bytes=128<<20, max_record_bytes=128, sort_run_bytes=8192)
    defaults.update(kwargs)
    return SerialShuffler(store, **defaults)


@pytest.mark.parametrize('fanout', [2,4,8,16,32,64])
def test_exact_bucket_transcript_and_all_fanouts(fanout):
    keys = OperationKeys._from_test(ROOT, RUN_ID)
    identity = 0x12345678, 0x0102030405060708
    expected = oracle_bucket(identity, (1,63), fanout)
    assert bucket_index(keys.bucket_key((1,63)), *identity, fanout) == expected
    digest = hashlib.blake2b(bytes.fromhex('123456780102030405060708'),
                             key=keys.bucket_key((1,63)), digest_size=8).digest()
    assert digest.hex() == '5cd44346f6853c79'
    assert int.from_bytes(digest, 'little') & (fanout-1) == expected
    assert digest != hashlib.blake2b(bytes.fromhex('123456780102030405060708'),
                                     key=keys.bucket_key((1,63)), digest_size=64).digest()[:8]


@pytest.mark.parametrize('fanout', [0,1,3,65,128,True])
def test_invalid_fanout_rejected(fanout):
    with pytest.raises(ConfigurationError):
        bucket_index(bytes(32), 0, 0, fanout)


@pytest.mark.parametrize('chunk', [1,7,64,65536])
def test_rank_stream_rows_endian_and_native_sort_match_independent_chacha(chunk):
    keys = OperationKeys._from_test(ROOT,RUN_ID)
    identities = [(0,index) for index in range(19)]
    expected = oracle_terminal(identities,(1,63))
    indices = terminal_permutation(keys,(1,63),19,_chunk_bytes=chunk,_collision_window=2)
    assert indices.dtype.kind in 'iu'
    assert [(0,int(index)) for index in indices] == expected


def test_full_rank_collision_retry_attempt_seven_and_cross_window(monkeypatch):
    calls = []
    keys = OperationKeys._from_test(ROOT,RUN_ID)
    def injected(destination,key,**kwargs):
        calls.append(key)
        # The duplicate at sorted positions1 and2 straddles a two-record window.
        high = [0,1,1,3,4] if len(calls)<8 else [1]*5
        low = [0,2,2,0,0] if len(calls)<8 else [4,3,2,1,0]
        destination[:] = struct.pack('<10Q',*(high+low))
    monkeypatch.setattr(shuffle,'_fill_rank_bytes',injected)
    result = terminal_permutation(keys,(2,),5,_collision_window=2)
    assert tuple(map(int,result)) == (4,3,2,1,0)
    assert calls == [keys.rank_key((2,),5,attempt) for attempt in range(8)]


def test_eight_collisions_fail_without_input_order_tiebreak(monkeypatch):
    calls=[]
    def colliding(destination,key,**kwargs):
        calls.append(key)
        destination[:] = bytes(len(destination))
    monkeypatch.setattr(shuffle,'_fill_rank_bytes',colliding)
    with pytest.raises(RankCollisionError):
        terminal_permutation(OperationKeys._from_test(ROOT,RUN_ID),(63,),7)
    assert len(calls)==8 and len(set(calls))==8


@pytest.mark.parametrize('mode',['memory','external','auto'])
def test_exact_independent_global_order_payload_and_counts_all_backends(tmp_path,mode):
    counts=(37,0,63)
    expected=oracle_order(counts)
    with store_at(tmp_path,mode) as store:
        with make_shuffle(store) as operation:
            # Deliberately reverse arrival order: source identity controls the
            # merge, not worker or record scheduling.
            for sample in reversed(range(len(counts))):
                for ordinal in reversed(range(counts[sample])):
                    operation.append_frame(frame(sample,ordinal))
            with operation.iter_ordered(counts) as ordered:
                observed=[]
                for position,pair in enumerate(ordered):
                    identity=pair.sample_index,pair.original_pair_index
                    assert pair.secure_pair_index==position
                    assert bytes(pair.frame)==frame(*identity)
                    decoded=decode_pair_view(pair.frame,max_record_bytes=128)
                    assert bytes(decoded.r1.quality)==b'!\r'
                    observed.append(identity)
            assert observed==expected
            assert operation.pairs==operation.emitted_pairs==100
            assert operation.emitted_mates==200
            assert operation.frame_bytes==sum(len(frame(*identity)) for identity in expected)
            assert operation.terminal_count>=32 and operation.max_depth==1
            assert operation.refinements==0


@pytest.mark.parametrize('mode',['memory','external','auto'])
def test_recursive_capacity_refinement_matches_independent_oracle(tmp_path,mode):
    counts=(73,41)
    expected=oracle_order(counts,max_pairs=2)
    with store_at(tmp_path,mode) as store:
        with make_shuffle(store) as operation:
            operation._limits=replace(operation._limits,pairs=2)
            for sample,count in enumerate(counts):
                for ordinal in range(count):operation.append_frame(frame(sample,ordinal))
            with operation.iter_ordered(counts) as pairs:
                observed=[(pair.sample_index,pair.original_pair_index) for pair in pairs]
            assert observed==expected
            assert operation.refinements>0 and 1<operation.max_depth<=32


def test_refinement_fanout_integer_ceiling_and_capacity_edges():
    allowance=128<<20
    assert refinement_fanout(32768,64<<20,allowance)==2
    assert refinement_fanout(32769,64<<20,allowance)==2
    assert refinement_fanout(32768*3,64<<20,allowance)==4
    assert refinement_fanout(32768*100,64<<20,allowance)==64
    assert refinement_fanout(1,64*(1<<20)+1,allowance)==2
    with pytest.raises(ResourceError):refinement_fanout(1,1,32<<20)


def test_depth32_failure_has_no_overflow_fallback_and_cleans(tmp_path,monkeypatch):
    monkeypatch.setattr(shuffle,'bucket_index',lambda *args:0)
    with store_at(tmp_path) as store:
        with make_shuffle(store) as operation:
            operation._limits=replace(operation._limits,pairs=1)
            operation.append_frame(frame(0,0));operation.append_frame(frame(1,0))
            with pytest.raises(ResourceError,match='depth'):
                with operation.iter_ordered((1,1)) as pairs:list(pairs)
            assert operation.refinements==31
            assert operation.emitted_pairs==0


@pytest.mark.parametrize('fault',['duplicate','missing','range'])
def test_identity_coverage_failure_leaves_no_owned_scratch(tmp_path,fault):
    with store_at(tmp_path) as store:
        with make_shuffle(store) as operation:
            operation.append_frame(frame(0,0))
            operation.append_frame(frame(0,0) if fault=='duplicate' else frame(1,1 if fault=='range' else 0))
            counts=(2,1) if fault=='missing' else (1,1)
            with pytest.raises(IntegrityError):
                with operation.iter_ordered(counts) as pairs:list(pairs)


def test_parser_pair_handoff_and_empty_sample_counts(tmp_path):
    with store_at(tmp_path,'memory') as store:
        with make_shuffle(store) as operation:
            left=RecordFramer(io.BytesIO(b'@id/1\r\nA\r\n+opaque\xff\r\n!\r\n'),line_mode='ingress',max_record_bytes=128)
            right=RecordFramer(io.BytesIO(b'@id/2\nC\n+opaque\xfe\n#\n'),line_mode='ingress',max_record_bytes=128)
            operation.append_pair(PairedReader(left,right,sample_index=1).next_pair())
            with operation.iter_ordered((0,1,0)) as pairs:
                pair=next(pairs)
                view=decode_pair_view(pair.frame,max_record_bytes=128)
                assert b'\n'.join(bytes(line) for line in view.r1.lines)+b'\n'==b'@id/1\nA\n+opaque\xff\n!\n'


def test_cancellation_before_and_after_native_sort(tmp_path,monkeypatch):
    cancelled=[False]
    original=np.lexsort
    def cancel_after(*args,**kwargs):
        result=original(*args,**kwargs)
        cancelled[0]=True
        return result
    with store_at(tmp_path) as store:
        with make_shuffle(store,cancel=lambda:cancelled[0]) as operation:
            for sample in range(2):
                for ordinal in range(10):operation.append_frame(frame(sample,ordinal))
            monkeypatch.setattr(shuffle.np,'lexsort',cancel_after)
            with pytest.raises(CancelledError):
                with operation.iter_ordered((10,10)) as pairs:list(pairs)
            assert operation.emitted_pairs==0


def test_rank_failure_aborts_staged_output_before_completion(tmp_path,monkeypatch):
    output,scratch=tmp_path/'output',tmp_path/'scratch'
    output.mkdir(mode=0o700);scratch.mkdir(mode=0o700)
    monkeypatch.setattr(shuffle,'bucket_index',lambda *args:0)
    monkeypatch.setattr(shuffle,'_fill_rank_bytes',lambda destination,key,**kwargs:destination.__setitem__(slice(None),bytes(len(destination))))
    budget=Budget(512<<20,descriptors=128)
    with pytest.raises(RankCollisionError):
        with OutputTransaction.begin(output,'manifest',budget) as transaction:
            artifact=transaction.create_partial('pairs.bin')
            with SegmentStore('external',budget,OperationKeys._from_test(ROOT,RUN_ID),temp_dir=scratch) as store:
                with make_shuffle(store) as operation:
                    operation.append_frame(frame(0,0));operation.append_frame(frame(1,0))
                    with operation.iter_ordered((1,1)) as pairs:
                        for pair in pairs:artifact.write(pair.frame)
            artifact.close()
            transaction.publish_all(b'test completion')
    assert not list(output.iterdir()) and not list(scratch.iterdir())
    assert budget.used_bytes==budget.used_descriptors==0


def test_AUD02_exact_100000_pair_chunk_boundary_fixture(tmp_path):
    # Public Plan10 fixture material, never a production entropy shortcut.
    root=hashlib.sha256(b'readmixer-python/shuffle-test/v1/0').digest()
    counts=(50000,50000)
    with store_at(tmp_path,root_key=root) as store:
        with make_shuffle(store,sort_run_bytes=4<<20) as operation:
            for sample,count in enumerate(counts):
                for ordinal in range(count):operation.append_frame(frame(sample,ordinal,annotation=b'x'))
            observed=[]
            with operation.iter_ordered(counts) as pairs:
                for pair in pairs:observed.append((pair.sample_index,pair.original_pair_index))
            assert len(observed)==100000==len(set(observed))
            crossed=sum(position//32768 != (sample*50000+ordinal)//32768
                        for position,(sample,ordinal) in enumerate(observed))
            assert crossed>=50000
            assert operation.terminal_count==64 and operation.max_depth==1


def test_plan10_fixed32root_4096_pair_distribution_fixture(tmp_path):
    transitions=0
    for counter in range(32):
        root=hashlib.sha256(f'readmixer-python/shuffle-test/v1/{counter}'.encode('ascii')).digest()
        directory=tmp_path/str(counter)
        directory.mkdir(mode=0o700)
        with store_at(directory,'memory',root_key=root) as store:
            with make_shuffle(store,sort_run_bytes=4<<20) as operation:
                for sample in range(2):
                    for ordinal in range(2048):operation.append_frame(frame(sample,ordinal,annotation=b'x'))
                labels=bytearray()
                with operation.iter_ordered((2048,2048)) as pairs:
                    for pair in pairs:labels.append(pair.sample_index)
                assert len(labels)==4096 and labels.count(0)==labels.count(1)==2048
                assert 256<=labels[:1024].count(0)<=768
                transitions+=sum(left!=right for left,right in zip(labels,labels[1:]))
    assert 60000<=transitions<=71000


def test_terminal_exact_and_one_over_each_capacity_constraint(tmp_path):
    from types import SimpleNamespace
    with store_at(tmp_path) as store:
        with make_shuffle(store) as operation:
            def shard(count, size):
                return SimpleNamespace(ref=SimpleNamespace(item_count=count,decoded_bytes=size))
            assert operation._fits(shard(32768,64<<20))
            assert not operation._fits(shard(32769,64<<20))
            assert not operation._fits(shard(1,(64<<20)+1))
            operation.terminal_bytes=64<<20
            assert operation._fits(shard(1,(32<<20)-128))
            assert not operation._fits(shard(1,(32<<20)-127))


def test_corruption_after_real_bucket_writes_prevents_terminal_emission(tmp_path,monkeypatch):
    original=SerialShuffler._terminal
    def corrupt(self,shard):
        path=self.store.scratch_directory/self.store._name(shard.ref)
        with path.open('r+b') as stream:
            stream.seek(-1,2)
            previous=stream.read(1)
            stream.seek(-1,2)
            stream.write(bytes([previous[0]^1]))
        yield from original(self,shard)
    with store_at(tmp_path) as store:
        with make_shuffle(store) as operation:
            for sample in range(2):
                for ordinal in range(10):operation.append_frame(frame(sample,ordinal))
            monkeypatch.setattr(SerialShuffler,'_terminal',corrupt)
            with pytest.raises(IntegrityError):
                with operation.iter_ordered((10,10)) as pairs:list(pairs)
            assert operation.emitted_pairs==0


def test_cancellation_before_sort_and_during_output(tmp_path,monkeypatch):
    with pytest.raises(CancelledError):
        terminal_permutation(OperationKeys._from_test(ROOT,RUN_ID),(0,),2,cancel=lambda:True)
    cancelled=[False]
    with store_at(tmp_path) as store:
        with make_shuffle(store,cancel=lambda:cancelled[0]) as operation:
            for sample in range(2):
                for ordinal in range(10):operation.append_frame(frame(sample,ordinal))
            with pytest.raises(CancelledError):
                with operation.iter_ordered((10,10)) as pairs:
                    next(pairs)
                    cancelled[0]=True
                    next(pairs)
            assert operation.emitted_pairs==1


def test_strict_memory_registry_exhaustion_fails_and_cleans(tmp_path):
    budget=Budget(512<<20)
    with SegmentStore('memory',budget,OperationKeys._from_test(ROOT,RUN_ID),
                      temp_dir=tmp_path,max_segments=2) as store:
        with make_shuffle(store) as operation:
            operation.append_frame(frame(0,0));operation.append_frame(frame(1,0))
            with pytest.raises(ResourceError,match='registry'):
                with operation.iter_ordered((1,1)) as pairs:list(pairs)
        assert store.scratch_directory is None
    assert budget.used_bytes==0 and not list(tmp_path.iterdir())


@pytest.mark.parametrize('mode',['memory','external','auto'])
@pytest.mark.parametrize('stage',['after-frontier-commit','after-parent-drop'])
def test_shuffle_ownership_transition_faults_remain_cleanup_safe(tmp_path,monkeypatch,mode,stage):
    failed=[False]
    if stage=='after-frontier-commit':
        constructor=shuffle._Frontier.__init__
        def frontier_init(self,*args,**kwargs):
            constructor(self,*args,**kwargs)
            original=self.ledger.flush
            def committed_then_fail():
                result=original()
                if self.ledger.root.record_count and not failed[0]:
                    failed[0]=True
                    raise ResourceError('injected failure after shard registry commit')
                return result
            self.ledger.flush=committed_then_fail
        monkeypatch.setattr(shuffle._Frontier,'__init__',frontier_init)
    with store_at(tmp_path,mode) as store:
        if stage=='after-parent-drop':
            original=store.drop
            def drop_then_fail(ref,**kwargs):
                result=original(ref,**kwargs)
                if ref.purpose==1 and not failed[0]:
                    failed[0]=True
                    raise ResourceError('injected failure after parent deletion')
                return result
            monkeypatch.setattr(store,'drop',drop_then_fail)
        with make_shuffle(store) as operation:
            for sample in range(2):
                for ordinal in range(5):operation.append_frame(frame(sample,ordinal))
            with pytest.raises(ResourceError,match='injected'):
                with operation.iter_ordered((5,5)) as pairs:list(pairs)
        assert failed[0] and store.resident_segments==0
