"""Adversarial independent joins and anchored admission of completed tiny pools."""
from contextlib import contextmanager
import hashlib
import json
import os
import struct

import pytest

from readmixer_py.artifacts.manifest import encode_manifest
from readmixer_py.config import MixConfig, StorageMode
from readmixer_py.crypto import OperationKeys
from readmixer_py.errors import IntegrityError, ReadMixerError
from readmixer_py.mixing import _mix
from readmixer_py.paircodec import decode_pair_view
from readmixer_py.pool import Pool
from readmixer_py.resources import Budget
from readmixer_py.scan import PoolScanner


@pytest.fixture
def tiny_pool(tmp_path):
    inputs=tmp_path/'inputs';inputs.mkdir()
    output=tmp_path/'output'
    for sample, count in (('alpha',3),('beta',4)):
        for role in (1,2):
            data=b''.join(f'@{sample}-{ordinal}/{role} '.encode()+b'opaque\xff\nACG\n+opaque\xfe\n!~#\n' for ordinal in range(count))
            (inputs/f'{sample}_R{role}.fastq').write_bytes(data)
    keys=OperationKeys._from_test(bytes(range(32)),bytes(range(16)))
    report=_mix(MixConfig(input_dir=inputs,output=output,max_ram=512<<20,
                          max_record_bytes=1024,storage=StorageMode.MEMORY),_keys=keys)
    manifest=output/report.manifest
    return manifest,json.loads(manifest.read_bytes())


@contextmanager
def admitted(manifest, *, indexes=True):
    budget=Budget(64<<20,descriptors=64)
    with Pool(manifest,budget,metadata_bytes=65536) as pool:
        pool.preflight(indexes=indexes)
        with PoolScanner(pool,max_record_bytes=1024) as scanner:
            yield pool,scanner
    assert budget.used_bytes==budget.used_descriptors==0


def mutate_manifest(path, document):
    path.write_bytes(encode_manifest(document,max_metadata_bytes=65536))
    path.chmod(0o600)


def test_sequential_and_indexed_union_reconstruct_the_same_pairs(tiny_pool):
    manifest, document=tiny_pool
    with admitted(manifest) as (pool,scanner):
        sequential=list(scanner.sequential(prove_greedy=True))
        assert pool.consumed_bytes==len(manifest.read_bytes())+sum(d['bytes'] for b in document['bundles'] for role,d in b.items() if isinstance(d,dict))
    with admitted(manifest) as (_pool,scanner):
        indexed=list(scanner.indexed({0,1}))
    assert indexed==sequential
    assert sorted((decode_pair_view(frame).sample_index,decode_pair_view(frame).original_pair_index) for frame,_tag in indexed)==[(0,i) for i in range(3)]+[(1,i) for i in range(4)]
    with admitted(manifest) as (_pool,scanner):
        selected=list(scanner.indexed({1}))
    assert selected==[(frame,tag) for frame,tag in sequential if decode_pair_view(frame).sample_index==1]


def test_explicit_sequential_mode_never_stats_or_opens_missing_index(tiny_pool,monkeypatch):
    manifest,document=tiny_pool
    index_names={bundle['index']['name'] for bundle in document['bundles']}
    for name in index_names:(manifest.parent/name).unlink()
    real_stat,real_open=os.stat,os.open
    def checked_stat(path,*args,**kwargs):
        assert path not in index_names,'sequential mode statted an index'
        return real_stat(path,*args,**kwargs)
    def checked_open(path,*args,**kwargs):
        assert path not in index_names,'sequential mode opened an index'
        return real_open(path,*args,**kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(os,'stat',checked_stat);patch.setattr(os,'open',checked_open)
        with admitted(manifest,indexes=False) as (_pool,scanner):
            assert len(list(scanner.sequential(indexes=False)))==7
    with pytest.raises(IntegrityError):
        with admitted(manifest):pass


@pytest.mark.parametrize('role',['r1','r2','key','index','r1_gzi','r2_gzi'])
def test_every_full_artifact_digest_is_checked_on_the_consumed_bytes(tiny_pool,role):
    manifest,document=tiny_pool
    document['bundles'][0][role]['sha256']='0'*64
    mutate_manifest(manifest,document)
    with pytest.raises(IntegrityError,match='digest'):
        with admitted(manifest) as (_pool,scanner):list(scanner.sequential())


@pytest.mark.parametrize('lane_offset',[16,24,32])
def test_selfconsistent_index_digest_cannot_bless_wrong_virtual_member_or_line_start(tiny_pool,lane_offset):
    manifest,document=tiny_pool
    desc=document['bundles'][0]['index'];path=manifest.parent/desc['name']
    data=bytearray(path.read_bytes())
    # First physical entry: R1/R2/key offsets start at +16/+24/+32.
    current,=struct.unpack_from('<Q',data,64+lane_offset)
    struct.pack_into('<Q',data,64+lane_offset,current+1)
    path.write_bytes(data);desc['sha256']=hashlib.sha256(data).hexdigest()
    mutate_manifest(manifest,document)
    with pytest.raises(IntegrityError):
        with admitted(manifest) as (_pool,scanner):list(scanner.indexed({0,1}))
    with pytest.raises(IntegrityError):
        with admitted(manifest) as (_pool,scanner):list(scanner.sequential())


def test_manifest_fifo_is_rejected_without_blocking(tmp_path,monkeypatch):
    manifest=tmp_path/('pool.'+'00'*16+'.manifest.json')
    os.mkfifo(manifest,0o600)
    original=os.open
    def guarded(path,flags,*args,**kwargs):
        if path==manifest.name:
            assert flags&os.O_NONBLOCK,'manifest FIFO would block before type validation'
        return original(path,flags,*args,**kwargs)
    monkeypatch.setattr(os,'open',guarded)
    budget=Budget(8<<20)
    with pytest.raises(IntegrityError):Pool(manifest,budget,metadata_bytes=65536)
    assert budget.used_bytes==budget.used_descriptors==0


def test_admitted_artifact_replaced_by_fifo_is_rejected_without_blocking(tiny_pool,monkeypatch):
    manifest,document=tiny_pool
    desc=document['bundles'][0]['r1'];path=manifest.parent/desc['name']
    original=os.open
    def guarded(name,flags,*args,**kwargs):
        if name==desc['name']:
            assert flags&os.O_NONBLOCK,'artifact FIFO would block before identity validation'
        return original(name,flags,*args,**kwargs)
    with admitted(manifest) as (pool,_scanner):
        path.unlink();os.mkfifo(path,0o600)
        with monkeypatch.context() as patch:
            patch.setattr(os,'open',guarded)
            with pytest.raises(IntegrityError):
                with pool.open(desc):pass


@pytest.mark.parametrize('replacement',['mode','inode','symlink'])
def test_artifact_identity_and_private_mode_are_rechecked_at_open(tiny_pool,replacement):
    manifest,document=tiny_pool
    desc=document['bundles'][0]['r1'];path=manifest.parent/desc['name']
    with admitted(manifest) as (pool,_scanner):
        if replacement=='mode':path.chmod(0o644)
        else:
            saved=path.with_suffix('.saved');path.rename(saved)
            if replacement=='inode':path.write_bytes(saved.read_bytes());path.chmod(0o600)
            else:path.symlink_to(saved)
        with pytest.raises(IntegrityError):
            with pool.open(desc):pass


def test_missing_manifest_has_a_typed_failure(tmp_path):
    budget=Budget(8<<20)
    with pytest.raises(ReadMixerError):Pool(tmp_path/('pool.'+'00'*16+'.manifest.json'),budget,metadata_bytes=65536)
    assert budget.used_bytes==budget.used_descriptors==0


@pytest.mark.parametrize('role',['index','r1_gzi','r2_gzi'])
def test_indexed_selection_checks_full_index_and_sidecar_hashes(tiny_pool,role):
    manifest,document=tiny_pool
    document['bundles'][0][role]['sha256']='0'*64
    mutate_manifest(manifest,document)
    with pytest.raises(IntegrityError,match='digest'):
        with admitted(manifest) as (_pool,scanner):list(scanner.indexed({1}))


def test_unselected_index_entries_are_still_structurally_validated(tiny_pool):
    manifest,document=tiny_pool
    desc=document['bundles'][0]['index'];path=manifest.parent/desc['name']
    data=bytearray(path.read_bytes())
    # A bogus sample is unselected, but must be rejected before filtering.
    struct.pack_into('<I',data,64,2)
    path.write_bytes(data);desc['sha256']=hashlib.sha256(data).hexdigest()
    mutate_manifest(manifest,document)
    with pytest.raises(IntegrityError):
        with admitted(manifest) as (_pool,scanner):list(scanner.indexed({1}))


def test_artifact_replacement_during_open_read_invalidates_success(tiny_pool):
    manifest,document=tiny_pool
    desc=document['bundles'][0]['r1'];path=manifest.parent/desc['name']
    with admitted(manifest) as (pool,_scanner):
        with pytest.raises(IntegrityError):
            with pool.open(desc) as stream:
                assert len(stream.read(desc['bytes']))==desc['bytes']
                saved=path.with_suffix('.original');path.rename(saved)
                path.write_bytes(saved.read_bytes());path.chmod(0o600)


def test_selfconsistent_sidecar_and_index_cannot_invent_compressed_member_boundaries(tmp_path):
    inputs=tmp_path/'inputs';inputs.mkdir();output=tmp_path/'output'
    for sample in ('alpha','beta'):
        for role in (1,2):
            payload=b''.join(f'@{sample}-{ordinal}/{role}\nACGT\n+\n!!!!\n'.encode() for ordinal in range(400))
            (inputs/f'{sample}_R{role}.fastq').write_bytes(payload)
    result=_mix(MixConfig(input_dir=inputs,output=output,max_ram=512<<20,max_record_bytes=1024,
                          storage=StorageMode.MEMORY),_keys=OperationKeys._from_test(bytes(range(32)),bytes(range(16))))
    manifest=output/result.manifest;document=json.loads(manifest.read_bytes());bundle=document['bundles'][0]
    side_desc=bundle['r1_gzi'];side_path=output/side_desc['name'];side=bytearray(side_path.read_bytes())
    assert struct.unpack_from('<Q',side)[0]>=1
    real_start,=struct.unpack_from('<Q',side,8)
    struct.pack_into('<Q',side,8,real_start+1)
    side_path.write_bytes(side);side_desc['sha256']=hashlib.sha256(side).hexdigest()
    index_desc=bundle['index'];index_path=output/index_desc['name'];index=bytearray(index_path.read_bytes())
    changed=0
    for offset in range(64,len(index),40):
        vo,=struct.unpack_from('<Q',index,offset+16)
        if vo>>16==real_start:
            struct.pack_into('<Q',index,offset+16,vo+(1<<16));changed+=1
    assert changed
    index_path.write_bytes(index);index_desc['sha256']=hashlib.sha256(index).hexdigest()
    mutate_manifest(manifest,document)
    for indexed in (False,True):
        with pytest.raises(IntegrityError):
            with admitted(manifest) as (_pool,scanner):
                list(scanner.indexed({0,1}) if indexed else scanner.sequential())
