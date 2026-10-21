"""Canonical BGZF protocol fixtures, hostile bytes, HTSlib and sidecar oracles."""
import gzip
import hashlib
import io
import os
from pathlib import Path
import random
import shutil
import struct
import subprocess
import zlib

import pytest

from readmixer_py.artifacts.bgzf import (
    BgzfReader, BgzfWriter, EOF_MEMBER, GziCursor, PAYLOAD_BYTES,
    build_gzi, encode_member, split_virtual_offset, virtual_offset,
)
from readmixer_py.errors import CancelledError, ConfigurationError, IntegrityError, ResourceError
from readmixer_py.resources import Budget


def produce(payload, level=6):
    sink = io.BytesIO()
    with BgzfWriter(sink, compression_level=level) as writer:
        for offset in range(0, len(payload), 971):
            writer.write(payload[offset:offset+971])
        stats = writer.finish()
    return sink.getvalue(), stats


def read_all(encoded, decoded_size, **kwargs):
    with BgzfReader(io.BytesIO(encoded), physical_limit=len(encoded),
                    uncompressed_limit=decoded_size, **kwargs) as reader:
        result = bytearray()
        while data := reader.read(1777):
            result.extend(data)
        stats = reader.stats
    return bytes(result), stats


@pytest.mark.parametrize('level', [0, 1, 6, 9])
@pytest.mark.parametrize('length', [0, 1, 65279, 65280, 65281, 130560, 200003])
def test_bounded_members_match_independent_gzip_and_raw_deflate(level, length):
    payload = random.Random(238 + length).randbytes(length)
    encoded, stats = produce(payload, level)
    assert gzip.decompress(encoded) == payload
    assert stats.sha256 == hashlib.sha256(encoded).hexdigest()
    assert stats.uncompressed_bytes == length
    assert stats.data_members == (length+65279)//65280
    assert encoded.endswith(EOF_MEMBER) and len(EOF_MEMBER) == 28
    position = decoded_position = 0
    while position < len(encoded)-28:
        assert encoded[position:position+16] == bytes.fromhex('1f8b08040000000000ff060042430200')
        physical = struct.unpack_from('<H', encoded, position+16)[0]+1
        assert physical <= 65536
        member = encoded[position:position+physical]
        decoded = zlib.decompress(member[18:-8], -15)
        assert decoded == payload[decoded_position:decoded_position+len(decoded)]
        assert len(decoded) == min(65280, length-decoded_position)
        assert struct.unpack('<II', member[-8:]) == (zlib.crc32(decoded), len(decoded))
        decoded_position += len(decoded)
        position += physical
    assert position == len(encoded)-28
    restored, observed = read_all(encoded, length)
    assert restored == payload and observed == stats


def test_empty_exact_golden_and_hardcoded_single_member():
    encoded, stats = produce(b'')
    assert encoded.hex() == '1f8b08040000000000ff0600424302001b0003000000000000000000'
    assert stats.physical_bytes == 28 and stats.data_members == 0
    # Independent stored-DEFLATE member: final stored block, LEN=3, NLEN=65532.
    header = bytes.fromhex('1f8b08040000000000ff060042430200') + struct.pack('<H', 33)
    golden = header + b'\x01\x03\x00\xfc\xffabc' + struct.pack('<II', 0x352441c2, 3)
    assert encode_member(b'abc', 0) == golden
    assert read_all(golden+EOF_MEMBER, 3)[0] == b'abc'


@pytest.mark.parametrize('offset', range(31))
def test_data_member_every_byte_corruption_rejected(offset):
    encoded, _ = produce(b'abc')
    assert len(encoded) == 59
    changed = bytearray(encoded)
    changed[offset] ^= 1
    with pytest.raises(IntegrityError):
        read_all(bytes(changed), 3)


@pytest.mark.parametrize('offset', range(28))
def test_eof_every_byte_corruption_rejected(offset):
    changed = bytearray(EOF_MEMBER)
    changed[offset] ^= 1
    with pytest.raises(IntegrityError):
        read_all(bytes(changed), 0)


@pytest.mark.parametrize('kind', ['missing-eof','trailing','duplicate-eof','short-nonterminal','oversized-isize','deflate-trailing','truncated'])
def test_hostile_stream_shape_rejected(kind):
    member = encode_member(b'abc')
    encoded = member+EOF_MEMBER
    if kind == 'missing-eof': encoded = member
    elif kind == 'trailing': encoded += b'x'
    elif kind == 'duplicate-eof': encoded += EOF_MEMBER
    elif kind == 'short-nonterminal': encoded = member+member+EOF_MEMBER
    elif kind == 'oversized-isize': encoded = member[:-4]+struct.pack('<I',65281)+EOF_MEMBER
    elif kind == 'deflate-trailing':
        bad = member[:-8]+b'junk'+member[-8:]
        encoded = bad[:16]+struct.pack('<H',len(bad)-1)+bad[18:]+EOF_MEMBER
    else: encoded = encoded[:-3]
    with pytest.raises(IntegrityError):
        read_all(encoded, 100000)


@pytest.mark.parametrize('length', [0, 1, 65280, 65281, 200000])
def test_standard_gzi_count_and_monotonic_conversion(length):
    payload = b'x' * length
    encoded, stats = produce(payload)
    output = io.BytesIO()
    result = build_gzi(output, io.BytesIO(encoded), uncompressed_bytes=length, physical_bytes=len(encoded))
    sidecar = output.getvalue()
    assert struct.unpack_from('<Q', sidecar)[0] == max(stats.data_members-1,0)
    assert result.entries == max(stats.data_members-1,0)
    assert result.bytes == 8+16*result.entries
    assert result.sha256 == hashlib.sha256(sidecar).hexdigest()
    # Independently walk BSIZE and reconstruct every .gzi entry.
    expected=[]
    position=decoded=0
    while position < len(encoded)-28:
        if position: expected.append((position,decoded))
        size=struct.unpack_from('<H',encoded,position+16)[0]+1
        decoded += struct.unpack_from('<I',encoded,position+size-4)[0]
        position += size
    assert sidecar == struct.pack('<Q',len(expected))+b''.join(struct.pack('<QQ',*entry) for entry in expected)
    with GziCursor(io.BytesIO(sidecar), uncompressed_bytes=length, physical_bytes=len(encoded)) as cursor:
        for offset in sorted(set([0, length-1, 65280]) & set(range(length))):
            start=0
            for compressed, uncompressed in expected:
                if uncompressed <= offset:start=compressed
            assert split_virtual_offset(cursor.virtual_offset(offset))[0] == start
    with GziCursor(io.BytesIO(sidecar), uncompressed_bytes=length, physical_bytes=len(encoded)) as cursor:
        read_all(encoded,length,on_member=cursor.observe_member)


@pytest.mark.parametrize('kind', ['count','trailing','truncated','nonmonotonic','uncompressed','compressed'])
def test_hostile_gzi_rejected(kind):
    encoded, stats = produce(b'x'*(3*PAYLOAD_BYTES+1))
    side=io.BytesIO()
    build_gzi(side,io.BytesIO(encoded),uncompressed_bytes=stats.uncompressed_bytes,physical_bytes=len(encoded))
    data=bytearray(side.getvalue())
    if kind=='count':struct.pack_into('<Q',data,0,1<<63)
    elif kind=='trailing':data+=b'x'
    elif kind=='truncated':data.pop()
    elif kind=='nonmonotonic':struct.pack_into('<Q',data,24,0)
    elif kind=='uncompressed':struct.pack_into('<Q',data,16,65279)
    else:struct.pack_into('<Q',data,8,len(encoded)-28)
    with pytest.raises(IntegrityError):
        with GziCursor(io.BytesIO(data), uncompressed_bytes=stats.uncompressed_bytes, physical_bytes=len(encoded)) as cursor:
            cursor.finish()


def test_indexed_records_cross_members_and_preserve_lone_cr():
    record1=b'@first/1\n'+b'A'*40000+b'\n+\n'+b'!'*40000+b'\n'
    record2=b'@second/1\nC\n+opaque\n\r\n'
    payload=record1+record2
    encoded, stats=produce(payload)
    side=io.BytesIO();build_gzi(side,io.BytesIO(encoded),uncompressed_bytes=len(payload),physical_bytes=len(encoded))
    with GziCursor(io.BytesIO(side.getvalue()),uncompressed_bytes=len(payload),physical_bytes=len(encoded)) as cursor:
        offsets=[cursor.virtual_offset(0),cursor.virtual_offset(len(record1))]
    with BgzfReader(io.BytesIO(encoded),physical_limit=len(encoded),uncompressed_limit=len(payload),indexed=True) as reader:
        assert reader.read_record_at(offsets[0],len(record1))==record1
        assert reader.read_record_at(offsets[1],len(record2))==record2
        assert reader.stats.sha256 is None
        with pytest.raises(IntegrityError):reader.seek_virtual(offsets[0])


def test_indexed_seeks_prove_boundary_and_line_start_without_reading_unselected_payload():
    payload=(b'x'*65279+b'\n')*8
    encoded,stats=produce(payload,0)
    side=io.BytesIO();build_gzi(side,io.BytesIO(encoded),uncompressed_bytes=len(payload),physical_bytes=len(encoded))
    with GziCursor(io.BytesIO(side.getvalue()),uncompressed_bytes=len(payload),physical_bytes=len(encoded)) as cursor:
        target=cursor.virtual_offset(6*PAYLOAD_BYTES)
    with BgzfReader(io.BytesIO(encoded),physical_limit=len(encoded),uncompressed_limit=len(payload),indexed=True) as reader:
        assert reader.readline_at(target,PAYLOAD_BYTES)==payload[:PAYLOAD_BYTES]
        assert reader.bytes_read < len(encoded)//2
        assert reader.stats.sha256 is None
    for invalid in (1, virtual_offset(5,0), virtual_offset(0,65280), virtual_offset(len(encoded)-28,0), virtual_offset(len(encoded),0)):
        with BgzfReader(io.BytesIO(encoded),physical_limit=len(encoded),uncompressed_limit=len(payload),indexed=True) as reader:
            with pytest.raises(IntegrityError):reader.seek_virtual(invalid)


def test_member_boundary_requires_preceding_lf_and_full_gzi_observation_matches():
    encoded,stats=produce(b'x'*(PAYLOAD_BYTES+1))
    second=struct.unpack_from('<H',encoded,16)[0]+1
    with BgzfReader(io.BytesIO(encoded),physical_limit=len(encoded),uncompressed_limit=stats.uncompressed_bytes,indexed=True) as reader:
        with pytest.raises(IntegrityError,match='text line'):reader.seek_virtual(virtual_offset(second,0))
    wrong=struct.pack('<QQQ',1,second+1,PAYLOAD_BYTES)
    with pytest.raises(IntegrityError,match='boundaries'):
        with GziCursor(io.BytesIO(wrong),uncompressed_bytes=stats.uncompressed_bytes,physical_bytes=len(encoded)) as cursor:
            read_all(encoded,stats.uncompressed_bytes,on_member=cursor.observe_member)


def test_line_record_limits_and_missing_terminators():
    for payload in (b'@id\nA\n+\n!', b'@id\nAA\n+\n!\n'):
        encoded,_=produce(payload)
        with BgzfReader(io.BytesIO(encoded),physical_limit=len(encoded),uncompressed_limit=len(payload)) as reader:
            with pytest.raises(IntegrityError):reader.read_record(100)
    encoded,_=produce(b'x'*100+b'\n')
    with BgzfReader(io.BytesIO(encoded),physical_limit=len(encoded),uncompressed_limit=101) as reader:
        with pytest.raises(ResourceError):reader.readline(100)
        with pytest.raises(ResourceError):reader.read(2<<20)


def test_writer_abort_admission_and_cancel_release_leases():
    budget=Budget(1<<20)
    with pytest.raises(ResourceError):BgzfWriter(io.BytesIO(),budget=budget)
    budget=Budget(8<<20)
    target=io.BytesIO()
    writer=BgzfWriter(target,budget=budget)
    writer.write(b'unfinished')
    writer.close()
    assert target.getvalue()==b'' and budget.used_bytes==0
    writer=BgzfWriter(target,budget=budget,cancel=lambda:True)
    with pytest.raises(CancelledError):writer.finish()
    assert budget.used_bytes==0


def test_htslib_bgzip_and_pysam_read_produced_canonical_stream(tmp_path):
    bgzip=shutil.which('bgzip')
    if bgzip is None:pytest.skip('HTSlib bgzip is not available; independent qualification pending')
    pysam=pytest.importorskip('pysam')
    payload=(b'@opaque\nACGT\n+\n!!!!\n'*9000)+b'@last\n\n+\n\n'
    encoded,_=produce(payload)
    path=tmp_path/'test.bgz';path.write_bytes(encoded)
    output=subprocess.run([bgzip,'-d','-c',str(path)],capture_output=True,check=True)
    assert output.stdout==payload
    with pysam.BGZFile(str(path),'rb') as reader:
        assert reader.read()==payload


def test_htslib_sidecar_and_virtual_offset_oracles(tmp_path):
    bgzip=shutil.which('bgzip')
    if bgzip is None:pytest.skip('HTSlib bgzip oracle unavailable')
    pysam=pytest.importorskip('pysam')
    record=b'@id\nACGT\n+\n!!!!\n'
    payload=record*9000
    encoded,stats=produce(payload)
    path=tmp_path/'oracle.bgz';path.write_bytes(encoded)
    subprocess.run([bgzip,'-r',str(path)],capture_output=True,check=True)
    generated=Path(str(path)+'.gzi').read_bytes()
    ours=io.BytesIO();build_gzi(ours,io.BytesIO(encoded),uncompressed_bytes=len(payload),physical_bytes=len(encoded))
    assert generated==ours.getvalue()
    with GziCursor(io.BytesIO(generated),uncompressed_bytes=len(payload),physical_bytes=len(encoded)) as cursor:
        offsets=[cursor.virtual_offset(index*len(record)) for index in (0,3000,7000)]
    with pysam.BGZFile(str(path),'rb') as reader:
        for offset in offsets:
            reader.seek(offset)
            assert reader.read(len(record))==record


def test_compression_fallback_is_bounded_and_valid(monkeypatch):
    import readmixer_py.artifacts.bgzf as module
    original=module.zlib.compressobj
    calls=[]
    class Oversized:
        def compress(self,payload):return b'x'*65536
        def flush(self):return b''
    def encoder(level,*args):
        calls.append(level)
        return Oversized() if level==6 else original(level,*args)
    monkeypatch.setattr(module.zlib,'compressobj',encoder)
    encoded=encode_member(b'abc',6)
    assert calls==[6,0]
    assert gzip.decompress(encoded+EOF_MEMBER)==b'abc'


def test_stream_closed_guards_and_sidecar_impossible_bounds():
    encoded,_=produce(b'x\n')
    reader=BgzfReader(io.BytesIO(encoded),physical_limit=len(encoded),uncompressed_limit=2,indexed=True)
    reader.close()
    for invoke in (lambda:reader.readline(10),lambda:reader.seek_virtual(0),lambda:reader.read(1)):
        with pytest.raises(ResourceError):invoke()
    for decoded,physical in ((0,29),(1,28),(1000000,29)):
        with pytest.raises(IntegrityError):GziCursor(io.BytesIO(bytes(8)),uncompressed_bytes=decoded,physical_bytes=physical)


def test_source_io_failures_are_typed_and_owned_handles_stay_open():
    class BadReader(io.BytesIO):
        def read(self,size):raise OSError('injected')
    source=BadReader()
    with pytest.raises(IntegrityError):
        with BgzfReader(source,physical_limit=100,uncompressed_limit=100) as reader:
            reader.read(1)
    assert not source.closed
    class BadWriter(io.BytesIO):
        def write(self,data):raise OSError('injected')
    sink=BadWriter()
    writer=BgzfWriter(sink)
    writer.write(b'x')
    with pytest.raises(ResourceError):writer.finish()
    assert not sink.closed


def test_indexed_scope_does_not_validate_unrelated_skipped_payloads():
    payload=(b'x'*65279+b'\n')*8
    encoded,_=produce(payload,0)
    side=io.BytesIO();build_gzi(side,io.BytesIO(encoded),uncompressed_bytes=len(payload),physical_bytes=len(encoded))
    with GziCursor(io.BytesIO(side.getvalue()),uncompressed_bytes=len(payload),physical_bytes=len(encoded)) as cursor:
        target=cursor.virtual_offset(6*PAYLOAD_BYTES)
    damaged=bytearray(encoded)
    damaged[100]^=1  # Unselected first member remains structurally addressable but has a bad CRC.
    with BgzfReader(io.BytesIO(damaged),physical_limit=len(damaged),uncompressed_limit=len(payload),indexed=True) as reader:
        assert reader.readline_at(target,PAYLOAD_BYTES)==payload[:PAYLOAD_BYTES]
        assert reader.stats.sha256 is None
    with pytest.raises(IntegrityError):read_all(bytes(damaged),len(payload))


def test_partial_stream_reads_and_writes_are_supported():
    class SmallSink(io.BytesIO):
        def write(self,data):return super().write(data[:3])
    class SmallSource(io.BytesIO):
        def read(self,size):return super().read(min(size,3))
    payload=b'abc\n'*20000
    sink=SmallSink()
    with BgzfWriter(sink) as writer:writer.write(payload)
    encoded=sink.getvalue()
    with BgzfReader(SmallSource(encoded),physical_limit=len(encoded),uncompressed_limit=len(payload)) as reader:
        assert reader.read(len(payload))==payload
    side=SmallSink();build_gzi(side,SmallSource(encoded),uncompressed_bytes=len(payload),physical_bytes=len(encoded))
    with GziCursor(SmallSource(side.getvalue()),uncompressed_bytes=len(payload),physical_bytes=len(encoded)) as cursor:
        cursor.virtual_offset(PAYLOAD_BYTES)


def test_member_endpoints_and_eof_are_not_alias_virtual_starts():
    record=b'@x\nA\n+\n!\n'
    payload=(b'x'*(PAYLOAD_BYTES-1)+b'\n')+record
    encoded,_=produce(payload)
    second=struct.unpack_from('<H',encoded,16)[0]+1
    for vo in (virtual_offset(0,PAYLOAD_BYTES),virtual_offset(second,len(record)),virtual_offset(len(encoded)-28,0)):
        with BgzfReader(io.BytesIO(encoded),physical_limit=len(encoded),uncompressed_limit=len(payload),indexed=True) as reader:
            with pytest.raises(IntegrityError):reader.read_record_at(vo,100)
    with BgzfReader(io.BytesIO(encoded),physical_limit=len(encoded),uncompressed_limit=len(payload),indexed=True) as reader:
        assert reader.read_record_at(virtual_offset(second,0),100)==record


def test_early_successful_context_exit_validates_unread_members_and_eof():
    payload=b'@x\nA\n+\n!\n'+b'x'*100000
    encoded,_=produce(payload)
    damaged=bytearray(encoded);damaged[-1]^=1
    budget=Budget(8<<20)
    with pytest.raises(IntegrityError):
        with BgzfReader(io.BytesIO(damaged),physical_limit=len(damaged),uncompressed_limit=len(payload),budget=budget) as reader:
            assert reader.read_record(100)==b'@x\nA\n+\n!\n'
    assert budget.used_bytes==0


def test_failed_write_releases_codec_lease_and_cannot_finish_twice():
    class ZeroSink:
        def write(self,data):return 0
    budget=Budget(8<<20)
    writer=BgzfWriter(ZeroSink(),budget=budget)
    writer.write(b'abc')
    with pytest.raises(ResourceError):writer.finish()
    with pytest.raises(ResourceError):writer.finish()
    assert budget.used_bytes==0


@pytest.mark.parametrize('cpus',[1,2,4])
def test_shared_compression_preserves_exact_lane_bytes_and_bounds_pending(cpus):
    from readmixer_py.execution import CompressionService,CpuObservations,resolve_cpu_budget
    from threading import get_ident
    owner=get_ident()
    class OwnedSink(io.BytesIO):
        def write(self,data):
            assert get_ident()==owner,'worker attempted to write an artifact'
            return super().write(data)
    budget=Budget(256<<20)
    payloads=[random.Random(lane).randbytes(PAYLOAD_BYTES*7+lane+1) for lane in range(3)]
    outputs=[OwnedSink() for _ in payloads]
    plan=resolve_cpu_budget(observations=CpuObservations(cpus))
    with CompressionService(budget,plan,encoder=encode_member) as service:
        writers=[BgzfWriter(sink,budget=budget,compression_service=service) for sink in outputs]
        for offset in range(0,max(map(len,payloads)),PAYLOAD_BYTES):
            for writer,payload in zip(writers,payloads):writer.write(payload[offset:offset+PAYLOAD_BYTES])
        for writer,payload in zip(writers,payloads):
            assert writer.uncompressed_bytes==len(payload)
            assert writer.finish().uncompressed_bytes==len(payload)
        assert service.pending_jobs==0
        assert service.peak_pending<=service.capacity
    assert budget.used_bytes==0
    for output,payload in zip(outputs,payloads):assert output.getvalue()==produce(payload)[0]


def test_shared_compression_failure_stops_all_lanes_before_eof_and_releases_leases():
    from readmixer_py.execution import CompressionService,CpuObservations,resolve_cpu_budget
    budget=Budget(128<<20)
    plan=resolve_cpu_budget(observations=CpuObservations(3))
    def fail(_payload,_level):raise RuntimeError('private synthetic payload must not leak')
    outputs=[io.BytesIO(),io.BytesIO()]
    service=CompressionService(budget,plan,encoder=fail)
    writers=[BgzfWriter(sink,budget=budget,compression_service=service) for sink in outputs]
    try:
        with pytest.raises(ResourceError) as found:
            for writer in writers:writer.write(b'x'*PAYLOAD_BYTES)
            writers[0].finish()
        assert 'private synthetic' not in str(found.value)
        with pytest.raises(ResourceError):writers[1].finish()
    finally:
        for writer in writers:writer.close()
        service.close()
    assert budget.used_bytes==0
    assert all(not sink.getvalue().endswith(EOF_MEMBER) for sink in outputs)
