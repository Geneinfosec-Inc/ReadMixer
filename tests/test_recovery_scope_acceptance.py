"""P15-T03/T04: public recovery scopes remain explicit on hostile payloads."""

import hashlib
import json
import shutil

import pytest

from readmixer_py.config import RecoveryMode, VerifyConfig
from readmixer_py.errors import IntegrityError
from readmixer_py.recovery import recover
from readmixer_py.verification import verify
from test_indexed_union_io import (
    IOTrace, SELECTED, assert_restored, recover_config, union_pool,
)


@pytest.mark.parametrize('role', ('r1', 'r2', 'key'))
def test_P15_T04_unselected_payload_is_outside_indexed_scope_but_inside_full_stream_scope(
        union_pool, tmp_path, monkeypatch, role):
    manifest, expected, (_document, lanes, entries, _ranges) = union_pool
    source = tmp_path / 'PRIVATE_pool'
    shutil.copytree(manifest.parent, source)
    manifest = source / manifest.name
    document = json.loads(manifest.read_bytes())
    bundle = next(bundle for bundle in document['bundles']
                  if all(row[0] not in SELECTED for row in entries[bundle['index']['name']]))
    descriptor = bundle[role]
    member = lanes[descriptor['name']][0]
    assert member.length > 0
    path = source / descriptor['name']
    data = bytearray(path.read_bytes())
    # Keep the complete header/length chain and declared outer integrity valid.
    # A wrong decoded CRC requires inflation; the selected-domain API must not
    # claim that it examined this entirely unselected bundle's payload.
    data[member.physical + member.size - 8] ^= 1
    path.write_bytes(data)
    descriptor['sha256'] = hashlib.sha256(data).hexdigest()
    manifest.write_text(json.dumps(document, separators=(',', ':')) + '\n')
    before = {p.name: p.read_bytes() for p in source.iterdir()}
    with monkeypatch.context() as patch:
        trace = IOTrace(patch)
        output = tmp_path / 'indexed'
        report = recover(recover_config(manifest, output))
        receipt = assert_restored(output, report, expected)
        assert report.validation_scope == receipt['validation_scope'] == 'selected_records'
        assert report.index_validation == 'validated'
        for lane in ('r1', 'r2', 'key'):
            observation = trace.sole_input(bundle[lane]['name'])
            assert not observation.full_hash and not observation.reads
        # All index and sidecar bytes remain required even in this bundle.
        for required in ('index', 'r1_gzi', 'r2_gzi'):
            trace.assert_full_file(bundle[required])
        assert not trace.live
    for operation in ('verify', 'sequential'):
        with monkeypatch.context() as patch:
            trace = IOTrace(patch)
            destination = tmp_path / operation
            if operation == 'sequential':
                destination.mkdir(mode=0o700)
                (destination / 'foreign.keep').write_bytes(b'PRIVATE foreign')
            with pytest.raises(IntegrityError, match='CRC') as caught:
                if operation == 'verify':
                    verify(VerifyConfig(manifest=manifest, max_ram=1 << 30, workers=1))
                else:
                    recover(recover_config(manifest, destination, RecoveryMode.SEQUENTIAL))
            assert caught.value.__traceback__ is not None
            observation = trace.sole_input(descriptor['name'])
            assert observation.full_hash and observation.reads
            # The actual compressed interval was read before inner rejection.
            assert any(start <= member.physical and end >= member.physical + member.size
                       for start, end in observation.reads) or sum(
                           end - start for start, end in observation.reads) >= member.size
            assert not trace.live
            assert str(tmp_path) not in str(caught.value)
            if operation == 'sequential':
                assert [p.name for p in destination.iterdir()] == ['foreign.keep']
                assert (destination / 'foreign.keep').read_bytes() == b'PRIVATE foreign'
    assert {p.name: p.read_bytes() for p in source.iterdir()} == before
