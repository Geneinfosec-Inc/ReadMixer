"""P15-T02: exact completion/inventory ceilings in complete public workflows.

Small private ceilings isolate each policy without generating huge catalogs.
Limits are installed only at the relevant reached checkpoint; ordinary source,
metadata, and bundle admission remain intact and cannot satisfy the test early.
"""

import json
import sys

import pytest

from readmixer_py import mix, recover, verify
from readmixer_py.artifacts.manifest import encode_manifest, parse_manifest
from readmixer_py.config import MixConfig, RecoverConfig, RecoveryMode, StorageMode, VerifyConfig
from readmixer_py.errors import ResourceError
from readmixer_py.transaction import OutputTransaction
from test_artifact_metadata import manifest_fixture

OPTIONS = dict(workers=1, max_ram=512 << 20, storage=StorageMode.EXTERNAL)
SAMPLES = ('A' + 'x' * 120, 'B' + 'x' * 120)


def sources(root):
    root.mkdir(mode=0o700)
    for sample in SAMPLES:
        for role in (1, 2):
            (root / f'{sample}_R{role}.fq').write_bytes(
                f'@private/{role}\nACGT\n+private annotation\n!!!!\n'.encode())
    return {path.name: path.read_bytes() for path in root.iterdir()}


def action(root, operation):
    inputs = root / 'inputs'
    expected = sources(inputs)
    if operation != 'mix':
        report = mix(MixConfig(input_dir=inputs, output=root / 'pool', **OPTIONS))
        manifest = root / 'pool' / report.manifest
    destination = root / 'destination'
    destination.mkdir(mode=0o700)
    foreign = destination / 'foreign'
    foreign.write_bytes(b'unchanged foreign bytes')
    def run():
        if operation == 'mix':
            return mix(MixConfig(input_dir=inputs, output=destination, **OPTIONS))
        return recover(RecoverConfig(manifest=manifest, output=destination, sample=SAMPLES,
                                     io=RecoveryMode(operation), **OPTIONS))
    return run, inputs, expected, destination, foreign


@pytest.mark.parametrize('operation', ('mix', 'indexed', 'sequential'))
@pytest.mark.parametrize('over', (0, 1), ids=('exact', 'one_over'))
@pytest.mark.parametrize('ceiling', ('metadata', 'physical'))
def test_P15_T02_real_manifest_and_receipt_completion_byte_boundary(
        tmp_path, monkeypatch, operation, over, ceiling):
    run, inputs, expected, destination, foreign = action(tmp_path, operation)
    original = OutputTransaction.publish_all
    reached = []
    budgets = []
    def at_marker(self, encoded):
        document = json.loads(encoded)
        expected_format = 'readmixer-python-pool' if operation == 'mix' else 'readmixer-python-recovery'
        assert document['format'] == expected_format
        # All output data has closed and been registered. Only completion
        # admission can fail from this point; shrink its ceiling by one byte.
        assert self._journal.inventory_count == (6 if operation == 'mix' else 8)
        assert not any(path.name.endswith(('.manifest.json', '.receipt.json')) for path in destination.iterdir())
        if ceiling == 'metadata':
            self.metadata_bytes = len(encoded) - over
        else:
            self.max_artifact_bytes = len(encoded) - over
        budgets.append(self.budget)
        reached.append((len(encoded), self._journal.inventory_count))
        return original(self, encoded)
    monkeypatch.setattr(OutputTransaction, 'publish_all', at_marker)
    if over:
        with pytest.raises(ResourceError, match='completion document exceeds') as retained:
            run()
        assert list(destination.iterdir()) == [foreign]
        assert retained.value.__traceback__ is not None
        assert budgets[0].active_leases == budgets[0].used_metadata_bytes == 0
        assert budgets[0].used_bytes == budgets[0].baseline_bytes
    else:
        report = run()
        marker = destination / (report.manifest if operation == 'mix' else report.receipt)
        assert marker.stat().st_size == reached[0][0]
        assert len(list(destination.iterdir())) == reached[0][1] + 2  # marker + foreign
        if operation == 'mix':
            assert verify(VerifyConfig(manifest=marker, **OPTIONS)).completed
    assert len(reached) == 1 and budgets[0].used_descriptors == 0
    assert {path.name: path.read_bytes() for path in inputs.iterdir()} == expected
    assert foreign.read_bytes() == b'unchanged foreign bytes'


@pytest.mark.parametrize('operation', ('mix', 'indexed', 'sequential'))
@pytest.mark.parametrize('over', (0, 1), ids=('exact', 'one_over'))
def test_P15_T02_complete_inventory_reserves_marker_and_refuses_next_group(
        tmp_path, monkeypatch, operation, over):
    run, inputs, expected, destination, foreign = action(tmp_path, operation)
    original_begin = OutputTransaction.begin.__func__
    original_create = OutputTransaction.create_partial
    owners, names = [], []
    cap = (7 if operation == 'mix' else 9) - over
    def begin(cls, *args, **kwargs):
        kwargs['max_artifacts'] = cap
        transaction = original_begin(cls, *args, **kwargs)
        owners.append(transaction)
        return transaction
    def create(self, name, *args, **kwargs):
        result = original_create(self, name, *args, **kwargs)
        names.append(name)
        return result
    monkeypatch.setattr(OutputTransaction, 'begin', classmethod(begin))
    monkeypatch.setattr(OutputTransaction, 'create_partial', create)
    if over:
        with pytest.raises(ResourceError, match='inventory|artifact count') as retained:
            run()
        assert retained.value.__traceback__ is not None
        assert len(owners) == 1
        # Mix has staged five artifacts when the sixth consumes the reserved
        # completion slot; recovery staged sample A before the next group.
        assert len(names) == (5 if operation == 'mix' else 4)
        assert list(destination.iterdir()) == [foreign]
        assert owners[0].budget.active_leases == owners[0].budget.used_metadata_bytes == 0
        assert owners[0].budget.used_bytes == owners[0].budget.baseline_bytes
    else:
        report = run()
        marker = destination / (report.manifest if operation == 'mix' else report.receipt)
        document = json.loads(marker.read_bytes())
        assert document['totals']['artifacts'] == cap
        assert len(names) == cap - 1 and len(list(destination.iterdir())) == cap + 1
    assert owners[0].budget.used_descriptors == 0
    assert {path.name: path.read_bytes() for path in inputs.iterdir()} == expected
    assert foreign.read_bytes() == b'unchanged foreign bytes'


def representation(value):
    """Independent recursive Python representation observation, without codec helpers."""
    if isinstance(value, dict):
        return sys.getsizeof(value) + sum(representation(k) + representation(v) for k, v in value.items())
    if isinstance(value, list):
        return sys.getsizeof(value) + sum(map(representation, value))
    return sys.getsizeof(value)


@pytest.mark.parametrize('boundary', ('encoded', 'represented'))
def test_P15_T02_encoded_and_parsed_manifest_limits_are_independent(boundary):
    document = manifest_fixture()
    encoded = encode_manifest(document)
    retained = representation(document)
    assert retained > len(encoded), 'fixture must distinguish wire bytes from resident objects'
    limits = dict(max_metadata_bytes=len(encoded), max_parsed_bytes=retained)
    assert encode_manifest(document, **limits) == encoded
    limits['max_metadata_bytes' if boundary == 'encoded' else 'max_parsed_bytes'] -= 1
    with pytest.raises(ResourceError, match='encoded metadata' if boundary == 'encoded' else 'parsed metadata'):
        encode_manifest(document, **limits)
    # Parsing additionally admits transient token/object overlap. Encoded bytes
    # fitting by themselves never waive that separate obligation.
    assert parse_manifest(encoded, max_metadata_bytes=len(encoded)) == document
    with pytest.raises(ResourceError, match='encoded metadata'):
        parse_manifest(encoded, max_metadata_bytes=len(encoded) - 1)
    with pytest.raises(ResourceError, match='JSON parsing workspace'):
        parse_manifest(encoded, max_metadata_bytes=len(encoded), max_parsed_bytes=retained)
