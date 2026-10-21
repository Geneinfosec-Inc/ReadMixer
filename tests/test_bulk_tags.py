"""P14-T06: bounded, transcript-preserving coordinator tag windows."""

import struct

import pytest

from readmixer_py.artifacts.bundles import BundleEmitter
from readmixer_py.crypto import BulkTagger, OperationKeys
from readmixer_py.errors import ConfigurationError, IntegrityError, ResourceError
from readmixer_py.resources import Budget
from readmixer_py.shuffle import OrderedPair
from readmixer_py.transaction import OutputTransaction


def keys():
    return OperationKeys._from_test(bytes(range(32)), bytes(range(16)))


@pytest.mark.parametrize('count', [1, 4096])
def test_bulk_window_matches_scalar_and_frozen_independent_vector(count):
    budget = Budget(64 << 20)
    identities = [(0x12345678, 0x0102030405060708)]
    identities += [(i, (1 << 64) - i) for i in range(1, count)]
    with BulkTagger(keys(), budget, batch_size=count) as tagger:
        actual = tagger.tags(identities)
        assert actual[0] == 'd7a44ffb2b88a7ac74695b65421e223b'
        assert actual == [keys().tag(*identity) for identity in identities]
        assert len(tagger._plain) == len(tagger._encrypted) == 16 * count <= 65536
        assert budget.used_metadata_bytes >= 384 * count
        assert budget.used_bytes > budget.used_metadata_bytes
        assert repr(keys().tag_root) not in repr(tagger)
    assert not actual
    assert budget.used_bytes == budget.used_metadata_bytes == budget.active_leases == 0


def test_partial_and_empty_windows_reuse_context_and_retire_previous_tags(monkeypatch):
    import readmixer_py.crypto as crypto
    create = crypto.AES.new
    contexts = []
    def record(*args, **kwargs):
        result = create(*args, **kwargs)
        contexts.append(result)
        return result
    monkeypatch.setattr(crypto.AES, 'new', record)
    budget = Budget(64 << 20)
    with BulkTagger(keys(), budget) as tagger:
        first = tagger.tags([(0, 0), (1, 0)])
        assert len(first) == 2
        previous = first[0]
        second = tagger.tags([(0, 0)])
        assert second is first and second == [previous]
        assert tagger.tags([]) is first and not first
        assert len(contexts) == 1
    assert budget.used_bytes == 0


@pytest.mark.parametrize('size', [0, -1, 4097, True, 1.0, None])
def test_batch_bound_rejected_before_admission(size):
    budget = Budget(64 << 20)
    with pytest.raises(ConfigurationError):
        BulkTagger(keys(), budget, batch_size=size)
    assert budget.used_bytes == budget.active_leases == 0


@pytest.mark.parametrize('identities', [
    ((0, 0),), iter([(0, 0)]), [(0, 0), (1, 0)], [[0, 0]], [(0,)],
    [(True, 0)], [(0, False)], [(-1, 0)], [(1 << 32, 0)],
    [(0, -1)], [(0, 1 << 64)], [(1.5, 0)], [(0, 'private-value')],
])
def test_invalid_window_is_rejected_before_overwriting_previous_result(identities):
    budget = Budget(64 << 20)
    with BulkTagger(keys(), budget, batch_size=1) as tagger:
        old = tagger.tags([(0, 0)])
        expected = old[0]
        with pytest.raises(ConfigurationError) as failure:
            tagger.tags(identities)
        assert old == [expected]
        assert 'private-value' not in str(failure.value)
    assert budget.used_bytes == 0


def test_maximum_identity_width_and_closed_workspace():
    budget = Budget(64 << 20)
    tagger = BulkTagger(keys(), budget)
    assert tagger.tags([(0xffffffff, 0xffffffffffffffff)]) == [keys().tag(0xffffffff, 0xffffffffffffffff)]
    tagger.close()
    tagger.close()
    with pytest.raises(ResourceError, match='closed'):
        tagger.tags([(0, 0)])
    with pytest.raises(ResourceError, match='closed'):
        tagger.__enter__()
    assert budget.used_bytes == budget.used_metadata_bytes == 0


def test_admission_failure_happens_before_native_context_creation(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('AES context constructed before resource admission')
    monkeypatch.setattr('readmixer_py.crypto.AES.new', forbidden)
    budget = Budget(64 << 20, metadata_limit_bytes=1000)
    with pytest.raises(ResourceError, match='metadata'):
        BulkTagger(keys(), budget)
    assert budget.used_bytes == budget.active_leases == 0


def test_native_constructor_failure_releases_workspace(monkeypatch):
    def fail(*args, **kwargs):
        raise MemoryError('injected native allocation failure')
    monkeypatch.setattr('readmixer_py.crypto.AES.new', fail)
    budget = Budget(64 << 20)
    with pytest.raises(MemoryError):
        BulkTagger(keys(), budget)
    assert budget.used_bytes == budget.used_metadata_bytes == budget.active_leases == 0


def test_interrupted_encryption_retains_charge_until_context_close():
    class Failure:
        def encrypt(self, *args, **kwargs):
            raise KeyboardInterrupt()
    budget = Budget(64 << 20)
    with BulkTagger(keys(), budget) as tagger:
        result = tagger.tags([(0, 0)])
        charge = budget.used_bytes
        tagger._cipher = Failure()
        with pytest.raises(KeyboardInterrupt):
            tagger.tags([(1, 0)])
        assert not result
        assert not tagger._busy
        assert budget.used_bytes == charge
    assert budget.used_bytes == 0


def frame(ordinal):
    lines = (f'@pair{ordinal}/1'.encode(), b'AC', b'+', b'II',
             f'@pair{ordinal}/2'.encode(), b'TG', b'+', b'JJ')
    return struct.pack('<IIQ8I', 48 + sum(map(len, lines)), 0, ordinal, *map(len, lines)) + b''.join(lines)


def emit(directory, *, bulk):
    directory.mkdir(mode=0o700)
    budget = Budget(256 << 20)
    operation = keys()
    with OutputTransaction.begin(directory, 'manifest', budget, max_artifact_bytes=131100) as transaction:
        with BulkTagger(operation, budget) as tagger, BundleEmitter(
                transaction, operation, (3,), max_record_bytes=1024, compression_level=0) as emitter:
            tags = tagger.tags([(0, i) for i in range(3)]) if bulk else None
            for index in range(3):
                ordered = OrderedPair(index, 0, index, memoryview(frame(index)))
                if bulk:
                    emitter.append(ordered, tag=tags[index])
                else:
                    emitter.append(ordered)
            emitter.finish()
        transaction.publish_all(b'{"fixture":"bulk-tag-equivalence"}\n')
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0
    return {path.name: path.read_bytes() for path in directory.iterdir()}


def test_precomputed_tags_preserve_all_six_artifact_bytes(tmp_path):
    scalar = emit(tmp_path / 'scalar', bulk=False)
    bulk = emit(tmp_path / 'bulk', bulk=True)
    assert len(scalar) == 7
    assert scalar == bulk


@pytest.mark.parametrize('tag', ['', '0' * 31, '0' * 33, 'A' * 32, 'g' * 32, b'0' * 32])
def test_emitter_rejects_malformed_tag_before_opening_outputs(tmp_path, tag):
    budget = Budget(256 << 20)
    with OutputTransaction.begin(tmp_path, 'manifest', budget, max_artifact_bytes=131100) as transaction:
        with BundleEmitter(transaction, keys(), (1,), max_record_bytes=1024) as emitter:
            with pytest.raises(IntegrityError, match='malformed coordinator tag'):
                emitter.append(OrderedPair(0, 0, 0, memoryview(frame(0))), tag=tag)
            assert emitter._active is None
    assert budget.used_bytes == budget.used_metadata_bytes == 0
