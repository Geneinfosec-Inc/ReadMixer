"""Protected multipass sorting, budget admission and exact identity coverage."""
from contextlib import contextmanager
import random

import pytest

from readmixer_py.crypto import OperationKeys
from readmixer_py.errors import CancelledError, ConfigurationError, IntegrityError, ResourceError
from readmixer_py.resources import Budget
from readmixer_py.sorting import ExternalSorter, IdentityCoverage
from readmixer_py.storage import SegmentStore, deserialize_segment_ref


@contextmanager
def store_at(path, mode='external', *, memory=256 << 20, descriptors=128):
    budget = Budget(memory, descriptors)
    with SegmentStore(mode, budget, OperationKeys._from_test(bytes(32), bytes(16)), temp_dir=path) as store:
        yield store
    assert budget.used_bytes == 0
    assert budget.used_descriptors == 0
    assert not list(path.iterdir())


def sorter(store, **kwargs):
    defaults = dict(key=lambda record: int(record), run_bytes=700, max_record_bytes=12, fan_in=2)
    defaults.update(kwargs)
    return ExternalSorter(store, **defaults)


@pytest.mark.parametrize('mode', ['memory', 'external', 'auto'])
def test_many_bounded_runs_multiple_merge_passes_and_all_modes(tmp_path, mode):
    values = list(range(37)) + [3, 3, 18]
    random.Random(127).shuffle(values)
    with store_at(tmp_path, mode) as store:
        with sorter(store) as stream:
            for number in values:
                stream.append(str(number).encode())
                assert stream._row_bytes <= stream.run_bytes
                assert len(stream._rows) <= 32768
                assert store.resident_segments <= 128
            assert stream.run_count > stream.fan_in
            with stream.iter_sorted() as records:
                assert [int(record) for record in records] == sorted(values)
            assert stream.merge_passes >= 4
            assert stream.run_count == 1
            assert not store._reader_pins
        assert store.resident_segments == 0
        if mode == 'memory':
            assert store.scratch_directory is None


@pytest.mark.parametrize('mode', ['memory', 'external', 'auto'])
def test_empty_single_and_empty_byte_records(tmp_path, mode):
    with store_at(tmp_path, mode) as store:
        with sorter(store, key=lambda record: record) as stream:
            with stream.iter_sorted() as records:
                assert list(records) == []
        with sorter(store, key=lambda record: record) as stream:
            stream.append(b'')
            with stream.iter_sorted() as records:
                assert list(records) == [b'']
            assert stream.merge_passes == 0


def test_keys_are_bounded_tuples_with_payload_preserved(tmp_path):
    records = [b'2:b', b'1:d', b'1:c', b'2:a']
    with store_at(tmp_path) as store:
        with sorter(store, key=lambda record: (int(record[:1]), record[2:])) as stream:
            for record in records:
                stream.append(record)
            with stream.iter_sorted() as output:
                assert list(output) == [b'1:c', b'1:d', b'2:a', b'2:b']


@pytest.mark.parametrize('invalid', [True, 2**64, b'x'*4096, [1], {'x': 1}, tuple(range(17)),
                                   (((((1,),),),),)])
def test_unbounded_or_noncanonical_key_rejected_before_retention(tmp_path, invalid):
    with store_at(tmp_path) as store:
        with sorter(store, key=lambda record: invalid) as stream:
            with pytest.raises(ConfigurationError):
                stream.append(b'1')
            assert stream.records == 0


def test_incomparable_single_record_runs_fail_with_typed_error(tmp_path):
    with store_at(tmp_path) as store:
        with sorter(store, key=lambda record: int(record) if record == b'1' else record,
                    run_bytes=330) as stream:
            stream.append(b'1')
            stream.append(b'2')
            with pytest.raises(IntegrityError):
                with stream.iter_sorted() as records:
                    list(records)


def test_key_error_does_not_expose_payload_in_public_message(tmp_path):
    def bad_key(record):
        raise ValueError(record)
    with store_at(tmp_path) as store:
        with sorter(store, key=bad_key) as stream:
            with pytest.raises(ConfigurationError) as error:
                stream.append(b'secret-read')
            assert 'secret-read' not in str(error.value)


@pytest.mark.parametrize('mode,budget', [('memory', 24 << 20), ('external', 32 << 20), ('auto', 48 << 20)])
def test_low_budget_merge_fails_cleanly_with_cleanup_reserve(tmp_path, mode, budget):
    with store_at(tmp_path, mode, memory=budget) as store:
        with sorter(store) as stream:
            for number in range(8):
                stream.append(str(number).encode())
            with pytest.raises(ResourceError):
                with stream.iter_sorted() as records:
                    list(records)
        assert store.resident_segments == 0


def test_fanin_reduces_to_descriptor_allowance(tmp_path):
    with store_at(tmp_path, descriptors=10) as store:
        with sorter(store, fan_in=32) as stream:
            for number in range(25, -1, -1):
                stream.append(str(number).encode())
            with stream.iter_sorted() as records:
                assert [int(record) for record in records] == list(range(26))
            assert 2 <= stream.effective_fan_in < 32
            assert store.budget.used_descriptors <= 10


def test_single_record_run_memory_overflow_is_atomic(tmp_path):
    with store_at(tmp_path) as store:
        with sorter(store, run_bytes=100) as stream:
            with pytest.raises(ResourceError):
                stream.append(b'1')
            assert stream.records == 0
            assert not stream._rows


def test_early_successful_consumer_exit_drains_and_validates_final_run(tmp_path):
    with store_at(tmp_path) as store:
        with sorter(store) as stream:
            for number in range(12):
                stream.append(str(number).encode())
            with stream.iter_sorted() as records:
                assert next(records) == b'0'
            assert not store._reader_pins
            with pytest.raises(ResourceError):
                stream.append(b'13')
            with pytest.raises(ResourceError):
                with stream.iter_sorted():
                    pass


def test_final_segment_corruption_detected_even_after_early_consumer_exit(tmp_path):
    with store_at(tmp_path) as store:
        with sorter(store, run_bytes=4096) as stream:
            stream.append(b'1')
            stream.append(b'2')
            stream._flush_run()
            with stream._runs.open_reverse() as cursor:
                ref = deserialize_segment_ref(next(cursor))
            path = store.scratch_directory / store._name(ref)
            data = bytearray(path.read_bytes())
            data[-1] ^= 1  # Corrupt the seal tag, leaving data block authentication intact.
            path.write_bytes(data)
            with pytest.raises(IntegrityError):
                with stream.iter_sorted() as records:
                    assert next(records) == b'1'


@pytest.mark.parametrize('mode', ['memory', 'external', 'auto'])
def test_cancellation_after_real_run_writes_cleans_all_owned_state(tmp_path, mode):
    cancelled = [False]
    with store_at(tmp_path, mode) as store:
        with sorter(store, cancel=lambda: cancelled[0]) as stream:
            for number in range(10):
                stream.append(str(number).encode())
            assert stream.run_count > 0
            cancelled[0] = True
            with pytest.raises(CancelledError):
                with stream.iter_sorted() as records:
                    list(records)
        assert store.resident_segments == 0


def test_failed_pending_ledger_flush_retains_run_until_cleanup(tmp_path, monkeypatch):
    with store_at(tmp_path) as store:
        with sorter(store) as stream:
            stream.append(b'1')
            def fail():
                raise ResourceError('injected registry write failure')
            monkeypatch.setattr(stream._runs, 'flush', fail)
            with pytest.raises(ResourceError):
                stream._flush_run()
            assert stream._pending is not None
        assert store.resident_segments == 0


def test_parent_delete_failure_keeps_child_and_retryable_prefix_ownership(tmp_path, monkeypatch):
    with store_at(tmp_path) as store:
        with sorter(store) as stream:
            for number in range(8):
                stream.append(str(number).encode())
            original = store.drop
            failed = [False]
            def fail_once(ref, **kwargs):
                if ref.purpose == 3 and not failed[0]:
                    failed[0] = True
                    raise ResourceError('injected parent deletion failure')
                return original(ref, **kwargs)
            monkeypatch.setattr(store, 'drop', fail_once)
            with pytest.raises(ResourceError):
                with stream.iter_sorted() as records:
                    list(records)
            assert stream._next_runs.root.record_count == 1
        assert store.resident_segments == 0


@pytest.mark.parametrize('counts', [(4, 0, 2), (), (0, 0)])
@pytest.mark.parametrize('mode,force_sort', [('memory', False), ('external', False), ('external', True), ('auto', True)])
def test_exact_coverage_bitmap_and_external_sort(tmp_path, counts, mode, force_sort):
    identities = [(sample, ordinal) for sample, count in enumerate(counts) for ordinal in range(count)]
    random.Random(133).shuffle(identities)
    with store_at(tmp_path, mode) as store:
        with IdentityCoverage(store, counts, bitset_limit_bytes=0 if force_sort else None,
                              run_bytes=700, fan_in=2) as coverage:
            for sample, ordinal in identities:
                coverage.add(sample, ordinal)
            coverage.finish()
            with pytest.raises(ResourceError):
                coverage.finish()


@pytest.mark.parametrize('force_sort', [False, True])
def test_coverage_duplicate_gap_and_bounds(tmp_path, force_sort):
    with store_at(tmp_path) as store:
        with IdentityCoverage(store, [3, 0, 2], bitset_limit_bytes=0 if force_sort else None,
                              run_bytes=700, fan_in=2) as coverage:
            for sample, ordinal in [(0, 0), (0, 1), (2, 0), (2, 1)]:
                coverage.add(sample, ordinal)
            if force_sort:
                coverage.add(0, 1)
                with pytest.raises(IntegrityError):
                    coverage.finish()
            else:
                with pytest.raises(IntegrityError):
                    coverage.add(0, 1)
                with pytest.raises(IntegrityError):
                    coverage.finish()
        with IdentityCoverage(store, [1]) as coverage:
            for sample, ordinal in [(-1, 0), (1, 0), (0, -1), (0, 1), (True, 0)]:
                with pytest.raises(IntegrityError):
                    coverage.add(sample, ordinal)
            assert coverage.observed == 0


def test_bitmap_admission_fallback_and_memory_refusal(tmp_path):
    with store_at(tmp_path, 'external', memory=64 << 20) as store:
        with IdentityCoverage(store, [1 << 40]) as coverage:
            assert coverage.mode == 'external-sort'
    with store_at(tmp_path, 'memory', memory=64 << 20) as store:
        with pytest.raises(ResourceError):
            IdentityCoverage(store, [1 << 40])
        assert store.scratch_directory is None


@pytest.mark.parametrize('counts', [[-1], [True], [1 << 64], [1 << 63, 1 << 63], [0] * 10001])
def test_coverage_invalid_counts_and_total_overflow(tmp_path, counts):
    with store_at(tmp_path) as store:
        with pytest.raises((ConfigurationError, ResourceError)):
            IdentityCoverage(store, counts)


@pytest.mark.parametrize('kwargs', [{'fan_in': 1}, {'fan_in': 33}, {'run_bytes': 0},
                                   {'max_record_bytes': 0}, {'max_record_bytes': 1 << 32}])
def test_sort_configuration_limits_before_admission(tmp_path, kwargs):
    with store_at(tmp_path) as store:
        with pytest.raises(ConfigurationError):
            sorter(store, **kwargs)


@pytest.mark.parametrize('mode', ['memory', 'external', 'auto'])
@pytest.mark.parametrize('stage', ['after-drop', 'after-ledger-commit'])
def test_interruption_after_ownership_transition_remains_cleanup_safe(tmp_path, monkeypatch, mode, stage):
    with store_at(tmp_path, mode) as store:
        with sorter(store) as stream:
            for number in range(8):
                stream.append(str(number).encode())
            failed = [False]
            if stage == 'after-drop':
                original = store.drop
                def drop_then_fail(ref, **kwargs):
                    result = original(ref, **kwargs)
                    if ref.purpose == 3 and not failed[0]:
                        failed[0] = True
                        raise ResourceError('injected post-deletion failure')
                    return result
                monkeypatch.setattr(store, 'drop', drop_then_fail)
            else:
                original = stream._runs.flush
                def flush_then_fail():
                    result = original()
                    if not failed[0]:
                        failed[0] = True
                        raise ResourceError('injected post-commit failure')
                    return result
                monkeypatch.setattr(stream._runs, 'flush', flush_then_fail)
            with pytest.raises(ResourceError):
                with stream.iter_sorted() as records:
                    list(records)
        assert store.resident_segments == 0


def test_incomparable_heap_siblings_are_typed_on_pop(tmp_path):
    keys = {b'a': (0, 0), b'b': (1, b'x'), b'c': (1, 1)}
    with store_at(tmp_path) as store:
        with sorter(store, key=keys.__getitem__, run_bytes=400, max_record_bytes=1, fan_in=3) as stream:
            for record in (b'c', b'b', b'a'):
                stream.append(record)
            with pytest.raises(IntegrityError):
                with stream.iter_sorted() as records:
                    list(records)


@pytest.mark.parametrize('mode', ['memory', 'external', 'auto'])
@pytest.mark.parametrize('length', [(1 << 20) + 29, (2 << 20) + 40, 3 << 20])
def test_record_crosses_authenticated_block_boundaries(tmp_path, mode, length):
    records = [bytes([number]) + b'x' * (length - 1) for number in (2, 0, 1)]
    with store_at(tmp_path, mode, memory=128 << 20) as store:
        with sorter(store, key=lambda record: record[0], run_bytes=length + 1024,
                    max_record_bytes=length) as stream:
            for record in records:
                stream.append(record)
            with stream.iter_sorted() as output:
                assert list(output) == [records[1], records[2], records[0]]


def test_independent_admitted_record_total_is_checked(tmp_path):
    with store_at(tmp_path) as store:
        with sorter(store) as stream:
            stream.append(b'1')
            stream._flush_run()
            stream.records += 1
            with pytest.raises(IntegrityError):
                with stream.iter_sorted() as records:
                    list(records)


def test_cancellation_polled_between_large_record_read_chunks(tmp_path, monkeypatch):
    cancelled = [False]
    with store_at(tmp_path) as store:
        with sorter(store, key=lambda record: record[0], run_bytes=2 << 20,
                    max_record_bytes=2 << 20, cancel=lambda: cancelled[0]) as stream:
            stream.append(b'x' * ((1 << 20) + 20))
            original = store.open_verified
            def track(ref, **kwargs):
                reader = original(ref, **kwargs)
                if ref.purpose == 3 and not kwargs.get('cleanup'):
                    readinto = reader.readinto
                    def cancel_after_chunk(destination):
                        count = readinto(destination)
                        cancelled[0] = True
                        return count
                    reader.readinto = cancel_after_chunk
                return reader
            monkeypatch.setattr(store, 'open_verified', track)
            with pytest.raises(CancelledError):
                with stream.iter_sorted() as records:
                    list(records)
        assert not store._reader_pins
