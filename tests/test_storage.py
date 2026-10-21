"""Protocol, ownership, pressure, and fault tests for protected scratch."""
from dataclasses import replace
import hashlib
import os
import struct
import zlib

from Crypto.Cipher import ChaCha20_Poly1305
import pytest

from readmixer_py.crypto import OperationKeys, observations_identity
from readmixer_py.disk import DiskBudget
from readmixer_py.errors import CancelledError, ConfigurationError, IntegrityError, ResourceError
from readmixer_py.resources import Budget
from readmixer_py.storage import (
    BLOCK_BYTES, BLOCK_HEADER, HEADER, MAGIC, SEAL, SegmentStore,
    deserialize_segment_ref, serialize_segment_ref,
)


def keys():
    return OperationKeys._from_test(bytes(range(32)), bytes(range(16)))


def segment(store, data=b"abc", count=1):
    with store.new_segment(5, observations_identity("verify", 0)) as writer:
        writer.append(data, item_count=count)
        return writer.seal()


def path(store, ref):
    return store.scratch_directory / (ref.segment_id.hex() + ".segment")


@pytest.mark.parametrize("mode", ["memory", "auto", "external"])
@pytest.mark.parametrize("data", [b"", b"abc", b"ACGT" * (BLOCK_BYTES // 2 + 5)])
def test_modes_read_identically_and_release_every_lease(tmp_path, mode, data):
    budget, disk = Budget(64 << 20), DiskBudget()
    with SegmentStore(mode, budget, keys(), temp_dir=tmp_path, disk_budget=disk) as store:
        ref = segment(store, data, count=7)
        assert ref.decoded_bytes == len(data)
        assert ref.item_count == 7
        assert ref.digest == hashlib.sha256(data).digest()
        assert ref.data_block_count == (len(data) + BLOCK_BYTES - 1) // BLOCK_BYTES
        result = bytearray()
        with store.open_verified(ref) as stream:
            while chunk := stream.read(16384):
                result.extend(chunk)
            assert stream.verified
        assert result == data
        assert deserialize_segment_ref(serialize_segment_ref(ref)) == ref
        store.drop(ref)
    assert budget.used_bytes == budget.used_descriptors == budget.active_leases == 0
    assert disk.committed_bytes == disk.pending_bytes == 0
    assert list(tmp_path.iterdir()) == []


def test_exact_wire_layout_and_no_plaintext(tmp_path):
    sentinel = b"private-fastq-identifier-" * 200
    with SegmentStore("external", Budget(32 << 20), keys(), temp_dir=tmp_path) as store:
        ref = segment(store, sentinel, 17)
        data = path(store, ref).read_bytes()
        assert HEADER.size == 76 and BLOCK_HEADER.size == 20 and SEAL.size == 56
        assert data[:8] == MAGIC
        assert HEADER.unpack_from(data) == (MAGIC, ref.run_id, ref.segment_id,
                                           ref.generation_salt, 5, b"\0\0\0")
        assert sentinel not in data
        assert os.stat(store.scratch_directory).st_mode & 0o777 == 0o700
        assert os.stat(path(store, ref)).st_mode & 0o777 == 0o600
        kind, codec, reserved, sequence, decoded, stored = BLOCK_HEADER.unpack_from(data, 76)
        assert (kind, codec, reserved, sequence, decoded) == (0, 1, b"\0\0", 0, len(sentinel))
        assert stored < decoded
        assert len(data) == 76 + 20 + stored + 16 + 20 + 56 + 16


# Every byte in the complete small segment is covered: public header, both
# block headers, ciphertext, both authentication tags, and encrypted final seal.
@pytest.mark.parametrize("offset", range(207))
def test_every_wire_byte_corruption_is_rejected(tmp_path, offset):
    with SegmentStore("external", Budget(32 << 20), keys(), temp_dir=tmp_path) as store:
        ref = segment(store)
        encoded = bytearray(path(store, ref).read_bytes())
        assert len(encoded) == 207
        encoded[offset] ^= 1
        path(store, ref).write_bytes(encoded)
        with pytest.raises(IntegrityError):
            with store.open_verified(ref) as stream:
                stream.read()


@pytest.mark.parametrize("delta", [-1, -100, 1])
def test_truncation_and_trailing_bytes_fail(tmp_path, delta):
    with SegmentStore("external", Budget(32 << 20), keys(), temp_dir=tmp_path) as store:
        ref = segment(store)
        p = path(store, ref)
        data = p.read_bytes()
        p.write_bytes(data[:delta] if delta < 0 else data + b"x")
        with pytest.raises(IntegrityError):
            with store.open_verified(ref):
                pass


def test_context_exit_drains_and_authenticates_unread_seal(tmp_path):
    with SegmentStore("external", Budget(32 << 20), keys(), temp_dir=tmp_path) as store:
        ref = segment(store, b"A" * (BLOCK_BYTES + 1))
        p = path(store, ref)
        data = bytearray(p.read_bytes())
        data[-1] ^= 1
        p.write_bytes(data)
        with pytest.raises(IntegrityError):
            with store.open_verified(ref) as stream:
                assert stream.read(1) == b"A"
        assert not store._reader_pins


def test_primary_consumer_error_survives_invalid_drain(tmp_path):
    with SegmentStore("external", Budget(32 << 20), keys(), temp_dir=tmp_path) as store:
        ref = segment(store)
        p = path(store, ref)
        data = bytearray(p.read_bytes())
        data[-1] ^= 1
        p.write_bytes(data)
        with pytest.raises(RuntimeError, match="consumer") as found:
            with store.open_verified(ref):
                raise RuntimeError("consumer")
        assert "drain" in found.value.__notes__[0]


def test_caller_expected_identity_and_seal_are_authoritative(tmp_path):
    with SegmentStore("external", Budget(32 << 20), keys(), temp_dir=tmp_path) as store:
        first, second = segment(store), segment(store)
        for changed in (replace(first, digest=b"x" * 32), replace(first, item_count=2),
                        replace(first, run_id=b"x" * 16), replace(first, purpose=2),
                        replace(first, logical_identity=observations_identity("verify", 1))):
            with pytest.raises(IntegrityError):
                store.open_verified(changed)
        path(store, first).write_bytes(path(store, second).read_bytes())
        with pytest.raises(IntegrityError):
            store.open_verified(first)


def test_detached_references_are_bounded_and_owned_by_caller(tmp_path):
    budget = Budget(32 << 20)
    with SegmentStore("external", budget, keys(), temp_dir=tmp_path, max_segments=1) as store:
        first = store.detach(segment(store))
        assert store.resident_segments == 0
        second = segment(store, b"second")
        with store.open_verified(first) as reader:
            assert reader.read() == b"abc"
            with pytest.raises(ResourceError, match="readers"):
                store.drop(first)
            with pytest.raises(ResourceError, match="readers"):
                store.close()
        with pytest.raises(ResourceError, match="registry"):
            segment(store)
        store.drop(first)
        store.drop(second)
    assert budget.used_bytes == 0


def test_close_cannot_destroy_transferred_ownership(tmp_path):
    store = SegmentStore("external", Budget(32 << 20), keys(), temp_dir=tmp_path)
    ref = store.detach(segment(store))
    with pytest.raises(ResourceError, match="cleanup"):
        store.close()
    assert path(store, ref).exists()
    store.drop(ref)
    store.close()


def test_strict_memory_does_not_touch_disk_even_when_full(tmp_path):
    budget = Budget(3 << 20)
    with SegmentStore("memory", budget, keys(), temp_dir=tmp_path) as store:
        baseline = budget.used_bytes
        with pytest.raises(ResourceError):
            segment(store, b"A" * (4 * BLOCK_BYTES))
        assert store.scratch_directory is None
        assert list(tmp_path.iterdir()) == []
        assert budget.used_bytes == baseline


def test_auto_spills_under_pressure_and_retains_logical_reference(tmp_path):
    budget = Budget(25 << 20)
    with SegmentStore("auto", budget, keys(), temp_dir=tmp_path) as store:
        first = segment(store, b"A" * (2 * BLOCK_BYTES))
        assert not store.is_external(first)
        with budget.acquire("pressure", 6 << 20):
            second = segment(store, b"B" * BLOCK_BYTES)
        assert store.is_external(first)
        with store.open_verified(first) as stream:
            assert stream.read(100) == b"A" * 100
        store.drop(first)
        store.drop(second)
    assert budget.used_bytes == 0


def test_failed_spill_preserves_source_and_ownership(tmp_path, monkeypatch):
    import readmixer_py.storage as module
    with SegmentStore("auto", Budget(40 << 20), keys(), temp_dir=tmp_path) as store:
        ref = segment(store, b"source remains resident")
        with monkeypatch.context() as patch:
            patch.setattr(module.os, "fsync", lambda *_: (_ for _ in ()).throw(OSError("injected")))
            with pytest.raises(ResourceError):
                store.spill(ref)
        assert not store.is_external(ref)
        with store.open_verified(ref) as reader:
            assert reader.read() == b"source remains resident"
        assert list(store.scratch_directory.iterdir()) == []


def test_memory_and_pinned_segments_cannot_spill(tmp_path):
    for mode in ("memory", "auto"):
        with SegmentStore(mode, Budget(40 << 20), keys(), temp_dir=tmp_path) as store:
            ref = segment(store)
            with store.open_verified(ref):
                with pytest.raises(ResourceError):
                    store.spill(ref)
                with pytest.raises(ResourceError):
                    store.drop(ref)


def test_replaced_inode_or_symlink_is_never_read_or_removed(tmp_path):
    store = SegmentStore("external", Budget(32 << 20), keys(), temp_dir=tmp_path)
    ref = segment(store)
    original = path(store, ref)
    saved = original.with_suffix(".saved")
    original.rename(saved)
    original.symlink_to(saved)
    try:
        with pytest.raises(IntegrityError):
            store.open_verified(ref)
        with pytest.raises(IntegrityError):
            store.drop(ref)
        assert original.is_symlink() and saved.exists()
    finally:
        original.unlink()
        saved.rename(original)
        store.close()


def test_input_tree_exclusion_and_private_modes_under_permissive_umask(tmp_path):
    root = tmp_path / "input"
    root.mkdir()
    child = root / "child"
    child.mkdir()
    for forbidden in (root, child):
        with pytest.raises(ConfigurationError):
            SegmentStore("external", Budget(32 << 20), keys(), temp_dir=forbidden, input_root=root)
    previous = os.umask(0)
    try:
        with SegmentStore("external", Budget(32 << 20), keys(), temp_dir=tmp_path, input_root=root) as store:
            ref = segment(store)
            assert stat_mode(store.scratch_directory) == 0o700
            assert stat_mode(path(store, ref)) == 0o600
    finally:
        os.umask(previous)


def stat_mode(path):
    return path.stat().st_mode & 0o777


def test_allocations_never_reuse_ids_even_with_repeated_salt(tmp_path, monkeypatch):
    import readmixer_py.storage as module
    monkeypatch.setattr(module.os, "urandom", lambda count: b"S" * count)
    with SegmentStore("memory", Budget(32 << 20), keys(), temp_dir=tmp_path) as store:
        w = store.new_segment(5, observations_identity("verify", 0))
        abandoned_id = w._base.segment_id
        w.abort()
        one, two = segment(store), segment(store)
        assert abandoned_id < one.segment_id < two.segment_id
        assert one.generation_salt == two.generation_salt
        assert store.keys.segment_key(one.segment_id, one.generation_salt, 5, one.logical_identity) != store.keys.segment_key(two.segment_id, two.generation_salt, 5, two.logical_identity)
        baseline = store.budget.used_bytes
        store._next_id = 1 << 128
        with pytest.raises(ResourceError, match="namespace"):
            store.allocate_id()


def _authenticated_data(store, ref, *, decoded, payload, codec=1, sequence=0, reserved=b"\0\0"):
    header = HEADER.pack(MAGIC, ref.run_id, ref.segment_id, ref.generation_salt, ref.purpose, b"\0" * 3)
    key = store.keys.segment_key(ref.segment_id, ref.generation_salt, ref.purpose, ref.logical_identity)
    block = BLOCK_HEADER.pack(0, codec, reserved, sequence, decoded, len(payload))
    cipher = ChaCha20_Poly1305.new(key=key, nonce=b"\0" * 4 + sequence.to_bytes(8, "big"))
    cipher.update(header + block)
    ciphertext, tag = cipher.encrypt_and_digest(payload)
    return header + block + ciphertext + tag


@pytest.mark.parametrize("payload,decoded", [
    (b"bad zlib", 100),
    (zlib.compress(b"A" * 100)[:-1], 100),
    (zlib.compress(b"A" * 100) + b"trailing", 100),
    (zlib.compress(b"A" * 100) + zlib.compress(b"A"), 100),
    (zlib.compress(b"A" * 10000), 100),
])
def test_authenticated_hostile_compression_is_bounded_and_rejected(tmp_path, payload, decoded):
    with SegmentStore("external", Budget(32 << 20), keys(), temp_dir=tmp_path) as store:
        ref = store.detach(segment(store, b"A" * 100))
        encoded = _authenticated_data(store, ref, decoded=decoded, payload=payload)
        # Keep enough physical bytes for a syntactically plausible segment ref.
        encoded += b"\0" * 92
        path(store, ref).write_bytes(encoded)
        changed = replace(ref, physical_bytes=len(encoded))
        with pytest.raises(IntegrityError):
            with store.open_verified(changed) as reader:
                reader.read(1)
        store.drop(ref)


def test_bounded_read_and_cancellation_release_pins(tmp_path):
    cancelled = False
    def check():
        if cancelled:
            raise CancelledError()
    with SegmentStore("memory", Budget(32 << 20), keys(), temp_dir=tmp_path, cancel=check) as store:
        ref = segment(store, b"A" * (3 * BLOCK_BYTES))
        with store.open_verified(ref) as reader:
            with pytest.raises(ResourceError, match="bounded"):
                reader.read()
            cancelled = True
            with pytest.raises(CancelledError):
                reader.read(1)
        assert not store._reader_pins
        store.drop(ref)  # Cleanup does not run cancellation callbacks.


def test_reference_codec_rejects_oversize_truncation_and_bad_identity(tmp_path):
    with SegmentStore("memory", Budget(32 << 20), keys(), temp_dir=tmp_path) as store:
        ref = segment(store)
        data = serialize_segment_ref(ref)
        for malformed in (b"", data[:-1], data + b"x", data + b"x" * 5000):
            with pytest.raises(IntegrityError):
                deserialize_segment_ref(malformed)
        with pytest.raises(IntegrityError):
            serialize_segment_ref(replace(ref, data_block_count=0))


def test_cancelled_cleanup_reader_and_ledger_guard(tmp_path):
    cancelled = False
    with SegmentStore("external", Budget(32 << 20), keys(), temp_dir=tmp_path,
                      cancel=lambda: cancelled) as store:
        ref = segment(store)
        cancelled = True
        with pytest.raises(CancelledError):
            store.open_verified(ref)
        with store.open_verified(ref, cleanup=True) as reader:
            assert reader.read() == b"abc"
        store._ledger_roots = 1
        with pytest.raises(ResourceError, match="ledger"):
            store.close()
        assert path(store, ref).exists()
        store._ledger_roots = 0


def test_failed_spill_stops_future_encryption_attempts(tmp_path, monkeypatch):
    import readmixer_py.storage as module
    with SegmentStore("auto", Budget(40 << 20), keys(), temp_dir=tmp_path) as store:
        ref = segment(store)
        with monkeypatch.context() as patch:
            patch.setattr(module.os, "fsync", lambda *_: (_ for _ in ()).throw(OSError("injected")))
            with pytest.raises(ResourceError):
                store.spill(ref)
        with pytest.raises(ResourceError, match="failed spill"):
            store.spill(ref)
        with pytest.raises(ResourceError, match="failed spill"):
            segment(store)
        with store.open_verified(ref) as reader:
            assert reader.read() == b"abc"


def test_allocation_exhaustion_releases_metadata_lease(tmp_path):
    budget = Budget(32 << 20)
    with SegmentStore("memory", budget, keys(), temp_dir=tmp_path) as store:
        baseline = store.budget.used_bytes
        store._next_id = 1 << 128
        with pytest.raises(ResourceError):
            segment(store)
        assert budget.used_bytes == baseline


def test_scratch_parent_symlink_is_rejected(tmp_path):
    from readmixer_py.errors import PublicationError
    parent = tmp_path / "real"
    parent.mkdir()
    link = tmp_path / "link"
    link.symlink_to(parent, target_is_directory=True)
    with SegmentStore("external", Budget(32 << 20), keys(), temp_dir=link) as store:
        with pytest.raises(PublicationError):
            segment(store)
    assert list(parent.iterdir()) == []


def test_auto_spill_has_pre_admitted_directory_and_codec_overlap(tmp_path):
    budget = Budget(24 << 20)
    with SegmentStore("auto", budget, keys(), temp_dir=tmp_path) as store:
        ref = segment(store, b"A" * BLOCK_BYTES)
        with budget.acquire("full-pressure", 1) as pressure:
            pressure.resize(pressure.bytes + budget.remaining_bytes)
            updated = store.spill(ref)
            assert updated.physical_bytes
            assert store.is_external(ref)


def test_supplied_writer_buffer_lease_transfers_without_double_charge(tmp_path):
    budget = Budget(5 << 20)
    with SegmentStore("memory", budget, keys(), temp_dir=tmp_path) as store:
        baseline = budget.used_bytes
        owned = budget.acquire("caller-buffer", BLOCK_BYTES + 256)
        with store.new_segment(5, observations_identity("verify", 0), owned) as writer:
            assert owned.released
            writer.append(b"abc", item_count=1)
            assert budget.used_bytes < (2 << 20)
            ref = writer.seal()
        assert budget.used_bytes - baseline < 16384
        store.drop(ref)


def test_data_block_reordering_and_deletion_fail(tmp_path):
    with SegmentStore("external", Budget(32 << 20), keys(), temp_dir=tmp_path) as store:
        ref = segment(store, b"A" * BLOCK_BYTES + b"B" * BLOCK_BYTES)
        p = path(store, ref)
        encoded = p.read_bytes()
        first_size = 20 + BLOCK_HEADER.unpack_from(encoded, 76)[5] + 16
        second_start = 76 + first_size
        second_size = 20 + BLOCK_HEADER.unpack_from(encoded, second_start)[5] + 16
        reordered = encoded[:76] + encoded[second_start:second_start+second_size] + encoded[76:second_start] + encoded[second_start+second_size:]
        p.write_bytes(reordered)
        with pytest.raises(IntegrityError, match="sequence"):
            with store.open_verified(ref) as reader:
                reader.read(1)
        p.write_bytes(encoded[:76] + encoded[second_start:])
        with pytest.raises(IntegrityError):
            store.open_verified(ref)


def test_authenticated_seal_must_match_expected_logical_counts(tmp_path):
    with SegmentStore("external", Budget(32 << 20), keys(), temp_dir=tmp_path) as store:
        ref = store.detach(segment(store))
        wrong = replace(ref, item_count=ref.item_count + 1)
        with pytest.raises(IntegrityError, match="expected reference"):
            with store.open_verified(wrong) as reader:
                reader.read()
        store.drop(ref)


def test_cleanup_missing_ok_does_not_relax_read_identity(tmp_path):
    with SegmentStore("memory", Budget(32 << 20), keys(), temp_dir=tmp_path) as store:
        ref = segment(store)
        store.drop(ref)
        store.drop(ref, missing_ok=True)
        with pytest.raises(IntegrityError):
            store.drop(ref)
        with pytest.raises(IntegrityError):
            store.open_verified(ref)
        with pytest.raises(IntegrityError):
            store.drop(replace(ref, run_id=b"x" * 16), missing_ok=True)


@pytest.mark.parametrize("mode", ["memory", "auto", "external"])
def test_atomic_pair_can_cross_blocks_but_never_segments(tmp_path, mode):
    from readmixer_py.crypto import pairs_identity
    from readmixer_py.paircodec import decode_pair_view
    length = 300_000
    lines = (b"@opaque/1", b"A" * length, b"+meta", b"!" * length,
             b"@opaque/2", b"C" * length, b"+meta", b"#" * length)
    frame = struct.pack("<IIQ8I", 48 + sum(map(len, lines)), 4, 17, *map(len, lines)) + b"".join(lines)
    with SegmentStore(mode, Budget(40 << 20), keys(), temp_dir=tmp_path) as store:
        with store.new_segment(1, pairs_identity((3,), 0, 0)) as writer:
            with pytest.raises(IntegrityError):
                writer.append_pair(frame[:-1])
            writer.append_pair(frame)
            ref = writer.seal()
        assert ref.item_count == 1 and ref.data_block_count == 2
        with store.open_verified(ref) as reader:
            rebuilt = reader.read(len(frame))
            pair = decode_pair_view(rebuilt)
            assert (pair.sample_index, pair.original_pair_index) == (4, 17)
            assert tuple(bytes(line) for line in (*pair.r1.lines, *pair.r2.lines)) == lines


def test_partial_enospc_preserves_accounting_and_removes_owned_file(tmp_path, monkeypatch):
    import errno
    import readmixer_py.storage as module
    budget, disk = Budget(32 << 20), DiskBudget()
    original = module.os.write
    calls = 0
    def failing_write(fd, data):
        nonlocal calls
        calls += 1
        if calls == 1:
            return original(fd, data[:9])
        raise OSError(errno.ENOSPC, "injected full filesystem")
    with SegmentStore("external", budget, keys(), temp_dir=tmp_path, disk_budget=disk) as store:
        with monkeypatch.context() as patch:
            patch.setattr(module.os, "write", failing_write)
            with pytest.raises(ResourceError, match="write failed"):
                segment(store)
        assert list(store.scratch_directory.iterdir()) == []
        assert disk.pending_bytes == disk.committed_bytes == 0
        assert store.resident_segments == 0
    assert budget.used_bytes == 0


def test_transient_path_walk_descriptors_are_admitted(tmp_path):
    with pytest.raises(ResourceError, match="descriptors"):
        SegmentStore("auto", Budget(32 << 20, descriptors=5), keys(), temp_dir=tmp_path)
    budget = Budget(32 << 20, descriptors=4)
    with SegmentStore("external", budget, keys(), temp_dir=tmp_path) as store:
        with pytest.raises(ResourceError, match="descriptors"):
            segment(store)
        assert store.scratch_directory is None
    assert budget.used_bytes == budget.used_descriptors == 0
    with SegmentStore("memory", Budget(16 << 20, descriptors=0), keys(), temp_dir=tmp_path) as store:
        ref = segment(store)
        with store.open_verified(ref) as reader:
            assert reader.read() == b"abc"


def test_invalid_cancel_callback_is_rejected_before_admission(tmp_path):
    budget = Budget(32 << 20)
    with pytest.raises(ConfigurationError, match="callable"):
        SegmentStore("auto", budget, keys(), temp_dir=tmp_path, cancel=True)
    assert budget.used_bytes == 0


@pytest.mark.parametrize("failure", ["read", "eof", "close"])
def test_reader_io_errors_are_typed_and_release_pins(tmp_path, failure):
    with SegmentStore("external", Budget(32 << 20), keys(), temp_dir=tmp_path) as store:
        ref = segment(store)
        reader = store.open_verified(ref)
        underlying = reader._file
        class FaultyFile:
            def read(self, size):
                if failure == "read" or failure == "eof" and size == 1:
                    raise OSError("injected read failure")
                return underlying.read(size)
            def close(self):
                underlying.close()
                if failure == "close":
                    raise OSError("injected close failure")
        reader._file = FaultyFile()
        with pytest.raises((IntegrityError, ResourceError)):
            reader.close()
        assert not store._reader_pins
        assert reader.lease.released
        store.drop(ref)


def test_reader_primary_integrity_error_survives_close_failure(tmp_path):
    with SegmentStore("external", Budget(32 << 20), keys(), temp_dir=tmp_path) as store:
        ref = segment(store)
        reader = store.open_verified(ref)
        underlying = reader._file
        class FaultyFile:
            def read(self, _size):
                raise OSError("primary read failure")
            def close(self):
                underlying.close()
                raise OSError("secondary close failure")
        reader._file = FaultyFile()
        with pytest.raises(IntegrityError, match="read failed") as found:
            reader.close()
        assert "close also failed" in found.value.__notes__[0]
        assert not store._reader_pins


def test_owned_segment_replaced_by_fifo_cannot_block_open(tmp_path, monkeypatch):
    with SegmentStore("external", Budget(32 << 20), keys(), temp_dir=tmp_path) as store:
        ref = segment(store)
        original = path(store, ref)
        saved = original.with_suffix(".saved")
        original.rename(saved)
        os.mkfifo(original, 0o600)
        real_open = os.open
        def guarded(name, flags, *args, **kwargs):
            if name == store._name(ref):
                assert flags & os.O_NONBLOCK, "FIFO replacement would block before fstat"
            return real_open(name, flags, *args, **kwargs)
        try:
            with monkeypatch.context() as patch:
                patch.setattr(os, "open", guarded)
                with pytest.raises(IntegrityError):
                    store.open_verified(ref)
            assert not store._reader_pins
        finally:
            original.unlink()
            saved.rename(original)
