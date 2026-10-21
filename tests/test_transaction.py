"""Faults target actual filesystem operations, including last-marker durability."""

import errno
import hashlib
import os
from pathlib import Path
import stat

import pytest

from readmixer_py import transaction
from readmixer_py.disk import DiskBudget
from readmixer_py.errors import CancelledError, ConfigurationError, InputError, IntegrityError, PublicationError, ResourceError
from readmixer_py.resources import Budget
from readmixer_py.transaction import OutputTransaction


def begin(tmp_path, **kwargs):
    budget = kwargs.pop("budget", Budget(64 << 20, descriptors=32))
    disk = kwargs.pop("disk_budget", DiskBudget())
    return OutputTransaction.begin(tmp_path, "manifest", budget, disk_budget=disk, **kwargs)


def registered(tx, name="part.bgz", payload=b"private final-format data"):
    artifact = tx.create_partial(name)
    artifact.write(payload)
    artifact.close()
    tx.register_closed(artifact, len(payload), hashlib.sha256(payload).digest())
    return artifact


def test_private_creation_and_durable_publication_under_permissive_umask(tmp_path):
    previous = os.umask(0)
    try:
        tx = begin(tmp_path)
        assert stat.S_IMODE(os.stat(tx._stage.path).st_mode) == 0o700
        artifact = registered(tx)
        assert stat.S_IMODE(os.stat(tx._stage.path / artifact.name).st_mode) == 0o600
        with tx.open_owned_partial(artifact) as stream:
            assert stream.read() == b"private final-format data"
        report = tx.publish_all(b'{"complete":true}')
    finally:
        os.umask(previous)
    assert report.committed and report.cleanup_complete and not report.warnings
    assert sorted(path.name for path in tmp_path.iterdir()) == ["manifest.json", "part.bgz"]
    assert (tmp_path / "manifest.json").read_bytes() == b'{"complete":true}'
    assert tx.budget.used_bytes == tx.budget.used_descriptors == 0
    assert tx.disk_budget.pending_bytes == 0
    assert tx.disk_budget.committed_bytes == len(b"private final-format data") + len(b'{"complete":true}')


def test_artifact_links_precede_last_marker_and_directory_syncs(tmp_path, monkeypatch):
    tx = begin(tmp_path)
    registered(tx, "one.bgz")
    registered(tx, "two.bgz")
    linked = []
    synced = []
    real_link, real_sync = os.link, os.fsync
    destination_fd = tx._destination.fd

    def link(source, dest, **kwargs):
        linked.append(dest)
        if dest == "manifest.json":
            assert linked == ["two.bgz", "one.bgz", "manifest.json"]
            assert any(fd == destination_fd for fd in synced)
            assert (tmp_path / "one.bgz").exists()
        else:
            assert not (tmp_path / "manifest.json").exists()
        return real_link(source, dest, **kwargs)

    def sync(fd):
        synced.append(fd)
        return real_sync(fd)

    monkeypatch.setattr(transaction.os, "link", link)
    monkeypatch.setattr(transaction.os, "fsync", sync)
    assert tx.publish_all(b"marker").committed
    assert linked == ["two.bgz", "one.bgz", "manifest.json"]


@pytest.mark.parametrize("name", ["../escaped", "/absolute", "nested/file", "..", ".", "bad\\name", "with space", "x" * 256, "x\x00"])
def test_basename_validation_before_file_creation(tmp_path, name):
    with begin(tmp_path) as tx:
        with pytest.raises(ConfigurationError):
            tx.create_partial(name)
        assert not list(tx._stage.path.iterdir())
    assert not list(tmp_path.iterdir())


def test_reserved_and_duplicate_names_are_rejected(tmp_path):
    with begin(tmp_path) as tx:
        with pytest.raises(PublicationError, match="reserved"):
            tx.create_partial("manifest.json")
        artifact = tx.create_partial("one")
        with pytest.raises(PublicationError, match="duplicate"):
            tx.create_partial("one")
        artifact.close()


@pytest.mark.parametrize("leaf_symlink", [True, False])
def test_destination_symlinks_rejected(tmp_path, leaf_symlink):
    actual = tmp_path / "actual"
    actual.mkdir()
    (actual / "child").mkdir()
    link = tmp_path / "linked"
    link.symlink_to(actual, target_is_directory=True)
    destination = link if leaf_symlink else link / "child"
    with pytest.raises(PublicationError):
        begin(destination)
    assert not list((actual / "child").iterdir())


def test_group_writable_destination_rejected_without_chmod(tmp_path):
    tmp_path.chmod(0o775)
    with pytest.raises(PublicationError, match="private directory"):
        begin(tmp_path)
    assert stat.S_IMODE(tmp_path.stat().st_mode) == 0o775
    tmp_path.chmod(0o755)
    tx = begin(tmp_path)
    tx.abort()


def test_output_input_root_exclusions_and_allowed_ancestor(tmp_path):
    root = tmp_path / "input"
    root.mkdir()
    child = root / "nested"
    child.mkdir()
    for destination in (root, child):
        with pytest.raises(InputError, match="outside"):
            begin(destination, input_root=root)
    with begin(tmp_path, input_root=root) as tx:
        assert root not in tx._stage.path.parents
    assert list(root.iterdir()) == [child]


def test_wrong_owner_rejected(tmp_path, monkeypatch):
    monkeypatch.setattr(transaction.os, "getuid", lambda: tmp_path.stat().st_uid + 1)
    with pytest.raises(PublicationError):
        begin(tmp_path)


@pytest.mark.parametrize("replacement", ["file", "symlink", "mode"])
def test_open_owned_partial_preserves_replaced_files_and_symlinks(tmp_path, replacement):
    tx = begin(tmp_path)
    artifact = registered(tx)
    partial = tx._stage.path / artifact.name
    foreign = tmp_path / "foreign"
    foreign.write_bytes(b"foreign")
    if replacement == "mode":
        partial.chmod(0o644)
    else:
        # Retain the original inode while creating its replacement, preventing
        # filesystem inode reuse from making this an ambiguous identity test.
        held = os.open(partial, os.O_RDONLY)
        partial.unlink()
        if replacement == "file":
            partial.write_bytes(b"foreign")
        else:
            partial.symlink_to(foreign)
    with pytest.raises(PublicationError):
        tx.open_owned_partial(artifact)
    warnings = tx.abort()
    assert foreign.read_bytes() == b"foreign"
    if replacement != "mode":
        assert partial.exists()
        assert warnings
        os.close(held)


def test_closed_registration_validates_size_digest_and_state(tmp_path):
    with begin(tmp_path) as tx:
        artifact = tx.create_partial("one")
        artifact.write(b"abc")
        with pytest.raises(PublicationError, match="closed"):
            tx.register_closed(artifact, 3, hashlib.sha256(b"abc").digest())
        artifact.close()
        with pytest.raises(IntegrityError, match="length"):
            tx.register_closed(artifact, 4, hashlib.sha256(b"abc").digest())
        with pytest.raises(IntegrityError, match="digest"):
            tx.register_closed(artifact, 3, hashlib.sha256(b"abd").digest())
        tx.register_closed(artifact, 3, hashlib.sha256(b"abc").hexdigest())
        with pytest.raises(PublicationError, match="already registered"):
            tx.register_closed(artifact, 3, hashlib.sha256(b"abc").digest())


def test_changed_same_size_partial_rejected_before_link(tmp_path):
    tx = begin(tmp_path)
    artifact = registered(tx, payload=b"first")
    (tx._stage.path / artifact.name).write_bytes(b"other")
    with pytest.raises(IntegrityError, match="changed"):
        tx.publish_all(b"marker")
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("collision", ["artifact", "marker"])
def test_existing_final_name_never_overwritten(tmp_path, collision):
    tx = begin(tmp_path)
    registered(tx)
    existing = tmp_path / ("part.bgz" if collision == "artifact" else "manifest.json")
    existing.write_bytes(b"foreign")
    original_inode = existing.stat().st_ino
    with pytest.raises(PublicationError) as failure:
        tx.publish_all(b"marker")
    assert isinstance(failure.value.__cause__, FileExistsError)
    assert existing.read_bytes() == b"foreign"
    assert existing.stat().st_ino == original_inode
    assert list(tmp_path.iterdir()) == [existing]
    assert tx.disk_budget.committed_bytes == 0


@pytest.mark.parametrize("fault", ["link", "directory_sync", "marker_sync"])
def test_precommit_faults_roll_back_own_names_only(tmp_path, monkeypatch, fault):
    tx = begin(tmp_path)
    registered(tx, "one")
    registered(tx, "two")
    foreign = tmp_path / "unrelated"
    foreign.write_bytes(b"keep")
    real_link, real_sync = os.link, os.fsync
    directory_syncs = 0
    marker_linked = False
    removal_order = []
    real_unlink = os.unlink

    def link(source, dest, **kwargs):
        nonlocal marker_linked
        if fault == "link" and dest == "two":
            raise OSError(errno.EXDEV, "injected cross-filesystem failure")
        result = real_link(source, dest, **kwargs)
        marker_linked = dest == "manifest.json" or marker_linked
        return result

    def sync(fd):
        nonlocal directory_syncs
        if fd == tx._destination.fd:
            directory_syncs += 1
            if directory_syncs == 1 and fault == "directory_sync":
                raise OSError(errno.EINVAL, "directory sync unsupported")
            if marker_linked and fault == "marker_sync" and directory_syncs == 2:
                raise OSError(errno.EIO, "marker durability unknown")
        return real_sync(fd)

    def unlink(name, **kwargs):
        if kwargs.get("dir_fd") == tx._destination.fd:
            removal_order.append(name)
        return real_unlink(name, **kwargs)

    monkeypatch.setattr(transaction.os, "link", link)
    monkeypatch.setattr(transaction.os, "fsync", sync)
    monkeypatch.setattr(transaction.os, "unlink", unlink)
    with pytest.raises(PublicationError) as error:
        tx.publish_all(b"marker")
    assert isinstance(error.value.__cause__, OSError)
    assert foreign.read_bytes() == b"keep"
    assert list(tmp_path.iterdir()) == [foreign]
    assert tx.disk_budget.committed_bytes == tx.disk_budget.pending_bytes == 0
    assert tx.budget.used_bytes == tx.budget.used_descriptors == 0
    if fault == "marker_sync":
        assert removal_order[0] == "manifest.json"
        assert "indeterminate" in str(error.value)


def test_failed_marker_rollback_preserves_primary_and_reports_indeterminate(tmp_path, monkeypatch):
    tx = begin(tmp_path)
    registered(tx)
    real_sync, real_unlink = os.fsync, os.unlink
    failed = False

    def sync(fd):
        nonlocal failed
        if not failed and fd == tx._destination.fd and (tmp_path / "manifest.json").exists():
            failed = True
            raise OSError(errno.EIO, "primary sync failure")
        return real_sync(fd)

    def unlink(name, **kwargs):
        if name == "manifest.json" and kwargs.get("dir_fd") == tx._destination.fd:
            raise OSError(errno.EACCES, "secondary cleanup failure")
        return real_unlink(name, **kwargs)

    monkeypatch.setattr(transaction.os, "fsync", sync)
    monkeypatch.setattr(transaction.os, "unlink", unlink)
    with pytest.raises(PublicationError) as error:
        tx.publish_all(b"marker")
    assert error.value.__cause__.errno == errno.EIO
    assert any("indeterminate" in note for note in error.value.__notes__)
    assert (tmp_path / "manifest.json").exists()
    assert not (tmp_path / "part.bgz").exists()


def test_rollback_does_not_unlink_replaced_final_inode(tmp_path, monkeypatch):
    tx = begin(tmp_path)
    registered(tx, "one")
    registered(tx, "two")
    real_link = os.link

    def link(source, dest, **kwargs):
        if dest == "one":
            first = tmp_path / "two"
            first.unlink()
            first.write_bytes(b"foreign replacement")
            raise OSError(errno.EIO, "injected link failure")
        return real_link(source, dest, **kwargs)

    monkeypatch.setattr(transaction.os, "link", link)
    with pytest.raises(PublicationError) as error:
        tx.publish_all(b"marker")
    assert (tmp_path / "two").read_bytes() == b"foreign replacement"
    assert any("replaced names" in note for note in error.value.__notes__)
    assert not (tmp_path / "manifest.json").exists()


def test_cleanup_failure_after_durable_commit_keeps_all_final_files(tmp_path, monkeypatch):
    tx = begin(tmp_path)
    registered(tx)
    real_unlink = os.unlink

    def unlink(name, **kwargs):
        if name == "manifest.json" and kwargs.get("dir_fd") == tx._stage.fd:
            raise OSError(errno.EACCES, "cleanup only")
        return real_unlink(name, **kwargs)

    monkeypatch.setattr(transaction.os, "unlink", unlink)
    report = tx.publish_all(b"marker")
    assert report.committed and not report.cleanup_complete
    assert report.warnings
    assert (tmp_path / "manifest.json").read_bytes() == b"marker"
    assert (tmp_path / "part.bgz").read_bytes() == b"private final-format data"
    tx.abort(RuntimeError("later failure"))
    assert (tmp_path / "manifest.json").exists()
    assert tx.budget.used_bytes == tx.budget.used_descriptors == 0


def test_artifact_count_physical_caps_and_metadata_caps_before_growth(tmp_path):
    with begin(tmp_path, max_artifacts=2, max_artifact_bytes=65_564) as tx:
        artifact = tx.create_partial("one")
        artifact.write(b"x" * 65_564)
        with pytest.raises(ResourceError, match="physical byte cap"):
            artifact.write(b"x")
        assert (tx._stage.path / "one").stat().st_size == 65_564
        with pytest.raises(ResourceError, match="inventory"):
            tx.create_partial("two")
    with begin(tmp_path, metadata_bytes=4096) as tx:
        tx.create_partial("one").close()
        with pytest.raises(ResourceError, match="metadata allowance"):
            tx.create_partial("two")
        with pytest.raises(ResourceError, match="metadata cap"):
            tx.publish_all(b"x" * 4097)
        assert not (tmp_path / "manifest.json").exists()


def test_descriptor_admission_precedes_new_file(tmp_path):
    budget = Budget(64 << 20, descriptors=4)
    with begin(tmp_path, budget=budget) as tx:
        with pytest.raises(ResourceError, match="descriptors"):
            tx.create_partial("one")
        assert not list(tx._stage.path.iterdir())
    assert budget.used_descriptors == 0


def test_partial_write_enospc_charges_prefix_then_cleanup_releases(tmp_path, monkeypatch):
    tx = begin(tmp_path)
    artifact = tx.create_partial("one")
    real_write = os.write
    writes = 0

    def write(fd, data):
        nonlocal writes
        writes += 1
        if writes == 1:
            return real_write(fd, data[:2])
        raise OSError(errno.ENOSPC, "injected disk full")

    monkeypatch.setattr(transaction.os, "write", write)
    with pytest.raises(PublicationError) as error:
        artifact.write(b"data")
    assert error.value.__cause__.errno == errno.ENOSPC
    assert tx.disk_budget.committed_bytes == 2
    tx.abort(error.value)
    assert tx.disk_budget.committed_bytes == 0
    assert not list(tmp_path.iterdir())


def test_cancel_after_artifact_link_rolls_back(tmp_path, monkeypatch):
    cancelled = False
    tx = begin(tmp_path, cancel=lambda: cancelled)
    registered(tx)
    real_link = os.link

    def link(*args, **kwargs):
        nonlocal cancelled
        result = real_link(*args, **kwargs)
        cancelled = True
        return result

    monkeypatch.setattr(transaction.os, "link", link)
    with pytest.raises(CancelledError):
        tx.publish_all(b"marker")
    assert not list(tmp_path.iterdir())
    assert tx.budget.used_bytes == 0


def test_context_preserves_original_exception_and_closes_writer(tmp_path):
    original = RuntimeError("primary")
    with pytest.raises(RuntimeError) as error:
        with begin(tmp_path) as tx:
            artifact = tx.create_partial("one")
            artifact.write(b"data")
            raise original
    assert error.value is original
    assert artifact.closed
    assert not list(tmp_path.iterdir())
    assert tx.budget.used_bytes == tx.budget.used_descriptors == 0


def test_recovery_receipt_and_foreign_artifact_handles(tmp_path):
    tx = OutputTransaction.begin(tmp_path, "recovery-receipt", Budget(64 << 20))
    separate = tmp_path / "other"
    separate.mkdir(mode=0o700)
    with begin(separate) as other:
        artifact = registered(other)
        with pytest.raises(PublicationError, match="belong"):
            tx.register_closed(artifact, artifact.bytes_written, hashlib.sha256(b"").digest())
    report = tx.publish_all(b"all selected samples")
    assert report.completion_basename == "recovery-receipt.json"
    assert (tmp_path / report.completion_basename).read_bytes() == b"all selected samples"


def test_preexisting_hardlink_to_staging_inode_is_not_owned_by_transaction(tmp_path):
    tx = begin(tmp_path)
    artifact = tx.create_partial("part.bgz")
    payload = b"private final-format data"
    artifact.write(payload)
    artifact.close()
    existing = tmp_path / artifact.name
    os.link(tx._stage.path / artifact.name, existing)
    tx.register_closed(artifact, len(payload), hashlib.sha256(payload).digest())
    with pytest.raises(PublicationError):
        tx.publish_all(b"marker")
    assert existing.read_bytes() == b"private final-format data"
    assert list(tmp_path.iterdir()) == [existing]


def test_interrupt_immediately_after_successful_link_uses_pending_intent(tmp_path, monkeypatch):
    tx = begin(tmp_path)
    registered(tx)
    real_link = os.link

    def link(*args, **kwargs):
        real_link(*args, **kwargs)
        raise KeyboardInterrupt()

    monkeypatch.setattr(transaction.os, "link", link)
    with pytest.raises(KeyboardInterrupt):
        tx.publish_all(b"marker")
    assert not list(tmp_path.iterdir())
    assert tx.disk_budget.committed_bytes == 0


def test_create_open_failure_releases_pending_descriptor_admission(tmp_path, monkeypatch):
    tx = begin(tmp_path)
    real_open = os.open
    before = tx.budget.used_descriptors

    def open_file(path, flags, *args, **kwargs):
        if flags & os.O_CREAT:
            raise OSError(errno.EACCES, "injected open failure")
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(transaction.os, "open", open_file)
    with pytest.raises(PublicationError, match="exclusive partial") as failure:
        tx.create_partial("one")
    assert failure.value.__cause__.errno == errno.EACCES
    assert tx.budget.used_descriptors == before
    tx.abort()
    assert not list(tmp_path.iterdir())


def test_completion_creation_failure_rolls_back_all_final_artifacts(tmp_path, monkeypatch):
    tx = begin(tmp_path)
    registered(tx)
    real_open = os.open

    def open_file(path, flags, *args, **kwargs):
        if path == "manifest.json" and flags & os.O_CREAT:
            raise OSError(errno.ENOSPC, "injected marker creation failure")
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(transaction.os, "open", open_file)
    with pytest.raises(PublicationError) as failure:
        tx.publish_all(b"marker")
    assert failure.value.__cause__.errno == errno.ENOSPC
    assert not list(tmp_path.iterdir())
    assert tx.disk_budget.committed_bytes == 0


def test_writer_fsync_failure_still_closes_descriptor_and_preserves_cause(tmp_path, monkeypatch):
    tx = begin(tmp_path)
    artifact = tx.create_partial("one")
    artifact.write(b"data")
    writer_fd = artifact._fd
    real_sync = os.fsync

    def sync(fd):
        if fd == writer_fd:
            raise OSError(errno.EIO, "injected file sync failure")
        return real_sync(fd)

    monkeypatch.setattr(transaction.os, "fsync", sync)
    with pytest.raises(PublicationError) as failure:
        artifact.close()
    assert artifact.closed
    assert failure.value.__cause__.errno == errno.EIO
    tx.abort(failure.value)
    assert not list(tmp_path.iterdir())
    assert tx.budget.used_descriptors == 0


def test_open_reader_failure_preserves_cause_and_releases_descriptor_lease(tmp_path, monkeypatch):
    tx = begin(tmp_path)
    artifact = registered(tx)
    real_open = os.open
    before = tx.budget.used_descriptors

    def open_file(path, flags, *args, **kwargs):
        if path == artifact.name:
            raise OSError(errno.EACCES, "injected owned reader failure")
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(transaction.os, "open", open_file)
    with pytest.raises(PublicationError) as failure:
        tx.open_owned_partial(artifact)
    assert failure.value.__cause__.errno == errno.EACCES
    assert tx.budget.used_descriptors == before
    tx.abort()


def test_hostile_preexisting_partial_symlink_is_never_followed_or_removed(tmp_path):
    tx = begin(tmp_path)
    target = tmp_path / "foreign"
    target.write_bytes(b"keep")
    trap = tx._stage.path / "one"
    trap.symlink_to(target)
    with pytest.raises(PublicationError):
        tx.create_partial("one")
    warnings = tx.abort()
    assert trap.is_symlink()
    assert target.read_bytes() == b"keep"
    assert warnings


def test_destination_becomes_writable_before_growth_fails_closed(tmp_path):
    tx = begin(tmp_path)
    artifact = tx.create_partial("one")
    tmp_path.chmod(0o777)
    with pytest.raises(PublicationError):
        artifact.write(b"private")
    assert artifact.bytes_written == 0
    tmp_path.chmod(0o700)
    tx.abort()


def test_registering_foreign_digest_does_not_publish_unregistered_artifact(tmp_path):
    with begin(tmp_path) as tx:
        artifact = tx.create_partial("one")
        artifact.close()
        with pytest.raises(ConfigurationError, match="SHA-256"):
            tx.register_closed(artifact, 0, b"bad")
        with pytest.raises(PublicationError, match="all artifacts"):
            tx.publish_all(b"marker")
        assert not (tmp_path / "manifest.json").exists()


def test_postcommit_sync_cleanup_failure_is_bounded_warning(tmp_path, monkeypatch):
    tx = begin(tmp_path)
    registered(tx)
    real_sync = os.fsync

    def sync(fd):
        if tx._state == "committed":
            raise OSError(errno.EIO, "injected cleanup sync failure")
        return real_sync(fd)

    monkeypatch.setattr(transaction.os, "fsync", sync)
    report = tx.publish_all(b"marker")
    assert report.committed and not report.cleanup_complete
    assert 1 <= len(report.warnings) <= 4
    assert (tmp_path / "manifest.json").exists()
    assert (tmp_path / "part.bgz").exists()


def test_begin_resource_rejection_and_cancel_leave_no_private_child(tmp_path):
    with pytest.raises(ResourceError):
        begin(tmp_path, budget=Budget(10))
    with pytest.raises(CancelledError):
        begin(tmp_path, cancel=lambda: True)
    assert not list(tmp_path.iterdir())


def test_close_secondary_failure_does_not_replace_primary_fsync_failure(tmp_path, monkeypatch):
    tx = begin(tmp_path)
    artifact = tx.create_partial("one")
    artifact.write(b"data")
    writer_fd = artifact._fd
    real_sync, real_close = os.fsync, os.close

    def sync(fd):
        if fd == writer_fd:
            raise OSError(errno.EIO, "primary fsync failure")
        return real_sync(fd)

    def close(fd):
        real_close(fd)
        if fd == writer_fd:
            raise OSError(errno.EBADF, "secondary close failure")

    monkeypatch.setattr(transaction.os, "fsync", sync)
    monkeypatch.setattr(transaction.os, "close", close)
    with pytest.raises(PublicationError) as failure:
        artifact.close()
    assert failure.value.__cause__.errno == errno.EIO
    assert "partial output descriptor cleanup failed" in failure.value.__notes__
    monkeypatch.setattr(transaction.os, "close", real_close)
    monkeypatch.setattr(transaction.os, "fsync", real_sync)
    tx.abort(failure.value)
    assert tx.budget.used_descriptors == 0
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("signal_number", [2, 15, None])
@pytest.mark.parametrize("boundary", ["unlink", "directory_check"])
def test_postcommit_interrupt_propagates_with_marker_and_preserves_outputs(tmp_path, monkeypatch, signal_number, boundary):
    tx = begin(tmp_path)
    registered(tx)
    interrupted = CancelledError(signal=signal_number) if signal_number is not None else KeyboardInterrupt()
    raised = [False]
    real_unlink, real_check = os.unlink, tx._destination.check

    def trigger():
        if tx._state == "committed" and not raised[0]:
            raised[0] = True
            raise interrupted

    def unlink(name, **kwargs):
        if kwargs.get("dir_fd") == tx._stage.fd:
            trigger()
        return real_unlink(name, **kwargs)

    def check():
        trigger()
        return real_check()

    if boundary == "unlink":
        monkeypatch.setattr(transaction.os, "unlink", unlink)
    else:
        monkeypatch.setattr(tx._destination, "check", check)
    with pytest.raises(type(interrupted)) as failure:
        tx.publish_all(b"marker")
    assert failure.value is interrupted
    if signal_number is not None:
        assert failure.value.signal == signal_number
    assert any("durably committed" in note and "manifest.json" in note for note in failure.value.__notes__)
    assert not any("indeterminate" in note for note in failure.value.__notes__)
    assert (tmp_path / "manifest.json").read_bytes() == b"marker"
    assert (tmp_path / "part.bgz").read_bytes() == b"private final-format data"
    assert sorted(path.name for path in tmp_path.iterdir()) == ["manifest.json", "part.bgz"]
    assert tx._state == "committed" and tx._resources_closed
    assert tx.budget.used_bytes == tx.budget.used_descriptors == 0


@pytest.mark.parametrize("interrupt_type", [CancelledError, KeyboardInterrupt])
def test_interrupt_immediately_after_commit_transition_does_not_report_indeterminate(tmp_path, monkeypatch, interrupt_type):
    tx = begin(tmp_path)
    registered(tx)
    interrupted = interrupt_type()
    triggered = [False]

    def assign(instance, name, value):
        object.__setattr__(instance, name, value)
        if instance is tx and name == "_state" and value == "committed" and not triggered[0]:
            triggered[0] = True
            raise interrupted

    monkeypatch.setattr(OutputTransaction, "__setattr__", assign)
    with pytest.raises(interrupt_type) as failure:
        tx.publish_all(b"marker")
    assert failure.value is interrupted
    assert tx._state == "committed"
    assert any("durably committed" in note and "manifest.json" in note for note in failure.value.__notes__)
    assert not any("indeterminate" in note for note in failure.value.__notes__)
    assert (tmp_path / "manifest.json").read_bytes() == b"marker"
    assert (tmp_path / "part.bgz").exists()
    assert tx.budget.used_bytes == tx.budget.used_descriptors == 0


def test_cleanup_interrupt_cannot_replace_context_primary_error(tmp_path, monkeypatch):
    primary = RuntimeError("primary work failure")
    interrupted = CancelledError(signal=15)
    tx = begin(tmp_path)
    artifact = registered(tx)
    real_unlink = os.unlink
    triggered = [False]

    def unlink(name, **kwargs):
        if name == artifact.name and not triggered[0]:
            triggered[0] = True
            raise interrupted
        return real_unlink(name, **kwargs)

    monkeypatch.setattr(transaction.os, "unlink", unlink)
    with pytest.raises(RuntimeError) as failure:
        with tx:
            raise primary
    assert failure.value is primary
    assert "output cleanup also failed; primary error preserved" in primary.__notes__
    assert not list(tmp_path.iterdir())
    assert tx.budget.used_bytes == tx.budget.used_descriptors == 0


def test_postcommit_context_primary_keeps_marker_context_if_cleanup_raises(tmp_path, monkeypatch):
    tx = begin(tmp_path)
    registered(tx)
    tx.publish_all(b"marker")
    primary = RuntimeError("postcommit caller failure")

    def cleanup(**kwargs):
        raise KeyboardInterrupt()

    monkeypatch.setattr(tx, "_cleanup", cleanup)
    with pytest.raises(RuntimeError) as failure:
        with tx:
            raise primary
    assert failure.value is primary
    assert any("durably committed" in note and "manifest.json" in note for note in primary.__notes__)
    assert (tmp_path / "manifest.json").read_bytes() == b"marker"
    assert (tmp_path / "part.bgz").exists()


def test_success_report_is_allocated_before_publication(tmp_path, monkeypatch):
    tx = begin(tmp_path)
    registered(tx)
    real_report = transaction.PublicationReport
    construction_states = []

    def report(*args, **kwargs):
        construction_states.append(tx._state)
        assert tx._state != "committed"
        return real_report(*args, **kwargs)

    monkeypatch.setattr(transaction, "PublicationReport", report)
    assert tx.publish_all(b"marker").committed
    assert construction_states == ["open"]


def test_owned_partial_replaced_by_fifo_cannot_block_open(tmp_path, monkeypatch):
    tx = begin(tmp_path)
    artifact = registered(tx)
    original = tx._stage.path / artifact.name
    saved = original.with_suffix(".saved")
    original.rename(saved)
    os.mkfifo(original, 0o600)
    real_open = os.open
    def guarded(name, flags, *args, **kwargs):
        if name == artifact.name:
            assert flags & os.O_NONBLOCK, "FIFO replacement would block before fstat"
        return real_open(name, flags, *args, **kwargs)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(os, "open", guarded)
            with pytest.raises(PublicationError):
                tx.open_owned_partial(artifact)
    finally:
        original.unlink()
        saved.rename(original)
        tx.abort()
    assert tx.budget.used_bytes == tx.budget.used_descriptors == 0
