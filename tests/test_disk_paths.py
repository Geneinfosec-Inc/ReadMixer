"""P03-T06 / P05-T04 actual-filesystem admission and ownership boundaries."""

import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from readmixer_py.disk import DiskBudget
from readmixer_py.errors import InputError, PublicationError, ResourceError
from readmixer_py.paths import open_directory, validate_basename


def facts(free):
    return SimpleNamespace(f_bavail=free, f_frsize=1)


def test_shared_filesystem_reserves_pending_without_double_charging_written(tmp_path, monkeypatch):
    free = [64 * 1024**2 + 100]
    monkeypatch.setattr(os, "fstatvfs", lambda fd: facts(free[0]))
    disk = DiskBudget()
    with open_directory(tmp_path) as directory:
        first = disk.reserve(directory.fd, 60)
        with pytest.raises(ResourceError):
            disk.reserve(directory.fd, 41)
        first.commit()
        free[0] -= 60  # A write has consumed actual filesystem capacity.
        with disk.reserve(directory.fd, 40) as second:
            second.commit()
        assert disk.committed_bytes == disk.peak_bytes == 100
        disk.release(directory.fd, 100)
        assert disk.committed_bytes == disk.pending_bytes == 0


def test_failed_query_and_partial_write_accounting(tmp_path, monkeypatch):
    disk = DiskBudget()
    with open_directory(tmp_path) as directory:
        with disk.reserve(directory.fd, 100) as ticket:
            ticket.commit(25)  # Caller observed a short write before failure.
        assert disk.committed_bytes == 25
        monkeypatch.setattr(os, "fstatvfs", lambda fd: (_ for _ in ()).throw(OSError("injected")))
        with pytest.raises(ResourceError, match="query"):
            disk.reserve(directory.fd, 1)
        disk.release(directory.fd, 25)  # Deletion accounting does not query free space.
    assert disk.pending_bytes == disk.committed_bytes == 0


def test_reservation_abort_and_bounded_registry(tmp_path):
    disk = DiskBudget(max_reservations=1)
    with open_directory(tmp_path) as directory:
        with pytest.raises(RuntimeError):
            with disk.reserve(directory.fd, 11):
                with pytest.raises(ResourceError, match="registry"):
                    disk.reserve(directory.fd, 1)
                raise RuntimeError("caller failed")
        assert disk.pending_bytes == disk.committed_bytes == 0
        with disk.reserve(directory.fd, 1) as ticket:
            with pytest.raises(ResourceError):
                ticket.commit(2)
        assert disk.pending_bytes == 0


def test_input_tree_exclusion_before_creation(tmp_path):
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    for target in (inputs, inputs / "child"):
        with pytest.raises(InputError):
            open_directory(target, input_root=inputs)
    assert not list(inputs.iterdir())
    with open_directory(tmp_path, input_root=inputs) as parent:
        child = parent.new_private_child()
        assert child.path.parent == tmp_path and inputs not in child.path.parents
        child.remove_if_empty()


def test_private_child_modes_and_foreign_replacement(tmp_path):
    old_umask = os.umask(0)
    try:
        with open_directory(tmp_path, private_destination=True) as parent:
            child = parent.new_private_child()
            assert os.stat(child.path).st_mode & 0o777 == 0o700
            original = child.path.with_name(child.path.name + "-original")
            child.path.rename(original)
            child.path.mkdir()
            with pytest.raises(PublicationError, match="replaced"):
                child.remove_if_empty()
            assert child.path.is_dir()
            child.close()
            child.path.rmdir()
            original.rmdir()
    finally:
        os.umask(old_umask)


def test_symlink_and_writable_destinations_rejected_without_chmod(tmp_path):
    target = tmp_path / "target"
    target.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(target, target_is_directory=True)
    with pytest.raises(PublicationError):
        open_directory(alias)
    target.chmod(0o777)
    with pytest.raises(PublicationError):
        open_directory(target, private_destination=True)
    assert target.stat().st_mode & 0o777 == 0o777


@pytest.mark.parametrize("name", ["../x", "a/b", "", ".", "..", "bad\x00", "x" * 241])
def test_generated_basename_bounds(name):
    from readmixer_py.errors import ConfigurationError
    with pytest.raises(ConfigurationError):
        validate_basename(name)


def test_reservation_overflow_is_atomic(tmp_path, monkeypatch):
    disk = DiskBudget()
    with open_directory(tmp_path) as directory:
        with disk.reserve(directory.fd, 1) as ticket:
            ticket.commit()
        state = disk._devices[os.fstat(directory.fd).st_dev]
        state.committed = (1 << 64) - 1
        with pytest.raises(ResourceError, match="u64"):
            disk.reserve(directory.fd, 1)
        assert disk.pending_bytes == disk._active == 0
        state.committed = 1
        disk.release(directory.fd, 1)
