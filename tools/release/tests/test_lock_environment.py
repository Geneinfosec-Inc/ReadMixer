"""Tests for release helpers, deliberately outside the frozen candidate's suite."""
from __future__ import annotations

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import stat
import zipfile

import pytest

SPEC = importlib.util.spec_from_file_location(
    "release_lock_environment", Path(__file__).parents[1] / "lock_environment.py")
lockenv = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(lockenv)


@pytest.fixture
def wheels(tmp_path):
    result = tmp_path / "wheels"
    result.mkdir()
    return result


def wheel(wheels, name="base", version="1.0", *, requires=(), extras=(),
          requires_python=">=3.11", tag="py3-none-any", headers=(),
          internal_tag=None, internal_name=None, internal_version=None,
          members=(), metadata_prefix=None):
    normalized = name.replace("-", "_")
    path = wheels / f"{normalized}-{version}-{tag}.whl"
    prefix = metadata_prefix or f"{normalized}-{version}.dist-info"
    metadata = "Metadata-Version: 2.1\n" + f"Name: {internal_name or name}\nVersion: {internal_version or version}\n"
    if requires_python is not None:
        metadata += f"Requires-Python: {requires_python}\n"
    metadata += "".join(f"Requires-Dist: {r}\n" for r in requires)
    metadata += "".join(f"Provides-Extra: {e}\n" for e in extras)
    metadata += "".join(f"{h}\n" for h in headers) + "\n"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(prefix + "/METADATA", metadata)
        archive.writestr(prefix + "/WHEEL", "Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: true\nTag: " + (internal_tag or tag) + "\n\n")
        archive.writestr(normalized + "/__init__.py", "fixture = True\n")
        for filename, data in members:
            archive.writestr(filename, data)
    return path


def capture(wheels, roots=("base",)):
    return lockenv.capture_environment(wheels, roots)


def test_capture_round_trip_is_canonical_and_private(wheels, tmp_path):
    path = wheel(wheels, requires=("child>=2",))
    wheel(wheels, "child", "2.1")
    manifest, lock = capture(wheels)
    assert lockenv.verify_environment(wheels, manifest, lock) == manifest
    assert [r["name"] for r in manifest["wheels"]] == ["base", "child"]
    assert manifest["wheels"][0]["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert manifest["wheels"][0]["bytes"] == path.stat().st_size
    assert manifest["lock_sha256"] == hashlib.sha256(lock).hexdigest()
    assert b"base==1.0 --hash=sha256:" in lock
    assert str(tmp_path).encode() not in lockenv.canonical_json(manifest)
    assert lockenv.canonical_json(manifest).endswith(b"\n")
    assert capture(wheels) == (manifest, lock)


@pytest.mark.parametrize("requires, extra_wheel, roots, message", [
    (("missing>=1",), None, ("base",), "missing required wheel"),
    (("child>=2",), ("child", "1.0"), ("base",), "violates"),
    ((), ("unused", "1.0"), ("base",), "outside the declared"),
    ((), None, ("base>=2",), "violates"),
    ((), None, ("missing",), "missing required wheel"),
    ((), None, ("base[missing]",), "extra is not provided"),
    (("child @ https://example.invalid/child.whl",), None, ("base",), "direct URL"),
])
def test_dependency_closure_rejects_incomplete_or_unselected_wheels(wheels, requires, extra_wheel, roots, message):
    wheel(wheels, requires=requires)
    if extra_wheel:
        wheel(wheels, *extra_wheel)
    with pytest.raises(lockenv.EnvironmentError, match=message):
        capture(wheels, roots)


def test_platform_and_python_markers_use_actual_host(wheels):
    machine = lockenv.native_host()["machine"]
    wheel(wheels, requires=(
        f'child>=1; platform_machine == "{machine}"',
        'absent; python_version < "3.0"',
        'absent; platform_machine == "imaginary-architecture"',
    ))
    wheel(wheels, "child")
    manifest, _ = capture(wheels)
    assert manifest["host"]["marker_environment"]["platform_machine"] == machine


def test_transitive_extras_propagate_to_fixed_point_and_cycles_terminate(wheels):
    wheel(wheels, requires=("child", "third"))
    wheel(wheels, "child", extras=("native",), requires=(
        'fourth; extra == "native"', "base", 'absent; extra == "docs"',
    ))
    wheel(wheels, "third", requires=("child[native]",))
    wheel(wheels, "fourth")
    manifest, lock = capture(wheels)
    assert len(manifest["wheels"]) == 4
    lockenv.verify_environment(wheels, manifest, lock)


def test_requested_root_extra_is_normalized(wheels):
    wheel(wheels, extras=("test-extra",), requires=('child; extra == "test-extra"',))
    wheel(wheels, "child")
    capture(wheels, ("base[test_extra]",))


def test_inactive_root_does_not_admit_orphan(wheels):
    wheel(wheels)
    with pytest.raises(lockenv.EnvironmentError, match="outside the declared"):
        capture(wheels, ('base; python_version < "3.0"',))


@pytest.mark.parametrize("kwargs, message", [
    ({"internal_name": "other"}, "name disagrees"),
    ({"internal_version": "2.0"}, "version disagrees"),
    ({"requires_python": ">=99"}, "Requires-Python"),
    ({"headers": ("Name: duplicate",)}, "repeated wheel metadata"),
    ({"headers": ("Requires-Python: >=1",)}, "repeated wheel metadata"),
    ({"internal_tag": "py2-none-any"}, "tags disagree"),
    ({"metadata_prefix": "other-1.0.dist-info"}, "directory disagrees"),
    ({"metadata_prefix": "base-2.0.dist-info"}, "directory disagrees"),
    ({"requires": ("%%%",)}, "invalid dependency"),
    ({"extras": ("bad extra!",)}, "invalid provided extra"),
])
def test_metadata_identity_and_constraints_checked(wheels, kwargs, message):
    wheel(wheels, **kwargs)
    with pytest.raises(lockenv.EnvironmentError, match=message):
        capture(wheels)


def test_wrong_native_wheel_tags_are_rejected(wheels):
    wheel(wheels, tag="cp27-cp27m-win32")
    with pytest.raises(lockenv.EnvironmentError, match="incompatible"):
        capture(wheels)


def test_duplicate_selected_distribution_is_rejected(wheels):
    wheel(wheels)
    wheel(wheels, version="2.0")
    with pytest.raises(lockenv.EnvironmentError, match="multiple selected"):
        capture(wheels)


@pytest.mark.parametrize("member", ["../escape", "/absolute", "pkg/../escape", "pkg\\escape", "C:/escape", "./package", "pkg//file", "pkg/./file"])
def test_unsafe_archive_members_are_rejected(wheels, member):
    wheel(wheels, members=((member, "bad"),))
    with pytest.raises(lockenv.EnvironmentError, match="unsafe"):
        capture(wheels)


def test_duplicate_archive_member_rejected(wheels):
    with pytest.warns(UserWarning, match="Duplicate"):
        wheel(wheels, members=(("base/__init__.py", "bad"),))
    with pytest.raises(lockenv.EnvironmentError, match="duplicate"):
        capture(wheels)


def test_archive_symlink_rejected(wheels):
    path = wheel(wheels)
    info = zipfile.ZipInfo("alias")
    info.create_system = 3
    info.external_attr = (stat.S_IFLNK | 0o777) << 16
    with zipfile.ZipFile(path, "a") as archive:
        archive.writestr(info, "target")
    with pytest.raises(lockenv.EnvironmentError, match="symlink"):
        capture(wheels)


def test_selected_wheel_symlink_rejected(wheels, tmp_path):
    path = wheel(wheels)
    saved = tmp_path / path.name
    path.rename(saved)
    path.symlink_to(saved)
    with pytest.raises(lockenv.EnvironmentError, match="symlink"):
        capture(wheels)


def test_nonwheel_and_nested_directory_rejected(wheels):
    wheel(wheels)
    (wheels / "hidden-data.txt").write_text("not a selected wheel")
    with pytest.raises(lockenv.EnvironmentError, match="filename"):
        capture(wheels)


@pytest.mark.parametrize("limit,value,message", [
    ("MAX_WHEELS", 0, "file-count"),
    ("MAX_WHEEL_BYTES", 10, "bounded regular"),
    ("MAX_ARCHIVE_ENTRIES", 1, "entry limit"),
    ("MAX_METADATA_BYTES", 10, "metadata size"),
    ("MAX_MANIFEST_BYTES", 10, "manifest size"),
    ("MAX_LOCK_BYTES", 10, "lock size"),
])
def test_bounds_are_enforced_before_acceptance(wheels, monkeypatch, limit, value, message):
    wheel(wheels)
    monkeypatch.setattr(lockenv, limit, value)
    with pytest.raises(lockenv.EnvironmentError, match=message):
        capture(wheels)


@pytest.mark.parametrize("field,new_value", [
    ("machine", "foreign"), ("pointer_bits", 32),
    ("python_full_version", "3.11.999"), ("soabi", "foreign-abi"),
    ("python_build", ["unseen", "build"]),
])
def test_verification_rejects_foreign_native_facts(wheels, field, new_value):
    wheel(wheels)
    manifest, lock = capture(wheels)
    manifest["host"][field] = new_value
    with pytest.raises(lockenv.EnvironmentError, match="actual native host"):
        lockenv.verify_environment(wheels, manifest, lock)


def test_unsupported_native_host_is_never_qualified(wheels, monkeypatch):
    host = lockenv.native_host()
    host["machine"] = "aarch64"
    monkeypatch.setattr(lockenv, "native_host", lambda: host)
    wheel(wheels)
    with pytest.raises(lockenv.EnvironmentError, match="support matrix"):
        capture(wheels)


@pytest.mark.parametrize("tamper", ["wheel_bytes", "lock", "manifest_hash", "unknown_field", "tool"])
def test_verification_detects_identity_tampering(wheels, tamper):
    path = wheel(wheels)
    manifest, lock = capture(wheels)
    if tamper == "wheel_bytes":
        with path.open("ab") as stream:
            stream.write(b"changed after capture")
    elif tamper == "lock":
        lock += b"--index-url https://example.invalid/\n"
    elif tamper == "manifest_hash":
        manifest["wheels"][0]["sha256"] = "0" * 64
    elif tamper == "unknown_field":
        manifest["claim"] = "passed"
    else:
        manifest["tool"]["sha256"] = "0" * 64
    with pytest.raises(lockenv.EnvironmentError, match="(lock bytes|manifest differs)"):
        lockenv.verify_environment(wheels, manifest, lock)


def test_cli_capture_verify_and_exclusive_outputs(wheels, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(lockenv, "DEFAULT_REQUIREMENTS", ("base",))
    wheel(wheels)
    manifest = tmp_path / "environment.json"
    lock = tmp_path / "requirements.lock"
    args = ["--wheelhouse", str(wheels), "--manifest", str(manifest), "--lock", str(lock)]
    assert lockenv.main(["capture", *args]) == 0
    captured = capsys.readouterr()
    result = json.loads(captured.out)
    assert result["status"] == "verified"
    assert result["wheels"] == 1
    assert str(tmp_path) not in captured.out
    assert stat.S_IMODE(lock.stat().st_mode) == 0o600
    assert stat.S_IMODE(manifest.stat().st_mode) == 0o600
    saved = (lock.read_bytes(), manifest.read_bytes())
    assert lockenv.main(["verify", *args]) == 0
    assert lockenv.main(["capture", *args]) == 1
    assert saved == (lock.read_bytes(), manifest.read_bytes())


def test_cli_rejects_duplicate_keys_and_noncanonical_json(wheels, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(lockenv, "DEFAULT_REQUIREMENTS", ("base",))
    wheel(wheels)
    manifest, lock = capture(wheels)
    mfile, lfile = tmp_path / "manifest.json", tmp_path / "requirements.lock"
    lfile.write_bytes(lock)
    args = ["verify", "--wheelhouse", str(wheels), "--manifest", str(mfile), "--lock", str(lfile)]
    mfile.write_text('{"schema": "a", "schema": "b"}')
    assert lockenv.main(args) == 1
    assert "duplicate JSON" in capsys.readouterr().err
    mfile.write_text(json.dumps(manifest, indent=2))
    assert lockenv.main(args) == 1
    assert "canonical JSON" in capsys.readouterr().err


def test_cli_filesystem_failure_does_not_echo_private_paths(tmp_path, capsys):
    args = ["verify", "--wheelhouse", str(tmp_path / "private-input-name"),
            "--manifest", str(tmp_path / "private-manifest"), "--lock", str(tmp_path / "private-lock")]
    assert lockenv.main(args) == 1
    captured = capsys.readouterr()
    assert str(tmp_path) not in captured.err
    assert "private" not in captured.err


def test_fifo_evidence_is_rejected_without_blocking(tmp_path):
    path = tmp_path / "fifo"
    os.mkfifo(path)
    with pytest.raises(lockenv.EnvironmentError, match="regular file"):
        lockenv._read_bounded(path, 100)


def test_symlink_evidence_rejected(tmp_path):
    target = tmp_path / "real"
    target.write_bytes(b"data")
    alias = tmp_path / "alias"
    alias.symlink_to(target)
    with pytest.raises(lockenv.EnvironmentError, match="symlink"):
        lockenv._read_bounded(alias, 100)


def test_wheelhouse_symlink_rejected(wheels, tmp_path):
    wheel(wheels)
    alias = tmp_path / "wheel-alias"
    alias.symlink_to(wheels, target_is_directory=True)
    with pytest.raises(lockenv.EnvironmentError, match="real directory"):
        capture(alias)


def test_fifo_wheel_is_rejected_without_blocking(wheels):
    os.mkfifo(wheels / "base-1.0-py3-none-any.whl")
    with pytest.raises(lockenv.EnvironmentError, match="regular file"):
        capture(wheels)
