#!/usr/bin/env python3
"""Capture/verify a native, already-selected wheelhouse; never resolve or install.

This qualification helper is deliberately outside the immutable candidate package.
All paths in the resulting manifest are basenames. Dependency selection happens
before capture; subsequent installation must use --no-index --require-hashes.
"""
from __future__ import annotations

import argparse
from collections import deque
from email import policy
from email.parser import BytesParser
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import stat
import struct
import sys
import sysconfig
from typing import Any, Iterable
import zipfile

from packaging.markers import default_environment
from packaging.requirements import InvalidRequirement, Requirement
from packaging.specifiers import SpecifierSet
from packaging.tags import parse_tag, sys_tags
from packaging.utils import canonicalize_name, parse_wheel_filename
from packaging.version import InvalidVersion, Version

SCHEMA = "readmixer.release.environment.v1"
TOOL_VERSION = 1
MAX_WHEELS = 256
MAX_WHEEL_BYTES = 2 * 1024**3
MAX_ARCHIVE_ENTRIES = 100_000
MAX_METADATA_BYTES = 2 * 1024**2
MAX_MANIFEST_BYTES = 4 * 1024**2
MAX_LOCK_BYTES = 128 * 1024
MAX_REQUIREMENTS = 64
DEFAULT_REQUIREMENTS = (
    "numpy>=1.26.4,<3", "pycryptodome>=3.23,<4", "psutil>=5.9.3,<8",
    "pytest>=7.4,<9", "build>=1,<2", "setuptools>=68", "wheel",
    "pip", "packaging", "pysam",
)


class EnvironmentError(ValueError):
    """A selected dependency environment cannot be safely replayed."""


def canonical_json(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"),
                       ensure_ascii=True, allow_nan=False) + "\n").encode("ascii")


def native_host() -> dict[str, Any]:
    """Record facts from this interpreter; never accept a requested foreign tag."""
    return {
        "implementation": platform.python_implementation(),
        "python_full_version": platform.python_version(),
        "python_build": list(platform.python_build()),
        "python_compiler": platform.python_compiler(),
        "machine": platform.machine(),
        "pointer_bits": struct.calcsize("P") * 8,
        "sysconfig_platform": sysconfig.get_platform(),
        "soabi": sysconfig.get_config_var("SOABI"),
        "system": platform.system(),
        "libc": list(platform.libc_ver()),
        "macos_version": platform.mac_ver()[0],
        "marker_environment": default_environment(),
    }


def _supported_host(host: dict[str, Any]) -> None:
    supported = (host["system"], host["machine"]) in {
        ("Linux", "x86_64"), ("Darwin", "arm64"),
    }
    python = host["marker_environment"]["python_version"]
    if (host["implementation"] != "CPython" or host["pointer_bits"] != 64
            or python not in {"3.11", "3.14"} or not supported):
        raise EnvironmentError("native host is outside the declared release support matrix")


def _requirement(text: str) -> Requirement:
    if not isinstance(text, str) or len(text) > 4096 or "\n" in text or "\r" in text:
        raise EnvironmentError("invalid or oversized requirement")
    try:
        result = Requirement(text)
    except InvalidRequirement as exc:
        raise EnvironmentError("invalid dependency requirement") from exc
    if result.url is not None:
        raise EnvironmentError("direct URL dependencies are not replayable from this wheelhouse")
    return result


def _same_stat(left: os.stat_result, right: os.stat_result) -> bool:
    return (left.st_dev, left.st_ino, left.st_size, left.st_mtime_ns,
            left.st_ctime_ns) == (right.st_dev, right.st_ino, right.st_size,
                                 right.st_mtime_ns, right.st_ctime_ns)


def _single_header(metadata: Any, name: str, *, required: bool = True) -> str | None:
    values = metadata.get_all(name, [])
    if len(values) > 1 or (required and len(values) != 1):
        raise EnvironmentError("missing or repeated wheel metadata field: " + name)
    return str(values[0]).strip() if values else None


def _inspect_wheel(path: Path, compatible: set[Any], host: dict[str, Any]) -> dict[str, Any]:
    filename = path.name
    if (len(filename) > 255 or not re.fullmatch(r"[A-Za-z0-9_.+-]+\.whl", filename)
            or filename.startswith(".")):
        raise EnvironmentError("unsafe wheel filename")
    try:
        name, version, _build, filename_tags = parse_wheel_filename(filename)
    except (ValueError, InvalidVersion) as exc:
        raise EnvironmentError("invalid wheel filename") from exc
    if not (filename_tags & compatible):
        raise EnvironmentError("wheel tags are incompatible with the actual native interpreter: " + filename)
    flags = (os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
             | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_CLOEXEC", 0))
    if path.is_symlink():
        raise EnvironmentError("symlink wheel is forbidden")
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise EnvironmentError("cannot safely open selected wheel") from exc
    try:
        with os.fdopen(fd, "rb") as stream:
            before = os.fstat(stream.fileno())
            if not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= MAX_WHEEL_BYTES:
                raise EnvironmentError("wheel is not a bounded regular file")
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
            stream.seek(0)
            with zipfile.ZipFile(stream) as archive:
                members = archive.infolist()
                if not 0 < len(members) <= MAX_ARCHIVE_ENTRIES:
                    raise EnvironmentError("wheel archive entry limit exceeded")
                seen: set[str] = set()
                total = 0
                for item in members:
                    parts = PurePosixPath(item.filename).parts
                    if (not item.filename or item.filename in seen or "\\" in item.filename
                            or ":" in item.filename or "\x00" in item.filename
                            or item.filename.startswith("/") or ".." in parts
                            or PurePosixPath(item.filename).as_posix() != item.filename.rstrip("/")
                            or stat.S_ISLNK(item.external_attr >> 16)):
                        raise EnvironmentError("unsafe, duplicate or symlink wheel archive member")
                    seen.add(item.filename)
                    total += item.file_size
                    if total > MAX_WHEEL_BYTES or item.flag_bits & 1:
                        raise EnvironmentError("oversized or encrypted wheel archive")
                metadata_names = [n for n in seen if re.fullmatch(r"[^/]+\.dist-info/METADATA", n)]
                wheel_names = [n for n in seen if re.fullmatch(r"[^/]+\.dist-info/WHEEL", n)]
                if len(metadata_names) != 1 or len(wheel_names) != 1:
                    raise EnvironmentError("wheel must have exactly one METADATA and WHEEL")
                metadata_name, wheel_name = metadata_names[0], wheel_names[0]
                if metadata_name.rsplit("/", 1)[0] != wheel_name.rsplit("/", 1)[0]:
                    raise EnvironmentError("wheel metadata directories disagree")
                directory = metadata_name.rsplit("/", 1)[0][:-len(".dist-info")]
                try:
                    directory_name, directory_version = directory.rsplit("-", 1)
                    valid_directory = (canonicalize_name(directory_name) == name
                                       and Version(directory_version) == version)
                except (ValueError, InvalidVersion):
                    valid_directory = False
                if not valid_directory:
                    raise EnvironmentError("wheel metadata directory disagrees with filename")
                documents = []
                for member in (metadata_name, wheel_name):
                    if archive.getinfo(member).file_size > MAX_METADATA_BYTES:
                        raise EnvironmentError("wheel metadata size limit exceeded")
                    documents.append(BytesParser(policy=policy.default).parsebytes(archive.read(member)))
                metadata, wheel = documents
                if metadata.defects or wheel.defects:
                    raise EnvironmentError("malformed wheel metadata")
                if canonicalize_name(_single_header(metadata, "Name")) != name:
                    raise EnvironmentError("wheel metadata name disagrees with filename")
                if Version(_single_header(metadata, "Version")) != version:
                    raise EnvironmentError("wheel metadata version disagrees with filename")
                if _single_header(wheel, "Wheel-Version") != "1.0":
                    raise EnvironmentError("unsupported wheel format version")
                tags = set()
                for tag in wheel.get_all("Tag", []):
                    tags.update(parse_tag(str(tag).strip()))
                if tags != filename_tags:
                    raise EnvironmentError("wheel internal tags disagree with filename")
                requires_python = _single_header(metadata, "Requires-Python", required=False)
                if requires_python and not SpecifierSet(requires_python).contains(
                        host["python_full_version"], prereleases=True):
                    raise EnvironmentError("wheel Requires-Python excludes the native interpreter")
                requirements = sorted(str(_requirement(str(v)))
                                      for v in metadata.get_all("Requires-Dist", []))
                if len(requirements) > 4096:
                    raise EnvironmentError("too many wheel dependencies")
                extras = sorted({canonicalize_name(str(v).strip())
                                 for v in metadata.get_all("Provides-Extra", [])})
                if any(not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", x) for x in extras):
                    raise EnvironmentError("invalid provided extra")
            after = os.fstat(stream.fileno())
            if not _same_stat(before, after) or not _same_stat(before, path.stat(follow_symlinks=False)):
                raise EnvironmentError("wheel changed during inspection")
    except (OSError, zipfile.BadZipFile, RuntimeError, ValueError) as exc:
        if isinstance(exc, EnvironmentError):
            raise
        raise EnvironmentError("invalid or unreadable selected wheel") from exc
    return {
        "name": str(name), "version": str(version), "filename": filename,
        "bytes": before.st_size, "sha256": digest,
        "tags": sorted(str(tag) for tag in filename_tags),
        "requires_python": requires_python,
        "requires_dist": requirements, "provides_extra": extras,
    }


def _dependency_closure(wheels: list[dict[str, Any]], roots: list[str],
                        markers: dict[str, str]) -> None:
    selected = {row["name"]: row for row in wheels}
    active: dict[str, set[str]] = {}
    pending: deque[str] = deque()

    def admit(requirement: Requirement) -> None:
        name = str(canonicalize_name(requirement.name))
        row = selected.get(name)
        if row is None:
            raise EnvironmentError("missing required wheel: " + name)
        if not requirement.specifier.contains(row["version"], prereleases=True):
            raise EnvironmentError("selected wheel violates a dependency version constraint: " + name)
        extras = {str(canonicalize_name(extra)) for extra in requirement.extras}
        if not extras.issubset(set(row["provides_extra"])):
            raise EnvironmentError("requested wheel extra is not provided: " + name)
        if name not in active:
            active[name] = set(extras)
            pending.append(name)
        elif not extras.issubset(active[name]):
            active[name].update(extras)
            pending.append(name)

    def applies(requirement: Requirement, extras: Iterable[str]) -> bool:
        return requirement.marker is None or any(
            requirement.marker.evaluate({**markers, "extra": extra}) for extra in extras)

    for text in roots:
        req = _requirement(text)
        if applies(req, ("",)):
            admit(req)
    while pending:
        name = pending.popleft()
        for text in selected[name]["requires_dist"]:
            req = _requirement(text)
            if applies(req, ("", *sorted(active[name]))):
                admit(req)
    orphaned = sorted(set(selected) - set(active))
    if orphaned:
        raise EnvironmentError("selected wheels are outside the declared dependency closure: " + ", ".join(orphaned))


def capture_environment(wheelhouse: Path, requirements: Iterable[str] | None = None
                        ) -> tuple[dict[str, Any], bytes]:
    """Validate native wheel compatibility/closure and return canonical lock data.

    ``requirements`` replaces default roots for programmatic callers (e.g. a
    different candidate). The command line always retains the complete default
    qualification roots and treats --require as additions.
    """
    host = native_host()
    _supported_host(host)
    root_inputs = list(DEFAULT_REQUIREMENTS if requirements is None else requirements)
    if not 0 < len(root_inputs) <= MAX_REQUIREMENTS:
        raise EnvironmentError("invalid number of root requirements")
    roots = sorted({str(_requirement(text)) for text in root_inputs})
    wheelhouse = Path(wheelhouse)
    if wheelhouse.is_symlink() or not wheelhouse.is_dir():
        raise EnvironmentError("wheelhouse must be a real directory")
    paths = []
    with os.scandir(wheelhouse) as entries:
        for entry in entries:
            paths.append(wheelhouse / entry.name)
            if len(paths) > MAX_WHEELS:
                raise EnvironmentError("wheelhouse file-count limit exceeded or empty")
    if not paths:
        raise EnvironmentError("wheelhouse file-count limit exceeded or empty")
    paths.sort(key=lambda path: path.name)
    compatible = set(sys_tags())
    wheels = [_inspect_wheel(path, compatible, host) for path in paths]
    wheels.sort(key=lambda row: row["name"])
    if len({row["name"] for row in wheels}) != len(wheels):
        raise EnvironmentError("multiple selected wheels for one distribution")
    _dependency_closure(wheels, roots, host["marker_environment"])
    lock = ("# Native qualification environment; install with --no-index --require-hashes.\n"
            + "".join(f"{row['name']}=={row['version']} --hash=sha256:{row['sha256']}\n"
                      for row in wheels)).encode("ascii")
    if len(lock) > MAX_LOCK_BYTES:
        raise EnvironmentError("lock size limit exceeded")
    manifest = {
        "schema": SCHEMA, "host": host, "roots": roots, "wheels": wheels,
        "lock_sha256": hashlib.sha256(lock).hexdigest(),
        "tool": {"name": "lock_environment.py", "version": TOOL_VERSION,
                 "sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()},
        "limitations": ["Captures an already selected native wheelhouse; does not resolve dependencies.",
                        "Native compatibility and dependency closure do not establish executed tests.",
                        "This lock records one interpreter build and host marker environment."],
    }
    if len(canonical_json(manifest)) > MAX_MANIFEST_BYTES:
        raise EnvironmentError("manifest size limit exceeded")
    return manifest, lock


def verify_environment(wheelhouse: Path, manifest: dict[str, Any], lock_bytes: bytes
                       ) -> dict[str, Any]:
    """Fail closed unless native facts, wheel bytes, metadata and lock all match."""
    if (not isinstance(manifest, dict) or manifest.get("schema") != SCHEMA
            or not isinstance(manifest.get("roots"), list)
            or not isinstance(lock_bytes, bytes) or len(lock_bytes) > MAX_LOCK_BYTES):
        raise EnvironmentError("invalid environment manifest or lock")
    if len(canonical_json(manifest)) > MAX_MANIFEST_BYTES:
        raise EnvironmentError("manifest size limit exceeded")
    if manifest.get("host") != native_host():
        raise EnvironmentError("actual native host does not match the captured environment")
    current, current_lock = capture_environment(wheelhouse, manifest["roots"])
    if current_lock != lock_bytes:
        raise EnvironmentError("lock bytes do not match the exact selected wheelhouse")
    if current != manifest:
        raise EnvironmentError("environment manifest differs from the verified wheelhouse or tool")
    return current


def _read_bounded(path: Path, limit: int) -> bytes:
    if path.is_symlink():
        raise EnvironmentError("symlink evidence is forbidden")
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
                 | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_CLOEXEC", 0))
    with os.fdopen(fd, "rb") as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_size > limit:
            raise EnvironmentError("evidence must be a bounded regular file")
        data = stream.read(limit + 1)
        if not _same_stat(before, os.fstat(stream.fileno())):
            raise EnvironmentError("evidence changed during reading")
    if len(data) > limit:
        raise EnvironmentError("evidence size limit exceeded")
    return data


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise EnvironmentError("duplicate JSON object key")
        result[key] = value
    return result


def _write_exclusive(path: Path, data: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL
                 | getattr(os, "O_NOFOLLOW", 0), 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="action", required=True)
    for action in ("capture", "verify"):
        command = subparsers.add_parser(action)
        command.add_argument("--wheelhouse", type=Path, required=True)
        command.add_argument("--lock", type=Path, required=True)
        command.add_argument("--manifest", type=Path, required=True)
        if action == "capture":
            command.add_argument("--require", action="append", default=[])
    args = parser.parse_args(argv)
    try:
        if args.action == "capture":
            manifest, lock = capture_environment(args.wheelhouse, (*DEFAULT_REQUIREMENTS, *args.require))
            if args.lock.exists() or args.manifest.exists() or args.lock == args.manifest:
                raise EnvironmentError("capture outputs must be distinct new files")
            _write_exclusive(args.lock, lock)
            _write_exclusive(args.manifest, canonical_json(manifest))
        else:
            raw = _read_bounded(args.manifest, MAX_MANIFEST_BYTES)
            manifest = json.loads(raw, object_pairs_hook=_unique_object)
            if canonical_json(manifest) != raw:
                raise EnvironmentError("manifest is not in canonical JSON encoding")
            verify_environment(args.wheelhouse, manifest,
                               _read_bounded(args.lock, MAX_LOCK_BYTES))
    except (EnvironmentError, OSError, ValueError, TypeError, KeyError, RecursionError) as exc:
        # Fixed diagnostic avoids leaking local paths from filesystem exceptions.
        detail = str(exc) if isinstance(exc, EnvironmentError) else "invalid or inaccessible environment inputs"
        print("environment verification failed: " + detail, file=sys.stderr)
        return 1
    print(json.dumps({"schema": SCHEMA, "status": "verified", "wheels": len(manifest["wheels"]),
                      "lock_sha256": manifest["lock_sha256"], "host": manifest["host"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
