#!/usr/bin/env python3
"""Transport only a verified candidate manifest and its two immutable archives.

No dependencies, diagnostics, source checkout, or private inputs enter packets.
A packet checksum authenticates bytes only against the separately trusted digest
provided by its sender; it is not a signature or a release-qualification claim.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import struct
import sys
import tempfile
import zipfile

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))
import candidate

SCHEMA = "readmixer.release.candidate-packet.v1"
MAX_MANIFEST = 4 << 20
MAX_MEMBER = 64 << 20
MAX_PACKET = 2 * MAX_MEMBER + MAX_MANIFEST + (64 << 10)
MAX_DIRECTORY = 64 << 10
MEMBERS = 3


class PacketError(ValueError):
    """The packet cannot safely represent the requested immutable candidate."""


def _safe_name(name: str) -> str:
    if (not isinstance(name, str) or len(name) > 255
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.+-]*", name)):
        raise PacketError("unsafe packet basename")
    return name


def _same_stat(left, right):
    return (left.st_dev, left.st_ino, left.st_size, left.st_mtime_ns, left.st_ctime_ns) == (
        right.st_dev, right.st_ino, right.st_size, right.st_mtime_ns, right.st_ctime_ns)


def _open_regular(path, maximum):
    path = Path(path)
    if path.is_symlink():
        raise PacketError("symlink input is forbidden")
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
                         | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_CLOEXEC", 0))
    stream = os.fdopen(descriptor, "rb")
    info = os.fstat(stream.fileno())
    if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= maximum:
        stream.close()
        raise PacketError("input must be a bounded nonempty regular file")
    return stream


def _write_new(path, source, maximum, expected=None):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL
                         | getattr(os, "O_NOFOLLOW", 0), 0o600)
    digest = hashlib.sha256()
    size = 0
    with os.fdopen(descriptor, "wb") as destination:
        while part := source.read(1 << 20):
            size += len(part)
            if size > maximum:
                raise PacketError("packet member exceeds its bound")
            digest.update(part)
            destination.write(part)
        destination.flush()
        os.fsync(destination.fileno())
    actual = {"bytes": size, "sha256": digest.hexdigest()}
    if expected is not None and actual != expected:
        raise PacketError("packet member differs from declared candidate identity")
    return actual


def _packet_header(stream):
    """Reject large/ambiguous central directories before ZipFile allocates them."""
    size = os.fstat(stream.fileno()).st_size
    if size < 22:
        raise PacketError("truncated packet")
    stream.seek(-22, os.SEEK_END)
    signature, disk, directory_disk, disk_entries, entries, directory_size, offset, comment = struct.unpack(
        "<4s4H2LH", stream.read(22))
    if (signature != b"PK\x05\x06" or disk != 0 or directory_disk != 0
            or disk_entries != MEMBERS or entries != MEMBERS or comment != 0
            or directory_size > MAX_DIRECTORY or offset + directory_size + 22 != size):
        raise PacketError("packet must have one bounded three-entry ZIP directory without comments or ZIP64")
    stream.seek(0)
    return offset


def _directory(archive):
    infos = archive.infolist()
    if len(infos) != MEMBERS:
        raise PacketError("packet must contain exactly three files")
    result = {}
    for info in infos:
        name = _safe_name(info.filename)
        mode = info.external_attr >> 16
        if (name in result or info.is_dir() or (stat.S_IFMT(mode) not in (0, stat.S_IFREG))
                or info.flag_bits != 0 or info.compress_type != zipfile.ZIP_STORED
                or info.compress_size != info.file_size or info.extra or info.comment):
            raise PacketError("packet contains duplicate or unsupported entries")
        maximum = MAX_MANIFEST if name == "candidate.json" else MAX_MEMBER
        if not 0 < info.file_size <= maximum:
            raise PacketError("packet member exceeds its bound")
        result[name] = info
    if "candidate.json" not in result:
        raise PacketError("packet candidate manifest is missing")
    return result


def _local_headers(stream, entries, directory_offset):
    # A forged central directory must not hide extra local members or prefixes.
    position = 0
    for info in sorted(entries.values(), key=lambda entry: entry.header_offset):
        if info.header_offset != position:
            raise PacketError("packet has hidden or overlapping local entries")
        stream.seek(position)
        raw = stream.read(30)
        if len(raw) != 30:
            raise PacketError("truncated packet local header")
        signature, version, flags, compression, _time, _date, crc, compressed, size, namelen, extralen = struct.unpack("<4s5H3L2H", raw)
        if (signature != b"PK\x03\x04" or version > 20 or flags != 0
                or compression != zipfile.ZIP_STORED or crc != info.CRC
                or compressed != info.file_size or size != info.file_size
                or extralen != 0 or namelen > 255
                or stream.read(namelen) != info.filename.encode("ascii")):
            raise PacketError("packet local and central headers disagree")
        position += 30 + namelen + info.file_size
    if position != directory_offset:
        raise PacketError("packet contains hidden bytes before its directory")


def _candidate_names(value):
    try:
        archives = value["archives"]
        if set(archives) != {"wheel", "sdist"}:
            raise PacketError("candidate must name exactly two archives")
        names = {"candidate.json", *(_safe_name(row["name"]) for row in archives.values())}
        if len(names) != MEMBERS or len({name.casefold() for name in names}) != MEMBERS:
            raise PacketError("candidate archive names collide")
        return names
    except (KeyError, TypeError) as exc:
        raise PacketError("candidate archive identities are missing") from exc


def pack_candidate(bundle: Path, output: Path):
    bundle, output = Path(bundle), Path(output)
    if bundle.is_symlink() or not bundle.is_dir():
        raise PacketError("candidate must be a real directory")
    if output.resolve().is_relative_to(bundle.resolve()):
        raise PacketError("packet output must be outside the candidate directory")
    value = candidate.load_candidate(bundle)
    names = _candidate_names(value)
    with os.scandir(bundle) as entries:
        actual = set()
        for entry in entries:
            actual.add(entry.name)
            if len(actual) > MEMBERS:
                raise PacketError("candidate directory contains unselected files")
    if actual != names:
        raise PacketError("candidate directory must contain only its three selected files")
    expected = {row["name"]: {key: row[key] for key in ("bytes", "sha256")}
                for row in value["archives"].values()}
    with _open_regular(bundle / "candidate.json", MAX_MANIFEST) as manifest_stream:
        manifest_raw = manifest_stream.read(MAX_MANIFEST + 1)
    if len(manifest_raw) > MAX_MANIFEST:
        raise PacketError("candidate manifest grew while reading")
    expected["candidate.json"] = {"bytes": len(manifest_raw), "sha256": hashlib.sha256(manifest_raw).hexdigest()}
    if json.loads(manifest_raw, object_pairs_hook=candidate._pairs) != value:
        raise PacketError("candidate manifest changed before packing")
    descriptor = os.open(output, os.O_RDWR | os.O_CREAT | os.O_EXCL
                         | getattr(os, "O_NOFOLLOW", 0), 0o600)
    with os.fdopen(descriptor, "w+b") as destination:
        with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_STORED, allowZip64=False) as archive:
            for name in sorted(names):
                info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                info.create_system = 3
                info.external_attr = (stat.S_IFREG | 0o600) << 16
                info.compress_type = zipfile.ZIP_STORED
                digest, size = hashlib.sha256(), 0
                maximum = MAX_MANIFEST if name == "candidate.json" else MAX_MEMBER
                with _open_regular(bundle / name, maximum) as source, archive.open(info, "w") as member:
                    before = os.fstat(source.fileno())
                    while part := source.read(1 << 20):
                        size += len(part)
                        if size > maximum:
                            raise PacketError("candidate member grew while packing")
                        member.write(part)
                        digest.update(part)
                    if not _same_stat(before, os.fstat(source.fileno())):
                        raise PacketError("candidate member changed while packing")
                if {"bytes": size, "sha256": digest.hexdigest()} != expected[name]:
                    raise PacketError("candidate member differs from declared identity")
        destination.flush()
        os.fsync(destination.fileno())
        if destination.tell() > MAX_PACKET:
            raise PacketError("packet size limit exceeded")
        destination.seek(0)
        digest = hashlib.file_digest(destination, "sha256").hexdigest()
        size = os.fstat(destination.fileno()).st_size
    if candidate.load_candidate(bundle) != value:
        raise PacketError("candidate changed during packing")
    return {"schema": SCHEMA, "candidate_id": value["candidate_id"], "sha256": digest,
            "bytes": size, "members": MEMBERS, "release_qualified": False}


def unpack_candidate(packet: Path, expected_sha256: str, output: Path, expected_candidate_id=None):
    if not isinstance(expected_sha256, str) or not re.fullmatch(r"[a-f0-9]{64}", expected_sha256):
        raise PacketError("a lowercase SHA-256 packet checksum is required")
    if expected_candidate_id is not None and not re.fullmatch(r"sha256:[a-f0-9]{64}", expected_candidate_id):
        raise PacketError("invalid expected candidate identity")
    packet, output = Path(packet), Path(output)
    if output.exists() or output.is_symlink():
        raise PacketError("unpack destination must be new")
    # Fully validate in private temporary storage before creating the requested
    # destination. A later publication failure retains an incomplete new directory;
    # it never overwrites an existing directory or returns a successful identity.
    with _open_regular(packet, MAX_PACKET) as source:
        before = os.fstat(source.fileno())
        if hashlib.file_digest(source, "sha256").hexdigest() != expected_sha256:
            raise PacketError("packet checksum does not match")
        directory_offset = _packet_header(source)
        with zipfile.ZipFile(source) as archive:
            entries = _directory(archive)
            _local_headers(source, entries, directory_offset)
            manifest_raw = archive.read(entries["candidate.json"])
            value = json.loads(manifest_raw, object_pairs_hook=candidate._pairs)
            names = _candidate_names(value)
            if set(entries) != names:
                raise PacketError("packet entries differ from the declared candidate archives")
            if expected_candidate_id is not None and value.get("candidate_id") != expected_candidate_id:
                raise PacketError("packet candidate identity does not match")
            with tempfile.TemporaryDirectory(prefix=".candidate-packet-", dir=output.parent) as temporary:
                staging = Path(temporary)
                for name, info in entries.items():
                    maximum = MAX_MANIFEST if name == "candidate.json" else MAX_MEMBER
                    with archive.open(info) as member:
                        _write_new(staging / name, member, maximum)
                verified = candidate.load_candidate(staging)
                if verified != value:
                    raise PacketError("packet candidate verification disagrees")
                if (not _same_stat(before, os.fstat(source.fileno()))
                        or not _same_stat(before, packet.stat(follow_symlinks=False))):
                    raise PacketError("packet changed during unpacking")
                output.mkdir(mode=0o700)
                # The manifest is last: incomplete output is never a valid bundle.
                for name in sorted(names, key=lambda name: name == "candidate.json"):
                    with _open_regular(staging / name, MAX_MEMBER) as member:
                        _write_new(output / name, member, MAX_MEMBER)
                if candidate.load_candidate(output) != value:
                    raise PacketError("transported candidate differs")
    return {"schema": SCHEMA, "candidate_id": value["candidate_id"],
            "sha256": expected_sha256, "bytes": before.st_size, "members": MEMBERS,
            "release_qualified": False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    pack = commands.add_parser("pack")
    pack.add_argument("--candidate", type=Path, required=True)
    pack.add_argument("--output", type=Path, required=True)
    unpack = commands.add_parser("unpack")
    unpack.add_argument("--archive", type=Path, required=True)
    unpack.add_argument("--sha256", required=True)
    unpack.add_argument("--candidate-id")
    unpack.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.action == "pack":
            result = pack_candidate(args.candidate, args.output)
        else:
            result = unpack_candidate(args.archive, args.sha256, args.output, args.candidate_id)
    except (ValueError, OSError, zipfile.BadZipFile, RuntimeError, TypeError, KeyError, RecursionError) as exc:
        detail = str(exc) if isinstance(exc, PacketError) else "invalid or inaccessible candidate packet"
        print("candidate transport failed: " + detail, file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
