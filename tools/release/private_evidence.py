#!/usr/bin/env python3
"""Authenticated transport for private release diagnostics, never a release gate.

Set READMIXER_EVIDENCE_KEY to standard base64 encoding of exactly 32 random bytes.
The key is never accepted on the command line or recorded in output. The input
is the flat report/private directory emitted by the external platform harness.
Only encrypted bytes should leave a private machine or be uploaded as artifacts.
"""
from __future__ import annotations

import argparse
import base64
import binascii
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

MAGIC = b"RMP17PV1"
HEADER_BYTES = 8 + 12 + 8
TAG_BYTES = 16
MAX_FILES = 256
MAX_MEMBER = 32 << 20
MAX_TOTAL = 128 << 20
MAX_MANIFEST = 256 << 10
MAX_ARCHIVE = MAX_TOTAL + MAX_MANIFEST + (256 << 10)
MANIFEST = "private-manifest.json"
SCHEMA = "readmixer.release.private-evidence.v1"


class EvidenceError(ValueError):
    """A private evidence container cannot safely authenticate or restore."""


def _key():
    try:
        value = os.environ["READMIXER_EVIDENCE_KEY"]
        key = base64.b64decode(value, validate=True)
        if len(key) != 32 or base64.b64encode(key).decode("ascii") != value:
            raise ValueError
        return key
    except (KeyError, ValueError, binascii.Error) as exc:
        raise EvidenceError("READMIXER_EVIDENCE_KEY must encode exactly 32 random bytes in canonical base64") from exc


def _name(value):
    if (not isinstance(value, str) or len(value) > 128
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.+-]*", value)):
        raise EvidenceError("private evidence requires safe flat basenames")
    return value


def _open(path, limit):
    path = Path(path)
    if path.is_symlink():
        raise EvidenceError("symlink evidence is forbidden")
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
                         | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_CLOEXEC", 0))
    stream = os.fdopen(descriptor, "rb")
    metadata = os.fstat(stream.fileno())
    if not stat.S_ISREG(metadata.st_mode) or not 0 <= metadata.st_size <= limit:
        stream.close()
        raise EvidenceError("evidence must be a bounded regular file")
    return stream


def _same(before, after):
    return (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) == (
        after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)


def _new(path):
    return os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL
                            | getattr(os, "O_NOFOLLOW", 0), 0o600), "wb")


def _copy(source, destination, limit):
    count, digest = 0, hashlib.sha256()
    while block := source.read(1 << 20):
        count += len(block)
        if count > limit:
            raise EvidenceError("evidence exceeds byte limit")
        destination.write(block)
        digest.update(block)
    return {"bytes": count, "sha256": digest.hexdigest()}


def _finish(stream):
    stream.flush()
    os.fsync(stream.fileno())


def _json(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n").encode("ascii")


def _pairs(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise EvidenceError("duplicate manifest key")
        value[key] = item
    return value


def _source_files(source):
    if source.is_symlink() or not source.is_dir():
        raise EvidenceError("source must be a real flat private directory")
    paths = []
    with os.scandir(source) as entries:
        for entry in entries:
            _name(entry.name)
            if entry.name.casefold() == MANIFEST.casefold() or not entry.is_file(follow_symlinks=False):
                raise EvidenceError("source contains a reserved name, directory or special file")
            paths.append(source / entry.name)
            if len(paths) > MAX_FILES:
                raise EvidenceError("private evidence file-count limit exceeded")
    if not paths or len({path.name.casefold() for path in paths}) != len(paths):
        raise EvidenceError("private evidence is empty or has conflicting basenames")
    return sorted(paths)


def _create_plaintext(source, destination):
    records, total = {}, 0
    paths = _source_files(source)
    with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_STORED, allowZip64=False) as archive:
        for path in paths:
            info = zipfile.ZipInfo(path.name, date_time=(1980, 1, 1, 0, 0, 0))
            info.create_system = 3
            info.external_attr = (stat.S_IFREG | 0o600) << 16
            with _open(path, MAX_MEMBER) as stream, archive.open(info, "w") as target:
                before = os.fstat(stream.fileno())
                record = _copy(stream, target, min(MAX_MEMBER, MAX_TOTAL - total))
                if not _same(before, os.fstat(stream.fileno())) or not _same(before, path.stat(follow_symlinks=False)):
                    raise EvidenceError("private source changed during encryption")
            total += record["bytes"]
            records[path.name] = record
        if paths != _source_files(source):
            raise EvidenceError("private source inventory changed during encryption")
        manifest = {"schema": SCHEMA, "files": records, "total_bytes": total}
        raw = _json(manifest)
        if len(raw) > MAX_MANIFEST:
            raise EvidenceError("private manifest exceeds bound")
        archive.writestr(MANIFEST, raw)
    if destination.stat().st_size > MAX_ARCHIVE:
        raise EvidenceError("private archive exceeds bound")
    return manifest


def encrypt_directory(source, output):
    from Crypto.Cipher import AES
    key = _key()
    source, output = Path(source), Path(output)
    if output.exists() or output.is_symlink():
        raise EvidenceError("encrypted output must be new")
    if output.resolve().is_relative_to(source.resolve()):
        raise EvidenceError("encrypted output must be outside the source directory")
    with tempfile.TemporaryDirectory(prefix=".private-evidence-", dir=output.parent) as temporary:
        os.chmod(temporary, 0o700)
        plain = Path(temporary) / "plain.zip"
        # ZipFile receives an already exclusively created private descriptor.
        with _new(plain):
            pass
        manifest = _create_plaintext(source, plain)
        nonce = os.urandom(12)
        size = plain.stat().st_size
        header = MAGIC + nonce + struct.pack(">Q", size)
        cipher = AES.new(key, AES.MODE_GCM, nonce=nonce, mac_len=TAG_BYTES)
        cipher.update(header)
        with _open(plain, MAX_ARCHIVE) as incoming, _new(output) as destination:
            destination.write(header)
            while block := incoming.read(1 << 20):
                destination.write(cipher.encrypt(block))
            destination.write(cipher.digest())
            _finish(destination)
    return {"schema": SCHEMA, "status": "encrypted", "files": len(manifest["files"]),
            "plaintext_file_bytes": manifest["total_bytes"], "encrypted_bytes": output.stat().st_size,
            "encrypted_sha256": _digest(output), "release_qualified": False}


def _digest(path):
    with _open(path, MAX_ARCHIVE + HEADER_BYTES + TAG_BYTES) as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _restore_plaintext(plain, staging):
    # Read a bounded end record before zipfile allocates the central directory.
    with _open(plain, MAX_ARCHIVE) as stream:
        size = os.fstat(stream.fileno()).st_size
        if size < 22:
            raise EvidenceError("truncated private archive")
        stream.seek(-22, os.SEEK_END)
        signature, disk, cddisk, on_disk, entries, cdsize, offset, comment = struct.unpack("<4s4H2LH", stream.read(22))
        if (signature != b"PK\x05\x06" or disk or cddisk or on_disk != entries or comment
                or not 2 <= entries <= MAX_FILES + 1 or cdsize > 256 << 10
                or offset + cdsize + 22 != size):
            raise EvidenceError("invalid bounded private archive directory")
        stream.seek(0)
        with zipfile.ZipFile(stream) as archive:
            infos = archive.infolist()
            if len(infos) != entries:
                raise EvidenceError("private archive entry count differs")
            names, total = {}, 0
            for info in infos:
                name = _name(info.filename)
                mode = info.external_attr >> 16
                if (name in names or info.is_dir() or stat.S_IFMT(mode) not in (0, stat.S_IFREG)
                        or info.flag_bits != 0 or info.compress_type != zipfile.ZIP_STORED
                        or info.compress_size != info.file_size or info.extra or info.comment):
                    raise EvidenceError("unsupported private archive entry")
                if info.file_size > (MAX_MANIFEST if name == MANIFEST else MAX_MEMBER):
                    raise EvidenceError("private member exceeds bound")
                names[name] = info
                if name != MANIFEST:
                    total += info.file_size
            if len({name.casefold() for name in names}) != len(names) or total > MAX_TOTAL or MANIFEST not in names:
                raise EvidenceError("invalid private archive inventory")
            raw = archive.read(names[MANIFEST])
            manifest = json.loads(raw, object_pairs_hook=_pairs)
            if (not isinstance(manifest, dict) or set(manifest) != {"schema", "files", "total_bytes"}
                    or manifest["schema"] != SCHEMA or not isinstance(manifest["files"], dict)
                    or set(manifest["files"]) != set(names) - {MANIFEST}
                    or type(manifest["total_bytes"]) is not int or manifest["total_bytes"] != total
                    or _json(manifest) != raw):
                raise EvidenceError("private manifest differs from archive")
            for name, record in manifest["files"].items():
                if (not isinstance(record, dict) or set(record) != {"bytes", "sha256"}
                        or type(record["bytes"]) is not int or record["bytes"] != names[name].file_size
                        or not isinstance(record["sha256"], str) or not re.fullmatch("[a-f0-9]{64}", record["sha256"])):
                    raise EvidenceError("invalid private file fingerprint")
                with archive.open(names[name]) as incoming, _new(staging / name) as destination:
                    observed = _copy(incoming, destination, MAX_MEMBER)
                    _finish(destination)
                if observed != record:
                    raise EvidenceError("restored private file differs from authenticated manifest")
    return manifest


def decrypt_archive(archive, output):
    from Crypto.Cipher import AES
    key = _key()
    archive, output = Path(archive), Path(output)
    if output.exists() or output.is_symlink():
        raise EvidenceError("decryption destination must be new")
    with tempfile.TemporaryDirectory(prefix=".private-evidence-", dir=output.parent) as temporary:
        temporary = Path(temporary)
        os.chmod(temporary, 0o700)
        plain = temporary / "plain.zip"
        staging = temporary / "verified"
        staging.mkdir(mode=0o700)
        with _open(archive, MAX_ARCHIVE + HEADER_BYTES + TAG_BYTES) as incoming, _new(plain) as destination:
            before = os.fstat(incoming.fileno())
            header = incoming.read(HEADER_BYTES)
            if len(header) != HEADER_BYTES or header[:8] != MAGIC:
                raise EvidenceError("unrecognized private evidence header")
            size = struct.unpack(">Q", header[-8:])[0]
            if size > MAX_ARCHIVE or before.st_size != HEADER_BYTES + size + TAG_BYTES:
                raise EvidenceError("private evidence length does not match header")
            cipher = AES.new(key, AES.MODE_GCM, nonce=header[8:20], mac_len=TAG_BYTES)
            cipher.update(header)
            remaining = size
            while remaining:
                block = incoming.read(min(remaining, 1 << 20))
                if not block:
                    raise EvidenceError("truncated private ciphertext")
                destination.write(cipher.decrypt(block))
                remaining -= len(block)
            tag = incoming.read(TAG_BYTES)
            try:
                cipher.verify(tag)
            except ValueError as exc:
                raise EvidenceError("private evidence authentication failed") from exc
            if not _same(before, os.fstat(incoming.fileno())) or not _same(before, archive.stat(follow_symlinks=False)):
                raise EvidenceError("private ciphertext changed during decryption")
            _finish(destination)
        manifest = _restore_plaintext(plain, staging)
        output.mkdir(mode=0o700)
        for name, record in manifest["files"].items():
            with _open(staging / name, MAX_MEMBER) as incoming, _new(output / name) as destination:
                if _copy(incoming, destination, MAX_MEMBER) != record:
                    raise EvidenceError("private file changed before publication")
                _finish(destination)
    return {"schema": SCHEMA, "status": "authenticated_and_restored", "files": len(manifest["files"]),
            "plaintext_file_bytes": manifest["total_bytes"], "encrypted_sha256": _digest(archive),
            "release_qualified": False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    encrypt = commands.add_parser("encrypt")
    encrypt.add_argument("--source", type=Path, required=True)
    encrypt.add_argument("--output", type=Path, required=True)
    decrypt = commands.add_parser("decrypt")
    decrypt.add_argument("--archive", type=Path, required=True)
    decrypt.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        value = encrypt_directory(args.source, args.output) if args.action == "encrypt" else decrypt_archive(args.archive, args.output)
    except (ValueError, OSError, RuntimeError, TypeError, KeyError, ImportError, RecursionError, zipfile.BadZipFile) as exc:
        detail = str(exc) if isinstance(exc, EvidenceError) else "invalid or inaccessible private evidence"
        print("private evidence transport failed: " + detail, file=sys.stderr)
        return 1
    print(json.dumps(value, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
