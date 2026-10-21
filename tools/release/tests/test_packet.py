from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
import stat
import struct
import sys
import tarfile
import zipfile

import pytest

sys.path.insert(0, str(Path(__file__).parents[1]))
import candidate
import packet


@pytest.fixture
def bundle(tmp_path):
    source = {"README.md": b"synthetic fixture\n", "src/readmixer_py/__init__.py": b"__version__ = '1.0'\n"}
    generated = {"PKG-INFO": b"Metadata-Version: 2.1\nName: readmixer-py\nVersion: 1.0\n", "setup.cfg": b"[egg_info]\n"}
    wheel = tmp_path / "readmixer_py-1.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("readmixer_py/__init__.py", source["src/readmixer_py/__init__.py"])
        archive.writestr("readmixer_py-1.0.dist-info/METADATA", generated["PKG-INFO"])
    sdist = tmp_path / "readmixer-py-1.0.tar.gz"
    with tarfile.open(sdist, "w:gz") as archive:
        for name, data in {**source, **generated}.items():
            item = tarfile.TarInfo("readmixer-py-1.0/" + name)
            item.size = len(data)
            archive.addfile(item, io.BytesIO(data))
    provenance = tmp_path / "provenance.json"
    provenance.write_text(json.dumps({"package_version": "1.0", "source_manifest": {
        name: {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()} for name, data in source.items()},
        "archives": {path.name: {"bytes": path.stat().st_size, "sha256": candidate.sha256_file(path)} for path in (wheel, sdist)}}))
    result = tmp_path / "candidate"
    candidate.adopt_candidate(wheel, sdist, provenance, result, 1)
    return result


def packed(bundle, tmp_path):
    path = tmp_path / "candidate.zip"
    result = packet.pack_candidate(bundle, path)
    return path, result


def rewrite_zip(original, changed, members=None, compression=zipfile.ZIP_STORED):
    with zipfile.ZipFile(original) as archive:
        records = [(item.filename, archive.read(item)) for item in archive.infolist()]
    if members:
        records = members(records)
    with zipfile.ZipFile(changed, "w", compression=compression) as archive:
        for name, data in records:
            archive.writestr(name, data)
    return hashlib.sha256(changed.read_bytes()).hexdigest()


def test_packet_round_trip_preserves_candidate_and_permissions(bundle, tmp_path):
    path, result = packed(bundle, tmp_path)
    output = tmp_path / "received"
    observed = packet.unpack_candidate(path, result["sha256"], output, result["candidate_id"])
    assert observed == result
    assert result["release_qualified"] is False
    assert candidate.load_candidate(output) == candidate.load_candidate(bundle)
    assert {p.name for p in output.iterdir()} == {p.name for p in bundle.iterdir()}
    for original in bundle.iterdir():
        assert (output / original.name).read_bytes() == original.read_bytes()
        assert stat.S_IMODE((output / original.name).stat().st_mode) == 0o600
    assert stat.S_IMODE(output.stat().st_mode) == 0o700
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    again = tmp_path / "again.zip"
    assert packet.pack_candidate(bundle, again) == result
    assert path.read_bytes() == again.read_bytes()


def test_checksum_failure_precedes_extraction(bundle, tmp_path):
    path, result = packed(bundle, tmp_path)
    output = tmp_path / "received"
    with pytest.raises(packet.PacketError, match="checksum"):
        packet.unpack_candidate(path, "0" * 64, output)
    assert not output.exists()
    assert not list(tmp_path.glob(".candidate-packet-*"))


def test_candidate_identity_mismatch_precedes_extraction(bundle, tmp_path):
    path, result = packed(bundle, tmp_path)
    output = tmp_path / "received"
    with pytest.raises(packet.PacketError, match="identity"):
        packet.unpack_candidate(path, result["sha256"], output, "sha256:" + "0" * 64)
    assert not output.exists()


@pytest.mark.parametrize("bad_name", ["../escape", "/absolute", "dir/file", "dir\\file", "./file", "C:escape", ".hidden", "bad\nname"])
def test_hostile_paths_never_escape(bundle, tmp_path, bad_name):
    original, _ = packed(bundle, tmp_path)
    changed = tmp_path / "changed.zip"
    digest = rewrite_zip(original, changed, lambda rows: [(bad_name, rows[0][1]), *rows[1:]])
    with pytest.raises(packet.PacketError, match="basename"):
        packet.unpack_candidate(changed, digest, tmp_path / "received")
    assert not (tmp_path / "received").exists()
    assert not (tmp_path / "escape").exists()


def test_duplicate_entries_rejected(bundle, tmp_path):
    original, _ = packed(bundle, tmp_path)
    changed = tmp_path / "changed.zip"
    with pytest.warns(UserWarning, match="Duplicate"):
        digest = rewrite_zip(original, changed, lambda rows: [rows[0], rows[0], rows[2]])
    with pytest.raises(packet.PacketError, match="duplicate"):
        packet.unpack_candidate(changed, digest, tmp_path / "received")


def test_compressed_and_extra_entries_rejected(bundle, tmp_path):
    original, _ = packed(bundle, tmp_path)
    changed = tmp_path / "changed.zip"
    digest = rewrite_zip(original, changed, compression=zipfile.ZIP_DEFLATED)
    with pytest.raises(packet.PacketError, match="unsupported"):
        packet.unpack_candidate(changed, digest, tmp_path / "received")
    digest = rewrite_zip(original, changed, lambda rows: [*rows, ("private.txt", b"never include")])
    with pytest.raises(packet.PacketError, match="three-entry"):
        packet.unpack_candidate(changed, digest, tmp_path / "received")


@pytest.mark.parametrize("filetype", [stat.S_IFLNK, stat.S_IFIFO, stat.S_IFDIR])
def test_special_entries_are_rejected(bundle, tmp_path, filetype):
    original, _ = packed(bundle, tmp_path)
    changed = tmp_path / "changed.zip"
    with zipfile.ZipFile(original) as archive:
        rows = [(info.filename, archive.read(info)) for info in archive.infolist()]
    with zipfile.ZipFile(changed, "w") as archive:
        for index, (name, data) in enumerate(rows):
            info = zipfile.ZipInfo(name)
            info.create_system = 3
            info.external_attr = ((filetype if index == 0 else stat.S_IFREG) | 0o600) << 16
            archive.writestr(info, data)
    with pytest.raises(packet.PacketError, match="unsupported"):
        packet.unpack_candidate(changed, candidate.sha256_file(changed), tmp_path / "received")


def test_existing_and_symlink_destinations_preserved(bundle, tmp_path):
    path, result = packed(bundle, tmp_path)
    output = tmp_path / "received"
    output.mkdir()
    (output / "foreign").write_bytes(b"preserve")
    with pytest.raises(packet.PacketError, match="new"):
        packet.unpack_candidate(path, result["sha256"], output)
    assert (output / "foreign").read_bytes() == b"preserve"
    alias = tmp_path / "alias"
    alias.symlink_to(output, target_is_directory=True)
    with pytest.raises(packet.PacketError, match="new"):
        packet.unpack_candidate(path, result["sha256"], alias)


def test_packet_output_never_overwrites(bundle, tmp_path):
    path, result = packed(bundle, tmp_path)
    original = path.read_bytes()
    with pytest.raises(FileExistsError):
        packet.pack_candidate(bundle, path)
    assert path.read_bytes() == original
    with pytest.raises(packet.PacketError, match="outside"):
        packet.pack_candidate(bundle, bundle / "packet.zip")


def test_unselected_source_content_is_not_transported(bundle, tmp_path):
    (bundle / "private.fastq").write_text("private synthetic placeholder")
    with pytest.raises(packet.PacketError, match="unselected"):
        packet.pack_candidate(bundle, tmp_path / "packet.zip")
    assert not (tmp_path / "packet.zip").exists()


def test_symlink_and_fifo_packet_inputs_rejected_without_blocking(bundle, tmp_path):
    path, result = packed(bundle, tmp_path)
    alias = tmp_path / "alias"
    alias.symlink_to(path)
    with pytest.raises(packet.PacketError, match="symlink"):
        packet.unpack_candidate(alias, result["sha256"], tmp_path / "received")
    fifo = tmp_path / "fifo"
    os.mkfifo(fifo)
    with pytest.raises(packet.PacketError, match="regular"):
        packet.unpack_candidate(fifo, result["sha256"], tmp_path / "received")


@pytest.mark.parametrize("limit", ["MAX_MANIFEST", "MAX_MEMBER", "MAX_PACKET", "MAX_DIRECTORY"])
def test_bounds_fail_before_publication(bundle, tmp_path, monkeypatch, limit):
    path, result = packed(bundle, tmp_path)
    monkeypatch.setattr(packet, limit, 1)
    with pytest.raises(packet.PacketError):
        packet.unpack_candidate(path, result["sha256"], tmp_path / "received")
    assert not (tmp_path / "received").exists()


def test_recomputed_outer_hash_does_not_hide_modified_candidate_archive(bundle, tmp_path):
    original, _ = packed(bundle, tmp_path)
    changed = tmp_path / "changed.zip"
    digest = rewrite_zip(original, changed, lambda rows: [(name, data + b"tampered" if name.endswith(".whl") else data) for name, data in rows])
    with pytest.raises(ValueError, match="changed"):
        packet.unpack_candidate(changed, digest, tmp_path / "received")
    assert not (tmp_path / "received").exists()
    assert not list(tmp_path.glob(".candidate-packet-*"))


def test_hidden_prefix_or_trailing_bytes_rejected(bundle, tmp_path):
    original, result = packed(bundle, tmp_path)
    for label, raw in (("prefix", b"hidden" + original.read_bytes()), ("suffix", original.read_bytes() + b"hidden")):
        changed = tmp_path / (label + ".zip")
        changed.write_bytes(raw)
        with pytest.raises(packet.PacketError):
            packet.unpack_candidate(changed, hashlib.sha256(raw).hexdigest(), tmp_path / label)
        assert not (tmp_path / label).exists()


def test_hidden_local_member_detected_even_if_directory_is_self_consistent(bundle, tmp_path):
    path, result = packed(bundle, tmp_path)
    raw = bytearray(path.read_bytes())
    # Add bytes before first local entry and consistently move the central directory
    # offset; ordinary ZIP readers accept this self-extracting archive shape.
    prefix = b"hidden-data"
    raw = bytearray(prefix) + raw
    eocd = len(raw) - 22
    old_directory = struct.unpack_from("<L", raw, eocd + 16)[0]
    struct.pack_into("<L", raw, eocd + 16, old_directory + len(prefix))
    position = old_directory + len(prefix)
    for _ in range(3):
        assert raw[position:position + 4] == b"PK\x01\x02"
        local = struct.unpack_from("<L", raw, position + 42)[0]
        struct.pack_into("<L", raw, position + 42, local + len(prefix))
        namelen, extralen, commentlen = struct.unpack_from("<3H", raw, position + 28)
        position += 46 + namelen + extralen + commentlen
    changed = tmp_path / "hidden.zip"
    changed.write_bytes(raw)
    with pytest.raises(packet.PacketError, match="hidden"):
        packet.unpack_candidate(changed, hashlib.sha256(raw).hexdigest(), tmp_path / "received")
    assert not (tmp_path / "received").exists()


def test_cli_returns_no_private_paths_on_failure(bundle, tmp_path, capsys):
    result = packet.main(["unpack", "--archive", str(tmp_path / "private-name"), "--sha256", "0" * 64,
                          "--output", str(tmp_path / "private-destination")])
    assert result == 1
    assert str(tmp_path) not in capsys.readouterr().err


def test_cli_reports_transport_without_release_qualification(bundle, tmp_path, capsys):
    path = tmp_path / "packet.zip"
    assert packet.main(["pack", "--candidate", str(bundle), "--output", str(path)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["release_qualified"] is False
    assert packet.main(["unpack", "--archive", str(path), "--sha256", result["sha256"],
                        "--candidate-id", result["candidate_id"], "--output", str(tmp_path / "received")]) == 0
    assert json.loads(capsys.readouterr().out) == result


def test_destination_created_during_validation_is_preserved(bundle, tmp_path, monkeypatch):
    path, result = packed(bundle, tmp_path)
    output = tmp_path / "received"
    original_write = packet._write_new
    writes = 0
    def competing_write(*args, **kwargs):
        nonlocal writes
        observed = original_write(*args, **kwargs)
        writes += 1
        if writes == 3:
            output.mkdir()
            (output / "foreign").write_bytes(b"preserved")
        return observed
    monkeypatch.setattr(packet, "_write_new", competing_write)
    with pytest.raises(FileExistsError):
        packet.unpack_candidate(path, result["sha256"], output)
    assert (output / "foreign").read_bytes() == b"preserved"
    assert not (output / "candidate.json").exists()


def test_copy_failure_cannot_leave_a_valid_candidate_marker(bundle, tmp_path, monkeypatch):
    path, result = packed(bundle, tmp_path)
    output = tmp_path / "received"
    original_write = packet._write_new
    def fail_publication(path, *args, **kwargs):
        if path.parent == output:
            raise OSError("injected storage failure")
        return original_write(path, *args, **kwargs)
    monkeypatch.setattr(packet, "_write_new", fail_publication)
    with pytest.raises(OSError, match="storage failure"):
        packet.unpack_candidate(path, result["sha256"], output)
    assert not (output / "candidate.json").exists()
    assert candidate.load_candidate(bundle)["expected_tests"] == 1


def test_ci_retains_reconcilable_public_report_layout_without_raw_private_files():
    workflow = Path(__file__).parents[3] / '.github/workflows/release-qualification.yml'
    text = workflow.read_text()
    block = text.split('          path: |\n')[-1]
    paths = {line.strip() for line in block.splitlines() if line.startswith('            ')}
    prefix = '${{ env.READMIXER_RELEASE_ROOT }}/report/'
    assert {prefix + item for item in (
        'report.json', 'harness/', 'environment.json', 'requirements.lock',
        'platform-applicability.json', 'rebuilt/*.whl')} <= paths
    # The complete private/ directory is restored only from authenticated
    # ciphertext; a broad report directory glob would disclose raw paths/logs.
    assert not any('/private' in path for path in paths)
    assert prefix not in paths
    assert prefix + '**' not in paths
    assert prefix + '*' not in paths
