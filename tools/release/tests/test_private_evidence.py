from __future__ import annotations

import base64
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import struct
import sys
import zipfile

import pytest
from Crypto.Cipher import AES

sys.path.insert(0, str(Path(__file__).parents[1]))
import private_evidence as private


@pytest.fixture
def key(monkeypatch):
    value = bytes(range(32))
    monkeypatch.setenv('READMIXER_EVIDENCE_KEY', base64.b64encode(value).decode('ascii'))
    return value


@pytest.fixture
def source(tmp_path):
    result = tmp_path / 'private'
    result.mkdir(mode=0o700)
    for name, data in {'stage.command.json': b'{"cwd":"/private/user/location"}\n',
                       'stage.stdout': b'synthetic fixture output\n',
                       'stage.stderr': b''}.items():
        (result / name).write_bytes(data)
    return result


def encrypted(source, tmp_path):
    path = tmp_path / 'evidence.rmp17'
    record = private.encrypt_directory(source, path)
    return path, record


def forged(payload, key, path):
    nonce = b'0123456789ab'
    header = private.MAGIC + nonce + struct.pack('>Q', len(payload))
    cipher = AES.new(key, AES.MODE_GCM, nonce=nonce, mac_len=16)
    cipher.update(header)
    path.write_bytes(header + cipher.encrypt(payload) + cipher.digest())
    return path


def zip_payload(members):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', compression=zipfile.ZIP_STORED) as archive:
        for name, data in members:
            archive.writestr(name, data)
    return buffer.getvalue()


def test_authenticated_roundtrip_retains_exact_bytes_and_private_permissions(source, tmp_path, key):
    path, encrypted_report = encrypted(source, tmp_path)
    result = private.decrypt_archive(path, tmp_path / 'restored')
    assert result['status'] == 'authenticated_and_restored'
    assert result['files'] == 3
    assert result['release_qualified'] is False
    assert result['encrypted_sha256'] == encrypted_report['encrypted_sha256']
    assert encrypted_report['encrypted_bytes'] == path.stat().st_size
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE((tmp_path / 'restored').stat().st_mode) == 0o700
    for original in source.iterdir():
        restored = tmp_path / 'restored' / original.name
        assert restored.read_bytes() == original.read_bytes()
        assert stat.S_IMODE(restored.stat().st_mode) == 0o600
    assert b'/private/user/location' not in path.read_bytes()
    assert not list(tmp_path.glob('.private-evidence-*'))
    assert str(tmp_path) not in json.dumps(result)


def test_random_nonce_changes_ciphertext_for_identical_input(source, tmp_path, key):
    first, _ = encrypted(source, tmp_path)
    second = tmp_path / 'again.rmp17'
    private.encrypt_directory(source, second)
    assert first.read_bytes()[8:20] != second.read_bytes()[8:20]
    assert first.read_bytes() != second.read_bytes()


@pytest.mark.parametrize('where', ['magic', 'nonce', 'length', 'ciphertext', 'tag', 'truncate', 'append'])
def test_tampered_transport_never_exposes_plaintext_destination(source, tmp_path, key, where):
    path, _ = encrypted(source, tmp_path)
    raw = bytearray(path.read_bytes())
    if where == 'truncate':
        raw = raw[:-1]
    elif where == 'append':
        raw += b'x'
    else:
        positions = {'magic': 0, 'nonce': 8, 'length': 27, 'ciphertext': 32, 'tag': len(raw)-1}
        raw[positions[where]] ^= 1
    path.write_bytes(raw)
    with pytest.raises(private.EvidenceError):
        private.decrypt_archive(path, tmp_path / 'restored')
    assert not (tmp_path / 'restored').exists()
    assert not list(tmp_path.glob('.private-evidence-*'))


def test_wrong_key_fails_authentication(source, tmp_path, key, monkeypatch):
    path, _ = encrypted(source, tmp_path)
    monkeypatch.setenv('READMIXER_EVIDENCE_KEY', base64.b64encode(b'x' * 32).decode())
    with pytest.raises(private.EvidenceError, match='authentication'):
        private.decrypt_archive(path, tmp_path / 'restored')
    assert not (tmp_path / 'restored').exists()
    assert not list(tmp_path.glob('.private-evidence-*'))


@pytest.mark.parametrize('value', ['', 'secret', 'A' * 43, base64.b64encode(b'x' * 31).decode(), base64.b64encode(b'x' * 33).decode()])
def test_invalid_keys_are_rejected_before_output(source, tmp_path, monkeypatch, value):
    monkeypatch.setenv('READMIXER_EVIDENCE_KEY', value)
    with pytest.raises(private.EvidenceError, match='32 random bytes'):
        private.encrypt_directory(source, tmp_path / 'output')
    assert not (tmp_path / 'output').exists()


def test_missing_key_is_not_generated_or_recorded_implicitly(source, tmp_path, monkeypatch):
    monkeypatch.delenv('READMIXER_EVIDENCE_KEY', raising=False)
    with pytest.raises(private.EvidenceError, match='32 random bytes'):
        private.encrypt_directory(source, tmp_path / 'output')
    assert not (tmp_path / 'output').exists()


@pytest.mark.parametrize('kind', ['symlink', 'directory', 'fifo', 'reserved', 'case-reserved', 'case-conflict'])
def test_unsafe_source_entries_cannot_enter_transport(source, tmp_path, key, kind, monkeypatch):
    if kind == 'symlink':
        (source / 'link').symlink_to(source / 'stage.stdout')
    elif kind == 'directory':
        (source / 'nested').mkdir()
    elif kind == 'fifo':
        os.mkfifo(source / 'pipe')
    elif kind == 'reserved':
        (source / private.MANIFEST).write_text('reserved')
    elif kind == 'case-reserved':
        (source / private.MANIFEST.upper()).write_text('reserved')
    else:
        # Default macOS APFS is case-insensitive, so two such real filenames
        # cannot coexist there. Exercise the inventory collision without
        # accidentally overwriting its real source fixture.
        original_scan = private.os.scandir
        class Alias:
            name = 'STAGE.STDOUT'
            def is_file(self, **_kwargs):
                return True
        @contextlib.contextmanager
        def conflicting_scan(directory):
            with original_scan(directory) as entries:
                values = list(entries)
                if directory == source:
                    values.append(Alias())
                yield iter(values)
        monkeypatch.setattr(private.os, 'scandir', conflicting_scan)
    with pytest.raises(private.EvidenceError):
        private.encrypt_directory(source, tmp_path / 'output')
    assert not (tmp_path / 'output').exists()


@pytest.mark.parametrize('limit', ['MAX_FILES', 'MAX_MEMBER', 'MAX_TOTAL', 'MAX_MANIFEST', 'MAX_ARCHIVE'])
def test_encryption_limits_are_enforced(source, tmp_path, key, monkeypatch, limit):
    monkeypatch.setattr(private, limit, 1)
    with pytest.raises(private.EvidenceError):
        private.encrypt_directory(source, tmp_path / 'output')
    assert not (tmp_path / 'output').exists()
    assert not list(tmp_path.glob('.private-evidence-*'))


def test_encrypted_output_and_restoration_never_overwrite(source, tmp_path, key):
    path, _ = encrypted(source, tmp_path)
    original = path.read_bytes()
    with pytest.raises(private.EvidenceError, match='new'):
        private.encrypt_directory(source, path)
    assert path.read_bytes() == original
    target = tmp_path / 'restored'
    target.mkdir()
    (target / 'foreign').write_bytes(b'preserve')
    with pytest.raises(private.EvidenceError, match='new'):
        private.decrypt_archive(path, target)
    assert (target / 'foreign').read_bytes() == b'preserve'


def test_ciphertext_symlink_and_fifo_rejected_without_blocking(source, tmp_path, key):
    path, _ = encrypted(source, tmp_path)
    link = tmp_path / 'link'
    link.symlink_to(path)
    with pytest.raises(private.EvidenceError, match='symlink'):
        private.decrypt_archive(link, tmp_path / 'restored')
    fifo = tmp_path / 'fifo'
    os.mkfifo(fifo)
    with pytest.raises(private.EvidenceError, match='regular'):
        private.decrypt_archive(fifo, tmp_path / 'restored')


def test_authenticated_but_hostile_paths_are_still_rejected(tmp_path, key):
    payload = zip_payload([('../escape', b'x'), (private.MANIFEST, b'{}')])
    path = forged(payload, key, tmp_path / 'forged')
    with pytest.raises(private.EvidenceError, match='basenames'):
        private.decrypt_archive(path, tmp_path / 'restored')
    assert not (tmp_path / 'escape').exists()
    assert not (tmp_path / 'restored').exists()


def test_authenticated_self_consistent_outer_encryption_does_not_bypass_inner_manifest(tmp_path, key):
    manifest = {'schema': private.SCHEMA, 'files': {'file': {'bytes': 4, 'sha256': '0' * 64}}, 'total_bytes': 4}
    payload = zip_payload([('file', b'data'), (private.MANIFEST, private._json(manifest))])
    path = forged(payload, key, tmp_path / 'forged')
    with pytest.raises(private.EvidenceError, match='differs from authenticated'):
        private.decrypt_archive(path, tmp_path / 'restored')
    assert not (tmp_path / 'restored').exists()
    assert not list(tmp_path.glob('.private-evidence-*'))


def test_cli_never_echoes_key_or_private_paths(source, tmp_path, key, capsys):
    path = tmp_path / 'output'
    assert private.main(['encrypt', '--source', str(source), '--output', str(path)]) == 0
    captured = capsys.readouterr()
    assert str(source) not in captured.out
    assert base64.b64encode(key).decode() not in captured.out + captured.err
    assert json.loads(captured.out)['status'] == 'encrypted'
    assert private.main(['decrypt', '--archive', str(tmp_path / 'private-missing'), '--output', str(tmp_path / 'restored')]) == 1
    failure = capsys.readouterr()
    assert str(tmp_path) not in failure.err
    assert 'private-missing' not in failure.err


def test_source_outside_encrypted_destination_scope(source, tmp_path, key):
    with pytest.raises(private.EvidenceError, match='outside'):
        private.encrypt_directory(source, source / 'output')


def test_interrupted_authenticated_restore_never_claims_success(source, tmp_path, key, monkeypatch):
    path, _ = encrypted(source, tmp_path)
    original_new = private._new
    output = tmp_path / 'restored'
    def fail_publish(path):
        if Path(path).parent == output:
            raise OSError('injected write failure')
        return original_new(path)
    monkeypatch.setattr(private, '_new', fail_publish)
    with pytest.raises(OSError, match='write failure'):
        private.decrypt_archive(path, output)
    assert not list(output.iterdir())
    assert not list(tmp_path.glob('.private-evidence-*'))
