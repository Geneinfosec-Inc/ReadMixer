"""The development utility checks prefixes and prints only aggregate results."""

import gzip
import json
from pathlib import Path
import subprocess
import sys

import pytest

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "check_storage_inputs.py"


def run_check(directory, *arguments):
    return subprocess.run([sys.executable, str(_SCRIPT), "--input", str(directory), *arguments],
                          capture_output=True, text=True, check=False, timeout=30)


def fixture_inputs(directory, *, malformed_suffix=False):
    for sample in ("private_A", "private_B"):
        for role in (1, 2):
            # Opaque non-UTF-8 quality and CRLF normalization must survive the
            # parser/codec/authenticated-storage path byte for byte.
            first = f"@private_read/{role} metadata\r\n".encode() + b"AC\r\n+private_plus\r\n\xff!\r\n"
            suffix = b"invalid unexamined FASTQ suffix\n" if malformed_suffix else b""
            (directory / f"{sample}_R{role}.fastq.gz").write_bytes(gzip.compress(first + suffix))


def test_prefix_storage_check_is_private_and_explicit_about_its_scope(tmp_path):
    fixture_inputs(tmp_path, malformed_suffix=True)
    result = run_check(tmp_path, "--pairs-per-sample", "1", "--max-ram", "128MiB")
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads(result.stdout)
    assert report["scope"] == "bounded_prefix_storage_check"
    assert report["full_input_validation"] is False
    assert report["mix_recovery_validation"] is False
    assert report["samples"] == report["nonempty_prefixes"] == 2
    assert report["pairs"] == report["verified_pairs"] == 2
    for flag in ("completed", "canonical_sha256_equal", "numeric_identities_equal",
                 "encrypted_external_storage", "all_segment_seals_verified", "scratch_cleanup_complete"):
        assert report[flag] is True
    assert report["peak_scratch_bytes"] > 0
    assert not result.stderr
    assert "private_" not in result.stdout
    assert str(tmp_path) not in result.stdout
    assert all(path.name.endswith(".fastq.gz") for path in tmp_path.iterdir())
    # The next record is malformed: increasing the explicit prefix bound must
    # fail instead of claiming a successful complete-input scan.
    malformed = run_check(tmp_path, "--pairs-per-sample", "2")
    assert malformed.returncode == 1
    assert json.loads(malformed.stdout)["completed"] is False
    assert "private_" not in malformed.stdout + malformed.stderr
    assert not malformed.stderr


def test_short_samples_complete_without_padding_to_requested_prefix(tmp_path):
    fixture_inputs(tmp_path)
    result = run_check(tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads(result.stdout)
    assert report["pairs_per_sample_limit"] == 128
    assert report["pairs"] == report["verified_pairs"] == 2
    assert report["canonical_sha256_equal"]


@pytest.mark.parametrize("arguments", [("--max-ram", "1.5GiB"), ("--pairs-per-sample", "0")])
def test_invalid_configuration_never_echoes_private_paths(tmp_path, arguments):
    result = run_check(tmp_path / "private_missing_input", *arguments)
    assert result.returncode != 0
    assert "private_missing_input" not in result.stdout + result.stderr
    assert str(tmp_path) not in result.stdout + result.stderr
