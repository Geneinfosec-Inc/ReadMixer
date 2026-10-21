"""Plan 01 API/CLI regressions; subprocesses exercise actual exit statuses."""

from dataclasses import FrozenInstanceError
import gzip
import json
import logging
import multiprocessing
import os
from pathlib import Path
import subprocess
import sys

import pytest

from readmixer_py import (
    ConfigurationError, InspectConfig, MixConfig, RecoverConfig, UnsupportedFormatError,
    VerifyConfig, inspect, mix, recover, verify,
)
from readmixer_py.config import parse_size
from readmixer_py.errors import CancelledError, InputError, IntegrityError, ResourceError

ROOT = Path(__file__).resolve().parents[1]


def cli(*args, cwd=None):
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT / "src")
    # Installed-candidate acceptance supplies the exact console executable.
    # Source tests use the same module entrypoint without relying on PATH.
    console = env.get("READMIXER_TEST_CONSOLE")
    if console is not None:
        assert Path(console).is_absolute() and Path(console).is_file()
    command = [console] if console else [sys.executable, "-m", "readmixer_py"]
    return subprocess.run([*command, *map(str, args)],
                          cwd=cwd, env=env, text=True, capture_output=True, timeout=20)


def write_pair(root, name, count=1, crlf=False):
    root.mkdir(exist_ok=True)
    for role in (1, 2):
        data = b"".join(b"@synthetic_" + str(i).encode() + f"/{role}\nXX\n+private\n!!\n".encode()
                        for i in range(count))
        (root / f"{name}_R{role}.fq").write_bytes(data.replace(b"\n", b"\r\n") if crlf else data)


@pytest.mark.parametrize("size,expected", [("1", 1), ("5GB", 5_000_000_000), ("5GiB", 5 * 1024**3),
                                          ("32MiB", 32 * 1024**2), ("001B", 1)])
def test_API_02_exact_byte_units(size, expected):
    assert parse_size(size) == expected


@pytest.mark.parametrize("size", ["0", "-1", "+1", "1.5GiB", "nan", "inf", " 1GB", "1 GB", "1GB\n",
                                  "1gb", "18446744073709551616", "9" * 5000])
def test_API_02_invalid_sizes(size):
    with pytest.raises(ConfigurationError):
        parse_size(size)


def test_API_02_frozen_api_and_strict_types(tmp_path):
    config = InspectConfig(input_dir=tmp_path)
    with pytest.raises(FrozenInstanceError):
        config.deep = True
    for kwargs in ({"max_ram": True}, {"workers": 0}, {"max_record_bytes": 1 << 32}, {"max_record_bytes": None}, {"deep": 1}):
        with pytest.raises(ConfigurationError):
            InspectConfig(input_dir=tmp_path, **kwargs)
    with pytest.raises(ConfigurationError):
        InspectConfig()
    with pytest.raises(ConfigurationError):
        InspectConfig(input_dir=tmp_path, samples=tmp_path / "sheet.tsv")
    with pytest.raises(ConfigurationError):
        RecoverConfig(manifest=tmp_path, output=tmp_path, sample=("a", "a"))
    with pytest.raises(ConfigurationError):
        RecoverConfig(manifest=tmp_path, output=tmp_path, sample=[])


@pytest.mark.parametrize("args", [[], ["mix"], ["inspect", "--input", "a", "--samples", "b"],
                                  ["-s"], ["--scramble"], ["-c", "1"], ["-z"],
                                  ["inspect", "--input", "private-path", "--max-ram", "1.2GiB"],
                                  ["recover", "--manifest", "m", "--output", "o", "--sample", "a", "--sample", "a"]])
def test_API_02_invalid_cli_is_nonzero_and_redacted(args, tmp_path):
    result = cli(*args, cwd=tmp_path)
    assert result.returncode == 2
    assert not result.stdout
    assert "private-path" not in result.stderr
    assert "Completed" not in result.stderr


def test_API_01_help_version_outside_source_and_no_native_imports(tmp_path):
    for option in ("--help", "--version"):
        result = cli(option, cwd=tmp_path)
        assert result.returncode == 0 and "readmixer-py" in result.stdout
    script = "from readmixer_py.cli import build_parser; import sys; build_parser(); assert 'numpy' not in sys.modules; assert 'psutil' not in sys.modules; assert 'Crypto' not in sys.modules"
    result = subprocess.run([sys.executable, "-c", script], env={**os.environ, "PYTHONPATH": str(ROOT / "src")},
                            capture_output=True, text=True, cwd=tmp_path)
    assert result.returncode == 0, result.stderr


def test_API_03_invalid_operations_fail_without_artifacts(tmp_path):
    target = tmp_path / "not-created"
    for operation, config in (
        (mix, MixConfig(input_dir=tmp_path, output=target)),
        (verify, VerifyConfig(manifest=tmp_path / "missing.json")),
        (recover, RecoverConfig(manifest=tmp_path / "missing.json", output=target, sample=("a",))),
    ):
        with pytest.raises((InputError, IntegrityError)):
            operation(config)
    result = cli("mix", "--input", tmp_path, "--output", target, "--json")
    assert result.returncode == 1 and not result.stdout
    assert not target.exists()


def test_API_04_library_does_not_change_global_state(tmp_path):
    handlers = list(logging.getLogger().handlers)
    method = multiprocessing.get_start_method(allow_none=True)
    with pytest.raises(InputError):
        inspect(InspectConfig(input_dir=tmp_path / "absent"))
    assert handlers == logging.getLogger().handlers
    assert method == multiprocessing.get_start_method(allow_none=True)


def test_API_06_shallow_counts_unknown_deep_exact_readonly(tmp_path):
    inputs = tmp_path / "inputs"
    write_pair(inputs, "sampleB", count=3, crlf=True)
    write_pair(inputs, "sampleA", count=2)
    write_pair(inputs, "empty", count=0)
    before = {p.name: p.read_bytes() for p in inputs.iterdir()}
    shallow = inspect(InspectConfig(input_dir=inputs))
    assert shallow.samples == 3 and shallow.pairs is None and shallow.mix_eligible is None
    assert shallow.decoded_bytes is None
    deep = inspect(InspectConfig(input_dir=inputs, deep=True))
    assert (deep.pairs, deep.reads, deep.nonempty_samples, deep.empty_samples) == (5, 10, 2, 1)
    assert deep.mix_eligible is True
    assert deep.physical_bytes == sum(map(len, before.values()))
    assert deep.normalized_bytes == sum(len(v.replace(b"\r\n", b"\n")) for v in before.values())
    assert before == {p.name: p.read_bytes() for p in inputs.iterdir()}
    assert list(tmp_path.iterdir()) == [inputs]


def test_API_06_empty_and_one_nonempty_are_successful_ineligible_inspections(tmp_path):
    write_pair(tmp_path, "empty", count=0)
    report = inspect(InspectConfig(input_dir=tmp_path, deep=True))
    assert report.pairs == 0 and report.empty_samples == 1 and not report.mix_eligible
    write_pair(tmp_path, "sample", count=1)
    report = inspect(InspectConfig(input_dir=tmp_path, deep=True))
    assert report.pairs == 1 and not report.mix_eligible


def test_API_07_json_has_aggregates_without_input_identifiers(tmp_path):
    write_pair(tmp_path, "PRIVATE_SAMPLE")
    result = cli("inspect", "--input", tmp_path, "--deep", "--json", "--log-level", "debug")
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["report_version"] == 1 and report["pairs"] == 1
    assert "PRIVATE_SAMPLE" not in result.stdout + result.stderr
    assert str(tmp_path) not in result.stdout + result.stderr
    assert "synthetic_" not in result.stdout + result.stderr


@pytest.mark.parametrize("fault", ["truncated", "mismatch", "gzip"])
def test_AUD_04_AUD_12_no_success_after_decode_failure(tmp_path, fault):
    write_pair(tmp_path, "private")
    if fault == "truncated":
        (tmp_path / "private_R2.fq").write_bytes(b"@private-header\nXX\n+\n")
    elif fault == "mismatch":
        (tmp_path / "private_R2.fq").write_bytes(b"@wrong/2\nXX\n+\n!!\n")
    else:
        (tmp_path / "private_R2.fq").write_bytes(gzip.compress(b"@synthetic_0/2\nXX\n+\n!!\n")[:-1])
    result = cli("inspect", "--input", tmp_path, "--deep", "--json")
    assert result.returncode == 1 and not result.stdout
    assert "private-header" not in result.stderr


def test_P03_actual_normalized_guard_and_cancellation(tmp_path):
    write_pair(tmp_path, "sample", count=2, crlf=True)
    count = sum(len(p.read_bytes().replace(b"\r\n", b"\n")) for p in tmp_path.iterdir())
    assert inspect(InspectConfig(input_dir=tmp_path, deep=True, max_total_uncompressed=count)).normalized_bytes == count
    with pytest.raises(ResourceError):
        inspect(InspectConfig(input_dir=tmp_path, deep=True, max_total_uncompressed=count - 1))
    with pytest.raises(CancelledError):
        inspect(InspectConfig(input_dir=tmp_path, cancel=lambda: True))


def test_API_01_old_location_forwards_and_reports_missing_install(tmp_path):
    script = ROOT / "readmixer" / "readmixer.py"
    result = subprocess.run([sys.executable, str(script), "--help"],
                            env={**os.environ, "PYTHONPATH": str(ROOT / "src")},
                            capture_output=True, text=True, cwd=tmp_path)
    assert result.returncode == 0 and "paired" in result.stdout.lower()
    result = subprocess.run([sys.executable, "-I", "-S", str(script), "--help"],
                            capture_output=True, text=True, cwd=tmp_path)
    assert result.returncode == 1 and "pip install -e ." in result.stderr


def test_API_02_mix_requires_concrete_record_and_artifact_limits(tmp_path):
    for kwargs in ({"max_record_bytes": None}, {"max_artifact_bytes": None}):
        with pytest.raises(ConfigurationError):
            MixConfig(input_dir=tmp_path, output=tmp_path, **kwargs)


@pytest.mark.parametrize("signum,expected", [(2, 130), (15, 143)])
def test_API_03_signals_restore_handlers_without_success(tmp_path, monkeypatch, capsys, signum, expected):
    import signal
    import readmixer_py.cli as command
    before = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    def cancelled(config):
        os.kill(os.getpid(), signum)
        raise AssertionError("signal must interrupt inspection")
    monkeypatch.setattr(command, "inspect", cancelled)
    assert command.main(["inspect", "--input", str(tmp_path), "--json"]) == expected
    output = capsys.readouterr()
    assert not output.out and "cancelled" in output.err
    assert {sig: signal.getsignal(sig) for sig in before} == before
