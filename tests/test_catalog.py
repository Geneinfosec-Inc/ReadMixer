"""Read-only catalog regressions, with filesystem and independent TSV fixtures."""

import gzip
import os
from pathlib import Path

import pytest

from readmixer_py import catalog as module
from readmixer_py.catalog import MAX_TSV_LINE_BYTES, build_catalog
from readmixer_py.errors import ConfigurationError, InputError, ResourceError


FASTQ = b"@read\nAC\n+metadata\n!!\n"


def pair(root: Path, sample: str = "sample", family: str = "r", *, gzip_mate: bool = False):
    root.mkdir(parents=True, exist_ok=True)
    suffixes = {"r": ("_R1", "_R2"), "r001": ("_R1_001", "_R2_001"), "numeric": ("_1", "_2")}[family]
    r1, r2 = (root / (sample + suffix + ".fq") for suffix in suffixes)
    r1.write_bytes(FASTQ)
    if gzip_mate:
        r2 = r2.with_suffix(".fq.gz")
        r2.write_bytes(gzip.compress(FASTQ))
    else:
        r2.write_bytes(FASTQ)
    return r1, r2


def sheet(root: Path, text: str | bytes) -> Path:
    path = root / "samples.tsv"
    path.write_bytes(text.encode() if isinstance(text, str) else text)
    return path


def test_recursive_families_are_sorted_by_exact_ascii_identity(tmp_path):
    pair(tmp_path / "nested" / "deeper", "zulu", "r001", gzip_mate=True)
    pair(tmp_path, "Alpha", "numeric")
    pair(tmp_path / "nested", "alpha", "r")
    (tmp_path / "ignored.gz").write_text("not FASTQ")
    (tmp_path / "ignored.FQ").write_text("not FASTQ")
    before = set(tmp_path.rglob("*"))
    with build_catalog(input_dir=tmp_path) as catalog:
        assert [(s.index, s.sample_id) for s in catalog] == [(0, "Alpha"), (1, "alpha"), (2, "zulu")]
        assert catalog.samples[2].r2.codec == "gzip"
        assert catalog.samples[0].r1.codec == "plain"
        assert catalog.physical_bytes == sum(source.snapshot.size for s in catalog for source in (s.r1, s.r2))
        assert catalog.encoded_bytes < catalog.metadata_bytes
        with catalog.open_input(catalog.samples[0].r1) as stream:
            assert stream.read() == FASTQ
    assert set(tmp_path.rglob("*")) == before


@pytest.mark.parametrize("names", [
    ("a_R1.fq",),
    ("a_R1.fq", "a_2.fq"),
    ("a_R1.fq", "a_R2.fq", "a_R1.fastq"),
    ("unpaired.fastq",),
    ("_R1.fq", "_R2.fq"),
    ("a_L001_R1.fq", "a_L001_R2.fq"),
    ("a_L001_R1_001.fq", "a_L001_R2_001.fq"),
    ("a_L001_1.fq", "a_L001_2.fq"),
    ("é_R1.fq", "é_R2.fq"),
    ("a" * 129 + "_R1.fq", "a" * 129 + "_R2.fq"),
])
def test_invalid_discovered_pairs_fail(tmp_path, names):
    for name in names:
        (tmp_path / name).write_bytes(FASTQ)
    with pytest.raises(InputError):
        build_catalog(input_dir=tmp_path)


def test_orphans_never_pair_across_directories(tmp_path):
    (tmp_path / "other").mkdir()
    (tmp_path / "a_R1.fq").write_bytes(FASTQ)
    (tmp_path / "other" / "a_R2.fq").write_bytes(FASTQ)
    with pytest.raises(InputError, match="unique"):
        build_catalog(input_dir=tmp_path)


def test_duplicate_ids_in_distinct_directories_fail(tmp_path):
    pair(tmp_path / "first")
    pair(tmp_path / "second")
    with pytest.raises(InputError, match="unique"):
        build_catalog(input_dir=tmp_path)


def test_hard_link_reuse_is_rejected(tmp_path):
    r1, r2 = pair(tmp_path)
    r2.unlink()
    os.link(r1, r2)
    with pytest.raises(InputError, match="physical input"):
        build_catalog(input_dir=tmp_path)


@pytest.mark.parametrize("kind", ["root", "directory", "mate", "irrelevant"])
def test_discovery_rejects_symlinks(tmp_path, kind):
    root = tmp_path / "inputs"
    r1, r2 = pair(root)
    if kind == "root":
        selected = tmp_path / "linked-root"
        selected.symlink_to(root, target_is_directory=True)
    else:
        selected = root
        if kind == "directory":
            (root / "dirlink").symlink_to(tmp_path, target_is_directory=True)
        elif kind == "mate":
            r2.unlink()
            r2.symlink_to(r1)
        else:
            (root / "unrelated-link.txt").symlink_to(r1)
    with pytest.raises(InputError):
        build_catalog(input_dir=selected)


def test_sample_sheet_custom_paths_are_sheet_relative_and_canonical(tmp_path, monkeypatch):
    data = tmp_path / "inputs"
    data.mkdir()
    (data / "one.custom").write_bytes(FASTQ)
    (data / "two").write_bytes(gzip.compress(FASTQ))
    (data / "three").write_bytes(b"")
    (data / "four").write_bytes(b"")
    path = sheet(data, "sample_id\tr1\tr2\r\nz\tone.custom\ttwo\r\nA\tthree\tfour")
    monkeypatch.chdir(tmp_path)
    with build_catalog(samples=path) as catalog:
        assert catalog.root == data
        assert [s.sample_id for s in catalog] == ["A", "z"]
        assert catalog.samples[1].r2.codec == "gzip"
        assert catalog.samples[0].r1.snapshot.size == 0


@pytest.mark.parametrize("content", [
    b"\xef\xbb\xbfsample_id\tr1\tr2\n",
    b"sample_id\tr2\tr1\n",
    b"sample_id\tr1\tr2\n\n",
    b"sample_id\tr1\tr2\n#comment\n",
    b"sample_id\tr1\tr2\na\tone\n",
    b"sample_id\tr1\tr2\na\tone\ttwo\textra\n",
    b"sample_id\tr1\tr2\na\t\ttwo\n",
    b"sample_id\tr1\tr2\n\xff\tone\ttwo\n",
    b"sample_id\tr1\tr2\na\tone\ttwo\na\tone\ttwo\n",
    b"sample_id\tr1\tr2\na_L001\tone\ttwo\n",
])
def test_strict_sample_sheet_grammar(tmp_path, content):
    (tmp_path / "one").write_bytes(FASTQ)
    (tmp_path / "two").write_bytes(FASTQ)
    with pytest.raises(InputError):
        build_catalog(samples=sheet(tmp_path, content))


@pytest.mark.parametrize("bad_path", ["../one", "nested/../one", "/one", "one\x00hidden", "."])
def test_sample_sheet_rejects_unsafe_or_nonregular_paths(tmp_path, bad_path):
    (tmp_path / "two").write_bytes(FASTQ)
    with pytest.raises(InputError):
        build_catalog(samples=sheet(tmp_path, f"sample_id\tr1\tr2\na\t{bad_path}\ttwo\n"))


@pytest.mark.parametrize("kind", ["sheet", "component", "leaf"])
def test_sample_sheet_rejects_symlinks(tmp_path, kind):
    sub = tmp_path / "actual"
    r1, r2 = pair(sub)
    path = sheet(tmp_path, f"sample_id\tr1\tr2\na\tactual/{r1.name}\tactual/{r2.name}\n")
    if kind == "sheet":
        linked = tmp_path / "linked.tsv"
        linked.symlink_to(path)
        path = linked
    elif kind == "component":
        (tmp_path / "link").symlink_to(sub, target_is_directory=True)
        path.write_text(f"sample_id\tr1\tr2\na\tlink/{r1.name}\tactual/{r2.name}\n")
    else:
        r2.unlink()
        r2.symlink_to(r1)
    with pytest.raises(InputError):
        build_catalog(samples=path)


def test_sample_sheet_does_not_waive_lane_names_or_hardlinks(tmp_path):
    (tmp_path / "a_L001_R1.custom").write_bytes(FASTQ)
    (tmp_path / "two").write_bytes(FASTQ)
    with pytest.raises(InputError, match="multilane"):
        build_catalog(samples=sheet(tmp_path, "sample_id\tr1\tr2\na\ta_L001_R1.custom\ttwo\n"))
    with pytest.raises(InputError, match="physical input"):
        build_catalog(samples=sheet(tmp_path, "sample_id\tr1\tr2\na\ttwo\ttwo\n"))


def test_gzip_extension_requires_magic_even_for_empty_files(tmp_path):
    (tmp_path / "a_R1.fq.gz").write_bytes(b"")
    (tmp_path / "a_R2.fq").write_bytes(FASTQ)
    with pytest.raises(InputError, match="gzip"):
        build_catalog(input_dir=tmp_path)


def test_catalog_limits_are_enforced_before_retention(tmp_path, monkeypatch):
    pair(tmp_path, "a")
    pair(tmp_path, "b")
    with pytest.raises(ResourceError, match="metadata"):
        build_catalog(input_dir=tmp_path, max_metadata_bytes=1)
    monkeypatch.setattr(module, "MAX_SAMPLES", 1)
    with pytest.raises(ResourceError, match="samples"):
        build_catalog(input_dir=tmp_path)
    monkeypatch.setattr(module, "MAX_SAMPLES", 10_000)
    monkeypatch.setattr(module, "MAX_INPUT_FILES", 1)
    with pytest.raises(ResourceError, match="input files"):
        build_catalog(input_dir=tmp_path)
    monkeypatch.setattr(module, "MAX_INPUT_FILES", 20_000)
    monkeypatch.setattr(module, "MAX_ENCODED_CATALOG_BYTES", 1)
    with pytest.raises(ResourceError, match="Encoded"):
        build_catalog(input_dir=tmp_path)


def test_tsv_line_bound_is_checked_before_decoding(tmp_path):
    path = sheet(tmp_path, b"sample_id\tr1\tr2\n" + b"\xff" * (MAX_TSV_LINE_BYTES + 1))
    with pytest.raises(ResourceError, match="16 KiB"):
        build_catalog(samples=path)


def test_tsv_line_exact_limit_and_one_over_have_explicit_outcomes(tmp_path):
    # Repeated '.' components are legal and normalize to existing filenames;
    # this produces a real boundary row without OS NAME_MAX interference.
    (tmp_path / "one").write_bytes(FASTQ)
    (tmp_path / "two").write_bytes(FASTQ)
    fixed = b"a\t" + b"one\ttwo\n"
    row = b"a\t" + b"./" * ((MAX_TSV_LINE_BYTES - len(fixed)) // 2) + b"one\ttwo\n"
    assert len(row) == MAX_TSV_LINE_BYTES
    path = sheet(tmp_path, b"sample_id\tr1\tr2\n" + row)
    with build_catalog(samples=path) as result:
        assert len(result) == 1
    path.write_bytes(b"sample_id\tr1\tr2\n" + row[:-1] + b"x\n")
    with pytest.raises(ResourceError):
        build_catalog(samples=path)


def test_input_replacement_before_open_is_rejected(tmp_path):
    r1, _ = pair(tmp_path)
    with build_catalog(input_dir=tmp_path) as catalog:
        other = tmp_path / "replacement"
        other.write_bytes(FASTQ)
        os.replace(other, r1)
        with pytest.raises(InputError, match="changed"):
            with catalog.open_input(catalog.samples[0].r1):
                pass


@pytest.mark.parametrize("change", ["in_place", "replacement", "symlink", "root"])
def test_input_change_during_read_is_rejected(tmp_path, change):
    root = tmp_path / "inputs"
    r1, _ = pair(root)
    with build_catalog(input_dir=root) as catalog:
        with pytest.raises(InputError):
            with catalog.open_input(catalog.samples[0].r1) as stream:
                assert stream.read() == FASTQ
                if change == "in_place":
                    r1.write_bytes(FASTQ + b"changed")
                elif change == "replacement":
                    replacement = root / "replacement"
                    replacement.write_bytes(FASTQ)
                    os.replace(replacement, r1)
                elif change == "symlink":
                    r1.unlink()
                    r1.symlink_to(root / "sample_R2.fq")
                else:
                    root.rename(tmp_path / "moved")
                    root.mkdir()


def test_primary_decoder_failure_is_preserved(tmp_path):
    r1, _ = pair(tmp_path)
    with build_catalog(input_dir=tmp_path) as catalog:
        with pytest.raises(ValueError, match="primary"):
            with catalog.open_input(catalog.samples[0].r1):
                r1.unlink()
                raise ValueError("primary")


def test_errors_do_not_expose_private_paths(tmp_path):
    private_name = "private_subject_identifier"
    (tmp_path / (private_name + "_R1.fq")).write_bytes(FASTQ)
    try:
        build_catalog(input_dir=tmp_path)
    except InputError as exc:
        assert private_name not in str(exc)
        assert str(tmp_path) not in str(exc)
    else:
        pytest.fail("orphan input was admitted")
    path = sheet(tmp_path, f"sample_id\tr1\tr2\na\t{private_name}\tmissing\n")
    with pytest.raises(InputError) as caught:
        build_catalog(samples=path)
    assert private_name not in str(caught.value.__cause__)


def test_catalog_closes_descriptor_and_rejects_reuse(tmp_path):
    pair(tmp_path)
    with build_catalog(input_dir=tmp_path) as catalog:
        fd = catalog._root_fd
        source = catalog.samples[0].r1
    with pytest.raises(OSError):
        os.fstat(fd)
    catalog.close()
    assert catalog.samples == ()
    with pytest.raises(InputError, match="closed"):
        with catalog.open_input(source):
            pass


def test_input_selector_validation(tmp_path):
    with pytest.raises(ConfigurationError):
        build_catalog()
    with pytest.raises(ConfigurationError):
        build_catalog(input_dir=tmp_path, samples=tmp_path / "sheet")
    for value in (False, 0, -1, 1.2):
        with pytest.raises(ConfigurationError):
            build_catalog(input_dir=tmp_path, max_metadata_bytes=value)
    for value in ("", "bad\x00path", Path("bad\x00path"), 123):
        with pytest.raises(ConfigurationError):
            build_catalog(input_dir=value)
    with pytest.raises(ConfigurationError):
        build_catalog(input_dir=tmp_path, cancel=False)


def test_very_deep_directory_tree_uses_iterative_discovery(tmp_path):
    # Exceeds normal Python recursion depth without exceeding PATH_MAX.
    root = tmp_path / "inputs"
    root.mkdir()
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    depth = 0
    try:
        for _ in range(1050):
            os.mkdir("x", dir_fd=fd)
            child = os.open("x", os.O_RDONLY | os.O_DIRECTORY, dir_fd=fd)
            os.close(fd)
            fd = child
            depth += 1
        for name in ("a_R1.fq", "a_R2.fq"):
            with os.fdopen(os.open(name, os.O_WRONLY | os.O_CREAT, dir_fd=fd, mode=0o600), "wb") as stream:
                stream.write(FASTQ)
        with build_catalog(input_dir=root, max_metadata_bytes=64 * 1024 * 1024) as catalog:
            assert len(catalog) == 1
    finally:
        # Clean iteratively too: pytest's recursive cleanup has a depth limit.
        for name in ("a_R1.fq", "a_R2.fq"):
            try:
                os.unlink(name, dir_fd=fd)
            except FileNotFoundError:
                pass
        for _ in range(depth):
            parent = os.open("..", os.O_RDONLY | os.O_DIRECTORY, dir_fd=fd)
            os.close(fd)
            os.rmdir("x", dir_fd=parent)
            fd = parent
        os.close(fd)


@pytest.mark.parametrize("source", ["discovery", "sheet"])
def test_cancellation_interrupts_catalog_and_closes_owned_descriptors(tmp_path, monkeypatch, source):
    from readmixer_py.errors import CancelledError

    for number in range(10):
        pair(tmp_path, f"sample{number}")
    path = sheet(tmp_path, "sample_id\tr1\tr2\n" + "".join(
        f"sample{number}\tsample{number}_R1.fq\tsample{number}_R2.fq\n" for number in range(10)
    ))
    opened = set()
    real_open, real_dup, real_close, real_fdopen = os.open, os.dup, os.close, os.fdopen

    def track_open(*args, **kwargs):
        fd = real_open(*args, **kwargs)
        opened.add(fd)
        return fd

    def track_dup(*args, **kwargs):
        fd = real_dup(*args, **kwargs)
        opened.add(fd)
        return fd

    def track_close(fd):
        opened.discard(fd)
        return real_close(fd)

    def track_fdopen(fd, *args, **kwargs):
        # fdopen transfers ownership to a Python file object; its .close() does
        # not pass through os.close, so verify ownership transfer separately.
        opened.discard(fd)
        return real_fdopen(fd, *args, **kwargs)

    monkeypatch.setattr(os, "open", track_open)
    monkeypatch.setattr(os, "dup", track_dup)
    monkeypatch.setattr(os, "close", track_close)
    monkeypatch.setattr(os, "fdopen", track_fdopen)
    calls = 0

    def cancel():
        nonlocal calls
        calls += 1
        return calls >= 8

    with pytest.raises(CancelledError):
        if source == "discovery":
            build_catalog(input_dir=tmp_path, cancel=cancel)
        else:
            build_catalog(samples=path, cancel=cancel)
    assert calls == 8
    assert not opened


def test_immediate_cancellation_creates_no_catalog(tmp_path):
    from readmixer_py.errors import CancelledError

    with pytest.raises(CancelledError):
        build_catalog(input_dir=tmp_path / "does-not-exist", cancel=lambda: True)
