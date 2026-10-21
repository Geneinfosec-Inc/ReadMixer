"""Bounded, read-only paired input discovery and descriptor ownership.

Catalog paths are private metadata. Error messages deliberately use roles and
numeric identities rather than echoing caller paths or sample identifiers.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field, fields, is_dataclass
import os
from pathlib import Path
import re
import stat
import sys
import traceback
from typing import BinaryIO, Callable, Iterator

from .errors import CancelledError, ConfigurationError, InputError, ResourceError
from .resources import Budget, checked_add


MAX_SAMPLES = 10_000
MAX_INPUT_FILES = 20_000
MAX_ENCODED_CATALOG_BYTES = 16 * 1024 * 1024
MAX_TSV_LINE_BYTES = 16 * 1024
DEFAULT_METADATA_BYTES = 16 * 1024 * 1024
_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z", re.ASCII)
_LANE_ID = re.compile(r"_L[0-9]{3}\Z", re.ASCII)
_LANE_FILE = re.compile(r"_L[0-9]{3}_(?:R[12](?:_001)?|[12])(?:\..*)?\Z", re.ASCII)
_FAMILIES = (
    ("r001", re.compile(r"(.+)_R([12])_001\Z", re.ASCII)),
    ("r", re.compile(r"(.+)_R([12])\Z", re.ASCII)),
    ("numeric", re.compile(r"(.+)_([12])\Z", re.ASCII)),
)
_FASTQ_SUFFIXES = (".fastq.gz", ".fq.gz", ".fastq", ".fq")


def _safe_io_error(message: str, error: OSError) -> InputError:
    """Drop path-bearing OS exception text at the public boundary."""
    result = InputError(message)
    result.__cause__ = OSError(error.errno, os.strerror(error.errno or 5))
    result.__suppress_context__ = True
    return result


def _flags(*, directory: bool = False) -> int:
    if not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_DIRECTORY"):
        raise InputError("This platform lacks required no-follow input opens")
    flags = os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    # A hostile replacement FIFO must fail type validation, not block open().
    flags |= os.O_DIRECTORY if directory else getattr(os, "O_NONBLOCK", 0)
    return flags


@dataclass(frozen=True, slots=True, weakref_slot=True)
class FileSnapshot:
    device: int
    inode: int
    size: int
    mtime_ns: int
    ctime_ns: int
    _metadata_owner: object = field(default=None, repr=False, compare=False)

    @classmethod
    def from_stat(cls, value: os.stat_result) -> FileSnapshot:
        return cls(value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)

    @property
    def identity(self) -> tuple[int, int]:
        return self.device, self.inode


@dataclass(frozen=True, slots=True, weakref_slot=True)
class InputFile:
    path: Path
    relative_path: Path
    snapshot: FileSnapshot
    codec: str
    _metadata_owner: object = field(default=None, repr=False, compare=False)

    @property
    def advertised_gzip(self) -> bool:
        return self.relative_path.name.endswith(".gz")


@dataclass(frozen=True, slots=True, weakref_slot=True)
class SampleInput:
    index: int
    sample_id: str
    r1: InputFile
    r2: InputFile
    _metadata_owner: object = field(default=None, repr=False, compare=False)


def _open_relative(root_fd: int, relative: Path | str, *, directory: bool = False) -> int:
    """Walk every component under the anchored root without following links."""
    parts = Path(relative).parts
    if not parts:
        return os.dup(root_fd)
    if Path(relative).is_absolute() or ".." in parts:
        raise InputError("An input path escapes its admitted root")
    current = os.dup(root_fd)
    try:
        for part in parts[:-1]:
            child = os.open(part, _flags(directory=True), dir_fd=current)
            os.close(current)
            current = child
        return os.open(parts[-1], _flags(directory=directory), dir_fd=current)
    except OSError as exc:
        raise _safe_io_error("Cannot safely open an admitted input path", exc)
    finally:
        os.close(current)


def _check_regular(fd: int) -> FileSnapshot:
    value = os.fstat(fd)
    if not stat.S_ISREG(value.st_mode):
        raise InputError("An input must be a regular nonsymlink file")
    return FileSnapshot.from_stat(value)


@dataclass(frozen=True, slots=True, weakref_slot=True)
class SampleCatalog:
    root: Path
    samples: tuple[SampleInput, ...]
    encoded_bytes: int
    metadata_bytes: int
    root_snapshot: FileSnapshot
    _root_fd: int
    _metadata_owner: object = field(default=None, repr=False, compare=False)
    peak_metadata_bytes: int = 0

    def __len__(self) -> int:
        return len(self.samples)

    def __iter__(self) -> Iterator[SampleInput]:
        return iter(self.samples)

    @property
    def physical_bytes(self) -> int:
        return sum(sample.r1.snapshot.size + sample.r2.snapshot.size for sample in self.samples)

    def __enter__(self) -> SampleCatalog:
        if self._root_fd < 0:
            raise InputError("The input catalog is closed")
        return self

    def close(self) -> None:
        """Close input descriptors; retained samples continue to own metadata."""
        if self._root_fd >= 0:
            fd = self._root_fd
            object.__setattr__(self, "_root_fd", -1)
            try:
                os.close(fd)
            finally:
                if self._metadata_owner is not None:
                    self._metadata_owner.close_descriptors()

    def dispose(self) -> None:
        """Discard owned graphs; borrowed samples/sources retain their owner.

        A borrower retaining a bare path or string must also retain its owning
        sample/source until its final use. No per-record public close is needed.
        """
        try:
            self.close()
        finally:
            object.__setattr__(self, "samples", ())
            object.__setattr__(self, "root", None)
            object.__setattr__(self, "root_snapshot", None)
            object.__setattr__(self, "_metadata_owner", None)

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            self.dispose()
        except BaseException:
            if exc is None:
                raise
            exc.add_note("catalog disposal also failed")

    def _check_root(self) -> None:
        if self._root_fd < 0:
            raise InputError("The input catalog is closed")
        try:
            fd = os.open(self.root, _flags(directory=True))
        except OSError as exc:
            raise _safe_io_error("The input root changed after catalog admission", exc)
        try:
            if FileSnapshot.from_stat(os.fstat(fd)).identity != self.root_snapshot.identity:
                raise InputError("The input root changed after catalog admission")
        finally:
            os.close(fd)

    @contextmanager
    def open_input(self, source: InputFile) -> Iterator[BinaryIO]:
        """Own one raw stream; verify its object and path before and after use.

        The caller must finish decoding inside this context. Completion checks
        are skipped when decoding itself failed so they cannot mask that error.
        Callers must supply quiescent files; this is not an immutable snapshot.
        """
        owner = self._metadata_owner
        if owner is not None:
            owner.pin_input()
        try:
            self._check_root()
            fd = _open_relative(self._root_fd, source.relative_path)
            try:
                if _check_regular(fd) != source.snapshot:
                    raise InputError("An input changed after catalog admission")
                stream = os.fdopen(fd, "rb", buffering=64 * 1024)
            except BaseException:
                os.close(fd)
                raise
            with stream:
                yield stream
                if _check_regular(stream.fileno()) != source.snapshot:
                    raise InputError("An input changed during decoding")
                self._check_root()
                current_fd = _open_relative(self._root_fd, source.relative_path)
                try:
                    if _check_regular(current_fd) != source.snapshot:
                        raise InputError("An input path changed during decoding")
                finally:
                    os.close(current_fd)
        finally:
            if owner is not None:
                owner.unpin_input()


def _representation_size(value: object) -> int:
    """Conservatively count referenced objects, including repeated references."""
    size = sys.getsizeof(value)
    if isinstance(value, Path):
        # pathlib has cached strings/parts in addition to its slotted object.
        return size + sys.getsizeof(str(value)) * 3 + _representation_size(value.parts)
    if is_dataclass(value) and not isinstance(value, type):
        return size + sum(_representation_size(getattr(value, f.name)) for f in fields(value)
                          if f.name != "_metadata_owner")
    if isinstance(value, (tuple, list)):
        return size + sum(_representation_size(item) for item in value)
    return size


class _MetadataOwner:
    """One shared reservation anchored by the catalog and every retained record."""
    __slots__ = ("lease", "_input_pins", "_sources_closed")

    def __init__(self, lease):
        self.lease = lease
        self._input_pins = 0
        self._sources_closed = False

    def pin_input(self):
        if self._sources_closed:
            raise InputError("The input catalog is closed")
        if self._input_pins >= 2:
            raise ResourceError("catalog admits at most two simultaneous input readers")
        self._input_pins += 1

    def unpin_input(self):
        self._input_pins -= 1
        if self._sources_closed:
            self.close_descriptors()

    def close_descriptors(self):
        self._sources_closed = True
        if not self._input_pins and not self.lease.released and self.lease.descriptors:
            self.lease.resize(self.lease.bytes, metadata_bytes=self.lease.metadata_bytes)

    def __del__(self):
        # The budget retains leases, but never this graph owner. Its final
        # reference disappearing is the accounting handoff for borrowed records.
        lease = getattr(self, "lease", None)
        if lease is not None:
            lease.release()


class _Admission:
    CONTROL_BYTES = 4096

    def __init__(self, limit: int, budget: Budget | None = None):
        if type(limit) is not int or limit <= 0:
            raise ConfigurationError("Catalog metadata allowance must be a positive integer")
        if limit < self.CONTROL_BYTES:
            raise ResourceError("Catalog metadata exceeds its admitted memory allowance")
        if budget is None:
            # Standalone internal callers still get one real accounting owner;
            # public operations always supply their existing invocation budget.
            budget = Budget(max(1 << 20, min(limit, 64 << 20) * 16))
        if not isinstance(budget, Budget):
            raise ConfigurationError("Catalog budget must be a Budget")
        lease = budget.acquire("catalog-metadata", self.CONTROL_BYTES, descriptors=6,
                               metadata_bytes=self.CONTROL_BYTES)
        try:
            self.owner = _MetadataOwner(lease)
        except BaseException:
            lease.release()
            raise
        self.limit, self.used, self.peak = limit, self.CONTROL_BYTES, self.CONTROL_BYTES
        self.encoded = 0

    def reserve(self, amount: int, *, encoded: int = 0) -> None:
        used, encoded_total = checked_add(self.used, amount), checked_add(self.encoded, encoded)
        if used > self.limit:
            raise ResourceError("Catalog metadata exceeds its admitted memory allowance")
        if encoded_total > MAX_ENCODED_CATALOG_BYTES:
            raise ResourceError("Encoded catalog exceeds its 16 MiB limit")
        self.owner.lease.resize(used, descriptors=self.owner.lease.descriptors, metadata_bytes=used)
        self.used, self.encoded = used, encoded_total
        self.peak = max(self.peak, used)

    def release(self, amount: int) -> None:
        remaining = self.used - amount
        if remaining < self.CONTROL_BYTES:
            raise ResourceError("Catalog metadata ownership underflow")
        self.owner.lease.resize(remaining, descriptors=self.owner.lease.descriptors,
                                metadata_bytes=remaining)
        self.used = remaining

    def retain(self, amount: int) -> None:
        if amount > self.used:
            raise ResourceError("Catalog representation exceeded its construction admission")
        self.release(self.used - amount)

    def discard(self):
        self.owner = None


@dataclass(slots=True)
class _Group:
    sample_id: str
    parent: tuple[int, int]
    family: str
    r1: InputFile | None = None
    r2: InputFile | None = None


class _Builder:
    def __init__(self, root: Path, fd: int, admission: _Admission, cancel: Callable[[], bool] | None):
        self.root, self.fd, self.admission, self.cancel = root, fd, admission, cancel
        self.groups: dict[str, _Group] = {}
        self.identities: set[tuple[int, int]] = set()

    def checkpoint(self) -> None:
        if self.cancel is not None and self.cancel():
            raise CancelledError()

    def group(self, sample_id: str, parent: tuple[int, int], family: str) -> _Group:
        _validate_id(sample_id)
        if sample_id in self.groups:
            group = self.groups[sample_id]
            if group.parent != parent:
                raise InputError("Sample IDs must be unique across input directories")
            if group.family != family:
                raise InputError("A sample uses incompatible mate naming families")
            return group
        if len(self.groups) >= MAX_SAMPLES:
            raise ResourceError("The catalog exceeds 10,000 samples")
        # Include dictionary capacity, UTF-8 encoding, group and final sample.
        self.admission.reserve(1024 + 4 * sys.getsizeof(sample_id), encoded=8 + len(sample_id))
        group = _Group(sample_id, parent, family)
        self.groups[sample_id] = group
        return group

    def source(self, relative: str) -> InputFile:
        self.checkpoint()
        if len(self.identities) >= MAX_INPUT_FILES:
            raise ResourceError("The catalog exceeds 20,000 input files")
        # Reserve path construction, split components, stat and dictionary/set
        # growth before retaining the candidate. Per-entry overcount is
        # deliberate: discarded construction metadata never escapes admission.
        # Include both relative/full pathlib representations, their component
        # tuples/caches, snapshots and identity-set capacity before allocation.
        root_text = str(self.root)
        charge = (4096 + 48 * (len(root_text) + len(relative))
                  + 512 * (len(self.root.parts) + relative.count(os.sep) + 2))
        self.admission.reserve(charge)
        path_bytes = len(os.fsencode(relative))
        self.admission.reserve(0, encoded=69 + path_bytes)
        path = Path(relative)
        if _LANE_FILE.search(path.name):
            raise InputError("Recognizable multilane inputs are unsupported")
        fd = _open_relative(self.fd, path)
        try:
            before = _check_regular(fd)
            if before.identity in self.identities:
                raise InputError("A physical input file is reused across mates or samples")
            magic = os.read(fd, 2)
            after = _check_regular(fd)
            if before != after:
                raise InputError("An input changed during catalog admission")
            if path.name.endswith(".gz") and magic != b"\x1f\x8b":
                raise InputError("An input advertised as gzip has no gzip signature")
            object.__setattr__(before, "_metadata_owner", self.admission.owner)
            source = InputFile(self.root / path, path, before,
                               "gzip" if magic == b"\x1f\x8b" else "plain", self.admission.owner)
            if _representation_size(source) + 512 > charge:
                raise ResourceError("Catalog source representation exceeds its preflight")
            self.identities.add(before.identity)
            return source
        except OSError as exc:
            raise _safe_io_error("Cannot inspect an input file", exc)
        finally:
            os.close(fd)


def _validate_id(sample_id: str) -> None:
    if not _ID.fullmatch(sample_id):
        raise InputError("Sample IDs must use 1 to 128 admitted ASCII characters")
    if _LANE_ID.search(sample_id):
        raise InputError("Recognizable multilane sample IDs are unsupported")


def _discovery(builder: _Builder) -> None:
    admission = builder.admission
    temporary = 0
    frontier = seen = entry = child = relative = expected = identity = None

    def reserve_temporary(amount):
        nonlocal temporary
        admission.reserve(amount)
        temporary += amount

    def release_temporary(amount):
        nonlocal temporary
        admission.release(amount)
        temporary -= amount

    try:
        # Iteration state, stat objects and one OS-bounded directory-entry name.
        reserve_temporary(8192)
        root_identity = FileSnapshot.from_stat(os.fstat(builder.fd)).identity
        frontier = [("", root_identity, 0)]
        seen = {root_identity}
        while frontier:
            builder.checkpoint()
            relative, expected, path_charge = frontier.pop()
            fd = _open_relative(builder.fd, relative, directory=True)
            try:
                if FileSnapshot.from_stat(os.fstat(fd)).identity != expected:
                    raise InputError("An input directory changed during discovery")
                with os.scandir(fd) as entries:
                    for entry in entries:
                        builder.checkpoint()
                        entry_stat = entry.stat(follow_symlinks=False)
                        if stat.S_ISLNK(entry_stat.st_mode):
                            raise InputError("Symlinks are forbidden in discovered input trees")
                        name = entry.name
                        # Charge the joined path BEFORE allocation, including
                        # frontier tuple/list capacity when ownership is handed on.
                        child_charge = (4096 + 64 * (len(relative) + len(name) + 1)
                                        + 512 * (relative.count(os.sep) + 2))
                        reserve_temporary(child_charge)
                        handed_on = False
                        try:
                            child = f"{relative}/{name}" if relative else name
                            if stat.S_ISDIR(entry_stat.st_mode):
                                identity = (entry_stat.st_dev, entry_stat.st_ino)
                                if identity in seen:
                                    raise InputError("An input directory has a repeated physical identity")
                                reserve_temporary(512)  # retained dedup identity/set capacity
                                seen.add(identity)
                                frontier.append((child, identity, child_charge))
                                handed_on = True
                            elif stat.S_ISREG(entry_stat.st_mode):
                                suffix = next((item for item in _FASTQ_SUFFIXES if name.endswith(item)), None)
                                if suffix is None:
                                    continue
                                stem = name[:-len(suffix)]
                                match = next(((family, regex.fullmatch(stem)) for family, regex in _FAMILIES
                                              if regex.fullmatch(stem)), None)
                                if match is None:
                                    raise InputError("A recognized FASTQ filename has no supported mate suffix")
                                family, parsed = match
                                assert parsed is not None
                                sample_id, role = parsed.groups()
                                group = builder.group(sample_id, expected, family)
                                field = "r" + role
                                if getattr(group, field) is not None:
                                    raise InputError("A sample declares multiple files for one mate role")
                                setattr(group, field, builder.source(child))
                            else:
                                raise InputError("An input tree contains a nonregular filesystem object")
                        finally:
                            child = None
                            if not handed_on:
                                release_temporary(child_charge)
            except OSError as exc:
                raise _safe_io_error("Cannot enumerate an input directory", exc)
            finally:
                os.close(fd)
                relative = expected = None
                release_temporary(path_charge)
    finally:
        if frontier is not None:
            frontier.clear()
        if seen is not None:
            seen.clear()
        # Drop remaining traversal aliases before shrinking their reservation.
        frontier = seen = entry = child = relative = expected = identity = None
        root_identity = entry_stat = name = stem = match = parsed = sample_id = role = group = None
        if temporary:
            release_temporary(temporary)


def _sheet_path(field: str) -> str:
    path = Path(field)
    if not field or "\x00" in field or path.is_absolute() or ".." in path.parts:
        raise InputError("Sample-sheet mate paths must be relative and contain no parent traversal")
    return field


def _read_sheet_line(stream: BinaryIO) -> str | None:
    raw = stream.readline(MAX_TSV_LINE_BYTES + 1)
    if not raw:
        return None
    if len(raw) > MAX_TSV_LINE_BYTES:
        raise ResourceError("A sample-sheet line exceeds its 16 KiB limit")
    if raw.endswith(b"\n"):
        raw = raw[:-1]
        if raw.endswith(b"\r"):
            raw = raw[:-1]
    try:
        return raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        raise InputError("The sample sheet must be UTF-8 without a BOM") from None


def _sample_sheet(builder: _Builder, name: str) -> None:
    # Simultaneously admit raw bounded bytes, UTF-8 decoding and split fields.
    builder.admission.reserve(8 * MAX_TSV_LINE_BYTES + 4096)
    fd = _open_relative(builder.fd, name)
    try:
        before = _check_regular(fd)
        stream = os.fdopen(fd, "rb", buffering=MAX_TSV_LINE_BYTES)
    except BaseException:
        os.close(fd)
        raise
    with stream:
        if _read_sheet_line(stream) != "sample_id\tr1\tr2":
            raise InputError("The sample sheet requires the exact sample_id, r1, r2 TSV header")
        while (line := _read_sheet_line(stream)) is not None:
            builder.checkpoint()
            values = line.split("\t")
            if len(values) != 3 or not all(values):
                raise InputError("Each sample-sheet row must have exactly three nonempty fields")
            sample_id, r1, r2 = values
            if sample_id in builder.groups:
                raise InputError("Sample IDs must be unique in the sample sheet")
            group = builder.group(sample_id, (0, 0), "sheet")
            group.r1 = builder.source(_sheet_path(r1))
            group.r2 = builder.source(_sheet_path(r2))
        if _check_regular(stream.fileno()) != before:
            raise InputError("The sample sheet changed during catalog admission")
        check = _open_relative(builder.fd, name)
        try:
            if _check_regular(check) != before:
                raise InputError("The sample sheet path changed during catalog admission")
        finally:
            os.close(check)


def build_catalog(
    *,
    input_dir: Path | str | None = None,
    samples: Path | str | None = None,
    max_metadata_bytes: int = DEFAULT_METADATA_BYTES,
    budget: Budget | None = None,
    cancel: Callable[[], bool] | None = None,
) -> SampleCatalog:
    """Build canonical paired inputs without creating or counting any data.

    The context manager disposes its root descriptor and metadata graph.
    ``close()`` alone closes descriptors while preserving charged samples;
    ``dispose()`` discards the graph, with retained record aliases still charged.
    ``metadata_bytes`` and ``peak_metadata_bytes`` are conservative admission
    bounds, not measured RSS. Public operations supply their shared ``budget``.
    """
    if (input_dir is None) == (samples is None):
        raise ConfigurationError("Exactly one input directory or sample sheet is required")
    selected_source = input_dir if input_dir is not None else samples
    if not isinstance(selected_source, (Path, str)) or "\x00" in str(selected_source) or not str(selected_source):
        raise ConfigurationError("The input selector must be a nonempty path without NUL bytes")
    if cancel is not None and not callable(cancel):
        raise ConfigurationError("Catalog cancellation requires a callable")
    if cancel is not None and cancel():
        raise CancelledError()
    admission = _Admission(max_metadata_bytes, budget)
    selected = root = root_snapshot = builder = ordered = result = group = sample_id = None
    fd = -1
    try:
        # This preflight covers normalization/component construction and root
        # snapshots before retaining a new pathlib graph.
        selector_text = os.fspath(selected_source)
        admission.reserve(4096 + 64 * len(selector_text)
                          + 512 * (selector_text.count(os.sep) + 2))
        selected = Path(os.path.abspath(selector_text))
        root = selected if input_dir is not None else selected.parent
        admission.reserve(0, encoded=64 + len(os.fsencode(root)))
        fd = os.open(root, _flags(directory=True))
        root_snapshot = FileSnapshot.from_stat(os.fstat(fd))
        object.__setattr__(root_snapshot, "_metadata_owner", admission.owner)
        builder = _Builder(root, fd, admission, cancel)
        if input_dir is not None:
            _discovery(builder)
        else:
            _sample_sheet(builder, selected.name)
        # Ordering, sorting workspace, tuple and samples coexist briefly.
        builder.checkpoint()
        admission.reserve(512 + len(builder.groups) * 256)
        ordered = []
        for index, sample_id in enumerate(sorted(builder.groups)):
            builder.checkpoint()
            group = builder.groups[sample_id]
            if group.r1 is None or group.r2 is None:
                raise InputError("A sample is missing one mate in its physical parent directory")
            ordered.append(SampleInput(index, sample_id, group.r1, group.r2, admission.owner))
        builder.checkpoint()
        result = SampleCatalog(root, tuple(ordered), admission.encoded, admission.used,
                               root_snapshot, fd, admission.owner, admission.peak)
        result._check_root()
        # Drop construction graphs before shrinking their charged representation.
        builder.groups.clear()
        builder.identities.clear()
        ordered.clear()
        builder = ordered = group = sample_id = selected = None
        retained = max(_Admission.CONTROL_BYTES, _representation_size(result) + 2048)
        admission.retain(retained)
        object.__setattr__(result, "metadata_bytes", retained)
        admission.discard()
        fd = -1  # Descriptor ownership has transferred to the catalog.
        return result
    except BaseException as error:
        # Clear completed helper frames before releasing their owned graph; the
        # stack/cause remain available, but a retained exception is not a hidden
        # catalog or borrowed-source owner after failed construction.
        traceback.clear_frames(error.__traceback__)
        if builder is not None:
            builder.groups.clear()
            builder.identities.clear()
        if ordered is not None:
            ordered.clear()
        builder = ordered = group = sample_id = root_snapshot = root = selected = None
        if result is not None:
            # fd remains owned here until the successful handoff above.
            object.__setattr__(result, "_root_fd", -1)
            result.dispose()
            result = None
        if fd >= 0:
            try:
                os.close(fd)
            except OSError:
                error.add_note("catalog descriptor cleanup also failed")
        admission.discard()
        if isinstance(error, OSError):
            raise _safe_io_error("Cannot build the paired input catalog", error)
        if isinstance(error, MemoryError):
            raise ResourceError("Cannot allocate admitted catalog metadata") from error
        raise
