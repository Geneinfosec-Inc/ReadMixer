"""One configuration contract for CLI and library users; sizes are integer bytes."""

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import re
from typing import Callable

from .errors import ConfigurationError

U64_MAX = (1 << 64) - 1
MAX_RECORD_BYTES = ((1 << 32) - 1 - 40) // 2
MIN_ARTIFACT_BYTES = 65_564
MAX_ARTIFACT_BYTES = 5_000_000_000
_SIZE = re.compile(r"([0-9]+)(B|KB|MB|GB|KiB|MiB|GiB)?", re.ASCII)
_FACTORS = {None: 1, "B": 1, "KB": 1000, "MB": 1000**2, "GB": 1000**3,
            "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3}
_SAMPLE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", re.ASCII)


class StorageMode(str, Enum):
    AUTO = "auto"
    EXTERNAL = "external"
    MEMORY = "memory"


class RecoveryMode(str, Enum):
    INDEXED = "indexed"
    SEQUENTIAL = "sequential"


def positive_integer(value: int, name: str, maximum: int = U64_MAX) -> None:
    if type(value) is not int or not 0 < value <= maximum:
        raise ConfigurationError(f"{name} must be a positive integer within its supported bound")


def parse_size(value: str) -> int:
    """Parse exact, positive decimal/binary byte units without float conversion."""
    match = _SIZE.fullmatch(value) if isinstance(value, str) and len(value) <= 24 else None
    if match is None:
        raise ConfigurationError("SIZE requires an integer with B, KB, MB, GB, KiB, MiB or GiB")
    result = int(match[1]) * _FACTORS[match[2]]
    positive_integer(result, "SIZE")
    return result


def _path(value: Path | None, name: str, *, required: bool = False) -> None:
    if value is None and not required:
        return
    if not isinstance(value, Path) or "\x00" in str(value):
        raise ConfigurationError(f"{name} must be a pathlib.Path without NUL bytes")


def _selectors(input_dir: Path | None, samples: Path | None) -> None:
    if (input_dir is None) == (samples is None):
        raise ConfigurationError("exactly one of input_dir or samples is required")
    _path(input_dir, "input_dir")
    _path(samples, "samples")


def _artifact_cap(value: int | None) -> None:
    if value is not None and (type(value) is not int or not MIN_ARTIFACT_BYTES <= value <= MAX_ARTIFACT_BYTES):
        raise ConfigurationError("max_artifact_bytes must be in 65564..5000000000 bytes")


def _compression(value: int) -> None:
    if type(value) is not int or not 0 <= value <= 9:
        raise ConfigurationError("compression_level must be an integer in 0..9")


@dataclass(frozen=True, kw_only=True)
class ResourceConfig:
    workers: int | None = None
    max_ram: int | None = None
    storage: StorageMode = StorageMode.AUTO
    temp_dir: Path | None = None
    max_record_bytes: int | None = None
    max_total_uncompressed: int | None = None
    cancel: Callable[[], bool] | None = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        for name in ("workers", "max_ram", "max_total_uncompressed"):
            value = getattr(self, name)
            if value is not None:
                positive_integer(value, name)
        if self.max_record_bytes is not None:
            positive_integer(self.max_record_bytes, "max_record_bytes", MAX_RECORD_BYTES)
        if not isinstance(self.storage, StorageMode):
            raise ConfigurationError("storage must be a StorageMode")
        _path(self.temp_dir, "temp_dir")
        if self.cancel is not None and not callable(self.cancel):
            raise ConfigurationError("cancel must be a callable returning a boolean")


@dataclass(frozen=True, kw_only=True)
class InspectConfig(ResourceConfig):
    input_dir: Path | None = None
    samples: Path | None = None
    deep: bool = False
    max_record_bytes: int = 1 << 20

    def __post_init__(self) -> None:
        super().__post_init__()
        _selectors(self.input_dir, self.samples)
        positive_integer(self.max_record_bytes, "max_record_bytes", MAX_RECORD_BYTES)
        if type(self.deep) is not bool:
            raise ConfigurationError("deep must be a boolean")


@dataclass(frozen=True, kw_only=True)
class MixConfig(ResourceConfig):
    output: Path
    input_dir: Path | None = None
    samples: Path | None = None
    max_record_bytes: int = 1 << 20
    max_artifact_bytes: int = MAX_ARTIFACT_BYTES
    compression_level: int = 6

    def __post_init__(self) -> None:
        super().__post_init__()
        _selectors(self.input_dir, self.samples)
        positive_integer(self.max_record_bytes, "max_record_bytes", MAX_RECORD_BYTES)
        positive_integer(self.max_artifact_bytes, "max_artifact_bytes")
        _path(self.output, "output", required=True)
        _artifact_cap(self.max_artifact_bytes)
        _compression(self.compression_level)


@dataclass(frozen=True, kw_only=True)
class VerifyConfig(ResourceConfig):
    manifest: Path
    originals: Path | None = None

    def __post_init__(self) -> None:
        super().__post_init__()
        _path(self.manifest, "manifest", required=True)
        _path(self.originals, "originals")


@dataclass(frozen=True, kw_only=True)
class RecoverConfig(ResourceConfig):
    manifest: Path
    output: Path
    sample: tuple[str, ...]
    io: RecoveryMode = RecoveryMode.INDEXED
    max_artifact_bytes: int | None = None
    compression_level: int = 6

    def __post_init__(self) -> None:
        super().__post_init__()
        _path(self.manifest, "manifest", required=True)
        _path(self.output, "output", required=True)
        if type(self.sample) is not tuple or not 1 <= len(self.sample) <= 10_000:
            raise ConfigurationError("sample must be a nonempty tuple of at most 10000 IDs")
        if any(not isinstance(s, str) or not _SAMPLE_ID.fullmatch(s) for s in self.sample):
            raise ConfigurationError("invalid sample ID grammar")
        if len(set(self.sample)) != len(self.sample):
            raise ConfigurationError("duplicate sample selections are not allowed")
        if not isinstance(self.io, RecoveryMode):
            raise ConfigurationError("io must be a RecoveryMode")
        _artifact_cap(self.max_artifact_bytes)
        _compression(self.compression_level)
