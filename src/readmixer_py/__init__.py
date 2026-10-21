"""ReadMixer Python foundations. Importing the API does not start native workers."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .inspection import InspectReport
    from .mixing import MixReport
    from .verification import VerifyReport
    from .recovery import RecoverReport

from .config import InspectConfig, MixConfig, RecoverConfig, RecoveryMode, StorageMode, VerifyConfig
from .errors import (
    CancelledError, ConfigurationError, InputError, IntegrityError,
    PublicationError, ReadMixerError, ResourceError, UnsupportedFormatError,
)

__version__ = "2.0.0.dev10"


def inspect(config: InspectConfig) -> "InspectReport":
    from .inspection import inspect as run
    return run(config)


def mix(config: MixConfig) -> "MixReport":
    from .mixing import mix as run
    return run(config)


def verify(config: VerifyConfig) -> "VerifyReport":
    from .verification import verify as run
    return run(config)


def recover(config: RecoverConfig) -> "RecoverReport":
    from .recovery import recover as run
    return run(config)


__all__ = [
    "inspect", "mix", "verify", "recover", "InspectConfig", "InspectReport", "MixConfig",
    "MixReport", "VerifyReport", "RecoverReport",
    "VerifyConfig", "RecoverConfig", "StorageMode", "RecoveryMode",
    "ReadMixerError", "ConfigurationError", "InputError", "UnsupportedFormatError",
    "ResourceError", "IntegrityError", "PublicationError", "CancelledError",
]


def __getattr__(name):
    if name == "InspectReport":
        from .inspection import InspectReport
        return InspectReport
    if name == "MixReport":
        from .mixing import MixReport
        return MixReport
    if name == "VerifyReport":
        from .verification import VerifyReport
        return VerifyReport
    if name == "RecoverReport":
        from .recovery import RecoverReport
        return RecoverReport
    raise AttributeError(name)
