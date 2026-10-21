"""Public, privacy-safe failures. Library callers never receive process exits."""


class ReadMixerError(Exception):
    """Base class for failures with safe, bounded public messages."""


class ConfigurationError(ReadMixerError):
    """An invalid operation or configuration."""


class InputError(ReadMixerError):
    """An input cannot be admitted or decoded."""


class UnsupportedFormatError(ReadMixerError):
    """A format or operation is not supported by this build."""


class ResourceError(ReadMixerError):
    """The operation exceeds a resource allowance."""


class IntegrityError(ReadMixerError):
    """An artifact failed structural or integrity validation."""


class RankCollisionError(IntegrityError):
    """All eight independent terminal rank batches contain a collision."""


class PublicationError(ReadMixerError):
    """An output could not be committed."""


class CancelledError(ReadMixerError):
    """Cooperative cancellation; signal is set only by a command handler."""

    def __init__(self, message: str = "operation cancelled", *, signal: int | None = None):
        super().__init__(message)
        self.signal = signal
