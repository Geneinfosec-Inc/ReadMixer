"""Admission for transient JSON representations and safe operation failure frames."""

from contextlib import contextmanager
from functools import wraps
import traceback

from .artifacts.manifest import _json_chunks, parsed_object_bytes
from .errors import ResourceError
from .resources import checked_add, checked_multiply


def discard_failed_frames(operation):
    """Keep causal tracebacks, without retaining failed operation object graphs."""
    @wraps(operation)
    def guarded(*args, **kwargs):
        try:
            return operation(*args, **kwargs)
        except BaseException as error:
            current = error
            seen = set()
            while current is not None and id(current) not in seen:
                seen.add(id(current))
                traceback.clear_frames(current.__traceback__)
                current = current.__cause__ or current.__context__
            raise
    return guarded


@contextmanager
def serialization_lease(budget, document, *, max_bytes):
    """Charge validation/traversal and encoded-copy overlap before serialization.

    The caller already owns the document graph. A separate representation-sized
    allowance covers validation sets, sorted keys and escaped chunks. Count the
    exact encoding with bounded chunks, then admit bytearray growth plus the
    immutable result. The caller must discard encoded references before exit.
    """
    workspace = checked_add(65536, parsed_object_bytes(document))
    with budget.acquire('metadata-serialization', workspace,
                        metadata_bytes=workspace) as lease:
        length = 1  # Canonical final LF.
        for chunk in _json_chunks(document):
            length = checked_add(length, len(chunk))
            if length > max_bytes:
                raise ResourceError('encoded metadata exceeds its admitted allowance')
        del chunk
        charge = checked_add(workspace, checked_multiply(3, length))
        lease.resize(charge, metadata_bytes=charge)
        yield lease
