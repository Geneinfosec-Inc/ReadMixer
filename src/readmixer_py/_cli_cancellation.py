"""Defer CLI signal exceptions only across an explicit resource handoff.

Library signal handlers remain untouched. The short transition ends as soon as
the caller can clean up the acquired resource; codec and worker I/O stay outside.
"""
from threading import local

_STATE = local()
_NOTE = 'CLI cancellation was deferred during resource adoption; original error preserved'


def defer_cli_signal(error, frame=None):
    if getattr(_STATE, 'depth', 0):
        if getattr(_STATE, 'pending', None) is None:
            _STATE.pending = error
        return True
    exiting = getattr(_STATE, 'exiting', None)
    if exiting is not None:
        if exiting.primary is None and exiting.dispatching is None:
            exiting.dispatching = error
        return True
    # Preserve a primary error during the final opcode of transition exit too.
    if frame is not None and frame.f_code is resource_transition.__exit__.__code__:
        if frame.f_locals.get('error') is not None:
            return True
    return False


class resource_transition:
    """Adopt the descriptor and pathname before dispatching CLI cancellation."""
    def __enter__(self):
        self.previous_depth = getattr(_STATE, 'depth', 0)
        _STATE.depth = self.previous_depth + 1
        return self

    def __exit__(self, error_type, error, traceback):
        if self.previous_depth:
            _STATE.depth = self.previous_depth
            return False
        self.primary = error
        self.dispatching = None
        previous_exit = getattr(_STATE, 'exiting', None)
        try:
            _STATE.exiting = self
            _STATE.depth = self.previous_depth
            pending = getattr(_STATE, 'pending', None)
            if pending is not None:
                self.dispatching = pending
                if error is not None and error is not pending and _NOTE not in getattr(error, '__notes__', ()):
                    error.add_note(_NOTE)
        finally:
            try:
                _STATE.pending = None
            finally:
                _STATE.exiting = previous_exit
        # A signal arriving during retirement is retained by the live token.
        # Retire all thread-local state before dispatching that exact exception.
        if error is None and self.dispatching is not None:
            raise self.dispatching
        return False
