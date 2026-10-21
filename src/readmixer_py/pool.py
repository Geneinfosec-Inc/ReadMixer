"""Private, identity-checked access to exactly the artifacts named by a pool."""

from contextlib import contextmanager
import hashlib
import os
import stat

from .artifacts.manifest import manifest_name, parse_manifest, parsed_object_bytes, _preflight_json
from .errors import IntegrityError, ResourceError
from .metadata import discard_failed_frames
from .paths import open_directory, validate_basename


def _snapshot(facts):
    if (not stat.S_ISREG(facts.st_mode) or facts.st_uid != os.getuid()
            or stat.S_IMODE(facts.st_mode) != 0o600):
        raise IntegrityError("pool artifacts must be private regular files owned by the current user")
    return facts.st_dev, facts.st_ino, facts.st_size, facts.st_mtime_ns, facts.st_ctime_ns


class _Input:
    def __init__(self, owner, stream, descriptor, snapshot, full_hash):
        self.owner, self.stream, self.descriptor = owner, stream, descriptor
        self.snapshot, self.full_hash = snapshot, full_hash
        self.digest = hashlib.sha256() if full_hash else None
        self.bytes_read = 0

    def read(self, size=-1):
        if type(size) is not int or not 0 <= size <= 1 << 20:
            raise ResourceError("pool file reads must have a bounded size")
        try:
            data = self.stream.read(size)
        except OSError as exc:
            raise IntegrityError("pool artifact read failed") from exc
        self.bytes_read += len(data)
        self.owner.consumed_bytes += len(data)
        if self.digest is not None:
            self.digest.update(data)
        return data

    def readinto(self, buffer):
        view = memoryview(buffer).cast('B')
        data = self.read(len(view))
        view[:len(data)] = data
        return len(data)

    def seek(self, offset, whence=0):
        if self.full_hash:
            raise IntegrityError("complete artifact validation requires a forward stream")
        return self.stream.seek(offset, whence)

    def tell(self):
        return self.stream.tell()

    def finish(self):
        if self.full_hash and (self.bytes_read != self.descriptor['bytes']
                               or self.digest.hexdigest() != self.descriptor['sha256']):
            raise IntegrityError("pool artifact physical length or digest mismatch")
        if _snapshot(os.fstat(self.stream.fileno())) != self.snapshot:
            raise IntegrityError("pool artifact changed while being consumed")


class Pool:
    """Admit bounded manifest metadata, then preflight only mode-required files."""

    @discard_failed_frames
    def __init__(self, manifest_path, budget, *, metadata_bytes, cancel=None):
        self.budget, self.cancel = budget, cancel
        self.root = self._metadata_lease = self._registry_lease = None
        self._files = {}
        self.manifest = None
        self.consumed_bytes = 0
        try:
            # Both encoded JSON and the parsed representation have explicit
            # independent ceilings. The loader checks its object cost as well.
            self._metadata_lease = budget.acquire('pool-metadata', 4096, descriptors=3, metadata_bytes=4096)
            self.root = open_directory(manifest_path.parent)
            name = validate_basename(manifest_path.name)
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_NONBLOCK", 0), dir_fd=self.root.fd)
            with os.fdopen(fd, 'rb', buffering=0) as source:
                before = _snapshot(os.fstat(source.fileno()))
                if before[2] > metadata_bytes:
                    raise ResourceError("pool manifest exceeds its admitted metadata ceiling")
                charge = 4096 + before[2] + 1
                self._metadata_lease.resize(charge, descriptors=3, metadata_bytes=charge)
                data = source.read(before[2] + 1)
                if len(data) != before[2] or _snapshot(os.fstat(source.fileno())) != before:
                    raise IntegrityError("pool manifest changed while being read")
            if _snapshot(os.stat(name, dir_fd=self.root.fd, follow_symlinks=False)) != before:
                raise IntegrityError("pool manifest pathname changed while being read")
            parsing = _preflight_json(data, min(4 * metadata_bytes, budget.remaining_metadata_bytes))
            charge = 4096 + len(data) + parsing
            self._metadata_lease.resize(charge, descriptors=3, metadata_bytes=charge)
            self.manifest = parse_manifest(data, max_metadata_bytes=metadata_bytes,
                                           max_parsed_bytes=parsing)
            if name != manifest_name(self.manifest['run_id']):
                raise IntegrityError("pool manifest basename does not match its run identity")
            self.source_manifest_sha256 = hashlib.sha256(data).hexdigest()
            self.consumed_bytes = len(data)
            del data
            retained = 4096 + parsed_object_bytes(self.manifest)
            self._metadata_lease.resize(retained, descriptors=3, metadata_bytes=retained)
            self._registry_lease = budget.acquire('pool-file-identities',
                                                  self.manifest['totals']['artifacts'] * 1024,
                                                  metadata_bytes=self.manifest['totals']['artifacts'] * 1024)
        except BaseException as exc:
            try:
                self.close()
            except BaseException:
                exc.add_note('pool admission cleanup also failed; primary error preserved')
            if isinstance(exc, OSError):
                raise IntegrityError("pool manifest could not be safely admitted") from exc
            raise

    @discard_failed_frames
    def preflight(self, *, indexes=True):
        seen = set()
        for bundle in self.manifest['bundles']:
            for role in ('r1', 'r1_gzi', 'r2', 'r2_gzi', 'key', 'index'):
                if role == 'index' and not indexes:
                    continue
                self.checkpoint()
                descriptor = bundle[role]
                name = validate_basename(descriptor['name'])
                try:
                    facts = os.stat(name, dir_fd=self.root.fd, follow_symlinks=False)
                except OSError as exc:
                    raise IntegrityError("a required pool artifact is unavailable") from exc
                snapshot = _snapshot(facts)
                if snapshot[2] != descriptor['bytes'] or snapshot[:2] in seen:
                    raise IntegrityError("pool artifact size or distinct file identity is invalid")
                seen.add(snapshot[:2])
                self._files[name] = snapshot

    def checkpoint(self):
        if self.cancel is not None and self.cancel():
            from .errors import CancelledError
            raise CancelledError()
        self.root.check()

    @contextmanager
    def open(self, descriptor, *, full_hash=True):
        self.checkpoint()
        name = descriptor['name']
        expected = self._files.get(name)
        if expected is None:
            raise IntegrityError("artifact was not admitted for this operation")
        with self.budget.acquire('pool-input', 8192, descriptors=1):
            try:
                fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_NONBLOCK", 0), dir_fd=self.root.fd)
            except OSError as exc:
                raise IntegrityError("pool artifact open failed") from exc
            with os.fdopen(fd, 'rb', buffering=0) as stream:
                if _snapshot(os.fstat(stream.fileno())) != expected:
                    raise IntegrityError("pool artifact identity changed before opening")
                reader = _Input(self, stream, descriptor, expected, full_hash)
                yield reader
                reader.finish()
                try:
                    visible = _snapshot(os.stat(name, dir_fd=self.root.fd, follow_symlinks=False))
                except OSError as exc:
                    raise IntegrityError("pool artifact pathname became unavailable") from exc
                if visible != expected:
                    raise IntegrityError("pool artifact pathname changed while being consumed")

    def close_sources(self):
        if self.root is not None:
            self.root.close()
            self.root = None
            if self._metadata_lease is not None:
                self._metadata_lease.resize(self._metadata_lease.bytes, metadata_bytes=self._metadata_lease.metadata_bytes)

    def close(self):
        primary = None

        def attempt(action):
            nonlocal primary
            try:
                action()
            except BaseException as error:
                if primary is None:
                    primary = error
                else:
                    primary.add_note('pool ownership cleanup also failed; primary error preserved')

        attempt(self.close_sources)
        # A source-close error must not strand the manifest, its registry, or
        # their leases when callers retain the failing verification traceback.
        # An interruption before close_sources transferred its directory still
        # leaves that owner here. AnchoredDirectory.close is idempotent and
        # clears its descriptor before the syscall, so retrying this owner never
        # closes a descriptor subsequently reused by another operation.
        if self.root is not None:
            root, self.root = self.root, None
            attempt(root.close)
        self._files.clear()
        if self.manifest is not None:
            self.manifest.clear()
            self.manifest = None
        for name in ('_registry_lease', '_metadata_lease'):
            lease = getattr(self, name, None)
            if lease is not None:
                attempt(lease.release)
                if lease.released:
                    setattr(self, name, None)
        if primary is not None:
            raise primary

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        try:
            self.close()
        except BaseException:
            if exc is None:
                raise
            exc.add_note('pool descriptor cleanup also failed')
