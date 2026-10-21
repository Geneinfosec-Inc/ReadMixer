"""Anchored private directory operations shared by scratch and publication."""

from dataclasses import dataclass, field
import os
from pathlib import Path
import re
import secrets
import stat

from .errors import ConfigurationError, InputError, PublicationError
from ._cli_cancellation import resource_transition


def _flags():
    if not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_DIRECTORY"):
        raise PublicationError("platform lacks required no-follow directory operations")
    return os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)


def validate_basename(name):
    if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,239}", name, re.ASCII):
        raise ConfigurationError("artifact basename must contain only bounded ASCII name characters")
    if name in (".", ".."):
        raise ConfigurationError("artifact basename cannot traverse directories")
    return name


def _absolute(path):
    if not isinstance(path, (Path, str)) or "\x00" in str(path):
        raise ConfigurationError("directory must be a valid filesystem path")
    candidate = Path(path).absolute()
    if ".." in candidate.parts:
        raise ConfigurationError("directory path cannot contain parent traversal")
    return candidate


def exclude_input_tree(candidate, input_root):
    if input_root is not None:
        root = _absolute(input_root).resolve(strict=True)
        resolved = _absolute(candidate).resolve(strict=False)
        if resolved == root or root in resolved.parents:
            raise InputError("output and scratch directories must be outside the input root")


def _walk(path):
    descriptor = os.open(path.anchor, _flags())
    try:
        for component in path.parts[1:]:
            child = os.open(component, _flags(), dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


@dataclass
class AnchoredDirectory:
    path: Path = field(repr=False)
    fd: int
    device: int
    inode: int
    name: str | None = None
    _parent: "AnchoredDirectory | None" = field(default=None, repr=False)
    _input_root: Path | None = field(default=None, repr=False)
    _private_destination: bool = field(default=False, repr=False)

    def check(self):
        if self.fd < 0:
            raise PublicationError("directory anchor is closed")
        current = os.fstat(self.fd)
        if (current.st_dev, current.st_ino) != (self.device, self.inode):
            raise PublicationError("directory anchor identity changed")
        if self._parent is not None and (current.st_uid != os.getuid() or stat.S_IMODE(current.st_mode) != 0o700):
            raise PublicationError("private child directory permissions changed")
        if self._private_destination and (current.st_uid != os.getuid() or current.st_mode & 0o022):
            raise PublicationError("destination must be current-user-owned and not group/other writable; choose a private directory")
        # Keep every traversal FD protected through the caller's final close;
        # deferring only inside _walk would dispatch before check_fd is owned.
        with resource_transition():
            try:
                check_fd = _walk(self.path)
                try:
                    visible = os.fstat(check_fd)
                    if (visible.st_dev, visible.st_ino) != (self.device, self.inode):
                        raise PublicationError("directory pathname was replaced")
                finally:
                    os.close(check_fd)
            except OSError as exc:
                raise PublicationError("cannot revalidate directory pathname") from exc
        exclude_input_tree(self.path, self._input_root)

    def new_private_child(self, prefix="rmpy-"):
        if not isinstance(prefix, str) or not re.fullmatch(r"[a-z][a-z0-9-]{0,31}", prefix):
            raise ConfigurationError("private child prefix is invalid")
        self.check()
        for _ in range(16):
            name = prefix + secrets.token_hex(16)
            path = self.path / name
            exclude_input_tree(path, self._input_root)
            try:
                os.mkdir(name, mode=0o700, dir_fd=self.fd)
            except FileExistsError:
                continue
            except OSError as exc:
                raise PublicationError("cannot exclusively create private staging directory") from exc
            descriptor = None
            expected = None
            try:
                expected = os.stat(name, dir_fd=self.fd, follow_symlinks=False)
                descriptor = os.open(name, _flags(), dir_fd=self.fd)
                current = os.fstat(descriptor)
                if ((current.st_dev, current.st_ino) != (expected.st_dev, expected.st_ino)
                        or not stat.S_ISDIR(current.st_mode) or current.st_uid != os.getuid()
                        or current.st_mode & 0o077):
                    raise PublicationError("new staging directory failed private ownership checks")
                result = AnchoredDirectory(path, descriptor, current.st_dev, current.st_ino,
                                           name, self, self._input_root, True)
                result.check()
                return result
            except BaseException:
                if descriptor is not None:
                    os.close(descriptor)
                # Do not remove a replacement or anything whose identity was not recorded.
                if expected is not None:
                    try:
                        now = os.stat(name, dir_fd=self.fd, follow_symlinks=False)
                        if (now.st_dev, now.st_ino) == (expected.st_dev, expected.st_ino):
                            os.rmdir(name, dir_fd=self.fd)
                    except OSError:
                        pass
                raise
        raise PublicationError("exclusive private directory name allocation exhausted")

    def remove_if_empty(self):
        if self._parent is None or self.name is None:
            raise PublicationError("caller-owned directories cannot be removed")
        self._parent.check()
        try:
            current = os.stat(self.name, dir_fd=self._parent.fd, follow_symlinks=False)
        except FileNotFoundError:
            self.close()
            return
        if (current.st_dev, current.st_ino) != (self.device, self.inode) or not stat.S_ISDIR(current.st_mode):
            raise PublicationError("refusing to remove a replaced private directory")
        try:
            os.rmdir(self.name, dir_fd=self._parent.fd)
            os.fsync(self._parent.fd)
        except OSError as exc:
            raise PublicationError("cannot remove or synchronize owned private directory") from exc
        finally:
            self.close()

    def close(self):
        if self.fd >= 0:
            descriptor, self.fd = self.fd, -1
            os.close(descriptor)

    def __enter__(self):
        self.check()
        return self

    def __exit__(self, *_exception):
        self.close()


def open_directory(path, *, private_destination=False, input_root=None):
    path = _absolute(path)
    try:
        exclude_input_tree(path, input_root)
        descriptor = _walk(path)
    except OSError as exc:
        raise PublicationError("cannot open an existing nonsymlink directory") from exc
    try:
        current = os.fstat(descriptor)
        result = AnchoredDirectory(path, descriptor, current.st_dev, current.st_ino,
                                   _input_root=input_root, _private_destination=private_destination)
        result.check()
        return result
    except BaseException:
        os.close(descriptor)
        raise
