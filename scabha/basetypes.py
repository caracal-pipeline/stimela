from __future__ import annotations

import os.path
from collections import OrderedDict
from dataclasses import dataclass, field
from inspect import isclass
from itertools import zip_longest
from pathlib import Path
from typing import Any, List, Union, get_args, get_origin

import uritools
from typeguard import (
    TypeCheckerCallable,
    TypeCheckError,
    TypeCheckMemo,
    check_type,
    checker_lookup_functions,
)

from .exceptions import UnsetError


def EmptyDictDefault():
    return field(default_factory=lambda: OrderedDict())


def EmptyListDefault():
    return field(default_factory=lambda: [])


def ListDefault(*args):
    return field(default_factory=lambda: list(args))


def DictDefault(**kw):
    return field(default_factory=lambda: dict(**kw))


def EmptyClassDefault(obj):
    return field(default_factory=obj)


@dataclass
class Unresolved(object):
    value: str = ""
    errors: List[Exception] = EmptyListDefault()

    def __post_init__(self):
        if not self.value:
            self.value = "; ".join(map(str, self.errors))
        if not self.errors:
            self.errors = [UnsetError(f"'{self.value}' undefined")]
        # prevent {}-substitutions on Unresolved message
        self.value = self.value.replace("{", "{{").replace("}", "}}")

    def __str__(self):
        return f"Unresolved({self.value})"


class UNSET(Unresolved):
    """Marks unset values in formulas"""

    pass


class Placeholder(Unresolved):
    """Marks placeholder values that are guaranteed to resolve later, such as for-loop iterants"""

    pass


class SkippedOutput(Unresolved):
    """Marks invalid outputs of skipped steps"""

    def __str__(self):
        return f"Skipped({self.value})"


class URI(str):
    def __init__(self, value):
        uri_components = uritools.urisplit(value)
        self.scheme = uri_components.scheme or "file"  # Protocol.
        self.authority = uri_components.authority
        self.query = uri_components.query
        self.fragment = uri_components.fragment

        # NOTE(JSKenyon): We assume that remote URIs are properly formed and
        # absolute i.e. we do not reason about relative paths for e.g. s3. The
        # following attempts to express paths relative to the cwd but will
        # prefer absolute paths when inputs are outside the cwd. This can be
        # changed when stimela's minimum Python >= 3.12 by using the newly
        # added `walk_up` option.
        if self.scheme == "file":
            cwd = Path.cwd().absolute()
            abs_path = Path(uri_components.path).expanduser().resolve()
            self.abs_path = str(abs_path)
            try:
                self.path = str(abs_path.relative_to(cwd))
            except ValueError as e:
                if "is not in the subpath" in str(e):
                    self.path = self.abs_path
                else:
                    raise e
        else:
            self.path = self.abs_path = uri_components.path

        self.full_uri = uritools.uricompose(
            scheme=self.scheme, authority=self.authority, path=self.abs_path, query=self.query, fragment=self.fragment
        )

        self.remote = self.scheme != "file"

    def __str__(self):
        return self.full_uri if self.remote else self.path

    def __repr__(self):
        return self.full_uri


class File(URI):
    @property
    def NAME(self):
        """
        Returns:
            str: Input filename as a string
        """
        return os.path.basename(self)

    @property
    def PATH(self):
        """
        Returns:
            File: Absolute path of file (os.path.abspath)
        """
        return File(os.path.abspath(self))

    @property
    def DIR(self):
        """
        Returns:
            File: Path to parent directory
        """
        return File(os.path.dirname(self))

    @property
    def BASEPATH(self):
        """
        Returns:
            str: Filename withtout the extension
        """
        return os.path.splitext(self)[0]

    @property
    def BASENAME(self):
        """
        Returns:
            str: Base name of file
        """
        return os.path.splitext(self.NAME)[0]

    @property
    def EXT(self):
        """

        Returns:
            str : File extension
        """
        return os.path.splitext(self)[1]

    @property
    def EXISTS(self):
        """

        Returns:
            bool: Does the file exist?
        """
        return os.path.exists(self)


class Directory(File):
    pass


class MS(Directory):
    pass


FILE_TYPES = (File, MS, Directory, URI)


def is_file_type(dtype):
    return any(dtype == t for t in FILE_TYPES)


def is_file_list_type(dtype):
    return any(dtype == List[t] for t in FILE_TYPES)


def check_filelike(value: Any, origin_type: Any, args: tuple[Any, ...], memo: TypeCheckMemo) -> None:
    """Custom checker for filelike objects. Currently checks for strings."""
    if not isinstance(value, str):
        raise TypeCheckError(f"{value} is not compatible with URI or its subclasses.")


def filelike_lookup(origin_type: Any, args: tuple[Any, ...], extras: tuple[Any, ...]) -> TypeCheckerCallable | None:
    """Lookup the custom checker for filelike objects."""
    if isclass(origin_type) and issubclass(origin_type, URI):
        return check_filelike

    return None


checker_lookup_functions.append(filelike_lookup)  # Register custom type checker.


def get_filelikes(dtype, value, filelikes=None):
    """Recursively recover all filelike elements from a composite dtype."""

    filelikes = set() if filelikes is None else filelikes

    if value is UNSET or isinstance(value, Unresolved):
        return []

    origin = get_origin(dtype)
    args = get_args(dtype)

    if origin:  # Implies composition.
        if origin is dict:
            # No further work required for empty collections.
            if len(value) == 0:
                return filelikes

            k_dtype, v_dtype = args

            for k, v in value.items():
                filelikes = get_filelikes(k_dtype, k, filelikes)
                filelikes = get_filelikes(v_dtype, v, filelikes)

        elif origin in (tuple, list, set):
            # No further work required for empty collections.
            if len(value) == 0:
                return filelikes

            # This is a special case for tuples of arbitrary
            # length i.e. list-like behaviour. We can simply
            # strip out the Ellipsis.
            args = tuple([arg for arg in args if arg != ...])

            for dt, v in zip_longest(args, value, fillvalue=args[0]):
                filelikes = get_filelikes(dt, v, filelikes)

        elif origin is Union:
            for dt in args:
                try:
                    check_type(value, dt)
                except TypeCheckError:  # Value doesn't match dtype - incorrect branch.
                    continue
                filelikes = get_filelikes(dt, value, filelikes)

        else:
            raise ValueError(f"Failed to traverse {dtype} dtype when looking for files.")

    else:
        if is_file_type(dtype):
            filelikes.add(value)

    return filelikes
