from dataclasses import field, dataclass
from collections import OrderedDict
from typing import List, Union, get_args, get_origin
import os.path
import re
from .exceptions import UnsetError
from itertools import zip_longest
from typeguard import check_type, TypeCheckError


def EmptyDictDefault():
    return field(default_factory=lambda:OrderedDict())

def EmptyListDefault():
    return field(default_factory=lambda:[])

def ListDefault(*args):
    return field(default_factory=lambda:list(args))

def DictDefault(**kw):
    return field(default_factory=lambda:dict(**kw))


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
    """Marks placeholder values that are guaranteed to resolve later, such as fot-loop iterants"""
    pass

class SkippedOutput(Unresolved):
    """Marks invalid outputs of skipped steps"""
    def __str__(self):
        return f"Skipped({self.value})"

class URI(str):
    def __init__(self, value):
        self.protocol, self.path, self.remote = URI.parse(value)

    @staticmethod
    def parse(value: str, expand_user=True):
        """
        Parses URI. If URI does not start with "protocol://", assumes "file://"

        Returns tuple of (protocol, path, is_remote)

        If expand_user is True, ~ in (file-protocol) paths will be expanded.
        """
        match = re.fullmatch("((\w+)://)(.*)", value)
        if not match:
            protocol, path, remote = "file", value, False
        else:
            _, protocol, path = match.groups()
            remote = protocol != "file"
        if not remote and expand_user:
            path = os.path.expanduser(path)
        return protocol, path, remote


class File(URI):
    @property
    def NAME(self):
        return File(os.path.basename(self))

    @property
    def PATH(self):
        return File(os.path.abspath(self))

    @property
    def DIR(self):
        return File(os.path.dirname(self))

    @property
    def BASEPATH(self):
        return File(os.path.splitext(self)[0])

    @property
    def BASENAME(self):
        return File(os.path.splitext(self.NAME)[0])

    @property
    def EXT(self):
        return os.path.splitext(self)[1]

    @property
    def EXISTS(self):
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


class Skip(object):
    def iterate_samples(self, collection):
        return ()


def get_filelikes(dtype, value, filelikes=None):
    """Recursively recover all filelike elements from a composite dtype."""

    filelikes = set() if filelikes is None else filelikes

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
            # length i.e. list-like behaviour.
            if ... in args:
                args = tuple([a for a in args if a != ...])

            for dt, v in zip_longest(args, value, fillvalue=args[0]):
                filelikes = get_filelikes(dt, v, filelikes)

        elif origin is Union:

            for dt in args:

                try:
                    # Do not check collection member types. 
                    check_type(value, dt, collection_check_strategy=Skip())
                except TypeCheckError:
                    continue
                filelikes = get_filelikes(dt, value, filelikes)

        else:
            raise ValueError(f"Failed to traverse {dtype} dtype when looking for files.")

    else:
        if is_file_type(dtype):
            filelikes.add(value)

    return filelikes
