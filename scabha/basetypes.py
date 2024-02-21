from dataclasses import field, dataclass
from collections import OrderedDict
from typing import List
import os.path
import re
from .exceptions import UnsetError

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




