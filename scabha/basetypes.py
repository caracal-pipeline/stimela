from dataclasses import field, dataclass
from collections import OrderedDict
from typing import List
from .exceptions import UnsetError
import os.path

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

class File(str):

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




