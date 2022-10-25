from dataclasses import field, dataclass
from collections import OrderedDict
from typing import List
from .exceptions import UnsetError

def EmptyDictDefault():
    return field(default_factory=lambda:OrderedDict())


def EmptyListDefault():
    return field(default_factory=lambda:[])
    

@dataclass
class Unresolved(object):
    value: str = ""
    errors: List[Exception] = EmptyListDefault()

    def __post_init__(self):
        if not self.value:
            self.value = "; ".join(map(str, self.errors))
        if not self.errors:
            self.errors = [UnsetError(f"'{self.value}' undefined")]

    def __str__(self):
        return f"Unresolved({self.value})"

class UNSET(Unresolved):
    """Marks unset values in formulas"""
    pass


import os.path

class File(str):

    @property
    def NAME(self):
        return File(os.path.basename(self))

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


class Directory(File):
    pass

class MS(Directory):
    pass




