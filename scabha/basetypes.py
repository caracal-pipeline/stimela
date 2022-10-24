from dataclasses import field, dataclass
from collections import OrderedDict
from typing import List


def EmptyDictDefault():
    return field(default_factory=lambda:OrderedDict())


def EmptyListDefault():
    return field(default_factory=lambda:[])
    

@dataclass
class Unresolved(object):
    value: str = ""
    errors: List[Exception] = EmptyListDefault

    def __post_init__(self):
        if not self.value:
            self.value = "; ".join(map(str, self.errors))

    def __str__(self):
        return f"Unresolved({self.value})"
