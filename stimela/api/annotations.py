"""Annotation markers for Stimela3 parameter metadata.

Uses PEP 593 ``Annotated`` to attach metadata to recipe and cab parameters:

- ``Info("description")`` — parameter documentation
- ``Out`` — marks a parameter as an output (default is input)
- ``Choices["a", "b"]`` — restricts parameter to enumerated values
- ``Param(cli_name="x")`` — advanced parameter configuration
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, get_args, get_origin, get_type_hints


class _OutputMarker:
    """Singleton marker for output parameters."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        return "Out"


Out = _OutputMarker()


@dataclass(frozen=True)
class Info:
    """Parameter documentation string.

    Equivalent to YAML's ``info:`` field.
    """

    description: str

    def __repr__(self):
        return f"Info({self.description!r})"


@dataclass(frozen=True)
class Param:
    """Advanced parameter configuration.

    Attributes:
        cli_name: Override the CLI flag name (replaces ``nom_de_guerre``).
        metavar: Override the CLI metavar.
        abbreviation: Short CLI flag (e.g. ``-n``).
    """

    cli_name: str | None = None
    metavar: str | None = None
    abbreviation: str | None = None


class _ChoicesMeta(type):
    def __getitem__(cls, items):
        if not isinstance(items, tuple):
            items = (items,)
        return ChoicesType(items)


class Choices(metaclass=_ChoicesMeta):
    """Type annotation for enumerated parameter values.

    Usage::

        band: Annotated[Choices["L", "UHF", "K"], Info("frequency band")] = "L"
    """


@dataclass(frozen=True)
class ChoicesType:
    """A choices constraint with specific allowed values."""

    values: tuple

    def __repr__(self):
        return f"Choices{list(self.values)}"


def extract_annotations(func) -> dict[str, dict[str, Any]]:
    """Extract Stimela annotations from a function's type hints.

    Returns a dict mapping parameter names to their metadata.
    """
    try:
        hints = get_type_hints(func, include_extras=True)
    except Exception:
        hints = {}

    result = {}
    for name, hint in hints.items():
        if name == "return":
            continue

        meta: dict[str, Any] = {
            "type": hint,
            "is_output": False,
            "info": None,
            "choices": None,
            "param": None,
        }

        from typing import Annotated

        if get_origin(hint) is Annotated:
            args = get_args(hint)
            meta["type"] = args[0]
            for annotation in args[1:]:
                if isinstance(annotation, _OutputMarker):
                    meta["is_output"] = True
                elif isinstance(annotation, Info):
                    meta["info"] = annotation.description
                elif isinstance(annotation, ChoicesType):
                    meta["choices"] = annotation.values
                elif isinstance(annotation, Param):
                    meta["param"] = annotation

        if isinstance(meta["type"], ChoicesType):
            meta["choices"] = meta["type"].values
            meta["type"] = str

        result[name] = meta

    return result
