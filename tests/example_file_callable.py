"""Example standalone Python file for testing (path/file.py)function syntax.

This file is loaded directly via importlib.util, not as a package module.
"""

from typing import Any, Dict

from scabha.cargo import Parameter


def file_function(a: int, b: str):
    """Simple callable for testing python flavour with file path syntax."""
    print(f"file_function({a},'{b}')")
    return a * 3


def file_function_dict(a: int, b: str):
    """Callable that returns a dict of outputs."""
    print(f"file_function_dict({a},'{b}')")
    return dict(x=a * 3, y=b + b + b)


_extra_schema = dict(
    extra_param=Parameter(dtype="str", info="an extra parameter added dynamically"),
)


def file_dynamic_schema(params: Dict[str, Any], inputs: Dict[str, Parameter], outputs: Dict[str, Parameter]):
    """Dynamic schema function loaded from a file path."""
    inputs = inputs.copy()

    for tag in params.get("tags", []):
        for key, value in _extra_schema.items():
            inputs[f"{tag}.{key}"] = value

    return inputs, outputs
