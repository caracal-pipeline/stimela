"""The @stimela.cab decorator for defining cabs in Python."""

from __future__ import annotations

import inspect
from typing import Any, Callable

from stimela.api.annotations import extract_annotations
from stimela.api.cab_proxy import CabProxy


def cab(
    func: Callable | None = None,
    *,
    command: str | None = None,
    image: str | None = None,
    flavour: str | None = None,
    wranglers: dict[str, Any] | None = None,
):
    """Decorator to define a cab from a Python function.

    Parameters annotated with ``Out`` are outputs; all others are inputs::

        @stimela.cab(command="wsclean")
        def wsclean(
            ms: Annotated[stimela.MS, Info("input MS")],
            restored: Annotated[stimela.File, Out, Info("restored image")] = None,
        ):
            ...

    Returns:
        CabProxy: A callable cab object.
    """

    def decorator(f: Callable) -> CabProxy:
        annotations = extract_annotations(f)
        sig = inspect.signature(f)

        cab_name = f.__name__
        cmd = command or f"{f.__module__}.{f.__qualname__}"

        inputs: dict[str, Any] = {}
        outputs: dict[str, Any] = {}

        for param_name, param in sig.parameters.items():
            ann = annotations.get(param_name, {})
            is_output = ann.get("is_output", False)
            info = ann.get("info", "")
            choices = ann.get("choices")
            param_type = ann.get("type", Any)

            schema: dict[str, Any] = {}
            if info:
                schema["info"] = info

            dtype = _python_type_to_dtype(param_type)
            if dtype:
                schema["dtype"] = dtype

            if choices:
                schema["choices"] = list(choices)

            if param.default is not inspect.Parameter.empty:
                schema["default"] = param.default

            if is_output:
                outputs[param_name] = schema
            else:
                if param.default is inspect.Parameter.empty:
                    schema["required"] = True
                inputs[param_name] = schema

        cab_config = {
            "name": cab_name,
            "command": cmd,
            "inputs": inputs,
            "outputs": outputs,
        }

        if image:
            cab_config["image"] = image
        if flavour:
            cab_config["flavour"] = flavour
        if wranglers:
            cab_config["management"] = {"wranglers": wranglers}

        proxy = CabProxy(cab_name, cab_config=cab_config)
        proxy.__doc__ = f.__doc__
        proxy.__module__ = f.__module__
        proxy.__qualname__ = f.__qualname__
        proxy.__wrapped__ = f

        return proxy

    if func is not None:
        return decorator(func)
    return decorator


def _python_type_to_dtype(t: Any) -> str | None:
    """Convert a Python type annotation to a stimela dtype string."""

    type_map = {
        str: "str",
        int: "int",
        float: "float",
        bool: "bool",
    }

    if t in type_map:
        return type_map[t]

    try:
        from scabha.basetypes import MS, URI, Directory, File

        scabha_map = {File: "File", Directory: "Directory", URI: "URI", MS: "MS"}
        if t in scabha_map:
            return scabha_map[t]
    except ImportError:
        pass

    origin = getattr(t, "__origin__", None)
    if origin is list:
        args = getattr(t, "__args__", ())
        if args:
            inner = _python_type_to_dtype(args[0])
            if inner:
                return f"List[{inner}]"
        return "List[str]"

    return None
