"""Utility functions for the kitchen module."""

import importlib
import importlib.util
import os
import re
from typing import Callable, Optional, Tuple

# Regex to match (path/to/file.py)function or (path/to/file.py)/function
_FILE_CALLABLE_RE = re.compile(r"^\((.+\.py)\)/?(\w+)$")


def parse_file_callable(spec: str) -> Optional[Tuple[str, str]]:
    """Parse a callable reference in (path/to/file.py)function syntax.

    Returns (file_path, function_name) if the spec matches the pattern,
    or None if it doesn't match.
    """
    match = _FILE_CALLABLE_RE.match(spec)
    if match:
        return match.group(1), match.group(2)
    return None


def resolve_file_path(file_path: str, yaml_dir: Optional[str] = None) -> str:
    """Resolve a file path, handling ./ prefix as relative to yaml_dir.

    A path starting with ./ is interpreted relative to the directory of the
    YAML file where it appears. Other paths are interpreted relative to CWD.

    Args:
        file_path: The path from the callable spec.
        yaml_dir: The directory of the YAML file, or None.

    Returns:
        Absolute path to the file.
    """
    if file_path.startswith("./") or file_path.startswith("../"):
        if yaml_dir:
            file_path = os.path.join(yaml_dir, file_path)
    return os.path.abspath(file_path)


def load_callable_from_file(file_path: str, func_name: str) -> Callable:
    """Load a callable from a Python file using importlib.

    Args:
        file_path: Absolute path to the Python file.
        func_name: Name of the function to load.

    Returns:
        The callable object.

    Raises:
        FileNotFoundError: If the file doesn't exist.
        ImportError: If the module can't be loaded.
        AttributeError: If the function doesn't exist in the module.
        TypeError: If the attribute is not callable.
    """
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"Python file not found: {file_path}")

    module_name = os.path.splitext(os.path.basename(file_path))[0]
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"can't create module spec from {file_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    func = getattr(module, func_name, None)
    if func is None:
        raise AttributeError(f"{func_name} not found in {file_path}")
    if not callable(func):
        raise TypeError(f"{func_name} in {file_path} is not callable")

    return func


def resolve_callable(spec: str, yaml_dir: Optional[str] = None) -> Callable:
    """Resolve a callable from either (path/file.py)function or module.function syntax.

    Args:
        spec: Callable specification string.
        yaml_dir: Directory of the YAML file (for resolving ./ paths).

    Returns:
        The resolved callable.

    Raises:
        Various exceptions if the callable can't be resolved.
    """
    parsed = parse_file_callable(spec)
    if parsed:
        file_path, func_name = parsed
        file_path = resolve_file_path(file_path, yaml_dir)
        return load_callable_from_file(file_path, func_name)
    else:
        # Fall back to module.function syntax
        if "." not in spec:
            raise ImportError(f"{spec}: module_name.function_name or (file.py)function expected")
        module_name, func_name = spec.rsplit(".", 1)
        mod = importlib.import_module(module_name)
        func = getattr(mod, func_name, None)
        if func is None:
            raise AttributeError(f"{func_name} not found in module {module_name}")
        if not callable(func):
            raise TypeError(f"{module_name}.{func_name} is not callable")
        return func
