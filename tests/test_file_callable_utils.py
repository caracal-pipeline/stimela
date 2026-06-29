"""Unit tests for the (path/file.py)function utility functions."""

import os

import pytest

from stimela.kitchen.utils import (
    load_callable_from_file,
    parse_file_callable,
    resolve_callable,
    resolve_file_path,
)


class TestParseFileCallable:
    def test_basic(self):
        assert parse_file_callable("(foo.py)bar") == ("foo.py", "bar")

    def test_with_path(self):
        assert parse_file_callable("(path/to/foo.py)bar") == ("path/to/foo.py", "bar")

    def test_with_slash_separator(self):
        assert parse_file_callable("(path/to/foo.py)/bar") == ("path/to/foo.py", "bar")

    def test_relative_dot_path(self):
        assert parse_file_callable("(./foo.py)bar") == ("./foo.py", "bar")

    def test_relative_dotdot_path(self):
        assert parse_file_callable("(../foo.py)bar") == ("../foo.py", "bar")

    def test_not_matching_module_syntax(self):
        assert parse_file_callable("module.function") is None

    def test_not_matching_no_parens(self):
        assert parse_file_callable("function") is None

    def test_not_matching_no_py_extension(self):
        assert parse_file_callable("(foo.txt)bar") is None


class TestResolveFilePath:
    def test_absolute_path(self):
        result = resolve_file_path("/absolute/path/foo.py")
        assert result == "/absolute/path/foo.py"

    def test_relative_path_no_yaml_dir(self):
        result = resolve_file_path("foo.py")
        assert result == os.path.abspath("foo.py")

    def test_dot_relative_with_yaml_dir(self):
        result = resolve_file_path("./foo.py", "/some/yaml/dir")
        assert result == "/some/yaml/dir/foo.py"

    def test_dotdot_relative_with_yaml_dir(self):
        result = resolve_file_path("../foo.py", "/some/yaml/dir")
        assert result == "/some/yaml/foo.py"

    def test_non_dot_relative_ignores_yaml_dir(self):
        result = resolve_file_path("foo.py", "/some/yaml/dir")
        assert result == os.path.abspath("foo.py")


class TestLoadCallableFromFile:
    def test_load_function(self, tmp_path):
        # Create a temporary Python file
        py_file = tmp_path / "test_mod.py"
        py_file.write_text("def my_func(x):\n    return x * 2\n")

        func = load_callable_from_file(str(py_file), "my_func")
        assert callable(func)
        assert func(5) == 10

    def test_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            load_callable_from_file("/nonexistent/path.py", "func")

    def test_function_not_found(self, tmp_path):
        py_file = tmp_path / "test_mod.py"
        py_file.write_text("def other_func():\n    pass\n")

        with pytest.raises(AttributeError):
            load_callable_from_file(str(py_file), "missing_func")

    def test_not_callable(self, tmp_path):
        py_file = tmp_path / "test_mod.py"
        py_file.write_text("NOT_A_FUNC = 42\n")

        with pytest.raises(TypeError):
            load_callable_from_file(str(py_file), "NOT_A_FUNC")


class TestResolveCallable:
    def test_file_syntax(self, tmp_path):
        py_file = tmp_path / "resolver_test.py"
        py_file.write_text("def greet(name):\n    return f'hello {name}'\n")

        func = resolve_callable(f"({py_file})greet")
        assert func("world") == "hello world"

    def test_module_syntax(self):
        func = resolve_callable("os.path.join")
        assert func("a", "b") == os.path.join("a", "b")

    def test_invalid_syntax(self):
        with pytest.raises(ImportError):
            resolve_callable("nodotshere")
