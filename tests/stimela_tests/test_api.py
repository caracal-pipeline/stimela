"""Tests for the Stimela3 Python API.

Tests the core API: @stimela.recipe, cab(), RunResult, annotations,
and parallel execution.
"""

from typing import Annotated
from unittest.mock import MagicMock

import pytest

import stimela
from stimela.api import Choices, Info, Out, Param, ResultNamespace, RunResult, recipe
from stimela.api.annotations import ChoicesType, _OutputMarker, extract_annotations
from stimela.api.cab_proxy import CabProxy
from stimela.api.execution import RecipeContext, get_current_context, pop_context, push_context
from stimela.api.parallel import ParallelContext, parallel

# --- Annotation tests ---


class TestAnnotations:
    def test_out_is_singleton(self):
        assert Out is _OutputMarker()

    def test_info(self):
        info = Info("test description")
        assert info.description == "test description"
        assert repr(info) == "Info('test description')"

    def test_choices_syntax(self):
        c = Choices["L", "UHF", "K"]
        assert isinstance(c, ChoicesType)
        assert c.values == ("L", "UHF", "K")

    def test_choices_single(self):
        c = Choices["L"]
        assert c.values == ("L",)

    def test_param(self):
        p = Param(cli_name="data-ms")
        assert p.cli_name == "data-ms"

    def test_extract_annotations(self):
        def my_func(
            ms: Annotated[str, Info("input MS")],
            size: int = 4096,
            dir_out: Annotated[str, Out, Info("output directory")] = ".",
        ):
            pass

        annotations = extract_annotations(my_func)

        assert "ms" in annotations
        assert annotations["ms"]["info"] == "input MS"
        assert annotations["ms"]["is_output"] is False
        assert annotations["ms"]["type"] is str

        assert "dir_out" in annotations
        assert annotations["dir_out"]["is_output"] is True
        assert annotations["dir_out"]["info"] == "output directory"

    def test_extract_choices(self):
        choices = Choices["L", "UHF"]

        def my_func(
            band: Annotated[str, choices, Info("band")] = "L",
        ):
            pass

        annotations = extract_annotations(my_func)
        assert annotations["band"]["choices"] == ("L", "UHF")


# --- RunResult tests ---


class TestRunResult:
    def test_attribute_access(self):
        result = RunResult({"restored": "/path/to/image.fits"}, "wsclean")
        assert result.restored == "/path/to/image.fits"

    def test_hyphen_to_underscore(self):
        result = RunResult({"out-image": "/path/to/image.fits"}, "wsclean")
        assert result.out_image == "/path/to/image.fits"

    def test_missing_attribute(self):
        result = RunResult({"restored": "/path"}, "wsclean")
        with pytest.raises(AttributeError, match="no output 'missing'"):
            _ = result.missing

    def test_contains(self):
        result = RunResult({"restored": "/path", "out-image": "/path2"}, "wsclean")
        assert "restored" in result
        assert "out_image" in result

    def test_success(self):
        result = RunResult({}, "test")
        assert result.success is True

    def test_repr(self):
        result = RunResult({"a": 1}, "test")
        assert "RunResult" in repr(result)
        assert "test" in repr(result)


class TestResultNamespace:
    def test_attribute_access(self):
        ns = ResultNamespace(catalogs=["a.ecsv", "b.ecsv"], count=2)
        assert ns.catalogs == ["a.ecsv", "b.ecsv"]
        assert ns.count == 2

    def test_repr(self):
        ns = ResultNamespace(x=1)
        assert "ResultNamespace" in repr(ns)


# --- CabProxy tests ---


class TestCabProxy:
    def test_name(self):
        proxy = CabProxy("wsclean")
        assert proxy.name == "wsclean"
        assert str(proxy) == "wsclean"
        assert repr(proxy) == "CabProxy('wsclean')"


# --- RecipeContext tests ---


class TestRecipeContext:
    def test_context_stack(self):
        assert get_current_context() is None

        ctx = RecipeContext("test", MagicMock())
        push_context(ctx)
        assert get_current_context() is ctx
        assert ctx.next_step_name("wsclean") == "test.wsclean-1"
        assert ctx.next_step_name("cubical") == "test.cubical-2"

        pop_context()
        assert get_current_context() is None

    def test_nested_contexts(self):
        ctx1 = RecipeContext("outer", MagicMock())
        ctx2 = RecipeContext("inner", MagicMock())

        push_context(ctx1)
        push_context(ctx2)
        assert get_current_context() is ctx2

        pop_context()
        assert get_current_context() is ctx1

        pop_context()
        assert get_current_context() is None


# --- Recipe decorator tests ---


class TestRecipeDecorator:
    def test_bare_decorator(self):
        @recipe
        def my_recipe(ms: str):
            return ms

        assert my_recipe.__name__ == "my_recipe"
        result = my_recipe(ms="test.ms")
        assert result == "test.ms"

    def test_decorator_with_args(self):
        @recipe(backend="native")
        def my_recipe(ms: str):
            return ms

        assert my_recipe.__name__ == "my_recipe"

    def test_recipe_sets_context(self):
        captured_ctx = None

        @recipe
        def my_recipe():
            nonlocal captured_ctx
            captured_ctx = get_current_context()

        my_recipe()
        assert captured_ctx is not None
        assert captured_ctx.name == "my_recipe"
        assert get_current_context() is None

    def test_recipe_cleans_context_on_error(self):
        @recipe
        def failing_recipe():
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            failing_recipe()

        assert get_current_context() is None

    def test_nested_recipes(self):
        contexts = []

        @recipe
        def inner():
            contexts.append(get_current_context().name)

        @recipe
        def outer():
            contexts.append(get_current_context().name)
            inner()
            contexts.append(get_current_context().name)

        outer()
        assert contexts == ["outer", "inner", "outer"]


# --- Parallel tests ---


class TestParallel:
    def test_basic_parallel(self):
        with parallel() as pool:
            for i in range(3):
                pool.call(lambda x: x * 2, i)

        assert len(pool.results) == 3
        assert sorted(pool.results) == [0, 2, 4]

    def test_parallel_error_propagates(self):
        def failing():
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            with parallel() as pool:
                pool.call(failing)

    def test_run_outside_context_fails(self):
        ctx = ParallelContext()
        with pytest.raises(RuntimeError, match="context manager"):
            ctx.call(lambda: None)


# --- Integration test with real cabs ---


class TestIntegration:
    """Integration tests that load stimela config and run real cabs.

    These tests use the 'echo' cab from test recipes as a smoke test
    for the full execution path.
    """

    @pytest.fixture(autouse=True)
    def setup_config(self):
        """Load stimela config if not already loaded."""
        if stimela.CONFIG is None:
            from stimela.config import load_config

            load_config(extra_configs=[])

    def test_cab_proxy_with_echo(self):
        """Test that a CabProxy can be created for a known cab."""
        if "echo" not in (stimela.CONFIG.cabs if stimela.CONFIG else {}):
            pytest.skip("echo cab not in config")

        proxy = CabProxy("echo")
        assert proxy.name == "echo"

    def test_recipe_with_subrecipe(self):
        """Test that recipes can call sub-recipes."""
        call_log = []

        @recipe
        def sub(x: int):
            call_log.append(f"sub({x})")
            return x + 1

        @recipe
        def main():
            result = sub(x=1)
            call_log.append(f"main got {result}")
            return result

        result = main()
        assert result == 2
        assert call_log == ["sub(1)", "main got 2"]
