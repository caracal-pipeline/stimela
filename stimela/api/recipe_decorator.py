"""The @stimela.recipe decorator."""

from __future__ import annotations

import functools
import logging
from typing import Any, Callable

from stimela.api.execution import RecipeContext, pop_context, push_context


class RecipeWrapper:
    """Wrapper that provides Stimela integration around a recipe function."""

    def __init__(self, func: Callable, backend: str | None = None):
        self._func = func
        self._backend = backend
        self._name = func.__name__
        self.__doc__ = func.__doc__
        self.__module__ = func.__module__
        self.__qualname__ = func.__qualname__
        self.__wrapped__ = func
        functools.update_wrapper(self, func)

    def __call__(self, *args: Any, _backend: str | None = None, **kwargs: Any) -> Any:
        backend = _backend or self._backend
        log = logging.getLogger(f"stimela.{self._name}")
        log.info(f"running recipe '{self._name}'")

        ctx = RecipeContext(name=self._name, log=log, backend=backend)
        push_context(ctx)
        try:
            result = self._func(*args, **kwargs)
        finally:
            pop_context()

        log.info(f"recipe '{self._name}' completed")
        return result

    def __repr__(self):
        return f"<Recipe {self._name}>"


def recipe(func: Callable | None = None, *, backend: str | None = None):
    """Decorator to mark a function as a Stimela recipe.

    Can be used with or without arguments::

        @stimela.recipe
        def my_recipe(ms: str): ...

        @stimela.recipe(backend="singularity")
        def my_recipe(ms: str): ...
    """
    if func is not None:
        return RecipeWrapper(func, backend=backend)
    return lambda f: RecipeWrapper(f, backend=backend)
