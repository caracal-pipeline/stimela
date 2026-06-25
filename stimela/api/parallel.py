"""Parallel execution context manager.

Replaces YAML's ``scatter`` keyword with explicit parallel execution::

    with stimela.parallel() as pool:
        for ms in ms_list:
            pool.run(wsclean, ms=ms, size=4096)
"""

from __future__ import annotations

import logging
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from stimela.api.cab_proxy import CabProxy


class ParallelContext:
    """Context manager for parallel step execution.

    Steps launched via ``run()`` or ``call()`` are queued and executed
    concurrently. Results are collected when the context exits.

    Args:
        max_workers: Maximum number of parallel workers. Defaults to
            the number of CPUs.
        use_threads: If True, use threads instead of processes.
            Useful when steps are I/O-bound or delegate to external
            processes (which is the common case for Stimela cabs).
    """

    def __init__(self, max_workers: int | None = None, use_threads: bool = True):
        self._max_workers = max_workers
        self._use_threads = use_threads
        self._futures: list[tuple[str, Future]] = []
        self._results: list[Any] = []
        self._executor = None

    def __enter__(self):
        if self._use_threads:
            self._executor = ThreadPoolExecutor(max_workers=self._max_workers)
        else:
            self._executor = ProcessPoolExecutor(max_workers=self._max_workers)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self._executor.shutdown(wait=False, cancel_futures=True)
            return False

        self._results = []
        for label, future in self._futures:
            try:
                self._results.append(future.result())
            except Exception as exc:
                log = logging.getLogger("stimela.parallel")
                log.error(f"parallel step '{label}' failed: {exc}")
                raise

        self._executor.shutdown(wait=True)
        return False

    def run(self, cab_proxy: "CabProxy", **params: Any) -> None:
        """Queue a cab for parallel execution.

        Args:
            cab_proxy: The cab to run.
            **params: Parameters for the cab (same as calling the cab directly).
        """
        if self._executor is None:
            raise RuntimeError("parallel() must be used as a context manager")

        label = str(cab_proxy)
        future = self._executor.submit(cab_proxy, **params)
        self._futures.append((label, future))

    def call(self, recipe_func: Callable, *args: Any, **kwargs: Any) -> None:
        """Queue a sub-recipe for parallel execution.

        Args:
            recipe_func: The recipe function to call.
            *args, **kwargs: Arguments for the recipe.
        """
        if self._executor is None:
            raise RuntimeError("parallel() must be used as a context manager")

        label = getattr(recipe_func, "__name__", str(recipe_func))
        future = self._executor.submit(recipe_func, *args, **kwargs)
        self._futures.append((label, future))

    @property
    def results(self) -> list[Any]:
        """Results from all parallel steps, in submission order."""
        return self._results


def parallel(max_workers: int | None = None, use_threads: bool = True) -> ParallelContext:
    """Create a parallel execution context.

    Usage::

        with stimela.parallel() as pool:
            for ms in ms_list:
                pool.run(wsclean, ms=ms, size=4096)
        # all steps complete here
    """
    return ParallelContext(max_workers=max_workers, use_threads=use_threads)
