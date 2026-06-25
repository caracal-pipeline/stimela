"""Callable cab proxy — wraps a cab definition and makes it callable."""

from __future__ import annotations

from typing import Any

from omegaconf import DictConfig

from stimela.api.result import RunResult


class CabProxy:
    """Callable wrapper around a Stimela cab definition.

    Instances are created by the cab registry (from YAML definitions) or
    by the ``@stimela.cab`` decorator (from Python definitions).

    Calling a CabProxy validates parameters, dispatches to the configured
    backend, and returns a ``RunResult`` with typed outputs::

        from cultcargo.cabs import wsclean
        result = wsclean(ms="obs.ms", size=4096)
        print(result.restored)
    """

    def __init__(self, cab_name: str, cab_config: DictConfig | dict | None = None):
        self._cab_name = cab_name
        self._cab_config = cab_config

    @property
    def name(self) -> str:
        return self._cab_name

    def __call__(
        self,
        *,
        _backend: str | None = None,
        _cache: str | None = None,
        _tags: list[str] | None = None,
        _check: bool = True,
        **params: Any,
    ) -> "RunResult":
        """Run this cab with the given parameters.

        Args:
            _backend: Override backend for this step.
            _cache: ``"exist"`` or ``"fresh"`` — skip if outputs are cached.
            _tags: Tags for selective step execution.
            _check: If True (default), raise on cab failure.
            **params: Cab parameters. ``None`` values are auto-skipped.

        Returns:
            RunResult with typed output attributes.
        """
        from stimela.api.execution import run_cab_proxy

        return run_cab_proxy(
            self,
            params=params,
            backend_override=_backend,
            cache=_cache,
            tags=_tags,
            check=_check,
        )

    def __repr__(self):
        return f"CabProxy({self._cab_name!r})"

    def __str__(self):
        return self._cab_name
