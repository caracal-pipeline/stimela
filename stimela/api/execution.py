"""Execution engine bridging the Python API to existing Stimela machinery.

This is the integration layer: CabProxy.__call__() → run_cab_proxy() →
existing Cab/Step/BackendRunner infrastructure. All parameter validation,
command construction, and backend dispatch is reused.
"""

from __future__ import annotations

import contextvars
import logging
import os
from collections import OrderedDict
from typing import Any

from omegaconf import DictConfig, OmegaConf
from scabha.basetypes import UNSET, Unresolved
from scabha.substitutions import SubstitutionNS

import stimela
from stimela import task_stats
from stimela.api.result import RunResult
from stimela.backends import StimelaBackendSchema, runner
from stimela.exceptions import BackendError, StepValidationError, StimelaCabRuntimeError
from stimela.kitchen.cab import Cab, get_cab_schema

# Thread-safe context stack using ContextVar so parallel() doesn't corrupt state
_recipe_context_var: contextvars.ContextVar[list[RecipeContext]] = contextvars.ContextVar(
    "recipe_context_stack", default=[]
)


class RecipeContext:
    """Tracks state for the currently executing recipe."""

    def __init__(self, name: str, log: logging.Logger, backend: str | None = None):
        self.name = name
        self.log = log
        self.backend = backend
        self.step_count = 0

    def next_step_name(self, cab_name: str) -> str:
        self.step_count += 1
        return f"{self.name}.{cab_name}-{self.step_count}"


def get_current_context() -> RecipeContext | None:
    stack = _recipe_context_var.get()
    return stack[-1] if stack else None


def push_context(ctx: RecipeContext):
    stack = _recipe_context_var.get()
    _recipe_context_var.set([*stack, ctx])


def pop_context() -> RecipeContext | None:
    stack = _recipe_context_var.get()
    if not stack:
        return None
    ctx = stack[-1]
    _recipe_context_var.set(stack[:-1])
    return ctx


def _ensure_config():
    if stimela.CONFIG is None:
        from stimela.config import load_config

        load_config(extra_configs=[])


def _filter_none_params(params: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in params.items() if v is not None}


def _instantiate_cab(cab_name: str, cab_config: DictConfig | dict | None = None) -> Cab:
    _ensure_config()

    if cab_config is not None:
        if isinstance(cab_config, dict):
            cab_config = OmegaConf.create(cab_config)
        cab_config = OmegaConf.unsafe_merge(get_cab_schema().copy(), cab_config)
        return Cab(**cab_config)

    config = stimela.CONFIG
    if cab_name not in config.cabs:
        raise StepValidationError(
            f"unknown cab '{cab_name}'. Ensure the cab package is installed and config is loaded."
        )
    return Cab(**config.cabs[cab_name])


def _resolve_backend(backend_override: str | None = None) -> DictConfig:
    _ensure_config()
    backend = OmegaConf.create({})

    ctx = get_current_context()
    if ctx and ctx.backend:
        backend = OmegaConf.merge(backend, {"select": ctx.backend})

    if backend_override:
        backend = OmegaConf.merge(backend, {"select": backend_override})

    return backend


def _check_cache(
    cab: Cab, params: dict, cache: str | None, fqname: str, log: logging.Logger
) -> tuple[bool, dict[str, Any]]:
    """Check if step can be skipped. Returns (should_skip, cached_outputs)."""
    if not cache:
        return False, {}

    if cache not in ("exist", "fresh"):
        log.warning(f"{fqname}: invalid _cache='{cache}', ignoring")
        return False, {}

    outputs = {}
    all_exist = True
    for name, schema in cab.outputs.items():
        value = params.get(name)
        if value is None:
            implicit = getattr(schema, "implicit", None)
            default = getattr(schema, "default", None)
            value = implicit or default
        if value and isinstance(value, str):
            if os.path.exists(value):
                outputs[name] = value
            else:
                all_exist = False
        else:
            outputs[name] = value

    if not all_exist:
        return False, {}

    if cache == "exist":
        log.info(f"{fqname}: outputs exist, skipping")
        return True, outputs

    if cache == "fresh":
        input_mtime = 0
        for name in cab.inputs:
            value = params.get(name)
            if value and isinstance(value, str) and os.path.exists(value):
                input_mtime = max(input_mtime, os.path.getmtime(value))

        if input_mtime == 0:
            log.info(f"{fqname}: outputs exist (no file inputs to compare), skipping")
            return True, outputs

        for name, value in outputs.items():
            if isinstance(value, str) and os.path.exists(value):
                if os.path.getmtime(value) < input_mtime:
                    return False, {}

        log.info(f"{fqname}: outputs are fresh, skipping")
        return True, outputs

    return False, {}


def run_cab_proxy(
    proxy: "stimela.api.cab_proxy.CabProxy",
    params: dict[str, Any],
    backend_override: str | None = None,
    cache: str | None = None,
    tags: list[str] | None = None,
    check: bool = True,
) -> RunResult:
    """Execute a cab via the Python API.

    This bridges CabProxy.__call__() to existing Stimela execution machinery.

    Note: ``tags`` is accepted for API completeness but tag-based step
    filtering is only meaningful when running via the CLI (``stimela exec
    --tags ...``). In direct Python calls, use ``if``/``else`` instead.
    """
    _ensure_config()

    cab_name = proxy.name
    ctx = get_current_context()
    fqname = ctx.next_step_name(cab_name) if ctx else cab_name
    log = ctx.log if ctx else logging.getLogger(f"stimela.{cab_name}")

    params = _filter_none_params(params)

    cab = _instantiate_cab(cab_name, proxy._cab_config)

    should_skip, cached_outputs = _check_cache(cab, params, cache, fqname, log)
    if should_skip:
        return RunResult(cached_outputs, cab_name)

    flat_params = cab.flatten_param_dict(OrderedDict(), params)

    backend = _resolve_backend(backend_override)
    backend = OmegaConf.merge(
        stimela.CONFIG.opts.backend,
        cab.backend or {},
        backend,
    )

    subst = SubstitutionNS()
    subst.config = stimela.CONFIG
    subst.recipe = SubstitutionNS()
    subst.current = SubstitutionNS(**flat_params)

    try:
        backend_opts = OmegaConf.merge(StimelaBackendSchema, backend)
        backend_opts = OmegaConf.to_object(backend_opts)
        backend_runner = runner.validate_backend_settings(backend_opts, log, cab=cab)
    except Exception as exc:
        raise BackendError(f"error setting up backend for {cab_name}", exc) from None

    validated_params = cab.validate_inputs(flat_params, subst=subst)

    log.info(f"running {cab_name}")

    # backend_opts is a StimelaBackendOptions dataclass after to_object()
    wrapper_or_backend = getattr(backend_opts, "current_wrapper", None) or getattr(
        backend_opts, "current_backend", "native"
    )

    with task_stats.declare_subtask(cab_name, wrapper_or_backend):
        try:
            backend_runner.run(
                cab,
                validated_params,
                fqname=fqname,
                log=log,
                subst=subst,
            )
        except Exception as exc:
            if check:
                raise StimelaCabRuntimeError(f"{cab_name} failed", exc) from None
            result = RunResult({}, cab_name)
            result._success = False
            return result

    try:
        output_params = cab.validate_outputs(validated_params, subst=subst)
    except Exception:
        output_params = validated_params

    # Filter out both UNSET sentinel and Unresolved instances
    outputs = {}
    for name in cab.outputs:
        if name in output_params:
            value = output_params[name]
            if value is not UNSET and not isinstance(value, Unresolved):
                outputs[name] = value

    log.info(f"{cab_name} completed")
    return RunResult(outputs, cab_name)
