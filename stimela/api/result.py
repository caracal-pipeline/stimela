"""Typed result objects returned by cab and recipe calls."""

from __future__ import annotations

from typing import Any


class RunResult:
    """Result of a cab execution.

    Outputs are accessible as attributes::

        result = wsclean(ms="obs.ms", size=4096)
        result.restored   # path to restored image

    YAML cab definitions use hyphens in output names (e.g. ``out-image``).
    These are accessible via underscores (``result.out_image``).
    """

    def __init__(self, outputs: dict[str, Any], cab_name: str = ""):
        self._outputs = outputs
        self._cab_name = cab_name
        self._success = True

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        outputs = self.__dict__.get("_outputs", {})
        if name in outputs:
            return outputs[name]
        hyphenated = name.replace("_", "-")
        if hyphenated in outputs:
            return outputs[hyphenated]
        cab = self.__dict__.get("_cab_name", "unknown")
        available = ", ".join(outputs.keys()) if outputs else "none"
        raise AttributeError(f"'{cab}' has no output '{name}'. Available outputs: {available}")

    def __repr__(self):
        outputs = ", ".join(f"{k}={v!r}" for k, v in self._outputs.items())
        return f"RunResult({self._cab_name}, {outputs})"

    def __contains__(self, name: str) -> bool:
        return name in self._outputs or name.replace("_", "-") in self._outputs

    @property
    def success(self) -> bool:
        return self._success


class ResultNamespace:
    """Namespace for returning multiple named values from a sub-recipe.

    Usage::

        return stimela.ResultNamespace(
            detection_catalogs=catalogs,
            dp_catalogs=dp_cats,
        )
    """

    def __init__(self, **kwargs: Any):
        self.__dict__.update(kwargs)

    def __repr__(self):
        items = ", ".join(f"{k}={v!r}" for k, v in self.__dict__.items())
        return f"ResultNamespace({items})"
