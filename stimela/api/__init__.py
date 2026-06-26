"""Stimela3 Python API.

This module provides the Python-native interface for defining and running
Stimela recipes. It is designed to coexist with the existing YAML recipe
system — both share the same cab registry, backend dispatch, and parameter
validation machinery.

Quick start::

    import stimela
    from typing import Annotated
    from stimela import Info, Out

    @stimela.recipe
    def my_pipeline(
        ms: Annotated[str, Info("input measurement set")],
        dir_out: Annotated[str, Out, Info("output directory")],
    ):
        from cultcargo.cabs import wsclean
        wsclean(ms=ms, size=4096)
"""

from stimela.api.annotations import Choices, Info, Out, Param
from stimela.api.cab_decorator import cab
from stimela.api.cab_proxy import CabProxy
from stimela.api.parallel import parallel
from stimela.api.recipe_decorator import recipe
from stimela.api.result import ResultNamespace, RunResult

__all__ = [
    "Choices",
    "CabProxy",
    "Info",
    "Out",
    "Param",
    "ResultNamespace",
    "RunResult",
    "cab",
    "parallel",
    "recipe",
]
