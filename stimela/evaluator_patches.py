"""Monkey-patches for scabha's expression evaluator.

This module extends scabha's evaluator with additional functionality needed by stimela:

- SLICE(list, stop) / SLICE(list, start, stop) / SLICE(list, start, stop, step):
  Function-based list slicing (issue #449). Use UNSET for omitted components.

Examples:
    =SLICE(recipe.mylist, 5)              # mylist[:5]  (first 5 elements)
    =SLICE(recipe.mylist, 2, 5)           # mylist[2:5]
    =SLICE(recipe.mylist, UNSET, 5)       # mylist[:5]
    =SLICE(recipe.mylist, 0, 10, 2)       # mylist[0:10:2]
"""

import logging

from scabha.evaluator import FunctionHandler

log = logging.getLogger(__name__)


def _SLICE(self, evaluator, args):
    """SLICE(list, stop) or SLICE(list, start, stop) or SLICE(list, start, stop, step).

    Provides list slicing functionality equivalent to Python's slice syntax.
    Use UNSET for omitted start/stop/step values.
    """
    from scabha.basetypes import UNSET, Unresolved
    from scabha.exceptions import FormulaError

    if len(args) < 2 or len(args) > 4:
        raise FormulaError(f"{'.'.join(evaluator.location)}: SLICE() expects 2 to 4 arguments, got {len(args)}")

    list_arg = evaluator._evaluate_result(args[0])
    if isinstance(list_arg, Unresolved):
        return list_arg

    eval_args = []
    for arg in args[1:]:
        val = evaluator._evaluate_result(arg, allow_unset=True)
        if val is UNSET or isinstance(val, Unresolved):
            # Treat UNSET as None (omitted slice component)
            eval_args.append(None)
        else:
            eval_args.append(val)

    if len(eval_args) == 1:
        # SLICE(list, stop) => list[:stop]
        return list_arg[: eval_args[0]]
    elif len(eval_args) == 2:
        # SLICE(list, start, stop) => list[start:stop]
        return list_arg[eval_args[0] : eval_args[1]]
    else:
        # SLICE(list, start, stop, step) => list[start:stop:step]
        return list_arg[eval_args[0] : eval_args[1] : eval_args[2]]


# Attach SLICE to FunctionHandler so it's available in formulas
FunctionHandler.SLICE = _SLICE


def apply_patches():
    """Apply all evaluator patches. Called at stimela import time."""
    import scabha.evaluator

    # Clear the parse cache since we added a new function
    scabha.evaluator._parse_cache.clear()

    # Reconstruct the parser to pick up the new SLICE function
    scabha.evaluator._parser = scabha.evaluator.construct_parser()

    log.debug("evaluator patches applied (SLICE function available)")
