"""Central deprecation warning mechanism for Stimela.

This module provides a way to issue and collect deprecation warnings throughout
a Stimela run, printing each unique warning only on first occurrence, and
providing a summary of all warnings at the end of the run.

Addresses https://github.com/caracal-pipeline/stimela/issues/371
"""

import logging
import warnings
from collections import OrderedDict
from typing import Optional

import rich.markup

from . import stimelogging

# Registry of all deprecation warnings issued during this run.
# Keyed by (category_label, message) to suppress duplicates.
_deprecation_registry: OrderedDict[tuple, dict] = OrderedDict()


def deprecation_warning(
    message: str,
    category: str = "general",
    log: Optional[logging.Logger] = None,
):
    """Issue a deprecation warning.

    The warning is logged immediately on first occurrence and collected for
    a summary at the end of the run.  Subsequent calls with the same
    *category* and *message* are silently suppressed.

    Args:
        message: Human-readable deprecation message.
        category: Short label grouping related warnings (e.g. "cab",
            "nom_de_guerre").  Used for deduplication and the summary.
        log: Logger to emit the warning to.  Falls back to the global
            Stimela logger when ``None``.
    """
    key = (category, message)
    if key in _deprecation_registry:
        _deprecation_registry[key]["count"] += 1
        return

    _deprecation_registry[key] = {"count": 1}

    # Also issue a Python-level FutureWarning so that programmatic callers
    # (e.g. test suites) can catch it with the warnings module.
    warnings.warn(message, FutureWarning, stacklevel=2)

    # Log the warning immediately. The end-of-run summary is handled
    # separately by get_deprecation_summary() in run.py, so we do not
    # use at_end=True here to avoid double-printing.
    if log is None:
        from . import logger

        log = logger()

    styled_msg = f"[bold yellow]Deprecation warning[/bold yellow]: {rich.markup.escape(message)}"

    stimelogging.log_and_remember(
        log,
        styled_msg,
        label=key,
        severity="warning",
        suppress_repeat=True,
        at_end=False,
    )


def has_deprecation_warnings() -> bool:
    """Return True if any deprecation warnings have been issued."""
    return bool(_deprecation_registry)


def get_deprecation_summary() -> list[str]:
    """Return a list of summary lines for all collected deprecation warnings."""
    lines = []
    for (category, message), info in _deprecation_registry.items():
        escaped_msg = rich.markup.escape(message)
        count = info["count"]
        if count > 1:
            lines.append(f"  \\[{category}] {escaped_msg} (x{count})")
        else:
            lines.append(f"  \\[{category}] {escaped_msg}")
    return lines


def clear_deprecation_warnings():
    """Clear all collected deprecation warnings. Mainly useful for testing."""
    _deprecation_registry.clear()
