from stimela import log_exception, stimelogging
from stimela.stimelogging import log_rich_payload
from stimela.exceptions import *


def keys_from_sel_string(dictionary: Dict[str, str], sel_string: str):
    """Select keys from a dictionary based on a slice string."""

    keys = list(dictionary.keys())    

    if ':' in sel_string:
        begin, end = sel_string.split(':', 1)
        if begin:
            try:
                first = keys.index(begin)
            except ValueError as exc:
                raise StepSelectionError(f"no such step: '{begin}' (in '{sel_string}')")
        else:
            first = 0
        if end:
            try:
                last = keys.index(end)
            except ValueError as exc:
                raise StepSelectionError(f"no such step: '{end}' (in '{sel_string}')")
        else:
            last = len(keys) - 1
        selected_keys = set(keys[first: last + 1])
    else:
        if sel_string not in keys:
            raise StepSelectionError(f"no such step: '{sel_string}'")
        selected_keys = set([sel_string])

    return selected_keys
