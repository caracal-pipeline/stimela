from itertools import chain
from benedict import benedict
from collections import namedtuple

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


class FlowRestrictor(object):

    Restrictions = namedtuple(
        "Restrictions",
        [
            "tags",
            "skip_tags",
            "step_ranges",
            "skip_ranges",
            "enable_steps"
        ]
    )

    def __init__(
        self,
        tags: List[str] = [],
        skip_tags: List[str] = [],
        step_ranges: List[str] = [],
        skip_ranges: List[str] = [],
        enable_steps: List[str] = []
    ):

        def process_commas(opts: List[str]):
            return set(chain(*(opt.split(",") for opt in opts)))

        def benedictify(opts: List[str]):
            return benedict.fromkeys(sorted(opts))

        self.tags = benedictify(process_commas(tags))
        self.skip_tags = benedictify(process_commas(skip_tags))
        self.step_ranges = benedictify(process_commas(step_ranges))
        self.skip_ranges = benedictify(process_commas(skip_ranges))
        self.enable_steps = benedictify(process_commas(enable_steps))

    def get_restriction(self, fqname: str, field: str):
        """Given fqname, return the applicable restrictions from field."""

        current_restrictions = getattr(self, field)

        # This implies that we are not at the outermost level. The
        # components of fqname after the first period will match the keys in
        # current_restrictions.
        if "." in fqname:
            key = fqname.split(".", 1)[1]
            # A missing key implies no restrictions.
            current_restrictions = current_restrictions.get(key, {})

        # NOTE(JSKenyon): We do not check for validity at this level as we may
        # have unusual keys in the dictionary.
        return list(current_restrictions.keys())

    def get_restrictions(self, fqname: str):
        """Given fqname, return applicable restrictions from all fields."""
        return self.Restrictions(*[self.get_restriction(fqname, f) for f in self.Restrictions._fields])
