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
            "always_tags",
            "never_tags",
            "step_ranges",
            "skip_ranges",
            "enable_steps"
        ]
    )

    def __init__(
        self,
        tags: List[str] = [],
        skip_tags: List[str] = [],
        always_tags: List[str] = [],
        never_tags: List[str] = [],
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
        self.always_tags = benedictify(process_commas(always_tags))
        self.never_tags = benedictify(process_commas(never_tags))
        self.step_ranges = benedictify(process_commas(step_ranges))
        self.skip_ranges = benedictify(process_commas(skip_ranges))
        self.enable_steps = benedictify(process_commas(enable_steps))

        # This status implies that all steps barring those which are explicitly
        # selected via a tag or a step range should be disabled. Needed for the
        # case where recipes are nested.
        self.has_selections = any([self.step_ranges, self.tags])

    def get_active_tags(
        self,
        fqname: str,
        field: str
    ):
        """Given fqname, return the applicable tags from field."""

        if "tags" not in field:
            raise ValueError(f"Cannot get tags from field {field}.")

        current_restrictions = getattr(self, field)
        # This implies that we are not at the outermost level. The
        # components of fqname after the first period will match the keys in
        # current_restrictions.
        if "." in fqname:
            key = fqname.split(".", 1)[1]
            # A missing key implies no restrictions.
            current_restrictions = current_restrictions.get(key, {})

        tags = [k for k, v in current_restrictions.items() if v is None]

        return set(tags)

    def get_active_steps(
        self,
        fqname: str,
        field: str,
        tag_field: bool = False
    ):
        """Given fqname, return the applicable restrictions from field."""

        current_restrictions = getattr(self, field)
        # This implies that we are not at the outermost level. The
        # components of fqname after the first period will match the keys in
        # current_restrictions.
        if "." in fqname:
            key = fqname.split(".", 1)[1]
            # A missing key implies no restrictions.
            current_restrictions = current_restrictions.get(key, {})

        if tag_field:
            steps = [k for k, v in current_restrictions.items() if v is not None]
        else:
            steps = list(current_restrictions.keys())

        # NOTE(JSKenyon): We do not check for validity at this level as we may
        # have unusual keys in the dictionary.
        return set(steps)

    def get_restrictions(self, fqname: str):
        """Given fqname, return applicable restrictions from all fields."""
        active_tags = self.get_active_tags(fqname, "tags")
        implied_steps = self.get_active_steps(fqname, "tags", tag_field=True)
        active_skip_tags = self.get_active_tags(fqname, "skip_tags")
        active_always_tags = self.get_active_tags(fqname, "always_tags")
        implied_steps |= self.get_active_steps(fqname, "always_tags", tag_field=True)
        active_never_tags = self.get_active_tags(fqname, "never_tags")

        active_step_ranges = self.get_active_steps(fqname, "step_ranges")
        active_skip_ranges = self.get_active_steps(fqname, "skip_ranges")
        active_enable_steps = self.get_active_steps(fqname, "enable_steps")

        return self.Restrictions(
            tags=active_tags,
            skip_tags=active_skip_tags,
            always_tags=active_always_tags,
            never_tags=active_never_tags,
            step_ranges=active_step_ranges.union(implied_steps),
            skip_ranges=active_skip_ranges,
            enable_steps=active_enable_steps
        )


class TagManager(object):

    def __init__(self, recipe, keep_outermost=True):
        """
        Given a recipe object, recurse through all subrecipes to construct a
        tag manager. The purpose of the manager is to provide a global
        representation of the tags in a given recipe (and its subrecipes),
        so that it is easier to reason about which steps are implied by the
        tags.
        """
        self.tag_graph = self.build_tag_graph(recipe)
        self.tag_graph.keypath_separator = ">>"
        self.tag_graph = self.tag_graph if keep_outermost else self.tag_graph[recipe.fqname]
        self.flat_tag_graph = self.tag_graph.flatten('.')

    @staticmethod
    def build_tag_graph(recipe, tag_graph=None):

        from stimela.kitchen.recipe import Recipe  # TODO: working around circular import - ew.

        tag_graph = tag_graph or benedict(keypath_separator=".")

        for step_name, step in recipe.steps.items():
            tags = getattr(step, "tags", [])
            for tag in tags:
                tag_graph[".".join((recipe.fqname, step_name, tag))] = None

            if isinstance(step.cargo, Recipe):
                tag_graph = TagManager.build_tag_graph(step.cargo, tag_graph=tag_graph)

        return tag_graph

    # @property
    # def always_tags(self):
    #     return [k for k in self.flat_tag_graph.keys() if k.rsplit('.', 1)[-1] == "always"]

    def generate_always_tags(self, recipe, always_tags=None):

        from stimela.kitchen.recipe import Recipe  # TODO: working around circular import - ew.

        always_tags = always_tags or [f"{recipe.fqname}.always"]

        for step_name, step in recipe.steps.items():
            if isinstance(step.cargo, Recipe):
                if "never" in step.tags:  # A never on a parent overides an always on a child.
                    continue
                always_tags.extend([".".join((recipe.fqname, step_name, "always"))])
                always_tags = self.generate_always_tags(step.cargo, always_tags=always_tags)

        return always_tags

    @property
    def never_tags(self):
        return [k for k in self.flat_tag_graph.keys() if k.rsplit('.', 1)[-1] == "never"]


def get_always_tags(recipe, strip_root=False):

    from stimela.kitchen.recipe import Recipe  # NOTE: Avoid circular import.

    def _generate_always_tags(recipe, always_tags=None):

        always_tags = always_tags or [f"{recipe.fqname}.always"]

        for step_name, step in recipe.steps.items():
            if isinstance(step.cargo, Recipe):
                if "never" in step.tags:  # A never on a parent overides an always on a child.
                    continue
                always_tags.extend([".".join((recipe.fqname, step_name, "always"))])
                always_tags = _generate_always_tags(step.cargo, always_tags=always_tags)

        return always_tags

    always_tags = _generate_always_tags(recipe)

    if strip_root:
        return [t.split(".", 1)[1] for t in always_tags]
    else:
        return always_tags