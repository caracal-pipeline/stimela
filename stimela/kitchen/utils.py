from itertools import chain
from benedict import benedict
from collections import namedtuple
from copy import deepcopy
import networkx as nx

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
        active_skip_tags = self.get_active_tags(fqname, "skip_tags")

        # NOTE(JSKenyon): This is important - specifying tags implies the
        # selection of one or more parent steps/recipes.
        implied_steps = self.get_active_steps(fqname, "tags", tag_field=True)
        implied_steps |= self.get_active_steps(fqname, "always_tags", tag_field=True)

        active_step_ranges = self.get_active_steps(fqname, "step_ranges")
        active_skip_ranges = self.get_active_steps(fqname, "skip_ranges")
        active_enable_steps = self.get_active_steps(fqname, "enable_steps")

        return self.Restrictions(
            tags=active_tags,
            skip_tags=active_skip_tags,
            step_ranges=active_step_ranges.union(implied_steps),
            skip_ranges=active_skip_ranges,
            enable_steps=active_enable_steps
        )

    def apply_tags(self, graph):
        """Given a graph, apply all the tags."""

        for tag in self.tags.keypaths():
            if "." not in tag:
                step_name, tag = graph.name, tag
            else:
                step_name, tag = tag.rsplit(".", 1)
                step_name = f"{graph.name}.{step_name}"
            for node_name in graph.adj[step_name].keys():
                node = graph.nodes[node_name]
                if tag in node['tags']:
                    node['enabled'] = True
                    node['explicit'] = True
                    # for ancestor in nx.ancestors(graph, node_name):
                    #     graph.nodes[ancestor]["enabled"] = True

    def apply_skip_tags(self, graph):
        """Given a graph, apply all the skip tags."""

        for tag in self.skip_tags.keypaths():
            if "." not in tag:
                step_name, tag = graph.name, tag
            else:
                step_name, tag = tag.rsplit(".", 1)
                step_name = f"{graph.name}.{step_name}"
            for node_name in graph.adj[step_name].keys():
                node = graph.nodes[node_name]
                if tag in node["tags"]:
                    node["enabled"] = False
                    # for descendant in nx.descendants(graph, node_name):
                    #     graph.nodes[descendant]["enabled"] = False

    def apply_always_tags(self, graph):

        for node_name, node in graph.nodes.items():
            if "always" in node.get("tags", tuple()):
                node["enabled"] = True
                # for ancestor in nx.ancestors(graph, node_name):
                #     graph.nodes[ancestor]["enabled"] = True

    def apply_never_tags(self, graph):

        # EDGE CASES:
        # 1. Never on parent, always on child - never takes precedence.
        # 2. Always on child

        for node_name, node in graph.nodes.items():
            if "never" in node.get("tags", tuple()):
                node["enabled"] = False
                # for descendant in nx.descendants(graph, node_name):
                #     graph.nodes[descendant]["enabled"] = False

    def apply_step_ranges(self, graph):

        for step_range in self.step_ranges.keypaths():
            if "." not in step_range:
                step_name, step_range = graph.name, step_range
            else:
                step_name, step_range = step_range.rsplit(".", 1)
                step_name = f"{graph.name}.{step_name}"

            if ":" in step_range:
                start, stop = step_range.split(":")
            else:
                start = stop = step_range

            start = f"{step_name}.{start}"
            stop = f"{step_name}.{stop}"

            force = start == stop

            adjacent_node_names = list(graph.adj[step_name].keys())

            start_ind = adjacent_node_names.index(start)
            stop_ind = adjacent_node_names.index(stop) + 1

            for node_name in adjacent_node_names[start_ind: stop_ind]:
                node = graph.nodes[node_name]
                node["explicit"] = True
                node["enabled"] = True
                if force:
                    node["force_enable"] = True


    def apply_skip_ranges(self, graph):

        for step_range in self.skip_ranges.keypaths():
            if "." not in step_range:
                step_name, step_range = graph.name, step_range
            else:
                step_name, step_range = step_range.rsplit(".", 1)
                step_name = f"{graph.name}.{step_name}"

            if ":" in step_range:
                start, stop = step_range.split(":")
            else:
                start = stop = step_range

            start = f"{step_name}.{start}"
            stop = f"{step_name}.{stop}"

            adjacent_node_names = list(graph.adj[step_name].keys())

            start_ind = adjacent_node_names.index(start)
            stop_ind = adjacent_node_names.index(stop) + 1

            for node_name in adjacent_node_names[start_ind: stop_ind]:
                node = graph.nodes[node_name]
                node["enabled"] = False

    def apply_enabled_steps(self, graph):

        for enable_step in self.enable_steps.keypaths():
            if "." not in enable_step:
                step_name, enable_step = graph.name, enable_step
            else:
                step_name, enable_step = enable_step.rsplit(".", 1)
                step_name = f"{graph.name}.{step_name}"

            if ":" in enable_step:
                start, stop = enable_step.split(":")
            else:
                start = stop = enable_step

            start = f"{step_name}.{start}"
            stop = f"{step_name}.{stop}"

            adjacent_node_names = list(graph.adj[step_name].keys())

            start_ind = adjacent_node_names.index(start)
            stop_ind = adjacent_node_names.index(stop) + 1

            for node_name in adjacent_node_names[start_ind: stop_ind]:
                node = graph.nodes[node_name]
                node["enabled"] = True
                node["explicit"] = True
                node["force_enable"] = True

    def finalize(self, graph):

        nodes = graph.nodes

        explicit_nodes = nx.get_node_attributes(graph, "explicit", False)
        enabled_nodes = nx.get_node_attributes(graph, "enabled", False)

        ee_nodes = {k for k in explicit_nodes.keys() if (explicit_nodes[k] and enabled_nodes[k])}

        # First off, traverse the graph and resolve skips i.e. disables.
        for node_name, node in nodes.items():

            # This node has been turned off explicitly.
            if not node.get("enabled", True):
                descendants = nx.descendants(graph, node_name)

                # If this node has been explicitly disabled...
                if any([d in ee_nodes for d in descendants]):
                    # ...ignore it if is descendents are explicitly enabled.
                    del node["enabled"]
                else:
                    # ...disable all descendents.
                    for des_name in descendants:
                        nodes[des_name]["enabled"] = False

        # If no nodes were explicitly selected, assume that we are running
        # the full recipe, possibly with skips.
        if not ee_nodes:
            for node_name, node in nodes.items():
                node["enabled"] = node.get("enabled", True)
            return

        # Do a second traversal, this time resolving enables i.e. selections.
        for node_name, node in nodes.items():
            # If a step is enabled...
            if node.get("enabled", False):
                # ...enable all of its ancestors.
                for ancestor in nx.ancestors(graph, node_name):
                    ancestor_node = nodes[ancestor]
                    ancestor_node["enabled"] = True

                # ...check if any descendent is explicitly enabled.
                descendants = nx.descendants(graph, node_name)

                # ...and has explicitly enabled decsendents, continue.
                if any([d in ee_nodes for d in descendants]):
                    continue

                # ...and has no explicitly enabled descenents, enable them.
                for descendant in descendants:
                    des_node = nodes[descendant]
                    des_node["enabled"] = des_node.get("enabled", True)

    def applicator(self, graph):

        # Start off by enabling always steps.
        self.apply_always_tags(graph)
        # Then disable all never steps; never trumps always.
        self.apply_never_tags(graph)
        # Turn on all tagged steps.
        self.apply_tags(graph)
        # Turn of skip tagged steps.
        self.apply_skip_tags(graph)
        # Turn on selected steps.
        self.apply_step_ranges(graph)
        # Turn off skipped steps.
        self.apply_skip_ranges(graph)
        # Turn on enabled steps.
        self.apply_enabled_steps(graph)
        # Having applied all of the above, figure out the steps to run.
        self.finalize(graph)

        _active_steps = [k for k, v in nx.get_node_attributes(graph, "enabled", False).items() if v]

        # NOTE: The actual enabling and disabling of steps will need to
        # remain recursive as each recipe has to call enable_step itself.
        # This means that we should move the graph manipulation to a
        # separate function which is then the input to the recursive
        # component.

        import ipdb; ipdb.set_trace()

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