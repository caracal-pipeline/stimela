from itertools import chain
from typing import Optional
from benedict import benedict
from collections import namedtuple
import networkx as nx

from stimela import log_exception, stimelogging
from stimela.stimelogging import log_rich_payload
from stimela.exceptions import *


def apply_tags(graph, tags):
    """Given a graph, apply all the tags."""

    for tag in tags:
        step_name, tag = tag.rsplit(".", 1)
        for node_name in graph.adj[step_name].keys():
            node = graph.nodes[node_name]
            if tag in node['tags']:
                node['enabled'] = True
                node['explicit'] = True

def apply_skip_tags(graph, skip_tags):
    """Given a graph, apply all the skip tags."""

    for tag in skip_tags:
        step_name, tag = tag.rsplit(".", 1)
        for node_name in graph.adj[step_name].keys():
            node = graph.nodes[node_name]
            if tag in node["tags"]:
                node["enabled"] = False

def apply_always_tags(graph):

    for node in graph.nodes.values():
        if "always" in node.get("tags", tuple()):
            node["enabled"] = True

def apply_never_tags(graph):

    for node in graph.nodes.values():
        if "never" in node.get("tags", tuple()):
            node["enabled"] = False

def apply_step_ranges(graph, step_ranges):

    for step_range in step_ranges:
        step_name, step_range = step_range.rsplit(".", 1)

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


def apply_skip_ranges(graph, skip_ranges):

    for step_range in skip_ranges:
        step_name, step_range = step_range.rsplit(".", 1)

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

def apply_enabled_steps(graph, enable_steps):

    for enable_step in enable_steps:
        step_name, enable_step = enable_step.rsplit(".", 1)

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

def finalize(graph):

    nodes = graph.nodes

    x_nodes = nx.get_node_attributes(graph, "explicit", False)
    e_nodes = nx.get_node_attributes(graph, "enabled", False)

    xe_nodes = {k for k in x_nodes.keys() if (x_nodes[k] and e_nodes[k])}

    # First off, traverse the graph and resolve skips i.e. disables.
    for node_name, node in nodes.items():

        # This node has been turned off explicitly.
        if not node.get("enabled", True):
            descendants = nx.descendants(graph, node_name)
            if any([d in xe_nodes for d in descendants]):
                # Explicitly enabled descendents ignore parent disables.
                del node["enabled"]
            else:
                # Disable all descendents.
                for des_name in descendants:
                    nodes[des_name]["enabled"] = False

    # If no nodes were explicitly selected, assume that we are running
    # the full recipe, possibly with skips. TODO: This may be slightly
    # flawed if we have tags on subrecipes as then we have xe_nodes, but
    # that doesn't preclude enabling other steps.
    if not xe_nodes:
        for node_name, node in nodes.items():
            node["enabled"] = node.get("enabled", True)
        return

    # Do a second traversal, this time resolving enables i.e. selections.
    for node_name, node in nodes.items():
        if node.get("enabled", False):
            # Enable all of this node's ancestors.
            for ancestor in nx.ancestors(graph, node_name):
                ancestor_node = nodes[ancestor]
                ancestor_node["enabled"] = True

            # Check if any descendant of the current node is explicitly
            # enabled and continue if so.
            descendants = nx.descendants(graph, node_name)
            if any([d in xe_nodes for d in descendants]):
                continue

            # Otherwise, enable all descendants.
            for descendant in descendants:
                des_node = nodes[descendant]
                des_node["enabled"] = des_node.get("enabled", True)

def reformat_opts(opts: List[str], prepend: Optional[str] = None):
    """Given a list of option strings, reformat them appropriately."""
    opts = set(chain(*(opt.split(",") for opt in opts)))
    return {f"{prepend}.{o}" for o in opts} if prepend else opts


def graph_to_constraints(
    graph: nx.DiGraph,
    tags: List[str] = [],
    skip_tags: List[str] = [],
    step_ranges: List[str] = [],
    skip_ranges: List[str] = [],
    enable_steps: List[str] = []
):

    root = graph.graph.get("root", None)

    # Convert the tags and steps into benedicts, the keypaths of which will
    # correspond to the node names in graph. Include root if set.
    tags = reformat_opts(tags, prepend=root)
    skip_tags = reformat_opts(skip_tags, prepend=root)
    step_ranges = reformat_opts(step_ranges, prepend=root)
    skip_ranges = reformat_opts(skip_ranges, prepend=root)
    enable_steps = reformat_opts(enable_steps, prepend=root)

    # This status implies that all steps barring those which are explicitly
    # selected via a tag or a step range should be disabled. Needed for the
    # case where recipes are nested.
    # self.has_selections = any([self.step_ranges, self.tags])

    # NOTE(JSKenyon): There is a slight dependence on order here for steps
    # which are affected by more than one of the following operations. Once
    # the tests are fleshed out, revisit this to ensure it behaves as expected.

    # Start off by enabling always steps.
    apply_always_tags(graph)
    # Then disable all never steps; never trumps always.
    apply_never_tags(graph)
    # Turn on all tagged steps.
    apply_tags(graph, tags)
    # Turn of skip tagged steps.
    apply_skip_tags(graph, skip_tags)
    # Turn on selected steps.
    apply_step_ranges(graph, step_ranges)
    # Turn off skipped steps.
    apply_skip_ranges(graph, skip_ranges)
    # Turn on enabled steps.
    apply_enabled_steps(graph, enable_steps)
    # Having applied all of the above, figure out the steps to run.
    finalize(graph)

    active_steps = [k for k, v in nx.get_node_attributes(graph, "enabled", False).items() if v]

    # NOTE: The actual enabling and disabling of steps will need to
    # remain recursive as each recipe has to call enable_step itself.
    # This means that we should move the graph manipulation to a
    # separate function which is then the input to the recursive
    # component.

    return RunConstraints(graph)


class RunConstraints:

    def __init__(self, graph):

        self.graph = graph

        enable_states = nx.get_node_attributes(graph, "enabled", False)
        self.enabled_nodes = [k for k, v in enable_states.items() if v]
        self.disabled_nodes = [k for k, v in enable_states.items() if not v]

        force_states = nx.get_node_attributes(graph, "force_enable")
        self.forced_nodes = [k for k in force_states.keys()]

    def get_enabled_steps(self, fqname):
        return {
            k.lstrip(f"{fqname}.") for k in self.graph.adj[fqname].keys()
            if k in self.enabled_nodes
        }

    def get_disabled_steps(self, fqname):
        return {
            k.lstrip(f"{fqname}.") for k in self.graph.adj[fqname].keys()
            if k in self.disabled_nodes
        }

    def get_forced_steps(self, fqname):
        return {
            k.lstrip(f"{fqname}.") for k in self.graph.adj[fqname].keys()
            if k in self.forced_nodes
        }
