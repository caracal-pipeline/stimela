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

    for tag in tags.keypaths():
        if "." not in tag:
            step_name, tag = graph.graph['root'], tag
        else:
            step_name, tag = tag.rsplit(".", 1)
            step_name = f"{graph.graph['root']}.{step_name}"
        for node_name in graph.adj[step_name].keys():
            node = graph.nodes[node_name]
            if tag in node['tags']:
                node['enabled'] = True
                node['explicit'] = True

def apply_skip_tags(graph, skip_tags):
    """Given a graph, apply all the skip tags."""

    for tag in skip_tags.keypaths():
        if "." not in tag:
            step_name, tag = graph.graph['root'], tag
        else:
            step_name, tag = tag.rsplit(".", 1)
            step_name = f"{graph.graph['root']}.{step_name}"
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

    for step_range in step_ranges.keypaths():
        if "." not in step_range:
            step_name, step_range = graph.graph['root'], step_range
        else:
            step_name, step_range = step_range.rsplit(".", 1)
            step_name = f"{graph.graph['root']}.{step_name}"

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

    for step_range in skip_ranges.keypaths():
        if "." not in step_range:
            step_name, step_range = graph.graph['root'], step_range
        else:
            step_name, step_range = step_range.rsplit(".", 1)
            step_name = f"{graph.graph['root']}.{step_name}"

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

    for enable_step in enable_steps.keypaths():
        if "." not in enable_step:
            step_name, enable_step = graph.graph['root'], enable_step
        else:
            step_name, enable_step = enable_step.rsplit(".", 1)
            step_name = f"{graph.graph['root']}.{step_name}"

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

def process_commas(opts: List[str]):
    return set(chain(*(opt.split(",") for opt in opts)))

def benedictify(opts: List[str], root: Optional[str] = None):
    bdict = benedict.fromkeys(sorted(opts))
    return benedict({root: bdict}) if root and bdict else bdict 

def graph_to_constraints(
    graph: nx.DiGraph,
    tags: List[str] = [],
    skip_tags: List[str] = [],
    step_ranges: List[str] = [],
    skip_ranges: List[str] = [],
    enable_steps: List[str] = []
):

    root = graph.graph.get("root", None)
    root = None  # NOTE: To keep things working for now - remove.

    # Convert the tags and steps into benedicts, the keypaths of which will
    # correspond to the node names in graph. Include root if set.
    tags = benedictify(process_commas(tags), root=root)
    skip_tags = benedictify(process_commas(skip_tags), root=root)
    step_ranges = benedictify(process_commas(step_ranges), root=root)
    skip_ranges = benedictify(process_commas(skip_ranges), root=root)
    enable_steps = benedictify(process_commas(enable_steps), root=root)

    # This status implies that all steps barring those which are explicitly
    # selected via a tag or a step range should be disabled. Needed for the
    # case where recipes are nested.
    # self.has_selections = any([self.step_ranges, self.tags])


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
    apply_skip_ranges(graph, skip_tags)
    # Turn on enabled steps.
    apply_enabled_steps(graph, enable_steps)
    # Having applied all of the above, figure out the steps to run.
    finalize(graph)

    _active_steps = [k for k, v in nx.get_node_attributes(graph, "enabled", False).items() if v]

    # NOTE: The actual enabling and disabling of steps will need to
    # remain recursive as each recipe has to call enable_step itself.
    # This means that we should move the graph manipulation to a
    # separate function which is then the input to the recursive
    # component.

    import ipdb; ipdb.set_trace()