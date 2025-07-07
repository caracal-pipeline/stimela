from itertools import chain
from typing import Optional, List, Tuple, Set
import networkx as nx

from stimela.exceptions import StepSelectionError

STATUS_HEIRARCHY = (
    "disabled",
    "enabled",
    "weakly_disabled",
    "weakly_enabled"
)

ENABLES = (
    "enabled",
    "weakly_enabled"
)

DISABLES = (
    "disabled",
    "weakly_disabled"
)


def apply_tags(
    graph: nx.DiGraph,
    tags: Set[str]
):
    """Given a graph, apply the 'enabled' status to steps associated with tags.

    Adds 'enabled' to the 'status' attribute of each graph node associated
    with the specified tags. If 'status' has not been set, it is assumed to be
    an empty set.

    Args:
        graph:
            The graph object on which to update the 'status' attribute.
        tags:
            The tags which need to be associated with the 'enabled' state.
            Tags are of the form '{recipe}.{tag}',
            '{recipe}.{subrecipe}.{tag}' etc.

    Raises:
        StepSelectionError: If the tags did not apply to any recipe steps.
    """
    for tag in tags:
        successful = False
        step_name, tag = tag.rsplit(".", 1)
        for node_name in graph.adj[step_name].keys():
            node = graph.nodes[node_name]
            if tag in node['tags']:
                node['status'] = node.get("status", set()) | {"enabled"}
                successful = True
        if not successful:
            raise StepSelectionError(
                f"'{tag}' is not a valid tag of '{step_name}'."
            )

def apply_skip_tags(
    graph: nx.DiGraph,
    skip_tags: Set[str]
):
    """Given a graph, apply the 'disabled' status to steps associated with tags.

    Adds 'disabled' to the 'status' attribute of each graph node associated
    with the specified tags. If 'status' has not been set, it is assumed to be
    an empty set.

    Args:
        graph:
            The graph object on which to update the 'status' attribute.
        tags:
            The tags which need to be associated with the 'disabled' state.
            Tags are of the form '{recipe}.{tag}',
            '{recipe}.{subrecipe}.{tag}' etc.

    Raises:
        StepSelectionError: If the tags did not apply to any recipe steps.
    """
    for tag in skip_tags:
        successful = False
        step_name, tag = tag.rsplit(".", 1)
        for node_name in graph.adj[step_name].keys():
            node = graph.nodes[node_name]
            if tag in node["tags"]:
                node['status'] = node.get("status", set()) | {"disabled"}
                successful = True
        if not successful:
            raise StepSelectionError(
                f"'{tag}' is not a valid tag of '{step_name}'."
            )

def apply_always_tags(graph):

    for node in graph.nodes.values():
        if "always" in node.get("tags", tuple()):
            node['status'] = node.get("status", set()) | {"weakly_enabled"}

def apply_never_tags(graph):

    for node in graph.nodes.values():
        if "never" in node.get("tags", tuple()):
            node['status'] = node.get("status", set()) | {"weakly_disabled"}

def apply_step_ranges(graph, step_ranges):

    node_names = list(graph.nodes.keys())

    for step_range in step_ranges:
        step_name, step_range = step_range.rsplit(".", 1)

        if step_range.startswith(":"):
            case = "unbounded_below"
            start, stop = step_range.rsplit(":")
        elif step_range.endswith(":"):
            case = "unbounded_above"
            start, stop = step_range.split(":")
        elif ":" in step_range:
            case = "bounded"
            start, stop = step_range.split(":")
        else:
            case = "single_step"
            start = stop = step_range

        # Unbounded below/above ranges use the first/last non-root node.
        # Problem case:
        #   - This will capture the parent node in the :step case as the nodes
        #     are handed in insertion order.
        start = f"{step_name}.{start}" if start else node_names[1]
        stop = f"{step_name}.{stop}" if stop else node_names[-1]

        status = "enabled" if start == stop else "weakly_enabled"

        if not (start in node_names and stop in node_names):
            raise StepSelectionError(
                f"Step/steps '{step_range}' not in '{step_name}'."
            )

        start_ind = node_names.index(start)
        stop_ind = node_names.index(stop) + 1

        if case == "unbounded_below":
            exclusions = nx.ancestors(graph, stop)
        else:
            exclusions = set()

        selected_nodes = [
            nn for nn in node_names[start_ind:stop_ind] if nn not in exclusions
        ]

        for node_name in selected_nodes:
            node = graph.nodes[node_name]
            node['status'] = node.get("status", set()) | {status}

def apply_skip_ranges(graph, skip_ranges):

    node_names = list(graph.nodes.keys())

    for skip_range in skip_ranges:
        step_name, skip_range = skip_range.rsplit(".", 1)

        if skip_range.startswith(":"):
            case = "unbounded_below"
            start, stop = skip_range.rsplit(":")
        elif skip_range.endswith(":"):
            case = "unbounded_above"
            start, stop = skip_range.split(":")
        elif ":" in skip_range:
            case = "bounded"
            start, stop = skip_range.split(":")
        else:
            case = "single_step"
            start = stop = skip_range

        # Unbounded below/above ranges use the first/last non-root node.
        # Problem case:
        #   - This will capture the parent node in the :step case as the nodes
        #     are handed in insertion order.
        start = f"{step_name}.{start}" if start else node_names[1]
        stop = f"{step_name}.{stop}" if stop else node_names[-1]

        status = "disabled" if start == stop else "weakly_disabled"

        if not (start in node_names and stop in node_names):
            raise StepSelectionError(
                f"Step/steps '{skip_range}' not in '{step_name}'."
            )

        start_ind = node_names.index(start)
        stop_ind = node_names.index(stop) + 1

        if case == "unbounded_below":
            exclusions = nx.ancestors(graph, stop)
        else:
            exclusions = set()

        selected_nodes = [
            nn for nn in node_names[start_ind:stop_ind] if nn not in exclusions
        ]

        for node_name in selected_nodes:
            node = graph.nodes[node_name]
            node['status'] = node.get("status", set()) | {status}

def apply_enabled_steps(graph, enable_steps):

    node_names = list(graph.nodes.keys())

    for enable_step in enable_steps:
        step_name, enable_step = enable_step.rsplit(".", 1)

        if enable_step.startswith(":"):
            case = "unbounded_below"
            start, stop = enable_step.rsplit(":")
        elif enable_step.endswith(":"):
            case = "unbounded_above"
            start, stop = enable_step.split(":")
        elif ":" in enable_step:
            case = "bounded"
            start, stop = enable_step.split(":")
        else:
            case = "single_step"
            start = stop = enable_step

        # Unbounded below/above ranges use the first/last non-root node.
        # Problem case:
        #   - This will capture the parent node in the :step case as the nodes
        #     are handed in insertion order.
        start = f"{step_name}.{start}" if start else node_names[1]
        stop = f"{step_name}.{stop}" if stop else node_names[-1]

        if not (start in node_names and stop in node_names):
            raise StepSelectionError(
                f"Step/steps '{enable_step}' not in '{step_name}'."
            )

        start_ind = node_names.index(start)
        stop_ind = node_names.index(stop) + 1

        if case == "unbounded_below":
            exclusions = nx.ancestors(graph, stop)
        else:
            exclusions = set()

        selected_nodes = [
            nn for nn in node_names[start_ind:stop_ind] if nn not in exclusions
        ]

        for node_name in selected_nodes:
            node = graph.nodes[node_name]
            node['force_enable'] = True

def finalize(graph, default_status):

    nodes = graph.nodes
    root = graph.graph["root"]  # Outermost recipe name.

    # First off, traverse the graph and resolve the statuses on each node.
    for node_name, node in nodes.items():
        status = node.get("status", None)
        if status:
            # Resolve to the highest priority of the set statuses.
            node["status"] = next(s for s in STATUS_HEIRARCHY if s in status)

    # Apply default status to all nodes which do not yet have one.
    # for node_name, node in nodes.items():
    #     node["status"] = node.get("status", default_status)

    # disabled_nodes = [k for k, v in graph.nodes(data=True) if v.get('status', None) == 'disabled']
    # enabled_nodes = [k for k,v in nx.get_node_attributes(graph, "status").items() if v == "enabled"]

    # At this point all nodes have their status and we can begin the resolution
    # process. This needs to be done according the the heirarchy and the strong
    # states need to be propagated first.
    for node_name, node in nodes.items():

        status = node.get("status", None)

        # Disabled nodes propagate to their descendants.
        if status == "disabled":
            for des_name in nx.descendants(graph, node_name):
                graph.nodes[des_name]["status"] = "disabled"

        # Enabled nodes propagate to their ancestors.
        elif status == "enabled":
            dependencies = nx.shortest_path(graph.reverse(), node_name, root)
            for anc_name in dependencies[1:]:
                anc_node = graph.nodes[anc_name]
                if anc_node.get("status", None) == "enabled":
                    break  # Stop checking.
                anc_node["status"] = "implicitly_enabled"

    # After this first loop of resolution, all nodes on disabled branches will
    # have been set to disabled, and and enabled nodes and their necessary
    # ancestors will have been set to enabled. This is important - after the
    # first round of resolution the following two cases can be ignored:
    #   - any node below a disabled node will be disabled.
    #   - any node preceding an enabled node will be enabled.
    # The next step is to figure out the states of all the remaining
    # weakly/disabled enabled nodes.

    for node_name, node in nodes.items():

        status = node.get("status", None)
        # Weakly disabled nodes propagate to their descendents.
        # Problem cases:
        #   - deselect by range will weakly disable weakly enabled.
        if status == "weakly_disabled":
            for des_name in nx.descendants(graph, node_name):
                graph.nodes[des_name]["status"] = "weakly_disabled"

        # Weakly enabled nodes propagate to their ancestors.
        # Problem cases:
        #   -
        elif status == "weakly_enabled":
            dependencies = nx.shortest_path(graph.reverse(), node_name, root)
            for anc_name in dependencies[1:]:
                anc_node = graph.nodes[anc_name]
                if anc_node.get("status", None) in ("enabled", "weakly_enabled"):
                    break
                else:
                    anc_node["status"] = "implicitly_enabled"

    # This final case is the toughest to resolve as it has some subcases.
    # A node which has no status should either be:
    #   - set to weakly enabled if the graph is unconstrained
    #   - set to weakly disabled in the graph is constrained
    #   - set to weakly enabled if it is the child of an explictly enabled
    #     node and lacks explicitly enabled siblings

    for node_name, node in nodes.items():

        status = node.get("status", None)

        # This node has no status at present, inherit from parent or, failing
        # that, use the default value. The latter case applies to the root.
        # Inherited statuses will be weakened.
        if status is None:  # This should be top down i.e. only need parent, not ancestors.
            dependencies = nx.shortest_path(graph.reverse(), node_name, root)
            for anc_name in dependencies[1:] or [node_name]:  # Handle root case.
                anc_node = graph.nodes[anc_name]
                anc_status = anc_node.get("status", None)

                if anc_status in ("enabled", "weakly_enabled"):
                    node["status"] = "weakly_enabled"
                    break
                elif anc_status == "implicitly_enabled":
                    node["status"] = default_status #"weakly_disabled"
                    break
                else:
                    node["status"] = default_status
                    break
            # parents = list(graph.reverse().neighbors(node_name))
            # if parents:
            #     parent_status = graph.nodes[parents[0]]["status"]
            #     if "enable" in parent_status:
            #         node["status"] = "weakly_enabled"
            #     else:
            #         node["status"] = "weakly_disabled"
            # else:
            # node["status"] = default_status

    for node_name, node in nodes.items():
        node["enabled"] = node["status"] in ("weakly_enabled", "implicitly_enabled", "enabled")


def reformat_opts(opts: List[str], prepend: Optional[str] = None):
    """Given a list of option strings, reformat them appropriately."""
    opts = set(chain(*(opt.split(",") for opt in opts)))
    return {f"{prepend}.{o}" for o in opts} if prepend else opts


def graph_to_constraints(
    graph: nx.DiGraph,
    tags: Tuple[str] = (),
    skip_tags: Tuple[str] = (),
    step_ranges: Tuple[str] = (),
    skip_ranges: Tuple[str] = (),
    enable_steps: Tuple[str] = ()
):

    root = graph.graph.get("root", None)

    # Special case - individually specified steps should ignore skip fields.
    implicit_enables = tuple([s for s in step_ranges if ":" not in s])
    # Unpack commas, prepend graph root and convert to sets.
    tags = reformat_opts(tags, prepend=root)
    skip_tags = reformat_opts(skip_tags, prepend=root)
    step_ranges = reformat_opts(step_ranges, prepend=root)
    skip_ranges = reformat_opts(skip_ranges, prepend=root)
    enable_steps = reformat_opts(enable_steps + implicit_enables, prepend=root)

    # NOTE(JSKenyon): There is a slight dependence on order here for steps
    # which are affected by more than one of the following operations. Once
    # the tests are fleshed out, revisit this to ensure it behaves as expected.
    default_status = "weakly_disbled" if any([tags, step_ranges]) else "weakly_enabled"

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
    finalize(graph, default_status=default_status)

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
