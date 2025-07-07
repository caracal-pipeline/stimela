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

def apply_tag_inclusions(
    graph: nx.DiGraph,
    tag_inclusions: Set[str]
):
    """Given a graph, apply the 'enabled' status to steps associated with tags.

    Adds 'enabled' to the 'status' attribute of each graph node associated
    with the specified tags. If 'status' has not been set, it is assumed to be
    an empty set.

    Args:
        graph:
            The graph object on which to update the 'status' attribute.
        tag_inclusions:
            The tags which need to be associated with the 'enabled' state.
            Tags are of the form '{recipe}.{tag}',
            '{recipe}.{subrecipe}.{tag}' etc.

    Raises:
        StepSelectionError: If the tags did not apply to any recipe steps.
    """
    for tag in tag_inclusions:
        successful = False
        step_name, tag = tag.rsplit(".", 1)
        for node_name in graph.successors(step_name):
            node = graph.nodes[node_name]
            if tag in node['tags']:
                node['status'] = node.get("status", set()) | {"enabled"}
                successful = True
        if not successful:
            raise StepSelectionError(
                f"'{tag}' is not a valid tag of '{step_name}'."
            )

def apply_tag_exclusions(
    graph: nx.DiGraph,
    tag_exclusions: Set[str]
):
    """Given a graph, apply the 'disabled' status to steps associated with tags.

    Adds 'disabled' to the 'status' attribute of each graph node associated
    with the specified tags. If 'status' has not been set, it is assumed to be
    an empty set.

    Args:
        graph:
            The graph object on which to update the 'status' attribute.
        tag_exclusions:
            The tags which need to be associated with the 'disabled' state.
            Tags are of the form '{recipe}.{tag}',
            '{recipe}.{subrecipe}.{tag}' etc.

    Raises:
        StepSelectionError: If the tags did not apply to any recipe steps.
    """
    for tag in tag_exclusions:
        successful = False
        step_name, tag = tag.rsplit(".", 1)
        for node_name in graph.successors(step_name):
            node = graph.nodes[node_name]
            if tag in node["tags"]:
                node['status'] = node.get("status", set()) | {"disabled"}
                successful = True
        if not successful:
            raise StepSelectionError(
                f"'{tag}' is not a valid tag of '{step_name}'."
            )

def apply_always_tags(graph: nx.DiGraph):
    """Apply the 'weakly_enabled' status to graph nodes tagged with 'always'.

    Apply the special 'always' tag by updating the status attribute of
    associated steps with the 'weakly_enabled' status.

    Args:
        graph:
            The graph object on which to update the 'status' attribute.
    """
    for node in graph.nodes.values():
        if "always" in node.get("tags", tuple()):
            node['status'] = node.get("status", set()) | {"weakly_enabled"}

def apply_never_tags(graph: nx.DiGraph):
    """Apply the 'weakly_disabled' status to graph nodes tagged with 'never'.

    Apply the special 'never' tag by updating the status attribute of
    associated steps with the 'weakly_disabled' status.

    Args:
        graph:
            The graph object on which to update the 'status' attribute.
    """
    for node in graph.nodes.values():
        if "never" in node.get("tags", tuple()):
            node['status'] = node.get("status", set()) | {"weakly_disabled"}

def apply_step_inclusions(
    graph: nx.DiGraph,
    step_inclusions: Set[str]
):
    """Update the status attributes on a graph based on step inclusions.

    Adds 'enabled' or 'weakly_enabled' to the 'status' attribute of each graph
    node selected by the step inclusions. Both half unbounded and bounded
    ranges add 'weakly_enabled' while inclusions of a single step apply
    'enabled' (as well as imply ignore_skips - see PLACEHOLDER). If 'status'
    has not been set, it is assumed to be an empty set.

    Args:
        graph:
            The graph object on which to update the 'status' attribute.
        step_inclusions:
            The steps which should be included when the recipe the graph
            represents is run. These will be of the form {recipe}.{step},
            {recipe}.{subrecipe}.{step} etc. Additionally, ranges are specified
            using {recipe}.{first_step}:{last_step}. Either the first or
            last step can be omitted to indicate a half-unbounded range.

    Raises:
        StepSelectionError: If the steps did not appear in the graph.
    """
    node_names = list(graph.nodes.keys())

    for step_inclusion in step_inclusions:
        step_name, inclusion_str = step_inclusion.rsplit(".", 1)

        if inclusion_str.startswith(":"):
            case = "unbounded_below"
            start, stop = inclusion_str.rsplit(":")
        elif inclusion_str.endswith(":"):
            case = "unbounded_above"
            start, stop = inclusion_str.split(":")
        elif ":" in inclusion_str:
            case = "bounded"
            start, stop = inclusion_str.split(":")
        else:
            case = "single_step"
            start = stop = inclusion_str

        # Unbounded below/above ranges use the first/last non-root node.
        # For now, this relies on node_names preserving insertion order.
        # NOTE(JSKenyon): This requires us to add special handling for the
        # unbounded_below case as the following logic will incorrectly enable
        # ancestor nodes.
        start = f"{step_name}.{start}" if start else node_names[1]
        stop = f"{step_name}.{stop}" if stop else node_names[-1]

        status = "enabled" if case == "single_step" else "weakly_enabled"

        if not (start in node_names and stop in node_names):
            raise StepSelectionError(
                f"Step/steps '{inclusion_str}' not in '{step_name}'."
            )

        start_ind = node_names.index(start)
        stop_ind = node_names.index(stop) + 1

        if case == "unbounded_below":  # Special case - see previous note.
            ancestors = nx.ancestors(graph, stop)
        else:
            ancestors = set()

        selected_nodes = set(node_names[start_ind:stop_ind]) - ancestors

        for node_name in selected_nodes:
            node = graph.nodes[node_name]
            node['status'] = node.get("status", set()) | {status}

def apply_step_exclusions(
    graph: nx.DiGraph,
    step_exclusions: Set[str]
):
    """Update the status attributes on a graph based on step exclusions.

    Adds 'disabled' or 'weakly_disabled' to the 'status' attribute of each
    graph node selected by the step exclusions. Both half unbounded and bounded
    ranges add 'weakly_disabled' while exclusions of a single step apply
    'disabled'. If 'status' has not been set, it is assumed to be an empty set.

    Args:
        graph:
            The graph object on which to update the 'status' attribute.
        step_exclusions:
            The steps which should be excluded when the recipe the graph
            represents is run. These will be of the form {recipe}.{step},
            {recipe}.{subrecipe}.{step} etc. Additionally, ranges are specified
            using {recipe}.{first_step}:{last_step}. Either the first or
            last step can be omitted to indicate a half-unbounded range.

    Raises:
        StepSelectionError: If the steps did not appear in the graph.
    """

    node_names = list(graph.nodes.keys())

    for step_exclusion in step_exclusions:
        step_name, exclusion_str = step_exclusion.rsplit(".", 1)

        if exclusion_str.startswith(":"):
            case = "unbounded_below"
            start, stop = exclusion_str.rsplit(":")
        elif exclusion_str.endswith(":"):
            case = "unbounded_above"
            start, stop = exclusion_str.split(":")
        elif ":" in exclusion_str:
            case = "bounded"
            start, stop = exclusion_str.split(":")
        else:
            case = "single_step"
            start = stop = exclusion_str

        # Unbounded below/above ranges use the first/last non-root node.
        # For now, this relies on node_names preserving insertion order.
        # NOTE(JSKenyon): This requires us to add special handling for the
        # unbounded_below case as the following logic will incorrectly disable
        # ancestor nodes.
        start = f"{step_name}.{start}" if start else node_names[1]
        stop = f"{step_name}.{stop}" if stop else node_names[-1]

        status = "disabled" if case == "single_step" else "weakly_disabled"

        if not (start in node_names and stop in node_names):
            raise StepSelectionError(
                f"Step/steps '{exclusion_str}' not in '{step_name}'."
            )

        start_ind = node_names.index(start)
        stop_ind = node_names.index(stop) + 1

        if case == "unbounded_below":  # Special case - see previous note.
            ancestors = nx.ancestors(graph, stop)
        else:
            ancestors = set()

        selected_nodes = set(node_names[start_ind:stop_ind]) - ancestors

        for node_name in selected_nodes:
            node = graph.nodes[node_name]
            node['status'] = node.get("status", set()) | {status}

def apply_step_unskips(
    graph: nx.DiGraph,
    step_unskips: Set[str]
):
    """Add the 'unskip' attribute to graph nodes based on step_unskips.

    This adds the 'unskip' attribute to the graph nodes assosciated with
    step_unskips. This has nothing to do with step selection. Instead, the
    'unskip' attribute effectively countermands any skip instructions which
    appear in the recipe. Unskip steps are NOT guaranteed to be included but,
    if they are, they are guaranteed to run regardless of skip fields in the
    recipe.

    Args:
        graph:
            The graph object on which to update the 'status' attribute.
        step_exclusions:
            The steps which should be excluded when the recipe the graph
            represents is run. These will be of the form {recipe}.{step},
            {recipe}.{subrecipe}.{step} etc. Additionally, ranges are specified
            using {recipe}.{first_step}:{last_step}. Either the first or
            last step can be omitted to indicate a half-unbounded range.

    Raises:
        StepSelectionError: If the steps did not appear in the graph.
    """

    node_names = list(graph.nodes.keys())

    for step_unskip in step_unskips:
        step_name, unskip_str = step_unskip.rsplit(".", 1)

        if unskip_str.startswith(":"):
            case = "unbounded_below"
            start, stop = unskip_str.rsplit(":")
        elif unskip_str.endswith(":"):
            case = "unbounded_above"
            start, stop = unskip_str.split(":")
        elif ":" in unskip_str:
            case = "bounded"
            start, stop = unskip_str.split(":")
        else:
            case = "single_step"
            start = stop = unskip_str

        # Unbounded below/above ranges use the first/last non-root node.
        # For now, this relies on node_names preserving insertion order.
        # NOTE(JSKenyon): This requires us to add special handling for the
        # unbounded_below case as the following logic will incorrectly unskip
        # ancestor nodes.
        start = f"{step_name}.{start}" if start else node_names[1]
        stop = f"{step_name}.{stop}" if stop else node_names[-1]

        if not (start in node_names and stop in node_names):
            raise StepSelectionError(
                f"Step/steps '{unskip_str}' not in '{step_name}'."
            )

        start_ind = node_names.index(start)
        stop_ind = node_names.index(stop) + 1

        if case == "unbounded_below":  # Special case - see previous note.
            ancestors = nx.ancestors(graph, stop)
        else:
            ancestors = set()

        selected_nodes = set(node_names[start_ind:stop_ind]) - ancestors

        for node_name in selected_nodes:
            node = graph.nodes[node_name]
            node['unskip'] = True

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
    tag_inclusions: Tuple[str] = (),
    tag_exclusions: Tuple[str] = (),
    step_inclusions: Tuple[str] = (),
    step_exclusions: Tuple[str] = (),
    step_unskips: Tuple[str] = ()
):

    try:
        root = graph.graph["root"]
    except KeyError:
        raise KeyError("Graph attribute 'root' must be set.")

    # Special case - individually specified steps should ignore skips.
    implicit_unskips = tuple([s for s in step_inclusions if ":" not in s])
    # Unpack commas, prepend graph root and convert to sets.
    tag_inclusions = reformat_opts(tag_inclusions, prepend=root)
    tag_exclusions = reformat_opts(tag_exclusions, prepend=root)
    step_inclusions = reformat_opts(step_inclusions, prepend=root)
    step_exclusions = reformat_opts(step_exclusions, prepend=root)
    step_unskips = reformat_opts(step_unskips + implicit_unskips, prepend=root)

    # If there are tag/step inclusions, assume nodes are disabled by default.
    if any([tag_inclusions, step_inclusions]):
        default_status = "weakly_disbled"
    else:
        default_status = "weakly_enabled"

    # NOTE(JSKenyon): The order of the following should no longer matter.
    apply_always_tags(graph)
    apply_never_tags(graph)
    apply_tag_inclusions(graph, tag_inclusions)
    apply_tag_exclusions(graph, tag_exclusions)
    apply_step_inclusions(graph, step_inclusions)
    apply_step_exclusions(graph, step_exclusions)
    apply_step_unskips(graph, step_unskips)

    # Having applied all of the above, figure out the steps to run.
    finalize(graph, default_status=default_status)

    return RunConstraints(graph)


class RunConstraints:

    def __init__(self, graph):

        self.graph = graph

        enable_states = nx.get_node_attributes(graph, "enabled", False)
        self.enabled_nodes = [k for k, v in enable_states.items() if v]
        self.disabled_nodes = [k for k, v in enable_states.items() if not v]

        force_states = nx.get_node_attributes(graph, "unskip")
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
