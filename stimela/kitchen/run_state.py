from itertools import chain
from typing import Optional, List, Tuple, Set
import networkx as nx

from stimela.exceptions import StepSelectionError

STATUS_HIERARCHY = (
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

def resolve_states(
    graph: nx.DiGraph,
    default_status: str = "weakly_enabled"
):
    """Given a graph with statuses, resolve them into a boolean enable flag.

    This function is resposible for the potentially complicated resolution
    of the various statuses which have been applied to the graph. Performs
    the following steps:
        - replace the status on each node with its highest priority status.
        - propagate the 'enabled' and 'disabled' statuses.
        - propagate the 'weakly_enabled' and 'weakly_disabled' statuses.
        - determine the status of stateless nodes.
    Currently, this function is likely suboptimal as it requires serveral
    traversals of the graph. Typical stimela recipes are unlikely to include
    thousands of nodes so code readability is currently a higher priority.

    Args:
        graph:
            The graph object to which the resolved statuses will be applied.
        default_status:
            The status to set on nodes which otherwise have no status. The
            default only applies to nodes which have an 'implicitly_enabled'
            parent.

    Raises:
        ValueError: If a status falls though the resolution logic.
    """
    root = graph.graph["root"]  # Outermost recipe name.

    # Firstly, traverse the graph and resolve the statuses on each node. This
    # resolution is based on the hierarchy defined by STATUS_HIERARCHY.
    for node_name, status in nx.get_node_attributes(graph, "status").items():
        resolved_status = next(s for s in STATUS_HIERARCHY if s in status)
        graph.nodes[node_name]["status"] = resolved_status

    # At this point all nodes which had a status field will have been resolved
    # to the state of highest priority. The next step is to propagate the two
    # stronger statuses:
    #   - 'disabled': This state is propagated to successors/descendents
    #     ensuring that those subgraphs will not be run.
    #   - 'enabled': This state propagates the 'implicitly_enabled' state to
    #     its ancestors, starting from the enabled node.
    for node_name, node in graph.nodes.items():
        status = node.get("status", None)
        if status == "disabled":
            for successor in graph.successors(node_name):
                graph.nodes[successor]["status"] = "disabled"
        elif status == "enabled":
            # Ancestors in reverse order. Slice excludes current node.
            ancestors = nx.shortest_path(graph.reverse(), node_name, root)[1:]
            for ancestor in ancestors:
                ancestor_node = graph.nodes[ancestor]
                ancestor_status = ancestor_node.get("status", None)
                if ancestor_status == "enabled":
                    break  # Stop checking if an ancestor was enabled.
                ancestor_node["status"] = "implicitly_enabled"

    # The next step is to propagate the two weaker statuses:
    #   - 'weakly_disabled': This state is propagated to successors/descendents
    #     ensuring that those subgraphs will not be run.
    #   - 'weakly_enabled': This state propagates the 'implicitly_enabled'
    #     state to its ancestors, starting from the weakly_enabled node.
    for node_name, node in graph.nodes.items():
        status = node.get("status", None)
        if status == "weakly_disabled":
            for successor in graph.successors(node_name):
                graph.nodes[successor]["status"] = "weakly_disabled"
        elif status == "weakly_enabled":
            # Ancestors in reverse order. Slice excludes current node.
            ancestors = nx.shortest_path(graph.reverse(), node_name, root)[1:]
            for ancestor in ancestors:
                ancestor_node = graph.nodes[ancestor]
                ancestor_status = ancestor_node.get("status", None)
                if ancestor_status in ("enabled", "weakly_enabled"):
                    break  # Stop checking if an ancestor was enabled.
                ancestor_node["status"] = "implicitly_enabled"

    # The final step in the resolution is to set the status on nodes which
    # have no status and ensure that existing statuses propagate through
    # implicitly enabled nodes correctly. There is no need to consider the
    # 'disabled' or 'weakly_disabled' states as they will have been propagated
    # in the previous steps. There are two cases based on whether the node's
    # parent is:
    #   - 'enabled' or 'weakly_enabled': The descendants of this node should
    #     should be 'weakly_enabled' as their parent is in a non-implicit
    #     enabled state.
    #   - neither 'enabled' nor 'weakly_enabled': The descendants of this node
    #     should be set to the default status i.e. 'weakly_enabled' if the
    #     graph has no step/tag inclusions, otherwise 'weakly_disabled'.
    # We first ensure that the root node has a status set to ensure it doesn't
    # trigger the subsequent reasoning about parent nodes.
    root_node = graph.nodes[root]
    root_node["status"] = root_node.get("status", default_status)
    for node_name, node in graph.nodes.items():
        status = node.get("status", None)
        if status in (None, "implicitly_enabled") and node_name != root:
            # There should only be a single parent as the graph is a tree.
            parent = list(graph.predecessors(node_name))[0]
            parent_node = graph.nodes[parent]
            parent_status = parent_node.get("status")  # Must be present.

            if parent_status in ("enabled", "weakly_enabled"):
                node["status"] = "weakly_enabled"
            else:
                # Do not alter 'implicitly_enabled' nodes in this case.
                node["status"] = status or default_status

    # Finally, simplify all enabled states into a boolean enabled status.
    for node_name, node in graph.nodes.items():
        node["enabled"] = "enabled" in node["status"]  # All enabled states.


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
    """Convert a networkx.DiGraph into a stimela RunConstraints object.

    Given a networkx.DiGraph representing a recipe (and its subrecipes),
    applies a number of selection, deselection and unskip operations by
    setting states on the graph nodes. These states are ultimately used
    to determine whether each node is enabled or disabled, and whether it
    must ignore skip fields in the recipe.

    The strings provided to the various optional fields will be of the form
    {subrecipe1}.{subrecipe2}...{subrecipeN}.{tag or step or step_range}.
    Options which apply to the root recipe will be of the form
    {tag or step or step_range}. A step range is given by
    {start_step:stop_step}. Omitting either start_step or stop_step implies
    an unbounded selection which will include all steps before/after the
    stop_step/start_step.

    Args:
        graph:
            A networkx.DiGraph object representing a stimela recipe.
        tag_inclusions:
            A tuple of strings of the form {(sub)recipe}.{tag} which represent
            the tags to include in the (sub)recipe. {tag} implies the
            root recipe.
        tag_exclusions:
            A tuple of strings of the form {(sub)recipe}.{tag} which represent
            the tags to exclude in the (sub)recipe. {tag} implies the
            root recipe.
        step_inclusions:
            A tuple of strings of the form {(sub)recipe}.{step} or
            {(sub)recipe}.{start_step}:{stop_step} which represent steps to
            include in the (sub)recipe. {step} or {start_step}:{stop_step}
            apply to the root recipe.
        step_exclusions:
            A tuple of strings of the form {(sub)recipe}.{step} or
            {(sub)recipe}.{start_step}:{stop_step} which represent steps to
            exclude in the (sub)recipe. {step} or {start_step}:{stop_step}
            apply to the root recipe.
        step_unskips:
            A tuple of strings of the form {(sub)recipe}.{step} or
            {(sub)recipe}.{start_step}:{stop_step} which represent steps to
            unskip in the (sub)recipe. {step} or {start_step}:{stop_step}
            apply to the root recipe.
    """

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
    resolve_states(graph, default_status=default_status)

    return RunConstraints(graph)


class RunConstraints:
    """Simple class used for querying the state of steps in a given recipe.

    This simple class wraps an underlying networkx.DiGraph object which
    represents the state of each step in a given recipe and its subrecipes.

    Attributes:
        graph:
            A networkx.DiGraph representing a stimela recipe.
        enabled_nodes:
            Nodes of the underlying graph which have the enabled state.
        disabled_nodes:
            Nodes of the underlying graph which have the disabled state.
        unskipped_nodes:
            Nodes of the underlying graph which have the unskipped state.
    """

    def __init__(self, graph: nx.DiGraph):
        """Initialises an instance from a networkx.DiGraph."""

        self.graph = graph

        enable_states = nx.get_node_attributes(graph, "enabled")
        self.enabled_nodes = [k for k, v in enable_states.items() if v]
        self.disabled_nodes = [k for k, v in enable_states.items() if not v]

        unskipped_states = nx.get_node_attributes(graph, "unskip")
        self.unskipped_nodes = [k for k, v in unskipped_states.items() if v]

    def get_enabled_steps(self, fqname):
        """Returns a tuple of enabled steps."""
        return tuple(
            k[len(fqname) + 1:] for k in self.graph.adj[fqname].keys()
            if k in self.enabled_nodes
        )

    def get_disabled_steps(self, fqname):
        """Returns a tuple of disabled steps."""
        return tuple(
            k[len(fqname) + 1:] for k in self.graph.adj[fqname].keys()
            if k in self.disabled_nodes
        )

    def get_unskipped_steps(self, fqname):
        """Returns a tuple of unskipped steps."""
        return tuple(
            k[len(fqname) + 1:] for k in self.graph.adj[fqname].keys()
            if k in self.unskipped_nodes
        )
