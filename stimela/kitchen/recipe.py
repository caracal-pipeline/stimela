import os, os.path, re, fnmatch, copy, traceback, logging
from typing import Any, Tuple, List, Dict, Optional, Union
from dataclasses import dataclass
from omegaconf import MISSING, OmegaConf, DictConfig, ListConfig
from omegaconf.errors import OmegaConfBaseException
from collections import OrderedDict
from collections.abc import Mapping
import rich.table

from concurrent.futures import ProcessPoolExecutor, as_completed

from stimela.config import EmptyDictDefault, EmptyListDefault
import stimela
from stimela import log_exception, stimelogging
from stimela.stimelogging import log_rich_payload
from stimela.exceptions import *

from scabha.validate import evaluate_and_substitute, evaluate_and_substitute_object, Unresolved, join_quote
from scabha.substitutions import SubstitutionNS
from scabha.cargo import Parameter, Cargo, ParameterCategory
from scabha.basetypes import File, Directory, MS, UNSET, Placeholder
from .cab import Cab
from .batch import Batch
from .step import Step
from stimela import task_stats 
from stimela import backends
from stimela.backends import StimelaBackendSchema
from stimela.kitchen.utils import keys_from_sel_string


class DeferredAlias(Unresolved):
    """Class used as placeholder for deferred alias lookup (i.e. before an aliased value is available)"""
    pass


@dataclass
class ForLoopClause(object):
    # name of list variable
    var: str 
    # This should be the name of an input that provides a list, or a list
    over: Optional[Any] = None
    # If !=0 , this is a scatter not a loop -- things may be evaluated in parallel using this many workers
    # (use -1 to scatter to unlimited number of workers)
    scatter: int = 0
    # How to indicate the status of the loop on the console.
    # Default is "i/N", where i is the current index plus 1, and N is the total number of loops. 
    # A format string can be supplied instead.
    display_status: Optional[str] = None

def IterantPlaceholder(name: str):
    return name

@dataclass
class Recipe(Cargo):
    """Represents a sequence of steps.

    Additional attributes available after validation with arguments are as per for a Cab:

        self.input_output:      combined parameter dict (self.input + self.output), maps name to Parameter

    Raises:
        various classes of validation errors
    """
    steps: Dict[str, Step] = EmptyDictDefault()     # sequence of named steps

    assign: Dict[str, Any] = EmptyDictDefault()     # assigns variables

    assign_based_on: Dict[str, Any] = EmptyDictDefault()
                                                    # assigns variables based on values of other variables

    aliases: Dict[str, Any] = EmptyDictDefault()

    # overrides backend options
    backend: Optional[Dict[str, Any]] = None

    # make recipe a for_loop-gather (i.e. parallel for loop)
    for_loop: Optional[ForLoopClause] = None

    def __post_init__ (self):
        Cargo.__post_init__(self)
        # flatten aliases and assignments
        self.aliases = self.flatten_param_dict(OrderedDict(), self.aliases)
        # check that schemas are valid
        for io in self.inputs, self.outputs:
            for name, schema in io.items():
                if not schema:
                    raise RecipeValidationError(f"recipe '{self.name}': '{name}' does not define a valid schema")
        # check for repeated aliases
        for name, alias_list in self.aliases.items():
            if name in self.inputs_outputs:
                raise RecipeValidationError(f"recipe '{self.name}': alias '{name}' also appears under inputs or outputs")
            if type(alias_list) is str:
                alias_list = self.aliases[name] = [alias_list]
            if not hasattr(alias_list, '__iter__') or not all(type(x) is str for x in alias_list):
                raise RecipeValidationError(f"recipe '{self.name}': alias '{name}': name or list of names expected")
            for x in alias_list:
                if '.' not in x:
                    raise RecipeValidationError(f"recipe '{self.name}': alias '{name}': invalid target '{x}' (missing dot)")
        # instantiate steps if needed (when creating from an omegaconf)
        if type(self.steps) is not OrderedDict:
            steps = OrderedDict()
            for label, stepconfig in self.steps.items():
                stepconfig.name = label
                stepconfig.fqname = f"{self.name}.{label}"
                try:
                    step = OmegaConf.unsafe_merge(StepSchema.copy(), stepconfig)
                    steps[label] = Step(**step)
                except Exception as exc:
                    raise StepValidationError(f"recipe '{self.name}': error in definition of step '{label}'", exc)
            self.steps = steps
        # check that assignments don't clash with i/o parameters
 
        self.validate_assignments(self.assign, self.assign_based_on, self.name)

        # check that for-loop variable does not clash
        if self.for_loop:
            for io, io_label in [(self.inputs, "input"), (self.outputs, "output")]:
                if self.for_loop.var in io:
                    raise RecipeValidationError(f"recipe '{self.name}': for_loop.var={self.for_loop.var} clashes with an {io_label} parameter")
        # marked when finalized
        self._alias_map  = None
        # set of keys protected from assignment
        self._protected_from_assign = set()
        self._for_loop_values = self._for_loop_scatter = None
        # process pool used to run for-loops
        self._loop_pool = None

    def validate_assignments(self, assign, assign_based_on, location):
        # collect a list of all assignments
        assignments = OrderedDict()
        for key in assign:
            assignments[key] = "assign"
        for basevar, lookup_list in assign_based_on.items():
            if not isinstance(lookup_list, Mapping):
                raise RecipeValidationError(f"{location}.{assign_based_on}.{basevar}: mapping expected")
            # for assign_list in lookup_list.values():
            #     for key in assign_list:
            #         assignments[key] = f"assign_based_on.{basevar}"
        # # check that none clash
        # for key, assign_label in assignments.items():
        #     for io, io_label in [(self.inputs, "input"), (self.outputs, "output")]:
        #         if key in io:
        #             raise RecipeValidationError(f"'{location}.{assign_label}.{key}' clashes with an {io_label}")

    def update_assignments(self, subst: SubstitutionNS, whose = None, params: Dict[str, Any] = {}, 
                            ignore_subst_errors: bool = False):
        """Updates variable assignments, using the recipe's (or a step's) 'assign' and 'assign_based_on' sections.
        Also updates the corresponding (recipe or step's) file logger.

        Args:
            subst (SubstitutionNS): substitution namespace
            whose (Step or None): if None, use recipe's (self) assignments, else use this step's
            params (dict, optional): dictionary of parameters 
            ignore_subst_errors (bool): ignore substitution errors (default is False)

        Raises:
            AssignmentError: on errors
        """
        whose = whose or self
        # short-circuit out if nothing to do
        if not whose.assign and not whose.assign_based_on:
            return

        def flatten_dict(input_dict, output_dict={},  prefix=""):
            for name, value in input_dict.items():
                name = f"{prefix}{name}"
                if isinstance(value, (dict, OrderedDict, DictConfig)):
                    flatten_dict(value, output_dict=output_dict, prefix=f"{name}.")
                else:
                    output_dict[name] = value
            return output_dict

        # accumulate all assignments for a final round of evaluation
        assign = {}

        def do_assign(assignments):
            """Helper function to process a list of assignments. Called repeatedly
            for the assign section, and for each assign_based_on entry.
            Substitution errors are ignored at this stage, a final round of re-evaluation with ignore=False is done at the end.
            """
            # flatten assignments
            flattened = assignments # flatten_dict(assignments)
            # drop entries protected from assignment
            flattened = {name: value for name, value in flattened.items() if name not in self._protected_from_assign}
            # merge into recipe namespace
            subst.recipe._merge_(flattened)
            # perform substitutions
            try:
                flattened = evaluate_and_substitute(flattened, subst, subst.recipe, location=[whose.fqname], ignore_subst_errors=True)
            except Exception as exc:
                raise AssignmentError(f"{whose.fqname}: error evaluating assignments", exc)
            assign.update(flattened)

        # perform direct assignments
        do_assign(whose.assign)

        # add assign_based_on entries to flattened_assign
        for basevar, value_list in whose.assign_based_on.items():
            # make sure the base variable is defined
            value = None
            # it will be in subst.recipe if it was assigned, or is an input
            if basevar in subst.recipe:
                value = str(subst.recipe[basevar])
            # else it may be a for-loop index that hasn't been assigned yet -- ignore
            elif self.for_loop is not None and basevar == self.for_loop.var:
                continue
            # else it might be an input with a default, check for that
            elif basevar in self.inputs_outputs and self.inputs_outputs[basevar].default is not UNSET:
                value = str(self.inputs_outputs[basevar].default)
            # else see if it is a config setting
            elif basevar.startswith("config."):
                comps = basevar.split('.')[1:]
                try:
                    value = self.config
                    for comp in comps:
                        value = value.get(comp)
                except Exception as exc:
                    value = None
            # nothing found? error then
            if value is None:
                if basevar in self.inputs_outputs:
                    raise AssignmentError(f"{whose.fqname}.assign_based_on: a value for '{basevar}' was not supplied")
                elif '.' in basevar:
                    raise AssignmentError(f"{whose.fqname}.assign_based_on: '{basevar}' is not a known config item")
                else:
                    raise AssignmentError(f"{whose.fqname}.assign_based_on: '{basevar}' is not a known variable")
            # look up list of assignments
            if value not in value_list:
                if 'DEFAULT' not in value_list:
                    raise AssignmentError(f"{whose.fqname}.assign_based_on: neither the '{basevar}={value}' case nor a DEFAULT case is defined")
                value = 'DEFAULT'
            assignments = value_list.get(value)
            # an empty section maps to None, so skip 
            if assignments is None:
                continue
            if not isinstance(assignments, (dict, OrderedDict, DictConfig)):
                raise AssignmentError(f"{whose.fqname}.assign_based_on.{basevar}.{value}: mapping expected, got {type(assignments)} instead")
            # process the assignments
            do_assign(assignments)

        # do final round of substitutions
        try:
            assign = evaluate_and_substitute(assign, subst, subst.recipe, location=[whose.fqname], ignore_subst_errors=ignore_subst_errors)
        except Exception as exc:
            raise AssignmentError(f"{whose.fqname}: error evaluating assignments", exc)
        # dispatch and reassign, since substitutions may have been performed
        for key, value in assign.items():
            self.assign_value(key, value, subst=subst, whose=whose)

    def assign_value(self, key: str, value: Any, override: bool = False,
                     subst: Optional[Dict[str, Any]] = None, whose: Optional[Any] = None):
        """assigns a parameter value to the recipe. Handles nested assignments and 
        assignments to local log options.
        """
        # ignore protected assignments
        if key in self._protected_from_assign and not override:
            return

        if '.' in key:
            nesting, subkey = key.split('.', 1)
        else:
            nesting, subkey = None, key

        # helper function to do nested assignment of config and subst and backend
        def assign_nested(container, nested_key, value):
            comps = nested_key.split('.')
            while len(comps) > 1:
                if comps[0] not in container:
                    raise AssignmentError(f"{self.fqname}: invalid assignment {key}={value}")
                container = container[comps[0]]
                comps.pop(0)
            container[comps[0]] = value

        # assigning to input or output? Provide default            
        if key in self.inputs_outputs:
            self.log.debug(f"default params assignment: {key}={value}")
            if value is UNSET:
                if key in self.defaults:
                    del self.defaults[key]
                self.inputs_outputs[key].default = UNSET
            else:
                self.defaults[key] = value
        # assigning to a substep? Invoke nested assignment
        elif nesting is not None and nesting in self.steps:
            return self.steps[nesting].assign_value(subkey, value, override=override)
        # assigning to config?
        elif nesting == "config":
            assign_nested(self.config, subkey, value)
            if subst is not None and 'config' in subst:
                assign_nested(subst.config, subkey, value)
        # elif nesting == "backend":
        #     assign_nested(self.backend, subkey, value)
        elif nesting == "log":
            whose = whose or self
            if type(value) is Unresolved:
                self.log.debug(f"ignoring unresolved log options assignment {key}={value}")
            else:
                self.log.debug(f"log options assignment: {key}={value}")
                whose.update_log_options(**{subkey: value})
            if whose is self and subst is not None and 'recipe' in subst:
                subst.recipe.log[subkey] = value
        # in override mode, assign to assign dict for future processing
        if override:
            if value is not UNSET:
                self.assign[key] = value
            self._protected_from_assign.add(key)

    def update_log_options(self, **options):
        for setting, value in options.items():
            try:
                self.logopts[setting] = value
            except Exception as exc:
                raise AssignmentError(f"invalid {self.fqname}.log.{setting} setting", exc)
        # propagate to children
        for step in self.steps.values():
            step.update_log_options(**options)

    @property
    def finalized(self):
        return self._alias_map is not None

    def enable_step(self, label, enable=True):
        self.finalize()
        step = self.steps.get(label)
        if step is None:
            raise RecipeValidationError(f"recipe '{self.name}': unknown step {label}", log=self.log)
        if enable:
            if step._skip is True:
                self.log.warning(f"enabling step '{label}' which is normally skipped")
            elif step._skip is not False:
                self.log.warning(f"enabling step '{label}' which is normally conditionally skipped ('{step.skip}')")
            step.skip = step._skip = False
            step.skip_if_outputs = None
        else:
            self.log.warning(f"will skip step '{label}'")
            step.skip = step._skip = True

    def restrict_steps(
        self,
        tags: List[str] = [],
        skip_tags: List[str] = [],
        step_ranges: List[str] = [],
        skip_ranges: List[str] = [],
        enable_steps: List[str] = []
    ):
        try:
            # extract subsets of tags and step specifications that refer to sub-recipes
            # this will map name -> (tags, skip_tags, step_ranges, enable_steps). Name is None for the parent recipe.
            subrecipe_entries = OrderedDict()
            def process_specifier_list(specs: List[str], num=0):
                for spec in specs:
                    if '.' in spec:
                        subrecipe, spec = spec.split('.', 1)
                        if subrecipe not in self.steps or not isinstance(self.steps[subrecipe].cargo, Recipe):
                            raise StepSelectionError(f"'{subrecipe}' (in '{subrecipe}.{spec}') does not refer to a valid subrecipe")
                    else:
                        subrecipe = None
                    entry = subrecipe_entries.setdefault(subrecipe, ([],[],[],[],[]))
                    entry[num].append(spec)
            # this builds up all the entries given on the command-line
            for num, options in enumerate((tags, skip_tags, step_ranges, skip_ranges, enable_steps)):
                process_specifier_list(options, num)

            self.log.info(f"selecting recipe steps for (sub)recipe: [bold green]{self.name}[/bold green]")

            # process our own entries - the parent recipe has None key.
            tags, skip_tags, step_ranges, skip_ranges, enable_steps = subrecipe_entries.get(None, ([],[],[],[],[]))

            # Check that all specified tags (if any), exist.
            known_tags = set.union(*([v.tags for v in self.steps.values()] or [set()]))
            unknown_tags = (set(tags) | set(skip_tags)) - known_tags
            if unknown_tags:
                unknown_tags = "', '".join(unknown_tags)
                raise StepSelectionError(f"Unknown tag(s) '{unknown_tags}'")

            # We have to handle the following functionality:
            #   - user specifies specific tag(s) to run
            #   - user specifies specific tag(s) to skip
            #   - user specifies step(s) to run
            #   - user specifies step(s) to skip
            #   - ensure steps tagged with always run unless explicitly skipped
            #   - individually specified steps to run must be force enabled

            always_steps = {k for k, v in self.steps.items() if "always" in v.tags}
            never_steps = {k for k, v in self.steps.items() if "never" in v.tags}
            tag_selected_steps = {k for k, v in self.steps.items() for t in tags if t in v.tags}
            tag_skipped_steps = {k for k, v in self.steps.items() for t in skip_tags if t in v.tags}
            selected_steps = [keys_from_sel_string(self.steps, sel_string) for sel_string in step_ranges]
            skipped_steps = [keys_from_sel_string(self.steps, sel_string) for sel_string in skip_ranges]

            # Steps which are singled out are special (cherry-picked). They MUST be enabled and run.
            # NOTE: Single step slices (e.g last_step:) will also trigger this behaviour and may be
            # worth raising a warning over.
            cherry_picked_steps = set.union(*([sel for sel in selected_steps if len(sel) == 1] or [set()]))
            enable_steps.extend(list(cherry_picked_steps))

            selected_steps = set.union(*(selected_steps or [set()]))
            skipped_steps = set.union(*(skipped_steps or [set()]))

            if always_steps:
                self.log.info(f"the following step(s) are marked as always run: ({', '.join(always_steps)})")
            if never_steps:
                self.log.info(f"the following step(s) are marked as never run: ({', '.join(never_steps)})")
            if tag_selected_steps:
                self.log.info(f"the following step(s) have been selected by tag: ({', '.join(tag_selected_steps)})")
            if tag_selected_steps:
                self.log.info(f"the following step(s) have been skipped by tag: ({', '.join(tag_skipped_steps)})")
            if selected_steps:
                self.log.info(f"the following step(s) have been explicitly selected: ({', '.join(selected_steps)})")
            if skipped_steps:
                self.log.info(f"the following step(s) have been explicitly skipped: ({', '.join(skipped_steps)})")
            if cherry_picked_steps:
                self.log.info(f"the following step(s) have been cherry-picked: ({', '.join(cherry_picked_steps)})")

            # Build up the active steps according to option priority.
            active_steps = (tag_selected_steps | selected_steps) or set(self.steps.keys())
            active_steps |= always_steps
            active_steps -= tag_skipped_steps
            active_steps -= never_steps - tag_selected_steps
            active_steps -= skipped_steps
            active_steps |= cherry_picked_steps

            # Enable steps explicitly enabled by the user as well as those
            # implicitly enabled by cherry-picking above.
            for name in enable_steps:
                if name in self.steps:
                    self.enable_step(name)  # config file may have skip=True, but we force-enable here
                else:
                    raise StepSelectionError(f"'{name}' does not refer to a valid step")

            if not active_steps:
                self.log.info("no steps have been selected for execution")
                return 0
            else:
                if len(active_steps) != len(self.steps):
                    # apply skip flags 
                    for label, step in self.steps.items():
                        if label not in active_steps:
                            step.skip = step._skip = True
                            # remove auto-aliases associated with skipped steps

                # see how many steps are actually going to run
                scheduled_steps = [label for label, step in self.steps.items() if not step._skip]
                # report scheduled steps to log if (a) they're a subset or (b) any selection options were passed
                if len(scheduled_steps) != len(self.steps) or None in subrecipe_entries:
                    self.log.info(f"the following recipe steps have been selected for execution:")
                    self.log.info(f"    [bold green]{' '.join(scheduled_steps)}[/bold green]")

                # now recurse into sub-recipes. If nothing was specified for a sub-recipe,
                # we still need to recurse in to make sure it applies its tags,
                for label, step in self.steps.items():
                    if label in active_steps and isinstance(step.cargo, Recipe):
                        options = subrecipe_entries.get(label, ([],[],[],[],[]))
                        step.cargo.restrict_steps(*options)

                return len(scheduled_steps)
        except StepSelectionError as exc:
            log_exception(exc, log=self.log)
            raise exc


    def add_step(self, step: Step, label: str = None):
        """Adds a step to the recipe. Label is auto-generated if not supplied

        Args:
            step (Step): step object to add
            label (str, optional): step label, auto-generated if None
        """
        if self.finalized:
            raise DefinitionError("recipe '{self.name}': can't add a step to a recipe that's been finalized")

        names = [s for s in self.steps if s.cab == step.cabname]
        label = label or f"{step.cabname}_{len(names)+1}"
        self.steps[label] = step
        step.fqname = f"{self.name}.{label}"


    def add(self, cabname: str, label: str = None, 
            params: Optional[Dict[str, Any]] = None, info: str = None):
        """Add a step to a recipe. This will create a Step instance and call add_step() 

        Args:
            cabname (str): name of cab to use for this step
            label (str): Alphanumeric label (must start with a lette) for the step. If not given will be auto generated 'cabname_d' where d is the number of times a particular cab has been added to the recipe.
            params (Dict): A parameter dictionary
            info (str): Documentation of this step
        """
        return self.add_step(Step(cab=cabname, params=params, info=info), label=label)

    @dataclass
    class AliasInfo(object):
        label: str                      # step label
        step: Step                      # step
        param: str                      # parameter name
        io: Dict[str, Parameter]        # points to self.inputs or self.outputs
        from_recipe: bool = False       # if True, value propagates from recipe up to step
        from_step: bool = False         # if True, value propagates from step down to recipe

    def _add_alias(self, alias_name: str, alias_target: Union[str, Tuple], 
                    category: Optional[int] = None,
                    has_value=False):
        wildcards = False
        if type(alias_target) is str:
            # $$ maps to full name, and $ maps to last element of name
            alias_target = alias_target.replace("$$", alias_name)
            alias_target = alias_target.replace("$", alias_name.rsplit('.', 1)[-1])
            step_spec, step_param_name = alias_target.split('.', 1)
            # treat label as a "(cabtype)" specifier?
            if re.match(r'^\(.+\)$', step_spec):
                steps = [(label, step) for label, step in self.steps.items() 
                        if (isinstance(step.cargo, Cab) and step.cab == step_spec[1:-1]) or
                            (isinstance(step.cargo, Recipe) and step.recipe == step_spec[1:-1])]
                wildcards = True
            # treat label as a wildcard?
            elif any(ch in step_spec for ch in '*?['):
                steps = [(label, step) for label, step in self.steps.items() if fnmatch.fnmatchcase(label, step_spec)]
                wildcards = True
            # else treat label as a specific step name
            else:
                steps = [(step_spec, self.steps.get(step_spec))]
        else:
            step, step_spec, step_param_name = alias_target
            steps = [(step_spec, step)]

        for (step_label, step) in steps:
            if step is None:
                raise RecipeValidationError(f"recipe '{self.name}': alias '{alias_name}' refers to unknown step '{step_label}'", log=self.log)
            # is the alias already defined
            existing_alias = self._alias_list.get(alias_name, [None])[0]
            # find it in inputs or outputs
            input_schema = step.inputs.get(step_param_name)
            output_schema = step.outputs.get(step_param_name)
            schema = input_schema or output_schema
            # if the step was matched by a wildcard, and it doesn't have such a parameter in the schema, or else if it is
            # already explicitly specified, then we don't alias it 
            if wildcards and (schema is None or step_param_name in step.params):
                continue                    
            # no a wildcard, but parameter not defined? This is an error
            if schema is None:
                raise RecipeValidationError(f"recipe '{self.name}': alias '{alias_name}' refers to unknown step parameter '{step_label}.{step_param_name}'", log=self.log)
            # implicit inputs cannot be aliased
            if input_schema and input_schema.implicit:
                raise RecipeValidationError(f"recipe '{self.name}': alias '{alias_name}' refers to implicit input '{step_label}.{step_param_name}'", log=self.log)
            # if alias is already defined, check for conflicts
            if existing_alias is not None:
                io = existing_alias.io
                if io is self.outputs:
                    raise RecipeValidationError(f"recipe '{self.name}': output alias '{alias_name}' is defined more than once", log=self.log)
                elif output_schema:
                    raise RecipeValidationError(f"recipe '{self.name}': alias '{alias_name}' refers to both an input and an output", log=self.log)
                alias_schema = io[alias_name] 
                # now we know it's a multiply-defined input, check for type consistency
                if alias_schema.dtype != schema.dtype:
                    raise RecipeValidationError(f"recipe '{self.name}': alias '{alias_name}': dtype {schema.dtype} of '{step_label}.{step_param_name}' doesn't match previous dtype {alias_schema.dtype}", log=self.log)
                orig_schema = self._orig_alias_schema[alias_name]
            # else alias not yet defined, insert a schema
            else:
                # get recipe's original schema for the parameter
                io = self.inputs if input_schema else self.outputs
                # if we have a schema defined for the alias, some params must be inherited from it 
                orig_schema = io.get(alias_name)
                self._orig_alias_schema[alias_name] = orig_schema
                # define schema based on copy of the target, but preserve default
                io[alias_name] = copy.copy(schema)
                alias_schema = io[alias_name] 
                # if default set in recipe schema, ignore any parameter setting in the step 
                if orig_schema is not None and orig_schema.default is not UNSET:
                    if step_param_name in step.params:
                        del step.params[step_param_name]
                    alias_schema.default = orig_schema.default
                # else check if explicit value or a default is specified in the step -- make it the recipe default
                else:
                    if step_param_name in step.params:
                        defval = step.params[step_param_name]
                    elif step_param_name in step.cargo.defaults:
                        defval = step.cargo.defaults[step_param_name]
                    else:
                        defval = schema.default
                    if defval is not UNSET:
                        alias_schema.required = False
                        alias_schema.default = defval
                        ## see https://github.com/caracal-pipeline/stimela/issues/284. No longer convinced
                        ## these parameters should be marked as Hidden. After all, the recipe explicitly specifies them!
                        ## mark it as hidden -- no need to expose parameters that are internally set this way
                        # alias_schema.category = ParameterCategory.Hidden
                # propagate info from recipe schema
                if orig_schema and orig_schema.info:
                    alias_schema.info = orig_schema.info
                # required flag overrides, if set from our own schema
                if orig_schema is not None and orig_schema.required is not None:
                    alias_schema.required = orig_schema.required
                # category is set by argument, else from own schema, else from target
                if category is not None:
                    alias_schema.category = category
                elif orig_schema is not None and orig_schema.category is not None:
                    alias_schema.category = orig_schema.category

            # if step parameter is implicit, mark the alias as implicit. Note that this only applies to outputs
            if schema.implicit:
                alias_schema.implicit = Unresolved(f"{step_label}.{step_param_name}")   # will be resolved when propagated from step
                self._implicit_params.add(alias_name)

            # this is True if the step's parameter is defined in any way (set, default, or implicit)
            have_step_param = step_param_name in step.params or step_param_name in step.cargo.defaults or \
                alias_schema.default is not UNSET or alias_schema.implicit is not None

            # if the step parameter is set and ours isn't, mark our schema as having a default
            if have_step_param and alias_schema.default is UNSET:
                alias_schema.default = DeferredAlias(f"{step_label}.{step_param_name}")

            # alias becomes required if any step parameter it refers to is required and not set, unless
            # the original schema forces a required value
            if schema.required and not have_step_param and (orig_schema is None or orig_schema.required is None):
                alias_schema.required = True

            self._alias_map[step_label, step_param_name] = alias_name, orig_schema
            self._alias_list.setdefault(alias_name, []).append(Recipe.AliasInfo(step_label, step, step_param_name, io))

    def finalize(self, config=None, log=None, name=None, fqname=None, backend=None, nesting=0):
        if not self.finalized:
            config = config or stimela.CONFIG

            backend = OmegaConf.merge(backend or config.opts.backend, self.backend or {})

            # fully qualified name, i.e. recipe_name.step_name.step_name etc.
            self.fqname = fqname = fqname or self.fqname or self.name

            # if logger is not provided, then init one
            if log is None:
                log = stimela.logger().getChild(self.fqname)
                log.propagate = True

            # init and/or update logger options
            self.logopts = config.opts.log.copy()

            # update file logger
            logsubst = SubstitutionNS(config=config, info=dict(fqname=fqname, taskname=fqname))
            stimelogging.update_file_logger(log, self.logopts, nesting=nesting, subst=logsubst, location=[self.fqname])

            # call Cargo's finalize method
            super().finalize(config, log=log, fqname=fqname, nesting=nesting)

            # finalize steps
            for label, step in self.steps.items():
                step_log = log.getChild(label)
                step_log.propagate = False
                try:
                    step.finalize(config, log=step_log, fqname=f"{fqname}.{label}", backend=backend, nesting=nesting+1)
                    # check that per-step assignments don't clash with i/o parameters
                    step.assign = self.flatten_param_dict(OrderedDict(), step.assign)
                    self.validate_assignments(step.assign, step.assign_based_on, f"{fqname}.{label}")
                except Exception as exc:
                    raise StepValidationError(f"error validating step '{label}'", exc, 
                                tb=not isinstance(exc, ScabhaBaseException))

            # collect aliases
            self._alias_map = OrderedDict()
            self._alias_list = OrderedDict()
            self._orig_alias_schema = OrderedDict()

            # collect from inputs and outputs
            for io in self.inputs, self.outputs:
                for name, schema in io.items():
                    if schema.aliases:
                        ## NB skip this check, allow aliases to override
                        # if schema.dtype != "str" or schema.choices or schema.writable:
                        #     raise RecipeValidationError(f"recipe '{self.name}': alias '{name}' should not specify type, choices or writability", log=log)
                        for alias_target in schema.aliases:
                            self._add_alias(name, alias_target)

            # collect from aliases section
            for name, alias_list in self.aliases.items():
                for alias_target in alias_list:
                    self._add_alias(name, alias_target)

            # automatically make aliases for step parameters that are unset, and don't have a default, and aren't implict 
            for label, step in self.steps.items():
                for name, schema in step.inputs_outputs.items():
                    # does it have a value set
                    has_value = name in step.params or name in step.cargo.defaults or \
                                schema.default is not UNSET 
                    if (label, name) not in self._alias_map and not schema.implicit and not has_value:
                        auto_name = f"{label}.{name}"
                        if auto_name in self.inputs or auto_name in self.outputs:
                            raise RecipeValidationError(f"recipe '{self.name}': auto-generated parameter name '{auto_name}' conflicts with another name. Please define an explicit alias for this.", log=log)
                        self._add_alias(auto_name, (step, label, name), 
                                        category=ParameterCategory.Required if schema.required and not has_value  
                                        else ParameterCategory.Obscure)

            # these will be re-merged when needed again
            self._inputs_outputs = None

            # check that for-loop is valid, if defined
            if self.for_loop is not None:
                # if for_loop.over is a str, treat it as a required input
                if type(self.for_loop.over) is str:
                    if self.for_loop.over not in self.inputs:
                        raise RecipeValidationError(f"recipe '{self.name}': for_loop.over={self.for_loop.over} is not a defined input", log=log)
                    # this becomes a required input
                    self.inputs[self.for_loop.over].required = True
                # else treat it as a list of values to be iterated over (and set over=None to indicate this)
                elif type(self.for_loop.over) in (list, tuple, ListConfig):
                    self._for_loop_values = list(self.for_loop.over)
                    self.for_loop.over = None
                else:
                    raise RecipeValidationError(f"recipe '{self.name}': for_loop.over is of invalid type {type(self.for_loop.over)}", log=log)

                # # insert empty loop variable
                # if self.for_loop.var not in self.assign:
                #     self.assign[self.for_loop.var] = ""

    def _prep_step(self, label, step, subst):
        parts = label.split("-")
        info = subst.info
        info.fqname = f"{self.fqname}.{label}"
        info.label = label 
        info.label_parts = parts
        info.suffix = parts[-1] if len(parts) > 1 else ''
        subst.current = step.params
        subst.steps[label] = subst.current

    def _preprocess_parameters(self, params: Dict[str, Any]):
        # split parameters into our own, and per-step, and UNSET directives
        own_params = {}
        unset_params = set()
        for name, value in params.items():
            # if self.for_loop is not None and name == self.for_loop.var:
            #     continue # unset_params.add(name)
            if name in self.inputs_outputs:
                # if value == "UNSET":
                #     unset_params.add(name)
                # elif value == "EMPTY":
                #     own_params[name] = ""
                if value is UNSET: 
                    unset_params.add(name)
                else:
                    own_params[name] = value
            elif '.' not in name: 
                raise ParameterValidationError(f"'{name}' does not refer to a known parameter")
            else:
                label, subname = name.split('.', 1)
                substep = self.steps.get(label)
                if substep is None:
                    raise ParameterValidationError(f"'{name}' does not refer to a known parameter or a step")
                substep.params[subname] = value
        return own_params, unset_params


    def prevalidate(self, params: Dict[str, Any], subst: Optional[SubstitutionNS]=None, backend=None, root=False):
        self.finalize(backend=backend)
        self.log.debug("prevalidating recipe")
        errors = []

        backend = OmegaConf.merge(backend or self.config.opts.backend, self.backend or {})

        # split parameters into our own, and per-step, and UNSET directives
        params,  unset_params = self._preprocess_parameters(params)

        subst_outer = subst  # outer dictionary is used to prevalidate our parameters

        subst = SubstitutionNS()
        info = SubstitutionNS(fqname=self.fqname, taskname=self.fqname, label='', label_parts=[], suffix='')
        # mutable=False means these sub-namespaces are not subject to {}-substitutions
        subst._add_('info', info, nosubst=True)
        subst._add_('self', info, nosubst=True)
        subst._add_('config', self.config, nosubst=True) 
        subst._add_('steps', {}, nosubst=True)
        subst._add_('previous', {}, nosubst=True)
        subst.recipe = SubstitutionNS(**params)
        subst.recipe.log = self.logopts
        if root:
            subst.root = subst.recipe

        if subst_outer is not None:
            if 'root' in subst_outer:
                subst._add_('root', subst_outer.root, nosubst=True)
            if 'recipe' in subst_outer:
                subst._add_('parent', subst_outer.recipe, nosubst=True)
        else:
            subst_outer = SubstitutionNS()
            info1 = info.copy()
            subst_outer._add_('info', info1, nosubst=True)
            subst_outer._add_('self', info1, nosubst=True)
            subst_outer._add_('config', self.config, nosubst=True) 
            subst_outer.current = subst.recipe

        # update assignments
        self.update_assignments(subst, params=params, ignore_subst_errors=True)
        # this may have changed the file logger, so update
        stimelogging.update_file_logger(self.log, self.logopts, nesting=self.nesting, subst=subst, location=[self.fqname])

        # add for-loop variable to inputs, if expected there
        if self.for_loop is not None and self.for_loop.var in self.inputs:
            params[self.for_loop.var] = Placeholder(self.for_loop.var)

        # prevalidate our own parameters. This substitutes in defaults and does {}-substitutions
        # we call this twice, potentially, so define as a function
        def prevalidate_self(params):
            try:
                params1 = Cargo.prevalidate(self, params, subst=subst_outer, backend=backend)
                # mark params that have become unset 
                unset_params.update(set(params) - set(params1))
                params = params1
                # validate for-loop, if needed
                self.validate_for_loop(params, strict=False)

            except ScabhaBaseException as exc:
                errors.append(exc)
            except Exception as exc:
                errors.append(RecipeValidationError("recipe failed prevalidation", exc, tb=True))

            # merge again, since values may have changed
            subst.recipe._merge_(params)
            return params

        params = prevalidate_self(params)

        # propagate alias values up to substeps, except for implicit values (these only ever propagate down to us)
        for name, aliases in self._alias_list.items():
            if name in params and type(params[name]) is not DeferredAlias and name not in self._implicit_params:
                for alias in aliases:
                    alias.from_recipe = True
                    alias.step.update_parameter(alias.param, params[name])
            elif name in unset_params:
                for alias in aliases:
                    alias.from_recipe = True
                    alias.step.unset_parameter(alias.param)

        # prevalidate step parameters 
        # we call this twice, potentially, so define as a function

        def prevalidate_steps():
            for label, step in self.steps.items():
                self._prep_step(label, step, subst)
                # update assignments, since substitutions (info.fqname and such) may have changed
                self.update_assignments(subst, params=params, ignore_subst_errors=True)
                # update assignments based on step content
                self.update_assignments(subst, whose=step, params=params, ignore_subst_errors=True)

                try:
                    step_params = step.prevalidate(subst)
                    subst.current._merge_(step_params)
                except ScabhaBaseException as exc:
                    errors.append(RecipeValidationError(f"step '{label}' failed prevalidation", exc))
                except Exception as exc:
                    errors.append(RecipeValidationError(f"step '{label}' failed prevalidation", exc, tb=True))

                # revert to recipe-level assignments
                self.update_assignments(subst, params=params, ignore_subst_errors=True)
                subst.previous = subst.current
                subst.steps[label] = subst.previous

        prevalidate_steps()

        # now check for aliases that need to be propagated up/down
        if not errors:
            revalidate_self = revalidate_steps = False
            for name, aliases in self._alias_list.items():
                # propagate up if alias is not set, or it is implicit=Unresolved (meaning it gets set from an implicit substep parameter)
                if name not in params or type(params[name]) is DeferredAlias or type(self.inputs_outputs[name].implicit) is Unresolved:
                    from_step = False
                    for alias in aliases:
                        # if alias is set in step but not with us, mark it as propagating down
                        if alias.param in alias.step.validated_params:
                            alias.from_step = from_step = revalidate_self = True
                            params[name] = alias.step.validated_params[alias.param]
                            # and break out, we do this for the first matching step only
                            break
                    # if we propagated an input value down from a step, check if we need to propagate it up to any other steps
                    # note that this only ever applies to inputs
                    if from_step:
                        for alias in aliases:
                            if not alias.from_step:
                                alias.from_recipe = revalidate_steps = True
                                alias.step.update_parameter(alias.param, params[name])

            # do we or any steps need to be revalidated?
            if revalidate_self:
                params = prevalidate_self(params)
            if revalidate_steps:
                prevalidate_steps()

        # check for missing parameters
        missing_params = [name for name, schema in self.inputs_outputs.items() if schema.required and name not in params]
        if missing_params:
            n = len(missing_params)
            msg = f"""recipe is missing {n} required parameter{'s' if n>1 else ''}:"""
            errors.append(RecipeValidationError(msg, missing_params))

        if errors:
            if len(errors) == 1:
                raise errors[0]
            else:
                raise RecipeValidationError(f"recipe '{self.name}': {len(errors)} errors", errors)

        self._prevalidated_steps = subst.steps

        self.log.debug("recipe pre-validated")

        return params

    def validate_for_loop(self, params, strict=False):
        # in case of for loops, get list of values to be iterated over 
        if self.for_loop is not None:
            # get scatter value
            if 'for_loop.scatter' in params:
                scatter = params['for_loop.scatter']
            elif 'for_loop.over' in self.assign:
                scatter = self.assign['for_loop.scatter']
            else:
                scatter = self.for_loop.scatter
            if type(scatter) is bool:
                scatter = -1 if scatter else 0
            elif type(scatter) is not int:
                raise ParameterValidationError(f"for_loop.scattter={scatter}: bool or int expected")
            self._for_loop_scatter = scatter

            # the over list can be in the for_loop clause, or in inputs
            if 'for_loop.over' in params:
                values = params['for_loop.over']
            elif 'for_loop.over' in self.assign:
                values = self.assign['for_loop.over']
            elif self.for_loop.over is not None:
                # check that it's legal
                if self.for_loop.over in self.assign:
                    values = self.assign[self.for_loop.over]
                elif self.for_loop.over in params:
                    values = params[self.for_loop.over]
                elif self.for_loop.over not in self.inputs:
                    raise ParameterValidationError(f"recipe '{self.name}': for_loop.over={self.for_loop.over} does not refer to a known parameter")
                else:
                    raise ParameterValidationError(f"recipe '{self.name}': for_loop.over={self.for_loop.over} is unset")
                if strict and isinstance(values, Unresolved):
                    raise ParameterValidationError(f"recipe '{self.name}': for_loop.over={self.for_loop.over} is unresolved", [values])
            else:
                if self._for_loop_values is None:
                    raise ParameterValidationError(f"recipe '{self.name}': for_loop.over is unset")
                values = self._for_loop_values 
            # finalize list of values
            if type(values) is ListConfig:
                values = list(values)
            elif not isinstance(values, (list, tuple)):
                values = [values]
            if self._for_loop_values is None:
                self.log.debug(f"recipe is a for-loop with '{self.for_loop.var}' iterating over {len(values)} values")
                self.log.debug(f"loop values: {values}")
            self._for_loop_values = values
        # else fake a single-value list
        else:
            self._for_loop_values = [None]

    def validate_inputs(self, params: Dict[str, Any], subst: Optional[SubstitutionNS]=None, loosely=False, remote_fs=False):

        params, _ = self._preprocess_parameters(params)

        if subst is None:
            subst = SubstitutionNS()
            info = SubstitutionNS(fqname=self.fqname)
            subst._add_('info', info, nosubst=True)
            subst._add_('self', info, nosubst=True)
            subst._add_('config', self.config, nosubst=True) 

            subst.recipe = SubstitutionNS(**params)
            subst.current = subst.recipe

        if 'current' in subst:
            subst.current._add_('steps', self._prevalidated_steps, nosubst=True)
        
        self.update_assignments(subst, params=params, ignore_subst_errors=True)

        params = Cargo.validate_inputs(self, params, subst=subst, loosely=loosely, remote_fs=remote_fs)

        self.validate_for_loop(params, strict=True)

        # # in case of a for-loop, assign first iterant
        # if self.for_loop is not None:
        #     if self.for_loop.var in self.inputs:
        #         params[self.for_loop.var] = IterantPlaceholder(self.for_loop.var)
        #     else:
        #         self.assign[self.for_loop.var] = IterantPlaceholder(self.for_loop.var)

        return params

    ## NB: OMS: is this really used or needed anywhere?
    # def _link_steps(self):
    #     """
    #     Adds  next_step and previous_step attributes to the recipe. 
    #     """
    #     steps = list(self.steps.values())
    #     N = len(steps)
    #     # Nothing to link if only one step
    #     if N == 1:
    #         return

    #     for i in range(N):
    #         step = steps[i]
    #         if i == 0:
    #             step.next_step = steps[1]
    #             step.previous_step = None
    #         elif i > 0 and i < N-2:
    #             step.next_step = steps[i+1]
    #             step.previous_step = steps[i-1]
    #         elif i == N-1:
    #             step.next_step = None
    #             step.previous_step = steps[i-2]

    def summary(self, params: Dict[str, Any], recursive=True, ignore_missing=False):
        """Returns list of lines with a summary of the recipe state
        """
        lines = [f"recipe '{self.name}':"] + Cargo.add_parameter_summary(params)
        if not ignore_missing:
            lines += [f"  {name} = ???" for name in self.inputs_outputs if name not in params]
        if recursive:
            lines.append("  steps:")
            for name, step in self.steps.items():
                stepsum = step.summary()
                lines.append(f"    {name}: {stepsum[0]}")
                lines += [f"    {x}" for x in stepsum[1:]]
        return lines

    _root_recipe_ns = None

    def rich_help(self, tree, max_category=ParameterCategory.Optional):
        Cargo.rich_help(self, tree, max_category=max_category)
        if self.for_loop:
            loop_tree = tree.add("For loop:")
            if self._for_loop_values is not None:
                over = f"{len(self._for_loop_values)} values"
            else:
                over = f"[bold]{self.for_loop.over}[/bold]"
            loop_tree.add(f"iterating [bold]{self.for_loop.var}[/bold] over {over}")
        if self.steps:
            have_skips = any(step._skip for step in self.steps.values())
            steps_tree = tree.add(f"Steps (note [italic]some steps[/italic] are skipped by default):" 
                                if have_skips else "Steps:")
            table = rich.table.Table.grid("", "", "", padding=(0,2)) # , show_header=False, show_lines=False, box=rich.box.SIMPLE)
            steps_tree.add(table)            
            for label, step in self.steps.items():
                style = "italic" if step._skip else "bold"
                table.add_row(f"[{style}]{label}[/{style}]", step.info)
                if step.tags:
                    table.add_row("", f"[italic](tags: {', '.join(step.tags)})[/italic]")
        else:
            steps_tree = tree.add("No recipe steps defined")


    def _update_aliases(self, name: str, value: Any):
        """Propagates recipe aliases up top parameters

        Args:
            name (str): name of recipe parameter
            value (Any): value
        """
        for alias in self._alias_list.get(name, []):
            if alias.from_recipe:
                alias.step.update_parameter(alias.param, value)


    def _iterate_loop_worker(self, params, subst, backend_settings, count, iter_var, subprocess=False, raise_exc=True):
        """"
        Needed for concurrency
        """
        # close progress bar in subprocesses
        if subprocess:
            task_stats.add_subprocess_id(count)
            task_stats.destroy_progress_bar()
        subst.info.subprocess = task_stats.get_subprocess_id()
        taskname = subst.info.taskname
        outputs = {}
        exception = tb = None
        task_attrs, task_kwattrs = (), {}
        try:
            # if for-loop, assign new value
            if self.for_loop:
                self.log.info(f"for loop iteration {count}: {self.for_loop.var} = {iter_var}")
                print(f"for loop iteration {count}: {self.for_loop.var} = {iter_var}")
                if self.for_loop.var in self.inputs_outputs:
                    params[self.for_loop.var] = iter_var
                else:
                    self.assign[self.for_loop.var] = iter_var
                # update variable index
                self.assign[f"{self.for_loop.var}@index"] = count
                # update alias
                self._update_aliases(self.for_loop.var, iter_var)
                # update status display
                status = None
                status_dict = dict(index0=count, 
                            index1=count+1, total=len(self._for_loop_values),
                            var=self.for_loop.var, value=iter_var)
                if self.for_loop.display_status:
                    try:
                        status = self.for_loop.display_status.format(**status_dict)
                    except Exception as exc:
                        self.log.warning(f"error formatting for-loop status: {exc}, falling back on default status display")
                if status is None:
                    status = "{index1}/{total}".format(**status_dict)
                task_stats.declare_subtask_status(status)
                taskname = f"{taskname}.{count}"
                subst.info.taskname = taskname 
                # task_stats.declare_subtask_attributes(count)
                # task_attrs = (count,)
                context = task_stats.declare_subtask(f"({count})")
            else:
                from contextlib import nullcontext
                context = nullcontext()
            with context: 
                for label, step in self.steps.items():
                    # update step info
                    self._prep_step(label, step, subst)
                    subst.info.taskname = f"{taskname}.{label}"
                    # reevaluate recipe level assignments (info.fqname etc. have changed)
                    self.update_assignments(subst, params=params)
                    # evaluate step-level assignments
                    self.update_assignments(subst, whose=step, params=params)
                    # step logger may have changed
                    stimelogging.update_file_logger(step.log, step.logopts, nesting=step.nesting, subst=subst, location=[step.fqname])
                    # set our info back temporarily to update log assignments

                    ## OMS: note to self, I had this here but not sure why. Seems like a no-op. Something with logname fiddling.
                    ## Leave as a puzzle to future self for a bit. Remove info from args.
                    # info_step = subst.info
                    # subst.info = info.copy()
                    # subst.info = info_step

                    if step.skip is True:
                        self.log.debug(f"step '{label}' will be explicitly skipped")
                    else:
                        self.log.info(f"processing step '{label}'")
                        if step.info:
                            self.log.info(f"  ({step.info})", extra=dict(color="GREEN", boldface=True))
                    try:
                        #step_params = step.run(subst=subst.copy(), batch=batch)  # make a copy of the subst dict since recipe might modify
                        step_params = step.run(backend=backend_settings, subst=subst.copy(), parent_log=self.log)  # make a copy of the subst dict since recipe might modify
                    except ScabhaBaseException as exc:
                        newexc = StimelaStepExecutionError(f"step '{step.fqname}' has failed, aborting the recipe", exc)
                        if not exc.logged:
                            log_exception(newexc, log=step.log)
                        raise newexc

                    # put step parameters into previous and steps[label] again, as they may have changed based on outputs)
                    subst.previous = step_params
                    subst.steps[label] = subst.previous
                    # revert to recipe level assignments

                    # now check for output aliases that need to be propagated down from steps
                    self.update_assignments(subst, whose=self, params=params)
                    for name, aliases in self._alias_list.items():
                        for alias in aliases:
                            if alias.from_step and alias.step is step:
                                # if step was skipped, mark output as not required
                                if alias.step._skip:
                                    self.outputs[name].required = False
                                # if step output is validated, add it to our output 
                                # if alias.param in alias.step.validated_params:
                                #     outputs[name] = alias.step.validated_params[alias.param]
                                if alias.param in step_params:
                                    outputs[name] = step_params[alias.param]

        except Exception as exc:
            # raise exception up if asked to
            if raise_exc:
                raise
            # else will be returned
            exception = exc
            tb = FormattedTraceback(sys.exc_info()[2])

        return task_attrs, task_kwattrs, task_stats.collect_stats(), outputs, exception, tb

    def build(self, backend={}, rebuild=False, build_skips=False, log: Optional[logging.Logger] = None):
        # set up backend
        backend = OmegaConf.merge(backend, self.backend or {})
        # build recursively
        log = log or self.log
        log.info(f"building image(s) for recipe '{self.fqname}'")
        for step in self.steps.values():
            step.build(backend, rebuild=rebuild, build_skips=build_skips, log=log)


    def _run(self, params: Dict[str, Any], subst: Optional[Dict[str, Any]] = None, backend: Dict = {}) -> Dict[str, Any]:
        """Internal method for running a recipe. Meant to be called from the containing step.

        Args:
            params (Dict[str, Any]): input parameters
            subst (Dict[str, Any], optional): Substitution namespace. Defaults to None.
            backend (Dict, optional): Extra backend settings from parent. Defaults to {}.

        Returns:
            Dict[str, Any]: Dictionary of outputs
        """
        # set up backend
        backend = OmegaConf.merge(backend, self.backend or {})

        # set up substitution namespace
        subst_outer = subst
        if subst is None:
            subst = SubstitutionNS()
            taskname = self.name
        else:
            taskname = subst.info.taskname

        info = SubstitutionNS(fqname=self.fqname, label='', label_parts=[], suffix='', taskname=taskname)
        # nosubst=True means these sub-namespaces are not subject to {}-substitutions
        info1 = info.copy()
        subst._add_('info', info1, nosubst=True)
        subst._add_('self', info1, nosubst=True)
        subst._add_('config', self.config, nosubst=True)
        subst._add_('steps', {}, nosubst=True)
        subst._add_('previous', {}, nosubst=True)
        subst._add_('current', {}, nosubst=True)
            
        subst.recipe = SubstitutionNS(**params)
        subst.recipe.log = self.logopts
        subst.recipe._add_('steps', subst.steps, nosubst=True)

        if subst_outer is not None:
            if 'root' in subst_outer:
                subst._add_('root', subst_outer.root, nosubst=True)
            if 'recipe' in subst_outer:
                subst._add_('parent', subst_outer.recipe, nosubst=True)
        else:
            subst.root = subst.recipe

        subst_copy = subst.copy()
        self.update_assignments(subst, params=params, ignore_subst_errors=True)

        # init backends if not already done
        if not backends.initialized:
            try:
                backend_opts = OmegaConf.merge(stimela.CONFIG.opts.backend, backend)
                backend_opts = evaluate_and_substitute_object(backend_opts, subst, 
                                                              recursion_level=-1, location=[self.fqname, "backend"])
                if getattr(backend_opts, 'verbose', 0):
                    opts_yaml = OmegaConf.to_yaml(backend_opts)
                    log_rich_payload(self.log, "initial backend settings are", opts_yaml, syntax="yaml") 
                backend_opts = OmegaConf.to_object(OmegaConf.merge(StimelaBackendSchema, backend_opts))
            except Exception as exc:
                newexc = BackendError("error validating backend settings", exc)
                raise newexc from None
            
            stimela.backends.init_backends(backend_opts, stimela.logger())

        try:
            self.log.info(f"running recipe '{self.name}'")

            # our inputs have been validated, so propagate aliases to steps. Check for missing stuff just in case
            for name, schema in self.inputs.items():
                if name in params:
                    value = params[name]
                    if isinstance(value, Unresolved) and not isinstance(value, Placeholder):
                        raise RecipeValidationError(f"recipe '{self.name}' has unresolved input '{name}'", log=self.log)
                    self._update_aliases(name, value)
                elif schema.required and (self.for_loop is None or name != self.for_loop.var): 
                        raise RecipeValidationError(f"recipe '{self.name}' is missing required input '{name}'", log=self.log)

            # form list of arguments for each invocation of the loop worker
            loop_worker_args = []
            for count, iter_var in enumerate(self._for_loop_values):
                loop_worker_args.append((params, subst, backend, count, iter_var))

            # if scatter is enabled, use a process pool
            if self._for_loop_scatter:
                nloop = len(loop_worker_args)
                if self._for_loop_scatter < 0:
                    num_workers = nloop
                else:
                    num_workers = min(self._for_loop_scatter, nloop) 
                inital_task_status = f"0/{nloop} complete, {num_workers} workers"
                task_stats.declare_subtask_status(inital_task_status)
                with ProcessPoolExecutor(num_workers) as pool:
                    # submit each iterant to pool
                    futures = [pool.submit(self._iterate_loop_worker, *args, subprocess=True, raise_exc=False) for args in loop_worker_args]
                    # update task stats, since they're recorded independently within each step, as well
                    # as get any exceptions from the nesting
                    errors = []
                    nfail = ncomplete = 0
                    for f in as_completed(futures):
                        attrs, kwattrs, stats, outputs, exc, tb = f.result()
                        task_stats.declare_subtask_attributes(*attrs, **kwattrs)
                        task_stats.add_missing_stats(stats)
                        if exc is not None:
                            errors.append(exc)
                            if not isinstance(exc, ScabhaBaseException):
                                errors.append(tb)
                            nfail += 1
                        else:
                            ncomplete += 1
                        if ncomplete:
                            status = f"[green]{ncomplete}[/green]/{nloop} complete"
                        else:
                            status = f"0/{nloop} complete"
                        if nfail:
                            status = f"{status}, [red]{nfail}[/red] failed"
                        status = f"{status}, {num_workers} workers"
                        task_stats.declare_subtask_status(status)
                    if errors:
                        pool.shutdown()
                        raise StimelaRuntimeError(f"{nfail}/{nloop} jobs have failed", errors)
                # drop a rendering of the progress bar onto the console, to overwrite previous garbage if it's there
                task_stats.restate_progress()
            # else just iterate directly
            else:
                for args in loop_worker_args:
                    _, _, _, outputs, _, _ = self._iterate_loop_worker(*args, raise_exc=True) 
            
            # either way, outputs contains output aliases from the last iteration
            params.update(**outputs)

            # current namespace becomes recipe again
            subst.current = subst.recipe
            
            self.log.info(f"recipe '{self.name}' executed successfully")
            return OrderedDict((name, value) for name, value in params.items() if name in self.outputs)
        finally:
            steps = subst.steps
            subst.update(subst_copy)
            subst.current.steps = steps

StepSchema = OmegaConf.structured(Step)
RecipeSchema = OmegaConf.structured(Recipe)

