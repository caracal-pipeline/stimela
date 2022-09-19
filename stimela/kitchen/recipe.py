from cmath import exp
from multiprocessing import cpu_count
import os, os.path, re, logging, fnmatch, copy, time
from typing import Any, Tuple, List, Dict, Optional, Union
from dataclasses import dataclass
from omegaconf import MISSING, OmegaConf, DictConfig, ListConfig
from collections import OrderedDict
from collections.abc import Mapping
import rich.table

from pathos.pools import ProcessPool
from pathos.serial import SerialPool
from multiprocessing import cpu_count
from stimela.config import EmptyDictDefault, EmptyListDefault
import stimela
from stimela import log_exception, stimelogging
from stimela.exceptions import *
from scabha.exceptions import SubstitutionError, SubstitutionErrorList
from scabha.validate import evaluate_and_substitute, Unresolved, join_quote
from scabha.substitutions import SubstitutionNS, substitutions_from 
from scabha.cargo import Parameter, Cargo, ParameterCategory
from scabha.types import File, Directory, MS, UNSET
from .cab import Cab
from .batch import Batch
from .step import Step, resolve_dotted_reference


class DeferredAlias(Unresolved):
    """Class used as placeholder for deferred alias lookup (i.e. before an aliased value is available)"""
    pass


@dataclass
class ForLoopClause(object):
    # name of list variable
    var: str 
    # This should be the name of an input that provides a list, or a list
    over: Any
    # If True, this is a scatter not a loop -- things may be evaluated in parallel
    scatter: bool = False




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

    # make recipe a for_loop-gather (i.e. parallel for loop)
    for_loop: Optional[ForLoopClause] = None

    # logging control, overrides opts.log.init_logname and opts.log.logname 
    init_logname: Optional[str] = None
    logname: Optional[str] = None
    batch: Optional[Batch] = None
    
    # # if not None, do a while loop with the conditional
    # _while: Conditional = None
    # # if not None, do an until loop with the conditional
    # _until: Conditional = None

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
        self._for_loop_values = None

    def protect_from_assignments(self, keys):
        self._protected_from_assign.update(keys)
        #self.log.debug(f"protected from assignment: {self._protected_from_assign}")

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
            flattened = flatten_dict(assignments)
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
            # else it might be an input with a default, check for that
            elif basevar in self.inputs_outputs and self.inputs_outputs[basevar].default is not UNSET:
                value = str(self.inputs_outputs[basevar].default)
            # else see if it is a config setting
            else:
                comps = basevar.split('.')
                if len(comps) > 1 and comps[0] in self.config:
                    try:
                        value = self.config
                        for comp in comps:
                            value = value.get(comp)
                    except Exception as exc:
                        value = None
            # nothing found? error then
            if value is None:
                raise AssignmentError(f"{whose.fqname}.assign_based_on.{basevar} is not a known input or variable or config item")
            # look up list of assignments
            if value not in value_list:
                if 'DEFAULT' not in value_list:
                    raise AssignmentError(f"{whose.fqname}.assign_based_on.{basevar}: unknown value '{value}', and no default defined")
                value = 'DEFAULT'
            assignments = value_list.get(value)
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
            subst.recipe[key] = value
            if key in self.inputs_outputs:
                self.log.debug(f"default params assignment: {key}={value}")
                self.defaults[key] = value
            elif key.startswith("log."):
                if type(value) is Unresolved:
                    self.log.debug(f"ignoring unresolved log options assignment {key}={value}")
                else:
                    self.log.debug(f"log options assignment: {key}={value}")
                    _, setting = key.split('.')
                    whose.update_log_options(**{setting: value})
            else:
                self.log.debug(f"variable assignment: {key}={value}")

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
        else:
            self.log.warning(f"will skip step '{label}'")
            step.skip = step._skip = True

    def restrict_steps(self, steps: List[str], force_enable=True):
        self.finalize()
        # check for unknown steps
        restrict_steps = set(steps)
        unknown_steps = restrict_steps.difference(self.steps)
        if unknown_steps:
            raise RecipeValidationError(f"recipe '{self.name}': unknown step(s) {join_quote(unknown_steps)}", log=self.log)

        # apply skip flags 
        for label, step in self.steps.items():
            if label not in restrict_steps:
                step.skip = step._skip = True
            elif force_enable:
                step.skip = step._skip = False

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
            if re.match('^\(.+\)$', step_spec):
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
                # check that 
                # default set from own schema, ignoring parameter setting 
                if orig_schema is not None and orig_schema.default is not UNSET:
                    # also clear parameter setting to propagate our default
                    if step_param_name in step.params:
                        del step.params[step_param_name]
                    alias_schema.default = orig_schema.default
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
                # parameter is not required if alias target is set in step
                if step_param_name in step.params or \
                        step_param_name in step.cargo.defaults or \
                        schema.default is not UNSET:
                    alias_schema.required = False
                    alias_schema.default = UNSET
                    # mark it as hidden -- no need to expose parameters that are internally set this way
                    alias_schema.category = ParameterCategory.Hidden

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

    def finalize(self, config=None, log=None, name=None, fqname=None, nesting=0):
        if not self.finalized:
            config = config or stimela.CONFIG

            # fully qualified name, i.e. recipe_name.step_name.step_name etc.
            self.fqname = fqname = fqname or self.fqname or self.name

            # if logger is not provided, then init one
            if log is None:
                log = stimela.logger().getChild(self.fqname)
                log.propagate = True

            # init and/or update logger options
            self.logopts = config.opts.log.copy()

            # update file logger
            logsubst = SubstitutionNS(config=config, info=dict(fqname=fqname))
            stimelogging.update_file_logger(log, self.logopts, nesting=nesting, subst=logsubst, location=[self.fqname])

            # call Cargo's finalize method
            super().finalize(config, log=log, fqname=fqname, nesting=nesting)

            # finalize steps
            for label, step in self.steps.items():
                step_log = log.getChild(label)
                step_log.propagate = True
                try:
                    step.finalize(config, log=step_log, fqname=f"{fqname}.{label}", nesting=nesting+1)
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
            if name in self.inputs_outputs:
                if value == "UNSET":
                    unset_params.add(name)
                elif value == "EMPTY":
                    own_params[name] = ""
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


    def prevalidate(self, params: Dict[str, Any], subst: Optional[SubstitutionNS]=None, root=False):
        self.finalize()
        self.log.debug("prevalidating recipe")
        errors = []

        # split parameters into our own, and per-step, and UNSET directives
        params,  unset_params = self._preprocess_parameters(params)

        subst_outer = subst  # outer dictionary is used to prevalidate our parameters

        subst = SubstitutionNS()
        info = SubstitutionNS(fqname=self.fqname, label='', label_parts=[], suffix='')
        # mutable=False means these sub-namespaces are not subject to {}-substitutions
        subst._add_('info', info.copy(), nosubst=True)
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
            subst_outer._add_('info', info.copy(), nosubst=True)
            subst_outer._add_('config', self.config, nosubst=True) 
            subst_outer._add_('config', self.config, nosubst=True) 
            subst_outer.current = subst.recipe

        # update assignments
        self.update_assignments(subst, params=params, ignore_subst_errors=True)
        # this may have changed the file logger, so update
        stimelogging.update_file_logger(self.log, self.logopts, nesting=self.nesting, subst=subst, location=[self.fqname])

        # add for-loop variable to inputs, if expected there
        if self.for_loop is not None and self.for_loop.var in self.inputs:
            params[self.for_loop.var] = Unresolved("for-loop")

        # prevalidate our own parameters. This substitutes in defaults and does {}-substitutions
        # we call this twice, potentially, so define as a function
        def prevalidate_self(params):
            try:
                params1 = Cargo.prevalidate(self, params, subst=subst_outer)
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
                    subst.current._merge_(step_params)   # these may have changed in prevalidation
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

        self.log.debug("recipe pre-validated")

        return params

    def validate_for_loop(self, params, strict=False):
        # in case of for loops, get list of values to be iterated over 
        if self.for_loop is not None:
            # if over != None (see finalize() above), list of values needs to be looked up in inputs
            # if it is None, then an explicit list was supplied and is already in self._for_loop_values.
            if self.for_loop.over is not None:
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
                    raise ParameterValidationError(f"recipe '{self.name}': for_loop.over={self.for_loop.over} is unresolved")
                if type(values) is ListConfig:
                    values = list(values)
                elif not isinstance(values, (list, tuple)):
                    values = [values]
                if self._for_loop_values is None:
                    self.log.info(f"recipe is a for-loop with '{self.for_loop.var}' iterating over {len(values)} values")
                    self.log.info(f"Loop values: {values}")
                self._for_loop_values = values
            if self.for_loop.var in self.inputs:
                params[self.for_loop.var] = self._for_loop_values[0]
            else:
                self.assign[self.for_loop.var] = self._for_loop_values[0]
        # else fake a single-value list
        else:
            self._for_loop_values = [None]

    def validate_inputs(self, params: Dict[str, Any], subst: Optional[SubstitutionNS]=None, loosely=False):

        params, _ = self._preprocess_parameters(params)

        self.validate_for_loop(params, strict=True)

        if subst is None:
            subst = SubstitutionNS()
            info = SubstitutionNS(fqname=self.fqname)
            subst._add_('info', info, nosubst=True)
            subst._add_('config', self.config, nosubst=True) 

            subst.recipe = SubstitutionNS(**params)
            subst.current = subst.recipe

        return Cargo.validate_inputs(self, params, subst=subst, loosely=loosely)

    def _link_steps(self):
        """
        Adds  next_step and previous_step attributes to the recipe. 
        """
        steps = list(self.steps.values())
        N = len(steps)
        # Nothing to link if only one step
        if N == 1:
            return

        for i in range(N):
            step = steps[i]
            if i == 0:
                step.next_step = steps[1]
                step.previous_step = None
            elif i > 0 and i < N-2:
                step.next_step = steps[i+1]
                step.previous_step = steps[i-1]
            elif i == N-1:
                step.next_step = None
                step.previous_step = steps[i-2]

    def summary(self, params: Dict[str, Any], recursive=True, ignore_missing=False):
        """Returns list of lines with a summary of the recipe state
        """
        lines = [f"recipe '{self.name}':"] + [f"  {name} = {value}" for name, value in params.items()]
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


    def _run(self, params, subst=None) -> Dict[str, Any]:
        """Internal recipe run method. Meant to be called from a wrapper Step object (which validates the parameters, etc.)

        Parameters
        ----------

        Returns
        -------
        Dict[str, Any]
            Dictionary of formal outputs

        Raises
        ------
        RecipeValidationError
        """

        # set up substitution namespace
        subst_outer = subst
        if subst is None:
            subst = SubstitutionNS()

        info = SubstitutionNS(fqname=self.fqname, label='', label_parts=[], suffix='')
        # nosubst=True means these sub-namespaces are not subject to {}-substitutions
        subst._add_('info', info.copy(), nosubst=True)
        subst._add_('config', self.config, nosubst=True)
        subst._add_('steps', {}, nosubst=True)
        subst._add_('previous', {}, nosubst=True)
            
        subst.recipe = SubstitutionNS(**params)
        subst.recipe.log = self.logopts

        if subst_outer is not None:
            if 'root' in subst_outer:
                subst._add_('root', subst_outer.root, nosubst=True)
            if 'recipe' in subst_outer:
                subst._add_('parent', subst_outer.recipe, nosubst=True)
        else:
            subst.root = subst.recipe

        subst_copy = subst.copy()

        try:
            # update variable assignments
            self.update_assignments(subst, params=params)
            # log options may have changed, so adjust
            stimelogging.update_file_logger(self.log, self.logopts, nesting=self.nesting, subst=subst, location=[self.fqname])

            # Harmonise before running
            self._link_steps()

            self.log.info(f"running recipe '{self.name}'")

            # our inputs have been validated, so propagate aliases to steps. Check for missing stuff just in case
            for name, schema in self.inputs.items():
                if name in params:
                    value = params[name]
                    if isinstance(value, Unresolved):
                        raise RecipeValidationError(f"recipe '{self.name}' has unresolved input '{name}'", log=self.log)
                    # propagate up all aliases
                    for alias in self._alias_list.get(name, []):
                        if alias.from_recipe:
                            alias.step.update_parameter(alias.param, value)
                else:
                    if schema.required: 
                        raise RecipeValidationError(f"recipe '{self.name}' is missing required input '{name}'", log=self.log)

            # iterate over for-loop values (if not looping, this is set up to [None] in advance)
            scatter = getattr(self.for_loop, "scatter", False)
            
            def loop_worker(inst, step, label, subst, count, iter_var):
                """"
                Needed for concurrency
                """

                # update step info
                inst._prep_step(label, step, subst)

                # if for-loop, assign new value
                if inst.for_loop:
                    inst.log.info(f"for loop iteration {count}: {inst.for_loop.var} = {iter_var}")
                    # update variable (in params, if expected there, else in assignments)
                    if inst.for_loop.var in inst.inputs_outputs:
                        params[inst.for_loop.var] = iter_var
                    else:
                        inst.assign[inst.for_loop.var] = iter_var
                    # update variable index
                    inst.assign[f"{inst.for_loop.var}@index"] = count
                    stimelogging.declare_subtask_attributes(f"{count+1}/{len(inst._for_loop_values)}")

                # reevaluate recipe level assignments (info.fqname etc. have changed)
                inst.update_assignments(subst, params=params)
                # evaluate step-level assignments
                inst.update_assignments(subst, whose=step, params=params)
                # step logger may have changed
                stimelogging.update_file_logger(step.log, step.logopts, nesting=step.nesting, subst=subst, location=[step.fqname])
                # set our info back temporarily to update log assignments
                info_step = subst.info
                subst.info = info.copy()
                subst.info = info_step

                inst.log.info(f"processing step '{label}'")
                if step.info:
                    inst.log.info(f"  ({step.info})", extra=dict(color="GREEN", boldface=True))
                try:
                    #step_params = step.run(subst=subst.copy(), batch=batch)  # make a copy of the subst dict since recipe might modify
                    step_params = step.run(subst=subst.copy(), parent_log=self.log)  # make a copy of the subst dict since recipe might modify
                except ScabhaBaseException as exc:
                    if not exc.logged:
                        log_exception(StimelaStepExecutionError(f"error running step '{label}'", exc))
                        exc.logged = True
                    raise

                # put step parameters into previous and steps[label] again, as they may have changed based on outputs)
                subst.previous = step_params
                subst.steps[label] = subst.previous
                # revert to recipe level assignments
                inst.update_assignments(subst, whose=inst, params=params)

            loop_futures = []

            for count, iter_var in enumerate(self._for_loop_values):
                for label, step in self.steps.items():
                    this_args = (self,step, label, subst, count, iter_var)
                    loop_futures.append(this_args)

            # Transpose the list before parsing to pool.map()
            loop_args = list(map(list, zip(*loop_futures)))
            max_workers = getattr(self.config.opts.dist, "ncpu", cpu_count()//4)
            if scatter:
                loop_pool = ProcessPool(max_workers, scatter=True)
                results = loop_pool.amap(loop_worker, *loop_args)
                while not results.ready():
                    time.sleep(1)
                results.get()
            else:
                # loop_pool = SerialPool(max_workers)
                # results = list(loop_pool.imap(loop_worker, *loop_args))
                results = [loop_worker(*args) for args in loop_futures]

            # now check for output aliases that need to be propagated down
            for name, aliases in self._alias_list.items():
                for alias in aliases:
                    if alias.from_step:
                        if alias.param in alias.step.validated_params:
                            params[name] = alias.step.validated_params[alias.param]

            subst.current = subst.recipe
            
            # # evaluate implicit outputs
            # implicits = {name: schema.implcit for name, schema in self.outputs if schema.implicit}
            # params.update(**evaluate_and_substitute(implicits, subst, subst.current, location=self.fqname))

            self.log.info(f"recipe '{self.name}' executed successfully")
            return OrderedDict((name, value) for name, value in params.items() if name in self.outputs)
        finally:
            subst.update(subst_copy)


    # def run(self, **params) -> Dict[str, Any]:
    #     """Public interface for running a step. Keywords are passed in as step parameters

    #     Returns
    #     -------
    #     Dict[str, Any]
    #         Dictionary of formal outputs
    #     """
    #     return Step(recipe=self, params=params, info=f"wrapper step for recipe '{self.name}'").run()

StepSchema = OmegaConf.structured(Step)
RecipeSchema = OmegaConf.structured(Recipe)

class PyRecipe(Recipe):
    """ 
        Interface to Recipe class for python recipes (not YAML recipes)
    """
    def __init__(self, name, dirs, backend=None, info=None, log=None):

        self.backend = backend
        self.name = name

        self.inputs: Dict[str, Any] = {}

        for dir_item in dirs:
            self.inputs[dir_item] = { 
                "dtype": Directory,
                "default": dirs[dir_item]
            }

