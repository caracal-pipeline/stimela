import glob, time
import os, os.path, re, logging
from typing import Any, Tuple, List, Dict, Optional, Union
from enum import Enum
from dataclasses import dataclass
from omegaconf import MISSING, OmegaConf, DictConfig, ListConfig
from collections import OrderedDict
from scabha import cargo

from scabha.exceptions import SubstitutionError, SubstitutionErrorList
from stimela.config import EmptyDictDefault, EmptyListDefault, StimelaLogConfig
import stimela
from stimela import logger, stimelogging

from stimela.exceptions import *

from scabha import validate
from scabha.validate import Unresolved, join_quote
from scabha.substitutions import SubstitutionNS, substitutions_from 

from . import runners

Conditional = Optional[str]

from scabha.cargo import Cargo, Cab


@dataclass
class Step:
    """Represents one processing step of a recipe"""
    cab: Optional[str] = None                       # if not None, this step is a cab and this is the cab name
    recipe: Optional["Recipe"] = None               # if not None, this step is a nested recipe
    params: Dict[str, Any] = EmptyDictDefault()     # assigns parameter values
    info: Optional[str] = None                      # comment or info
    skip: bool = False                              # if true, step is skipped unless explicitly enabled
    tags: List[str] = EmptyListDefault()
    backend: Optional[str] = None                   # backend setting, overrides opts.config.backend if set

    name: str = ''                                  # step's internal name
    fqname: str = ''                                # fully-qualified name e.g. recipe_name.step_label

    assign: Dict[str, Any] = EmptyDictDefault()     # assigns variables when step is executed

    _skip: Conditional = None                       # skip this step if conditional evaluates to true
    _break_on: Conditional = None                   # break out (of parent recipe) if conditional evaluates to true

    def __post_init__(self):
        self.fqname = self.fqname or self.name
        if bool(self.cab) == bool(self.recipe):
            raise StepValidationError("step must specify either a cab or a nested recipe, but not both")
        self.cargo = self.config = None
        self._prevalidated = None
        self.tags = set(self.tags)
        # convert params into stadard dict, else lousy stuff happens when we imnsetr non-standard objects
        if isinstance(self.params, DictConfig):
            self.params = OmegaConf.to_container(self.params)

    def summary(self, recursive=True):
        return self.cargo and self.cargo.summary(recursive=recursive)

    @property
    def finalized(self):
        return self.cargo is not None

    @property
    def prevalidated(self):
        return self._prevalidated

    @property
    def missing_params(self):
        return self.cargo.missing_params

    @property
    def invalid_params(self):
        return self.cargo.invalid_params

    @property
    def unresolved_params(self):
        return self.cargo.unresolved_params

    @property
    def inputs(self):
        return self.cargo.inputs

    @property
    def outputs(self):
        return self.cargo.outputs

    @property
    def inputs_outputs(self):
        return self.cargo.inputs_outputs

    @property
    def log(self):
        """Logger object passed from cargo"""
        return self.cargo and self.cargo.log
    
    @property
    def logopts(self):
        """Logger options passed from cargo"""
        return self.cargo and self.cargo.logopts

    @property
    def nesting(self):
        """Logger object passed from cargo"""
        return self.cargo and self.cargo.nesting

    def update_parameter(self, name, value):
        self.params[name] = value
        # only pass value up to cargo if has already been validated. This avoids redefinition errors from nested aliases.
        # otherwise, just keep the value in our dict (cargo will get it upon validation)
        if self.cargo is not None and self.prevalidated:
            self.cargo.update_parameter(name, value)

    def finalize(self, config=None, log=None, logopts=None, fqname=None, nesting=0):
        if not self.finalized:
            if fqname is not None:
                self.fqname = fqname
            self.config = config = config or stimela.CONFIG

            if bool(self.cab) == bool(self.recipe):
                raise StepValidationError("step must specify either a cab or a nested recipe, but not both")
            # if recipe, validate the recipe with our parameters
            if self.recipe:
                # instantiate from omegaconf object, if needed
                if type(self.recipe) is not Recipe:
                    self.recipe = Recipe(**self.recipe)
                self.cargo = self.recipe
            else:
                if self.cab not in self.config.cabs:
                    raise StepValidationError(f"unknown cab {self.cab}")
                self.cargo = Cab(**config.cabs[self.cab])
            self.cargo.name = self.name

            # if logger is not provided, then init one
            if log is None:
                log = stimela.logger().getChild(self.fqname)
                log.propagate = True

            # init and/or update logger options
            logopts = (logopts if logopts is not None else self.config.opts.log).copy()
            if 'log' in self.assign:
                logopts.update(**self.assign.log)

            # update file logging if not recipe (a recipe will do it in its finalize() anyway, with its own substitions)
            if not self.recipe:
                logsubst = SubstitutionNS(config=config, info=dict(fqname=self.fqname))
                stimelogging.update_file_logger(log, logopts, nesting=nesting, subst=logsubst, location=[self.fqname])

            # finalize the cargo
            self.cargo.finalize(config, log=log, logopts=logopts, fqname=self.fqname, nesting=nesting)

            # set backend
            self.backend = self.backend or self.config.opts.backend


    def prevalidate(self, subst: Optional[SubstitutionNS]=None):
        if not self.prevalidated:
            self.finalize()
            # validate cab or recipe
            self.cargo.prevalidate(self.params, subst)
            self.log.debug(f"{self.cargo.name}: {len(self.missing_params)} missing, "
                            f"{len(self.invalid_params)} invalid and "
                            f"{len(self.unresolved_params)} unresolved parameters")
            if self.invalid_params:
                raise StepValidationError(f"{self.cargo.name} has the following invalid parameters: {join_quote(self.invalid_params)}")
            self._prevalidated = True

    def log_summary(self, level, title, color=None):
        extra = dict(color=color, boldface=True)
        if self.log.isEnabledFor(level):
            self.log.log(level, f"### {title}", extra=extra)
            del extra['boldface']
            for line in self.summary(recursive=False):
                self.log.log(level, line, extra=extra)

    def run(self, params=None, subst=None):
        """Runs the step"""
        self.prevalidate()
        if params is None:
            params = self.params

        skip_warned = False   # becomes True when warnings are given

        self.log.debug(f"validating inputs {subst and list(subst.keys())}")
        validated = None
        try:
            params = self.cargo.validate_inputs(params, loosely=self.skip, subst=subst)
            validated = True

        except ScabhaBaseException as exc:
            level = logging.WARNING if self.skip else logging.ERROR
            if not exc.logged:
                if type(exc) is SubstitutionErrorList:
                    self.log.log(level, f"unresolved {{}}-substitution(s):")
                    for err in exc.errors:
                        self.log.log(level, f"  {err}")
                else:
                    self.log.log(level, f"error validating inputs: {exc}")
                exc.logged = True
            self.log_summary(level, "summary of inputs follows", color="WARNING")
            # raise up, unless step is being skipped
            if self.skip:
                self.log.warning("since the step is being skipped, this is not fatal")
                skip_warned = True
            else:
                raise

        # log inputs
        if validated and not self.skip:
            self.log_summary(logging.INFO, "validated inputs", color="GREEN")
            if subst is not None:
                subst.current = params

        # bomb out if some inputs failed to validate or substitutions resolve
        if self.cargo.invalid_params or self.cargo.unresolved_params:
            invalid = self.cargo.invalid_params + self.cargo.unresolved_params
            if self.skip:
                self.log.warning(f"invalid inputs: {join_quote(invalid)}")
                if not skip_warned:
                    self.log.warning("since the step was skipped, this is not fatal")
                    skip_warned = True
            else:
                raise StepValidationError(f"invalid inputs: {join_quote(invalid)}", log=self.log)

        if not self.skip:
            try:
                if type(self.cargo) is Recipe:
                    self.cargo.backend = self.cargo.backend or self.backend
                    self.cargo._run()
                elif type(self.cargo) is Cab:
                    self.cargo.backend = self.cargo.backend or self.backend
                    runners.run_cab(self.cargo, log=self.log, subst=subst)
                else:
                    raise RuntimeError("Unknown cargo type")
            except ScabhaBaseException as exc:
                if not exc.logged:
                    self.log.error(f"error running step: {exc}")
                    exc.logged = True
                raise

        self.log.debug(f"validating outputs")
        validated = False
        # insert output values into params for re-substitution and re-validation
        output_params = {name: value for name, value in params.items() if name in self.cargo.outputs}
        try:
            params = self.cargo.validate_outputs(output_params, loosely=self.skip, subst=subst)
            validated = True
        except ScabhaBaseException as exc:
            level = logging.WARNING if self.skip else logging.ERROR
            if not exc.logged:
                if type(exc) is SubstitutionErrorList:
                    self.log.log(level, f"unresolved {{}}-substitution(s):")
                    for err in exc.errors:
                        self.log.log(level, f"  {err}")
                else:
                    self.log.log(level, f"error validating outputs: {exc}")
                exc.logged = True
            # raise up, unless step is being skipped
            if self.skip:
                self.log.warning("since the step was skipped, this is not fatal")
            else:
                self.log_summary(level, "failed outputs", color="WARNING")
                raise

        if validated:
            self.log_summary(logging.DEBUG, "validated outputs")

        # bomb out if an output was invalid
        invalid = [name for name in self.cargo.invalid_params + self.cargo.unresolved_params if name in self.cargo.outputs]
        if invalid:
            if self.skip:
                self.log.warning(f"invalid outputs: {join_quote(invalid)}")
                self.log.warning("since the step was skipped, this is not fatal")
            else:
                raise StepValidationError(f"invalid outputs: {join_quote(invalid)}", log=self.log)

        return {name: value for name, value in self.cargo.params.items()}

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
        self.missing_params:    dict (name to Parameter) of required parameters that have not been specified

    Raises:
        various classes of validation errors
    """
    steps: Dict[str, Step] = EmptyDictDefault()     # sequence of named steps

    assign: Dict[str, Any] = EmptyDictDefault()     # assigns variables

    aliases: Dict[str, Any] = EmptyDictDefault()

    defaults: Dict[str, Any] = EmptyDictDefault()

    # make recipe a for_loop-gather (i.e. parallel for loop)
    for_loop: Optional[ForLoopClause] = None

    # logging control, overrides opts.log.init_logname and opts.log.logname 
    init_logname: Optional[str] = None
    logname: Optional[str] = None
    
    # # if not None, do a while loop with the conditional
    # _while: Conditional = None
    # # if not None, do an until loop with the conditional
    # _until: Conditional = None

    def __post_init__ (self):
        Cargo.__post_init__(self)
        # check that schemas are valid
        for io in self.inputs, self.outputs:
            for name, schema in io.items():
                if not schema:
                    raise RecipeValidationError(f"'{name}' does not define a valid schema")
        # check for repeated aliases
        for name, alias_list in self.aliases.items():
            if name in self.inputs_outputs:
                raise RecipeValidationError(f"alias '{name}' also appears under inputs or outputs")
            if type(alias_list) is str:
                alias_list = self.aliases[name] = [alias_list]
            if not hasattr(alias_list, '__iter__') or not all(type(x) is str for x in alias_list):
                raise RecipeValidationError(f"alias '{name}': name or list of names expected")
            for x in alias_list:
                if '.' not in x:
                    raise RecipeValidationError(f"alias '{name}': invalid target '{x}' (missing dot)")
        # instantiate steps if needed (when creating from an omegaconf)
        if type(self.steps) is not OrderedDict:
            steps = OrderedDict()
            for label, stepconfig in self.steps.items():
                stepconfig.name = label
                stepconfig.fqname = f"{self.name}.{label}"
                steps[label] = Step(**stepconfig)
            self.steps = steps
        # check that assignments don't clash with i/o parameters
        for assign, assign_label in [(self.assign, "assign")] + [(step.assign, f"{label}.assign") for label, step in self.steps.items()]:
            for key in assign:
                for io, io_label in [(self.inputs, "inputs"), (self.outputs, "outputs")]:
                    if key in io:
                        raise RecipeValidationError(f"'{assign_label}.{key}' clashes with recipe {io_label}")
        # check that for-loop variable does not clash
        if self.for_loop:
            for io, io_label in [(self.inputs, "inputs"), (self.outputs, "outputs")]:
                if self.for_loop.var in io:
                    raise RecipeValidationError(f"'for_loop.var={self.for_loop.var}' clashes with recipe {io_label}")
        # map of aliases
        self._alias_map = None

    @property
    def finalized(self):
        return self._alias_map is not None

    def enable_step(self, label, enable=True):
        self.finalize()
        step = self.steps.get(label)
        if step is None:
            raise RecipeValidationError(f"unknown step {label}", log=self.log)
        if step.skip and enable:
            self.log.warning(f"enabling step '{label}' which was previously marked as skipped")
        elif not step.skip and not enable:
            self.log.warning(f"will skip step '{label}'")
        step.skip = not enable

    def restrict_steps(self, steps: List[str], force_enable=True):
        self.finalize()
        # check for unknown steps
        restrict_steps = set(steps)
        unknown_steps = restrict_steps.difference(self.steps)
        if unknown_steps:
            raise RecipeValidationError(f"unknown step(s) {join_quote(unknown_steps)}", log=self.log)

        # apply skip flags 
        for label, step in self.steps.items():
            if label not in restrict_steps:
                step.skip = True
            elif force_enable:
                step.skip = False

    def add_step(self, step: Step, label: str = None):
        """Adds a step to the recipe. Label is auto-generated if not supplied

        Args:
            step (Step): step object to add
            label (str, optional): step label, auto-generated if None
        """
        if self.finalized:
            raise DefinitionError("can't add a step to a recipe that's been finalized")

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


    def _add_alias(self, alias_name: str, alias_target: Union[str, Tuple]):
        if type(alias_target) is str:
            step_label, step_param_name = alias_target.split('.', 1)
            step = self.steps.get(step_label)
        else:
            step, step_label, step_param_name = alias_target

        if step is None:
            raise RecipeValidationError(f"alias '{alias_name}' refers to unknown step '{step_label}'", log=self.log)
        # find it in inputs or outputs
        input_schema = step.inputs.get(step_param_name)
        output_schema = step.outputs.get(step_param_name)
        schema = input_schema or output_schema
        if schema is None:
            raise RecipeValidationError(f"alias '{alias_name}' refers to unknown step parameter '{step_label}.{step_param_name}'", log=self.log)
        # check that it's not an input that is already set
        if step_param_name in step.params and input_schema:
            raise RecipeValidationError(f"alias '{alias_name}' refers to input '{step_label}.{step_param_name}' that is already predefined", log=self.log)
        # check that its I/O is consistent
        if (input_schema and alias_name in self.outputs) or (output_schema and alias_name in self.inputs):
            raise RecipeValidationError(f"alias '{alias_name}' can't refer to both an input and an output", log=self.log)
        # see if it's already defined consistently
        io = self.inputs if input_schema else self.outputs
        existing_schema = io.get(alias_name)
        if existing_schema is None:
            io[alias_name] = schema.copy()      # make copy of schema object                  
            existing_schema = io[alias_name]    # get reference to this copy now
            existing_schema.required = False # will be set to True below as needed
            existing_schema.implicit = None
        else:
            # if existing type was unset, set it quietly
            if not existing_schema.dtype:
                existing_schema.dtype = schema.dtype
            # check if definition conflicts
            elif schema.dtype != existing_schema.dtype:
                raise RecipeValidationError(f"alias '{alias_name}': dtype {schema.dtype} of '{step_label}.{step_param_name}' doesn't match previous dtype {existing_schema.dtype}", log=self.log)

        # this is True if the step's parameter is defined in any way (set, default, or implicit)
        have_step_param = step_param_name in step.params or step_param_name in step.cargo.defaults or \
            schema.default is not None or schema.implicit is not None

        # alias becomes required if any step parameter it refers to was required and unset
        if schema.required and not have_step_param:
            existing_schema.required = True
        # alias becomes implicit if any step parameter it refers to is defined (and it doesn't have its own default)
        if have_step_param and alias_name not in self.defaults:
            existing_schema.implicit = f"{step_label}.{step_param_name}"

        self._alias_map[step_label, step_param_name] = alias_name
        self._alias_list.setdefault(alias_name, []).append((step, step_param_name))

    def finalize(self, config=None, log=None, logopts=None, fqname=None, nesting=0):
        if not self.finalized:
            config = config or stimela.CONFIG

            # fully qualified name, i.e. recipe_name.step_name.step_name etc.
            self.fqname = fqname = fqname or self.fqname or self.name

            # if logger is not provided, then init one
            if log is None:
                log = stimela.logger().getChild(self.fqname)
                log.propagate = True

            # init and/or update logger options
            logopts = (logopts if logopts is not None else config.opts.log).copy()
            if 'log' in self.assign:
                logopts.update(**self.assign.log)

            # update file logger
            logsubst = SubstitutionNS(config=config, info=dict(fqname=fqname))
            stimelogging.update_file_logger(log, logopts, nesting=nesting, subst=logsubst, location=[self.fqname])

            # call Cargo's finalize method
            super().finalize(config, log=log, logopts=logopts, fqname=fqname, nesting=nesting)

            # finalize steps
            for label, step in self.steps.items():
                step_log = log.getChild(label)
                step_log.propagate = True
                step.finalize(config, log=step_log, logopts=logopts, fqname=f"{fqname}.{label}", nesting=nesting+1)

            # collect aliases
            self._alias_map = OrderedDict()
            self._alias_list = OrderedDict()

            # collect from inputs and outputs
            for io in self.inputs, self.outputs:
                for name, schema in io.items():
                    if schema.aliases:
                        if schema.dtype != "str" or schema.choices or schema.writable:
                            raise RecipeValidationError(f"alias '{name}' should not specify type, choices or writability", log=log)
                        schema.dtype = ""       # tells _add_alias to not check
                        for alias_target in schema.aliases:
                            self._add_alias(name, alias_target)

            # collect from aliases section
            for name, alias_list in self.aliases.items():
                for alias_target in alias_list:
                    self._add_alias(name, alias_target)

            # automatically make aliases for step parameters that are unset, and don't have a default, and aren't implict 
            for label, step in self.steps.items():
                for name, schema in step.inputs_outputs.items():
                    if (label, name) not in self._alias_map and name not in step.params \
                            and name not in step.cargo.defaults and schema.default is None \
                            and not schema.implicit:
                        auto_name = f"{label}_{name}"
                        if auto_name in self.inputs or auto_name in self.outputs:
                            raise RecipeValidationError(f"auto-generated parameter name '{auto_name}' conflicts with another name. Please define an explicit alias for this.", log=log)
                        self._add_alias(auto_name, (step, label, name))

            # these will be re-merged when needed again
            self._inputs_outputs = None

            # check that for-loop is valid, if defined
            if self.for_loop is not None:
                # if for_loop.over is a str, treat it as a required input
                if type(self.for_loop.over) is str:
                    if self.for_loop.over not in self.inputs:
                        raise RecipeValidationError(f"for_loop: over: '{self.for_loop.over}' is not a defined input", log=log)
                    # this becomes a required input
                    self.inputs[self.for_loop.over].required = True
                # else treat it as a list of values to be iterated over (and set over=None to indicate this)
                elif type(self.for_loop.over) in (list, tuple, ListConfig):
                    self._for_loop_values = list(self.for_loop.over)
                    self.for_loop.over = None
                else:
                    raise RecipeValidationError(f"for_loop: over is of invalid type {type(self.for_loop.over)}", log=log)

                # insert empty loop variable
                if self.for_loop.var not in self.assign:
                    self.assign[self.for_loop.var] = ""

    def _prep_step(self, label, step, subst):
        parts = label.split("-")
        info = subst.info
        info.fqname = f"{self.fqname}.{label}"
        info.label = label 
        info.label_parts = parts
        info.suffix = parts[-1] if len(parts) > 1 else ''
        subst.current = step.params
        subst.steps[label] = subst.current

    def prevalidate(self, params: Optional[Dict[str, Any]], subst: Optional[SubstitutionNS]=None):
        self.finalize()
        self.log.debug("prevalidating recipe")
        errors = []

        subst = SubstitutionNS()
        info = SubstitutionNS(fqname=self.fqname)
        # mutable=False means these sub-namespaces are not subject to {}-substitutions
        subst._add_('info', info, nosubst=True)
        subst._add_('config', self.config, nosubst=True) 
        subst._add_('steps', {}, nosubst=True)
        subst._add_('previous', {}, nosubst=True)
        subst._add_('recipe', self.make_substitition_namespace(ns=self.assign))
        subst.recipe._merge_(params)

        # add for-loop variable to inputs, if expected there
        if self.for_loop is not None and self.for_loop.var in self.inputs:
            params[self.for_loop.var] = Unresolved("for-loop")

        # validate our own parameters
        try:
            Cargo.prevalidate(self, params, subst=subst)
        except ScabhaBaseException as exc:
            msg = f"recipe pre-validation failed: {exc}"
            errors.append(RecipeValidationError(msg, log=self.log))

        # merge again
        subst.recipe._merge_(self.params)

        # propagate aliases up to substeps
        for name, value in self.params.items():
            self._propagate_parameter(name, value)

        # check for missing parameters
        if self.missing_params:
            msg = f"""recipe '{self.name}' is missing the following required parameters: {join_quote(self.missing_params)}"""
            errors.append(RecipeValidationError(msg, log=self.log))

        # prevalidate step parameters 
        for label, step in self.steps.items():
            self._prep_step(label, step, subst)

            try:
                step.prevalidate(subst)
            except ScabhaBaseException as exc:
                if type(exc) is SubstitutionErrorList:
                    self.log.error(f"unresolved {{}}-substitution(s):")
                    for err in exc.errors:
                        self.log.error(f"  {err}")
                msg = f"step '{label}' failed pre-validation: {exc}"
                errors.append(RecipeValidationError(msg, log=self.log))

            subst.previous = subst.current
            subst.steps[label] = subst.previous


        if errors:
            raise RecipeValidationError(f"{len(errors)} error(s) validating the recipe '{self.name}'", log=self.log)

        self.log.debug("recipe pre-validated")

    def validate_inputs(self, params: Dict[str, Any], subst: Optional[SubstitutionNS]=None, loosely=False):
        # in case of for loops, get list of values to be iterated over 
        if self.for_loop is not None:
            # if over != None (see finalize() above), list of values needs to be looked up in inputs
            if self.for_loop.over is not None:
                self._for_loop_values = self.params[self.for_loop.over]
                if not isinstance(self._for_loop_values, (list, tuple)):
                    self._for_loop_values = [self._for_loop_values]
            self.log.info(f"recipe is a for-loop with '{self.for_loop.var}' iterating over {len(self._for_loop_values)} values")
            # add first value to inputs, if needed
            if self.for_loop.var in self.inputs and self._for_loop_values:
                params[self.for_loop.var] = self._for_loop_values[0]
        # else fake a single-value list
        else:
            self._for_loop_values = [None]

        # add 'recipe' substitutions
        if subst is None:
            subst = SubstitutionNS()
            info = SubstitutionNS(fqname=self.fqname)
            subst._add_('info', info, nosubst=True)
            subst._add_('config', self.config, nosubst=True) 
        subst._add_('recipe', self.make_substitition_namespace(ns=self.assign))
        subst.recipe._merge_(params)

        params = Cargo.validate_inputs(self, params, subst=subst, loosely=loosely)
        
        return params


    def _propagate_parameter(self, name, value):
        ### OMS: not sure why I had this, why not propagae unresolveds?
        ## if type(value) is not validate.Unresolved:
        for step, step_param_name in self._alias_list.get(name, []):
            if self.inputs_outputs[name].implicit:
                if step_param_name in step.cargo.params:
                    self.params[name] = step.cargo.params[name]
            else:
                step.update_parameter(step_param_name, value)

    def update_parameter(self, name: str, value: Any):
        """[summary]

        Parameters
        ----------
        name : str
            [description]
        value : Any
            [description]
        """
        self.params[name] = value
        # resolved values propagate up to substeps if aliases, and propagate back if implicit
        self._propagate_parameter(name, value)

    def summary(self, recursive=True):
        """Returns list of lines with a summary of the recipe state
        """
        lines = [f"recipe '{self.name}':"] + [f"  {name} = {value}" for name, value in self.params.items()] + \
                [f"  {name} = ???" for name in self.missing_params]
        if recursive:
            lines.append("  steps:")
            for name, step in self.steps.items():
                stepsum = step.summary()
                lines.append(f"    {name}: {stepsum[0]}")
                lines += [f"    {x}" for x in stepsum[1:]]
        return lines

    _root_recipe_ns = None

    def _run(self) -> Dict[str, Any]:
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
        subst = SubstitutionNS()
        info = SubstitutionNS(fqname=self.fqname)
        # mutable=False means these sub-namespaces are not subject to {}-substitutions
        subst._add_('info', info, nosubst=True)
        subst._add_('config', self.config, nosubst=True) 
        subst._add_('steps', {}, nosubst=True)
        subst._add_('previous', {}, nosubst=True)
        recipe_ns = self.make_substitition_namespace(ns=self.assign)
        subst._add_('recipe', recipe_ns)

        # add root-level recipe info
        if self.nesting <= 1:
            Recipe._root_recipe_ns = recipe_ns
        subst._add_('root', Recipe._root_recipe_ns)

        logopts = self.config.opts.log.copy()
        if 'log' in self.assign:
            logopts.update(**self.assign.log)
        
        # update logfile name (since this may depend on substitutions)
        stimelogging.update_file_logger(self.log, self.logopts, nesting=self.nesting, subst=subst, location=[self.fqname])

        self.log.info(f"running recipe '{self.name}'")

        # our inputs have been validated, so propagate aliases to steps. Check for missing stuff just in case
        for name, schema in self.inputs.items():
            if name in self.params:
                value = self.params[name]
                if type(value) is validate.Unresolved:
                    raise RecipeValidationError(f"recipe '{self.name}' has unresolved input '{name}'", log=self.log)
                # propagate up all aliases
                for step, step_param_name in self._alias_list.get(name, []):
                    step.update_parameter(step_param_name, value)
            else:
                if schema.required: 
                    raise RecipeValidationError(f"recipe '{self.name}' is missing required input '{name}'", log=self.log)


        # iterate over for-loop values (if not looping, this is set up to [None] in advance)
        for count, iter_var in enumerate(self._for_loop_values):

            # if for-loop, assign new value
            if self.for_loop:
                self.log.info(f"for loop iteration {count}: {self.for_loop.var} = {iter_var}")
                subst.recipe[self.for_loop.var] = self.assign[self.for_loop.var] = iter_var
                # update logfile name (since this may depend on substitutions)
                stimelogging.update_file_logger(self.log, self.logopts, nesting=self.nesting, subst=subst, location=[self.fqname])

            for label, step in self.steps.items():
                # merge in variable assignments and add step params as "current" namespace
                subst.recipe._merge_(step.assign)
                # update info
                self._prep_step(label, step, subst)

                # update log options again (based on assign.log which may have changed)
                if 'log' in step.assign:
                    logopts.update(**step.assign.log)

                # update logfile name regardless (since this may depend on substitutions)
                info.fqname = step.fqname
                stimelogging.update_file_logger(step.log, step.logopts, nesting=step.nesting, subst=subst, location=[step.fqname])
    
                self.log.info(f"{'skipping' if step.skip else 'running'} step '{label}'")
                try:
                    step_params = step.run(subst=subst.copy())  # make a copy of the subst dict since recipe might modify
                except ScabhaBaseException as exc:
                    if not exc.logged:
                        self.log.error(f"error running step '{label}': {exc}")
                        exc.logged = True
                    raise
                # put step parameters into previous and steps[label] again, as they may have changed based on outputs)
                subst.previous = step_params
                subst.steps[label] = subst.previous

                # check aliases, our outputs need to be retrieved from the step
                for name, schema in self.outputs.items():
                    for step1, step_param_name in self._alias_list.get(name, []):
                        if step1 is step and step_param_name in step_params:
                            self.params[name] = step_params[step_param_name]
                            # clear implicit setting
                            self.outputs[name].implicit = None

        self.log.info(f"recipe '{self.name}' executed successfully")
        return {name: value for name, value in self.params.items() if name in self.outputs}


    def run(self, **params) -> Dict[str, Any]:
        """Public interface for running a step. Keywords are passed in as step parameters

        Returns
        -------
        Dict[str, Any]
            Dictionary of formal outputs
        """
        return Step(recipe=self, params=params, info=f"wrapper step for recipe '{self.name}'").run()
