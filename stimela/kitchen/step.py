import os, os.path, re, logging, fnmatch, copy, time
from typing import Any, Tuple, List, Dict, Optional, Union
from dataclasses import dataclass
from omegaconf import MISSING, OmegaConf, DictConfig, ListConfig
from omegaconf.errors import OmegaConfBaseException
from collections import OrderedDict

from stimela import config
from stimela.config import EmptyDictDefault, EmptyListDefault
import stimela
from stimela import log_exception, stimelogging
from stimela.exceptions import *
import scabha.exceptions
from scabha.exceptions import SubstitutionError, SubstitutionErrorList
from scabha.validate import evaluate_and_substitute, Unresolved, join_quote
from scabha.substitutions import SubstitutionNS, substitutions_from 
from scabha.basetypes import UNSET, Placeholder, MS, File, Directory
from .cab import Cab, get_cab_schema

Conditional = Optional[str]


def resolve_dotted_reference(key, base, current, context): 
    """helper function to look up a key like a.b.c in a nested dict-like structure"""
    path = key.split('.')
    if path[0]:
        section = base
    else:
        if not current:
            raise NameError(f"{context}: leading '.' not permitted here")
        section = current
        path = path[1:]
        if not path:
            raise NameError(f"{context}: '.' not permitted")
    varname = path[-1]
    for element in path[:-1]:
        if not element:
            raise NameError(f"{context}: '..' not permitted")
        if element in section:
            section = section[element]
        else:
            raise NameError(f"{context}: '{element}' in '{key}' is not a valid config section")
    return section, varname


@dataclass
class Step:
    """Represents one processing step of a recipe"""
    cab: Optional[Any] = None                       # if not None, this step is a cab and this is the cab name
    recipe: Optional[Any] = None                    # if not None, this step is a nested recipe
    params: Dict[str, Any] = EmptyDictDefault()     # assigns parameter values
    info: Optional[str] = None                      # comment or info
    skip: Optional[str] = None                      # if this evaluates to True, step is skipped 
    tags: List[str] = EmptyListDefault()
    backend: Optional["stimela.config.Backend"] = None                   # backend setting, overrides opts.config.backend if set

    name: str = ''                                  # step's internal name
    fqname: str = ''                                # fully-qualified name e.g. recipe_name.step_label

    assign: Dict[str, Any] = EmptyDictDefault()     # assigns recipe-level variables when step is executed

    assign_based_on: Dict[str, Any] = EmptyDictDefault()
                                                    # assigns recipe-level variables when step is executed based on value of another variable

    # runtime settings
    runtime: Dict[str, Any] = EmptyDictDefault()

    # _skip: Conditional = None                       # skip this step if conditional evaluates to true
    # _break_on: Conditional = None                   # break out (of parent recipe) if conditional evaluates to true

    def __post_init__(self):
        self.fqname = self.fqname or self.name
        if not bool(self.cab) and not bool(self.recipe):
            raise StepValidationError(f"step '{self.name}': step must specify either a cab or a nested recipe")
        if bool(self.cab) == bool(self.recipe):
            raise StepValidationError(f"step '{self.name}': step can't specify both a cab and a nested recipe")
        self.cargo = self.config = None
        self.tags = set(self.tags)
        # convert params into standard dict, else lousy stuff happens when we insert non-standard objects
        if isinstance(self.params, DictConfig):
            self.params = OmegaConf.to_container(self.params)
        # after (pre)validation, this contains parameter values
        self.validated_params = None
        # the "skip" attribute is reevaluated at runtime since it may contain substitutions, but if it's set to a bool
        # constant, self._skip will be preset already
        if self.skip in {"True", "true", "1"}:
            self._skip = True
        elif self.skip in {"False", "false", "0", "", None}:
            self._skip = False
        else:
            # otherwise, self._skip stays at None, and will be re-evaluated at runtime
            self._skip = None
        
        
    def summary(self, params=None, recursive=True, ignore_missing=False, inputs=True, outputs=True):
        summary_params = OrderedDict()
        for name, value in (params or self.validated_params or self.params).items():
            schema = self.cargo.inputs_outputs[name]
            if (inputs and (schema.is_input or schema.is_named_output)) or \
                (outputs and schema.is_output):
                summary_params[name] = value
        return self.cargo and self.cargo.summary(recursive=recursive, params=summary_params, ignore_missing=ignore_missing)

    @property
    def finalized(self):
        return self.cargo is not None

    @property
    def missing_params(self):
        return OrderedDict([(name, schema) for name, schema in self.cargo.inputs_outputs.items() 
                            if schema.required and name not in self.validated_params])

    @property
    def invalid_params(self):
        return [name for name, value in self.validated_params.items() if isinstance(value, scabha.exceptions.Error)]

    @property
    def unresolved_params(self):
        return [name for name, value in self.validated_params.items() if isinstance(value, Unresolved) and not isinstance(value, Placeholder)]

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

    def unset_parameter(self, name):
        if name in self.params:
            del self.params[name]

    def update_log_options(self, **options):
        from .recipe import Recipe
        for setting, value in options.items():
            try:
                self.logopts[setting] = value
            except Exception as exc:
                raise AssignmentError(f"invalid {self.fqname}.log.{setting} setting", exc)
        # propagate to children
        if isinstance(self.cargo, Recipe):
            self.cargo.update_log_options(**options)

    _instantiated_cabs = {}

    def finalize(self, config=None, log=None, fqname=None, nesting=0):
        from .recipe import Recipe, RecipeSchema
        if not self.finalized:
            if fqname is not None:
                self.fqname = fqname
            self.config = config = config or stimela.CONFIG

            # if recipe, validate the recipe with our parameters
            if self.recipe:
                # first, if it is a string, look it up in library
                recipe_name = f"{self.fqname}:recipe"
                if type(self.recipe) is str:
                    recipe_name = f"nested recipe '{self.recipe}'"
                    # undotted name -- look in lib.recipes
                    if '.' not in self.recipe:
                        if self.recipe not in self.config.lib.recipes:
                            raise StepValidationError(f"recipe '{self.recipe}' not found in lib.recipes")
                        self.recipe = self.config.lib.recipes[self.recipe]
                    # dotted name -- look in config
                    else: 
                        section, var = resolve_dotted_reference(self.recipe, config, current=None, context=f"step '{self.name}'")
                        if var not in section:
                            raise StepValidationError(f"recipe '{self.recipe}' not found")
                        self.recipe = section[var]
                    # self.recipe is now hopefully a DictConfig or a Recipe object, so fall through below to validate it 
                # instantiate from omegaconf object, if needed
                if type(self.recipe) is DictConfig:
                    try:
                        self.recipe = Recipe(**OmegaConf.unsafe_merge(RecipeSchema.copy(), self.recipe))
                    except OmegaConfBaseException as exc:
                        raise StepValidationError(f"error in recipe '{recipe_name}", exc)
                elif not isinstance(self.recipe, Recipe):
                    raise StepValidationError(f"recipe field must be a string or a nested recipe, got {type(self.recipe)}")
                self.cargo = self.recipe
            else:
                if type(self.cab) is str:
                    if self.cab in self._instantiated_cabs:
                        self.cargo = copy.copy(self._instantiated_cabs[self.cab])
                    else:
                        if self.cab not in self.config.cabs:
                            raise StepValidationError(f"unknown cab '{self.cab}'")
                        try:
                            self._instantiated_cabs[self.cab] = Cab(**config.cabs[self.cab])
                            self.cargo = copy.copy(self._instantiated_cabs[self.cab])
                        except Exception as exc:
                            raise StepValidationError(f"error in cab '{self.cab}'", exc)
                else:
                    if type(self.cab) is DictConfig:
                        cab_name = f"{self.fqname}:cab"
                        try:
                            self.cab = Cab(**OmegaConf.unsafe_merge(get_cab_schema().copy(), self.cab))
                        except OmegaConfBaseException as exc:
                            raise StepValidationError(f"error in cab '{cab_name}", exc)
                    elif not isinstance(self.cab, Cab):
                        raise StepValidationError(f"cab field must be a string or an inline cab, got {type(self.cab)}")
                    self.cargo = self.cab
            self.cargo.name = self.cargo.name or self.name

            # flatten parameters
            self.params = self.cargo.flatten_param_dict(OrderedDict(), self.params)

            # if logger is not provided, then init one
            if log is None:
                log = stimela.logger().getChild(self.fqname)
                log.propagate = True

            # finalize the cargo
            self.cargo.finalize(config, log=log, fqname=self.fqname, nesting=nesting)

            # build dictionary of defaults from cargo
            self.defaults = {name: schema.default for name, schema in self.cargo.inputs_outputs.items() 
                             if schema.default is not UNSET and not isinstance(schema.default, Unresolved) }
            self.defaults.update(**self.cargo.defaults)
            
            # set missing parameters from defaults
            for name, value in self.defaults.items():
                if name not in self.params:
                    self.params[name] = value

            # check for valid backend
            if type(self.cargo) is Cab:
                if self.backend is not None:
                    self._backend = self.backend
                    if self._backend.name not in stimela.config.AVAILABLE_BACKENDS:
                        status = stimela.config.get_backend_status(self._backend.name)
                        raise StepValidationError(f"backend '{self._backend.name}' is not available ({status})")
                elif self.cargo.backend is not None:
                    self._backend = self.cargo.backend
                    if self._backend.name not in stimela.config.AVAILABLE_BACKENDS:
                        status = stimela.config.get_backend_status(self._backend.name)
                        raise StepValidationError(f"backend '{self._backend.name}' specified by cab is not available ({status})")
                # no need to check this, it's checked at startup
                else:
                    self._backend =  stimela.CONFIG.opts.backend
            else:
                self._backend = None

    def prevalidate(self, subst: Optional[SubstitutionNS]=None, root=False):
        self.finalize()
        # validate cab or recipe
        params = self.validated_params = self.cargo.prevalidate(self.params, subst, root=root)
        # add missing outputs
        for name in self.cargo.outputs:
            if name not in params:
                params[name] = UNSET(name)
        self.log.debug(f"{self.cargo.name}: {len(self.missing_params)} missing, "
                        f"{len(self.invalid_params)} invalid and "
                        f"{len(self.unresolved_params)} unresolved parameters")
        if self.invalid_params:
            raise StepValidationError(f"step '{self.name}': {self.cargo.name} has the following invalid parameters: {join_quote(self.invalid_params)}")
        return params

    def log_summary(self, level, title, color=None, ignore_missing=True, inputs=False, outputs=False):
        extra = dict(color=color)
        if self.log.isEnabledFor(level):
            self.log.log(level, f"### {title}", extra=extra)
            for line in self.summary(recursive=False, inputs=inputs, outputs=outputs, ignore_missing=ignore_missing):
                self.log.log(level, line, extra=extra)

    def log_exception(self, exc, severity="error"):
        log_exception(exc, severity=severity, log=self.log)

    def run(self, subst=None, batch=None, parent_log=None):
        """Runs the step"""
        from .recipe import Recipe
        from . import runners

        if self.validated_params is None:
            self.prevalidate(self.params)
        # some messages go to the parent logger -- if not defined, default to our own logger
        if parent_log is None:
            parent_log = self.log

        with stimelogging.declare_subtask(self.name) as subtask:
            # evaluate the skip attribute (it can be a formula and/or a {}-substititon)
            skip = self._skip
            if self._skip is None and subst is not None:
                skips = dict(skip=self.skip)
                skips = evaluate_and_substitute(skips, subst, subst.current, location=[self.fqname], ignore_subst_errors=False)
                self.log.debug(f"dynamic skip attribute evaluation returns {skips}")
                skip = skips.get("skip")
                # formulas with unset variables return UNSET instance
                if isinstance(skip, UNSET):
                    if skip.errors:
                        raise StepValidationError(f"{self.fqname}.skip: error evaluating '{self.skip}'", skip.errors)
                    else:
                        raise StepValidationError(f"{self.fqname}.skip: error evaluating '{self.skip}'", SubstitutionError(f"unknown variable '{skip.value}'"))

            # Since prevalidation will have populated default values for potentially missing parameters, use those values
            # For parameters that aren't missing, use whatever value that was suplied
            params = self.validated_params.copy()
            params.update(**self.params)

            skip_warned = False   # becomes True when warnings are given

            self.log.debug(f"validating inputs {subst and list(subst.keys())}")
            validated = None
            try:
                params = self.cargo.validate_inputs(params, loosely=skip, subst=subst)
                validated = True

            except ScabhaBaseException as exc:
                severity = "warning" if skip else "error"
                level = logging.WARNING if skip else logging.ERROR
                if not exc.logged:
                    if type(exc) is SubstitutionErrorList:
                        self.log_exception(StepValidationError(f"unresolved {{}}-substitution(s) in inputs:", exc.nested), severity=severity)
                        # for err in exc.errors:
                        #     self.log.log(level, f"  {err}")
                    else:
                        self.log_exception(StepValidationError(f"error validating inputs:", exc), severity=severity)
                    exc.logged = True
                self.log_summary(level, "summary of inputs follows", color="WARNING", inputs=True)
                # raise up, unless step is being skipped
                if skip:
                    parent_log.warning("since the step is being skipped, this is not fatal")
                    skip_warned = True
                else:
                    raise

            self.validated_params.update(**params)

            # log inputs
            if validated and not skip:
                self.log_summary(logging.INFO, "validated inputs", color="GREEN", ignore_missing=True, inputs=True)
                if subst is not None:
                    subst.current = params

            ## check for (a) invalid params (b) unresolved inputs 
            # (c) unresolved outputs of File/MS/Directory type 
            invalid = self.invalid_params
            for name in self.unresolved_params:
                schema = self.cargo.inputs_outputs[name]
                if schema.is_input or schema.is_named_output:
                    invalid.append(name)
            if invalid:
                invalid = self.invalid_params + self.unresolved_params
                if skip:
                    self.log.warning(f"invalid inputs: {join_quote(invalid)}")
                    if not skip_warned:
                        parent_log.warning("since the step was skipped, this is not fatal")
                        skip_warned = True
                else:
                    raise StepValidationError(f"step '{self.name}': invalid inputs: {join_quote(invalid)}", log=self.log)

            if not skip:
                if type(self.cargo) is Recipe:
                    self.cargo._run(params, subst)
                elif type(self.cargo) is Cab:
                    if self.backend is not None:
                        backend = self.backend
                    elif self.cargo.backend is not None:
                        backend = self.cargo.backend
                    else:
                        backend =  stimela.CONFIG.opts.backend
                    cabstat = runners.run_cab(self, params, backend=backend, subst=subst, batch=batch)
                    # check for runstate
                    if cabstat.success is False:
                        raise StimelaCabRuntimeError(f"error running cab '{self.cargo.name}'", cabstat.errors)
                    for msg in cabstat.warnings:
                        self.log.warning(f"cab '{self.cargo.name}': {msg}")
                    params.update(**cabstat.outputs)
                else:
                    raise RuntimeError("step '{self.name}': unknown cargo type")
            else:
                if self._skip is None and subst is not None:
                    parent_log.info(f"skipping step based on setting of '{self.skip}'")
                else:
                    parent_log.info("skipping step based on explicit setting")

            self.log.debug(f"validating outputs")
            validated = False

            try:
                params = self.cargo.validate_outputs(params, loosely=skip, subst=subst)
                validated = True
            except ScabhaBaseException as exc:
                severity = "warning" if skip else "error"
                level = logging.WARNING if self.skip else logging.ERROR
                if not exc.logged:
                    if type(exc) is SubstitutionErrorList:
                        self.log_exception(StepValidationError(f"unresolved {{}}-substitution(s) in inputs:", exc.nested), severity=severity)
                        # for err in exc.errors:
                        #     self.log.log(level, f"  {err}")
                    else:
                        self.log_exception(StepValidationError(f"error validating outputs:", exc), severity=severity)
                    exc.logged = True
                # raise up, unless step is being skipped
                if skip:
                    self.log.warning("since the step was skipped, this is not fatal")
                else:
                    self.log_summary(level, "failed outputs", color="WARNING", inputs=False, outputs=True)
                    raise

            if validated:
                self.validated_params.update(**params)
                if subst is not None:
                    subst.current._merge_(params)
                self.log_summary(logging.DEBUG, "validated outputs", ignore_missing=True, outputs=True)

            # bomb out if an output was invalid
            invalid = [name for name in self.invalid_params + self.unresolved_params if name in self.cargo.outputs]
            if invalid:
                if skip:
                    parent_log.warning(f"invalid outputs: {join_quote(invalid)}")
                    parent_log.warning("since the step was skipped, this is not fatal")
                else:
                    raise StepValidationError(f"invalid outputs: {join_quote(invalid)}", log=self.log)

        return params

