import os, os.path, re, logging, copy, shutil, time
from typing import Any, Tuple, List, Dict, Optional, Union
from dataclasses import dataclass
from omegaconf import MISSING, OmegaConf, DictConfig, ListConfig
from omegaconf.errors import OmegaConfBaseException
from collections import OrderedDict
from contextlib import nullcontext
from rich.markup import escape

from stimela.config import EmptyDictDefault, EmptyListDefault
import stimela
from stimela import log_exception, stimelogging, task_stats
from stimela.stimelogging import log_rich_payload
from stimela.backends import StimelaBackendSchema, runner
from stimela.exceptions import *
import scabha.exceptions
from scabha.exceptions import SubstitutionError, SubstitutionErrorList
from scabha.validate import evaluate_and_substitute, evaluate_and_substitute_object, Unresolved, join_quote
from scabha.substitutions import SubstitutionNS, substitutions_from 
from scabha.basetypes import UNSET, Placeholder, MS, File, Directory, SkippedOutput
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

OUTPUTS_EXISTS = "exist"
OUTPUTS_FRESH = "fresh"

@dataclass
class Step:
    """Represents one processing step of a recipe"""
    cab: Optional[Any] = None                       # if not None, this step is a cab and this is the cab name
    recipe: Optional[Any] = None                    # if not None, this step is a nested recipe
    params: Dict[str, Any] = EmptyDictDefault()     # assigns parameter values
    info: Optional[str] = None                      # comment or info string
    skip: Optional[str] = None                      # if this evaluates to True, step is skipped.  
    skip_if_outputs: Optional[str] = None           # skip if outputs "exist' or "fresh"
    tags: List[str] = EmptyListDefault()

    name: str = ''                                  # step's internal name
    fqname: str = ''                                # fully-qualified name e.g. recipe_name.step_label

    assign: Dict[str, Any] = EmptyDictDefault()     # assigns recipe-level variables when step is executed

    assign_based_on: Dict[str, Any] = EmptyDictDefault()
                                                    # assigns recipe-level variables when step is executed based on value of another variable

    # optional backend settings
    backend: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        self.fqname = self.fqname or self.name
        if not bool(self.cab) and not bool(self.recipe):
            raise StepValidationError(f"step '{self.name}': step must specify either a cab or a nested recipe")
        if bool(self.cab) == bool(self.recipe):
            raise StepValidationError(f"step '{self.name}': step can't specify both a cab and a nested recipe")
        self.cargo = self.config = None
        self.tags = set(self.tags)
        # check backend setting
        if self.backend:
            try:
                OmegaConf.merge(StimelaBackendSchema, self.backend)
            except OmegaConfBaseException as exc:
                raise StepValidationError(f"step '{self.name}': invalid backend setting", exc)
        # convert params into standard dict, else lousy stuff happens when we insert non-standard objects
        if isinstance(self.params, DictConfig):
            self.params = OmegaConf.to_container(self.params)
        # after (pre)validation, this contains parameter values
        self.validated_params = None
        # parameters protected from assignment (because they've been set on the command line, presumably)
        self._assignment_overrides = set()
        if self.skip_if_outputs and self.skip_if_outputs not in (OUTPUTS_EXISTS, OUTPUTS_FRESH):
            raise StepValidationError(f"step '{self.name}': invalid 'skip_if_outputs={self.skip_if_outputs}' setting")
        # the "skip" attribute is reevaluated at runtime since it may contain substitutions, but if it's set to a bool
        # constant, self._skip will be preset already
        # validate skip attribute
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

    def finalize(self, config=None, log=None, fqname=None, backend=None, nesting=0):
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
            self.cargo.apply_dynamic_schemas(self.params)
            self.params = self.cargo.flatten_param_dict(OrderedDict(), self.params)

            # if logger is not provided, then init one
            if log is None:
                log = stimela.logger().getChild(self.fqname)
                log.propagate = False

            # finalize the cargo
            self.cargo.finalize(config, log=log, fqname=self.fqname, backend=backend, nesting=nesting)

            # build dictionary of defaults from cargo
            self.defaults = {name: schema.default for name, schema in self.cargo.inputs_outputs.items() 
                             if schema.default is not UNSET and not isinstance(schema.default, Unresolved) }
            self.defaults.update(**self.cargo.defaults)
            
            # set missing parameters from defaults
            for name, value in self.defaults.items():
                if name not in self.params:
                    self.params[name] = value

            # check for valid backend
            backend_opts = OmegaConf.to_object(OmegaConf.merge(
                StimelaBackendSchema,
                backend or {}, 
                self.cargo.backend or {}, 
                self.backend or {}))
            runner.validate_backend_settings(backend_opts, log=log)

    def prevalidate(self, subst: Optional[SubstitutionNS]=None, root=False, backend=None):
        self.finalize(backend=backend)
        # apply dynamic schemas
        params = self.params
        if self.cargo.has_dynamic_schemas:
            # prevalidate in order to resolve substitutions in existing parameters
            params = self.cargo.prevalidate(params, subst, root=root)
            self.cargo.apply_dynamic_schemas(params, subst)
            # will prevvalidate again below based on these updated schemas
        # validate cab or recipe
        params = self.validated_params = self.cargo.prevalidate(params, subst, root=root)
        # add missing outputs
        for name, schema in self.cargo.outputs.items():
            if name not in params and schema.required:
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
                self.log.log(level, escape(line), extra=extra)

    def log_exception(self, exc, severity="error", log=None):
        log_exception(exc, severity=severity, log=log or self.log)

    def assign_value(self, key: str, value: Any, override: bool = False):
        """assigns parameter value or nested variable value to this step

        Args:
            key (str): name
            value (Any): value
            override (bool): If True, value will override all future assignments (used for command-line overrides)
                             Defaults to False.
        """
        # ignore assignment if an override assignment was done earlier
        if key in self._assignment_overrides and not override:
            return
        if override:
            self._assignment_overrides.add(key)
        # assigning parameter directly? Add to self.params
        if key in self.inputs_outputs:
            self.params[key] = value
            # and remove from prevalidated params
            if self.validated_params and key in self.validated_params:
                del self.validated_params[key]
        # else delegate to cargo to assign
        else:
            try:
                self.cargo.assign_value(key, value, override=override)
            except ScabhaBaseException as exc:
                raise AssignmentError(f"{self.name}: invalid assignment {key}={value}", exc)


    def build(self, backend=None, rebuild=False, build_skips=False, log: Optional[logging.Logger] = None):
        # skipping step? ignore the build
        if self.skip is True and not build_skips:
            return
        backend = OmegaConf.merge(backend or {}, self.cargo.backend or {}, self.backend or {})
        log = log or self.log
        # recurse into sub-recipe 
        from .recipe import Recipe
        if type(self.cargo) is Recipe:
            return self.cargo.build(backend, rebuild=rebuild, build_skips=build_skips, log=log)
        # else build 
        else:
            # validate backend settings and call the build function
            try:
                backend_opts = OmegaConf.merge(self.config.opts.backend, backend)
                if getattr(backend_opts, 'verbose', 0):
                    opts_yaml = OmegaConf.to_yaml(backend_opts)
                    log_rich_payload(self.log, "effective backend settings are", opts_yaml, syntax="yaml") 
                backend_opts = OmegaConf.to_object(OmegaConf.merge(stimela.CONFIG.opts.backend, backend_opts))
                backend_runner = runner.validate_backend_settings(backend_opts, log=log)
            except Exception as exc:
                newexc = BackendError("error validating backend settings", exc)
                raise newexc from None
            log.info(f"building image for step '{self.fqname}' using the {backend_runner.backend_name} backend")
            with task_stats.declare_subtask(self.name):
                return backend_runner.build(self.cargo, log=log, rebuild=rebuild)


    def run(self, backend: Optional[Dict] = None, subst: Optional[Dict[str, Any]] = None, 
            is_outer_step: bool=False,
            parent_log: Optional[logging.Logger] = None) -> Dict[str, Any]:
        """executes the step

        Args:
            backend (Dict, optional): Backend settings inherited from parent.
            subst (Dict[str, Any], optional): Substitution namespace. Defaults to None.
            parent_log (logging.Logger, optional): parent logger for parent-related messages. Defaults to using the step logger if not supplied.

        Raises:
            StepValidationError: _description_
            StepValidationError: _description_
            StepValidationError: _description_
            StimelaCabRuntimeError: _description_
            RuntimeError: _description_
            StepValidationError: _description_

        Returns:
            Dict[str, Any]: step outputs
        """

        from .recipe import Recipe

        # some messages go to the parent logger -- if not defined, default to our own logger
        if parent_log is None:
            parent_log = self.log

        backend = OmegaConf.merge(backend or {}, self.cargo.backend or {}, self.backend or {})

        # validate backend settings
        try:
            backend_opts = OmegaConf.merge(self.config.opts.backend, backend)
            backend_opts = evaluate_and_substitute_object(backend_opts, subst, 
                                                          recursion_level=-1, location=[self.fqname, "backend"])
            if not is_outer_step and backend_opts.verbose:
                opts_yaml = OmegaConf.to_yaml(backend_opts)
                log_rich_payload(self.log, "current backend settings are", opts_yaml, syntax="yaml") 
            backend_opts = OmegaConf.merge(StimelaBackendSchema, backend_opts)
            backend_opts = OmegaConf.to_object(backend_opts)
            backend_runner = runner.validate_backend_settings(backend_opts, log=self.log)
        except Exception as exc:
            newexc = BackendError("error validating backend settings", exc)
            raise newexc from None

        # if step is being explicitly skipped, omit from profiling, and drop info/warning messages to debug level
        explicit_skip = self.skip is True 
        if explicit_skip:
            context = nullcontext()
            parent_log_info = parent_log_warning = parent_log.debug
        else:
            context = task_stats.declare_subtask(self.name, hide_local_metrics=backend_runner.is_remote)
            stimelogging.declare_chapter(f"{self.fqname}")
            parent_log_info, parent_log_warning = parent_log.info, parent_log.warning

        if self.validated_params is None:
            self.prevalidate(self.params)

        with context:
            # evaluate the skip attribute (it can be a formula and/or a {}-substititon)
            skip = self._skip
            if self._skip is None and subst is not None:
                skip = evaluate_and_substitute_object(self.skip, subst, 
                                                      location=[self.fqname, "skip"])
                if skip is UNSET:  # skip: =IFSET(recipe.foo) will return UNSET
                    skip = False
                self.log.debug(f"dynamic skip attribute evaluation returns {skip}")
                # formulas with unset variables return UNSET instance
                if isinstance(skip, UNSET):
                    if skip.errors:
                        raise StepValidationError(f"{self.fqname}.skip: error evaluating '{self.skip}'", skip.errors)
                    else:
                        raise StepValidationError(f"{self.fqname}.skip: error evaluating '{self.skip}'", SubstitutionError(f"unknown variable '{skip.value}'"))

            # Since prevalidation will have populated default values for potentially missing parameters, use those values
            # For parameters that aren't missing, use whatever value that was suplied
            # preserve order of specified params, to allow ordered substitutions to occur
            params = self.params.copy()
            params.update([(key, value) for key, value in self.validated_params.items() if key not in params])

            skip_warned = False   # becomes True when warnings are given

            self.log.debug(f"validating inputs {subst and list(subst.keys())}")
            validated = None
            try:
                params = self.cargo.validate_inputs(params, loosely=skip, remote_fs=backend_runner.is_remote_fs, subst=subst)
                validated = True

            except ScabhaBaseException as exc:
                severity = "warning" if skip else "error"
                level = logging.WARNING if skip else logging.ERROR
                if not exc.logged and not explicit_skip:
                    if type(exc) is SubstitutionErrorList:
                        self.log_exception(StepValidationError(f"unresolved {{}}-substitution(s) in inputs:", exc.nested), 
                                           severity=severity)
                        # for err in exc.errors:
                        #     self.log.log(level, f"  {err}")
                    else:
                        self.log_exception(StepValidationError(f"error validating inputs:", exc), 
                                           severity=severity)
                    exc.logged = True
                if not explicit_skip:
                    self.log_summary(level, "summary of inputs follows", color="WARNING", inputs=True)
                # raise up, unless step is being skipped
                if skip:
                    parent_log_warning("since the step is being skipped, this is not fatal")
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
                if skip:
                    parent_log_warning(f"invalid inputs: {join_quote(invalid)}")
                    if not skip_warned:
                        parent_log_warning("since the step was skipped, this is not fatal")
                        skip_warned = True
                else:
                    raise StepValidationError(f"step '{self.name}': invalid inputs: {join_quote(invalid)}", log=self.log)

            ## check if we need to skip based on existing/fresh file outputs
            skip_if_outputs = self.skip_if_outputs
            # don't check if skipping anyway
            if skip:
                skip_if_outputs = None
            # don't check if remote filesystem
            elif backend_runner.is_remote_fs:
                parent_log_info(f"ignoring skip_if_outputs: {skip_if_outputs} because backend has remote filesystem")
                skip_if_outputs = None
            # don't check if force-disabled
            elif (skip_if_outputs == OUTPUTS_EXISTS and stimela.CONFIG.opts.disable_skips.exist) or \
                    (skip_if_outputs == OUTPUTS_FRESH and stimela.CONFIG.opts.disable_skips.fresh):
                parent_log_info(f"ignoring skip_if_outputs: {skip_if_outputs} because it has been force-disabled")
                skip_if_outputs = None

            ## if skip on fresh outputs is in effect, find mtime of most recent input 
            if skip_if_outputs:
                # max_mtime will remain 0 if we're not echecking for freshness, or if there are no file-type inputs
                max_mtime, max_mtime_path = 0, None
                if skip_if_outputs == OUTPUTS_FRESH:
                    parent_log_info("checking if file-type outputs of step are fresh")
                    for name, value in params.items():
                        schema = self.inputs_outputs[name]
                        if schema.is_input and not schema.skip_freshness_checks:
                            if schema.is_file_type:
                                values = [value]
                            elif schema.is_file_list_type:
                                values = value
                            else:
                                continue
                            for filename in values:
                                if type(filename) is str and os.path.exists(filename):
                                    mtime = os.path.getmtime(filename)
                                    if mtime > max_mtime:
                                        max_mtime = mtime
                                        max_mtime_path = filename
                    if max_mtime:
                        parent_log_info(f"  most recently modified input is {max_mtime_path} ({time.ctime(max_mtime)})")
                else:
                    parent_log_info("checking if file-type outputs of step exist")

                ## now go through outputs -- all_exist flag will be cleared if we find one that doesn't exist,
                ## or is older than an input
                all_exist = True
                for name, schema in self.outputs.items():
                    # ignore outputs not in params (implicit outputs will be already in there thanks to validation above)
                    if name in params:
                        # check for files or lists of files, and skip otherwise
                        if schema.is_file_type:
                            filenames = [params[name]]
                        elif schema.is_file_list_type:
                            filenames = params[name]
                            # empty list of files treated as non-existing output
                            if not filenames:
                                if schema.must_exist:
                                    all_exist = False
                                    parent_log_info(f"  {name}: no existing file(s)")
                                    break  # abort the check
                                else:
                                    parent_log_info(f"  {name}: no existing file(s), but they are not required")
                                    continue
                        else:
                            continue # go on to next parameter
                        # collect messages rather than logging them directly, to avoid log diarrhea for long file lists
                        messages = []
                        # ok, we have a list of files to check
                        for num, value in enumerate(filenames):
                            if type(value) is not str:  # skip funny values that aren't strings
                                continue
                            # form up label for messages
                            label = f"{name}[{num}]" if schema.is_file_list_type else name
                            if os.path.exists(value):
                                # max_mtime==0 means we're only checking for existence, not freshness
                                if max_mtime:
                                    if schema.skip_freshness_checks:
                                        messages.append(f"{label} = {value} marked as skipped from freshness checks")
                                    else:
                                        mtime = os.path.getmtime(value)
                                        if mtime < max_mtime:
                                            parent_log_info(f"{label} = {value} is not fresh")
                                            all_exist = False
                                            break
                                        else:
                                            messages.append(f"{label} = {value} is fresh")
                                else:
                                    messages.append(f"{label} = {value} exists")
                            elif schema.must_exist is not False:
                                all_exist = False
                                parent_log_info(f"  {label} = {value} doesn't exist")
                                break
                            else:
                                messages.append(f"{label} = {value} doesn't exist, but is not required to")
                        # abort the checks if we encountered a fail
                        if not all_exist:
                            break
                        # else log the collected messages
                        if len(messages) > 2:
                            messages = [messages[0], "  ...", messages[-1]]
                        for msg in messages:
                            parent_log_info(f"  {msg}")
                if all_exist:
                    parent_log_info("all required outputs are OK, skipping this step")
                    skip = True

            if not skip:
                # check for outputs that need removal
                if not backend_runner.is_remote_fs:
                    for name, schema in self.outputs.items():
                        if name in params and schema.path_policies.remove_if_exists and schema.is_file_type:
                            path = params[name]
                            if type(path) is str and os.path.exists(path):
                                if os.path.isdir(path) and not os.path.islink(path):
                                    shutil.rmtree(path)
                                else:
                                    os.unlink(path)

                if type(self.cargo) is Recipe:
                    self.cargo._run(params, subst, backend=backend)
                elif type(self.cargo) is Cab:
                    cabstat = backend_runner.run(self.cargo, params=params, log=self.log, subst=subst, fqname=self.fqname)
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
                    parent_log_info(f"skipping step based on conditonal settings")
                else:
                    parent_log.debug("skipping step based on explicit setting")

            self.log.debug(f"validating outputs")
            validated = False

            try:
                params = self.cargo.validate_outputs(params, loosely=skip,remote_fs=backend_runner.is_remote_fs, subst=subst)
                validated = True
            except ScabhaBaseException as exc:
                severity = "warning" if skip else "error"
                level = logging.WARNING if self.skip else logging.ERROR
                if not exc.logged:
                    if type(exc) is SubstitutionErrorList:
                        self.log_exception(StepValidationError(f"unresolved {{}}-substitution(s) in outputs:", exc.nested), severity=severity)
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
            invalid = [name for name in self.invalid_params + self.unresolved_params 
                        if name in self.cargo.outputs and self.cargo.outputs[name].required is not False]
            if invalid:
                if skip:
                    parent_log_warning(f"invalid outputs: {join_quote(invalid)}")
                    parent_log_warning("since the step was skipped, this is not treated as an error for now, but may cause errors downstream")
                    for key in invalid:
                        params[key] = SkippedOutput(key)
                else:
                    # check if invalid steps are due to subrecipe with skipped steps, ignpre those
                    truly_invalid = [name for name in invalid if not isinstance(params.get(name), SkippedOutput)]
                    if truly_invalid:
                        raise StepValidationError(f"invalid outputs: {join_quote(truly_invalid)}", log=self.log)
                    parent_log_warning(f"invalid outputs: {join_quote(invalid)}")
                    parent_log_warning("since some sub-steps were skipped, this is not treated as an error for now, but may cause errors downstream")

        return params

