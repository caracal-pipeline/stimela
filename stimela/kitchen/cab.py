import os.path
import itertools
import yaml
import shlex
import re
from typing import Any, List, Dict, Optional, Union
from collections import OrderedDict
from enum import Enum
from dataclasses import dataclass
from omegaconf import MISSING, OmegaConf, DictConfig
from omegaconf.errors import OmegaConfBaseException
import rich.markup

from scabha.cargo import Parameter, Cargo, ListOrString, ParameterPolicies, ParameterCategory
from stimela.exceptions import CabValidationError, StimelaCabRuntimeError, StimelaBaseImageError
from scabha.exceptions import SchemaError
from scabha.basetypes import EmptyDictDefault, EmptyListDefault, EmptyClassDefault
from stimela.backends import flavours, StimelaBackendSchema
from . import wranglers
from scabha.substitutions import substitutions_from

ParameterPassingMechanism = Enum("ParameterPassingMechanism", "args yaml", module=__name__)


@dataclass 
class CabManagement(object):        # defines common cab management behaviours
    environment: Optional[Dict[str, str]] = EmptyDictDefault()
    cleanup: Optional[Dict[str, ListOrString]]     = EmptyDictDefault()   
    wranglers: Optional[Dict[str, ListOrString]]   = EmptyDictDefault()   

@dataclass
class ImageInfo(object):
    name: Optional[str] = None          # image name
    registry: Optional[str] = None      # registry/org or org (for Dockerhub)
    version: str = "latest"
    path: Optional[str] = None          # prebuilt image path (for some backends only)

    def __post_init__(self):
        if not self.name:
            if not self.path:
                raise StimelaBaseImageError("image name or path must be specified")
            self.name = os.path.basename(self.path)

    @staticmethod
    def from_string(spec: str):
        """Creates ImageInfo from string"""
        # get version specs
        if ":" in spec:
            spec, version = spec.rsplit(":", 1)
        else:
            version = "latest"
        # get registry
        if "/" in spec:
            registry, name = spec.rsplit("/", 1)
        else:
            registry, name = None, spec
        return ImageInfo(name, registry, version)
    
    def to_string(self):
        if self.registry:
            return f"{self.registry}/{self.name}:{self.version}"
        else:        
            return f"{self.name}:{self.version}"
        
    def __str__(self):
        return self.to_string()

ImageInfoSchema = OmegaConf.structured(ImageInfo)

@dataclass
class Cab(Cargo):
    """Represents a cab i.e. an atomic task in a recipe.
    See dataclass fields below for documentation of fields.

    Additional attributes available after validation with arguments:

        self.input_output:      combined parameter dict (self.input + self.output), maps name to Parameter
        self.missing_params:    dict (name to Parameter) of required parameters that have not been specified
    
    Raises:
        CabValidationError: [description]
    """
    # if set, the cab is run in a container, and this is the image name
    # if not set, commands are run by the native runner
    image: Optional[Any] = None                   

    # command to run, inside the container or natively
    # this is not split into individual arguments, but passed to sh -c as is
    command: str = MISSING

    # optional arguments to be passed to command, before any stimela-formed arguments
    args: List[str] = EmptyListDefault()

    ## moved to backend: native
    # # if set, activates this virtual environment first before running the command (not much sense doing this inside the container)
    # virtual_env: Optional[str] = None

    # cab flavour. Default will run the command as a binary (inside image or virtual_env). Otherwise specify
    # a string flavour, or a mapping to specify options (see backends.flavours)
    flavour: Optional[Any] = None

    # overrides backend options
    backend: Optional[Dict[str, Any]] = None

    # controls how params are passed. args: via command line argument, yml: via a single yml string
    parameter_passing: ParameterPassingMechanism = ParameterPassingMechanism.args

    # cab management and cleanup definitions
    management: CabManagement = EmptyClassDefault(CabManagement)

    # default parameter conversion policies
    policies: ParameterPolicies = EmptyClassDefault(ParameterPolicies)

    def __post_init__ (self):
        Cargo.__post_init__(self)
        for param in self.inputs.keys():
            if param in self.outputs:
                raise CabValidationError(f"cab {self.name}: parameter '{param}' appears in both inputs and outputs")
            
        # check image setting
        if self.image:
            if type(self.image) is str:
                self.image = ImageInfo.from_string(self.image)
            elif isinstance(self.image, DictConfig):
                try:
                    self.image = ImageInfo(**OmegaConf.merge(ImageInfoSchema, self.image))
                except OmegaConfBaseException as exc:
                    raise CabValidationError(f"cab {self.name}: invalid image setting", exc)
            else:
                raise CabValidationError(f"cab {self.name}: invalid image setting")

        # setup wranglers
        self._wranglers = []
        for pattern, actions in self.management.wranglers.items():
            self._wranglers.append(wranglers.create_list(pattern, actions))

        # check flavours
        self.flavour = flavours.init_cab_flavour(self)

        # set name from command or image
        if self.name is None:
            self.name = self.command or self.image

        # split off first word of name to avoid non-alphanumeric characters
        match = re.match(r"(\w+)", self.name)
        if match:
            self.name = match.group(1) or self.flavour.kind
        else:
            self.name = self.flavour.kind

        # check backend setting
        if self.backend:
            try:
                OmegaConf.merge(StimelaBackendSchema, self.backend)
            except OmegaConfBaseException as exc:
                raise CabValidationError(f"cab {self.name}: invalid backend setting", exc)


    def summary(self, params=None, recursive=True, ignore_missing=False):
        lines = [f"cab {self.name}:"] 
        if params is not None:
            Cargo.add_parameter_summary(params, lines)
            lines += [f"  {name} = ???" for name, schema in self.inputs_outputs.items()
                        if name not in params and (not ignore_missing or schema.required)]
        return lines

    def rich_help(self, tree, max_category=ParameterCategory.Optional):
        tree.add(f"command: {self.command}")
        if self.image:
            tree.add(f"image: {self.image}")
        ## moved to backend.native options
        # if self.virtual_env:
        #     tree.add(f"virtual environment: {self.virtual_env}")
        Cargo.rich_help(self, tree, max_category=max_category)

    def get_schema_policy(self, schema, policy, default=None):
        """Resolves a policy setting. If the policy is set here, returns it. If None and set in the cab,
        returns that. Else returns default value.
        """
        if getattr(schema.policies, policy) is not None:
            return getattr(schema.policies, policy)
        elif getattr(self.policies, policy) is not None:
            return getattr(self.policies, policy)
        else:
            return default

    def build_command_line(self, params: Dict[str, Any], 
                        subst: Optional[Dict[str, Any]] = None, 
                        virtual_env: Optional[str] = None,
                        check_executable: bool = True):
        
        try:
            with substitutions_from(subst, raise_errors=True) as context:
                command = context.evaluate(self.command, location=["command"])
                args = [context.evaluate(arg, location=[f"args[{i}]"]) for i, arg in enumerate(self.args)]
        except Exception as exc:
            raise CabValidationError(f"error constructing cab command", exc)

        # # collect command
        # if check_executable:
        #     if "/" not in command:
        #         from scabha.proc_utils import which
        #         command0 = command
        #         command = which(command, extra_paths=virtual_env and [f"{virtual_env}/bin"])
        #         if command is None:
        #             raise CabValidationError(f"{command0}: not found", log=self.log)
        #     else:
        #         if not os.path.isfile(command) or not os.stat(command).st_mode & stat.S_IXUSR:
        #             raise CabValidationError(f"{command} doesn't exist or is not executable")

        self.log.debug(f"command is {command}")

        return shlex.split(command) + args + self.build_argument_list(params)

    def update_environment(self, subst):
        try:
            with substitutions_from(subst, raise_errors=True) as context:
                environ = CabManagement().environment
                for key,val in self.management.environment.items():
                    environ[key] = context.evaluate(val,
                                location=["management", "environment"])
        except Exception as exc:
            raise CabValidationError(f"Error applying environment variables", exc)

        if environ:
            self.management.environment = environ


    def filter_input_params(self, params: Dict[str, Any], apply_nom_de_guerre=True):
        """Filters dict of params, returning only those that should be passed to a cab
        (i.e. inputs or named outputs, and not skipped and not implicit)
        """
        filtered_params = OrderedDict()
        for name, schema in self.inputs_outputs.items():
            # get skip setting
            skip = self.get_schema_policy(schema, 'skip')
            if skip is None and schema.implicit:
                skip = self.get_schema_policy(schema, 'skip_implicits')
            # skip if explicitly True
            if skip:
                continue
            # skip non-named outputs, unless skip is explicitly False
            if not (schema.is_input or schema.is_named_output) and skip is not False:
                continue
            # ok, definitely not skipping now
            output_name = (apply_nom_de_guerre and schema.nom_de_guerre) or name
            if name in params:
                filtered_params[output_name] = params[name]
            elif self.get_schema_policy(schema, 'pass_missing_as_none'):
                filtered_params[output_name] = None
        return filtered_params


    def build_argument_list(self, params: Dict[str, Any]):
        """
        Converts command, and current dict of parameters, into a list of command-line arguments.

        pardict:     dict of parameters. If None, pulled from default config.
        positional:  list of positional parameters, if any
        mandatory:   list of mandatory parameters.
        repeat:      How to treat iterable parameter values. If a string (e.g. ","), list values will be passed as one
                    command-line argument, joined by that separator. If True, list values will be passed as
                    multiple repeated command-line options. If None, list values are not allowed.
        repeat_dict: Like repeat, but defines this behaviour per parameter. If supplied, then "repeat" is used
                    as the default for parameters not in repeat_dict.

        Returns list of arguments.
        """

        # collect parameters
        # apply filtering logic here (removing it below)
        value_dict = self.filter_input_params(params, apply_nom_de_guerre=False)

        if self.parameter_passing is ParameterPassingMechanism.yaml:
            return [yaml.safe_dump(value_dict)]

        def get_policy(schema: Parameter, policy: str, default=None):
            return self.get_schema_policy(schema, policy, default)

        def stringify_argument(name, value, schema, option=None):
            key_value = get_policy(schema, 'key_value')

            if value is None:
                return None
            if schema.dtype == "bool" and not value and get_policy(schema, 'explicit_false') is None:
                return None

            is_list = hasattr(value, '__iter__') and type(value) is not str
            format_policy = get_policy(schema, 'format')
            format_list_policy = get_policy(schema, 'format_list')
            format_scalar_policy = get_policy(schema, 'format_list_scalar')
            split_policy = get_policy(schema, 'split')
            
            if type(value) is str and split_policy:
                value = value.split(split_policy or None)
                is_list = True

            if is_list:
                # apply formatting policies to a list of values
                if format_list_policy:
                    value = [fmt.format(*value, **value_dict) for fmt in format_list_policy]
                elif format_policy:
                    value = [format_policy.format(x, **value_dict) for x in value]
                else:
                    value = [str(x) for x in value]
            else:
                # apply formatting policies to a scalar valye
                if format_scalar_policy:
                    value = [fmt.format(value, **value_dict) for fmt in format_scalar_policy]
                    is_list = True
                elif format_policy:
                    value = format_policy.format(value, **value_dict)
                else:
                    value = str(value)

            if is_list:
                # check repeat policy and form up representation
                repeat_policy = get_policy(schema, 'repeat')
                if repeat_policy == "list":
                    if key_value:
                        raise CabValidationError(f"Repeat policy 'list' is incompatible with schema policy 'key_value' for parameter '{name}'")
                    return [option] + list(value) if option else list(value)
                elif repeat_policy == "[]":
                    val = "[" + ",".join(value) + "]"
                    return [option] + [val] if option else val
                elif repeat_policy == "repeat":
                    return list(itertools.chain.from_iterable([option, x] for x in value)) if option else list(value)
                elif type(repeat_policy) is str:
                    return [option, repeat_policy.join(value)] if option else repeat_policy.join(value)
                elif repeat_policy is None:
                    raise CabValidationError(f"list-type parameter '{name}' does not have a repeat policy set", log=self.log)
                else:
                    raise SchemaError(f"unknown repeat policy '{repeat_policy}'", log=self.log)
            else:
                return [option, value] if option else [value]

        # check for missing parameters and collect positionals

        pos_args = [], []

        for name, schema in self.inputs_outputs.items():
            if schema.required and name not in value_dict:
                raise CabValidationError(f"required parameter '{name}' is missing", log=self.log)
            if name in value_dict:
                positional_first = get_policy(schema, 'positional_head') 
                positional = get_policy(schema, 'positional') or positional_first
                if positional:
                    pargs = pos_args[0 if positional_first else 1]
                    value = stringify_argument(name, value_dict[name], schema)
                    if type(value) is list:
                        pargs += value
                    elif value is not None:
                        pargs.append(value)
                    value_dict.pop(name)

        args = []
                    
        # now check for optional parameters that remain in the dict
        for name, value in value_dict.items():
            if name not in self.inputs_outputs:
                raise RuntimeError(f"unknown parameter '{name}'")
            schema = self.inputs_outputs[name]

            key_value = get_policy(schema, 'key_value')

            # apply replacementss
            replacements = get_policy(schema, 'replace')
            if replacements:
                for rep_from, rep_to in replacements.items():
                    try:
                        name = name.replace(rep_from, rep_to)
                    except TypeError:
                        raise TypeError(f"Could not perform policy replacement for parameter [{name}] : {rep_from} => {rep_to}")

            prefix = get_policy(schema, 'prefix')
            if prefix is None:
                prefix = "--"
            option = prefix + (schema.nom_de_guerre or name)

            if schema.dtype == "bool":
                explicit = get_policy(schema, "explicit_" + str(value).lower()) 
                # if explicit setting is given, this also becomes the option value
                # in key=value mode, just give that value directly
                strval = str(value) if explicit is None else str(explicit)
                if key_value:
                    args += [f"{option}={strval}"]
                # in option mode, use --option value for explicit settings, 
                # else give option for True and omit for False. TODO: some tools may eventually need a policy for
                # passing --no-option for False.
                else:
                    args += [option, strval] if explicit is not None else ([option] if value else [])
            else:
                value = stringify_argument(name, value, schema, option=option)
                if type(value) is list:
                    if key_value:
                        assert len(value) == 2
                        value = [f"{value[0]}={value[1]}"]
                    args += value
                elif value is not None:
                    args.append(value)

        return pos_args[0] + args + pos_args[1]

    def reset_status(self, extra_wranglers: List = []):
        return Cab.RuntimeStatus(self, extra_wranglers=extra_wranglers)

    class RuntimeStatus(object):
        """Represents the runtime status of a cab"""

        def __init__(self, cab: "Cab", extra_wranglers: List = []):
            self.cab = cab
            self.wranglers = list(cab._wranglers) + list(extra_wranglers)
            self._success = None
            self._errors = []
            self._warnings = []
            self._outputs = OrderedDict()

        @property
        def success(self):
            return self._success

        @property
        def errors(self):
            return self._errors

        @property
        def warnings(self):
            return self._warnings

        @property
        def outputs(self):
            return self._outputs

        def declare_success(self):
            if self._success is None:
                self._success = True

        def declare_failure(self, error: Optional[Union[str, Exception]] = None):
            self._success = False
            if error is not None:
                if type(error) is str:
                    error = StimelaCabRuntimeError(error)
                self._errors.append(error)

        def declare_warning(self, message: str):
            self._warnings.append(message)

        def declare_outputs(self, outputs: Dict):
            self._outputs.update(**outputs)

        def apply_wranglers(self, output, severity):
            # make sure any unintended [rich style] tags are escaped in output
            output = rich.markup.escape(output)
            suppress = False
            for regex, wranglers in self.wranglers:
                match = regex.search(output) 
                if match:
                    for wrangler in wranglers:
                        mod_output, mod_severity = wrangler.apply(self, output, match)
                        # has wrangler asked to suppress the output?
                        if mod_output is None:
                            suppress = True
                        else:
                            output = mod_output
                        # has wrangler modified the severity?
                        if mod_severity is not None:
                            severity = max(severity, mod_severity)

            return (None, 0) if suppress else (output, severity)

CabSchema = None

def get_cab_schema():
    global CabSchema
    if CabSchema is None:
        CabSchema = OmegaConf.structured(Cab)
    return CabSchema
