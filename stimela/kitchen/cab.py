import os.path, re, stat, itertools, logging, yaml, shlex
from typing import Any, List, Dict, Optional, Union
from collections import OrderedDict
from enum import Enum, IntEnum
from dataclasses import dataclass
from omegaconf import MISSING

from scabha.cargo import Parameter, Cargo, ListOrString, ParameterPolicies, ParameterCategory
from stimela.exceptions import CabValidationError
from scabha.exceptions import SchemaError
from scabha.basetypes import EmptyDictDefault, EmptyListDefault
import stimela

ParameterPassingMechanism = Enum("ParameterPassingMechanism", "args yaml", module=__name__)


@dataclass 
class CabManagement:        # defines common cab management behaviours
    environment: Optional[Dict[str, str]] = EmptyDictDefault()
    cleanup: Optional[Dict[str, ListOrString]]     = EmptyDictDefault()   
    wranglers: Optional[Dict[str, ListOrString]]   = EmptyDictDefault()   


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
    image: Optional[str] = None                   

    # command to run, inside the container or natively
    command: str = MISSING

    # if set, activates this virtual environment first before running the command (not much sense doing this inside the container)
    virtual_env: Optional[str] = None

    # cab flavour. Default will run the command as a binary (inside image or virtual_env). "python" will treat the command
    # as a Python package.module.function specification. 
    #   Future examples would be e.g. "casa" to treat it as a CASA task 
    flavour: Optional[str] = None  

    # controls how params are passed. args: via command line argument, yml: via a single yml string
    parameter_passing: ParameterPassingMechanism = ParameterPassingMechanism.args

    # cab management and cleanup definitions
    management: CabManagement = CabManagement()

    # default parameter conversion policies
    policies: ParameterPolicies = ParameterPolicies()

    # runtime settings
    backend: Optional['stimela.config.Backend']
    runtime: Dict[str, Any] = EmptyDictDefault()

    # copy names of logging levels into wrangler actions
    wrangler_actions =  {attr: value for attr, value in logging.__dict__.items() if attr.upper() == attr and type(value) is int}

    # then add litetal constants for other wrangler actions
    ACTION_SUPPRESS = wrangler_actions["SUPPRESS"] = "SUPPRESS"
    ACTION_DECLARE_SUCCESS = wrangler_actions["DECLARE_SUCCESS"] = "DECLARE_SUPPRESS"
    ACTION_DECLARE_FAILURE = wrangler_actions["DECLARE_FAILURE"] = "DECLARE_FAILURE"

    _path: Optional[str] = None   # path to image definition yaml file, if any

    def __post_init__ (self):
        if self.name is None:
            self.name = self.image or self.command.split()[0]
        Cargo.__post_init__(self)
        for param in self.inputs.keys():
            if param in self.outputs:
                raise CabValidationError(f"cab {self.name}: parameter '{param}' appears in both inputs and outputs")
        # check flavours
        match_old_python = re.match("^\((.+)\)(.+)$", self.command)
        if match_old_python:
            if self.flavour is not None and self.flavour.lower() != "python":
                raise CabValidationError("cab {self.name}: '(module)function' implies python flavour, but '{self.flavour}' is specified")
            self.flavour = "python"
            self.py_module, self.py_function = match_old_python.groups()
        else:
            if self.flavour is None:
                self.flavour = "binary" 
            else:
                self.flavour = self.flavour.lower()
            if self.flavour == "python" or self.flavour == "python-ext":
                if '.' in self.command:
                    self.py_module, self.py_function = self.command.rsplit('.', 1)
                else:
                    raise CabValidationError("cab {self.name}: 'python' flavour requires a command of the form module.function")
            elif self.flavour != "binary":
                raise CabValidationError("cab {self.name}: unknown cab flavour '{self.flavour}'")

        # setup wranglers
        self._wranglers = []
        for match, actions in self.management.wranglers.items():
            replace = None
            if type(actions) is str:
                actions = [actions]
            if type(actions) is not list:
                raise CabValidationError(f"wrangler entry {match}: expected action or list of actions")
            for action in actions:
                if action.startswith("replace:"):
                    replace = action.split(":", 1)[1]
                elif action not in self.wrangler_actions:
                    raise CabValidationError(f"wrangler entry {match}: unknown action '{action}'")
            actions = [self.wrangler_actions[act] for act in actions if act in self.wrangler_actions]
            try:
                rexp = re.compile(match)
            except Exception as exc:
                raise CabValidationError(f"wrangler entry {match} is not a valid regular expression")
            self._wranglers.append((re.compile(match), replace, actions))
        self._runtime_status = None


    def summary(self, params=None, recursive=True, ignore_missing=False):
        lines = [f"cab {self.name}:"] 
        if params is not None:
            for name, value in params.items():
                # if type(value) is validate.Error:
                #     lines.append(f"  {name} = ERR: {value}")
                # else:
                lines.append(f"  {name} = {value}")
            lines += [f"  {name} = ???" for name, schema in self.inputs_outputs.items()
                        if name not in params and (not ignore_missing or schema.required)]
        return lines

    def rich_help(self, tree, max_category=ParameterCategory.Optional):
        tree.add(f"command: {self.command}")
        if self.image:
            tree.add(f"image: {self.image}")
        if self.virtual_env:
            tree.add(f"virtual environment: {self.virtual_env}")
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

    def build_command_line(self, params: Dict[str, Any], subst: Optional[Dict[str, Any]] = None, search=True):
        from scabha.substitutions import substitutions_from

        try:
            with substitutions_from(subst, raise_errors=True) as context:
                venv = context.evaluate(self.virtual_env, location=["virtual_env"])
                command = context.evaluate(self.command, location=["command"])
        except Exception as exc:
            raise CabValidationError(f"error constructing cab command", exc)


        if venv:
            venv = os.path.expanduser(venv)
            if not os.path.isfile(f"{venv}/bin/activate"):
                raise CabValidationError(f"virtual environment {venv} doesn't exist")
            self.log.debug(f"virtual environment is {venv}")
        else:
            venv = None

        command_line = shlex.split(os.path.expanduser(command))
        command = command_line[0]
        args = command_line[1:]
        # collect command
        if search:
            if "/" not in command:
                from scabha.proc_utils import which
                command0 = command
                command = which(command, extra_paths=venv and [f"{venv}/bin"])
                if command is None:
                    raise CabValidationError(f"{command0}: not found", log=self.log)
            else:
                if not os.path.isfile(command) or not os.stat(command).st_mode & stat.S_IXUSR:
                    raise CabValidationError(f"{command} doesn't exist or is not executable")

        self.log.debug(f"command is {command}")

        return ([command] + args + self.build_argument_list(params)), venv


    def build_argument_list(self, params):
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

        value_dict = dict(**params)

        if self.parameter_passing is ParameterPassingMechanism.yaml:
            return [yaml.safe_dump(value_dict)]

        def get_policy(schema: Parameter, policy: str, default=None):
            return self.get_schema_policy(schema, policy, default)

        def stringify_argument(name, value, schema, option=None):
            key_value = get_policy(schema, 'key_value')
            if key_value:
                return f"{name}={value}"

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
                    if len(format_list_policy) != len(value):
                        raise CabValidationError("length of format_list_policy does not match length of '{name}'", log=self.log)
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
                    value = [" ".join(value)]
                    return [option] + value if option else value
                elif repeat_policy == "[]":
                    val = "[" + ",".join(value) + "]"
                    return [option] + [val] if option else val
                elif repeat_policy == "repeat":
                    return list(itertools.chain([option, x] for x in value)) if option else list(value)
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
                skip = get_policy(schema, 'skip') or (schema.implicit and get_policy(schema, 'skip_implicits', True))
                if positional:
                    if not skip:
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

            # default behaviour for unset skip_implicits is True
            skip_implicits = get_policy(schema, 'skip_implicits', True)

            if get_policy(schema, 'skip') or (schema.implicit and skip_implicits):
                continue

            key_value = get_policy(schema, 'key_value')
            key_value_command_line = get_policy(schema, 'key_value_command_line')

            # apply replacementss
            replacements = get_policy(schema, 'replace')
            if replacements:
                for rep_from, rep_to in replacements.items():
                    try:
                        name = name.replace(rep_from, rep_to)
                    except TypeError:
                        raise TypeError(f"Could not perform policy replacement for parameter [{name}] : {rep_from} => {rep_to}")

            option = (get_policy(schema, 'prefix') or "--") + (schema.nom_de_guerre or name)

            if schema.dtype == "bool":
                if key_value:
                    args += [f"{name}={value}"]
                else:
                    explicit = get_policy(schema, 'explicit_true' if value else 'explicit_false')
                    args += [option, str(explicit)] if explicit is not None else ([option] if value else [])
            else:
                
                value = stringify_argument(name, value, schema, option=option)
                if type(value) is list:
                    if key_value_command_line:
                        if len(value) > 1:
                            value = [f"{value[0]}={value[1]}"]
                    args += value
                elif value is not None:
                    args.append(value)

        return pos_args[0] + args + pos_args[1]


    @property
    def runtime_status(self):
        return self._runtime_status

    def reset_runtime_status(self):
        self._runtime_status = None

    def apply_output_wranglers(self, output, severity):
        suppress = False
        modified_output = output
        for regex, replace, actions in self._wranglers:
            if regex.search(output):
                if replace is not None:
                    modified_output = regex.sub(replace, output)
                for action in actions:
                    if type(action) is int:
                        severity = action
                    elif action is self.ACTION_SUPPRESS:
                        suppress = True
                    elif action is self.ACTION_DECLARE_FAILURE and self._runtime_status is None:
                        self._runtime_status  = False
                        modified_output = "[FAILURE] " + modified_output
                        severity = logging.ERROR
                    elif action is self.ACTION_DECLARE_SUCCESS and self._runtime_status is None:
                        self._runtime_status = True
                        modified_output = "[SUCCESS] " + modified_output
        return (None, 0) if suppress else (modified_output, severity)



