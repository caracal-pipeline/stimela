import re, os.path,json
from typing import Optional, Any, Union, Dict
from dataclasses import dataclass

import stimela
from scabha.exceptions import SubstitutionError
from stimela.exceptions import CabValidationError
from stimela.kitchen.cab import Cab
from scabha.cab_utils import CAB_OUTPUT_PREFIX
from stimela.kitchen import wranglers
from scabha.substitutions import substitutions_from

from . import _CallableFlavour, _BaseFlavour


def form_python_function_arguments(cab: Cab, params: Dict[str, Any], subst: Dict[str, Any]):
    arguments = []
    for io in cab.inputs, cab.outputs:
        for key, schema in io.items():
            if not schema.policies.skip and schema.is_input or schema.is_named_output:
                if key in params:
                    arguments.append(f"{key}={repr(params[key])}")
                elif cab.get_schema_policy(schema, 'pass_missing_as_none'):
                    arguments.append(f"{key}=None")
    return arguments

def get_python_interpreter_args(cab: Cab, subst: Dict[str, Any]):
    # get virtual env, if specified
    with substitutions_from(subst, raise_errors=True) as context:
        venv = context.evaluate(cab.virtual_env, location=["virtual_env"])

    if venv:
        venv = os.path.expanduser(venv)
        interpreter = f"{venv}/bin/python"
        if not os.path.isfile(interpreter):
            raise CabValidationError(f"virtual environment {venv} doesn't exist")
    else:
        interpreter = "python"

    return [interpreter]


@dataclass
class PythonCallableFlavour(_CallableFlavour):
    kind: str = "python"
    # don't log full command by default, as that's full of code
    log_full_command: bool = False

    def finalize(self, cab: Cab):
        super().finalize(cab)
        if '.' in cab.command:
            self.py_module, self.py_function = cab.command.rsplit('.', 1)
        else:
            raise CabValidationError(f"cab {cab.name}: python flavour requires a command of the form module.function")
        # form up outputs handler
        if self.output is not None or self.output_dict:
            self._yield_output = f"print(f'{CAB_OUTPUT_PREFIX}{{json.dumps(retval)}}')"
            pattern = re.compile(f"{CAB_OUTPUT_PREFIX}(.*)")
            if self.output_dict:
                wrangler = wranglers.ParseJSONOutputDict(pattern, "PARSE_JSON")
            else:
                wrangler = wranglers.ParseOutput(pattern, "PARSE_OUTPUT", self.output, "json")
            wrangs = [wrangler]
            if not stimela.VERBOSE:
                wrangs.append(wranglers.Suppress(pattern, "SUPPRESS"))
            cab._wranglers.append((pattern, wrangs))
        else:
            self._yield_output = ""
        self.command_name = self.py_function

    def form_python_code(self, cab: Cab, params: Dict[str, Any], subst: Dict[str, Any]):
        # form list of arguments
        arguments = form_python_function_arguments(cab, params, subst)

        # form up command string
        return f"""
import sys, json
sys.path.append('.')
from {self.py_module} import {self.py_function}
try:
    from click import Command
except ImportError:
    Command = None
if Command is not None and isinstance({self.py_function}, Command):
    print("invoking callable {self.py_module}.{self.py_function}() (as click command) using external interpreter")
    {self.py_function} = {self.py_function}.callback
else:
    print("invoking callable {self.py_module}.{self.py_function}() using external interpreter")

retval = {self.py_function}({','.join(arguments)})
{self._yield_output}
        """

    def get_arguments(self, cab: Cab, params: Dict[str, Any], subst: Dict[str, Any]):
        args = get_python_interpreter_args(cab, subst)
        args += ["-c", self.form_python_code(cab, params, subst)]
        return args


@dataclass
class PythonCodeFlavour(_BaseFlavour):
    kind: str = "python-code"
    # if set to a string, inputs will be passed in as a dict assigned to a variable of that name
    input_dict: Optional[str] = None
    # if True, inputs will be passed in as named variables
    input_vars: bool = True
    # if True, outputs will be collected from named variables
    output_vars: bool = True
    # if True, command will have {}-substitutions done on it
    subst: bool = False
    # don't log full command by default, as that's full of code
    log_full_command: bool = False

    def finalize(self, cab: Cab):
        super().finalize(cab)
        pattern = re.compile(f"{CAB_OUTPUT_PREFIX}(.*)")
        wrangs = [wranglers.ParseJSONOutputDict(pattern, "PARSE_JSON")]
        if not stimela.VERBOSE:
            wrangs.append(wranglers.Suppress(pattern, "SUPPRESS"))
        cab._wranglers.append((pattern, wrangs))
        self.command_name = "[python]"

    def get_arguments(self, cab: Cab, params: Dict[str, Any], subst: Dict[str, Any]):
        if self.subst:
            with substitutions_from(subst, raise_errors=True) as context:
                try:
                    command = context.evaluate(cab.command, location=["command"])
                except Exception as exc:
                    raise SubstitutionError("error substituting inline Python code", exc)
        else:
            command = cab.command

        # only pass inputs and named outputs
        pass_params = {name: value for name, value in params.items()
                        if cab.inputs_outputs[name].is_input or cab.inputs_outputs[name].is_named_output
        }
        params_arg = json.dumps(pass_params)
        pre_command = "import json\n"
        inp_dict = self.input_dict or "_params"
        pre_command = f"""import sys, json
{inp_dict} = json.loads(sys.argv[1])
"""
        if self.input_vars:
            for name in pass_params:
                var_name = name.replace("-", "_").replace(".", "__")
                pre_command += f"""{var_name} = {inp_dict}["{name}"]\n"""

        post_command = ""
        pass_outputs = [name for name, schema in cab.outputs.items()
                        if not schema.is_named_output and not schema.implicit]
        if pass_outputs:
            post_command += "from scabha.cab_utils import yield_output\n"
            if self.output_vars:
                for name in pass_outputs:
                    var_name = name.replace("-", "_").replace(".", "__")
                    post_command += f"yield_output(**{{'{name}': {var_name}}})\n"                

        args = get_python_interpreter_args(cab, subst)
        args += ["-c", pre_command + command + post_command, params_arg]
        return args
