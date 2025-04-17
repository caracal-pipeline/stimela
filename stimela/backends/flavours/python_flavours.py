import re, os.path, json, zlib, codecs, base64, logging
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


def form_python_function_call(function: str, cab: Cab, params: Dict[str, Any]):
    """
    Helper. Converts a function name and a list of parameters into a string
    representation of a Python function call that can be parsed by the interpreter.
    Uses the cab schema info and policies to decide which parametets to include.

    Args:
        function (str) function name
        cab (Cab): cab definition
        params (Dict[str, Any]): dict of parameters, e.g. {a: 1, b: 'foo'}

    Returns:
        str: function invocation, e.g. "func(a=1, b='foo')"
    """
    arguments = []
    for key, value in cab.filter_input_params(params).items():
        arguments.append(f"{key}={repr(value)}")
    return f"{function}({', '.join(arguments)})"


def format_dict_as_function_call(func_name: Optional[str], d: Dict[str, Any], indent=4):
    """formats dict as a function call"""
    if func_name:
        lines = [f"{func_name}("] 
        comma = ","
    else:
        lines = []
        comma = ''
    pad = ' ' * indent
    for k, v in d.items():
        lines.append(f"{pad}{k}={repr(v)}{comma}")
    # strip trailing comma
    if func_name:
        lines[-1] = lines[-1][:-1] + ")"
    return lines


def get_python_interpreter_args(cab: Cab, subst: Dict[str, Any], virtual_env: Optional[str]=None):
    """
    Helper. Given a cab definition, forms up appropriate argument list to
    invoke the interpreter. Invokes a virtual environment as appropriate.

    Args:
        cab (Cab):              cab definition
        subst (Dict[str, Any]): substitution namespace

    Raises:
        CabValidationError: on errors

    Returns:
        List[str]: [command, arguments, ...] needed to invoke the interpreter
    """
    # get virtual env, if specified
    if virtual_env:
        virtual_env = os.path.expanduser(virtual_env)
        interpreter = f"{virtual_env}/bin/{cab.flavour.interpreter_binary}"
        if not os.path.isfile(interpreter):
            raise CabValidationError(f"{interpreter} doesn't exist")
    else:
        interpreter = cab.flavour.interpreter_binary

    args =  cab.flavour.interpreter_command.format(python=interpreter).split()

    return args


@dataclass
class PythonCallableFlavour(_CallableFlavour):
    """
    Represents a cab flavour that is a Python callable. Cab command field is
    expected to be in the form of [package.]module.function
    """
    kind: str = "python"
    # name of python binary to use  
    interpreter_binary: str = "python"
    # Full command used to launch interpreter. {python} gets substituted for the interpreter path
    interpreter_command: str = "{python} -u"
    # commands run prior to invoking function
    pre_command: Optional[str] = None
    # commands run post invoking function
    post_command: Optional[str] = None

    def finalize(self, cab: Cab):
        super().finalize(cab)
        # form up outputs handler
        if self.output is not None or self.output_dict:
            self._yield_output = f"print(f'{CAB_OUTPUT_PREFIX}{{json.dumps(_result)}}')"
            pattern = re.compile(f"{CAB_OUTPUT_PREFIX}(.*)")
            if self.output_dict:
                wrangler = wranglers.ParseJSONOutputDict(pattern, "PARSE_JSON")
            else:
                wrangler = wranglers.ParseOutput(pattern, "PARSE_OUTPUT", self.output, '1', "json")
            wrangs = [wrangler]
            if not stimela.VERBOSE:
                wrangs.append(wranglers.Suppress(pattern, "SUPPRESS"))
            cab._wranglers.append((pattern, wrangs))
        else:
            self._yield_output = ""

    def get_image_name(self, cab: Cab, backend: 'stimela.backend.StimelaBackendOptions'):
        from stimela import CONFIG
        from stimela.backends import resolve_image_name
        return resolve_image_name(backend, cab.image or CONFIG.images['default-python'])

    def get_arguments(self, cab: Cab, params: Dict[str, Any], subst: Dict[str, Any],
                      virtual_env: Optional[str]=None, check_executable: bool = True,
                      log: Optional[logging.Logger] = None):
        # substitute command and split into module/function
        with substitutions_from(subst, raise_errors=True) as context:
            try:
                command = context.evaluate(cab.command, location=["command"])
            except Exception as exc:
                raise SubstitutionError(f"error substituting Python callable '{cab.command}'", exc)

        if '.' in command:
            py_module, py_function = cab.command.rsplit('.', 1)
        else:
            raise CabValidationError(f"cab {cab.name}: python flavour requires a command of the form module.function")
        self.command_name = py_function

        # convert inputs into a JSON string
        pass_params = cab.filter_input_params(params)
        pass_params = {key.replace("-","_").replace(".","__"): value for key, value in pass_params.items()}
        params_string = base64.b64encode(
                            zlib.compress(json.dumps(pass_params).encode('ascii'), 2)
                        ).decode('ascii')
        
        # log invocation
        if log:
            log.info(f"preparing function call:", extra=dict(prefix="###", style="dim"))
            for line in format_dict_as_function_call(cab.command, pass_params, indent=4):
                log.info(f"    {line}", extra=dict(prefix="###", style="dim"))

        # form up command string
        if stimela.VERBOSE:
            msg1 = f"""print("## importing {py_module}.{py_function}")"""
            msg2 = f"""print(f"## invoking callable {command}({{repr(_inputs)}}) (as click command) using external interpreter")"""
            msg3 = f"""print(f"## invoking callable {command}({{repr(_inputs)}}) using external interpreter")"""
            msg4 = f"""print("## return value is ", _result)"""
        else:
            msg1 = msg2 = msg3 = msg4 = ""
        code = f"""
import sys, json, zlib, base64
_inputs = json.loads(zlib.decompress(
                        base64.b64decode(sys.argv[1].encode("ascii"))
                    ).decode("ascii"))
sys.path.append('.')
{self.pre_command or 'pass'}
{msg1}
from {py_module} import {py_function}
try:
    from click import Command
except ImportError:
    Command = None
if Command is not None and isinstance({py_function}, Command):
    {msg2}
    {py_function} = {py_function}.callback
else:
    {msg3}
    pass
_result = {py_function}(**_inputs)
{msg4}
{self._yield_output}
{self.post_command or 'pass'}
        """

        args = get_python_interpreter_args(cab, subst, virtual_env=virtual_env)
        return args + ["-c", code, params_string], args + ["-c", "..."]


@dataclass
class PythonCodeFlavour(_BaseFlavour):
    """
    Represents a cab flavour that is inlined Python code. Cab command field is
    Python code. Parameters can be passed in as local variables, or a dict
    """
    kind: str = "python-code"
    # if set to a string, inputs will be passed in as a dict assigned to a variable of that name
    input_dict: Optional[str] = None
    # if True, inputs will be passed in as named variables
    input_vars: bool = True
    # if True, outputs will be collected from named variables
    output_vars: bool = True
    # if True, command will have {}-substitutions done on it
    subst: bool = False
    # name of python binary to use  
    interpreter_binary: str = "python"
    # Full command used to launch interpreter. {python} gets substituted for the interpreter path
    interpreter_command: str = "{python} -u"
    # commands run prior to invoking code
    pre_command: Optional[str] = None
    # commands run post invoking code
    post_command: Optional[str] = None

    def finalize(self, cab: Cab):
        super().finalize(cab)
        pattern = re.compile(f"{CAB_OUTPUT_PREFIX}(.*)")
        wrangs = [wranglers.ParseJSONOutputDict(pattern, "PARSE_JSON")]
        if not stimela.VERBOSE:
            wrangs.append(wranglers.Suppress(pattern, "SUPPRESS"))
        cab._wranglers.append((pattern, wrangs))
        self.command_name = "[python]"

    def get_image_name(self, cab: Cab, backend: 'stimela.backend.StimelaBackendOptions'):
        from stimela import CONFIG
        from stimela.backends import resolve_image_name
        return resolve_image_name(backend, cab.image or CONFIG.images['default-python'])

    def get_arguments(self, cab: Cab, params: Dict[str, Any], subst: Dict[str, Any],
                      virtual_env: Optional[str]=None, check_executable: bool = True,
                      log: Optional[logging.Logger] = None):
        # do substitutions on command, if necessary
        if self.subst:
            with substitutions_from(subst, raise_errors=True) as context:
                try:
                    command = context.evaluate(cab.command, location=["command"])
                except Exception as exc:
                    raise SubstitutionError("error substituting inline Python code", exc)
        else:
            command = cab.command

        # only pass inputs and named outputs
        pass_params = cab.filter_input_params(params)

        # log invocation
        if log:
            log.info(f"preparing python code invocation with arguments:", extra=dict(prefix="###", style="dim"))
            for line in format_dict_as_function_call("", pass_params, indent=4):
                log.info(f"{line}", extra=dict(prefix="###", style="dim"))

        # form up code to parse params from JSON string that will be given as sys.argv[1]
        params_arg = json.dumps(pass_params)
        inp_dict = self.input_dict or "_params"

        pre_command = f"""import sys, json
{inp_dict} = json.loads(sys.argv[1])
"""
        if self.pre_command:
            pre_command += self.pre_command

        if self.input_vars:
            for name in pass_params:
                var_name = name.replace("-", "_").replace(".", "__")
                pre_command += f"""{var_name} = {inp_dict}["{name}"]\n"""

        # form up code to print outputs in JSON
        post_command = ""
        pass_outputs = [name for name, schema in cab.outputs.items()
                        if not schema.is_named_output and not schema.implicit]
        if pass_outputs:
            post_command += "from scabha.cab_utils import yield_output\n"
            if self.output_vars:
                for name in pass_outputs:
                    var_name = name.replace("-", "_").replace(".", "__")
                    post_command += f"yield_output(**{{'{name}': {var_name}}})\n"
        
        if self.post_command:
            post_command += self.post_command

        # form up interpreter invocation
        args = get_python_interpreter_args(cab, subst, virtual_env=virtual_env)
        return args + ["-c", pre_command + command + post_command, params_arg], args + ["-c", "..."]
