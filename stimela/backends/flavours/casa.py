import re
import os.path
import json
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
from .python_flavours import form_python_function_call


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
class CasaTaskFlavour(_CallableFlavour):
    kind: str = "casa-task"
    casa: Optional[str] = None
    casa_opts: Optional[str] = None

    def finalize(self, cab: Cab):
        super().finalize(cab)

        err_patt = re.compile("(?P<content>(\tSEVERE\t|ABORTING|\*\*\* Error \*\*\*)(.*))$")
        cab._wranglers.append((
            err_patt, [
                wranglers.DeclareError(err_patt, "ERROR", message="CASA error: {content}" )
            ]
        ))

    def get_arguments(self, cab: Cab, params: Dict[str, Any], subst: Dict[str, Any]):
        with substitutions_from(subst, raise_errors=True) as context:
            try:
                command = context.evaluate(cab.command, location=["command"])
            except Exception as exc:
                raise SubstitutionError(f"error substituting casa task '{cab.command}'", exc)
            if self.casa:
                try:
                    casa = context.evaluate(self.casa, location=["casa"])
                except Exception as exc:
                    raise SubstitutionError(f"error substituting casa path '{casa}'", exc)
            else:
                casa = stimela.CONFIG.opts.runtime.get('casa', "casa")
            if self.casa_opts:
                try:
                    casa_opts = context.evaluate(self.casa_opts, location=["casa_opts"])
                except Exception as exc:
                    raise SubstitutionError(f"error substituting casa options '{casa_opts}'", exc)
            else:
                casa_opts = stimela.CONFIG.opts.runtime.get('casa_opts', "--log2term --nologger --nologfile")

        self.command_name = command
        # convert inputs into a JSON string
        pass_params = cab.filter_input_params(params)
        params_string = json.dumps(pass_params)

        # unicode instance only exists in python2, python3 bytes
        code = f"""
import sys, json
kw = json.loads(sys.argv[-1])

try:
    utype = unicode
except NameError:
    utype = bytes

def stringify(x):
    if isinstance(x, (utype, str)):
        return str(x)
    elif isinstance(x, list):
        return [stringify(y) for y in x]
    else:
        return x

kw = {{key: stringify(value) for key, value in kw.items()}}

{command}(**kw)

"""

        args =  casa.strip().split() + casa_opts.strip().split() + ["-c", code, params_string]
        return args


