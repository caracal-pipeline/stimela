import re
import os.path
import json
from typing import Optional, Any, Union, Dict, List
from dataclasses import dataclass
import tempfile

import stimela
from scabha.basetypes import EmptyListDefault
from scabha.exceptions import SubstitutionError
from stimela.kitchen.cab import Cab
from scabha.cab_utils import CAB_OUTPUT_PREFIX
from stimela.kitchen import wranglers
from scabha.substitutions import substitutions_from

from . import _CallableFlavour, _BaseFlavour
from .python_flavours import form_python_function_call



@dataclass
class CasaTaskFlavour(_CallableFlavour):
    kind: str = "casa-task"
    path: Optional[str] = None                       # path to CASA executable
    opts: Optional[List[str]] = EmptyListDefault()   # additional options 

    def finalize(self, cab: Cab):
        super().finalize(cab)

        err_patt = re.compile("(?P<content>(\tSEVERE\t|ABORTING|\*\*\* Error \*\*\*)(.*))$")
        cab._wranglers.append((
            err_patt, [
                wranglers.DeclareError(err_patt, "ERROR", message="CASA error: {content}" )
            ]
        ))

    def get_image_name(self, cab: Cab, backend: 'stimela.backend.StimelaBackendOptions'):
        from stimela import CONFIG
        from stimela.backends import resolve_image_name
        return resolve_image_name(backend, cab.image or CONFIG.images['default-casa'])

    def get_arguments(self, cab: Cab, params: Dict[str, Any], subst: Dict[str, Any], 
                            virtual_env: Optional[str]=None, check_executable: bool = True):

        with substitutions_from(subst, raise_errors=True) as context:
            try:
                command = context.evaluate(cab.command, location=["command"])
            except Exception as exc:
                raise SubstitutionError(f"error substituting casa task '{cab.command}'", exc)
            casa_config = stimela.CONFIG.opts.runtime.get('casa', {})
            casa = self.path or casa_config.get('path', 'casa')
            try:
                casa = context.evaluate(casa, location=["path"])
            except Exception as exc:
                raise SubstitutionError(f"error substituting casa path '{casa}'", exc)
            casa_opts = self.opts or casa_config.get('opts', ["--log2term", "--nologger", "--nologfile"]) or []
            if casa_opts:
                try:
                    casa_opts = [context.evaluate(opt, location=["opts"]) for opt in casa_opts]
                except Exception as exc:
                    raise SubstitutionError(f"error substituting casa options '{casa_opts}'", exc)

        # check for virtual_env
        if virtual_env and "/" not in command:
            command = f"{virtual_env}/bin/{command}"

        self.command_name = command
        # convert inputs into a JSON string
        pass_params = cab.filter_input_params(params)
        params_string = json.dumps(pass_params)

        # unicode instance only exists in python2, python3 bytes
        code = f"""
import sys, json
kwin = json.loads('{params_string}')

try:
    utype = unicode
except NameError:
    utype = bytes

kw = dict()

for key, val in kwin.items():
    # stringify in a loop to avoid isue #300
    if isinstance(val, (utype, str)):
        x = str(val)
    elif isinstance(val, list):
        x = [stringify(y) for y in val]
    else:
        x = val
        
    kw[key] = x

{command}(**kw)

"""

        args =  casa.strip().split() + list(casa_opts) + ["-c", code]
        return args

