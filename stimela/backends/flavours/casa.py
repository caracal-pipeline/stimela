import re
import logging
from typing import Optional, Any, Union, Dict, List
from dataclasses import dataclass

import stimela
from scabha.basetypes import EmptyListDefault
from scabha.exceptions import SubstitutionError
from stimela.kitchen.cab import Cab
from stimela.kitchen import wranglers
from scabha.substitutions import substitutions_from

from . import _CallableFlavour
from .python_flavours import format_dict_as_function_call


@dataclass
class CasaTaskFlavour(_CallableFlavour):
    kind: str = "casa-task"
    path: Optional[str] = None                       # path to CASA executable
    opts: Optional[List[str]] = EmptyListDefault()   # additional options 
    wrapper: Optional[str] = None                    # wrapper command (e.g. xvfb-run -a)

    def finalize(self, cab: Cab):
        super().finalize(cab)

        # catch CASA error messages, except the MeasTable::dUTC complaints which are all-pervasive
        rex = "(?P<content>(\tSEVERE\\s+(?!([a-z]+::)?MeasTable::dUTC)|ABORTING|\\*\\*\\* Error \\*\\*\\*)(.*))$"
        err_patt = re.compile(rex)
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
                            virtual_env: Optional[str]=None, 
                            check_executable: bool = True,
                            log: Optional[logging.Logger] = None):

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
            wrapper = self.wrapper if self.wrapper is not None else casa_config.get('wrapper', 'xvfb-run -a')
            if wrapper:
                try:
                    wrapper = context.evaluate(wrapper, location=["wrapper"]).split()
                except Exception as exc:
                    raise SubstitutionError(f"error substituting wrapper '{wrapper}'", exc)
            else:
                wrapper = []

        self.command_name = command
        pass_params = dict(cab.filter_input_params(params))

        # log invocation
        if log:
            log.info(f"preparing CASA task call:", extra=dict(prefix="###", style="dim"))
            for line in format_dict_as_function_call(command, pass_params, indent=4):
                log.info(f"    {line}", extra=dict(prefix="###", style="dim"))

        # parse the params direcly as python dictionary
        # no need to string conversion between strings/bytes/unicode
        # this works for both python 2.7 and 3.x
        code = f"{command}(**{pass_params})"

        args =  wrapper + casa.strip().split() + list(casa_opts)
        
        return args + ["-c", code], args + ["-c", "..."]
    


