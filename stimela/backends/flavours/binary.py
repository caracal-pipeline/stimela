import shlex
import logging
from typing import Optional, Any, Union, Dict
from dataclasses import dataclass

from stimela.exceptions import CabValidationError
from stimela.kitchen.cab import Cab
from scabha.cab_utils import CAB_OUTPUT_PREFIX
from stimela.kitchen import wranglers
from scabha.substitutions import substitutions_from

from . import _CallableFlavour, _BaseFlavour
import stimela

@dataclass
class BinaryFlavour(_BaseFlavour):
    """
    Represents a cab flavour that is a command run via the shell
    """
    kind: str = "binary"

    def get_arguments(self, cab: Cab, params: Dict[str, Any], subst: Dict[str, Any], 
                      virtual_env: Optional[str]=None, check_executable: bool = True, log: Optional[logging.Logger] = None):

        # build command line from parameters
        args = cab.build_command_line(params, subst, virtual_env=virtual_env, check_executable=check_executable)

        # prepend virtual env invocation, if asked
        if virtual_env:
            venv_args = f". {virtual_env}/bin/activate && "
            args = ["/bin/bash", "-c", venv_args + " ".join(shlex.quote(arg) for arg in args)]

        return args, args
    
    def get_image_name(self, cab: Cab, backend: 'stimela.backend.StimelaBackendOptions'):
        from stimela.backends import resolve_image_name
        from stimela import CONFIG
        return resolve_image_name(backend, cab.image or CONFIG.images['default-python'])

