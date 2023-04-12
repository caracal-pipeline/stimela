import shlex
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

    def get_arguments(self, cab: Cab, params: Dict[str, Any], subst: Dict[str, Any]):

        # build command line from parameters
        args, venv = cab.build_command_line(params, subst, search=False)

        # prepend virtual env invocation, if asked
        if venv:
            args = ["/bin/bash", "--rcfile", f"{venv}/bin/activate", "-c", " ".join(shlex.quote(arg) for arg in args)]

        return args
    
    def get_image_name(self, cab: Cab, backend: 'stimela.backend.StimelaBackendOptions'):
        return cab.image.to_string(backend.default_registry)        

