import shlex
from typing import Optional, Any, Union, Dict
from dataclasses import dataclass

from stimela.exceptions import CabValidationError
from stimela.kitchen.cab import Cab
from scabha.cab_utils import CAB_OUTPUT_PREFIX
from stimela.kitchen import wranglers
from scabha.substitutions import substitutions_from

from . import _CallableFlavour, _BaseFlavour


@dataclass
class BinaryFlavour(_BaseFlavour):
    kind: str = "binary"

    def get_arguments(self, cab: Cab, params: Dict[str, Any], subst: Dict[str, Any]):

        # build command line from parameters
        args, venv = cab.build_command_line(params, subst)

        # prepend virtual env invocation, if asked
        if venv:
            args = ["/bin/bash", "--rcfile", f"{venv}/bin/activate", "-c", " ".join(shlex.quote(arg) for arg in args)]

        return args

