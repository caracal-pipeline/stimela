import re
from typing import Optional, Any, Union, Dict
from dataclasses import dataclass
from dataclasses import dataclass
from omegaconf import MISSING, OmegaConf, DictConfig
from omegaconf.errors import OmegaConfBaseException
from stimela.exceptions import CabValidationError
from scabha.exceptions import ScabhaBaseException
import stimela


class _BaseFlavour(object):
    def finalize(self, cab: "stimela.kitchen.cab.Cab"):
        self.command_name = cab.command.split()[0]

    def get_arguments(self, cab: "stimela.kitchen.cab.Cab", params: Dict[str, Any], subst: Dict[str, Any]):
        pass


@dataclass
class _CallableFlavour(_BaseFlavour):
    # if True, function returns dict of outputs
    output_dict: bool = False
    # if not None, function returns the named output
    output: Optional[str] = None

    @property
    def dict_outputs(self):
        return self.outputs == "{}"

    def finalize(self, cab: "stimela.kitchen.cab.Cab"):
        if self.output_dict and self.output:
            raise CabValidationError(f"cab {cab.name}: can't specify both 'output_dict' and 'output'")
        if self.output and self.output not in cab.outputs: 
            raise CabValidationError(f"cab {cab.name}: flavour.outputs='{self.outputs}' is not a known output")

_flavour_map = None
_flavour_schemas = None

def lookup_flavour(kind):
    global _flavour_map, _flavour_schemas
    if _flavour_map is None:
        from .binary import BinaryFlavour
        from .python_flavours import PythonCallableFlavour, PythonCodeFlavour

        _flavour_map = {
            'binary': BinaryFlavour,
            'python': PythonCallableFlavour,
            'python-code': PythonCodeFlavour
        }

        _flavour_schemas = {name: OmegaConf.structured(cls) for name, cls in _flavour_map.items()}

    if kind in _flavour_map:
        return _flavour_map[kind], _flavour_schemas[kind]
    else:
        return None, None

def init_cab_flavour(cab: "stimela.kitchen.cab.Cab"):
    flavour = cab.flavour
    if flavour is None:
        from .binary import BinaryFlavour
        flavour = BinaryFlavour()
    elif isinstance(flavour, str):
        cls, _ = lookup_flavour(flavour)
        if cls is None:
            raise CabValidationError(f"unknown flavour '{flavour}'")
        flavour = cls()
    elif isinstance(flavour, DictConfig):
        if 'kind' not in flavour:
            raise CabValidationError(f"flavour.kind not specified")
        cls, schema = lookup_flavour(flavour.kind)
        if cab is None:
            raise CabValidationError(f"unknown flavour.kind '{flavour.kind}'")
        try:
            defs = OmegaConf.merge(schema, flavour)
            flavour = cls(**defs)
        except (OmegaConfBaseException, ScabhaBaseException) as exc:
            raise CabValidationError(f"error in flavour definition", exc)
    else:
        raise CabValidationError(f"flavour must be a string or a mapping")
    flavour.finalize(cab)
    return flavour
