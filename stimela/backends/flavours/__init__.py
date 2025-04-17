import re
from typing import Optional, Any, Union, Dict
import logging
from dataclasses import dataclass
from dataclasses import dataclass
from omegaconf import MISSING, OmegaConf, DictConfig
from omegaconf.errors import OmegaConfBaseException
from stimela.exceptions import CabValidationError
from scabha.exceptions import ScabhaBaseException
import stimela


class _BaseFlavour(object):
    """
    A flavour class represents a particular kind of runnable task
    (binary, python callable, inline python code, etc.)
    """
    def finalize(self, cab: "stimela.kitchen.cab.Cab"):
        """Finalizes flavour definition, given a cab"""
        self.command_name = cab.command.split()[0]

    def get_arguments(self, cab: "stimela.kitchen.cab.Cab", 
                            params: Dict[str, Any], 
                            subst: Dict[str, Any],
                            virtual_env: Optional[str] = None,
                            check_executable: bool = True,
                            log: Optional[logging.Logger] = None):
        """Returns command line arguments for running this flavour of task, given
        a cab and a set of parameters. 

        Args:
            cab (Cab):               cab definition
            params (Dict[str, Any]): parameter dict
            subst (Dict[str, Any]):  substitution namespace 
            virtual_env (Optional[str]): virtual environment to run in, or None
            check_executable (bool):  if True, cab may check for the executable to exist (but doesn't have to)
            log (Optional[Logger]):  optional logger


        Returns:
            Tuple[List, List]:       tuple of full_argument_list, abbreviated_argument_list
                                     The full list is meant for execution, the abbreviated list is meant for
                                     display and logging
        """
        pass


@dataclass
class _CallableFlavour(_BaseFlavour):
    """
    Represents a callable function (in python)
    """
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
    """
    Given a "kind" string, looks up appropriate flavour class, and corresponding OmegeCaonf schema
    Returns:
        (class, OmegaConf.DictConfig): class and schema 
    """
    global _flavour_map, _flavour_schemas
    # init map the first time we're called
    if _flavour_map is None:
        from .binary import BinaryFlavour
        from .python_flavours import PythonCallableFlavour, PythonCodeFlavour
        from .casa import CasaTaskFlavour

        _flavour_map = {
            'binary': BinaryFlavour,
            'python': PythonCallableFlavour,
            'python-code': PythonCodeFlavour,
            'casa-task': CasaTaskFlavour
        }

        _flavour_schemas = {name: OmegaConf.structured(cls) for name, cls in _flavour_map.items()}

    if kind in _flavour_map:
        return _flavour_map[kind], _flavour_schemas[kind]
    else:
        return None, None

def init_cab_flavour(cab: "stimela.kitchen.cab.Cab"):
    """
    Given a cab definition, creates an object of the appropriate flavour class. 
    Cab.flavour can be specified as a string (default class definition), or as a mapping
    with a "kind" attribute, in which case flavour parameters may be passed.
    """
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
