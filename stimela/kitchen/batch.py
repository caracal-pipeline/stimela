from typing import Any, List, Dict, Optional, Union
from dataclasses import dataclass

from scabha.basetypes import EmptyDictDefault, EmptyListDefault
import stimela.kitchen

@dataclass
class Batch:
    scheduler: str = "slurm"
    cpus: int = 4
    mem: str = "128gb"
    email: Optional[str] = None

    def __init_cab__(self, cab: 'stimela.kitchen.cab.Cab', params: Dict[str, Any], subst: Optional[Dict[str, Any]], log: Any=None):
        self.cab = cab
        self.log = log
        self.args, self.venv = self.cab.build_command_line(params, subst)

