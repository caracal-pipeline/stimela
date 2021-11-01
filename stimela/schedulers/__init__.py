from typing import Dict, Optional, Any 
from scabha.cargo import Cab

@dataclass
class Batch(object):
    self.name = name
    self.cpus = cpus
    self.mem = mem
    self.email = email

    def __init_cab(cab: Cab, subst: Optional[Dict[str, Any]], log)
        self.cab = cab
        self.log = log
        self.args, self.venv = self.cab.build_command_line(subst)
