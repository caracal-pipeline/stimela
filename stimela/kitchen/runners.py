import shlex
from typing import Dict, Optional, Any
from scabha.cargo import Cab, Batch, Parameter
from stimela import logger
from stimela.utils.xrun_poll import xrun
from stimela.exceptions import StimelaCabRuntimeError


def run_cab(cab: Cab, params: Dict[str, Any], log=None, subst: Optional[Dict[str, Any]] = None, batch: Batch=None):
    log = log or logger()
    backend = __import__(f"stimela.backends.{cab.backend.name}", 
                         fromlist=[cab.backend.name])
    return backend.run(cab, params=params, log=log, subst=subst, batch=batch)
