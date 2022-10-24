from typing import Dict, Optional, Any
from stimela import logger

from .cab import Cab
from .batch import Batch

def run_cab(cab: Cab, params: Dict[str, Any], log=None, subst: Optional[Dict[str, Any]] = None, batch: Batch=None):
    log = log or logger()
    backend = __import__(f"stimela.backends.{cab.backend.name}", 
                         fromlist=[cab.backend.name])
    return backend.run(cab, params=params, log=log, subst=subst, batch=batch)
