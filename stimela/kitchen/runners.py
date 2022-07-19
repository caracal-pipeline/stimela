from typing import Dict, Optional, Any
from stimela import logger, config

from .step import Step
from .batch import Batch

def run_cab(step: Step, params: Dict[str, Any], backend: 'config.Backend', subst: Optional[Dict[str, Any]] = None, batch: Batch=None):
    log = step.log
    cab = step.cargo
    runtime = step.runtime

    backend = __import__(f"stimela.backends.{backend.name}", 
                         fromlist=[backend.name])
    return backend.run(cab, runtime=runtime, params=params, log=log, subst=subst, batch=batch, fqname=step.fqname)
