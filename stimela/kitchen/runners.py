from typing import Dict, Optional, Any
from stimela import config

from .step import Step
from .batch import Batch
from .cab import Cab

def run_cab(step: Step, params: Dict[str, Any], 
            backend: 'config.Backend', 
            subst: Optional[Dict[str, Any]] = None, 
            batch: Batch=None) -> Cab.RuntimeStatus:
    log = step.log
    cab = step.cargo

    ## NOTE(Sphe)
    # Not sure is this the best implementation
    # But the runtime environment has to default to config.run unless explicitly given in the step
    # But more importantly it has be updated by the Cab environment before running the cab
    
    step.runtime = step.runtime or subst.config.run.copy()
    cab.update_environment(subst=subst)
    step.runtime.env.update(**cab.management.environment)

    backend = __import__(f"stimela.backends.{backend.name}", 
                         fromlist=[backend.name])
    
    return backend.run(cab, runtime=step.runtime, params=params, log=log, subst=subst, batch=batch, fqname=step.fqname)
