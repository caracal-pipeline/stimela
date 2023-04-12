from typing import Dict, Optional, Any
from omegaconf import OmegaConf
from omegaconf.errors import OmegaConfBaseException 
from stimela.backends import StimelaBackendSchema
from stimela.exceptions import StimelaCabRuntimeError, BackendError
import stimela.kitchen

from . import get_backend, get_backend_status

def validate_backend_settings(backend_opts: Dict[str, Any]):
    """Checks that backend settings refer to a valid backend
    
    Returs tuple of main, wrapper, where 'main' the the main backend, and 'wrapper' is an optional wrapper backend 
    such as slurm.
    """
    # construct backend object
    try:
        backend_opts = OmegaConf.merge(StimelaBackendSchema, backend_opts)
    except OmegaConfBaseException as exc:
        raise BackendError("invalid backend specification", exc)

    main = main_backend = None
    # select containerization engine, if any
    for engine in 'singularity', 'kube', 'docker', 'native':
        if backend_opts[engine] and backend_opts[engine].enable:
            if main:
                raise BackendError(f"can't use both '{engine}' and '{main}'")
            main_backend = get_backend(engine)
            if main_backend is None:
                raise BackendError(f"backend '{engine}' not available ({get_backend_status(engine)})")
            main = engine
    
    if main is None:
        main = 'native'
        main_backend = get_backend('native')

    # check that selected backend is enabled 
    if main_backend is None:
        raise BackendError(f"backend '{main}' not available ({get_backend_status(main)})")
    
    # check if slurm wrapper is to be applied
    wrapper = None
    if False:   # placeholder -- should be: if backend.slurm and backed.slurm.enable
        wrapper = get_backend('slurm') 
        if wrapper is None:
            raise BackendError(f"backend 'slurm' not available ({get_backend_status('slurm')})")

    return backend_opts, main_backend, wrapper


def run_cab(step: 'stimela.kitchen.step.Step', params: Dict[str, Any], 
            backend: Optional[Dict[str, Any]] = None, 
            subst: Optional[Dict[str, Any]] = None) -> 'stimela.kitchen.cab.Cab.RuntimeStatus':

    log = step.log
    cab = step.cargo
    backend_opts, main, wrapper =  validate_backend_settings(backend) 

    return main.run(cab, params=params, log=log, subst=subst, backend=backend_opts, fqname=step.fqname)
