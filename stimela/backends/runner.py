from typing import Dict, Optional, Any
from omegaconf import OmegaConf
from omegaconf.errors import OmegaConfBaseException 
from stimela.backends import StimelaBackendOptions, StimelaBackendSchema
from stimela.exceptions import BackendError

from . import get_backend, get_backend_status

def validate_backend_settings(backend_opts: Dict[str, Any]):
    """Checks that backend settings refer to a valid backend
    
    Returs tuple of options, main, wrapper, where 'main' the the main backend, and 'wrapper' is an optional wrapper backend 
    such as slurm.
    """
    if not isinstance(backend_opts, StimelaBackendOptions):
        backend_opts = OmegaConf.to_object(backend_opts)

    main = main_backend = None
    selected = backend_opts.select or ['native']
    # select containerization engine, if any
    for engine in selected: 
        # check that backend has not been disabled
        opts = getattr(backend_opts, engine, None)
        if not opts or opts.enable:
            main_backend = get_backend(engine)
            if main_backend is not None:
                main = engine
                break
    else:
        raise BackendError(f"selected backends ({', '.join(selected)}) not available")
    
    # check if slurm wrapper is to be applied
    wrapper = None
    if False:   # placeholder -- should be: if backend.slurm and backed.slurm.enable
        wrapper = get_backend('slurm') 
        if wrapper is None:
            raise BackendError(f"backend 'slurm' not available ({get_backend_status('slurm')})")

    return backend_opts, main_backend, wrapper

