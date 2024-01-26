import logging
from typing import Dict, Optional, Any
from dataclasses import dataclass
from omegaconf import OmegaConf
from omegaconf.errors import OmegaConfBaseException 
import stimela
from stimela.backends import StimelaBackendOptions, StimelaBackendSchema
from stimela.exceptions import BackendError

from . import get_backend, get_backend_status, slurm


@dataclass
class BackendWrapper(object):
    opts: StimelaBackendOptions
    is_remote: bool
    is_remote_fs: bool
    main_backend: None
    command_wrapper: None

    def run(self, cab: 'stimela.kitchen.cab.Cab', params: Dict[str, Any], fqname: str,
            log: logging.Logger, subst: Optional[Dict[str, Any]] = None):
        self.main_backend.run(cab, params, fqname=fqname, backend=self.opts, log=log, subst=subst, 
                              command_wrapper=self.command_wrapper)
       


def validate_backend_settings(backend_opts: Dict[str, Any]) -> BackendWrapper:
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
            main_backend = get_backend(engine, opts)
            if main_backend is not None:
                main = engine
                break
    else:
        raise BackendError(f"selected backends ({', '.join(selected)}) not available")

    is_remote = is_remote_fs = main_backend.is_remote()

    # check if slurm wrapper is to be applied
    if backend_opts.slurm.enabled:
        if is_remote:
            raise BackendError(f"can't combine slurm with {main} backend")
        is_remote = True
        is_remote_fs = False
        command_wrapper = backend_opts.slurm.command_wrapper
    else:
        command_wrapper = None

    return BackendWrapper(opts=backend_opts, is_remote=is_remote, is_remote_fs=is_remote_fs, 
                          main_backend=main_backend, command_wrapper=command_wrapper)

