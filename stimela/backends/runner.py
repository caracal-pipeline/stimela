import logging
from typing import Dict, Optional, Any, Callable
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
    backend: Any
    backend_name: str
    run_command_wrapper: Optional[Callable]
    build_command_wrapper: Optional[Callable]

    def run(self, cab: 'stimela.kitchen.cab.Cab', params: Dict[str, Any], fqname: str,
            log: logging.Logger, subst: Optional[Dict[str, Any]] = None):
        return self.backend.run(cab, params, fqname=fqname, backend=self.opts, log=log, subst=subst, 
                              command_wrapper=self.run_command_wrapper)
        
    def build(self, cab: 'stimela.kitchen.cab.Cab', log: logging.Logger, rebuild=False):
       if not hasattr(self.backend, 'build'):
           raise BackendError(f"{self.backend_name} backend does not support the build command")
       return self.backend.build(cab, backend=self.opts, log=log, rebuild=rebuild,
                                command_wrapper=self.build_command_wrapper)


def validate_backend_settings(backend_opts: Dict[str, Any]) -> BackendWrapper:
    """Checks that backend settings refer to a valid backend
    
    Returs tuple of options, main, wrapper, where 'main' the the main backend, and 'wrapper' is an optional wrapper backend 
    such as slurm.
    """
    if not isinstance(backend_opts, StimelaBackendOptions):
        backend_opts = OmegaConf.to_object(backend_opts)

    backend_name = backend = None
    selected = backend_opts.select or ['native']
    # select containerization engine, if any
    for name in selected: 
        # check that backend has not been disabled
        opts = getattr(backend_opts, name, None)
        if not opts or opts.enable:
            backend = get_backend(name, opts)
            if backend is not None:
                backend_name = name
                break
    else:
        raise BackendError(f"selected backends ({', '.join(selected)}) not available")

    is_remote = is_remote_fs = backend.is_remote()

    # check if slurm wrapper is to be applied
    if backend_opts.slurm.enable:
        if is_remote:
            raise BackendError(f"can't combine slurm with {backend_name} backend")
        is_remote = True
        is_remote_fs = False
        run_command_wrapper = backend_opts.slurm.run_command_wrapper
        build_command_wrapper = backend_opts.slurm.build_command_wrapper
    else:
        run_command_wrapper = build_command_wrapper = None

    return BackendWrapper(opts=backend_opts, is_remote=is_remote, is_remote_fs=is_remote_fs, 
                          backend=backend, backend_name=backend_name, 
                          run_command_wrapper=run_command_wrapper,
                          build_command_wrapper=build_command_wrapper)

