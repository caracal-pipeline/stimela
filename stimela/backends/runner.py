import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

from omegaconf import OmegaConf

import stimela
from stimela.backends import StimelaBackendOptions
from stimela.exceptions import BackendError
from stimela.kitchen.cab import Cab

from . import get_backend


@dataclass
class BackendRunner(object):
    opts: StimelaBackendOptions
    is_remote: bool
    is_remote_fs: bool
    backend: Any
    backend_name: str
    wrapper: Any

    def run(
        self,
        cab: "stimela.kitchen.cab.Cab",
        params: Dict[str, Any],
        fqname: str,
        log: logging.Logger,
        subst: Optional[Dict[str, Any]] = None,
    ):
        return self.backend.run(
            cab, params, fqname=fqname, backend=self.opts, log=log, subst=subst, wrapper=self.wrapper
        )

    def build(self, cab: "stimela.kitchen.cab.Cab", log: logging.Logger, rebuild=False):
        if not hasattr(self.backend, "build"):
            log.warning(f"the {self.backend_name} backend does support or require image builds")
        else:
            return self.backend.build(cab, backend=self.opts, log=log, rebuild=rebuild, wrapper=self.wrapper)


def validate_backend_settings(
    backend_opts: Dict[str, Any],
    cab: Cab,
    log: logging.Logger,
) -> BackendRunner:
    """Checks that backend settings refer to a valid backend

    Args:
        backend_opts (Dict): Options to set for the backend runner
        cab (object): Cab instance associated with backend
        log (object): Logger object

    Returns BackendRunner object: tuple of options, main, wrapper, where 'main' the the main backend,
    and 'wrapper' is an optional wrapper backend such as slurm.
    """
    if not isinstance(backend_opts, StimelaBackendOptions):
        backend_opts = OmegaConf.to_object(backend_opts)

    selected = backend_opts.select or ["singularity", "native"]

    # select containerization engine, if any
    for name in selected:
        backend_name = backend = None
        opts = getattr(backend_opts, name, None)
        # check that backend has not been disabled
        if not opts or opts.enable:
            backend = get_backend(name, opts)
            if backend is not None:
                backend_name = name
                if getattr(backend, "requires_container_image", False):
                    if isinstance(cab, Cab) and not cab.image:
                        continue
                break
    else:
        selected_string = ", ".join(selected)
        # backend is available but cab.image is not set when cab is a Cab.
        if backend_name:
            raise BackendError(
                f"Selected backdend(s) '{selected_string}' require a container image but"
                f" The cab '{cab.name}' does not specify one."
            )
        else:
            raise BackendError(f"selected backends ({', '.join(selected)}) not available.")

    is_remote = is_remote_fs = backend.is_remote()

    # check if slurm wrapper is to be applied
    if backend_opts.slurm.enable:
        if is_remote:
            raise BackendError(f"can't combine slurm with {backend_name} backend")
        is_remote = True
        is_remote_fs = False
        backend_opts.slurm.validate(log)
        wrapper = backend_opts.slurm
    # otherwise use empty wrapper
    else:
        wrapper = None

    return BackendRunner(
        opts=backend_opts,
        is_remote=is_remote,
        is_remote_fs=is_remote_fs,
        backend=backend,
        backend_name=backend_name,
        wrapper=wrapper,
    )
