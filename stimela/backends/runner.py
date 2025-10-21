import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

from omegaconf import OmegaConf

import stimela
from stimela.backends import (
    StimelaBackendOptions,
    get_backend,
    get_backend_status,
)
from stimela.exceptions import BackendError
from stimela.kitchen.cab import Cab


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
    log: logging.Logger,
    cab: Optional[Cab] = None,
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
    # select backend engine

    excuses = {}
    for name in selected:
        # check that backend has not been disabled
        opts = getattr(backend_opts, name, None)
        if opts and not opts.enable:
            excuses[name] = "disabled"
            continue
        backend = get_backend(name, opts)
        if backend is None:
            excuses[name] = get_backend_status(name)  # will tell what's wrong
            continue
        if getattr(backend, "requires_container_image", False) and isinstance(cab, Cab) and not cab.image:
            excuses[name] = "container image not specified by cab"
            continue
        # this one wins
        backend_name = name
        break
    else:
        reason = "; ".join([f"{name}: {excuse}" for name, excuse in excuses.items()])
        raise BackendError(f"unable to select a backend ({reason})")

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
