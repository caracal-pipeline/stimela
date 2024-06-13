import logging
from dataclasses import dataclass
from typing import Dict, Any, Optional
from enum import Enum
from omegaconf import ListConfig, OmegaConf
from stimela.exceptions import BackendSpecificationError, BackendError
from stimela.stimelogging import log_exception
from scabha.basetypes import EmptyDictDefault, EmptyClassDefault

from .singularity import SingularityBackendOptions
from .kube import KubeBackendOptions
from .native import NativeBackendOptions
from .slurm import SlurmOptions

import stimela

## left as memo to self
# Backend = Enum("Backend", "docker singularity podman kubernetes native", module=__name__)
Backend = Enum("Backend", "singularity kube native", module=__name__)

SUPPORTED_BACKENDS = set(Backend.__members__)


def get_backend(name: str, backend_opts: Optional[Dict] = None):
    """
    Gets backend, given a name and an optional set of options for that backend.
    Returns backend module, or None if it is not available.
    """
    if name not in SUPPORTED_BACKENDS:
        return None
    backend = __import__(f"stimela.backends.{name}", fromlist=[name])
    if backend.is_available(backend_opts):
        return backend
    return None


def get_backend_status(name: str):
    if name not in SUPPORTED_BACKENDS:
        return "unknown backend"
    backend = __import__(f"stimela.backends.{name}", fromlist=[name])
    return backend.get_status()

@dataclass 
class StimelaBackendOptions(object):
    default_registry: str = "quay.io/stimela2"

    # overrides registries -- useful if you have a pull-through cache set up
    override_registries: Dict[str, str] = EmptyDictDefault()
    
    select: Any = "singularity,native"   # should be Union[str, List[str]], but OmegaConf doesn't support it, so handle in __post_init__ for now
    
    singularity: Optional[SingularityBackendOptions] = EmptyClassDefault(SingularityBackendOptions)
    kube: Optional[KubeBackendOptions] = EmptyClassDefault(KubeBackendOptions)
    native: Optional[NativeBackendOptions] = EmptyClassDefault(NativeBackendOptions)
    docker: Optional[Dict] = None  # placeholder for future impl
    slurm: Optional[SlurmOptions] = EmptyClassDefault(SlurmOptions)

    ## Resource limits applied during run -- see resource module
    rlimits: Dict[str, Any] = EmptyDictDefault()

    verbose: int = 0  # be verbose about backend selections. Higher levels mean more verbosity
    
    def __post_init__(self):
        # resolve "select" field
        if type(self.select) is str:
            if not self.select:
                self.select = []
            else:
                self.select = [x.strip() for x in self.select.split(",")]
        elif isinstance(self.select, (list, tuple, ListConfig)):
            self.select = list(self.select)
        else:
            raise BackendSpecificationError(f"invalid backend.select setting of type {self.select}")
        # provide default options for available backends
        if self.singularity is None and get_backend("singularity"):
            self.singularity = SingularityBackendOptions()
        if self.native is None and get_backend("native"):
            self.native = NativeBackendOptions()
        if self.kube is None and get_backend("kube"):
            self.kube = KubeBackendOptions()
        if self.slurm is None:
            self.slurm = SlurmOptions()

StimelaBackendSchema = OmegaConf.structured(StimelaBackendOptions)


def resolve_image_name(backend: StimelaBackendOptions, image: 'stimela.kitchen.Cab.ImageInfo'):
    """
    Resolves image name -- applies override registries, if any exist
    """
    # if image is defined, use name and registry within
    image_name = image.name
    registry_name = image.registry
    version = image.version or "latest"
    # resolve registry name
    if registry_name == "DEFAULT" or not registry_name:
        registry_name = backend.default_registry
    elif registry_name == "LOCAL":
        registry_name = ''
    # apply any registry overrides
    if registry_name in backend.override_registries:
        registry_name = backend.override_registries[registry_name]
    if registry_name:
        return f"{registry_name}/{image_name}:{version}"
    else:
        return f"{image_name}:{version}"


def _call_backends(backend_opts: StimelaBackendOptions, log: logging.Logger, method: str, desc: str, raise_exc: bool=True):
    selected = backend_opts.select or ['native']
    if type(selected) is str:
        selected = [selected]

    for engine in selected: 
        # check that backend has not been disabled
        opts = getattr(backend_opts, engine, None)
        if not opts or opts.enable:
            backend = get_backend(engine, opts)
            func = backend and getattr(backend, method, None)
            if func:
                try:
                    func(backend_opts, log)
                except BackendError as exc:
                    exc1 = BackendError(f"error {desc} {engine} backend", exc)
                    if raise_exc:
                        raise exc1 from None
                    else:
                        log_exception(exc1, log=log)

initialized = None

def init_backends(backend_opts: StimelaBackendOptions, log: logging.Logger):
    global initialized
    if initialized is None:
        initialized = backend_opts
        return _call_backends(backend_opts, log, "init", "initializing")

def close_backends(log: logging.Logger):
    global initialized
    if initialized is not None:
        result = _call_backends(initialized, log, "close", "closing")
        initialized = None
        return result

def cleanup_backends(backend_opts: StimelaBackendOptions, log: logging.Logger):
    return _call_backends(backend_opts, log, "cleanup", "cleaning up")


