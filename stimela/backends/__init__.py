import os.path
from dataclasses import dataclass
from typing import Union, Dict, Any, List, Optional
from enum import Enum
from omegaconf import ListConfig, OmegaConf
from stimela.exceptions import BackendSpecificationError
from scabha.basetypes import EmptyDictDefault

import stimela.kitchen

from .singularity import SingularityBackendOptions
from .kubernetes import KubernetesBackendOptions
from .native import NativeBackendOptions

## left as memo to self
# Backend = Enum("Backend", "docker singularity podman kubernetes native", module=__name__)
Backend = Enum("Backend", "singularity kubernetes native", module=__name__)

SUPPORTED_BACKENDS = set(Backend.__members__)


def get_backend(name: str):
    if name not in SUPPORTED_BACKENDS:
        return None
    backend = __import__(f"stimela.backends.{name}", fromlist=[name])
    if backend.is_available():
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

    select: Any = ""   # should be Union[str, List[str]], but OmegaConf doesn't support it, so handle in __post_init__ for now
    
    singularity: Optional[SingularityBackendOptions] = None
    kube: Optional[KubernetesBackendOptions] = None
    native: Optional[NativeBackendOptions] = None 
    docker: Optional[Dict] = None  # placeholder for future impl
    slurm: Optional[Dict] = None   # placeholder for future impl

    ## Resource limits applied during run -- see resource module
    rlimits: Dict[str, Any] = EmptyDictDefault()

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
        if self.kube is None and get_backend("kubernetes"):
            self.kube = KubernetesBackendOptions()
                        

StimelaBackendSchema = OmegaConf.structured(StimelaBackendOptions)

## commenting out for now -- will need to fix when we reactive the kube backend (and have tests for it)

# def resolve_required_mounts(params: Dict[str, Any], 
#                             inputs: Dict[str, 'stimela.kitchen.cab.Parameter'], 
#                             outputs: Dict[str, 'stimela.kitchen.cab.Parameter'],
#                             prior_mounts: Dict[str, bool]):

#     targets = {}

#     # helper function to accumulate list of target paths to be mounted
#     def add_target(path, must_exist, readwrite):
#         if must_exist and not os.path.exists(path):
#             raise SchemaError(f"{path} does not exist.")

#         path = os.path.abspath(path)

#         # if path doesn't exist, mount parent dir as read/write (file will be created in there)
#         if not os.path.lexists(path):
#             add_target(os.path.dirname(path), must_exist=True, readwrite=True)
#         # else path is real
#         else:
#             # already mounted? Make sure readwrite is updated
#             if path in targets:
#                 targets[path] = targets[path] or readwrite
#             else:
#                 # not mounted, but is a link
#                 if os.path.islink(path):
#                     # add destination as target
#                     add_target(os.path.realpath(path), must_exist=must_exist, readwrite=readwrite)
#                     # add parent dir as readonly target (to resolve the symlink)
#                     add_target(os.path.dirname(path), must_exist=True, readwrite=False)
#                 # add to mounts
#                 else:
#                     targets[path] = readwrite

#     # go through parameters and accumulate target paths
#     for name, value in params.items():
#         schema = inputs.get(name) or outputs.get(name)
#         if schema is None:
#             raise SchemaError(f"parameter {name} not in defined inputs or outputs for this cab. This should have been caught by validation earlier!")

#         dtype = schema._dtype 
#         if dtype in (File, Directory, MS):
#             files = [value]
#         elif dtype in (List[File], List[Directory], List[MS]):
#             files = value
#         else:
#             continue

#         must_exist = schema.must_exist
#         if must_exist is None:
#             must_exist = name in inputs            
#         readwrite = schema.writable or name in outputs

#         # for symlink targets, we need to mount the parent directory
#         for path in files:
#             add_target(path, must_exist=must_exist, readwrite=readwrite)

    
#     # now eliminate unnecessary targets (those that have a parent mount with the same read/write property)
#     skip_targets = set()

#     for path, readwrite in targets.items():
#         parent = os.path.dirname(path)
#         while parent != "/":  
#             # if parent already mounted, and is as writeable as us, skip us
#             if (parent in targets and targets[parent] >= readwrite) or \
#                 (parent in prior_mounts and prior_mounts[parent] >= readwrite):
#                 skip_targets.add(path)
#                 break
#             parent = os.path.dirname(parent)

#     for path in skip_targets:
#         targets.pop(path)

#     return targets

        

        