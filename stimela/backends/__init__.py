import os.path
from dataclasses import dataclass
from typing import Union, Dict, Any, List, Optional
from enum import Enum
from omegaconf import OmegaConf, MISSING

import stimela.kitchen
from stimela.exceptions import SchemaError
from scabha.basetypes import File, Directory, MS, EmptyDictDefault, EmptyListDefault


# @dataclass
# class StimelaImageBuildInfo:
#     stimela_version: str = ""
#     user: str = ""
#     date: str = ""
#     host: str = ""  

# @dataclass
# class StimelaImageInfo:
#     name: str = ""
#     version: str = ""
#     full_name: str = ""
#     iid: str = ""
#     build: Union[StimelaImageBuildInfo, None] = None

# @dataclass
# class ImageBuildInfo:
#     info: Optional[str] = ""
#     dockerfile: Optional[str] = "Dockerfile"
#     production: Optional[bool] = True          # False can be used to mark test (non-production) images 

@dataclass
class StimelaImage:
    name: str
    version: str
    # name: str = MISSING
    # info: str = "image description"
    # images: Dict[str, ImageBuildInfo] = MISSING
    # _path: Optional[str] = None   # path to image definition yaml file, if any

    # # optional library of common parameter sets
    # params: Dict[str, Any] = EmptyDictDefault()


from .singularity import SingularityBackendOptions
from .kubernetes import KubernetesBackendOptions

#Backend = Enum("Backend", "docker singularity podman kubernetes native", module=__name__)
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
    registry: str = "quay.io/stimela2"
    
    singularity: Optional[SingularityBackendOptions] = None
    kube: Optional[KubernetesBackendOptions] = None
    native: Optional[Dict] = None  # native backend has no options for now 
    docker: Optional[Dict] = None  # placeholder for future impl
    slurm: Optional[Dict] = None   # placeholder for future impl

StimelaBackendSchema = OmegaConf.structured(StimelaBackendOptions)




def resolve_required_mounts(params: Dict[str, Any], 
                            inputs: Dict[str, 'stimela.kitchen.cab.Parameter'], 
                            outputs: Dict[str, 'stimela.kitchen.cab.Parameter'],
                            prior_mounts: Dict[str, bool]):

    targets = {}

    # helper function to accumulate list of target paths to be mounted
    def add_target(path, must_exist, readwrite):
        if must_exist and not os.path.exists(path):
            raise SchemaError(f"{path} does not exist.")

        path = os.path.abspath(path)

        # if path doesn't exist, mount parent dir as read/write (file will be created in there)
        if not os.path.lexists(path):
            add_target(os.path.dirname(path), must_exist=True, readwrite=True)
        # else path is real
        else:
            # already mounted? Make sure readwrite is updated
            if path in targets:
                targets[path] = targets[path] or readwrite
            else:
                # not mounted, but is a link
                if os.path.islink(path):
                    # add destination as target
                    add_target(os.path.realpath(path), must_exist=must_exist, readwrite=readwrite)
                    # add parent dir as readonly target (to resolve the symlink)
                    add_target(os.path.dirname(path), must_exist=True, readwrite=False)
                # add to mounts
                else:
                    targets[path] = readwrite

    # go through parameters and accumulate target paths
    for name, value in params.items():
        schema = inputs.get(name) or outputs.get(name)
        if schema is None:
            raise SchemaError(f"parameter {name} not in defined inputs or outputs for this cab. This should have been caught by validation earlier!")

        dtype = schema._dtype 
        if dtype in (File, Directory, MS):
            files = [value]
        elif dtype in (List[File], List[Directory], List[MS]):
            files = value
        else:
            continue

        must_exist = schema.must_exist
        if must_exist is None:
            must_exist = name in inputs            
        readwrite = schema.writable or name in outputs

        # for symlink targets, we need to mount the parent directory
        for path in files:
            add_target(path, must_exist=must_exist, readwrite=readwrite)

    
    # now eliminate unnecessary targets (those that have a parent mount with the same read/write property)
    skip_targets = set()

    for path, readwrite in targets.items():
        parent = os.path.dirname(path)
        while parent != "/":  
            # if parent already mounted, and is as writeable as us, skip us
            if (parent in targets and targets[parent] >= readwrite) or \
                (parent in prior_mounts and prior_mounts[parent] >= readwrite):
                skip_targets.add(path)
                break
            parent = os.path.dirname(parent)

    for path in skip_targets:
        targets.pop(path)

    return targets

        

        