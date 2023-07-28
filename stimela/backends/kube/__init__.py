from typing import Dict, List, Optional, Any
from enum import Enum
from omegaconf import OmegaConf
from dataclasses import dataclass
from scabha.basetypes import EmptyDictDefault, EmptyListDefault
import json, yaml


try:
    import kubernetes
    AVAILABLE = True
    STATUS = "ok"
    from .run_kube import run
except ImportError:
    AVAILABLE = False
    STATUS = "please reinstall with the optional kube dependency (stimela[kube])"

    def run(*args, **kw):
        raise RuntimeError(f"kubernetes backend {STATUS}")
    
def is_available():
    return AVAILABLE

def get_status():
    return STATUS

def is_remote():
    return True

from .run_kube import run

@dataclass
class KubernetesDaskRuntime(object):
    num_workers: int = 0
    cpu_limit: int = 1
    memory_limit: Optional[str] = None
    threads_per_worker: int = 1
    name: Optional[str] = None
    persist: bool = False

# dict of methods for converting an object to text format
_InjectedFileFormatters = dict(
    yaml = yaml.dump,
    json = json.dumps,
    txt = str
)

InjectedFileFormats = Enum("InjectedFileFormats", " ".join(_InjectedFileFormatters.keys()), module=__name__)

@dataclass
class KubernetesFileInjection(object):
    format: InjectedFileFormats = "txt"
    content: Any = ""

@dataclass
class KubernetesLocalMount(object):
    path: str
    dest: str = ""              # destination path -- same as local if empty
    readonly: bool = False      # mount as readonly, but it doesn't work (yet?)
    mkdir: bool = False         # create dir, if it is missing


@dataclass
class KubernetesBackendOptions(object):
    enable:         bool = True
    namespace:      Optional[str] = None
    dask_cluster:   KubernetesDaskRuntime = KubernetesDaskRuntime()
    inject_files:   Dict[str, KubernetesFileInjection] = EmptyDictDefault()
    pre_commands:   List[str] = EmptyListDefault()
    local_mounts:   Dict[str, KubernetesLocalMount] = EmptyDictDefault()
    volumes:        Dict[str, str] = EmptyDictDefault()
    env:            Dict[str, str] = EmptyDictDefault()
    dir:            Optional[str] = None                 # change to specific directory inside container

    # if >0, events will be collected and reported
    verbose:        int = 0

    # user and group IDs -- if None, use local user
    uid:            Optional[int] = None
    gid:            Optional[int] = None
    # memory limit/requirement
    memory:         Optional[str] = None

    # user-defined set of pod types -- each is a pod spec structure
    predefined_pod_types: Dict[str, Dict[str, Any]] = EmptyDictDefault()

    # selects a specific pod type from the defined set
    pod_type:       Optional[str] = None

    # arbitrary additional structure copied into the pod spec
    custom_pod_spec:  Dict[str, Any] = EmptyDictDefault()  



KubernetesBackendSchema = OmegaConf.structured(KubernetesBackendOptions)
