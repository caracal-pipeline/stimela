from typing import Dict, List, Optional, Any
from enum import Enum
from omegaconf import OmegaConf
from dataclasses import dataclass
import yaml
import json
import secrets
import getpass

from scabha.basetypes import EmptyDictDefault, EmptyListDefault

session_id = secrets.token_hex(8)
session_user = getpass.getuser()

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

# dict of methods for converting an object to text format
InjectedFileFormatters = dict(
    yaml = yaml.dump,
    json = json.dumps,
    txt = str
)

InjectedFileFormats = Enum("InjectedFileFormats", " ".join(InjectedFileFormatters.keys()), module=__name__)

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
class KubernetesPodLimits(object):
    request: Optional[str] = None
    limit: Optional[str] = None

@dataclass
class KubernetesPodSpec(object):
    # selects a specific pod type from the defined set
    type:           Optional[str] = None
    # memory limit/requirement
    memory:         Optional[KubernetesPodLimits] = None
    cpu:            Optional[KubernetesPodLimits] = None
    # arbitrary additional structure copied into the pod spec
    custom_pod_spec:  Dict[str, Any] = EmptyDictDefault()

@dataclass
class KubernetesDaskCluster(object):
    capture_logs: bool = True
    capture_logs_style: Optional[str] = "blue"
    name: Optional[str] = None
    num_workers: int = 0
    threads_per_worker: int = 1
    worker_pod: KubernetesPodSpec = KubernetesPodSpec()
    scheduler_pod: KubernetesPodSpec = KubernetesPodSpec()
    forward_dashboard_port: int = 8787          # set to non-0 to forward the http dashboard to this local port


@dataclass
class KubernetesBackendOptions(object):
    enable:         bool = True
    namespace:      Optional[str] = None
    dask_cluster:   Optional[KubernetesDaskCluster] = None
    service_account: str = "compute-runner"
    kubectl_path:   str = "kubectl"

    inject_files:   Dict[str, KubernetesFileInjection] = EmptyDictDefault()
    pre_commands:   List[str] = EmptyListDefault()
    local_mounts:   Dict[str, KubernetesLocalMount] = EmptyDictDefault()
    volumes:        Dict[str, str] = EmptyDictDefault()
    env:            Dict[str, str] = EmptyDictDefault()
    dir:            Optional[str] = None                 # change to specific directory inside container

    always_pull_images: bool = False                     # change to True to repull

    debug_mode: bool = False                             # in debug mode, payload is not run

    cleanup_pods_on_exit: bool = True                    # extra pod cleanup at exit

    report_pods_on_startup: bool = True                  # report any running pods for this user on startup
    cleanup_pods_on_startup: bool = True                 # clean up any running pods on startup

    job_pod:        KubernetesPodSpec = KubernetesPodSpec()

    # if >0, events will be collected and reported
    verbose_events:        int = 0
    # format string for reporting kubernetes events, this can include rich markup
    verbose_event_format:  str = "\[kubernetes event type: {event.type}, reason: {event.reason}] {event.message}"
    verbose_event_color:   str = "blue"

    # user and group IDs -- if None, use local user
    uid:            Optional[int] = None
    gid:            Optional[int] = None
    
    # user-defined set of pod types -- each is a pod spec structure keyed by pod_type
    predefined_pod_specs: Dict[str, Dict[str, Any]] = EmptyDictDefault()


KubernetesBackendSchema = OmegaConf.structured(KubernetesBackendOptions)
