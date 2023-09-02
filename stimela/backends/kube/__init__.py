from typing import Dict, List, Optional, Any
from omegaconf import OmegaConf
from dataclasses import dataclass
from scabha.basetypes import EmptyDictDefault, EmptyListDefault


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
from .run_kube import KubernetesDaskCluster, KubernetesFileInjection, KubernetesLocalMount, KubernetesPodSpec

@dataclass
class KubernetesBackendOptions(object):
    enable:         bool = True
    namespace:      Optional[str] = None
    dask_cluster:   Optional[KubernetesDaskCluster] = None
    service_account: str = "compute-runner"
    
    inject_files:   Dict[str, KubernetesFileInjection] = EmptyDictDefault()
    pre_commands:   List[str] = EmptyListDefault()
    local_mounts:   Dict[str, KubernetesLocalMount] = EmptyDictDefault()
    volumes:        Dict[str, str] = EmptyDictDefault()
    env:            Dict[str, str] = EmptyDictDefault()
    dir:            Optional[str] = None                 # change to specific directory inside container

    always_pull_images: bool = False                            # change to True to repull

    status_bar: bool = True                              # enable status bar display for k8s

    debug_mode: bool = False                             # in debug mode, payload is not run

    job_pod:        KubernetesPodSpec = KubernetesPodSpec()

    # if >0, events will be collected and reported
    verbose_events:        int = 0
    # format string for reporting kubernetes events, this can include rich markup
    verbose_event_format:  str = "[blue]\[kubernetes event type: {event.type}, reason: {event.reason}] {event.message}[/blue]"

    # user and group IDs -- if None, use local user
    uid:            Optional[int] = None
    gid:            Optional[int] = None
    
    # user-defined set of pod types -- each is a pod spec structure keyed by pod_type
    predefined_pod_specs: Dict[str, Dict[str, Any]] = EmptyDictDefault()


KubernetesBackendSchema = OmegaConf.structured(KubernetesBackendOptions)
