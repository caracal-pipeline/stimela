from typing import Dict, List, Optional, Any
from enum import Enum
from omegaconf import OmegaConf
from dataclasses import dataclass
import time
import yaml
import json
import secrets
import getpass
import logging
import time

import stimela
from scabha.basetypes import EmptyDictDefault, EmptyListDefault, ListDefault

session_id = secrets.token_hex(8)
session_user = getpass.getuser()

resource_labels = dict(stimela_user=session_user,
                       stimela_session_id=session_id)

try:
    import kubernetes
    from kubernetes.client import CustomObjectsApi
    from kubernetes.client.api import core_v1_api
    AVAILABLE = True
    STATUS = "ok"
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

def init(backend: 'stimela.backend.StimelaBackendOptions', log: logging.Logger):
    from . import infrastructure
    infrastructure.init(backend, log)

def close(backend: 'stimela.backend.StimelaBackendOptions', log: logging.Logger):
    from . import infrastructure
    infrastructure.close(backend, log)

def cleanup(backend: 'stimela.backend.StimelaBackendOptions', log: logging.Logger):
    from . import infrastructure
    infrastructure.cleanup(backend, log)

def run(cab: 'stimela.kitchen.cab.Cab', params: Dict[str, Any], fqname: str,
        backend: 'stimela.backend.StimelaBackendOptions',
        log: logging.Logger, subst: Optional[Dict[str, Any]] = None):
    from . import run_kube
    return run_kube.run(cab=cab, params=params, fqname=fqname, backend=backend, log=log, subst=subst)

_kube_client = _kube_config = None

def get_kube_api():
    global _kube_client
    global _kube_config

    if _kube_config is None:
        _kube_config = True
        kubernetes.config.load_kube_config()

    return core_v1_api.CoreV1Api(), CustomObjectsApi()


# dict of methods for converting an object to text format
InjectedFileFormatters = dict(
    yaml = yaml.dump,
    json = json.dumps,
    txt = str
)

InjectedFileFormats = Enum("InjectedFileFormats", " ".join(InjectedFileFormatters.keys()), module=__name__)

@dataclass
class KubePodSpec(object):
    """Pod spec options. Used for job pods and for dask cluster pods."""
    @dataclass
    class PodLimits(object):
        """Pod limits and requuirements"""
        request: Optional[str] = None
        limit: Optional[str] = None

    # selects a specific pod type from a KubeBackendOptions.predefined_pod_specs
    type:           Optional[str] = None
    # memory limit/requirement
    memory:         Optional[PodLimits] = None
    cpu:            Optional[PodLimits] = None
    # arbitrary additional structure copied into the pod spec
    custom_pod_spec:  Dict[str, Any] = EmptyDictDefault()



@dataclass
class KubeBackendOptions(object):
    """
    Kube backend options class. Note that this can be defined globally in options, and 
    redefined/augmented at cab and step level.
    """

    @dataclass
    class Infrastructure(object):
        """Infrastructural options. These can only be defined globally -- ignored at cab/step level"""
        @dataclass
        class ExitOptions(object):
            cleanup_pods: bool = True                 # extra pod cleanup at exit
        @dataclass
        class StartupOptions(object):
            report_pods: bool = True                  # report any running pods for this user on startup
            cleanup_pods: bool = True                 # clean up any running pods on startup
            report_pvcs: bool = True                  # report any transient PVCs
            cleanup_pvcs: bool = True                 # cleanup any transient PVCs

        on_exit:    ExitOptions = ExitOptions()                     # startup behaviour options
        on_startup: StartupOptions = StartupOptions()               # cleanup behaviour options

    @dataclass 
    class Volume(object):
        """Persistent volume claim config."""
        name: Optional[str] = None                                # populated with PVC name when allocated
        capacity: Optional[str] = None                            # capacity (e.g. 120Gi)
        storage_class_name: Optional[str] = None                  # k8s storage class
        access_modes: List[str] = ListDefault("ReadWriteOnce")    # ReadOnlyMany/ReadWriteMany for multi-attach
        provision_timeout: int = 1200                             # How long to wait for provisioning before timing out
        mount: Optional[str] = None                               # mount point

        # lifecycle policy
        # persist: leave the PVC in place for future re-use
        # session: delete at end of stimela run
        # step: delete at end of step (only applies to per-step PVCs)
        Lifecycle = Enum("Lifecycle", "persist session step", module=__name__)
        lifecycle: Lifecycle = Lifecycle.session 

        reuse: bool = True        # if a PVC with that name already exists, reuse it, else error
        append_id: bool = True    # for session- or step-lifecycle PVCs, append ID to name

        def __post_init__ (self):
            self.status = "Created"
            self.creation_time = time.time()
            self.metadata = None

    # subclasses for options
    @dataclass
    class DaskCluster(object):
        capture_logs: bool = True
        capture_logs_style: Optional[str] = "blue"
        name: Optional[str] = None
        num_workers: int = 0
        threads_per_worker: int = 1
        worker_pod: KubePodSpec = KubePodSpec()
        scheduler_pod: KubePodSpec = KubePodSpec()
        forward_dashboard_port: int = 8787          # set to non-0 to forward the http dashboard to this local port

    @dataclass
    class FileInjection(object):
        format: InjectedFileFormats = "txt"
        content: Any = ""

    @dataclass
    class LocalMount(object):
        path: str
        dest: str = ""              # destination path -- same as local if empty
        readonly: bool = False      # mount as readonly, but it doesn't work (yet?)
        mkdir: bool = False         # create dir, if it is missing


    enable:         bool = True
    namespace:      Optional[str] = None
    dask_cluster:   Optional[DaskCluster] = None
    service_account: str = "compute-runner"
    kubectl_path:   str = "kubectl"

    infrastructure: Infrastructure = Infrastructure()

    volumes:        Dict[str, Volume] = EmptyDictDefault()

    inject_files:   Dict[str, FileInjection] = EmptyDictDefault()
    pre_commands:   List[str] = EmptyListDefault()
    local_mounts:   Dict[str, LocalMount] = EmptyDictDefault()
    env:            Dict[str, str] = EmptyDictDefault()
    dir:            Optional[str] = None                 # change to specific directory inside container

    provisioning_timeout: int = 600                      # timeout in seconds for pods/jobs to provision. 0 to disable

    always_pull_images: bool = False                     # change to True to repull

    debug_mode: bool = False                             # in debug mode, payload is not run

    job_pod:        KubePodSpec = KubePodSpec()

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


KubeBackendSchema = OmegaConf.structured(KubeBackendOptions)
