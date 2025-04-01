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
import pwd
import grp
import os

import stimela
from scabha.basetypes import ( EmptyDictDefault, DictDefault, EmptyListDefault, 
                            ListDefault, EmptyClassDefault)
from stimela.exceptions import BackendError

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
    memory:         Optional[PodLimits] = EmptyClassDefault(PodLimits)
    cpu:            Optional[PodLimits] = EmptyClassDefault(PodLimits)
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

        on_exit:    ExitOptions = EmptyClassDefault(ExitOptions)         # startup behaviour options
        on_startup: StartupOptions = EmptyClassDefault(StartupOptions)   # cleanup behaviour options

    @dataclass 
    class Volume(object):
        """Persistent volume claim config."""
        name: Optional[str] = None                                # populated with PVC name when allocated
        capacity: Optional[str] = None                            # capacity (e.g. 120Gi)
        storage_class_name: Optional[str] = None                  # k8s storage class
        access_modes: List[str] = ListDefault("ReadWriteOnce")    # ReadOnlyMany/ReadWriteMany for multi-attach
        provision_timeout: int = 1200                             # How long to wait for provisioning before timing out
        mount: Optional[str] = None                               # mount point

        from_snapshot: Optional[str] = None                       # create from snapshot

        # Status of PVC at start of sesssion or at start of step:
        # must_exist: reuse, error if it doesn't exist
        # allow_reuse: reuse if exists, else create
        # recreate: delete if exists and recreate
        # cant_exist: report an error if it exists, else create
        ExistPolicy = Enum("ExistPolicy", "must_exist allow_reuse recreate cant_exist", module=__name__)
        at_start: ExistPolicy = ExistPolicy.allow_reuse
        at_step: ExistPolicy = ExistPolicy.allow_reuse

        # commands issued in the volume at initialization. E.g. "chmod 777", "mkdir xxx", etc. These run as root.
        init_commands: List[str] = EmptyListDefault()
        # commands issued in the volume before each step initialization. E.g. "rm -fr *", "mkdir xxx", etc. 
        # These run as the user.
        step_init_commands: List[str] = EmptyListDefault()

        # lifecycle policy
        # persist: leave the PVC in place for future re-use
        # session: delete at end of stimela run
        # step: delete at end of step (only applies to per-step PVCs)
        Lifecycle = Enum("Lifecycle", "persist session step", module=__name__)
        lifecycle: Lifecycle = Lifecycle.session 

        append_id: bool = True    # for session- or step-lifecycle PVCs, append ID to name

        def __post_init__ (self):
            self.status = "Created"
            self.creation_time = time.time()
            self.metadata = None
            self.owner = None
            self.initialized = False

    # subclasses for options
    @dataclass
    class DaskCluster(object):
        enable: bool = False
        capture_logs: bool = True
        capture_logs_style: Optional[str] = "blue"
        name: Optional[str] = None
        num_workers: int = 1
        threads_per_worker: int = 1
        memory_limit: Optional[str] = None
        worker_pod: KubePodSpec = EmptyClassDefault(KubePodSpec)
        scheduler_pod: KubePodSpec = EmptyClassDefault(KubePodSpec)
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


    enable: bool = True

    # infrastructure settings are global and can't be changed per cab or per step
    infrastructure: Infrastructure = EmptyClassDefault(Infrastructure)

    context:        Optional[str] = None   # k8s context -- use default if not given -- can't change
    namespace:      Optional[str] = None   # k8s namespace
    
    dask_cluster:   Optional[DaskCluster] = EmptyClassDefault(DaskCluster)  # if set, a DaskJob will be created
    service_account: str = "compute-runner"
    kubectl_path:   str = "kubectl"

    volumes:        Dict[str, Volume] = EmptyDictDefault()

    # inject_files:   Dict[str, FileInjection] = EmptyDictDefault()
    # pre_commands:   List[str] = EmptyListDefault()
    # local_mounts:   Dict[str, LocalMount] = EmptyDictDefault()
    env:            Dict[str, str] = EmptyDictDefault()
    dir:            Optional[str] = None                 # change to specific directory inside container

    provisioning_timeout: int = 600                      # timeout in seconds for pods/jobs to provision. 0 to disable

    connection_timeout: int = 60                         # connection timeout when talking to cluster, in seconds

    always_pull_images: bool = False                     # change to True to repull

    @dataclass
    class DebugOptions(object):
        verbose: int = 0                      # debug log level. Higher numbers mean more verbosity
        pause_on_start: bool = False        # pause instead of running payload
        pause_on_cleanup: bool = False      # pause before attempting cleanup

        save_spec: Optional[str] = None     # if set, pod/job specs will be saved as YaML to the named file. {}-substitutions apply to filename.

        # if >0, events will be collected and reported
        log_events:  bool = False
        # format string for reporting kubernetes events, this can include rich markup
        event_format:  str = "=NOSUBST('\\[k8s event type: {event.type}, reason: {event.reason}] {event.message}')"
        event_colors:  Dict[str, str] = DictDefault(
                                warning="blue", error="yellow", default="grey50")
    
    debug: DebugOptions = EmptyClassDefault(DebugOptions)


    job_pod: KubePodSpec = EmptyClassDefault(KubePodSpec)
    
    capture_logs_style: Optional[str] = "blue"

    @dataclass 
    class UserInfo(object):
        # user and group names and IDs -- if None, use local user
        name:           Optional[str] = None
        group:          Optional[str] = None
        uid:            Optional[int] = None
        gid:            Optional[int] = None
        gecos:          Optional[str] = None
        home:           Optional[str] = None     # home dir inside container, default is /home/{user}
        home_ramdisk:   bool = True              # home dir mounted as RAM disk, else local disk
        inject_nss:     bool = True              # inject user info for NSS_WRAPPER

    user: UserInfo = EmptyClassDefault(UserInfo)
    
    # user-defined set of pod types -- each is a pod spec structure keyed by pod_type
    predefined_pod_specs: Dict[str, Dict[str, Any]] = EmptyDictDefault()


KubeBackendSchema = OmegaConf.structured(KubeBackendOptions)
    
def is_available(opts: Optional[KubeBackendOptions]= None):
    return AVAILABLE

def get_status():
    return STATUS

def is_remote():
    return True

def init(backend: 'stimela.backend.StimelaBackendOptions', log: logging.Logger):
    from . import infrastructure
    global AVAILABLE, STATUS
    if not infrastructure.init(backend, log):
        AVAILABLE = False
        STATUS = "initialization error"

def close(backend: 'stimela.backend.StimelaBackendOptions', log: logging.Logger):
    from . import infrastructure
    if AVAILABLE:
        infrastructure.close(backend, log)

def cleanup(backend: 'stimela.backend.StimelaBackendOptions', log: logging.Logger):
    from . import infrastructure
    infrastructure.cleanup(backend, log)

def run(cab: 'stimela.kitchen.cab.Cab', params: Dict[str, Any], fqname: str,
        backend: 'stimela.backend.StimelaBackendOptions',
        log: logging.Logger, subst: Optional[Dict[str, Any]] = None,
        wrapper: Optional['stimela.backends.runner.BackendWrapper'] = None):
    from . import run_kube
    return run_kube.run(cab=cab, params=params, fqname=fqname, backend=backend, log=log, subst=subst)

_kube_config = _kube_context = _kube_namespace = None 

def get_kube_api(context: Optional[str]=None):
    global _kube_config, _kube_context, _kube_namespace

    if _kube_config is None:
        _kube_config = True
        kubernetes.config.load_kube_config(context=context)
        contexts, current_context = kubernetes.config.list_kube_config_contexts()
        if context is None:
            context = current_context['name']
        _kube_context = context
        for ctx in contexts:
            if ctx['name'] == context:
                _kube_namespace = ctx['context']['namespace']
                break
        else:
            _kube_namespace = "default"

    elif context is not None and context != _kube_context:
        raise BackendError(f"k8s context has changed (was {_kube_context}, now {context}), this is not permitted")

    return _kube_namespace, core_v1_api.CoreV1Api(), CustomObjectsApi()

def get_context_namespace():
    return _kube_context, _kube_namespace

_uid = os.getuid()
_gid = os.getgid()

session_user_info = KubeBackendOptions.UserInfo(
    name=session_user,
    group=grp.getgrgid(_gid).gr_name,
    uid=_uid,
    gid=_gid,
    home=f"/home/{session_user}",
    gecos=pwd.getpwuid(_uid).pw_gecos
)
