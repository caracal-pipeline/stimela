import logging
import atexit
import json
import time
from typing import Optional, Dict, List

from stimela.backends import StimelaBackendOptions
from stimela.stimelogging import log_exception
from stimela.task_stats import update_process_status

from stimela.exceptions import BackendError
from . import session_id, session_user, resource_labels, run_kube, KubeBackendOptions, get_kube_api, get_context_namespace

Lifecycle = KubeBackendOptions.Volume.Lifecycle

from kubernetes import client
from kubernetes.client.rest import ApiException
from kubernetes.config import ConfigException

from .kube_utils import resolve_unit

# Note that transient PVCs have a stimela claim name (without ID suffix) and an actual name (with ID suffix)
# Non-transient PVCs have the same name (no suffix in each case)
# This maps stimela claim names to Volume objects
active_pvcs: Dict[str, KubeBackendOptions.Volume] = {}

# This is a dict of terminating PVS, mapping actual names to stimela claim names
terminating_pvcs: Dict[str, str]

# This is a dict of PVCs requiring initialization
session_init_commands: Dict[str, List[str]] = {}

# logger used for global kube messages
klog: Optional[logging.Logger] = None

def _delete_pod(kube_api, podname, namespace, log, warn_not_found=True):
    log.info(f"deleting pod {podname}")
    try:
        resp = kube_api.delete_namespaced_pod(name=podname, namespace=namespace)
        log.debug(f"delete_namespaced_pod({podname}): {resp}")
    except ApiException as exc:
        body = json.loads(exc.body)
        if "reason" in body and body["reason"] == "NotFound" and warn_not_found:
            log.warning(f"pod {podname} not found, this is probably OK, perhaps it just died on its own")
        else:
            log_exception(BackendError(f"k8s API error while deleting pod {podname}", (exc, body)), 
                        severity="error", log=log)

def cleanup(backend: StimelaBackendOptions, log: logging.Logger):
    return init(backend, log, cleanup=True)

def init(backend: StimelaBackendOptions, log: logging.Logger, cleanup: bool = False):
    global klog
    klog = log.getChild("kube")
    kube = backend.kube

    if cleanup:
        klog.info("cleaning up backend")
    else:
        atexit.register(close, backend, klog)
        klog.info("initializing kube backend")

    try:
        namespace, kube_api, _ = get_kube_api(kube.context)
    except ConfigException as exc:
        log_exception(exc, log=klog)
        log_exception(BackendError("error initializing kube backend", exc), log=klog)
        return False
    
    context, namespace = get_context_namespace()
    klog.info(f"k8s context is {context}, namespace is {namespace}")

    if cleanup or kube.infrastructure.on_startup.report_pods or kube.infrastructure.on_startup.cleanup_pods:
        klog.info("checking for k8s pods from other sessions")

        try:
            pods = kube_api.list_namespaced_pod(namespace=namespace, 
                                                label_selector=f"stimela_user={session_user}")
        except ApiException as exc:
            raise BackendError(f"k8s API error while listing pods", json.loads(exc.body))
        
        running_pods = []
        for pod in pods.items:
            if pod.status.phase in ("Running", "Pending") and not pod.metadata.deletion_timestamp:
                running_pods.append(pod.metadata.name)

        if running_pods:
            if cleanup or kube.infrastructure.on_startup.cleanup_pods:
                klog.warning(f"you have {len(running_pods)} pod(s) running from another stimela session")
                if not cleanup:
                    klog.warning(f"since kube.infrastructure.on_starup.cleanup_pods is set, these will be terminated")
                for podname in running_pods:
                    _delete_pod(kube_api, podname, namespace, klog)

            elif kube.infrastructure.on_startup.report_pods:
                klog.warning(f"you have {len(running_pods)} pod(s) running from another stimela session")
                klog.warning(f"set kube.infrastructure.on_startup.report_pods to false to disable this warning")
        else:
            klog.info("no pods running")

    refresh_pvc_list(kube)

    # cleanup transient PVCs
    transient_pvcs = {name: pvc for name, pvc in active_pvcs.items() 
                        if pvc.metadata.labels and 'stimela_transient_pvc' in pvc.metadata.labels}
    
    if transient_pvcs:
        if cleanup or kube.infrastructure.on_startup.cleanup_pvcs:
            klog.warning(f"you have {len(transient_pvcs)} transient PVC(s) from another stimela session")
            if not cleanup:
                klog.warning(f"since kube.infrastructure.on_starup.cleanup_pvcs is set, these will be deleted")
            delete_pvcs(kube, transient_pvcs.keys(), log=log, refresh=False, force=True)
            refresh_pvc_list(kube)

        elif kube.infrastructure.on_startup.report_pvcs:
            klog.warning(f"you have {len(transient_pvcs)} transient PVC(s) from another stimela session")
            klog.warning(f"set kube.infrastructure.on_startup.report_pvcs to false to disable this warning")
    elif cleanup:
        klog.info("checking for transient PVCs: none found")

    # resolve global-level volumes
    if not cleanup and kube.volumes:
        resolve_volumes(kube, log=klog, refresh=False) # no refresh needed

    return True


def refresh_pvc_list(kube: KubeBackendOptions):
    namespace, kube_api, _ = get_kube_api()
    global active_pvcs, terminating_pvcs
    # get existing pvcs
    try:
        list_pvcs = kube_api.list_namespaced_persistent_volume_claim(namespace)
    except ApiException as exc:
        raise BackendError(f"k8s API error while listing PVCs", json.loads(exc.body)) from None
    pvc_names = []
    terminating_pvcs = {}
    # convert to PVC entry 
    for pvc in list_pvcs.items:
        if pvc.metadata.labels and 'stimela_pvc_name' in pvc.metadata.labels:
            name = pvc.metadata.labels['stimela_pvc_name']
        else:
            name = pvc.metadata.name
        # add to terminating list, if marked for deletion
        if pvc.metadata.deletion_timestamp:
            terminating_pvcs[pvc.metadata.name] = name
            continue
        pvc_names.append(name)
        # insert new entry if it doesn't exist
        if name not in active_pvcs:
            active_pvcs[name] = KubeBackendOptions.Volume(name=pvc.metadata.name, 
                                    capacity=pvc.spec.resources.requests['storage'], 
                                    lifecycle=Lifecycle.persist)
        active_pvcs[name].status = pvc.status.phase
        active_pvcs[name].metadata = pvc.metadata
        active_pvcs[name].owner = pvc.metadata.labels and pvc.metadata.labels.get("stimela_user")
    # delete stale entries
    active_pvcs = {name: active_pvcs[name] for name in pvc_names} 


def resolve_volumes(kube: KubeBackendOptions, log: logging.Logger, step_token=None, refresh=True):
    namespace, kube_api, _ = get_kube_api()
    ExistsPolicy = KubeBackendOptions.Volume.ExistPolicy
    global terminating_pvcs

    if refresh:
        refresh_pvc_list(kube)

    # look for required PVCs
    for name, pvc in kube.volumes.items():
        exist_policy = pvc.at_step if step_token else pvc.at_start
        exist_policy_desc = 'at step' if step_token else 'at start'
        # Exists? Check that size is enough
        pvc0 = active_pvcs.get(name)
        # check for existing PVCs
        if pvc0 is not None:
            if exist_policy == ExistsPolicy.cant_exist:
                raise BackendError(f"PVC '{name}' already exists: according to its '{exist_policy_desc}' policy, this is an error")
            # check if we need to re-create
            if exist_policy == ExistsPolicy.recreate:
                log.info(f"PVC '{name}' already exists: re-creating according to its '{exist_policy_desc}' policy")
                delete_pvcs(kube, pvc_names=[name], log=log, force=True, refresh=False)
                pvc0 = None
            # else reusing -- check capacity 
            elif pvc.capacity is None or resolve_unit(pvc.capacity) <= resolve_unit(pvc0.capacity):
                log.info(f"found existing PVC '{name}' of size {pvc0.capacity}, status is {pvc0.status}")
                # copy name -- pre-existsing PVC may have been auto-named
                pvc.name = pvc0.name
            else:
                raise BackendError(f"Existing PVC '{name}' of size {pvc0.capacity} is smaller than the requested {pvc.capacity}")
            
        # Doesn't exist? Create
        if pvc0 is None:
            if exist_policy == ExistsPolicy.must_exist:
                raise BackendError(f"PVC '{name}' doesn't exist: according to its '{exist_policy_desc}' policy, this is an error")
            if pvc.storage_class_name is None or pvc.capacity is None:
                raise BackendError(f"Can't create PVC '{name}': storage class name or capacity not specified")
            # create new one
            pvc.name = name
            pvc.status = 'Creating'
            pvc.creation_time = time.time()
            labels = resource_labels.copy()
            labels['stimela_pvc_name'] = name
            labels['stimela_pvc_initialized'] = ''
            # append token for limited-lifecycle PVCs
            if pvc.append_id:
                if pvc.lifecycle == Lifecycle.session:
                    pvc.name = f"{name}-{session_id}"
                    labels['stimela_transient_pvc'] = 'session'
                elif pvc.lifecycle == Lifecycle.step:
                    if step_token is None:
                        raise BackendError(f"PVC '{name}' with lifecycle=step not allowed at infrastructure level")
                    pvc.name = f"{name}-{step_token}"
                    labels['stimela_transient_pvc'] = 'step'
            # if existing PVC with that is still terminating, wait
            if pvc.name in terminating_pvcs:
                log.info(f"waiting for existing PVC '{pvc.name}' to terminate before re-creating")
                _await_pvc_termination(namespace, pvc, log=log)
            # create
            newpvc = client.V1PersistentVolumeClaim()
            newpvc.metadata = client.V1ObjectMeta(name=pvc.name, labels=labels)
            if pvc.from_snapshot:
                data_source = dict(name=pvc.from_snapshot, 
                                   kind='VolumeSnapshot',
                                   apiGroup='snapshot.storage.k8s.io')
                log.info(f"creating new PVC '{pvc.name}' of size {pvc.capacity} from snapshot '{pvc.from_snapshot}'")
            else:
                log.info(f"creating new PVC '{pvc.name}' of size {pvc.capacity}")
                data_source = None
            newpvc.spec = client.V1PersistentVolumeClaimSpec(
                access_modes=list(pvc.access_modes),
                storage_class_name=pvc.storage_class_name,
                data_source=data_source,
                resources=client.V1ResourceRequirements(requests={"storage": pvc.capacity}))
            try:
                resp = kube_api.create_namespaced_persistent_volume_claim(namespace, newpvc)
            except ApiException as exc:
                raise BackendError(f"k8s API error while creating PVC '{pvc.name}'", json.loads(exc.body)) from None
            pvc.owner = session_user
            active_pvcs[name] = pvc
        # reusing volume -- but check if it is initialized
        else:
            pvc0.initialized = pvc0.metadata.labels and pvc0.metadata.labels.get("stimela_pvc_initialized")

    return list(kube.volumes.keys())


def await_pvcs(namespace, pvc_names, log: logging.Logger):
    namespace, kube_api, _ = get_kube_api()

    waiting_pvcs = set(pvc_names)
    waiting_reported = set()

    while waiting_pvcs:
        update_process_status()
        # recheck state of all PVCs we're waiting on
        for name in list(waiting_pvcs):
            pvc = active_pvcs.get(name)
            if pvc is None:
                raise BackendError(f"'{name}' does not refer to a previously defined PVC")
            if pvc.status == "Bound":
                waiting_pvcs.remove(name)
                continue
            # get updated status
            try:
                pvc_entry = kube_api.read_namespaced_persistent_volume_claim(name=pvc.name, namespace=namespace)
            except ApiException as exc:
                raise BackendError(f"k8s API error while reading PVC '{pvc.name}'", json.loads(exc.body))
            pvc.status = pvc_entry.status.phase
            if pvc.status == 'Bound':
                log.info(f"PVC '{pvc.name}' is now bound")
                waiting_pvcs.remove(name)
                continue
            # check for timeout
            if pvc.provision_timeout and time.time() > pvc.creation_time + pvc.provision_timeout:
                raise BackendError(f"timed out waiting for PVC '{pvc.name}' to provision")
            # report that we're waiting
            if name not in waiting_reported:
                log.info(f"waiting for PVC '{pvc.name}' to provision")
                waiting_reported.add(name)
        # wait some more
        time.sleep(1)

def _await_pvc_termination(namespace, pvc: KubeBackendOptions.Volume, log: logging.Logger):
    global terminating_pvcs
    namespace, kube_api, _ = get_kube_api()
    time0 = time.time()
    while True:
        update_process_status()
        # recheck state of all PVCs we're waiting on
        try:
            kube_api.read_namespaced_persistent_volume_claim(name=pvc.name, namespace=namespace)
        except ApiException as exc:
            body = json.loads(exc.body)
            if "reason" in body and body["reason"] == "NotFound":
                log.info(f"PVC '{pvc.name}' has terminated")
                del terminating_pvcs[pvc.name]
                return
            else:
                raise BackendError(f"k8s API error while reading PVC '{pvc.name}'", json.loads(exc.body))
        if pvc.provision_timeout and time.time() > time0 + pvc.provision_timeout:
            raise BackendError(f"timed out waiting for PVC '{pvc.name}' to terminate")
        # wait some more
        time.sleep(1)



def delete_pvcs(kube: KubeBackendOptions, pvc_names, log: logging.Logger, force=False, step=True, session=False, refresh=True):
    namespace, kube_api, _ = get_kube_api()
    global terminating_pvcs

    if refresh:
        refresh_pvc_list(kube)
    
    for name in pvc_names:
        if name in terminating_pvcs:
            continue

        pvc = active_pvcs.get(name)
        if pvc is None:
            raise BackendError(f"'{name}' does not refer to a previously defined PVC")
        
        if pvc.status != 'Terminating' and \
            force or \
            (step and pvc.lifecycle == Lifecycle.step) or \
            (session and pvc.lifecycle == Lifecycle.session):
            log.info(f"deleting PVC '{pvc.name}'")
            try:
                resp = kube_api.delete_namespaced_persistent_volume_claim(name=pvc.name, namespace=namespace)
            except ApiException as exc:
                body = json.loads(exc.body)
                log_exception(BackendError(f"k8s API error while deleting PVC '{pvc.name}'", (exc, body)), 
                                severity="error", log=log)
                continue
            log.debug(f"delete_namespaced_persistent_volume_claim({pvc.name}): {resp}")
            pvc.status = 'Terminating'
            terminating_pvcs[pvc.name] = name


def close(backend: StimelaBackendOptions, log: logging.Logger):
    kube = backend.kube
    context, namespace = get_context_namespace()
    if context is None:
        return 
    
    klog.info("closing kube backend")

    # release PVCs
    delete_pvcs(kube, list(active_pvcs.keys()), log=klog, session=True, step=True, refresh=False)

    # cleanup pods, if any
    if kube.infrastructure.on_exit.cleanup_pods:
        namespace, kube_api, _ = run_kube.get_kube_api() 

        try:
            pods = kube_api.list_namespaced_pod(namespace=namespace, 
                                                label_selector=f"stimela_session_id={session_id}")
        except ApiException as exc:
            body = json.loads(exc.body)
            log_exception(BackendError(f"k8s API error while listing pods", (exc, body)), severity="error", log=klog)
            return
        
        running_pods = []
        for pod in pods.items:
            if pod.status.phase in ("Running", "Pending") and not pod.metadata.deletion_timestamp:
                running_pods.append(pod.metadata.name)

        if running_pods:
            klog.warning(f"you have {len(running_pods)} pod(s) still pending or running from this session")
            klog.warning(f"since kube.infrastructure.on_exit.cleanup_pods is set, these will be terminated")
            for podname in running_pods:
                _delete_pod(kube_api, podname, namespace, klog)

    atexit.unregister(close)

