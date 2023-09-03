import logging
from typing import Dict, Any
import re
import json

from omegaconf import OmegaConf

from stimela.exceptions import StimelaCabParameterError, StimelaCabRuntimeError, BackendError
from stimela.stimelogging import update_process_status, log_exception
import stimela.backends

try:
    from kubernetes.client.rest import ApiException
    _enabled = True
except ImportError:
    _enabled = False
    pass  # pesumably handled by disabling the backend in __init__

k8s_cpu_units = {
    "": 1,
    "m": 1e-3,
    "u": 1e-6, # not sure this exists
    "n": 1e-9
}

k8s_memory_units_in_bytes = {
    "": 1,
    "b": 1,
    "Ki": 2**10,
    "Mi": 2**20,
    "Gi": 2**30,
    "Ti": 2**40,
    "Pi": 2**50,
    "Ei": 2**60,
    # Decimal units
    "K": 10**3,
    "M": 10**6,
    "G": 10**9,
    "T": 10**12,
    "P": 10**15,
    "E": 10**18
}

def resolve_unit(quantity:str, units: Dict):
    match = re.fullmatch("^(\d+)(.*)$", quantity)
    if not match or match.group(2) not in units:
        raise ApiException(f"invalid quantity '{quantity}'")
    return int(match.group(1))*units[match.group(2)]

def apply_pod_spec(kps, pod_spec: Dict[str, Any], predefined_pod_specs: Dict[str, Dict[str, Any]], log: logging.Logger,  kind: str) -> Dict[str, Any]:
    """applies this pod spec, as long with any predefined specs"""
    if kps:
        # apply predefined types
        if kps.type is not None:
            log.info(f"selecting predefined pod type '{kps.type}' for {kind}")
            predefined_pod_spec = predefined_pod_specs.get(kps.type)
            if predefined_pod_spec is None:
                raise StimelaCabRuntimeError(f"'{kps.type}' not found in predefined_pod_specs")
        else:
            predefined_pod_spec = {}
        # apply custom type and merge
        if predefined_pod_spec or kps.custom_pod_spec:
            pod_spec = OmegaConf.to_container(OmegaConf.merge(pod_spec, predefined_pod_spec, kps.custom_pod_spec))

        # add RAM resources
        if kps.memory is not None:
            res = pod_spec['containers'][0].setdefault('resources', {})
            # good practice to set these equal
            if kps.memory.request:
                res.setdefault('requests', {})['memory'] = kps.memory.request or kps.memory.limit
            if kps.memory.limit:
                res.setdefault('limits', {})['memory'] = kps.memory.limit or kps.memory.request
            log.info(f"setting {kind} memory resources to {res['limits']['memory']}")
        if kps.cpu is not None:
            res = pod_spec['containers'][0].setdefault('resources', {})
            if kps.cpu.request:
                res.setdefault('requests', {})['cpu'] = kps.cpu.request
            if kps.cpu.limit:
                res.setdefault('limits', {})['cpu'] = kps.cpu.limit
            log.info(f"setting {kind} CPU resources to {res['limits']['cpu']}")

    return pod_spec

class StatusReporter(object):
    def __init__(self, namespace: str, log: logging.Logger, 
                 podname: str,
                 kube: 'stimela.backends.kube.KubernetesBackendOptions',
                 event_handler: lambda event:None,
                 update_interval: float = 1,
                 enable_metrics: bool = True):
        from . import run_kube
        self.kube = kube
        self.kube_api, self.custom_api = run_kube.get_kube_api() 
        self.namespace = namespace
        self.log = log
        self.podname = podname
        self.label_selector = f"stimela_job={podname}"
        self.event_handler = event_handler
        self.update_interval = update_interval
        self.enable_metrics = enable_metrics
        
         # API errors added here when reported -- use this dict to avoid reissuing multiple errors
        self.api_errors_reported = {}
        self.pod_statuses = {}
        self.reported_events = set()
        self.main_status = None

    def set_pod_name(self, podname):
        self.podname = podname
        self.label_selector = f"stimela_job={podname}"

    def set_main_status(self, mainstat):
        self.main_status = mainstat

    def report_api_error(self, name, exc):
        if name not in self.api_errors_reported:
            self.api_errors_reported[name] = exc
            self.log.warning(f"k8s API error for {name}: {exc}")

    def log_events(self):
        try:
            pods = self.kube_api.list_namespaced_pod(namespace=self.namespace, label_selector=self.label_selector)
        except ApiException as exc:
            self.report_api_error("list_namespaced_pod", exc)
            return
        for pod in pods.items:
            # get new events
            try:
                events = self.kube_api.list_namespaced_event(namespace=self.namespace, 
                                field_selector=f"involvedObject.kind=Pod,involvedObject.name={pod.metadata.name}")
            except ApiException as exc:
                self.report_api_error("list_namespaced_event", exc)
                return
            for event in events.items:
                if event.metadata.uid not in self.reported_events:
                    self.reported_events.add(event.metadata.uid)
                    try:
                        self.event_handler(event)
                    except Exception as exc:
                        self.log.error(self.kube.verbose_event_format.format(event=event))
                        raise
                    # no error from handler, report event if configured to
                    if self.kube.verbose_events:
                        self.log.info(self.kube.verbose_event_format.format(event=event), 
                                      extra=dict(color=self.kube.verbose_event_color))
                        

    def update(self):
        from . import session_user
        self.log_events()

        # update k8s stats and metrics
        pods = metrics = None
        # get pod statuses
        try:
            pods = self.kube_api.list_namespaced_pod(self.namespace,
                                            label_selector=f"stimela_user={session_user}")
        except ApiException as exc:
            self.report_api_error("list_namespaced_pod", exc)
        # process statuses if we got them
        if pods:
            for pod in pods.items:
                pname = pod.metadata.name
                if pod.metadata.deletion_timestamp:
                    pod_status = "Terminating"
                else:
                    pod_status = pod.status.phase
                # if pod.status.container_statuses:
                #     print(f"Pod: {pname}, status: {pod_status}, {[st.state for st in pod.status.container_statuses]}")
                # get container states
                if pod.status.container_statuses:
                    for cst in pod.status.container_statuses:
                        for state in cst.state.waiting, cst.state.terminated: 
                            if hasattr(state, 'reason'):
                                pod_status += f":{state.reason}"
                self.pod_statuses[pname] = pod_status
        # get metrics
        if self.enable_metrics:
            try:
                metrics = self.custom_api.list_namespaced_custom_object('metrics.k8s.io', 'v1beta1', self.namespace, 'pods')
            except ApiException as exc:
                self.report_api_error("metrics.k8s.io", exc)
        # process metrics if we got them
        if metrics:
            totals = dict(cpu=0, memory=0)
            for item in metrics['items']:
                pname = item['metadata']['name']
                if pname in self.pod_statuses:
                    for container in item['containers']:
                        usage = container['usage']
                        totals['cpu'] += resolve_unit(usage.get('cpu'), k8s_cpu_units)
                        totals['memory'] += resolve_unit(usage.get('memory'), k8s_memory_units_in_bytes)
                    # print(f"Pod: {pname}, CPU: {usage.get('cpu')}, Memory: {usage.get('memory')}")
        # add main pod/job status
        status = self.main_status
        if status:
            status = f"|[blue]{status}[/blue]"
        elif self.podname in self.pod_statuses:
            status = f"|[blue]{self.pod_statuses[self.podname]}[/blue]"
        # add count of running pods
        npods = len(self.pod_statuses)
        pods = ""
        nrun = sum([stat.startswith("Running") for stat in self.pod_statuses.values()])
        if nrun:
            pods += f"[green]{nrun}[/green]R"
        npend = sum([stat.startswith("Pending") for stat in self.pod_statuses.values()])
        if npend:
            pods += f"[yellow]{npend}[/yellow]P"
        nterm = sum([stat.startswith("Terminating") for stat in self.pod_statuses.values()])
        if nterm:
            pods += f"[blue]{nterm}[/blue]T"
        nuk = npods - nrun - npend - nterm
        if nuk:
            pods += f"[red]{nuk}[/red]U"
        status = (status or '') + (f"|pods {pods}" if npods else "")
        # add metrics
        if metrics:
            cores = totals['cpu']
            mem_gb = round(totals['memory'] / 2**30)
            status += f"|cores [green]{totals['cpu']:.2f}[/green]|mem [green]{mem_gb}[/green]G"
            stats = dict(k8s_cores=cores, k8s_mem=mem_gb)
        else:
            stats = None
        
        return status, stats


def check_pods_on_startup(kube: 'stimela.backends.kube.KubernetesBackendOptions'):
    from stimela.stimelogging import logger
    log = logger()
    from . import run_kube, session_user
    kube_api, _ = run_kube.get_kube_api() 
    try:
        pods = kube_api.list_namespaced_pod(namespace=kube.namespace, 
                                            label_selector=f"stimela_user={session_user}")
    except ApiException as exc:
        body = json.loads(exc.body)
        log_exception(BackendError(f"k8s API error while listing pods", (exc, body)), severity="error")
        return
    
    running_pods = []
    for pod in pods.items:
        if pod.status.phase in ("Running", "Pending") and not pod.metadata.deletion_timestamp:
            running_pods.append(pod.metadata.name)

    if running_pods:
        if kube.cleanup_pods_on_startup:
            log.warning(f"k8s: you have {len(running_pods)} pod(s) running from another stimela session")
            log.warning(f"since kube.cleanup_pods_on_startup is set, these will be terminated")
            for podname in running_pods:
                log.info(f"deleting pod {podname}")
                try:
                    resp = kube_api.delete_namespaced_pod(name=podname, namespace=kube.namespace)
                except ApiException as exc:
                    body = json.loads(exc.body)
                    log_exception(BackendError(f"k8s API error while deleting pod {podname}", (exc, body)), severity="error")
                    continue
                log.debug(f"delete_namespaced_pod({podname}): {resp}")

        elif kube.report_pods_on_startup:
            log.warning(f"k8s: you have {len(running_pods)} pod(s) running from another stimela session")
            log.warning(f"set kube.report_pods_on_startup=false to disable this warning")


def check_pods_on_exit(kube: 'stimela.backends.kube.KubernetesBackendOptions'):
    from . import run_kube, session_id
    from stimela.stimelogging import logger
    log = logger()

    kube_api, _ = run_kube.get_kube_api() 
    try:
        pods = kube_api.list_namespaced_pod(namespace=kube.namespace, 
                                            label_selector=f"stimela_session_id={session_id}")
    except ApiException as exc:
        body = json.loads(exc.body)
        log_exception(BackendError(f"k8s API error while listing pods", (exc, body)), severity="error")
        return
    
    running_pods = []
    for pod in pods.items:
        if pod.status.phase in ("Running", "Pending") and not pod.metadata.deletion_timestamp:
            running_pods.append(pod.metadata.name)

    if running_pods and kube.cleanup_pods_on_exit:
        log.warning(f"k8s: you have {len(running_pods)} pod(s) still pending or running from this session")
        log.warning(f"since kube.cleanup_pods_on_exit is set, these will be terminated")
        for podname in running_pods:
            log.info(f"deleting pod {podname}")
            try:
                resp = kube_api.delete_namespaced_pod(name=podname, namespace=kube.namespace)
            except ApiException as exc:
                body = json.loads(exc.body)
                log_exception(BackendError(f"k8s API error while deleting pod {podname}", (exc, body)), severity="error")
                continue
            log.debug(f"delete_namespaced_pod({podname}): {resp}")
