import logging
import time
from typing import Dict, Any
import re

from omegaconf import OmegaConf

from stimela.exceptions import StimelaCabParameterError, StimelaCabRuntimeError, BackendError
from stimela.stimelogging import update_process_status

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
    def __init__(self, kube_api, custom_api, namespace: str, log: logging.Logger, 
                 podname: str,
                 kube: 'KubernetesBackendOptions',
                 event_handler: lambda event:None,
                 update_interval: float = 1,
                 enable_metrics: bool = True):
        self.kube = kube
        self.kube_api = kube_api
        self.custom_api = custom_api 
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
                        self.log.info(self.kube.verbose_event_format.format(event=event))
                        

    def update(self):
        self.log_events()

        # update k8s stats and metrics
        pods = metrics = None
        # get pod statuses
        try:
            pods = self.kube_api.list_namespaced_pod(self.namespace)
        except ApiException as exc:
            self.report_api_error("list_namespaced_pod", exc)
        # process statuses if we got them
        if pods:
            for pod in pods.items:
                pname = pod.metadata.name
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
        nrun = sum([stat == "Running" for stat in self.pod_statuses.values()])
        status = (status or '') + f"|pods [green]{nrun}[/green]/[green]{len(self.pod_statuses)}[/green]" 
        # add metrics
        if metrics:
            cores = totals['cpu']
            mem_gb = round(totals['memory'] / 2**30)
            status += f"|cores [green]{totals['cpu']:.2f}[/green]|mem [green]{mem_gb}[/green]G"
            stats = dict(k8s_cores=cores, k8s_mem=mem_gb)
        else:
            stats = None
        
        return status, stats
