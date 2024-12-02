import logging
from typing import Dict, Any
import re
from datetime import datetime
from rich.markup import escape
from requests import ConnectionError
from urllib3.exceptions import HTTPError

from omegaconf import OmegaConf

from stimela.exceptions import StimelaCabRuntimeError

from kubernetes.client.rest import ApiException
from . import get_kube_api, KubeBackendOptions

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

def resolve_unit(quantity:str, units: Dict = k8s_memory_units_in_bytes):
    match = re.fullmatch(r"^(\d+)(.*)$", quantity)
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
                log.info(f"setting {kind} memory request to {res['requests']['memory']}")
            if kps.memory.limit:
                res.setdefault('limits', {})['memory'] = kps.memory.limit or kps.memory.request
                log.info(f"setting {kind} memory limit to {res['limits']['memory']}")
        if kps.cpu is not None:
            res = pod_spec['containers'][0].setdefault('resources', {})
            if kps.cpu.request:
                res.setdefault('requests', {})['cpu'] = kps.cpu.request
                log.info(f"setting {kind} CPU request to {kps.cpu.request}")
            if kps.cpu.limit:
                res.setdefault('limits', {})['cpu'] = kps.cpu.limit
                log.info(f"setting {kind} CPU limit to {kps.cpu.limit}")

    return pod_spec

class StatusReporter(object):
    def __init__(self, log: logging.Logger,
                 podname: str,
                 kube: KubeBackendOptions,
                 event_handler: None,
                 update_interval: float = 1,
                 enable_metrics: bool = True):
        self.kube = kube
        self.namespace, self.kube_api, self.custom_api = get_kube_api()
        self.log = log
        self.podname = podname
        self.label_selector = f"stimela_job={podname}"
        self.event_handler = event_handler
        self.update_interval = update_interval
        self.enable_metrics = enable_metrics
        self.pod_statuses = {}
        self.reported_events = set()
        self.main_status = None
        self.pvcs = []
         # API errors added here when reported -- use this dict to avoid reissuing multiple errors
        self.api_errors_reported = {}
        self._request_timeout = (5, 5)
        self._connected = None
        self._last_connected = None
        self._last_disconnected = None

    def set_event_handler(self, event_handler):
        self.event_handler = event_handler

    def set_pod_name(self, podname):
        self.podname = podname
        self.label_selector = f"stimela_job={podname}"

    def set_pvcs(self, pvcs: Dict[str, KubeBackendOptions.Volume]):
        self.pvcs = [("PersistentVolumeClaim", pvc.name) for pvc in pvcs.values()]

    def set_main_status(self, mainstat):
        self.main_status = mainstat

    def report_api_error(self, name, exc):
        if name not in self.api_errors_reported:
            self.api_errors_reported[name] = exc
            self.log.warning(f"k8s API error for {name}: {exc}")

    @property
    def connected(self):
        return self._connected

    @connected.setter
    def connected(self, status: bool):
        if status:
            self._last_connected = datetime.now()
        else:
            self._last_disconnected = datetime.now()
        self._connected = status

    def log_events(self):
        # get list of associated pods
        try:
            pods = self.kube_api.list_namespaced_pod(namespace=self.namespace, label_selector=self.label_selector,
                                                     _request_timeout=self._request_timeout)
        except ApiException as exc:
            self.report_api_error("list_namespaced_pod", exc)
            return
        objects = [("Pod", pod.metadata.name) for pod in pods.items] + self.pvcs
        for kind, name in objects:
            # get new events
            try:
                events = self.kube_api.list_namespaced_event(namespace=self.namespace,
                                field_selector=f"involvedObject.kind={kind},involvedObject.name={name}",
                                _request_timeout=self._request_timeout)
            except ApiException as exc:
                self.report_api_error("list_namespaced_event", exc)
                return
            for event in events.items:
                if event.metadata.uid not in self.reported_events:
                    self.reported_events.add(event.metadata.uid)
                    if self.event_handler:
                        try:
                            self.event_handler(event)
                        except Exception as exc:
                            self.log.error(self.kube.debug.event_format.format(event=event))
                            raise
                    # no error from handler, report event if configured to
                    if self.kube.debug.log_events:
                        color = self.kube.debug.event_colors.get(event.type.lower()) \
                                or self.kube.debug.event_colors.get("default")
                        # escape console markup on string fields
                        for key, value in event.__dict__.items():
                            if type(value) is str:
                                setattr(event, key, escape(value))
                        self.log.info(self.kube.debug.event_format.format(event=event),
                                      extra=dict(color=color) if color else {})

    def update(self):
        from . import session_user
        metrics = []
        try:
            self.log_events()
            self.connected = True

            # update k8s stats and metrics
            pods = metrics = None
            # get pod statuses
            try:
                pods = self.kube_api.list_namespaced_pod(self.namespace,
                                                label_selector=f"stimela_user={session_user}",
                                                _request_timeout=self._request_timeout)
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
                    metrics = self.custom_api.list_namespaced_custom_object('metrics.k8s.io', 'v1beta1', self.namespace, 'pods',
                                                _request_timeout=self._request_timeout)
                except ApiException as exc:
                    self.report_api_error("metrics.k8s.io", exc)
        except (ConnectionError, HTTPError) as exc:
            self.connected = False
            # self.log.warning(f"disconnected: {exc}")
        # add connection status
        if not self.connected:
            interval = str(datetime.now() - self._last_connected)
            interval = interval.split(".", 1)[0]
            return [f"lost connection [red]{interval}[/red] ago"], None

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
        report_metrics = []
        if self.main_status:
            report_metrics.append(f"[blue]{self.main_status}[/blue]")
        elif self.podname in self.pod_statuses:
            report_metrics.append(f"[blue]{self.pod_statuses[self.podname]}[/blue]")
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
        nsucc = sum([stat.startswith("Succeeded") for stat in self.pod_statuses.values()])
        if nsucc:
            pods += f"[green]{nsucc}[/green]S"
        nfail = sum([stat.startswith("Failed") for stat in self.pod_statuses.values()])
        if nfail:
            pods += f"[red]{nfail}[/red]F"
        nuk = npods - nrun - npend - nterm - nsucc - nfail
        if nuk:
            pods += f"[red]{nuk}[/red]U"
        if npods:
            report_metrics.append(f"pods {pods}")
        # add metrics
        if metrics:
            cores = totals['cpu']
            mem_gb = round(totals['memory'] / 2**30)
            report_metrics += [
                f"cores [green]{totals['cpu']:.2f}[/green]",
                f"mem [green]{mem_gb}[/green]G"
            ]
            stats = dict(k8s_cores=cores, k8s_mem=mem_gb)
        else:
            stats = None

        if self._last_disconnected is not None:
            report_metrics.append("reconnected")

        return report_metrics, stats


