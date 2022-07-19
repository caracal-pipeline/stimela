from dataclasses import dataclass
import logging, time, json, datetime, yaml, os.path, uuid

from typing import Dict, List, Optional, Any
from enum import Enum
from collections import OrderedDict

from omegaconf import OmegaConf, DictConfig, ListConfig
from scabha.basetypes import EmptyDictDefault, EmptyListDefault

import stimela
from stimela.kitchen.cab import Cab
from stimela.kitchen.step import Step
from stimela.utils.xrun_asyncio import xrun, dispatch_to_log
from stimela.exceptions import StimelaCabParameterError, StimelaCabRuntimeError, CabValidationError
from stimela.stimelogging import log_exception, declare_subcommand, declare_subtask, update_process_status

import kubernetes
from kubernetes.client.api import core_v1_api
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream
from dask_kubernetes import make_pod_spec, KubeCluster
from distributed import Client

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
class KubernetesRuntime(object):
    namespace: str
    dask_cluster: KubernetesDaskRuntime = KubernetesDaskRuntime()
    inject_files: Dict[str, KubernetesFileInjection] = EmptyDictDefault()
    pre_commands: List[str] = EmptyListDefault()

KubernetesRuntimeSchema = OmegaConf.structured(KubernetesRuntime)

_kube_client = _kube_config = None

def get_kube_api():
    global _kube_client
    global _kube_config
    
    if _kube_config is None:
        _kube_config = True 
        kubernetes.config.load_kube_config()

    return core_v1_api.CoreV1Api()



def run(cab: Cab, params: Dict[str, Any], runtime: Dict[str, Any], fqname: str,
        log: logging.Logger, subst: Optional[Dict[str, Any]] = None, batch=None):
    """Runs cab contents

    Args:
        cab (Cab): cab object
        log (logger): logger to use
        subst (Optional[Dict[str, Any]]): Substitution dict for commands etc., if any.

    Returns:
        Any: return value (e.g. exit code) of content
    """
    if not cab.image:
        raise StimelaCabRuntimeError(f"kubernetes runner requires cab.image to be set")

    if 'kube' in stimela.CONFIG.opts.runtime:
        kube = OmegaConf.merge(KubernetesRuntimeSchema, stimela.CONFIG.opts.runtime.kube)

    # get kube info from cab's runtime section
    if 'kube' in cab.runtime:
        kube = OmegaConf.merge(KubernetesRuntimeSchema, cab.runtime.kube)

    # augment with step-specific one
    if 'kube' in runtime:
        kube = OmegaConf.merge(KubernetesRuntimeSchema, runtime.kube)

    namespace = kube.namespace
    if not namespace:
        raise StimelaCabRuntimeError(f"runtime.kube.namespace must be set")

    # form up command
    args, venv = cab.build_command_line(params, subst, search=False)
    command_name = args[0]
    if venv:
        raise StimelaCabRuntimeError("kubernetes backend does not support cab.virtual_env settings")
    
    cab.reset_runtime_status()
    
    cluster = podname = cluster_name = None
    kube_api = get_kube_api()

    start_time = datetime.datetime.now()
    def elapsed(since=None):
        """Returns string representing elapsed time"""
        return str(datetime.datetime.now() - (since or start_time)).split('.', 1)[0]

    last_update = 0
    def update_status():
        nonlocal last_update
        if time.time() - last_update >= 1:
            update_process_status()
            last_update = time.time()


    with declare_subtask(f"kubernetes:{os.path.basename(command_name)}"):        
        try:
            if kube.dask_cluster.num_workers:
                cluster_name = kube.dask_cluster.name
                if kube.dask_cluster.persist:
                    try:
                        resp = kube_api.read_namespaced_service(name=cluster_name, namespace=namespace)
                        cluster = True
                        log.info(f"persistent dask cluster {cluster_name} appears to be up")
                    except ApiException as exc:
                        log.info(f"persistent dask cluster {cluster_name} is not up")

                if cluster is None:
                    with declare_subcommand("starting dask cluster"):
                        log.info(f"starting dask cluster {cluster_name} for {command_name}")
                        pod_spec = make_pod_spec(image=cab.image,
                                                cpu_limit=kube.dask_cluster.cpu_limit,
                                                memory_limit=kube.dask_cluster.memory_limit,
                                                threads_per_worker=kube.dask_cluster.num_workers)

                        cluster = KubeCluster(pod_spec, name=cluster_name, namespace=namespace, shutdown_on_close=not kube.dask_cluster.persist)
                        update_status()
                        cluster.scale(kube.dask_cluster.num_workers)
                        update_status()

            podname = fqname.replace(".", "--").replace("_", "--") + "--" + uuid.uuid4().hex

            pod_manifest = {
                'apiVersion': 'v1',
                'kind': 'Pod',
                'metadata': {
                    'name': podname
                },
                'spec': {
                    'containers': [{
                        'image': cab.image,
                        'name': podname,
                        "args": [
                            "/bin/sh",
                            "-c",
                            "while true;do date;sleep 5; done"
                        ]
                        #                    'command': "ls",
                        #'args': ['ls', '-lrt', "/"]
                        # 'command': args[0],
                        # 'args': args[1:]
                    }]
                }
            }
            # start pod and wait for it to come up
            with declare_subcommand("starting pod"):
                log.info(f"starting pod {podname} for {command_name}")
                resp = kube_api.create_namespaced_pod(body=pod_manifest, namespace=namespace)
                log.debug(f"create_namespaced_pod({podname}): {resp}")

                while True:
                    update_status()
                    resp = kube_api.read_namespaced_pod(name=podname, namespace=namespace)
                    log.debug(f"read_namespaced_pod({podname}): {resp}")
                    if resp.status.phase != 'Pending':
                        break
                    time.sleep(.5)
                log.info(f"  pod started after {elapsed()}")


            def run_pod_command(command, cmdname, input=None, wrangler=None):
                if type(command) is str:
                    command = ["/bin/sh", "-c", command]
                has_input = bool(input)

                resp = stream(kube_api.connect_get_namespaced_pod_exec, podname, namespace,
                            command=command,
                            stderr=True, stdin=has_input,
                            stdout=True, tty=False,
                            _preload_content=False)

                while resp.is_open():
                    update_status()
                    resp.update(timeout=1)
                    if resp.peek_stdout():
                        for line in resp.read_stdout().rstrip().split("\n"):
                            dispatch_to_log(log, line, cmdname, "stdout", 
                                            output_wrangler=wrangler)
                    if resp.peek_stderr():
                        for line in resp.read_stderr().rstrip().split("\n"):
                            dispatch_to_log(log, line, cmdname, "stderr", 
                                            output_wrangler=wrangler)
                    if has_input:
                        if input:
                            resp.write_stdin(input)
                            input = None
                        else:
                            break

                retcode = resp.returncode
                resp.close()
                return retcode

            # inject files into pod
            if 'inject_files' in kube:
                with declare_subcommand("configuring pod (inject)"):
                    for filename, injection in kube.inject_files.items_ex():
                        content = injection.content
                        formatter = _InjectedFileFormatters.get(injection.format.name)
                        if formatter is None:
                            raise StimelaCabParameterError(f"unsupported format {injection.format.name} for {filename}")
                        # convert content to something serializable
                        if isinstance(content, (DictConfig, ListConfig)):
                            content = OmegaConf.to_container(content)
                        content = formatter(content)
                        log.info(f"injecting {filename} into pod {podname}")
                        retcode = run_pod_command(f"mkdir -p {os.path.dirname(filename)}; cat >{filename}", "inject", input=content)
                        if retcode:
                            log.warning(f"injection returns exit code {retcode} after {elapsed()}")
                        else:
                            log.info(f"injection successful after {elapsed()}") 


            if 'pre_commands' in kube:
                with declare_subcommand("configuring pod (pre-commands)"):
                    for pre_command in kube.pre_commands:
                        log.info(f"running pre-command '{pre_command}' in pod {podname}")
                        # calling exec and waiting for response
                        retcode = run_pod_command(pre_command, pre_command.split()[0])
                        if retcode:
                            log.warning(f"pre-command returns exit code {retcode} after {elapsed()}")
                        else:
                            log.info(f"pre-command successful after {elapsed()}") 

            log.info(f"running {command_name} in pod {podname}")
            with declare_subcommand(os.path.basename(command_name)):
                retcode = run_pod_command(args, command_name, wrangler=cab.apply_output_wranglers)

            if retcode:
                raise StimelaCabRuntimeError(f"{command_name} returns error code {retcode} after {elapsed()}")
            else:
                log.info(f"{command_name} returns exit code {retcode} after {elapsed()}")

            # check if command was marked as failed by the output wrangler
            if cab.runtime_status is False:
                raise StimelaCabRuntimeError(f"{command_name} was marked as failed based on its output")

            return retcode

        # handle various failure modes by logging errors appropriately
        except KeyboardInterrupt:
            log.error(f"kubernetes invocation of {command_name} interrupted with Ctrl+C after {elapsed()}")
            raise StimelaCabRuntimeError(f"{command_name} interrupted with Ctrl+C after {elapsed()}")
        except ApiException as exc:
            if exc.body:
                exc = (exc, json.loads(exc.body))
            log.error(f"kubernetes invocation of {command_name} failed with an ApiException after {elapsed()}")
            raise StimelaCabRuntimeError("kubernetes API error", exc)
        # this drops out as a normal error response
        except StimelaCabRuntimeError as exc:
            log.error(f"kubernetes invocation of {command_name} failed after {elapsed()}")
            raise
        except Exception as exc:
            log.error(f"kubernetes invocation of {command_name} failed after {elapsed()}")
            raise StimelaCabRuntimeError("kubernetes backend error", exc)

        # cleanup
        finally: 
            if cluster and not kube.dask_cluster.persist:
                update_status()
                log.info(f"stopping dask cluster {cluster_name}")
                log.info(f"cluster logs: {cluster.get_logs()}")
                cluster.close() 
            if podname:
                try:
                    update_status()
                    log.info(f"deleting pod {podname}")
                    resp = kube_api.delete_namespaced_pod(name=podname, namespace=namespace)
                    log.debug(f"delete_namespaced_pod({podname}): {resp}")
                    # while True:
                    #     resp = kube_api.read_namespaced_pod(name=podname, namespace=namespace)
                    #     log.debug(f"read_namespaced_pod({podname}): {resp}")
                    #     log.info(f"  pod phase is {resp.status.phase} after {elapsed()}")
                    #     time.sleep(.5)
                except ApiException as exc:
                    body = json.loads(exc.body)
                    log_exception(StimelaCabRuntimeError(f"kubernetes API error while deleting pod {podname}", (exc, body)), severity="warning")
                except Exception as exc:
                    log_exception(StimelaCabRuntimeError(f"kubernetes API error while deleting pod {podname}: {exc}"), severity="warning")


# kubectl -n rarg get pods -A
# kubectl -n rarg delete service recipetestqcdaskcluster
# kubectl -n rarg delete poddisruptionbudget recipetestqcdaskcluster
# kubectl -n rarg port-forward service/qc-test-cluster 18787:http-dashboard
# kubectl -n rarg logs pod_id 
"""
https://kubernetes.dask.org/en/latest/kubecluster.html#dask_kubernetes.KubeCluster

We recommend adding the --death-timeout, '60' arguments and the restartPolicy: Never attribute 
to your worker specification. This ensures that these pods will clean themselves up if your Python process disappears unexpectedly.

OMS: this should be set in the structure returned by make_pod_spec(). Seems to be set by default.

https://kubernetes.dask.org/en/latest/testing.ht

By default we set the --keep-cluster flag in setup.cfg which means the Kubernetes container will persist between pytest runs to 
avoid creation/teardown time. Therefore you may want to manually remove the container when you are done working on dask-kubernetes:
"""