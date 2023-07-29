from dataclasses import dataclass
import logging, time, json, datetime, yaml, os.path, uuid, pathlib

from typing import Dict, List, Optional, Any

from omegaconf import OmegaConf, DictConfig, ListConfig
from scabha.basetypes import EmptyDictDefault, EmptyListDefault

import stimela
from stimela.utils.xrun_asyncio import dispatch_to_log
from stimela.exceptions import StimelaCabParameterError, StimelaCabRuntimeError, CabValidationError
from stimela.stimelogging import log_exception
#from stimela.backends import resolve_required_mounts
# these are used to drive the status bar
from stimela.stimelogging import declare_subcommand, declare_subtask, declare_subtask_attributes, update_process_status

# needs pip install kubernetes dask-kubernetes

import kubernetes
from kubernetes.client.api import core_v1_api
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream
from dask_kubernetes import make_pod_spec, KubeCluster

_kube_client = _kube_config = None

def get_kube_api():
    global _kube_client
    global _kube_config

    if _kube_config is None:
        _kube_config = True
        kubernetes.config.load_kube_config()

    return core_v1_api.CoreV1Api()


def run(cab: 'stimela.kitchen.cab.Cab', params: Dict[str, Any], fqname: str,
        backend: 'stimela.backend.StimelaBackendOptions',
        log: logging.Logger, subst: Optional[Dict[str, Any]] = None):
    """Runs cab contents

    Args:
        cab (Cab): cab object
        log (logger): logger to use
        subst (Optional[Dict[str, Any]]): Substitution dict for commands etc., if any.

    Returns:
        Any: return value (e.g. exit code) of content
    """

    from . import _InjectedFileFormatters
    from stimela.backends import resolve_registry_name

    if not cab.image:
        raise StimelaCabRuntimeError(f"kubernetes runner requires cab.image to be set")

    kube = backend.kube

    namespace = kube.namespace
    if not namespace:
        raise StimelaCabRuntimeError(f"runtime.kube.namespace must be set")

    args = cab.flavour.get_arguments(cab, params, subst, check_executable=False)
    log.debug(f"command line is {args}")

    cabstat = cab.reset_status()

    command_name = cab.flavour.command_name

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

    numba_cache_dir = os.path.expanduser("~/.cache/numba")
    pathlib.Path(numba_cache_dir).mkdir(parents=True, exist_ok=True)

    pod_created = None

    with declare_subtask(f"{os.path.basename(command_name)}:kube"):
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
            image_name = resolve_registry_name(backend, str(cab.image))
            log.info(f"using image {image_name}")

            pod_manifest = dict(
                apiVersion  =  'v1',
                kind        =  'Pod',
                metadata    = dict(name=podname),
            )
            # form up pod spec
            pod_spec = dict(
                containers = [dict(
                        image   = image_name,
                        imagePullPolicy = 'IfNotPresent',
                        name    = podname,
                        args    = ["/bin/sh", "-c", "while true;do date;sleep 5; done"],
                        env     = [],
                        securityContext = dict(
                                runAsNonRoot = True,
                                runAsUser = os.getuid() if kube.uid is None else kube.uid,
                                runAsGroup = os.getgid() if kube.gid is None else kube.gid
                        ),
                        volumeMounts = []
                )],
                volumes = []
            )
            # apply predefined types
            if kube.pod_type is not None:
                log.info(f"selecting predefined pod type '{kube.pod_type}'")
                predefined_pod_spec = kube.predefined_pod_types.get(kube.pod_type)
                if predefined_pod_spec is None:
                    raise StimelaCabRuntimeError(f"backend.kube.pod_type={kube.pod_type} not found in predefined_pod_types")
            else:
                predefined_pod_spec = {}
            # apply custom type and merge
            if predefined_pod_spec or kube.custom_pod_spec: 
                pod_spec = OmegaConf.to_container(OmegaConf.merge(pod_spec, predefined_pod_spec, kube.custom_pod_spec)) 
            
            pod_manifest['spec'] = pod_spec

            # add RAM resources
            if kube.memory is not None:
                res = pod_manifest['spec']['containers'][0].setdefault('resources', {})
                res.setdefault('requests', {})['memory'] = kube.memory
                res.setdefault('limits', {})['memory'] = kube.memory
                log.info(f"setting memory request/limit to {kube.memory}")

            # add runtime env settings
            for name, value in kube.env.items():
                value = os.path.expanduser(value)
                pod_manifest['spec']['containers'][0]['env'].append(dict(name=name, value=value))

            # add local mounts
            def add_local_mount(name, path, dest, readonly):
                name = name.replace("_", "-")  # sanizitze name
                pod_manifest['spec']['volumes'].append(dict(
                    name = name,
                    hostPath = dict(path=path, type='Directory' if os.path.isdir(path) else 'File')
                ))
                pod_manifest['spec']['containers'][0]['volumeMounts'].append(dict(name=name, mountPath=dest, readOnly=readonly))

            # this will accumulate mounted paths from runtime spec
            prior_mounts = {}

            # add local mounts from runtime spec
            for name, mount in kube.local_mounts.items():
                path = os.path.abspath(os.path.expanduser(mount.path))
                dest = os.path.abspath(os.path.expanduser(mount.dest)) if mount.dest else path
                if not os.path.exists(path) and mount.mkdir:
                    pathlib.Path(path).mkdir(parents=True)
                add_local_mount(name, path, dest, mount.readonly)
                if path == dest:
                    prior_mounts[path] = not mount.readonly

            # add local mounts to support parameters
            req_mounts = {} # resolve_required_mounts(params, cab.inputs, cab.outputs, prior_mounts=prior_mounts)
            for i, (path, readwrite) in enumerate(req_mounts.items()):
                log.info(f"adding local mount {path} (readwrite={readwrite})")
                add_local_mount(f"automount-{i}", path, path, not readwrite)

            # add persistent volumes
            pvcs = {}
            for i, (name, path) in enumerate(kube.volumes.items()):
                volume_name = pvcs.get(name)
                if volume_name is None:
                    volume_name = pvcs[name] = name
                    pod_manifest['spec']['volumes'].append(dict(
                        name = volume_name,
                        persistentVolumeClaim = dict(claimName=name)
                    ))
                pod_manifest['spec']['containers'][0]['volumeMounts'].append(dict(name=volume_name, mountPath=path))

            if kube.verbose_events > 0:
                reported_events = set()
                field_selector = f"involvedObject.name={podname}"
                def log_pod_events():
                    # get new events
                    events = kube_api.list_namespaced_event(namespace=namespace, field_selector=field_selector)
                    for event in events.items:
                        if event.metadata.uid not in reported_events:
                            log.info(kube.verbose_event_format.format(event=event))
                            reported_events.add(event.metadata.uid)
            else:
                log_pod_events = lambda:None

            # start pod and wait for it to come up
            with declare_subcommand("starting pod") as subcommand:
                log.info(f"starting pod {podname} to run {command_name}")
                resp = kube_api.create_namespaced_pod(body=pod_manifest, namespace=namespace)
                log.debug(f"create_namespaced_pod({podname}): {resp}")
                pod_created = resp

                while True:
                    update_status()
                    resp = kube_api.read_namespaced_pod_status(name=podname, namespace=namespace)
                    log.debug(f"read_namespaced_pod_status({podname}): {resp.status}")
                    phase = resp.status.phase
                    if phase == 'Running':
                        break
                    subcommand.update_status(f"phase: {phase}")
                    log_pod_events()
                    time.sleep(1)
                log.info(f"  pod started after {elapsed()}")
                # resp = kube_api.read_namespaced_pod(name=podname, namespace=namespace)
                # log.info(f"  read_namespaced_pod {resp}")

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
                    log_pod_events()

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

            # do we need to chdir
            if kube.dir:
                args = ["python", "-c", f"import os,sys; os.chdir('{kube.dir}'); os.execlp('{args[0]}', *sys.argv[1:])"] + list(args)

            log.info(f"running {command_name} in pod {podname}")
            with declare_subcommand(os.path.basename(command_name)):
                retcode = run_pod_command(args, command_name, wrangler=cabstat.apply_wranglers)
            log_pod_events()

            # check if output marked it as a fail
            if cabstat.success is False:
                log.error(f"declaring '{command_name}' as failed based on its output")

            # if retcode != 0 and not explicitly marked as success, mark as failed
            if retcode and cabstat.success is not True:
                cabstat.declare_failure(f"{command_name} returns error code {retcode} after {elapsed()}")
                if retcode == 137:
                    log.error(f"the pod was killed with an out-of-memory condition (backend.kube.memory setting is {kube.memory})")
            else:
                log.info(f"{command_name} returns exit code {retcode} after {elapsed()}")

            return cabstat

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
            import traceback
            traceback.print_exc()
            raise StimelaCabRuntimeError("kubernetes backend error", exc)

        # cleanup
        finally:
            if cluster and not kube.dask_cluster.persist:
                update_status()
                log.info(f"stopping dask cluster {cluster_name}")
                log.info(f"cluster logs: {cluster.get_logs()}")
                cluster.close()
            if podname and pod_created:
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
                    log_pod_events()
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
