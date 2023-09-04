import logging, time, json, datetime, os.path, pathlib, secrets
from typing import Dict, Optional, Any
import subprocess

from omegaconf import OmegaConf, DictConfig, ListConfig

import stimela
from stimela.utils.xrun_asyncio import dispatch_to_log
from stimela.exceptions import StimelaCabParameterError, StimelaCabRuntimeError, BackendError
from stimela.stimelogging import log_exception
#from stimela.backends import resolve_required_mounts
# these are used to drive the status bar
from stimela.stimelogging import declare_subcommand, declare_subtask, update_process_status

# needs pip install kubernetes dask-kubernetes

try:
    import kubernetes
    from kubernetes.client import CustomObjectsApi
    from kubernetes.client.api import core_v1_api
    from kubernetes.client.rest import ApiException
    from kubernetes.stream import stream
    _enabled = True
except ImportError:
    _enabled = False
    pass  # pesumably handled by disabling the backend in __init__

from .kube_utils import apply_pod_spec

import rich

_kube_client = _kube_config = None

def get_kube_api():
    global _kube_client
    global _kube_config

    if _kube_config is None:
        _kube_config = True
        kubernetes.config.load_kube_config()

    return core_v1_api.CoreV1Api(), CustomObjectsApi()


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
    from . import InjectedFileFormatters, session_id, session_user
    from .kube_utils import StatusReporter

    if not cab.image:
        raise StimelaCabRuntimeError(f"kube runner requires cab.image to be set")

    kube = backend.kube

    namespace = kube.namespace
    if not namespace:
        raise StimelaCabRuntimeError(f"runtime.kube.namespace must be set")

    args = cab.flavour.get_arguments(cab, params, subst, check_executable=False)
    log.debug(f"command line is {args}")

    cabstat = cab.reset_status()

    command_name = cab.flavour.command_name

    # generate podname
    tmp_name = session_user + "--" + fqname.replace(".", "--").replace("_", "--")
    token_hex = secrets.token_hex(4)
    podname = tmp_name[0:50] + "--" + token_hex

    kube_api, custom_obj_api = get_kube_api()

    image_name = cab.flavour.get_image_name(cab, backend)
    if not image_name:
        raise BackendError(f"cab '{cab.name}' does not define an image")

    start_time = datetime.datetime.now()
    def elapsed(since=None):
        """Returns string representing elapsed time"""
        return str(datetime.datetime.now() - (since or start_time)).split('.', 1)[0]

    numba_cache_dir = os.path.expanduser("~/.cache/numba")
    pathlib.Path(numba_cache_dir).mkdir(parents=True, exist_ok=True)

    pod_created = dask_job_created = port_forward_proc = None

    def k8s_event_handler(event):
        if event.message.startswith("Error: ErrImagePull"):
            raise StimelaCabRuntimeError(f"k8s failed to pull the image {image_name}'. Preceding log messages may contain extra information.")
        if event.reason == "Failed":
            raise StimelaCabRuntimeError(f"k8s has reported a 'Failed' event. Preceding log messages may contain extra information.")

    statrep = StatusReporter(namespace, podname=podname, log=log, kube=kube,
                             event_handler=k8s_event_handler)

    with declare_subtask(f"{os.path.basename(command_name)}:kube", status_reporter=statrep.update):
        try:
            log.info(f"using image {image_name}")

            pod_labels = dict(stimela_job=podname,
                              stimela_user=session_user,
                              stimela_session_id=session_id,
                              stimela_fqname=fqname,
                              stimela_cab=cab.name)

            # depending on whether or not a dask cluster is configured, we do either a DaskJob or a regular pod
            if kube.dask_cluster and kube.dask_cluster.num_workers:
                log.info(f"defining dask job with a cluster of {kube.dask_cluster.num_workers} workers")

                from . import daskjob
                dask_job_name = f"dj-{token_hex}"
                dask_job_spec = daskjob.render(OmegaConf.create(dict(
                    job_name=dask_job_name,
                    labels=pod_labels,
                    namespace=namespace,
                    image=image_name,
                    imagePullPolicy='Always' if kube.always_pull_images else 'IfNotPresent',
                    memory_limit=kube.dask_cluster.worker_pod.memory and kube.dask_cluster.worker_pod.memory.limit,
                    nworkers=kube.dask_cluster.num_workers,
                    threads_per_worker=kube.dask_cluster.threads_per_worker,
                    cmdline=["/bin/sh", "-c", "while true;do date;sleep 5; done"],
                    service_account=kube.service_account,
                    mount_file=None,
                    volume=[f"{name}:{path}" for name, path in kube.volumes.items()]
                )))

                # apply pod type specifications
                if kube.dask_cluster.worker_pod:
                    dask_job_spec[0]["spec"]["cluster"]["spec"]["worker"]["spec"] = \
                        apply_pod_spec(kube.dask_cluster.worker_pod, dask_job_spec[0]["spec"]["cluster"]["spec"]["worker"]["spec"],
                                                           kube.predefined_pod_specs, log, kind='worker')
                if kube.dask_cluster.scheduler_pod:
                    dask_job_spec[0]["spec"]["cluster"]["spec"]["scheduler"]["spec"] = \
                        apply_pod_spec(kube.dask_cluster.scheduler_pod, dask_job_spec[0]["spec"]["cluster"]["spec"]["scheduler"]["spec"],
                                                           kube.predefined_pod_specs, log, kind='scheduler')

            else:
                dask_job_spec = dask_job_name = None

            # form up normal pod spec -- either to be run directly, or injected into the dask job
            pod_manifest = dict(
                apiVersion  =  'v1',
                kind        =  'Pod',
                metadata    = dict(name=podname, labels=pod_labels),
            )

            # form up pod spec
            uid = os.getuid() if kube.uid is None else kube.uid
            gid = os.getgod() if kube.gid is None else kube.gid
            pod_spec = dict(
                containers = [dict(
                        image   = image_name,
                        imagePullPolicy = 'Always' if kube.always_pull_images else 'IfNotPresent',
                        name    = podname,
                        args    = ["/bin/sh", "-c", "while true;do date;sleep 5; done"],
                        env     = [],
                        securityContext = dict(
                                runAsNonRoot = uid!=0,
                                runAsUser = uid,
                                runAsGroup = gid,
                        ),
                        volumeMounts = []
                )],
                volumes = [],
                serviceAccountName = kube.service_account,
                automountServiceAccountToken = True
            )

            # apply pod specification
            pod_spec = apply_pod_spec(kube.job_pod, pod_spec, kube.predefined_pod_specs, log, kind='job')

            pod_manifest['spec'] = pod_spec

            # add runtime env settings
            for name, value in kube.env.items():
                value = os.path.expanduser(value)
                pod_manifest['spec']['containers'][0]['env'].append(dict(name=name, value=value))
            if dask_job_spec:
                pod_manifest['spec']['containers'][0]['env'].append(dict(name="DASK_SCHEDULER_ADDRESS",
                                                                        value=f"tcp://{dask_job_name}-scheduler.{namespace}.svc.cluster.local:8786"))

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

            # start pod and wait for it to come up
            aux_pod_threads = {}
            if dask_job_spec is None:
                with declare_subcommand("starting pod") as subcommand:
                    log.info(f"starting pod {podname} to run {command_name}")
                    resp = kube_api.create_namespaced_pod(body=pod_manifest, namespace=namespace)
                    log.debug(f"create_namespaced_pod({podname}): {resp}")
                    pod_created = resp

                    while True:
                        update_process_status()
                        resp = kube_api.read_namespaced_pod_status(name=podname, namespace=namespace)
                        log.debug(f"read_namespaced_pod_status({podname}): {resp.status}")
                        phase = resp.status.phase
                        if phase == 'Running':
                            break
                        time.sleep(1)
            # else dask job
            else:
                with declare_subcommand("starting dask job") as subcommand:
                    log.info(f"starting dask job {dask_job_name} to run {command_name}")
                    dask_job_spec[0]["spec"]["job"]["spec"] = pod_spec
                    dask_job_spec[0]["spec"]["job"]["metadata"] = dict(name=podname)
                    group = 'kubernetes.dask.org'  # the CRD's group name
                    version = 'v1'  # the CRD's version
                    plural = 'daskjobs'  # the plural name of the CRD
                    resp = custom_obj_api.create_namespaced_custom_object(group, version, namespace, plural, dask_job_spec[0])
                    log.debug(f"create_namespaced_custom_object({dask_job_name}): {resp}")
                    dask_job_created = resp
                    job_status = None

                    # wait for dask job to start up
                    while True:
                        resp = custom_obj_api.get_namespaced_custom_object_status(group, version, namespace, plural,
                                                                                name=dask_job_name)
                        job_status = 'status' in resp and resp['status']['jobStatus']
                        statrep.set_main_status(job_status)
                        update_process_status()
                        # get podname from job once it's running
                        if job_status == 'Running':
                            podname = resp['status']['jobRunnerPodName']
                            statrep.set_pod_name(podname)
                            log.info(f"job running as pod {podname}")
                            break
                        time.sleep(1)

                    def print_logs(name):
                        style = kube.dask_cluster.capture_logs_style
                        try:
                            dispatch_to_log(log, f"started logging thread", command_name, "stdout", prefix=f"{name}#", style=style, output_wrangler=None)
                            # rich.print(f"[yellow]started log thread for {name}[/yellow]")
                            # Open a stream to the logs
                            stream = None
                            while stream is None:
                                try:
                                    stream = kube_api.read_namespaced_pod_log(name=name, namespace=namespace, _preload_content=False)
                                except ApiException as exc:
                                    # rich.print(f"[yellow]error starting stream for {name}, sleeping[/yellow]")
                                    time.sleep(2)
                            for line in stream:
                                dispatch_to_log(log, line.decode().rstrip(), command_name, "stdout", prefix=f"{name}#", style=style, output_wrangler=None)
                            stream.close()
                        finally:
                            dispatch_to_log(log, f"exiting logging thread", command_name, "stdout", prefix=f"{name}#", style=style, output_wrangler=None)
                        # rich.print(f"[yellow]stopped log thread for {name}[/yellow]")

                    # get other pods associated with DaskJob
                    if kube.dask_cluster.capture_logs:
                        pods = kube_api.list_namespaced_pod(namespace=namespace, label_selector=statrep.label_selector)
                        for pod in pods.items:
                            # get new events
                            name = pod.metadata.name
                            if name != podname:
                                import threading
                                aux_pod_threads[name] = threading.Thread(target=print_logs, kwargs=dict(name=name))
                                aux_pod_threads[name].start()

                    # start port forwarding
                    if kube.dask_cluster.forward_dashboard_port:
                        log.info(f"starting port-forward process for http-dashboard to local port {kube.dask_cluster.forward_dashboard_port}")
                        port_forward_proc = subprocess.Popen([kube.kubectl_path,
                            "port-forward", f"service/{dask_job_name}-scheduler",
                            f"{kube.dask_cluster.forward_dashboard_port}:http-dashboard"])

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
                    update_process_status()
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
                        formatter = InjectedFileFormatters.get(injection.format.name)
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


            if kube.debug_mode:
                log.warning("kube.debug_mode enabled")
                log.warning(f"to access the pod, run")
                log.warning(f"  $ kubectl exec -it {podname} -- /bin/bash")
                log.warning(f"your command line inside the pod is:")
                if kube.dir:
                    log.warning(f"  $ cd {kube.dir}")
                log.warning(f"  $ {' '.join(args)}")
                args = ["bash", "-c", "while sleep 600; do echo debug mode still active; done"]
                log.warning("press Ctrl+C when done debugging")
            else:
                # do we need to chdir
                if kube.dir:
                    args = ["python", "-c", f"import os,sys; os.chdir('{kube.dir}'); os.execlp('{args[0]}', *sys.argv[1:])"] + list(args)
                log.info(f"running {command_name} in pod {podname}")

            with declare_subcommand(os.path.basename(command_name)):
                retcode = run_pod_command(args, command_name, wrangler=cabstat.apply_wranglers)
            update_process_status()

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
            log.error(f"k8s invocation of {command_name} interrupted with Ctrl+C after {elapsed()}")
            raise StimelaCabRuntimeError(f"{command_name} interrupted with Ctrl+C after {elapsed()}")
        except ApiException as exc:
            if exc.body:
                exc = (exc, json.loads(exc.body))
            import traceback
            traceback.print_exc()
            log.error(f"k8s invocation of {command_name} failed with an ApiException after {elapsed()}")
            raise StimelaCabRuntimeError("k8s API error", exc)
        # this drops out as a normal error response
        except StimelaCabRuntimeError as exc:
            log.error(f"k8s invocation of {command_name} failed after {elapsed()}")
            raise
        except Exception as exc:
            log.error(f"k8s invocation of {command_name} failed after {elapsed()}")
            import traceback
            traceback.print_exc()
            raise StimelaCabRuntimeError("kube backend error", exc)

        # cleanup
        finally:
            try:
                # clean up port forwarder
                if port_forward_proc:
                    retcode = port_forward_proc.poll()
                    if retcode is not None:
                        log.warning(f"kubectl port-forward process has died with code {retcode}")
                        port_forward_proc.wait()
                    else:
                        log.info("terminating kubectl port-forward process")
                        port_forward_proc.terminate()
                        try:
                            retcode = port_forward_proc.wait(1)
                        except subprocess.TimeoutExpired:
                            log.warning("kubectl port-forward process hasn't terminated -- killing it")
                            port_forward_proc.kill()
                            try:
                                retcode = port_forward_proc.wait(1)
                            except subprocess.TimeoutExpired:
                                log.warning("kubectl port-forward process refuses to die")
                        if retcode is not None:
                            log.info(f"kubectl port-forward process has exited with code {retcode}")

                if podname and pod_created: # or dask_job_created:
                    try:
                        update_process_status()
                        log.info(f"deleting pod {podname}")
                        resp = kube_api.delete_namespaced_pod(name=podname, namespace=namespace)
                        log.debug(f"delete_namespaced_pod({podname}): {resp}")
                        # while True:
                        #     resp = kube_api.read_namespaced_pod(name=podname, namespace=namespace)
                        #     log.debug(f"read_namespaced_pod({podname}): {resp}")
                        #     log.info(f"  pod phase is {resp.status.phase} after {elapsed()}")
                        #     time.sleep(.5)
                        update_process_status()
                    except ApiException as exc:
                        body = json.loads(exc.body)
                        log_exception(StimelaCabRuntimeError(f"k8s API error while deleting pod {podname}", (exc, body)), severity="warning")
                    except Exception as exc:
                        log_exception(StimelaCabRuntimeError(f"error while deleting pod {podname}: {exc}"), severity="warning")
                if dask_job_created:
                    try:
                        update_process_status()
                        log.info(f"deleting dask job {dask_job_name}")
                        custom_obj_api.delete_namespaced_custom_object(group, version, namespace, plural, dask_job_name)
                    except ApiException as exc:
                        body = json.loads(exc.body)
                        log_exception(StimelaCabRuntimeError(f"k8s API error while deleting dask job {dask_job_name}", (exc, body)), severity="warning")
                    except Exception as exc:
                        log_exception(StimelaCabRuntimeError(f"error while deleting dask job {dask_job_name}: {exc}"), severity="warning")
                # wait for aux pod threads
                cleanup_time = datetime.datetime.now()
                def cleanup_elapsed():
                    return (datetime.datetime.now() - cleanup_time).total_seconds()
                while aux_pod_threads and cleanup_elapsed() < 5:
                    update_process_status()
                    for name, thread in list(aux_pod_threads.items()):
                        if thread.is_alive():
                            log.info(f"rejoining log thread for {name}")
                            thread.join(0.01)
                        if thread.is_alive():
                            log.info(f"logging thread alive, trying to delete auxuliary pod {name}")
                            try:
                                kube_api.delete_namespaced_pod(name=name, namespace=namespace)
                            except ApiException as exc:
                                log.warning(f"deleting pod {name} failed: {exc}")
                                pass
                        if not thread.is_alive():
                            del aux_pod_threads[name]
                    if aux_pod_threads:
                        log.warning(f"{len(aux_pod_threads)} logging threads for sub-pods still alive, sleeping for a bit")
                        time.sleep(2)
                if aux_pod_threads:
                    log.info(f"{len(aux_pod_threads)} logging threads for sub-pods still alive after {cleanup_elapsed():.1}s, giving up on the cleanup")
            except KeyboardInterrupt:
                log.error(f"kube cleanup interrupted with Ctrl+C after {elapsed()}")
                raise StimelaCabRuntimeError(f"{command_name} cleanup interrupted with Ctrl+C after {elapsed()}")
            except Exception as exc:
                log.error(f"kube cleanup failed after {elapsed()}")
                import traceback
                traceback.print_exc()
                raise StimelaCabRuntimeError("kube backend cleanup error", exc)




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
