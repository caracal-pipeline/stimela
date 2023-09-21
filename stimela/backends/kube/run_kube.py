import logging, time, json, datetime, os.path, pathlib, secrets, traceback
from typing import Dict, Optional, Any, List
from dataclasses import fields
import subprocess
import rich
import traceback

from omegaconf import OmegaConf, DictConfig, ListConfig

from stimela.utils.xrun_asyncio import dispatch_to_log
from stimela.exceptions import StimelaCabParameterError, StimelaCabRuntimeError, BackendError
from stimela.stimelogging import log_exception
from stimela.stimelogging import declare_subcommand, declare_subtask, update_process_status
from stimela.backends import StimelaBackendOptions
from stimela.kitchen.cab import Cab

# needs pip install kubernetes dask-kubernetes

from . import get_kube_api, InjectedFileFormatters, session_id, session_user, resource_labels, session_user_info, KubeBackendOptions
from .kube_utils import StatusReporter

from kubernetes.client.rest import ApiException
from kubernetes.stream import stream

from .kube_utils import apply_pod_spec
from . import infrastructure


def run(cab: Cab, params: Dict[str, Any], fqname: str,
        backend: StimelaBackendOptions,
        log: logging.Logger, subst: Optional[Dict[str, Any]] = None):
    """Runs cab contents

    Args:
        cab (Cab): cab object
        log (logger): logger to use
        subst (Optional[Dict[str, Any]]): Substitution dict for commands etc., if any.

    Returns:
        Any: return value (e.g. exit code) of content
    """

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

    pod_created = dask_job_created = volumes_provisioned = port_forward_proc = None

    aux_pod_threads = {}

    def k8s_event_handler(event):
        objkind = event.involved_object.kind
        objname = event.involved_object.name
        if event.message.startswith("Error: ErrImagePull"):
            raise BackendError(f"{objkind} '{objname}': failed to pull the image '{image_name}'. Preceding log messages may contain extra information.",
                               event.to_dict())
        if event.reason == "Failed":
            raise BackendError(f"{objkind} '{objname}' reported a 'Failed' event. Preceding log messages may contain extra information.",
                               event.to_dict())
        if event.reason == "ProvisioningFailed":
            raise BackendError(f"{objkind} '{objname}' reported a 'ProvisioningFailed' event. Preceding log messages may contain extra information.",
                              event.to_dict())
        # if event.reason == "FailedScheduling":
        #     raise StimelaCabRuntimeError(f"k8s has reported a 'FailedScheduling' event. Preceding log messages may contain extra information.")

    statrep = StatusReporter(namespace, podname=podname, log=log, kube=kube,
                             event_handler=k8s_event_handler)

    with declare_subtask(f"{os.path.basename(command_name)}:kube", status_reporter=statrep.update):
        try:
            log.info(f"using image {image_name}")

            # create pod labels
            pod_labels = dict(stimela_job=podname,
                              stimela_fqname=fqname,
                              stimela_cab=cab.name,
                              **resource_labels)

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

            # setup user information -- current user info by default, overridden by backend options
            uinfo = KubeBackendOptions.UserInfo()
            for fld in fields(uinfo):
                value = getattr(kube.user, fld.name)
                setattr(uinfo, fld.name, value if value is not None else getattr(session_user_info, fld.name))

            command = "while true; do date; sleep 5; done"
            if uinfo.inject_nss:
                command = \
                    "cp /etc/passwd $HOME/.passwd; " + \
                    "cp /etc/group $HOME/.group; " + \
                    "echo $USER:x:$USER_UID:$USER_GID:$USER_GECOS:$HOME:/bin/sh >> $HOME/.passwd; " + \
                    "echo $GROUP:x:$USER_GID: >> $HOME/.group; " + \
                    command

            # form up pod spec
            pod_spec = dict(
                containers = [dict(
                        image   = image_name,
                        imagePullPolicy = 'Always' if kube.always_pull_images else 'IfNotPresent',
                        name    = podname,
                        command = ["/bin/sh"],
                        args    = ["-c", command],
                        env     = [
                            dict(name="USER", value=uinfo.name),
                            dict(name="GROUP", value=uinfo.group),
                            dict(name="HOME", value=uinfo.home),
                            dict(name="USER_UID", value=str(uinfo.uid)),
                            dict(name="USER_GID", value=str(uinfo.gid)),
                            dict(name="USER_GECOS", value=str(uinfo.gecos))
                        ],
                        securityContext = dict(
                                runAsNonRoot = uinfo.uid!=0,
                                runAsUser = uinfo.uid,
                                runAsGroup = uinfo.gid,
                        ),
                        volumeMounts = [dict(
                            name = "home-directory",
                            mountPath = uinfo.home
                        )]
                )],
                volumes = [dict(
                    name = "home-directory",
                    emptyDir = dict(medium="Memory") if uinfo.home_ramdisk else {} 
                )],
                serviceAccountName = kube.service_account,
                automountServiceAccountToken = True
            )

            session_init_container = None
            step_init_container = None            

            def add_volume_init(volume_name: str, mount:str, commands: List[str], root: bool=False):
                nonlocal session_init_container, step_init_container
                # create init container if not already created
                if root:
                    cont = session_init_container
                    if cont is None:
                        cont = session_init_container = dict(
                            name="volume-session-init",
                            image="busybox",
                            command=["/bin/sh", "-c", ""],
                            volumeMounts=[])
                        pod_spec.setdefault('initContainers', []).append(cont)
                else:
                    cont = step_init_container
                    if cont is None:
                        cont = step_init_container = dict(
                            name="volume-step-init",
                            image="busybox",
                            command=["/bin/sh", "-c", ""],
                            securityContext=dict(
                                    runAsNonRoot = uinfo.uid!=0,
                                    runAsUser = uinfo.uid,
                                    runAsGroup = uinfo.gid),
                            volumeMounts=[])
                        pod_spec.setdefault('initContainers', []).append(cont)
                # add to its commands
                log.info(f"adding init commands for PVC '{name}': {'; '.join(commands)}")
                cont['command'][-1] += f"cd {mount}; {'; '.join(commands)}; "
                cont['volumeMounts'].append(dict(
                    name=volume_name, mountPath=mount,
                ))

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
                name = name.replace("_", "-")  # sanitize name
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
            # check that mount paths are set before we try to resolve volumes
            for name, pvc in kube.volumes.items():
                if not pvc.mount:
                    raise BackendError(f"volume {name} does not specify a mount path")
            # get the PVCs etc for the volumes
            with declare_subcommand("provisioning storage"):
                volumes_provisioned = infrastructure.resolve_volumes(kube, log=log, step_token=token_hex)
                # create volume specs
                for name, pvc in kube.volumes.items():
                    pod_manifest['spec']['volumes'].append(dict(
                        name = name,
                        persistentVolumeClaim = dict(claimName=pvc.name)
                    ))
                    pod_manifest['spec']['containers'][0]['volumeMounts'].append(dict(name=name, mountPath=pvc.mount))
                    # add init commands, if needed
                    pvc0 = infrastructure.active_pvcs[name]
                    # add session init -- this only happens once, if volume is created
                    session_init = infrastructure.session_init_commands.pop(name, None)
                    if session_init is not None:
                        session_init.insert(0, f"chown {kube.user.uid}.{kube.user.gid} .")
                        add_volume_init(name, pvc.mount, session_init, root=True)
                    # add step init
                    if pvc.step_init_commands:
                        if pvc0.owner != session_user:
                            log.warning(f"skipping step initialization, since volume is owned by {pvc0.owner} not {session_user}")
                        else:
                            add_volume_init(name, pvc.mount, pvc.step_init_commands, root=False)

                # add to status reporter
                statrep.set_pvcs(kube.volumes)
                # add init if needed
                

            # start pod and wait for it to come up
            provisioning_deadline = time.time() + (kube.provisioning_timeout or 1e+10)
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
                        if time.time() >= provisioning_deadline:
                            log.error("timed out waiting for pod to start. The log above may contain more information.")
                            raise BackendError(f"pod failed to start after {kube.provisioning_timeout}s")
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
                        if time.time() >= provisioning_deadline:
                            log.error("timed out waiting for dask job to start. The log above may contain more information.")
                            raise BackendError(f"job failed to start after {kube.provisioning_timeout}s")
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
            if kube.inject_files:
                with declare_subcommand("configuring pod (inject)"):
                    for filename, injection in kube.inject_files.items():
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


            if kube.pre_commands:
                with declare_subcommand("configuring pod (pre-commands)"):
                    for pre_command in kube.pre_commands:
                        log.info(f"running pre-command '{pre_command}' in pod {podname}")
                        # calling exec and waiting for response
                        retcode = run_pod_command(pre_command, pre_command.split()[0])
                        if retcode:
                            log.warning(f"pre-command returns exit code {retcode} after {elapsed()}")
                        else:
                            log.info(f"pre-command successful after {elapsed()}")

            if kube.debug.pause_on_start:
                log.warning("kube.debug.pause_on_start is enabled")
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
                    args = ["python3", "-c", f"import os,sys; os.chdir('{kube.dir}'); os.execlp('{args[0]}', *sys.argv[1:])"] + list(args)
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
            raise StimelaCabRuntimeError(f"{command_name} interrupted with Ctrl+C after {elapsed()}") from None
        except ApiException as exc:
            if exc.body:
                exc = (exc, json.loads(exc.body))
            import traceback
            traceback.print_exc()
            log.error(f"k8s API error after {elapsed()}: {exc}")
            raise BackendError("k8s API error", exc) from None
        # this drops out as a normal error response
        except StimelaCabRuntimeError as exc:
            log.error(f"cab runtime error after {elapsed()}: {exc}")
            raise
        except BackendError as exc:
            log.error(f"kube backend error after {elapsed()}: {exc}")
            raise
        except Exception as exc:
            log.error(f"k8s invocation of {command_name} failed after {elapsed()}")
            import traceback
            traceback.print_exc()
            raise StimelaCabRuntimeError("kube backend error", exc) from None

        # cleanup
        finally:
            if kube.debug.pause_on_cleanup:
                log.warning("kube.debug.pause_on_cleanup is enabled -- pausing -- press Ctrl+C to proceed")
                try:
                    while True:
                        time.sleep(1)
                except KeyboardInterrupt:
                    pass
                log.info("proceeding with cleanup")
            statrep.set_event_handler(None)
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

                # cleean up PVCs
                if volumes_provisioned:
                    infrastructure.delete_pvcs(kube, volumes_provisioned, log=log, step=True)

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
                        traceback.print_exc()
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
