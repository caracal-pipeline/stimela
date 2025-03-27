import logging, time, json, datetime, os.path, pathlib, secrets, shlex
from typing import Dict, Optional, Any, List, Callable
from dataclasses import fields
from requests import ConnectionError
from urllib3.exceptions import HTTPError
import subprocess
import yaml
import traceback

from omegaconf import OmegaConf, DictConfig, ListConfig

from stimela.utils.xrun_asyncio import dispatch_to_log
from stimela.exceptions import StimelaCabParameterError, StimelaCabRuntimeError, BackendError
from stimela.stimelogging import log_exception, log_rich_payload
from stimela.task_stats import declare_subcommand, declare_subtask, update_process_status
from stimela.backends import StimelaBackendOptions
from stimela.kitchen.cab import Cab

# needs pip install kubernetes dask-kubernetes

from . import get_kube_api, InjectedFileFormatters, session_id, session_user, resource_labels, session_user_info, KubeBackendOptions
from .kube_utils import StatusReporter

from kubernetes.client.rest import ApiException

from .kube_utils import apply_pod_spec
from . import infrastructure, pod_proxy
from stimela.backends.utils import resolve_remote_mounts


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

    kube = backend.kube

    args, log_args = cab.flavour.get_arguments(cab, params, subst, check_executable=False)

    log.debug(f"command line is {' '.join(log_args)}")

    cabstat = cab.reset_status()

    command_name = cab.flavour.command_name

    # generate podname
    tmp_name = session_user + "--" + fqname.replace(".", "--").replace("_", "--")
    token_hex = secrets.token_hex(4)
    podname = tmp_name[0:50] + "--" + token_hex
    # K8s don't like uppercase
    podname = podname.lower()

    namespace, kube_api, custom_obj_api = get_kube_api()

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
    bailout_with_exceptions = []

    def k8s_event_handler(event):
        objkind = event.involved_object.kind
        objname = event.involved_object.name
        if event.message.startswith("Error: ErrImagePull"):
            raise BackendError(f"{objkind} '{objname}': failed to pull the image '{image_name}'. Preceding log messages may contain extra information.",
                               event.to_dict())
        if event.reason == "Failed":
            raise BackendError(f"{objkind} '{objname}' reported a 'Failed' event. Preceding log messages may contain extra information.",
                               event.to_dict())
        # if event.reason == "ProvisioningFailed":
        #     raise BackendError(f"{objkind} '{objname}' reported a 'ProvisioningFailed' event. Preceding log messages may contain extra information.",
        #                       event.to_dict())
        # if event.reason == "FailedScheduling":
        #     raise StimelaCabRuntimeError(f"k8s has reported a 'FailedScheduling' event. Preceding log messages may contain extra information.")


    # define debug-print function
    if kube.debug.verbose:
        def dprint(level, message, payload, console_payload: Optional[Any] = None, syntax: Optional[str] = None):
            if level <= kube.debug.verbose:
                log_rich_payload(log, message, payload, console_payload=console_payload, syntax=syntax)
    else:
        def dprint(level, *args, **kw):
            pass

    statrep = StatusReporter(podname=podname, log=log, kube=kube,
                             event_handler=k8s_event_handler)

    if kube.debug.pause_on_start:
        command = "while sleep 600; do echo debug mode still active; done"
    else:
        command = args[0] + " " + " ".join(shlex.quote(x) for x in args[1:])
        if kube.dir:
            command = f"cd {kube.dir}; {command}"

    try:
        with declare_subtask(f"{cab.name}.kube-init", status_reporter=statrep.update):
            log.info(f"using image {image_name}")

            # create pod labels
            pod_labels = dict(stimela_job=podname,
                                stimela_fqname=fqname,
                                stimela_cab=os.path.basename(cab.name),
                                **resource_labels)

            # depending on whether or not a dask cluster is configured, we do either a DaskJob or a regular pod
            if kube.dask_cluster and kube.dask_cluster.enable:
                log.info(f"defining dask job with a cluster of {kube.dask_cluster.num_workers} workers")

                from . import daskjob
                dask_job_name = f"dj-{token_hex}"
                dask_job_spec = daskjob.render(OmegaConf.create(dict(
                    job_name=dask_job_name,
                    labels=pod_labels,
                    namespace=namespace,
                    image=image_name,
                    pull_policy='Always' if kube.always_pull_images else 'IfNotPresent',
                    memory_limit=kube.dask_cluster.memory_limit if kube.dask_cluster.memory_limit is not None
                                    else kube.dask_cluster.worker_pod.memory and kube.dask_cluster.worker_pod.memory.limit,
                    nworkers=kube.dask_cluster.num_workers,
                    threads_per_worker=kube.dask_cluster.threads_per_worker,
                    # cmdline=["/bin/sh", "-c", "while true;do date;sleep 5; done"],
                    service_account=kube.service_account,
                    mount_file=None,
                    environment_variables=kube.env
                )))

                # apply pod type specifications
                if kube.dask_cluster.worker_pod:
                    dprint(1, kube.dask_cluster.worker_pod)
                    dprint(1, dask_job_spec[0]["spec"]["cluster"]["spec"]["worker"]["spec"])
                    dask_job_spec[0]["spec"]["cluster"]["spec"]["worker"]["spec"] = \
                        apply_pod_spec(kube.dask_cluster.worker_pod, dask_job_spec[0]["spec"]["cluster"]["spec"]["worker"]["spec"],
                                                            kube.predefined_pod_specs, log, kind='worker')
                    dprint(1, "Worker pod success")
                if kube.dask_cluster.scheduler_pod:
                    dprint(1, kube.dask_cluster.scheduler_pod)
                    dprint(1, dask_job_spec[0]["spec"]["cluster"]["spec"]["scheduler"]["spec"])
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
            pod = pod_proxy.PodProxy(kube, podname, image_name, command=command, log=log)

            pod_manifest['spec'] = pod.pod_spec

            # add runtime env settings for port forwarding
            if dask_job_spec:
                pod_manifest['spec']['containers'][0]['env'].append(
                        dict(name="DASK_SCHEDULER_ADDRESS",
                        value=f"tcp://{dask_job_name}-scheduler.{namespace}.svc.cluster.local:8786"))

            # add persistent volumes
            # check that mount paths are set before we try to resolve volumes
            for name, pvc in kube.volumes.items():
                if not pvc.mount:
                    raise BackendError(f"volume {name} does not specify a mount path")

            # get the PVCs etc for the volumes
            volumes_initialized = []
            with declare_subcommand("provisioning storage"):
                volumes_provisioned = infrastructure.resolve_volumes(kube, log=log, step_token=token_hex)
                # create volume specs
                for name, pvc in kube.volumes.items():
                    pod_manifest['spec']['volumes'].append(dict(
                        name = name,
                        persistentVolumeClaim = dict(claimName=pvc.name)
                    ))
                    pod.add_volume(name, pvc.mount)
                    # add init commands, if needed
                    pvc0 = infrastructure.active_pvcs[name]
                    # add session init -- this only happens once, if volume is created
                    if not pvc0.initialized:
                        session_init = pvc.init_commands or []
                        if session_init:
                            # if pvc0.owner != session_user:
                            #     log.warning(f"skipping session initialization on PVC '{name}': owned by {pvc0.owner} not {session_user}")
                            # else:
                                pod.add_volume_init(pvc.mount, session_init, root=True)
                                volumes_initialized.append(pvc0)
                    # add step init
                    if pvc.step_init_commands:
                        # if pvc0.owner != session_user:
                        #     log.warning(f"skipping step initialization on PVC '{name}': owned by {pvc0.owner} not {session_user}")
                        # else:
                            pod.add_volume_init(pvc.mount, pvc.step_init_commands, root=False)

                # add to status reporter
                statrep.set_pvcs(kube.volumes)

            # add commands for checking required files
            pod.add_file_check_commands(params, cab)
            # start the threaded status update, since log reading blocks
            pod.start_status_update_thread()

            # start pod and wait for it to come up
            provisioning_deadline = time.time() + (kube.provisioning_timeout or 1e+10)
            if dask_job_spec is None:
                with declare_subcommand("starting pod"):
                    log.info(f"starting pod {podname} to run {command_name}")
                    dprint(1, "pod manifest", pod_manifest)
                    if kube.debug.save_spec:
                        log.info(f"saving pod manifest to {kube.debug.save_spec}")
                        open(kube.debug.save_spec, "wt").write(yaml.dump(pod_manifest))
                    resp = kube_api.create_namespaced_pod(body=pod_manifest, namespace=namespace)
                    dprint(2, "response", resp)
                    pod_created = resp
                    connected = True
                    while pod.check_status():
                        try:
                            resp = kube_api.read_namespaced_pod_status(name=podname, namespace=namespace,
                                                                    _request_timeout=(1, 1))
                        except (ConnectionError, HTTPError) as exc:
                            if connected:
                                log.warn("lost connection to k8s cluster while waiting for the pod to start")
                                log.warn("this is not fatal if the connection eventually resumes")
                                log.warn("use Ctrl+C if you want to give up")
                                connected = statrep.connected = False
                            time.sleep(1)
                            continue
                        phase = resp.status.phase
                        if not connected:
                            log.info("connection resumed", extra=dict(style="green"))
                            connected = statrep.connected = True
                        if phase == 'Running' or phase == 'Succeeded':
                            pod.dispatch_container_logs(kube.capture_logs_style, job=False)
                            break
                        elif phase == 'Failed':
                            pod.dispatch_container_logs(kube.capture_logs_style)
                            raise BackendError("pod startup failed, check logs above")
                        if time.time() >= provisioning_deadline:
                            log.error("timed out waiting for pod to start. The log above may contain more information.")
                            raise BackendError(f"pod failed to start after {kube.provisioning_timeout}s")
                        time.sleep(1)
            # else dask job
            else:
                with declare_subcommand("starting dask job"):
                    log.info(f"starting dask job {dask_job_name} to run {command_name}")
                    # overwrites dask_job job pod spec with one we built up here --
                    # that's ok since there's nothing useful in there (but look out for args.mounts)
                    dask_job_spec[0]["spec"]["job"]["spec"] = pod.pod_spec
                    dask_job_spec[0]["spec"]["job"]["metadata"] = dict(name=podname)
                    # copy job pod volumes to workers
                    volumes = pod.pod_spec['volumes']
                    mounts = pod.pod_spec['containers'][0]['volumeMounts']
                    for name, spec in dask_job_spec[0]["spec"]["cluster"]["spec"].items():
                        spec['spec'].setdefault('volumes', []).extend(volumes)
                        for cont in spec['spec']['containers']:
                            cont.setdefault('volumeMounts', []).extend(mounts)
                    # start the job
                    group, version, plural = 'kubernetes.dask.org', 'v1', 'daskjobs'
                    dprint(1, "daskjob spec", dask_job_spec[0])
                    if kube.debug.save_spec:
                        log.info(f"saving dask job spec to {kube.debug.save_spec}")
                        open(kube.debug.save_spec, "wt").write(yaml.dump(dask_job_spec[0]))
                    resp = custom_obj_api.create_namespaced_custom_object(group, version,
                                            namespace, plural , dask_job_spec[0])
                    dprint(2, "response", resp)
                    dask_job_created = resp
                    job_status = None
                    connected = True
                    # wait for dask job to start up
                    while pod.check_status():
                        try:
                            resp = custom_obj_api.get_namespaced_custom_object_status(group, version,
                                                    namespace, plural, name=dask_job_name, _request_timeout=(1, 1))
                        except (ConnectionError, HTTPError) as exc:
                            if connected:
                                log.warn("lost connection to k8s cluster while waiting for the daskjob to start")
                                log.warn("this is not fatal if the connection eventually resumes")
                                log.warn("use Ctrl+C if you want to give up")
                                connected = statrep.connected = False
                            time.sleep(1)
                            continue
                        if not connected:
                            log.info("connection resumed", extra=dict(style="green"))
                            connected = statrep.connected = True
                        job_status = 'status' in resp and resp['status']['jobStatus']
                        # get podname from job once it's running
                        if job_status:
                            statrep.set_main_status(job_status)
                            if job_status == 'Running' or job_status.startswith('Success'):
                                podname = resp['status']['jobRunnerPodName']
                                statrep.set_pod_name(podname)
                                log.info(f"job running as pod {podname}")
                                pod.name = podname
                                pod.dispatch_container_logs(kube.capture_logs_style, job=False)
                                break
                            elif job_status == 'Failed':
                                pod.dispatch_container_logs(kube.capture_logs_style)
                                raise BackendError("job startup failed, check logs above")

                        if time.time() >= provisioning_deadline:
                            log.error("timed out waiting for dask job to start. The log above may contain more information.")
                            raise BackendError(f"job failed to start after {kube.provisioning_timeout}s")
                        time.sleep(1)

                    # get other pods associated with DaskJob and watch their logs
                    if kube.dask_cluster.capture_logs:
                        pods = kube_api.list_namespaced_pod(namespace=namespace, label_selector=statrep.label_selector)
                        for auxpod in pods.items:
                            if auxpod.metadata.name != podname:
                                pod.start_logging_thread(auxpod.metadata.name, style=kube.dask_cluster.capture_logs_style)

                    # start port forwarding
                    if kube.dask_cluster.forward_dashboard_port:
                        log.info(f"starting port-forward process for http-dashboard to local port {kube.dask_cluster.forward_dashboard_port}")
                        port_forward_proc = subprocess.Popen([kube.kubectl_path,
                            "port-forward", f"service/{dask_job_name}-scheduler",
                            f"{kube.dask_cluster.forward_dashboard_port}:http-dashboard"])

            log.info(f"  pod started after {elapsed()}")

            # if any volumes had session init commands issued, mark them as initialized
            for pvc in volumes_initialized:
                log.info(f"marking PVC {pvc.name} as initialized")
                pvc.initialized = True
                patch_body = dict(metadata=dict(labels=dict(stimela_pvc_initialized="True")))
                kube_api.patch_namespaced_persistent_volume_claim(
                    name=pvc.name,
                    namespace=namespace,
                    body=patch_body)

            # resp = kube_api.read_namespaced_pod(name=podname, namespace=namespace)
            # log.info(f"  read_namespaced_pod {resp}")

            if kube.debug.pause_on_start:
                log.warning("kube.debug.pause_on_start is enabled")
                log.warning(f"to access the pod, run")
                log.warning(f"  $ kubectl exec -it {podname} -- /bin/bash")
                log.warning(f"your command line inside the pod is:")
                if kube.dir:
                    log.warning(f"  $ cd {kube.dir}")
                log.warning(f"  $ {' '.join(args)}")
                log.warning("press Ctrl+C here when done debugging to halt the pod")
            else:
                # do we need to chdir
                log.info(f"running: {command}")

        with declare_subtask(f"{cab.name}.kube-run", status_reporter=statrep.update):
            retcode = None
            connected = True
            last_log_timestamp = None
            seen_logs = set()
            while retcode is None and pod.check_status():
                try:
                    for entry in kube_api.read_namespaced_pod_log(name=podname, namespace=namespace, container="job",
                                follow=True, timestamps=True,
    #                            since_time=last_log_timestamp,
                                _preload_content=False,
                                _request_timeout=(kube.connection_timeout, kube.connection_timeout),
                            ).stream():
                        if not connected:
                            log.info("connection resumed", extra=dict(style="green"))
                            connected = True
                        # log.info(f"got [blue]{entry.decode()}[/blue]")
                        for line in entry.decode().rstrip().split("\n"):
                            if " " in line:
                                timestamp, content = line.split(" ", 1)
                            else:
                                timestamp, content = line, ""
                            key = timestamp, hash(content)
                            last_log_timestamp = timestamp
                            if key in seen_logs:
                                continue
                            seen_logs.add(key)
                            dispatch_to_log(log, content, command_name, "stdout",
                                            output_wrangler=cabstat.apply_wranglers)

                    # check for return code
                    resp = kube_api.read_namespaced_pod_status(name=podname, namespace=namespace)
                    statrep.connected = connected = True
                    contstat = resp.status.container_statuses[0].state
                    waiting = contstat.waiting
                    running = contstat.running
                    terminated = contstat.terminated
                    if waiting:
                        log.info("container state is 'waiting'")
                        dprint(2, "waiting", waiting)
                    elif running:
                        log.info(f"container state is 'running'")
                        dprint(2, "running", running)
                    elif terminated:
                        retcode = terminated.exit_code
                        log.info(f"container state is 'terminated', exit code is {retcode}")
                        dprint(2, "terminated", terminated)
                        break
                except (ConnectionError, HTTPError) as exc:
                    # this could be a real connection error, or maybe the process has gone quiet
                    # so check with the statrep object, since that maintains its own connection status
                    if not statrep.connected:
                        if connected:
                            log.warn("lost connection to k8s cluster: will try to reconnect")
                            log.warn("this is not fatal if the connection eventually resumes")
                            log.warn("use Ctrl+C if you want to give up")
                            connected = False
                    time.sleep(1)

            # check if output marked it as a fail
            if cabstat.success is False:
                log.error(f"declaring '{command_name}' as failed based on its output")

            # if retcode != 0 and not explicitly marked as success, mark as failed
            if retcode and cabstat.success is not True:
                cabstat.declare_failure(f"{command_name} returns error code {retcode} after {elapsed()}")
                if retcode == 137:
                    log.error(f"the pod was killed with an out-of-memory condition. Check your kube.job_pod.memory settings")
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
        traceback.print_exc()
        log.error(f"k8s API error after {elapsed()}: {exc}")
        raise BackendError("k8s API error", exc) from None
    except (ConnectionError, HTTPError) as exc:
        log.error(f"k8s connection error after {elapsed()}: {exc}")
        raise BackendError("k8s connection error", exc) from None
    # this drops out as a normal error response
    except StimelaCabRuntimeError as exc:
        log.error(f"cab runtime error after {elapsed()}: {exc}")
        raise
    except BackendError as exc:
        log.error(f"kube backend error after {elapsed()}: {exc}")
        raise
    except Exception as exc:
        log.error(f"k8s invocation of {command_name} failed after {elapsed()}")
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
            pod.initiate_cleanup()
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
            pod.cleanup()
        except KeyboardInterrupt:
            log.error(f"kube cleanup interrupted with Ctrl+C after {elapsed()}")
            raise StimelaCabRuntimeError(f"{command_name} cleanup interrupted with Ctrl+C after {elapsed()}")
        except Exception as exc:
            log.error(f"kube cleanup failed after {elapsed()}")
            traceback.print_exc()
            raise StimelaCabRuntimeError("kube backend cleanup error", exc)




