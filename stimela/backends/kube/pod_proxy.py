from dataclasses import fields
from typing import List, Dict
import logging
import os.path
import time, datetime
import threading
from stimela.exceptions import BackendError
from stimela.utils.xrun_asyncio import dispatch_to_log
from stimela.task_stats import update_process_status
from stimela.kitchen.cab import Cab, Parameter

from kubernetes.client import ApiException
from kubernetes.stream import stream

from . import KubeBackendOptions, session_user_info, get_kube_api
from .kube_utils import apply_pod_spec
from stimela.backends.utils import resolve_remote_mounts

class PodProxy(object):

    def __init__(self, kube: KubeBackendOptions, podname: str, image_name: str, command: str, log: logging.Logger):
        self.namespace, self.kube_api, _ = get_kube_api()
        self.kube = kube
        self.name = podname
        self.log = log
        # setup user information -- current user info by default, overridden by backend options
        self.uinfo = KubeBackendOptions.UserInfo()
        for fld in fields(self.uinfo):
            value = getattr(kube.user, fld.name)
            setattr(self.uinfo, fld.name, value if value is not None else getattr(session_user_info, fld.name))

        # setup command
        if self.uinfo.inject_nss:
            command = \
                "cp /etc/passwd $HOME/.passwd; " + \
                "cp /etc/group $HOME/.group; " + \
                "echo $USER:x:$USER_UID:$USER_GID:$USER_GECOS:$HOME:/bin/sh >> $HOME/.passwd; " + \
                "echo $GROUP:x:$USER_GID: >> $HOME/.group; " + \
                command

        self.pod_spec = dict(
            containers = [dict(
                    image   = image_name,
                    imagePullPolicy = 'Always' if kube.always_pull_images else 'IfNotPresent',
                    name    = "job",
                    command = ["/bin/bash"],
                    args    = ["-c", command],
                    env     = [
                        dict(name="USER", value=self.uinfo.name),
                        dict(name="GROUP", value=self.uinfo.group),
                        dict(name="HOME", value=self.uinfo.home),
                        dict(name="USER_UID", value=str(self.uinfo.uid)),
                        dict(name="USER_GID", value=str(self.uinfo.gid)),
                        dict(name="USER_GECOS", value=str(self.uinfo.gecos))
                    ],
                    securityContext = dict(
                            runAsNonRoot = self.uinfo.uid!=0,
                            runAsUser = self.uinfo.uid,
                            runAsGroup = self.uinfo.gid,
                    ),
                    volumeMounts = [dict(
                        name = "home-directory",
                        mountPath = self.uinfo.home
                    )]
            )],
            volumes = [dict(
                name = "home-directory",
                emptyDir = dict(medium="Memory") if self.uinfo.home_ramdisk else {} 
            )],
            serviceAccountName = kube.service_account,
            automountServiceAccountToken = True,
            restartPolicy = "Never"
        )
        # apply predefined pod type specifications
        self.pod_spec = apply_pod_spec(kube.job_pod, self.pod_spec, kube.predefined_pod_specs, self.log, kind='job')

        # add runtime env settings
        for name, value in kube.env.items():
            value = os.path.expanduser(value)
            self.pod_spec['containers'][0]['env'].append(dict(name=name, value=value))

        self._session_init_container = None
        self._step_init_container = None          
        self._exit_logging_threads = False
        self._aux_pod_threads = {}
        self._mounts = {}
        # accumulate list of exceptions that are raised in the updater thread
        self._exceptions = []

    def _status_updater(self):
        while not self._exit_logging_threads:
            try:
                update_process_status()
            except BackendError as exc:
                self._exceptions.append(exc)
            time.sleep(1)

    def start_status_update_thread(self):
        if "status" not in self._aux_pod_threads:
            thread = threading.Thread(target=self._status_updater)
            self._aux_pod_threads["status"] = thread, None, "status update thread"
            thread.start()

    def check_status(self):
        """
        Checks if status thread has caught any exceptions, and re-reises them if so.
        Returns True of no exceptions.
        """
        if self._exceptions:
            if len(self._exceptions) == 1:
                raise self._exceptions[0]
            else:
                raise BackendError("k8s backend errors", *self._exceptions)
        return True

    @property
    def session_init_container(self):
        if self._session_init_container is None:
            self._session_init_container = dict(
                name="volume-session-init",
                image="quay.io/quay/busybox",
                command=["/bin/sh", "-c", ""],
                volumeMounts=[])
            self.pod_spec.setdefault('initContainers', []).append(self._session_init_container)
        return self._session_init_container

    @property
    def step_init_container(self):
        if self._step_init_container is None:
            self._step_init_container = dict(
                name="volume-step-init",
                image="quay.io/quay/busybox",
                command=["/bin/sh", "-c", ""],
                securityContext=dict(
                        runAsNonRoot = self.uinfo.uid!=0,
                        runAsUser = self.uinfo.uid,
                        runAsGroup = self.uinfo.gid),
                volumeMounts=[])
            self.pod_spec.setdefault('initContainers', []).append(self._step_init_container)
        return self._step_init_container
    
    @staticmethod
    def add_init_container_command(cont, command):
        cont['command'][-1] += command

    @staticmethod
    def add_init_container_mount(cont, volume_name, mount):
        for mnt in cont['volumeMounts']:
            if mnt['mountPath'] == mount:
                return
        cont['volumeMounts'].append(dict(
            name=volume_name, mountPath=mount
        ))

    def add_volume(self, volume_name, mount):
        if mount in self._mounts:
            raise BackendError(f"multiple volumes configured for mount {mount}")
        self.pod_spec['containers'][0]['volumeMounts'].append(dict(
            name=volume_name, mountPath=mount
        ))
        self._mounts[mount] = volume_name

    def add_volume_init(self, mount: str, commands: List[str], root: bool=False):
        """Adds volume initialization commands to init container"""
        # get approproate init container
        cont = self.session_init_container if root else self.step_init_container
        # add to its commands
        volume_name = self._mounts[mount]
        self.log.info(f"adding init commands for PVC '{volume_name}': {'; '.join(commands)}")
        self.add_init_container_command(cont, f"cd {mount}; {'; '.join(commands)}; ")
        self.add_init_container_mount(cont, volume_name, mount)

    def dispatch_container_logs(self, style: str, job: bool = True):
        containers = self.pod_spec.get('initContainers', [])
        if job:
            containers += self.pod_spec['containers']
        for cont in containers:
            contname = cont['name']
            try:
                loglines = self.kube_api.read_namespaced_pod_log(name=self.name, namespace=self.namespace, container=contname)
                for line in loglines.split("\n"):
                    if line:
                        dispatch_to_log(self.log, line, contname, "stdout", prefix=f"{contname}#", 
                                        style=style, output_wrangler=None)
            except ApiException as exc:
                dispatch_to_log(self.log, "no logs", contname, "stdout", prefix=f"{contname}#", 
                                style=style, output_wrangler=None)

    def _print_logs(self, name, style, container=None):
        """Helper function -- prints logs from a pod in a separate thread"""
        prefix = f"{name}#" if container is None else f"{name}.{container}#" 
        dispatch_to_log(self.log, f"started logging thread for {name} {container}", name, "stdout", prefix=prefix, style=style, output_wrangler=None)
        try:
            while not self._exit_logging_threads:
                try:
                    # rich.print(f"[yellow]started log thread for {name}[/yellow]")
                    # Open a stream to the logs
                    stream = self.kube_api.read_namespaced_pod_log(name=name, namespace=self.namespace, container=container, 
                                                                _preload_content=False, follow=True)
                    for line in stream.stream():
                        dispatch_to_log(self.log, line.decode().rstrip(), name, "stdout", prefix=prefix, style=style, output_wrangler=None)
                except ApiException as exc:
                    # dispatch_to_log(log, f"error reading log: {exc}", command_name, "stdout", prefix=prefix, style=style, output_wrangler=None)
                    pass
                time.sleep(2)
        finally:
            dispatch_to_log(self.log, f"exiting logging thread", name, "stdout", prefix=prefix, style=style, output_wrangler=None)
    
    def add_file_check_commands(self, params: Dict[str, Parameter], cab: Cab):
        # add commands to check for files and make directories
        must_exist_list, mkdir_list, remove_if_exists_list, active_mounts = \
            resolve_remote_mounts(params, cab.inputs, cab.outputs, cwd=self.kube.dir, mounts=set(self._mounts.keys()))
        
        if must_exist_list or mkdir_list or remove_if_exists_list:
            cont = self.step_init_container
            for path in mkdir_list:
                error = f"VALIDATION ERROR: mkdir {path} failed"
                self.add_init_container_command(cont, f"ls {path} >/dev/null; if mkdir -p {path}; then echo Created directory {path}; else echo {error}; exit 1; fi; ")
            for path in must_exist_list:
                error = f"VALIDATION ERROR: {path} doesn\\'t exist"
                self.add_init_container_command(cont, f"ls {path} >/dev/null; if test -e '{path}'; then echo Checking {path}: exists; else echo {error}; exit 1; fi; ")
            for path in remove_if_exists_list:
                self.add_init_container_command(cont, f"ls {path} >/dev/null; if test -e {path}; then echo Removing {path}; rm -fr {path}; true; fi; ")
            # make sure the relevant mounts are provided inside init container
            for mount in active_mounts:
                self.add_init_container_mount(cont, self._mounts[mount], mount)

    def start_logging_thread(self, name, style, container=None):
        """Starts thread to pick up logs"""
        thread = threading.Thread(target=self._print_logs, 
                        kwargs=dict(name=name, container=container, style=style))
        self._aux_pod_threads[name] = thread, name, f"auxiliary logging thread for {name}"
        thread.start()

    def initiate_cleanup(self):
        self._exit_logging_threads = True
        
    def cleanup(self):
        cleanup_time = datetime.datetime.now()
        def cleanup_elapsed():
            return (datetime.datetime.now() - cleanup_time).total_seconds()
        while self._aux_pod_threads and cleanup_elapsed() < 5:
            update_process_status()
            for name, (thread, aux_pod, desc) in list(self._aux_pod_threads.items()):
                if thread.is_alive():
                    thread.join(0.01)
                if thread.is_alive() and aux_pod:
                    self.log.info(f"{desc} alive, trying to delete associated pod")
                    try:
                        self.kube_api.delete_namespaced_pod(name=aux_pod, namespace=self.namespace)
                    except ApiException as exc:
                        self.log.warning(f"deleting pod {aux_pod} failed: {exc}")
                        pass
                if not thread.is_alive():
                    del self._aux_pod_threads[name]
            if self._aux_pod_threads:
                # self.log.info(f"{len(self._aux_pod_threads)} auxiliary threads still alive, sleeping for a bit")
                time.sleep(0.5)
        if self._aux_pod_threads:
            self.log.info(f"{len(self._aux_pod_threads)} auxiliary threads still alive after {cleanup_elapsed():.1}s, giving up on the cleanup")

    def run_pod_command(self, command, cmdname, input=None, wrangler=None):
        if type(command) is str:
            command = ["/bin/sh", "-c", command]
        has_input = bool(input)

        resp = stream(self.kube_api.connect_get_namespaced_pod_exec, self.name, self.namespace,
                    command=command,
                    stderr=True, stdin=has_input,
                    stdout=True, tty=False,
                    _preload_content=False)

        while resp.is_open():
            update_process_status()
            resp.update(timeout=1)
            if resp.peek_stdout():
                for line in resp.read_stdout().rstrip().split("\n"):
                    dispatch_to_log(self.log, line, cmdname, "stdout", output_wrangler=wrangler)
            if resp.peek_stderr():
                for line in resp.read_stderr().rstrip().split("\n"):
                    dispatch_to_log(self.log, line, cmdname, "stderr", output_wrangler=wrangler)
            if has_input:
                if input:
                    resp.write_stdin(input)
                    input = None
                else:
                    break

        retcode = resp.returncode
        resp.close()
        return retcode

            # # inject files into pod
            # if kube.inject_files:
            #     with declare_subcommand("configuring pod (inject)"):
            #         for filename, injection in kube.inject_files.items():
            #             content = injection.content
            #             formatter = InjectedFileFormatters.get(injection.format.name)
            #             if formatter is None:
            #                 raise StimelaCabParameterError(f"unsupported format {injection.format.name} for {filename}")
            #             # convert content to something serializable
            #             if isinstance(content, (DictConfig, ListConfig)):
            #                 content = OmegaConf.to_container(content)
            #             content = formatter(content)
            #             log.info(f"injecting {filename} into pod {podname}")
            #             retcode = run_pod_command(f"mkdir -p {os.path.dirname(filename)}; cat >{filename}", "inject", input=content)
            #             if retcode:
            #                 log.warning(f"injection returns exit code {retcode} after {elapsed()}")
            #             else:
            #                 log.info(f"injection successful after {elapsed()}")


            # if kube.pre_commands:
            #     with declare_subcommand("configuring pod (pre-commands)"):
            #         for pre_command in kube.pre_commands:
            #             log.info(f"running pre-command '{pre_command}' in pod {podname}")
            #             # calling exec and waiting for response
            #             retcode = run_pod_command(pre_command, pre_command.split()[0])
            #             if retcode:
            #                 log.warning(f"pre-command returns exit code {retcode} after {elapsed()}")
            #             else:
            #                 log.info(f"pre-command successful after {elapsed()}")



            # # add local mounts
            # def add_local_mount(name, path, dest, readonly):
            #     name = name.replace("_", "-")  # sanitize name
            #     pod_manifest['spec']['volumes'].append(dict(
            #         name = name,
            #         hostPath = dict(path=path, type='Directory' if os.path.isdir(path) else 'File')
            #     ))
            #     pod_manifest['spec']['containers'][0]['volumeMounts'].append(dict(name=name, mountPath=dest, readOnly=readonly))

            # # this will accumulate mounted paths from runtime spec
            # prior_mounts = {}

            # # add local mounts from runtime spec
            # for name, mount in kube.local_mounts.items():
            #     path = os.path.abspath(os.path.expanduser(mount.path))
            #     dest = os.path.abspath(os.path.expanduser(mount.dest)) if mount.dest else path
            #     if not os.path.exists(path) and mount.mkdir:
            #         pathlib.Path(path).mkdir(parents=True)
            #     add_local_mount(name, path, dest, mount.readonly)
            #     if path == dest:
            #         prior_mounts[path] = not mount.readonly

            # # add local mounts to support parameters
            # req_mounts = {} # resolve_required_mounts(params, cab.inputs, cab.outputs, prior_mounts=prior_mounts)
            # for i, (path, readwrite) in enumerate(req_mounts.items()):
            #     log.info(f"adding local mount {path} (readwrite={readwrite})")
            #     add_local_mount(f"automount-{i}", path, path, not readwrite)

