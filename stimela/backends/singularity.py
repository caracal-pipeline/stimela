import subprocess
import os
import logging
import pathlib
import shutil
from tempfile import mkdtemp
from contextlib import ExitStack
from enum import Enum
import stimela
from shutil import which
from dataclasses import dataclass
from omegaconf import OmegaConf
from typing import Dict, Any, Optional, Union
from scabha.basetypes import EmptyDictDefault
import datetime
from stimela.utils.xrun_asyncio import xrun
from stimela.exceptions import BackendError
from . import native

ReadWrite = Enum("BindMode", "ro rw", module=__name__)

@dataclass
class SingularityBackendOptions(object):
    @dataclass
    class BindDir(object):
        host: Optional[str] = None      # host path, default uses label, or else "empty" for tmpdir
        target: Optional[str] = None    # container path: ==host by default
        mode: ReadWrite = "rw"
        mkdir: bool = False             # create host directory if it doesn't exist
        conditional: Union[bool, str] = True # bind conditionally (will be formula-evaluated)

    enable: bool = True
    image_dir: str = os.path.expanduser("~/.singularity")
    auto_build: bool = True
    rebuild: bool = False
    executable: Optional[str] = None
    remote_only: bool = False      # if True, won't look for singularity on local system -- useful in combination with slurm wrapper

    contain: bool = True           # if True, runs with --contain
    containall: bool = False       # if True, runs with --containall
    bind_tmp: bool = True          # if True, implicitly binds an empty /tmp directory
    clean_tmp: bool = True         # if False, temporary directories will not be cleaned up. Useful for debugging.

    # optional extra bindings
    bind_dirs: Dict[str, BindDir] = EmptyDictDefault()
    env: Dict[str, str] = EmptyDictDefault()
    

SingularityBackendSchema = OmegaConf.structured(SingularityBackendOptions)

STATUS = VERSION = BINARY = None

# images rebuilt in this run
_rebuilt_images = set()


class CustomTemporaryDirectory(object):
    """Custom context manager for tempfile.mkdtemp()."""
    def __init__(self, clean_up=True):
        self.name = mkdtemp()
        self.clean_up = clean_up  # Workaround for < Python3.12

    def __enter__(self):
        return self.name

    def __exit__(self, exc_type, exc_value, traceback):
        if self.clean_up:
            shutil.rmtree(self.name)


def is_available(opts: Optional[SingularityBackendOptions] = None):
    global STATUS, VERSION, BINARY
    if STATUS is None:
        if opts and opts.remote_only:
            STATUS = VERSION = "remote"
        else:
            BINARY = (opts and opts.executable) or which("singularity")
            if BINARY:
                __version_string = subprocess.check_output([BINARY, "--version"]).decode("utf8")
                STATUS = VERSION = __version_string.strip().split()[-1]
                # if VERSION < "3.0.0":
                #     suffix = ".img"
                # else:
                #     suffix = ".sif"
            else:
                STATUS = "not installed"
                VERSION = None    
    return VERSION is not None

def get_status():
    is_available()
    return STATUS

def is_remote():
    return False

def init(backend: 'stimela.backend.StimelaBackendOptions', log: logging.Logger):
    pass

def get_image_info(cab: 'stimela.kitchen.cab.Cab', backend: 'stimela.backend.StimelaBackendOptions'):
    """returns image name/path corresponding to cab

    Args:
        cab (stimela.kitchen.cab.Cab): _description_
        backend (stimela.backend.StimelaBackendOptions): _description_

    Returns:
        name, path, enable_update: tuple of docker image name, path to singularity image on disk, and enable-updates flag
    """

    # prebuilt image
    if cab.image and cab.image.path:
        simg_path = cab.image.path
        if not os.path.exists(simg_path):
            raise BackendError(f"image {simg_path} for cab '{cab.name}' doesn't exist")
        return os.path.basename(simg_path), simg_path

    image_name = cab.flavour.get_image_name(cab, backend)

    if not image_name:
        raise BackendError(f"cab '{cab.name}' does not define an image")
    
    # convert to filename
    simg_name = image_name.replace("/", "-") + ".simg"
    simg_path = os.path.join(backend.singularity.image_dir, simg_name) 

    return image_name, simg_path


def build(cab: 'stimela.kitchen.cab.Cab', backend: 'stimela.backend.StimelaBackendOptions', log: logging.Logger,
            wrapper: Optional['stimela.backend.runner.BackendWrapper']=None,
            build=True, rebuild=False):
    """Builds image for cab, if necessary.

    build: if True, build missing images regardless of backend settings
    rebuild: if True, rebuild all images regardless of backend settings

    Returns:
        str: path to corresponding singularity image
    """

    # ensure image directory exists
    if os.path.exists(backend.singularity.image_dir):
        if not os.path.isdir(backend.singularity.image_dir):
            raise BackendError(f"invalid singularity image directory {backend.singularity.image_dir}")
    else:
        try:
            pathlib.Path(backend.singularity.image_dir).mkdir(parents=True)
        except OSError as exc:
            raise BackendError(f"failed to create singularity image directory {backend.singularity.image_dir}: {exc}")

    image_name, simg_path = get_image_info(cab, backend)

    # this is True if we're allowed to build missing images
    build = build or rebuild or backend.singularity.auto_build   
    # this is True if we're asked to force-rebuild images
    rebuild = rebuild or backend.singularity.rebuild
    
    cached_image_exists = os.path.exists(simg_path)

    # no image? Better have builds enabled then
    if not cached_image_exists:
        log.info(f"singularity image {simg_path} does not exist")
        if not build:
            raise BackendError(f"no image, and singularity build options not enabled")
    # else we have an image
    # if rebuild is enabled, delete it
    elif rebuild:
        if simg_path in _rebuilt_images:
            log.info(f"singularity image {simg_path} was rebuilt earlier")
        else:
            log.info(f"singularity image {simg_path} exists but a rebuild was specified")
            os.unlink(simg_path)        
            cached_image_exists = False
    else:
        log.info(f"singularity image {simg_path} exists")

    ## OMS: taking this out for now, need some better auto-update logic, let's come back to it later
    ## Please retain the code for now
        
    # # else check if it need to be auto-updated
    # elif auto_update_allowed and backend.singularity.auto_update:
    #     if image_name in _auto_updated_images:
    #         log.info("image was used earlier in this run, not checking for auto-updates again")
    #     else:
    #         _auto_updated_images.add(image_name)
    #         # force check of docker binary
    #         docker.is_available()
    #         if docker.BINARY is None:
    #             log.warn("a docker runtime is required for auto-update of singularity images: forcing unconditional rebuild")
    #             build = True
    #         else:
    #             log.info("singularity auto-update: pulling and inspecting docker image")
    #             # pull image from hub
    #             retcode = xrun(docker.BINARY, ["pull", image_name], 
    #                         shell=False, log=log,
    #                             return_errcode=True, command_name="(docker pull)", 
    #                             log_command=True, 
    #                             log_result=True)
    #             if retcode != 0:
    #                 raise BackendError(f"docker pull failed with return code {retcode}") 
    #             if os.path.exists(simg_path):
    #                 # check timestamp
    #                 result = subprocess.run(
    #                         [docker.BINARY, "inspect", "-f", "{{ .Created }}", image_name],
    #                         capture_output=True)
    #                 if result.returncode != 0:
    #                     for line in result.stdout.split("\n"):
    #                         log.warn(f"docker inpect stdout: {line}")
    #                     for line in result.stderr.split("\n"):
    #                         log.error(f"docker inpect stderr: {line}")
    #                     raise BackendError(f"docker inspect failed with return code {result.returncode}")
    #                 timestamp = result.stdout.decode().strip()
    #                 log.info(f"docker inspect returns timestamp {timestamp}")
    #                 # parse timestamps like '2023-04-07T13:39:19.187572398Z'
    #                 # Pre-3.11 pythons don't do it natively so we mess around...
    #                 match = re.fullmatch("(.*)T([^.]*)(\.\d+)?Z?", timestamp)
    #                 if not match:
    #                     raise BackendError(f"docker inspect returned invalid timestamp '{timestamp}'")
    #                 try:
    #                     dt = datetime.datetime.fromisoformat(f'{match.group(1)} {match.group(2)} +00:00')
    #                 except ValueError as exc:
    #                     raise BackendError(f"docker inspect returned invalid timestamp '{timestamp}', exc")

    #                 if dt.timestamp() > os.path.getmtime(simg_path):
    #                     log.warn("docker image is newer than cached singularity image, rebuilding")
    #                     os.unlink(simg_path)        
    #                     cached_image_exists = False
    #                 else:
    #                     log.info("cached singularity image appears to be up-to-date")

    # if image doesn't exist, build it. We will have already checked for build settings
    # being enabled above
    if not cached_image_exists:
        log.info(f"(re)building image {simg_path}")

        args = [BINARY, "build", simg_path, f"docker://{image_name}"]

        if wrapper:
            # args, log_args = wrapper.wrap_build_command(args, log=log)
            ret = wrapper.wrap_build_command(args, log=log)
            print(ret)
            args, log_args = ret

        retcode = xrun(args[0], args[1:], shell=False, log=log,
                    return_errcode=True, command_name="(singularity build)", 
                    gentle_ctrl_c=True,
                    log_command=' '.join(args), 
                    log_result=True)

        if retcode:
            raise BackendError(f"singularity build returns {retcode}")

        if not os.path.exists(simg_path):
            raise BackendError(f"singularity build did not return an error code, but the image did not appear")
        
        _rebuilt_images.add(simg_path)
        
    return simg_path



def run(cab: 'stimela.kitchen.cab.Cab', params: Dict[str, Any], fqname: str,
        backend: 'stimela.backend.StimelaBackendOptions',
        log: logging.Logger, subst: Optional[Dict[str, Any]] = None,
        wrapper: Optional['stimela.backends.runner.BackendWrapper'] = None):

    """Runs cab contents

    Args:
        cab (Cab): cab object
        log (logger): logger to use
        subst (Optional[Dict[str, Any]]): Substitution dict for commands etc., if any.

    Returns:
        Any: return value (e.g. exit code) of content
    """
    from .utils import resolve_required_mounts

    native.update_rlimits(backend.rlimits, log)

    # get path to image, rebuilding if backend options allow this
    simg_path = build(cab, backend=backend, log=log, build=False, wrapper=wrapper)

    # build up command line    
    cwd = os.getcwd()
    args = [backend.singularity.executable or BINARY, 
            "exec", 
            "--pwd", cwd]
    if backend.singularity.containall:
        args.append("--containall")
    elif backend.singularity.contain:
        args.append("--contain")
    if backend.singularity.env:
        args += ["--env", ",".join([f"{k}={v}" for k, v in backend.singularity.env.items()])]

    # initial set of mounts has cwd as read-write
    mounts = {cwd: True}
    # dict of container paths to host paths
    container_to_host_path = {}

    with ExitStack() as exit_stack: 
        # add extra binds
        for label, bind in backend.singularity.bind_dirs.items():
            # skip if conditional is False
            if not bind.conditional:
                log.info(f"bind_dirs.{label}: skipping based on conditional == {bind.conditional}")
                continue

            # expand ~ in paths
            src = os.path.expanduser(bind.host).rstrip("/")
            dest = os.path.expanduser(bind.target or src).rstrip("/")
            rw = bind.mode == ReadWrite.rw

            # handle binding of empty temp dirs
            if bind.host == "empty":
                if not bind.target:
                    raise BackendError(f"bind_dirs.{label}: a target must be specified when host=empty")
                tmpdir = CustomTemporaryDirectory(clean_up=backend.singularity.clean_tmp)
                src = exit_stack.enter_context(tmpdir)
                log.info(f"bind_dirs.{label}: using temporary directory {src}")

            # resolve symlinks
            if os.path.realpath(src) != src:
                src = os.path.realpath(src)
                log.info(f"bind_dirs.{label}: binding symlink target {src}")
            
            # make directory if needed
            if bind.mkdir:
                # I think files can be bound too, so only do this check for directories
                if os.path.exists(src):
                    if not os.path.isdir(src):
                        raise BackendError(f"bind_dirs.{label}: host path is not a directory")
                else:
                    try:
                        pathlib.Path(src).mkdir(parents=True)
                    except Exception as exc:
                        raise BackendError(f"bind_dirs.{label}: error creating directory {bind.host}", exc)
                
            # if already present in mounts, potentially upgrade to rw
            mounts[src] = mounts.get(src, False) or rw
            # if paths different, create a remapping
            if src != dest:
                if dest in container_to_host_path:
                    if container_to_host_path[dest] != src:
                        raise BackendError(f"bind_dirs.{label}: conflicting bind paths for {dest}")
                else:
                    container_to_host_path[dest] = src

        # get extra required filesystem bindings from supplied parameters
        resolve_required_mounts(mounts, params, cab.inputs, cab.outputs, remappings=container_to_host_path)

        # redo mounts as a list of (container_path, source_path, rw)
        source_to_containter_path = {src: dest for dest, src in container_to_host_path.items()}
        # make list of mounts
        mounts = [(source_to_containter_path.get(src, src), src, rw) for src, rw in mounts.items()]
        # add implicit /tmp mount
        if backend.singularity.bind_tmp:
            for target, _, _ in mounts:
                if target == "/tmp":
                    log.info("/tmp directory already bound, not adding an explicit binding")
                    break
            else:
                tmpdir = CustomTemporaryDirectory(clean_up=backend.singularity.clean_tmp)
                tmpdir_name = exit_stack.enter_context(tmpdir)
                mounts.append(("/tmp", tmpdir_name, True))

        # sort mount paths before iterating -- this ensures that parent directories come first
        # (singularity doesn't like it if you specify a bind of a subdir before a bind of a parent) 
        for dest, src, rw in sorted(mounts):
            mode = 'rw' if rw else 'ro'
            if src == dest:
                log.info(f"binding {src} as {mode}")
            else:
                log.info(f"binding {src} to {dest} as {mode}")
            args += ["--bind", f"{src}:{dest}:{mode}"]

        args += [simg_path]
        log_args = args.copy()

        args1, log_args1 = cab.flavour.get_arguments(cab, params, subst, check_executable=False, log=log)
        args += args1
        log_args += log_args1

        cabstat = cab.reset_status()

        command_name = f"{cab.flavour.command_name}" or None

        # run command
        start_time = datetime.datetime.now()
        def elapsed(since=None):
            """Returns string representing elapsed time"""
            return str(datetime.datetime.now() - (since or start_time)).split('.', 1)[0]

        # log.info(f"argument lengths are {[len(a) for a in args]}")

        if wrapper:
            args, log_args = wrapper.wrap_run_command(args, log_args, fqname=fqname, log=log)

        log.debug(f"command line is {' '.join(log_args)}")

        retcode = xrun(args[0], args[1:], shell=False, log=log,
                    output_wrangler=cabstat.apply_wranglers,
                    return_errcode=True, command_name=command_name, 
                    gentle_ctrl_c=True,
                    log_command=' '.join(log_args), 
                    log_result=False)

        # check if output marked it as a fail
        if cabstat.success is False:
            log.error(f"declaring '{command_name}' as failed based on its output")

        # if retcode != 0 and not explicitly marked as success, mark as failed
        if retcode and cabstat.success is not True:
            cabstat.declare_failure(f"{command_name} returns error code {retcode} after {elapsed()}")
        else:
            log.info(f"{command_name} returns exit code {retcode} after {elapsed()}")

        return cabstat


# class SingularityError(Exception):
#     pass

# def pull(image, name, docker=True, directory=".", force=False):
#     """ 
#         pull an image
#     """
#     if docker:
#         fp = "docker://{0:s}".format(image)
#     else:
#         fp = image
#     if not os.path.exists(directory):
#         os.mkdir(directory)

#     image_path = os.path.abspath(os.path.join(directory, name))
#     if os.path.exists(image_path) and not force:
#         stimela.logger().info(f"Singularity image already exists at '{image_path}'. To replace it, please re-run with the 'force' option")
#     else:
#         utils.xrun(f"cd {directory} && singularity", ["pull", 
#         	"--force" if force else "", "--name", 
#          	name, fp])

#     return 0

# class Container(object):
#     def __init__(self, image, name,
#                  volumes=None,
#                  logger=None,
#                  time_out=-1,
#                  runscript="/singularity",
#                  environs=None,
#                  workdir=None,
#                  execdir="."):
#         """
#         Python wrapper to singularity tools for managing containers.
#         """

#         self.image = image
#         self.volumes = volumes or []
#         self.environs = environs or []
#         self.logger = logger
#         self.status = None
#         self.WORKDIR = workdir
#         self.RUNSCRIPT = runscript
#         self.PID = os.getpid()
#         self.uptime = "00:00:00"
#         self.time_out = time_out
#         self.execdir = execdir

#         self._env = os.environ.copy()

#         hashname = hashlib.md5(name.encode('utf-8')).hexdigest()[:3]
#         self.name = hashname if version < "3.0.0" else name

#     def add_volume(self, host, container, perm="rw", noverify=False):
#         if os.path.exists(host) or noverify:
#             if self.logger:
#                 self.logger.debug("Mounting volume [{0}] in container [{1}] at [{2}]".format(
#                     host, self.name, container))
#             host = os.path.abspath(host)
#         else:
#             raise IOError(
#                 "Path {0} cannot be mounted on container: File doesn't exist".format(host))

#         self.volumes.append(":".join([host, container, perm]))

#         return 0

#     def add_environ(self, key, value):
#         self.logger.debug("Adding environ varaible [{0}={1}] "\
#                     "in container {2}".format(key, value, self.name))
#         self.environs.append("=".join([key, value]))
#         key_ = f"SINGULARITYENV_{key}"
	
#         self.logger.debug(f"Setting singularity environmental variable {key_}={value} on host")
#         self._env[key_] = value

#         return 0

#     def run(self, *args, output_wrangler=None):
#         """
#         Run a singularity container instance
#         """

#         if self.volumes:
#             volumes = " --bind " + " --bind ".join(self.volumes)
#         else:
#             volumes = ""

#         if not os.path.exists(self.image):
#             self.logger.error(f"The image, {self.image}, required to run this cab does not exist."\
#                     " Please run 'stimela pull --help' for help on how to download the image")
#             raise SystemExit from None

#         self.status = "running"
#         self._print("Starting container [{0:s}]. Timeout set to {1:d}. The container ID is printed below.".format(
#             self.name, self.time_out))
        
#         utils.xrun(f"cd {self.execdir} && singularity", ["run", "--workdir", self.execdir, "--containall"] \

#                     + list(args) + [volumes, self.image, self.RUNSCRIPT],
#                     log=self.logger, timeout=self.time_out, output_wrangler=output_wrangler,
#                     env=self._env, logfile=self.logfile)

#         self.status = "exited"

#         return 0

#     def _print(self, message):
#         if self.logger:
#             self.logger.info(message)
#         else:
#             print(message)

#         return 0
