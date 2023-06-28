import subprocess
import os
import re
import logging
from stimela import utils
import stimela
from shutil import which
from dataclasses import dataclass
from omegaconf import OmegaConf
from typing import Dict, List, Any, Optional, Tuple
import datetime
from stimela.utils.xrun_asyncio import xrun

from stimela.exceptions import BackendError

from . import native, docker

@dataclass
class SingularityBackendOptions(object):
    enable: bool = True
    image_dir: str = os.path.expanduser("~/.singularity")
    auto_build: bool = True
    rebuild: bool = False
    auto_update: bool = False
    executable: Optional[str] = None

SingularityBackendSchema = OmegaConf.structured(SingularityBackendOptions)

STATUS = VERSION = BINARY = None

_auto_updated_images = set()

def is_available():
    global STATUS, VERSION, BINARY
    if STATUS is None:
        BINARY = which("singularity")
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


def get_image_info(cab: 'stimela.kitchen.cab.Cab', backend: 'stimela.backend.StimelaBackendOptions'):
    """returns image name/path corresponding to cab

    Args:
        cab (stimela.kitchen.cab.Cab): _description_
        backend (stimela.backend.StimelaBackendOptions): _description_

    Returns:
        name, path: tuple of docker image name, and path to singularity image on disk
    """

    image_name = cab.flavour.get_image_name(cab, backend)

    if not image_name:
        raise BackendError(f"cab '{cab.name}' (singularity backend): image name not defined")
    
    # form up full image name (with registry and version)
    if "/" not in image_name:
        image_name = f"{backend.registry}/{image_name}"
    if ":" not in image_name:
        image_name = f"{image_name}:latest"

    # convert to filename
    simg_name = image_name.replace("/", "-") + ".simg"
    simg_path = os.path.join(backend.singularity.image_dir, simg_name) 

    return image_name, simg_path


def build_command_line(cab: 'stimela.kitchen.cab.Cab', backend: 'stimela.backend.StimelaBackendOptions',
                        params: Dict[str, Any], 
                        subst: Optional[Dict[str, Any]] = None,
                        binary: Optional[str] = None,
                        simg_path: Optional[str] = None):
    args = cab.flavour.get_arguments(cab, params, subst, check_executable=False)

    if simg_path is None:
        _, simg_path = get_image_info(cab, backend)

    return [binary or backend.singularity.executable or BINARY, "exec", simg_path] + args



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
    native.update_rlimits(backend.rlimits, log)

    image_name, simg_path = get_image_info(cab, backend)

    rebuild = backend.singularity.rebuild  
    cached_image_exists = os.path.exists(simg_path)

    # no image? Better have builds enabled then
    if not cached_image_exists:
        log.info(f"cached singularity image {simg_path} does not exist")
        rebuild = rebuild or backend.singularity.auto_build or backend.singularity.auto_update
        if not rebuild:
            raise BackendError(f"no image, and singularity build options not enabled")

    # check if existing image needs to be rebuilt
    if backend.singularity.auto_update and not rebuild:
        if image_name in _auto_updated_images:
            log.info("image was used earlier in this run, not checking for auto-updates again")
        else:
            _auto_updated_images.add(image_name)
            # force check of docker binary
            docker.is_available()
            if docker.BINARY is None:
                log.warn("a docker runtime is required for auto-update of singularity images: forcing unconditional rebuild")
                rebuild = True
            else:
                log.info("singularity auto-update: pulling and inspecting docker image")
                # pull image from hub
                retcode = xrun(docker.BINARY, ["pull", image_name], 
                            shell=False, log=log,
                                return_errcode=True, command_name="(docker pull)", 
                                log_command=True, 
                                log_result=True)
                if retcode != 0:
                    raise BackendError(f"docker pull failed with return code {retcode}") 
                if os.path.exists(simg_path):
                    # check timestamp
                    result = subprocess.run(
                            [docker.BINARY, "inspect", "-f", "{{ .Created }}", image_name],
                            capture_output=True)
                    if result.returncode != 0:
                        for line in result.stdout.split("\n"):
                            log.warn(f"docker inpect stdout: {line}")
                        for line in result.stderr.split("\n"):
                            log.error(f"docker inpect stderr: {line}")
                        raise BackendError(f"docker inspect failed with return code {result.returncode}")
                    timestamp = result.stdout.decode().strip()
                    log.info(f"docker inspect returns timestamp {timestamp}")
                    # parse timestamps like '2023-04-07T13:39:19.187572398Z'
                    # Pre-3.11 pythons don't do it natively so we mess around...
                    match = re.fullmatch("(.*)T([^.]*)(\.\d+)?Z?", timestamp)
                    if not match:
                        raise BackendError(f"docker inspect returned invalid timestamp '{timestamp}'")
                    try:
                        dt = datetime.datetime.fromisoformat(f'{match.group(1)} {match.group(2)} +00:00')
                    except ValueError as exc:
                        raise BackendError(f"docker inspect returned invalid timestamp '{timestamp}', exc")

                    if dt.timestamp() > os.path.getmtime(simg_path):
                        log.warn("docker image is newer than cached singularity image, rebuilding")
                        rebuild = True
                    else:
                        log.info("cached singularity image appears to be up-to-date")

    # delete image if rebuild is being forced
    if cached_image_exists and rebuild:
        os.unlink(simg_path)        
        cached_image_exists = False

    # if image doesn't exist, build it. We will have already checked for build settings
    # being enabled above
    if not cached_image_exists:
        log.info(f"(re)building image {simg_path}")

        args = [BINARY, "build", simg_path, f"docker://{image_name}"]

        retcode = xrun(args[0], args[1:], shell=False, log=log,
                    return_errcode=True, command_name="(singularity build)", 
                    gentle_ctrl_c=True,
                    log_command=True, 
                    log_result=True)

        if retcode:
            raise BackendError(f"singularity build returns {retcode}")

        if not os.path.exists(simg_path):
            raise BackendError(f"singularity build did not return an error code, but the image did not appear")
    
    args = build_command_line(cab, backend, params, subst, simg_path=simg_path)

    log.debug(f"command line is {args}")

    cabstat = cab.reset_status()

    command_name = f"{cab.flavour.command_name}"

    # run command
    start_time = datetime.datetime.now()
    def elapsed(since=None):
        """Returns string representing elapsed time"""
        return str(datetime.datetime.now() - (since or start_time)).split('.', 1)[0]

    # log.info(f"argument lengths are {[len(a) for a in args]}")

    retcode = xrun(args[0], args[1:], shell=False, log=log,
                output_wrangler=cabstat.apply_wranglers,
                return_errcode=True, command_name=command_name, 
                gentle_ctrl_c=True,
                log_command=True if cab.flavour.log_full_command else command_name, 
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
