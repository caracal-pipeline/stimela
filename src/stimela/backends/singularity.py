import datetime
import logging
import os
import pathlib
import subprocess
from contextlib import ExitStack
from dataclasses import dataclass
from shutil import which
from typing import Any, Dict, Optional, Union

from omegaconf import OmegaConf
from scabha.basetypes import EmptyDictDefault

import stimela
from stimela.exceptions import BackendError
from stimela.utils.xrun_asyncio import xrun

from . import native
from .binds import BindDir, CustomTemporaryDirectory, ReadWrite  # noqa: F401 (re-exported for compatibility)

# can only execute cabs that specify a container image
requires_container_image = True


@dataclass
class SingularityBackendOptions(object):
    BindDir = BindDir  # nested alias kept for backwards compatibility

    enable: bool = True
    image_dir: str = os.path.expanduser("~/.singularity")
    auto_build: bool = True
    rebuild: bool = False
    executable: Optional[str] = None
    remote_only: bool = (
        False  # if True, won't look for singularity on local system -- useful in combination with slurm wrapper
    )

    contain: bool = True  # if True, runs with --contain
    containall: bool = False  # if True, runs with --containall
    # If True, runs with --cleanenv, so that the container environment is determined by the image
    # plus the explicit 'env' settings below, and not by whatever the Stimela process happens to
    # have inherited or had mutated into it. Implied by --containall. See caracal-pipeline/stimela#611.
    clean_env: bool = True
    bind_tmp: Union[bool, str] = (
        True  # binds /tmp to "tmp" class if True. If string, uses specified ephemeral storage class (e.g. "ram")
    )
    clean_tmp: bool = True  # if False, temporary directories will not be cleaned up. Useful for debugging.

    # optional extra bindings
    bind_dirs: Dict[str, BindDir] = EmptyDictDefault()
    # extra environment variable settings
    env: Dict[str, str] = EmptyDictDefault()

    # ephemeral storage settings, e.g. {tmp: /tmp, ram: /dev/shm}
    ephemeral: Dict[str, str] = EmptyDictDefault()


SingularityBackendSchema = OmegaConf.structured(SingularityBackendOptions)

STATUS = VERSION = BINARY = None

# images rebuilt in this run
_rebuilt_images = set()


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
            else:
                STATUS = "not installed"
                VERSION = None
    return VERSION is not None


def get_status():
    is_available()
    return STATUS


def is_remote():
    return False


def init(backend: "stimela.backend.StimelaBackendOptions", log: logging.Logger):
    pass


def get_image_info(cab: "stimela.kitchen.cab.Cab", backend: "stimela.backend.StimelaBackendOptions"):
    """returns image name/path corresponding to cab

    Args:
        cab (stimela.kitchen.cab.Cab): _description_
        backend (stimela.backend.StimelaBackendOptions): _description_

    Returns:
        name, path, enable_update: tuple of docker image name, path to singularity image on disk, and
            enable-updates flag
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


def build(
    cab: "stimela.kitchen.cab.Cab",
    backend: "stimela.backend.StimelaBackendOptions",
    log: logging.Logger,
    wrapper: Optional["stimela.backend.runner.BackendWrapper"] = None,
    build=True,
    rebuild=False,
):
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
            raise BackendError("no image, and singularity build options not enabled")
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
    #             log.warn(
    #                 "a docker runtime is required for auto-update of singularity images: "
    #                 "forcing unconditional rebuild"
    #             )
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

        args = log_args = [BINARY, "build", simg_path, f"docker://{image_name}"]

        if wrapper:
            args, log_args = wrapper.wrap_build_command(args, log=log)

        retcode = xrun(
            args[0],
            args[1:],
            shell=False,
            log=log,
            return_errcode=True,
            command_name="(singularity build)",
            gentle_ctrl_c=True,
            log_command=" ".join(log_args),
            log_result=True,
        )

        if retcode:
            raise BackendError(f"singularity build returns {retcode}")

        if not os.path.exists(simg_path):
            raise BackendError("singularity build did not return an error code, but the image did not appear")

        _rebuilt_images.add(simg_path)

    return simg_path


def run(
    cab: "stimela.kitchen.cab.Cab",
    params: Dict[str, Any],
    fqname: str,
    backend: "stimela.backend.StimelaBackendOptions",
    log: logging.Logger,
    subst: Optional[Dict[str, Any]] = None,
    wrapper: Optional["stimela.backends.runner.BackendWrapper"] = None,
):
    """Runs cab contents

    Args:
        cab (Cab): cab object
        log (logger): logger to use
        subst (Optional[Dict[str, Any]]): Substitution dict for commands etc., if any.

    Returns:
        Any: return value (e.g. exit code) of content
    """
    from .utils import resolve_container_binds

    native.update_rlimits(backend.rlimits, log)

    # get path to image, rebuilding if backend options allow this
    simg_path = build(cab, backend=backend, log=log, build=False, wrapper=wrapper)

    # build up command line
    cwd = os.getcwd()
    args = [backend.singularity.executable or BINARY, "exec", "--pwd", cwd]
    if backend.singularity.containall:
        args.append("--containall")
    else:
        if backend.singularity.contain:
            args.append("--contain")
        # --containall already implies --cleanenv, so this is only needed in the other branch.
        if backend.singularity.clean_env:
            args.append("--cleanenv")
    if backend.singularity.env:
        args += ["--env", ",".join([f"{k}={v}" for k, v in backend.singularity.env.items()])]

    with ExitStack() as exit_stack:
        # work out bind mounts (sorted so that parent directories come first)
        mounts, ephem_binds = resolve_container_binds(
            backend.singularity, "singularity", cab, params, exit_stack, log=log, wrapper=wrapper, cwd=cwd
        )
        for dest, src, rw in mounts:
            mode = "rw" if rw else "ro"
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
            return str(datetime.datetime.now() - (since or start_time)).split(".", 1)[0]

        if wrapper:
            args, log_args = wrapper.wrap_run_command(args, log_args, ephem_binds=ephem_binds, fqname=fqname, log=log)

        log.debug(f"command line is {' '.join(log_args)}")

        retcode = xrun(
            args[0],
            args[1:],
            shell=False,
            log=log,
            output_wrangler=cabstat.apply_wranglers,
            return_errcode=True,
            command_name=command_name,
            gentle_ctrl_c=True,
            log_command=" ".join(log_args),
            log_result=False,
        )

        # check if output marked it as a fail
        if cabstat.success is False:
            log.error(f"declaring '{command_name}' as failed based on its output")

        # if retcode != 0 and not explicitly marked as success, mark as failed
        if retcode and cabstat.success is not True:
            cabstat.declare_failure(f"{command_name} returns error code {retcode} after {elapsed()}")
        else:
            log.info(f"{command_name} returns exit code {retcode} after {elapsed()}")

        return cabstat
