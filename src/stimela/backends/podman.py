"""Podman backend: runs cabs in OCI containers via podman.

Images are pulled from their registries straight into podman's local image store (no image files are kept
by Stimela itself). The container is run with --rm, so nothing is left behind. By default the container
shares the host's network and IPC namespaces, and (for rootless podman) runs under --userns=keep-id, so that
files written to bound directories are owned by the invoking user. This gives behaviour as close as possible to
the singularity backend.
"""

import datetime
import logging
import os
import shutil
import subprocess
from contextlib import ExitStack
from dataclasses import dataclass
from shutil import which
from typing import Any, Dict, List, Optional, Union

from omegaconf import OmegaConf
from scabha.basetypes import EmptyDictDefault, EmptyListDefault

import stimela
from stimela.exceptions import BackendError
from stimela.monitoring import local as local_monitoring
from stimela.utils.xrun_asyncio import xrun

from . import native
from .binds import BindDir, CustomTemporaryDirectory

# can only execute cabs that specify a container image
requires_container_image = True


@dataclass
class PodmanBackendOptions(object):
    BindDir = BindDir  # nested alias, for symmetry with SingularityBackendOptions

    enable: bool = True
    executable: Optional[str] = None  # path to podman binary. Default looks for "podman" in PATH.
    remote_only: bool = (
        False  # if True, won't look for podman on local system -- useful in combination with slurm wrapper
    )
    auto_pull: bool = True  # if True, pulls missing images automatically. If False, missing images are an error.
    repull: bool = False  # if True, re-pulls images (once per run) even if present locally, to pick up updates

    # --userns setting. "keep-id" maps the invoking user's UID/GID into the container, so that output files
    # are owned by the user. Only valid for rootless podman (ignored when running as root). Null for podman's
    # default.
    userns: Optional[str] = "keep-id"
    # --network setting. "host" shares the host network (as singularity does). Null for podman's default.
    network: Optional[str] = "host"
    # --ipc setting. "host" shares the host IPC namespace and /dev/shm (as singularity does). Podman's own default
    # gives a 64MB /dev/shm, which is too small for many multiprocessing/dask workloads. Null for podman's default.
    ipc: Optional[str] = "host"
    # if True, runs with --init, so that signals are forwarded and zombie processes are reaped
    init: bool = True
    # HOME directory inside the container. Default is /tmp (which is a fresh temporary directory if bind_tmp is
    # set), similar to singularity --contain. Null to leave it to podman. Ignored if env sets HOME explicitly.
    home: Optional[str] = "/tmp"
    # extra arguments to "podman run", e.g. ["--security-opt", "label=disable"] on SELinux systems,
    # or ["--device", "nvidia.com/gpu=all"] for GPUs
    run_args: List[str] = EmptyListDefault()

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


PodmanBackendSchema = OmegaConf.structured(PodmanBackendOptions)

STATUS = VERSION = BINARY = None

# images pulled in this run
_pulled_images = set()


def is_available(opts: Optional[PodmanBackendOptions] = None):
    global STATUS, VERSION, BINARY
    if STATUS is None:
        if opts and opts.remote_only:
            STATUS = VERSION = "remote"
        else:
            BINARY = (opts and opts.executable) or which("podman")
            if BINARY:
                try:
                    # output is "podman version X.Y.Z"
                    __version_string = subprocess.check_output([BINARY, "--version"]).decode("utf8")
                    STATUS = VERSION = __version_string.strip().split()[-1]
                except (OSError, subprocess.CalledProcessError) as exc:
                    STATUS = f"error running {BINARY} --version: {exc}"
                    VERSION = None
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


def _get_executable(backend: "stimela.backend.StimelaBackendOptions"):
    return backend.podman.executable or BINARY or "podman"


def _make_tmpdir_remover(backend: "stimela.backend.StimelaBackendOptions", log: logging.Logger):
    """Returns a function for removing temporary directories that were bound into a container.

    With rootless podman, the container runtime can leave files owned by subordinate UIDs in a bound directory.
    E.g. if /tmp is bound to a temporary directory, and the working directory is also under /tmp, then the
    runtime creates the mountpoint for the latter inside the former, as the container's root user. The invoking
    user can't remove these with plain rmtree, but can via "podman unshare". Failure to clean up is reported
    as a warning, since it shouldn't fail an otherwise successful step.
    """
    binary = _get_executable(backend)

    def remove(path: str):
        try:
            shutil.rmtree(path)
            return
        except PermissionError:
            pass
        if os.geteuid() != 0:
            try:
                subprocess.run([binary, "unshare", "rm", "-rf", path], capture_output=True)
            except OSError:
                pass
        if os.path.exists(path):
            log.warning(f"failed to remove temporary directory {path}")

    return remove


def get_image_name(cab: "stimela.kitchen.cab.Cab", backend: "stimela.backend.StimelaBackendOptions"):
    """Returns fully-qualified image name for cab"""
    # an image given only as a path (ImageInfo then fills in name from the path's basename) is a prebuilt
    # singularity image file, which podman can't use
    if cab.image and cab.image.path and cab.image.name == os.path.basename(cab.image.path):
        raise BackendError(
            f"cab '{cab.name}' specifies only a prebuilt image file ({cab.image.path}), which the podman backend "
            "cannot use"
        )
    image_name = cab.flavour.get_image_name(cab, backend)
    if not image_name:
        raise BackendError(f"cab '{cab.name}' does not define an image")
    return image_name


def image_exists(binary: str, image_name: str) -> bool:
    """Checks if image is present in the local podman store"""
    try:
        return subprocess.run([binary, "image", "exists", image_name], capture_output=True).returncode == 0
    except OSError as exc:
        raise BackendError(f"error running {binary} image exists", exc)


def build(
    cab: "stimela.kitchen.cab.Cab",
    backend: "stimela.backend.StimelaBackendOptions",
    log: logging.Logger,
    wrapper: Optional["stimela.backend.runner.BackendWrapper"] = None,
    build=True,
    rebuild=False,
):
    """Pulls image for cab into the podman image store, if necessary.

    build: if True, pull missing images regardless of backend settings
    rebuild: if True, re-pull images regardless of backend settings

    Returns:
        str: name of image
    """
    opts = backend.podman
    binary = _get_executable(backend)
    image_name = get_image_name(cab, backend)

    # this is True if we're asked to force a re-pull
    rebuild = rebuild or opts.repull
    # this is True if we're allowed to pull missing images (an explicit re-pull implies this)
    build = build or rebuild or opts.auto_pull

    if image_name in _pulled_images:
        log.info(f"podman image {image_name} was pulled earlier in this run")
        return image_name

    # can we inspect the image store that the image will be used from? Not if podman lives on remote nodes only,
    # or if the wrapper pulls remotely (each node then has its own store, and "podman run --pull" deals with it)
    local_store = not opts.remote_only and getattr(wrapper, "build_local", True)

    if local_store:
        if image_exists(binary, image_name):
            if not rebuild:
                log.info(f"podman image {image_name} exists")
                return image_name
            log.info(f"podman image {image_name} exists but a re-pull was specified")
        else:
            log.info(f"podman image {image_name} does not exist")
            if not build:
                raise BackendError("no image, and podman auto_pull not enabled")
    elif not build:
        return image_name

    args = log_args = [binary, "pull", image_name]
    if wrapper:
        args, log_args = wrapper.wrap_build_command(args, log=log)

    retcode = xrun(
        args[0],
        args[1:],
        shell=False,
        log=log,
        return_errcode=True,
        command_name="(podman pull)",
        gentle_ctrl_c=True,
        log_command=" ".join(log_args),
        log_result=True,
    )

    if retcode:
        raise BackendError(f"podman pull returns {retcode}")

    _pulled_images.add(image_name)

    return image_name


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

    opts = backend.podman

    native.update_rlimits(backend.rlimits, log)

    # make sure image is present (pulling if backend options allow this). When running via a wrapper,
    # the image is used from the remote node's store, so we rely on "podman run --pull" there instead.
    if wrapper is None and not opts.remote_only:
        image_name = build(cab, backend=backend, log=log, build=False)
    else:
        image_name = get_image_name(cab, backend)

    # build up command line
    cwd = os.getcwd()
    # Pull policy. Locally, build() above has already pulled (or re-pulled) the image as needed. Via a wrapper,
    # the image comes from the remote node's store, so a re-pull is requested as "newer", which pulls if the
    # registry has a different image than the node's cached one.
    if opts.repull and (wrapper is not None or opts.remote_only):
        pull_policy = "newer"
    else:
        pull_policy = "missing" if opts.auto_pull else "never"
    # note: scalar options use the "--opt=value" form, which podman parses unambiguously even for flags that
    # take optional values
    args = [_get_executable(backend), "run", "--rm", f"--pull={pull_policy}"]
    if opts.init:
        args.append("--init")
    if opts.userns:
        if opts.userns == "keep-id" and os.geteuid() == 0:
            log.debug("running as root, ignoring podman.userns=keep-id (valid for rootless podman only)")
        else:
            args.append(f"--userns={opts.userns}")
    if opts.network:
        args.append(f"--network={opts.network}")
    if opts.ipc:
        args.append(f"--ipc={opts.ipc}")
    args += ["--workdir", cwd]
    if opts.home and "HOME" not in opts.env:
        args += ["--env", f"HOME={opts.home}"]
    for name, value in opts.env.items():
        args += ["--env", f"{name}={value}"]
    args += list(opts.run_args)

    with ExitStack() as exit_stack:
        # work out bind mounts (sorted so that parent directories come first)
        mounts, ephem_binds = resolve_container_binds(
            opts,
            "podman",
            cab,
            params,
            exit_stack,
            log=log,
            wrapper=wrapper,
            cwd=cwd,
            remover=_make_tmpdir_remover(backend, log),
        )
        for dest, src, rw in mounts:
            mode = "rw" if rw else "ro"
            if src == dest:
                log.info(f"binding {src} as {mode}")
            else:
                log.info(f"binding {src} to {dest} as {mode}")
            args += ["--volume", f"{src}:{dest}:{mode}"]

        args1, log_args1 = cab.flavour.get_arguments(cab, params, subst, check_executable=False, log=log)
        if not args1:
            raise BackendError(f"cab '{cab.name}' produced an empty command line")

        # The cab command is run via --entrypoint, overriding any ENTRYPOINT the image may define. This mirrors
        # "singularity exec", which runs the given command directly rather than via the image's runscript.
        args += ["--entrypoint", args1[0]]
        log_args = args.copy()
        args += [image_name] + list(args1[1:])
        log_args += [image_name] + list(log_args1[1:])

        # Locally, have podman write the PID of conmon (which supervises the container processes) to a file, so
        # that the monitoring code can track the workload: conmon is daemonized, and not a descendant of Stimela.
        conmon_pidfile = None
        if wrapper is None:
            pidfile_dir = exit_stack.enter_context(CustomTemporaryDirectory())
            conmon_pidfile = os.path.join(pidfile_dir, "conmon.pid")
            args.insert(2, f"--conmon-pidfile={conmon_pidfile}")
            log_args.insert(2, f"--conmon-pidfile={conmon_pidfile}")

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

        if conmon_pidfile:
            local_monitoring.register_external_pidfile(conmon_pidfile)
        try:
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
        finally:
            if conmon_pidfile:
                local_monitoring.unregister_external_pidfile(conmon_pidfile)

        # check if output marked it as a fail
        if cabstat.success is False:
            log.error(f"declaring '{command_name}' as failed based on its output")

        # if retcode != 0 and not explicitly marked as success, mark as failed
        if retcode and cabstat.success is not True:
            cabstat.declare_failure(f"{command_name} returns error code {retcode} after {elapsed()}")
        else:
            log.info(f"{command_name} returns exit code {retcode} after {elapsed()}")

        return cabstat
