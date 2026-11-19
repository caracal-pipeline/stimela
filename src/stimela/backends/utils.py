import logging
import os
import pathlib
from contextlib import ExitStack
from typing import Any, Callable, Dict, List, Optional, Tuple

from scabha.basetypes import MS, URI, Directory, File, get_filelikes
from scabha.exceptions import SchemaError

from stimela.exceptions import BackendError
from stimela.kitchen.cab import Parameter

from .binds import CustomTemporaryDirectory, ReadWrite


def resolve_container_binds(
    opts: Any,
    section: str,
    cab: "stimela.kitchen.cab.Cab",  # noqa: F821
    params: Dict[str, Any],
    exit_stack: ExitStack,
    log: logging.Logger,
    wrapper: Optional[Any] = None,
    cwd: Optional[str] = None,
    remover: Optional[Callable[[str], None]] = None,
) -> Tuple[List[Tuple[str, str, bool]], Dict[str, str]]:
    """Works out the set of bind mounts needed by a container backend.

    Args:
        opts: backend-specific options section. Must provide bind_dirs, ephemeral, bind_tmp and clean_tmp
            attributes (see SingularityBackendOptions, PodmanBackendOptions)
        section (str): name of the options section (used in error messages)
        cab: cab being run
        params: parameters of the cab
        exit_stack: temporary directories are entered into this context stack, so they will be cleaned up
            when the stack is closed
        log: logger
        wrapper: wrapper backend (e.g. slurm), if any. If set, ephemeral directories are not created here, but
            are rather stashed as "::EPHn::" placeholders for the wrapper to create
        cwd: current directory (always mounted read-write). Default is os.getcwd().
        remover: function used to remove temporary directories on cleanup. Default is shutil.rmtree.

    Returns:
        Tuple of mounts, ephem_binds. Mounts is a list of (container_path, host_path, readwrite) tuples, sorted so
        that parent directories come before subdirectories. ephem_binds is a dict of placeholder names to
        ephemeral storage locations, to be passed to the wrapper.
    """
    cwd = cwd or os.getcwd()
    # initial set of mounts has cwd as read-write
    mounts = {cwd: True}
    # dict of container paths to host paths
    container_to_host_path = {}

    # get dict of ephemeral base directories
    ephem_classes = dict(opts.ephemeral) if opts.ephemeral else {"tmp": "/tmp"}
    ephem_binds = {}

    # add extra binds
    for label, bind in opts.bind_dirs.items():
        # skip if conditional is False
        if not bind.conditional:
            log.info(f"bind_dirs.{label}: skipping based on conditional == {bind.conditional}")
            continue

        # expand ~ in paths
        src = os.path.expanduser(bind.host).rstrip("/")
        dest = os.path.expanduser(bind.target or src).rstrip("/")
        rw = bind.mode == ReadWrite.rw

        # handle binding of ephemeral dirs
        host = bind.host
        if host == "empty":
            if "tmp" not in ephem_classes:
                raise BackendError(
                    f"bind_dirs.{label}: deprecated 'host: empty' setting requires that a 'tmp' ephemeral storage "
                    f"class be defined"
                )
            host = "ephemeral:tmp"
        ephem_class = host.startswith("ephemeral:")
        if ephem_class or host == "ephemeral":
            if not bind.target:
                raise BackendError(f"bind_dirs.{label}: a target must be specified when host=ephemeral")
            target = os.path.expanduser(bind.target)
            # parse storage class specification, if given
            if ephem_class:
                ephem_class = host.split(":", 1)[1]
                if ephem_class not in ephem_classes:
                    raise BackendError(
                        f"bind_dirs.{label}: class '{ephem_class}' not present in '{section}.ephemeral' section"
                    )
                else:
                    ephem_target = ephem_classes[ephem_class]
            else:
                ephem_class, ephem_target = list(ephem_classes.items())[0]
            # create temporary directory, or else stash ephem binding for the wrapper
            if wrapper:
                src = f"EPH{len(ephem_binds)}"
                ephem_binds[src] = ephem_target
                src = f"::{src}::"
            else:
                tmpdir = CustomTemporaryDirectory(clean_up=opts.clean_tmp, dir=ephem_target, remover=remover)
                src = exit_stack.enter_context(tmpdir)
                log.info(f"bind_dirs.{label}: using temporary directory {src}")
            container_to_host_path[target] = src
            mounts[src] = rw
        # else real binding
        else:
            # resolve symlinks
            if os.path.exists(src) and os.path.realpath(src) != src:
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
            elif not os.path.exists(src):
                raise BackendError(f"bind_dirs.{label}: host path '{src}' does not exist")

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
    source_to_container_path = {src: dest for dest, src in container_to_host_path.items()}
    # make list of mounts
    mounts = [(source_to_container_path.get(src, src), src, rw) for src, rw in mounts.items()]
    # add implicit /tmp mount
    if opts.bind_tmp:
        for target, _, _ in mounts:
            if target == "/tmp":
                log.info("/tmp directory already bound, not adding an explicit binding")
                break
        else:
            if type(opts.bind_tmp) is str:
                tmp_class = opts.bind_tmp
            else:
                tmp_class = "tmp"
            tmp_target = ephem_classes.get(tmp_class, None)
            if tmp_target is None:
                raise BackendError(f"bind_tmp uses an undefined ephemeral storage class '{tmp_class}'")
            if wrapper:
                src = f"EPH{len(ephem_binds)}"
                ephem_binds[src] = tmp_target
                mounts.append(("/tmp", f"::{src}::", True))
            else:
                tmpdir = CustomTemporaryDirectory(clean_up=opts.clean_tmp, dir=tmp_target, remover=remover)
                tmpdir_name = exit_stack.enter_context(tmpdir)
                mounts.append(("/tmp", tmpdir_name, True))

    # sort mount paths -- this ensures that parent directories come first
    # (singularity doesn't like it if you specify a bind of a subdir before a bind of a parent)
    return sorted(mounts), ephem_binds


def resolve_required_mounts(
    mounts: Dict[str, bool],
    params: Dict[str, Any],
    inputs: Dict[str, Parameter],
    outputs: Dict[str, Parameter],
    remappings: Dict[str, str] = {},
):
    # helper function to accumulate list of target paths to be mounted
    def add_target(param_name, path, must_exist, readwrite):
        if not os.path.isdir(path):
            path = os.path.dirname(path)
        # if file doesn't exit, bind parent or throw error
        if not os.path.exists(path):
            if must_exist:
                raise SchemaError(f"parameter '{param_name}': path '{path}' does not exist")
            path = os.path.dirname(path)
        path = path.rstrip("/")
        # check for remapping -- this can happen if bind_dirs already mounts this container path to a different
        # host path, in which case we have a conflict
        if path in remappings:
            raise BackendError(f"{param_name}: {path} already bound to a different host path")
        # insert into mounts (with identical target path if not already present, and possibly upgrading RO to RW)
        mounts[path] = mounts.get(path, False) or readwrite

    # go through parameters and accumulate target paths
    for name, value in params.items():
        schema = inputs.get(name) or outputs.get(name)
        if schema is None:
            raise SchemaError(
                f"parameter {name} not in defined inputs or outputs for this cab. "
                f"This should have been caught by validation earlier!"
            )

        files = get_filelikes(schema._dtype, value)

        if not files:
            continue

        must_exist = schema.must_exist and name in inputs
        readwrite = schema.writable or name in outputs

        for path in files:
            uri = URI(path)
            if uri.remote:
                continue
            path = uri.path
            path = os.path.abspath(path).rstrip("/")
            realpath = os.path.abspath(os.path.realpath(path))
            add_target(name, realpath, must_exist=must_exist, readwrite=readwrite)
            add_target(name, path, must_exist=must_exist, readwrite=readwrite)
            # check if parent directory access is required
            if schema.path_policies.access_parent or schema.path_policies.write_parent:
                add_target(name, os.path.dirname(path), must_exist=True, readwrite=schema.path_policies.write_parent)
                add_target(
                    name, os.path.dirname(realpath), must_exist=True, readwrite=schema.path_policies.write_parent
                )

    # now, for any mount that has a symlink in the path, add the symlink target to mounts
    for path, readwrite in list(mounts.items()):
        if path.startswith("::"):
            continue
        while path != "/":
            if os.path.islink(path):
                chain = [path]
                while os.path.islink(path):
                    path = os.readlink(path)
                    # Check if the path is absolute; if not, resolve it relative to the directory of the previous link.
                    if not os.path.isabs(path):
                        path = os.path.abspath(os.path.join(os.path.dirname(chain[-1]), path))
                    chain.append(path)
                    add_target(name, path, must_exist=True, readwrite=readwrite)
            path = os.path.dirname(path)

    # now eliminate unnecessary mounts (those that have a parent mount with no lower read/write privileges)
    skip_targets = set()

    for path, readwrite in mounts.items():
        if path.startswith("::"):
            continue
        parent = os.path.dirname(path)
        while parent != "/":
            # if parent already mounted, and is as writeable as us, skip us
            if parent in mounts and mounts[parent] >= readwrite:
                skip_targets.add(path)
                break
            parent = os.path.dirname(parent)

    for path in skip_targets:
        mounts.pop(path)


def resolve_remote_mounts(
    params: Dict[str, Any],
    inputs: Dict[str, Parameter],
    outputs: Dict[str, Parameter],
    cwd: str = "/",
    mounts: set = set(),
):
    must_exist_list = set()
    mkdir_list = set()
    remove_if_exists_list = set()
    active_mounts = set()

    # go through parameters and accumulate target paths
    for name, value in params.items():
        schema = inputs.get(name) or outputs.get(name)
        if schema is None:
            raise SchemaError(
                f"parameter {name} not in defined inputs or outputs for this cab. "
                f"This should have been caught by validation earlier!"
            )

        dtype = schema._dtype
        if dtype in (File, Directory, MS):
            files = [value]
        elif dtype in (List[File], List[Directory], List[MS]):
            files = value
        else:
            continue

        # normalize paths, add CWD, and check that they refer to mounted volumes
        checked_files = []
        for path in files:
            if not os.path.isabs(path):
                path = os.path.join(cwd, path)
            path = os.path.normpath(path)
            # check that it is in mounts
            volumes = set(mount for mount in mounts if os.path.commonpath([path, mount]) == mount)
            if not volumes:
                raise BackendError(f"{name}={path} does not refer to mounted volume")
            active_mounts.update(volumes)
            checked_files.append(path)

        # add to lists of checks to be performed
        must_exist = schema.must_exist
        if must_exist is None:
            must_exist = name in inputs

        if must_exist and name in inputs:
            must_exist_list.update(checked_files)
        if schema.mkdir:
            mkdir_list.update([os.path.dirname(path) for path in checked_files])
        # parent directory must exist, in case of outputs
        elif name in outputs:
            must_exist_list.update([os.path.dirname(path) for path in checked_files])
        if schema.path_policies.remove_if_exists:
            remove_if_exists_list.update(checked_files)

    return must_exist_list, mkdir_list, remove_if_exists_list, active_mounts
