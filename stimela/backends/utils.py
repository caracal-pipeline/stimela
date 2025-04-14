import os

from typing import Dict, List, Any, Dict, Tuple
from stimela.kitchen.cab import Cab, Parameter
from scabha.exceptions import SchemaError
from stimela.exceptions import BackendError
from scabha.basetypes import File, Directory, MS, URI, get_filelikes

## commenting out for now -- will need to fix when we reactive the kube backend (and have tests for it)

def resolve_required_mounts(mounts: Dict[str, bool],
                            params: Dict[str, Any], 
                            inputs: Dict[str, Parameter], 
                            outputs: Dict[str, Parameter],
                            remappings: Dict[str, str] = {}
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
            raise SchemaError(f"parameter {name} not in defined inputs or outputs for this cab. This should have been caught by validation earlier!")

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
                add_target(name, os.path.dirname(path), must_exist=True, 
                           readwrite=schema.path_policies.write_parent)
                add_target(name, os.path.dirname(realpath), must_exist=True, 
                           readwrite=schema.path_policies.write_parent)
    
    # now, for any mount that has a symlink in the path, add the symlink target to mounts
    for path, readwrite in list(mounts.items()):
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
        parent = os.path.dirname(path)
        while parent != "/":
            # if parent already mounted, and is as writeable as us, skip us
            if parent in mounts and mounts[parent] >= readwrite:
                skip_targets.add(path)
                break
            parent = os.path.dirname(parent)

    for path in skip_targets:
        mounts.pop(path)

def resolve_remote_mounts(params: Dict[str, Any], 
                            inputs: Dict[str, Parameter], 
                            outputs: Dict[str, Parameter],
                            cwd: str = "/",
                            mounts: set = set()):

    must_exist_list = set()
    mkdir_list = set()
    remove_if_exists_list = set()
    active_mounts = set()

    # go through parameters and accumulate target paths
    for name, value in params.items():
        schema = inputs.get(name) or outputs.get(name)
        if schema is None:
            raise SchemaError(f"parameter {name} not in defined inputs or outputs for this cab. This should have been caught by validation earlier!")

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

        

        