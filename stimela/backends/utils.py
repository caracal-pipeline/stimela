import os

from typing import Dict, List, Any, Dict
from stimela.kitchen.cab import Cab, Parameter
from scabha.exceptions import SchemaError
from stimela.exceptions import BackendError
from scabha.basetypes import File, Directory, MS

## commenting out for now -- will need to fix when we reactive the kube backend (and have tests for it)

def resolve_required_mounts(params: Dict[str, Any], 
                            inputs: Dict[str, Parameter], 
                            outputs: Dict[str, Parameter],
                            prior_mounts: Dict[str, bool]):

    mkdirs = {}
    targets = {}

    # helper function to accumulate list of target paths to be mounted
    def add_target(path, must_exist, readwrite):
        if must_exist and not os.path.exists(path):
            raise SchemaError(f"{path} does not exist.")

        path = os.path.abspath(path)

        # if path doesn't exist, mount parent dir as read/write (file will be created in there)
        if not os.path.lexists(path):
            add_target(os.path.dirname(path), must_exist=True, readwrite=True)
        # else path is real
        else:
            # already mounted? Make sure readwrite is updated
            if path in targets:
                targets[path] = targets[path] or readwrite
            else:
                # not mounted, but is a link
                if os.path.islink(path):
                    # add destination as target
                    add_target(os.path.realpath(path), must_exist=must_exist, readwrite=readwrite)
                    # add parent dir as readonly target (to resolve the symlink)
                    add_target(os.path.dirname(path), must_exist=True, readwrite=False)
                # add to mounts
                else:
                    targets[path] = readwrite

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

        must_exist = schema.must_exist
        if must_exist is None:
            must_exist = name in inputs            
        readwrite = schema.writable or name in outputs

        for path in files:
            path = path.rstrip("/")
            # check parent access
            if schema.access_parent_dir or schema.write_parent_dir:
                add_target(os.path.dirname(path), must_exist=True, readwrite=schema.write_parent_dir)
            # for symlink targets, we need to mount the parent directory
            if os.path.islink(path):
                add_target(os.path.dirname(path), must_exist=True, readwrite=readwrite)
            else:
                add_target(path, must_exist=must_exist, readwrite=readwrite)

    
    # now eliminate unnecessary targets (those that have a parent mount with the same read/write property)
    skip_targets = set()

    for path, readwrite in targets.items():
        parent = os.path.dirname(path)
        while parent != "/":  
            # if parent already mounted, and is as writeable as us, skip us
            if (parent in targets and targets[parent] >= readwrite) or \
                (parent in prior_mounts and prior_mounts[parent] >= readwrite):
                skip_targets.add(path)
                break
            parent = os.path.dirname(parent)

    for path in skip_targets:
        targets.pop(path)

    return targets


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
        if schema.remove_if_exists:
            remove_if_exists_list.update(checked_files)

    return must_exist_list, mkdir_list, remove_if_exists_list, active_mounts

        

        