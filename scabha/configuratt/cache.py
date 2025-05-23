import os.path
import sys
import glob
import hashlib
import pathlib
import dill as pickle

import uuid
from dataclasses import make_dataclass

from omegaconf.omegaconf import OmegaConf, DictConfig, ListConfig
from omegaconf.errors import OmegaConfBaseException
from typing import Any, List, Dict, Optional, OrderedDict, Union, Callable

from scabha.exceptions import ScabhaBaseException

from yaml.error import YAMLError
from .deps import ConfigDependencies
from .resolvers import resolve_config_refs
from .common import *

# path for cache
CACHEDIR = os.environ.get("CONFIGURATT_CACHE_DIR") or os.path.expanduser(
    "~/.cache/configuratt")


def _compute_hash(filelist, extra_keys):
    filelist = list(filelist) + list(extra_keys)
    return hashlib.md5(" ".join(filelist).encode()).hexdigest()


def set_cache_dir(cachedir: str):
    global CACHEDIR
    CACHEDIR = cachedir


def clear_cache(log=None):
    if os.path.isdir(CACHEDIR):
        files = glob.glob(f"{CACHEDIR}/*")
        log and log.info(f"clearing {len(files)} cached config(s) from cache")
    else:
        files = []
        log and log.info(f"no configs in cache")
    for filename in files:
        try:
            os.unlink(filename)
        except Exception as exc:
            log and log.error(f"failed to remove cached config {filename}: {exc}")
            sys.exit(1)

def load_cache(filelist: List[str], extra_keys=[], verbose=None):
    filehash = _compute_hash(filelist, extra_keys)
    if not os.path.isdir(CACHEDIR):
        if verbose:
            print(f"{CACHEDIR} does not exist")
        return None, None
    filename = os.path.join(CACHEDIR, filehash)
    if not os.path.exists(filename):
        if verbose:
            print(f"hash file {filename} does not exist")
        return None, None
    # check that all configs are older than the cache
    cache_mtime = os.path.getmtime(filename)
    for f in filelist:
        if os.path.getmtime(f) > cache_mtime:
            if verbose:
                print(f"Config {f} is newer than the cache, forcing reload")
            return None, None
    # load cache
    try:
        conf, deps = pickle.load(open(filename, 'rb'))
        if not isinstance(deps, ConfigDependencies):
            raise TypeError(f"cached deps object is of type {type(deps)}, expecting ConfigDependencies")
    except Exception as exc:
        print(f"Error loading cached config from {filename}: {exc}. Removing the cache.")
        os.unlink(filename)
        return None, None
    # check that all dependencies are older than the cache
    if deps.have_deps_changed(cache_mtime, verbose=verbose):
        return None, None
    if verbose:
        print(f"Loaded cached config for {' '.join(filelist)} from {filename}")
    return conf, deps


def save_cache(filelist: List[str], conf, deps: ConfigDependencies, extra_keys=[], verbose=False):
    pathlib.Path(CACHEDIR).mkdir(parents=True, exist_ok=True)
    filelist = list(filelist)   # add self to dependencies
    filehash = _compute_hash(filelist, extra_keys)
    filename = os.path.join(CACHEDIR, filehash)
    # add ourselves to dependencies, so that cache is cleared if implementation changes
    deps.add(__file__, version=PACKAGE_VERSION)
    pickle.dump((conf, deps), open(filename, "wb"), 2)
    if verbose:
        print(f"Caching config for {' '.join(filelist)} as {filename}")




