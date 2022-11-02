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


def load(path: str, use_sources: Optional[List[DictConfig]] = [], name: Optional[str]=None,
            location: Optional[str]=None,
            global_deps: Optional[ConfigDependencies] = None,
            includes: bool=True, selfrefs: bool=True, include_path: str=None,
            use_cache: bool = True, verbose: bool = False):
    """Loads config file, using a previously loaded config to resolve _use references.

    Args:
        path (str): path to config file
        use_sources (Optional[List[DictConfig]]): list of existing configs to be used to resolve "_use" references,
                or None to disable
        name (Optional[str]): name of this config file, used for error messages
        location (Optional[str]): location where this config is being loaded (if not at root level)
        includes (bool, optional): If True (default), "_include" references will be processed
        selfrefs (bool, optional): If False, "_use" references will only be looked up in existing config.
            If True (default), they'll also be looked up within the loaded config.
        include_path (str, optional):
            if set, path to each config file will be included in the section as element 'include_path'

    Returns:
        Tuple of (conf, dependencies)
            conf (DictConfig): config object
            dependencies (ConfigDependencies): filenames that were _included
    """
    conf, dependencies = load_cache((path,), verbose=verbose) if use_cache else (None, None)

    if conf is None:
        subconf = OmegaConf.load(path)
        name = name or os.path.basename(path)
        dependencies = ConfigDependencies()
        dependencies.add(path)
        # include ourself into sources, if _use is in effect, and we've enabled selfrefs
        if use_sources is not None and selfrefs:
            use_sources = [subconf] + list(use_sources)
        conf, deps = resolve_config_refs(subconf, pathname=path, location=location, name=name,
                        includes=includes, use_sources=use_sources, use_cache=False, include_path=include_path)
        # update loaded dependencies with locations
        # deps.update_locations(conf, path)
        # update overall dependencies
        dependencies.update(deps)

        if use_cache:
            save_cache((path,), conf, dependencies, verbose=verbose)

    return conf, dependencies

def load_nested(filelist: List[str],
                structured: Optional[DictConfig] = None,
                typeinfo = None,
                use_sources: Optional[List[DictConfig]] = [],
                location: Optional[str] = None,
                nameattr: Union[Callable, str, None] = None,
                config_class: Optional[str] = None,
                include_path: Optional[str] = None,
                use_cache: bool = True,
                verbose: bool = False):
    """Builds nested configuration from a set of YAML files corresponding to sub-sections

    Parameters
    ----------
    conf : OmegaConf object
        root OmegaConf object to merge content into
    filelist : List[str]
        list of subsection config files to load
    schema : Optional[DictConfig]
        schema to be applied to each file, if any
    use_sources : Optional[List[DictConfig]]
        list of existing configs to be used to resolve "_use" references, or None to disable
    location : Optional[str]
        if set, contents of files are being loaded under 'location.subsection_name'. If not set, then 'subsection_name' is being
        loaded at root level. This is used for correctly formatting error messages and such.
    nameattr : Union[Callable, str, None]
        if None, subsection_name will be taken from the basename of the file. If set to a string such as 'name', will set
        subsection_name from that field in the subsection config. If callable, will be called with the subsection config object as a single
        argument, and must return the subsection name
    config_class : Optional[str]
        name of config dataclass to form (when using typeinfo), if None, then generated automatically
    include_path : Optional[str]
        if set, path to each config file will be included in the section as element 'include_path'

    Returns
    -------
        Tuple of (conf, dependencies)
            conf (DictConfig): config object
            dependencies (set): set of filenames that were _included

    Raises
    ------
    NameError
        If subsection name is not resolved
    """
    section_content, dependencies = load_cache(filelist, verbose=verbose) if use_cache else (None, None)

    if section_content is None:
        section_content = {} # OmegaConf.create()
        dependencies = ConfigDependencies()

        for path in filelist:
            # load file
            subconf, deps = load(path, location=location, use_sources=use_sources, include_path=include_path)
            dependencies.update(deps)
            if include_path:
                subconf[include_path] = path

            # figure out section name
            if nameattr is None:
                name = os.path.splitext(os.path.basename(path))[0]
            elif callable(nameattr):
                name = nameattr(subconf)
            elif nameattr in subconf:
                name = subconf.get(nameattr)
            else:
                raise NameError(f"{path} does not contain a '{nameattr}' field")

            # # resolve _use and _include statements
            # try:
            #     subconf = resolve_config_refs(subconf, f"{location}.{name}" if location else name, conf, subconf))
            # except (OmegaConfBaseException, YAMLError) as exc:
            #     raise ConfigurattError(f"config error in {path}: {exc}")

            # apply schema
            if structured is not None:
                try:
                    subconf = OmegaConf.merge(structured, subconf)
                except (OmegaConfBaseException, YAMLError) as exc:
                    raise ConfigurattError(f"schema error in {path}: {exc}")

            section_content[name] = subconf

        if structured is None and typeinfo is not None:
            if config_class is None:
                config_class = "ConfigClass_" + uuid.uuid4().hex
            fields = [(name, typeinfo) for name in section_content.keys()]
            datacls = make_dataclass(config_class, fields)
            # datacls.__module__ == __name__  # for pickling
            structured = OmegaConf.structured(datacls)
            section_content = OmegaConf.merge(structured, section_content)

        if use_cache:
            save_cache(filelist, section_content, dependencies, verbose=verbose)

    return section_content, dependencies


