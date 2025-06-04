import os.path
import uuid
from dataclasses import make_dataclass
from typing import Any, List, Dict, Optional, OrderedDict, Union, Callable
from omegaconf.omegaconf import OmegaConf, DictConfig, ListConfig
from omegaconf.errors import OmegaConfBaseException
from yaml.error import YAMLError

from scabha.exceptions import ScabhaBaseException
from .deps import ConfigDependencies
from .resolvers import resolve_config_refs
from .cache import load_cache, save_cache
from .common import *

def load(path: str, use_sources: Optional[List[DictConfig]] = [], name: Optional[str]=None,
        location: Optional[str]=None,
        includes: bool=True, selfrefs: bool=True, include_path: str=None,
        use_cache: bool = True,
        no_toplevel_cache = False,
        include_stack = [],
        verbose: bool = False):
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
        include_stack: list of paths which have been included. Used to catch recursive includes.
        include_path (str, optional):
            if set, path to each config file will be included in the section as element 'include_path'

    Returns:
        Tuple of (conf, dependencies)
            conf (DictConfig): config object
            dependencies (ConfigDependencies): filenames that were _included
    """
    use_toplevel_cache = use_cache and not no_toplevel_cache
    conf, dependencies = load_cache((path,), verbose=verbose) if use_toplevel_cache else (None, None)

    if conf is None:
        # create self:xxx resolver
        self_namespace = dict(
            path = path,
            dirname = os.path.dirname(path),
            basename = os.path.basename(path)
        )
        def self_namespace_resolver(arg):
            if arg in self_namespace:
                return self_namespace[arg]
            raise KeyError(f"invalid '${{self:arg}}' substitution in {path}")
        
        OmegaConf.register_new_resolver('self', self_namespace_resolver)
        try:
            subconf = OmegaConf.load(path)
            # force resolution of interpolations at this point (otherwise they happen lazily)
            resolved = OmegaConf.to_container(subconf, resolve=True)
            subconf = OmegaConf.create(resolved)
        finally:
            OmegaConf.clear_resolver('self')

        name = name or os.path.basename(path)
        dependencies = ConfigDependencies()
        dependencies.add(path)
        # include ourself into sources, if _use is in effect, and we've enabled selfrefs
        if use_sources is not None and selfrefs:
            use_sources = [subconf] + list(use_sources)
        conf, deps = resolve_config_refs(subconf, pathname=path, location=location, name=name,
                            includes=includes, use_cache=use_cache, use_sources=use_sources, include_path=include_path,
                            include_stack=include_stack + [path])
        # update overall dependencies
        dependencies.update(deps)

        # # check for missing requirements
        # dependencies.scan_requirements(conf, location, path)

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


def check_requirements(conf: DictConfig, bases: List[DictConfig], strict: bool = True):
    # build requirements map first using recursive helper
    def _check_item(section, name):
        """Helper function, checks if a dotted name x.y.z is in conf, or .x is in section"""
        name_elems = name.split(".")
        # if name starts with ".", look in current section, else check config and any bases
        if name_elems[0] == "":
            name_elems = name_elems[1:]
            configs = [section]
        else:
            configs = [conf] + bases
        # try all configs
        for config in configs:
            # start looking at this config's top level
            section = config
            for elem in name_elems:
                # break loop if not found at this level
                if elem not in section:
                    break
                try:
                    section = section[elem]
                except Exception as exc:
                    break
            # got to the end? Success
            else:
                return True
        # nothing found -- return False
        return False

    def _scan(section, location=""):
        # Short-circuit out if section is empty. Seems both DictConfig and ListConfig can be a kind of None
        # (funny OmegeConf API feature), where they're an instance of DictConfig or ListConfig, but don't support __getitem__.
        #
        if not section:
            return [], False

        delete_self = False
        to_delete = []
        unresolved = []
        if isinstance(section, DictConfig):
            # get requirements from section
            reqs = pop_conf(section, "_requires", [])
            contingents = pop_conf(section, "_contingent", [])
            if type(reqs) is str:
                reqs = [reqs]
            if type(contingents) is str:
                contingents = [contingents]
            if not isinstance(reqs, (list, ListConfig)):
                raise ConfigurattError(f"'{location or ''}._requires' must be a string or a sequence")
            if not isinstance(contingents, (list, ListConfig)):
                raise ConfigurattError(f"'{location or ''}._contingent' must be a string or a sequence")
            # check requirements and contingencies
            for req in reqs:
                if not _check_item(section, req):
                    unresolved.append((location, req, ConfigurattError(f"section '{location}' has missing requirement '{req}'")))
            for cont in contingents:
                if not _check_item(section, cont):
                    unresolved.append((location, cont, None))
                    delete_self = True
            # recurse into content
            for name, value in section.items_ex(resolve=False):
                unres, delete = _scan(value, location=f"{location}.{name}" if location else name)
                unresolved += unres
                if delete:
                    to_delete.append(name)
            # delete what needs to be deleted
            for name in to_delete:
                del section[name]
        # lists -- recurse into content
        elif isinstance(section, ListConfig):
            for i, value in enumerate(section._iter_ex(resolve=False)):
                unres, delete = _scan(value, location=f"{location or ''}[{i}]")
                unresolved += unres
                if delete:
                    to_delete.append(i)
            # delete what needs to be deleted
            for i in to_delete[::-1]:
                del section[i]
            delete_self = False
        return unresolved, delete_self
    # build list of unresolved items
    unresolved, delete = _scan(conf)

    # top-level config wants to be deleted? this is an error, it's not supposed to be contingent
    if delete:
        raise ConfigurattError("top-level configuration is contingent on missing sections")

    # in strict mode, throw any unresolved errors
    if strict:
        errors = [exc for (_, _, exc) in unresolved if exc is not None]
        if errors:
            raise ConfigurattError("configuration has missing requirements", nested=errors)

    return unresolved
