import os.path
import importlib
import re
import fnmatch
from collections.abc import Sequence

from omegaconf.omegaconf import OmegaConf, DictConfig, ListConfig
from omegaconf.errors import OmegaConfBaseException
from typing import Any, List, Dict, Optional, OrderedDict, Union, Callable

from .common import *
from .deps import ConfigDependencies, FailRecord


def _lookup_nameseq(name_seq: List[str], source_dict: Dict):
    """Internal helper: looks up nested item ('a', 'b', 'c') in a nested dict

    Parameters
    ----------
    name_seq : List[str]
        sequence of keys to look up
    source_dict : Dict
        nested dict

    Returns
    -------
    Any
        value if found, else None
    """
    source = source_dict
    names = list(name_seq)
    while names:
        source = source.get(names.pop(0), None)
        if source is None:
            return None
    return source        


def _lookup_name(name: str, *sources: List[Dict]):
    """Internal helper: looks up a nested item ("a.b.c") in a list of dicts

    Parameters
    ----------
    name : str
        section name to look up, e.g. "a.b.c"

    Returns
    -------
    Any
        first matching item found

    Raises
    ------
    NameError
        if matching item is not found
    """
    name_seq = name.split(".")
    for source in sources:
        result = _lookup_nameseq(name_seq, source)
        if result is not None:
            return result
    raise ConfigurattError(f"unknown key {name}")


def _flatten_subsections(conf, depth: int = 1, sep: str = "__"):
    """Recursively flattens subsections in a DictConfig (modifying in place)
    A structure such as
        a:
            b: 1
            c: 2
    Becomes
        a__b: 1
        a__c: 2

    Args:
        conf (DictConfig): config to flatten
        depth (int):       depth to which to flatten. Default is 1 level.
        sep (str):         separator to use, default is "__"
    """
    subsections = [(key, value) for key, value in conf.items() if isinstance(value, DictConfig)]
    for name, subsection in subsections:
        pop_conf(conf, name)
        if depth > 1:
            _flatten_subsections(subsection, depth-1, sep)
        for key, value in subsection.items():
            conf[f"{name}{sep}{key}"] = value


def _scrub_subsections(conf: DictConfig, scrubs: Union[str, List[str]]):
    """
    Scrubs named subsections from a config.

    Args:
        conf (DictConfig): config to scrub
        scrubs (Union[str, List[str]]): sections to remove (can include dots to remove nested sections)
    """
    if isinstance(scrubs, str):
        scrubs = [scrubs]
    
    for scrub in scrubs:
        if '.' in scrub:
            name, remainder = scrub.split(".", 1)
        else:
            name, remainder = scrub, None
        # apply name as pattern
        is_pattern = '*' in name or '?' in name
        matches = fnmatch.filter(conf.keys(), name)
        if not matches:
            # if no matches to pattern, it's ok, otherwise raise error
            if is_pattern:
                return
            raise ConfigurattError(f"no entry matching '{name}'")
        # recurse into or remove matching entries
        for key in matches:
            if remainder:
                subconf = conf[key]
                if type(subconf) is DictConfig:
                    _scrub_subsections(subconf, remainder)
                elif not is_pattern:
                    raise ConfigurattError(f"'{name}' does not refer to a subsection")
            else:
                del conf[key]

def resolve_config_refs(conf, pathname: str, location: str, name: str, includes: bool, 
                        use_sources: Optional[List[DictConfig]],
                        use_cache = True, 
                        include_path: Optional[str]=None):
    """Resolves cross-references ("_use" and "_include" statements) in config object

    Parameters
    ----------
    conf : OmegaConf object
        input configuration object
    pathname : str
        full path to this config (directory component of that is used for _includes)
    location : str
        location of this configuration section, used for messages
    name : str
        name of this configuration file, used for messages
    includes : bool
        If True, "_include" references will be processed
    use_sources : optional list of OmegaConf objects
        one or more config object(s) in which to look up "_use" references. None to disable _use statements
    include_path (str, optional):
        if set, path to each config file will be included in the section as element 'include_path'

    Returns
    -------
    Tuple of (conf, dependencies)
    conf : OmegaConf object    
        This may be a new object if a _use key was resolved, or it may be the existing object
    dependencies : ConfigDependencies
        Set of filenames that were _included

    Raises
    ------
    ConfigurattError
        If a _use or _include directive is malformed
    """
    errloc = f"config error at {location or 'top level'} in {name}"
    dependencies = ConfigDependencies()
    # self-referencing enabled if first source is ourselves
    selfrefs =  use_sources and conf is use_sources[0]

    from scabha.configuratt import load, PATH

    if isinstance(conf, DictConfig):
        
        ## NB: perhaps have _use and _include take effect at the point they're inserted?
        ## also add an _all statement to insert a section into all section that follow
        # since _use and _include statements can be nested, keep on processing until all are resolved        
        updated = True
        recurse = 0
        flatten = pop_conf(conf, "_flatten", 0)
        flatten_sep = pop_conf(conf, "_flatten_sep", "__")
        scrub = pop_conf(conf, "_scrub", None)
        if isinstance(scrub, str):
            scrub = [scrub]
        
        while updated:
            updated = False
            # check for infinite recursion
            recurse += 1
            if recurse > 20:
                raise ConfigurattError(f"{errloc}: recursion limit exceeded, check your _use and _include statements")

            # handle _include entries
            if includes:
                include_directive = pop_conf(conf, "_include", None)
                if include_directive:
                    updated = True
                    include_files = []
                    # process includes recursively
                    def process_include_directive(directive: str, subpath=None):
                        if isinstance(directive, str):
                            include_files.append(directive if subpath is None else f"{subpath}/{directive}")
                        elif isinstance(directive, (tuple, list, ListConfig)):
                            for dir1 in directive:
                                process_include_directive(dir1, subpath)
                        elif isinstance(directive, DictConfig):
                            for key, value in directive.items_ex():
                                process_include_directive(value, subpath=key if subpath is None else f"{subpath}/{key}")
                        else:
                            raise ConfigurattError(f"{errloc}: _include contains invalid entry of type {type(directive)}")
                    process_include_directive(include_directive)

                    # load includes
                    accum_incl_conf = OmegaConf.create()
                    for incl in include_files:
                        if not incl:
                            raise ConfigurattError(f"{errloc}: empty _include specifier")
                        # check for [flags] at end of specifier
                        match = re.match("^(.*)\[(.*)\]$", incl)
                        if match:
                            incl = match.group(1)
                            flags = set([x.strip().lower() for x in match.group(2).split(",")])
                        else:
                            flags = {}

                        # check for (module)filename.yaml or (module)/filename.yaml style
                        match = re.match("^\\((.+)\\)/?(.+)$", incl)
                        if match:
                            modulename, filename = match.groups()
                            try:
                                mod = importlib.import_module(modulename)
                            except ImportError as exc:
                                if 'optional' in flags:
                                    dependencies.add_fail(FailRecord(incl, pathname, modulename=modulename, fname=filename))
                                    if 'warn' in flags:
                                        print(f"Warning: unable to import module for optional include {incl}")
                                    continue
                                raise ConfigurattError(f"{errloc}: _include {incl}: can't import {modulename} ({exc})")

                            filename = os.path.join(os.path.dirname(mod.__file__), filename)
                            if not os.path.exists(filename):
                                if 'optional' in flags:
                                    dependencies.add_fail(FailRecord(incl, pathname, modulename=modulename, fname=filename))
                                    if 'warn' in flags:
                                        print(f"Warning: unable to find optional include {incl}")
                                    continue
                                raise ConfigurattError(f"{errloc}: _include {incl}: {filename} does not exist")

                        # absolute path -- one candidate
                        elif os.path.isabs(incl):
                            if not os.path.exists(incl):
                                if 'optional' in flags:
                                    dependencies.add_fail(FailRecord(incl, pathname))
                                    if 'warn' in flags:
                                        print(f"Warning: unable to find optional include {incl}")
                                    continue
                                raise ConfigurattError(f"{errloc}: _include {incl} does not exist")
                            filename = incl
                        # relative path -- scan PATH for candidates
                        else:
                            paths = ['.', os.path.dirname(pathname)] + PATH
                            candidates = [os.path.join(p, incl) for p in paths] 
                            for filename in candidates:
                                if os.path.exists(filename):
                                    break
                            else:
                                if 'optional' in flags:
                                    dependencies.add_fail(FailRecord(incl, pathname))
                                    if 'warn' in flags:
                                        print(f"Warning: unable to find optional include {incl}")
                                    continue
                                raise ConfigurattError(f"{errloc}: _include {incl} not found in {':'.join(paths)}")

                        # load included file
                        incl_conf, deps = load(filename, location=location, 
                                            name=f"{filename}, included from {name}",
                                            includes=True, 
                                            use_cache=use_cache,
                                            use_sources=None)   # do not expand _use statements in included files, this is done below

                        dependencies.update(deps)
                        if include_path is not None:
                            incl_conf[include_path] = filename

                        # flatten structure
                        if flatten:
                            _flatten_subsections(incl_conf, flatten, flatten_sep)

                        # accumulate included config so that later includes override earlier ones
                        accum_incl_conf = OmegaConf.unsafe_merge(accum_incl_conf, incl_conf)

                    if scrub:
                        try:
                            _scrub_subsections(accum_incl_conf, scrub)
                        except ConfigurattError as exc:
                            raise ConfigurattError(f"{errloc}: error scrubbing {', '.join(scrub)}", exc)

                    # merge: our section overrides anything that has been included
                    conf = OmegaConf.unsafe_merge(accum_incl_conf, conf)
                    if selfrefs:
                        use_sources[0] = conf

            # handle _use entries
            if use_sources is not None:
                merge_sections = pop_conf(conf, "_use", None)
                if merge_sections:
                    updated = True
                    if type(merge_sections) is str:
                        merge_sections = [merge_sections]
                    elif not isinstance(merge_sections, Sequence):
                        raise TypeError(f"invalid {name}._use field of type {type(merge_sections)}")
                    if len(merge_sections):
                        # convert to actual sections
                        merge_sections = [_lookup_name(name, *use_sources) for name in merge_sections]
                        # merge them all together
                        base = merge_sections[0].copy()
                        base.merge_with(*merge_sections[1:])
                        # resolve references before flattening
                        base, deps = resolve_config_refs(base, pathname=pathname, name=name, 
                                                location=f"{location}._use" if location else "_use", 
                                                includes=includes, 
                                                use_sources=use_sources, use_cache=use_cache,
                                                include_path=include_path)
                        dependencies.update(deps)
                        if flatten:
                            _flatten_subsections(base, flatten, flatten_sep)
                        if scrub:
                            try:
                                _scrub_subsections(base, scrub)
                            except ConfigurattError as exc:
                                raise ConfigurattError(f"{errloc}: error scrubbing {', '.join(scrub)}", exc)

                        base.merge_with(conf)
                        conf = base
                        if selfrefs:
                            use_sources[0] = conf

        # recurse into content
        for key, value in conf.items_ex(resolve=False):
            if isinstance(value, (DictConfig, ListConfig)):
                value1, deps = resolve_config_refs(value, pathname=pathname, name=name, 
                                                location=f"{location}.{key}" if location else key, 
                                                includes=includes, 
                                                use_sources=use_sources, use_cache=use_cache,
                                                include_path=include_path)
                dependencies.update(deps)
                # reassigning is expensive, so only do it if there was an actual change 
                if value1 is not value:
                    conf[key] = value1
                    
    # recurse into lists
    elif isinstance(conf, ListConfig):
        # recurse in
        for i, value in enumerate(conf._iter_ex(resolve=False)):
            if isinstance(value, (DictConfig, ListConfig)):
                value1, deps = resolve_config_refs(value, pathname=pathname, name=name, 
                                                location=f"{location or ''}[{i}]", 
                                                includes=includes, 
                                                use_sources=use_sources, use_cache=use_cache,
                                                include_path=include_path)
                dependencies.update(deps)
                if value1 is not value:
                    conf[i] = value

    return conf, dependencies
