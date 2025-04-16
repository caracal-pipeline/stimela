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
                        include_path: Optional[str]=None,
                        include_stack=[]):
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
    include_stack (list, optional):
        stack of files from which this one was included. Used to catch recursion.

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
        
        while updated:
            updated = False
            # check for infinite recursion
            recurse += 1
            if recurse > 20:
                raise ConfigurattError(f"{errloc}: recursion limit exceeded, check your _use and _include statements")
            # handle _include/_include_post entries
            if includes:
                # helper function: process includes recursively
                def process_include_directive(include_files: List[str], keyword: str, directive: Any, subpath=None):
                    if isinstance(directive, str):
                        include_files.append(directive if subpath is None else f"{subpath}/{directive}")
                    elif isinstance(directive, (tuple, list, ListConfig)):
                        for dir1 in directive:
                            process_include_directive(include_files, keyword, dir1, subpath)
                    elif isinstance(directive, DictConfig):
                        for key, value in directive.items_ex():
                            process_include_directive(include_files, keyword, value, subpath=key if subpath is None else f"{subpath}/{key}")
                    else:
                        raise ConfigurattError(f"{errloc}: {keyword} contains invalid entry of type {type(directive)}")
                
                # helper function: load list of _include or _include_post files, returns accumulated DictConfig
                def load_include_files(keyword):
                    # pop include directive, return if None
                    include_directive = pop_conf(conf, keyword, None)
                    if include_directive is None:
                        return None
                    # get corresponding _scrub or _scrub_post directive
                    scrub = pop_conf(conf, keyword.replace("include", "scrub"), None)
                    if isinstance(scrub, str):
                        scrub = [scrub]

                    include_files = []
                    process_include_directive(include_files, keyword, include_directive)

                    accum_incl_conf = OmegaConf.create()

                    # load includes
                    for incl in include_files:
                        if not incl:
                            raise ConfigurattError(f"{errloc}: empty {keyword} specifier")
                        # check for [flags] at end of specifier
                        match = re.match(r'^(.*)\[(.*)\]$', incl)
                        if match:
                            incl = match.group(1)
                            flags = set([x.strip().lower() for x in match.group(2).split(",")])
                            warn = 'warn' in flags
                            optional = 'optional' in flags
                        else:
                            flags = {}
                            warn = optional = False

                        # helper function -- finds given include file (including trying an implicit .yml or .yaml extension)
                        # returns full name of file if found, else return None if include is optional, else 
                        # adds fail record and raises exception.
                        # If opt=True, this is stronger than optional (no warnings raised)
                        def find_include_file(path: str, opt: bool = False):
                            # if path already has an extension, only try the pathname itself
                            if os.path.splitext(path)[1]:
                                paths = [path]
                            # else try the pathname itself, plus implicit extensions
                            else:
                                paths = [path] + [path + ext for ext in IMPLICIT_EXTENSIONS]
                            # now try all of them and return a matching one if found
                            for path in paths:
                                if os.path.isfile(path):
                                    return path
                            # end of loop with no matching files? Raise error
                            else:
                                if opt:
                                    return None
                                elif optional:
                                    dependencies.add_fail(FailRecord(path, pathname, warn=warn))
                                    if warn:
                                        print(f"Warning: unable to find optional include {path}")
                                    return None
                                raise ConfigurattError(f"{errloc}: {keyword} {path} does not exist")

                        # check for (location)filename.yaml or (location)/filename.yaml style
                        match = re.match(r"^\((.+)\)/?(.+)$", incl)
                        if match:
                            modulename, filename = match.groups()
                            if modulename.startswith("."):
                                filename = os.path.join(os.path.dirname(pathname), modulename, filename)
                                filename = find_include_file(filename)
                                if filename is None:
                                    continue
                            else:
                                try:
                                    mod = importlib.import_module(modulename)
                                except ImportError as exc:
                                    if optional:
                                        dependencies.add_fail(FailRecord(incl, pathname, modulename=modulename, 
                                                                         fname=filename, warn=warn))
                                        if warn:
                                            print(f"Warning: unable to import module for optional include {incl}")
                                        continue
                                    raise ConfigurattError(f"{errloc}: {keyword} {incl}: can't import {modulename} ({exc})")
                                if mod.__file__ is not None:
                                    path = os.path.dirname(mod.__file__)
                                else:
                                    path = getattr(mod, '__path__', None)
                                    if path is None:
                                        if optional:
                                            dependencies.add_fail(FailRecord(incl, pathname, modulename=modulename, 
                                                                             fname=filename, warn=warn))
                                            if warn:
                                                print(f"Warning: unable to resolve path for optional include {incl}, does {modulename} contain __init__.py?")
                                            continue
                                        raise ConfigurattError(f"{errloc}: {keyword} {incl}: can't resolve path for {modulename}, does it contain __init__.py?")
                                    path = path[0]

                                filename = find_include_file(os.path.join(path, filename))
                                if filename is None:
                                    continue
                        # absolute path -- one candidate
                        elif os.path.isabs(incl):
                            filename = find_include_file(incl)
                            if filename is None:
                                continue
                        # relative path -- scan PATH for candidates
                        else:
                            paths = ['.', os.path.dirname(pathname)] + PATH
                            candidates = [os.path.join(p, incl) for p in paths] 
                            for filename in candidates:
                                filename = find_include_file(filename, opt=True)
                                if filename is not None:
                                    break
                            # none found in candidates -- process error
                            else:
                                if optional:
                                    dependencies.add_fail(FailRecord(incl, pathname, warn=warn))
                                    if warn:
                                        print(f"Warning: unable to find optional include {incl}")
                                    continue
                                raise ConfigurattError(f"{errloc}: {keyword} {incl} not found in {':'.join(paths)}")

                        # check for recursion
                        for path in include_stack:
                            if os.path.samefile(path, filename):
                                raise ConfigurattError(f"{errloc}: {filename} is included recursively")
                        # load included file
                        incl_conf, deps = load(filename, location=location, 
                                            name=f"{filename}, included from {name}",
                                            includes=True, 
                                            include_stack=include_stack,
                                            use_cache=use_cache,
                                            use_sources=None)   # do not expand _use statements in included files, this is done below

                        dependencies.update(deps)
                        if include_path is not None:
                            incl_conf[include_path] = filename

                        # accumulate included config so that later includes override earlier ones
                        accum_incl_conf = OmegaConf.unsafe_merge(accum_incl_conf, incl_conf)

                    if scrub:
                        try:
                            _scrub_subsections(accum_incl_conf, scrub)
                        except ConfigurattError as exc:
                            raise ConfigurattError(f"{errloc}: error scrubbing {', '.join(scrub)}", exc)
                    
                    return accum_incl_conf

                accum_pre = load_include_files("_include")
                accum_post = load_include_files("_include_post")

                # merge: our section overrides anything that has been included
                conf = OmegaConf.unsafe_merge(accum_pre or {}, conf, accum_post or {})
                if accum_pre or accum_post:
                    updated = True
                if selfrefs:
                    use_sources[0] = conf

            # handle _use entries
            if use_sources is not None:
                def load_use_sections(keyword):
                    merge_sections = pop_conf(conf, keyword, None)
                    if merge_sections is None:
                        return None
                    scrub = pop_conf(conf, keyword.replace("use", "scrub"), None)
                    if type(merge_sections) is str:
                        merge_sections = [merge_sections]
                    elif not isinstance(merge_sections, Sequence):
                        raise TypeError(f"invalid {name}.{keyword} directive of type {type(merge_sections)}")
                    if len(merge_sections):
                        # convert to actual sections
                        merge_sections = [_lookup_name(name, *use_sources) for name in merge_sections]
                        # merge them all together
                        base = merge_sections[0].copy()
                        base.merge_with(*merge_sections[1:])
                        # resolve references before flattening
                        base, deps = resolve_config_refs(base, pathname=pathname, name=name, 
                                                location=f"{location}.{keyword}" if location else keyword, 
                                                includes=includes, 
                                                use_sources=use_sources, use_cache=use_cache,
                                                include_path=include_path)
                        dependencies.update(deps)
                        if scrub:
                            try:
                                _scrub_subsections(base, scrub)
                            except ConfigurattError as exc:
                                raise ConfigurattError(f"{errloc}: error scrubbing {', '.join(scrub)}", exc)
                        return base
                    return None
                
                base = load_use_sections("_use")
                if base is not None:
                    base.merge_with(conf)
                    conf = base
                post = load_use_sections("_use_post")
                if post is not None:
                    conf.merge_with(post)
                
                if selfrefs:
                    use_sources[0] = conf

        # recurse into content
        for key, value in conf.items_ex(resolve=False):
            if isinstance(value, (DictConfig, ListConfig)):
                value1, deps = resolve_config_refs(value, pathname=pathname, name=name, 
                                                location=f"{location}.{key}" if location else key, 
                                                includes=includes, 
                                                include_stack=include_stack,
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
                                                include_stack=include_stack,
                                                use_sources=use_sources, use_cache=use_cache,
                                                include_path=include_path)
                dependencies.update(deps)
                if value1 is not value:
                    conf[i] = value

    return conf, dependencies
