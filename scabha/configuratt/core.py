import importlib
import os.path
import re
import uuid
from collections.abc import Sequence
from dataclasses import make_dataclass
from typing import Any, Callable, List, Optional, Union

from omegaconf.errors import OmegaConfBaseException
from omegaconf.omegaconf import DictConfig, ListConfig, OmegaConf
from yaml.error import YAMLError

from .cache import load_cache, save_cache
from .common import IMPLICIT_EXTENSIONS, PATH, ConfigurattError, pop_conf
from .deps import ConfigDependencies, FailRecord
from .helpers import _lookup_name, _scrub_subsections


def load(
    path: str,
    use_sources: Optional[List[DictConfig]] = [],
    name: Optional[str] = None,
    location: Optional[str] = None,
    includes: bool = True,
    selfrefs: bool = True,
    include_path: str = None,
    use_cache: bool = True,
    no_toplevel_cache=False,
    include_stack=[],
    verbose: bool = False,
):
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
        self_namespace = dict(path=path, dirname=os.path.dirname(path), basename=os.path.basename(path))

        def self_namespace_resolver(arg):
            if arg in self_namespace:
                return self_namespace[arg]
            raise KeyError(f"invalid '${{self:arg}}' substitution in {path}")

        OmegaConf.register_new_resolver("self", self_namespace_resolver)
        try:
            subconf = OmegaConf.load(path)
            # force resolution of interpolations at this point (otherwise they happen lazily)
            resolved = OmegaConf.to_container(subconf, resolve=True)
            subconf = OmegaConf.create(resolved)
        finally:
            OmegaConf.clear_resolver("self")

        name = name or os.path.basename(path)
        dependencies = ConfigDependencies()
        dependencies.add(path)
        # include ourself into sources, if _use is in effect, and we've enabled selfrefs
        if use_sources is not None and selfrefs:
            use_sources = [subconf] + list(use_sources)
        conf, deps = resolve_config_refs(
            subconf,
            pathname=path,
            location=location,
            name=name,
            includes=includes,
            use_cache=use_cache,
            use_sources=use_sources,
            include_path=include_path,
            include_stack=include_stack + [path],
        )
        # update overall dependencies
        dependencies.update(deps)

        # # check for missing requirements
        # dependencies.scan_requirements(conf, location, path)

        if use_cache:
            save_cache((path,), conf, dependencies, verbose=verbose)

    return conf, dependencies


def load_nested(
    filelist: List[str],
    structured: Optional[DictConfig] = None,
    typeinfo=None,
    use_sources: Optional[List[DictConfig]] = [],
    location: Optional[str] = None,
    nameattr: Union[Callable, str, None] = None,
    config_class: Optional[str] = None,
    include_path: Optional[str] = None,
    use_cache: bool = True,
    verbose: bool = False,
):
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
        if set, contents of files are being loaded under 'location.subsection_name'. If not set, then 'subsection_name'
        is being loaded at root level. This is used for correctly formatting error messages and such.
    nameattr : Union[Callable, str, None]
        if None, subsection_name will be taken from the basename of the file. If set to a string such as 'name', will
        set subsection_name from that field in the subsection config. If callable, will be called with the subsection
        config object as a single argument, and must return the subsection name
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
        section_content = {}  # OmegaConf.create()
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


def resolve_config_refs(
    conf,
    pathname: str,
    location: str,
    name: str,
    includes: bool,
    use_sources: Optional[List[DictConfig]],
    use_cache=True,
    include_path: Optional[str] = None,
    include_stack=[],
):
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
    selfrefs = use_sources and conf is use_sources[0]

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
                            process_include_directive(
                                include_files, keyword, value, subpath=key if subpath is None else f"{subpath}/{key}"
                            )
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
                        match = re.match(r"^(.*)\[(.*)\]$", incl)
                        if match:
                            incl = match.group(1)
                            flags = set([x.strip().lower() for x in match.group(2).split(",")])
                            warn = "warn" in flags
                            optional = "optional" in flags
                        else:
                            flags = {}
                            warn = optional = False

                        # helper function -- finds given include file (including trying an implicit .yml or .yaml
                        # extension) returns full name of file if found, else return None if include is optional,
                        # else adds fail record and raises exception. If opt=True, this is stronger than optional
                        # (no warnings raised)
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
                                        dependencies.add_fail(
                                            FailRecord(incl, pathname, modulename=modulename, fname=filename, warn=warn)
                                        )
                                        if warn:
                                            print(f"Warning: unable to import module for optional include {incl}")
                                        continue
                                    raise ConfigurattError(
                                        f"{errloc}: {keyword} {incl}: can't import {modulename} ({exc})"
                                    )
                                if mod.__file__ is not None:
                                    path = os.path.dirname(mod.__file__)
                                else:
                                    path = getattr(mod, "__path__", None)
                                    if path is None:
                                        if optional:
                                            dependencies.add_fail(
                                                FailRecord(
                                                    incl, pathname, modulename=modulename, fname=filename, warn=warn
                                                )
                                            )
                                            if warn:
                                                print(
                                                    f"Warning: unable to resolve path for optional include {incl}, "
                                                    f"does {modulename} contain __init__.py?"
                                                )
                                            continue
                                        raise ConfigurattError(
                                            f"{errloc}: {keyword} {incl}: can't resolve path for {modulename}, does "
                                            f"it contain __init__.py?"
                                        )
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
                            paths = [".", os.path.dirname(pathname)] + PATH
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
                        incl_conf, deps = load(
                            filename,
                            location=location,
                            name=f"{filename}, included from {name}",
                            includes=True,
                            include_stack=include_stack,
                            use_cache=use_cache,
                            use_sources=None,
                        )  # do not expand _use statements in included files, this is done below

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
                        base, deps = resolve_config_refs(
                            base,
                            pathname=pathname,
                            name=name,
                            location=f"{location}.{keyword}" if location else keyword,
                            includes=includes,
                            use_sources=use_sources,
                            use_cache=use_cache,
                            include_path=include_path,
                        )
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
                value1, deps = resolve_config_refs(
                    value,
                    pathname=pathname,
                    name=name,
                    location=f"{location}.{key}" if location else key,
                    includes=includes,
                    include_stack=include_stack,
                    use_sources=use_sources,
                    use_cache=use_cache,
                    include_path=include_path,
                )
                dependencies.update(deps)
                # reassigning is expensive, so only do it if there was an actual change
                if value1 is not value:
                    conf[key] = value1

    # recurse into lists
    elif isinstance(conf, ListConfig):
        # recurse in
        for i, value in enumerate(conf._iter_ex(resolve=False)):
            if isinstance(value, (DictConfig, ListConfig)):
                value1, deps = resolve_config_refs(
                    value,
                    pathname=pathname,
                    name=name,
                    location=f"{location or ''}[{i}]",
                    includes=includes,
                    include_stack=include_stack,
                    use_sources=use_sources,
                    use_cache=use_cache,
                    include_path=include_path,
                )
                dependencies.update(deps)
                if value1 is not value:
                    conf[i] = value

    return conf, dependencies
