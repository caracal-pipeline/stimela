import os.path
import importlib
import hashlib
import datetime
import fnmatch
import subprocess
from shutil import which
from dataclasses import dataclass

from omegaconf.omegaconf import OmegaConf, DictConfig, ListConfig
from typing import Any, List, Dict, Optional, OrderedDict, Union, Callable

from .common import *

@dataclass
class FailRecord(object):
    filename: str
    origin: Optional[str] = None
    modulename: Optional[str] = None
    fname: Optional[str] = None
    warn: bool = True

@dataclass
class RequirementRecord(object):
    location: str
    requires: str
    filename: str
    optional: bool = False


class ConfigDependencies(object):
    _git_cache = {}

    def __init__(self):
        self.deps = OmegaConf.create()
        self.fails = OmegaConf.create()
        self._git = which("git")
        # self.provides = OmegaConf.create()
        # self.requires = OmegaConf.create()
    
    def add(self, filename: str, origin: Optional[str]=None, missing=False, **extra_attrs):
        """Adds a file to the set of dependencies

        Args:
            filename (str): filename
            origin (str or None): if not None, marks dependency as originating from another dependency
            missing (bool, optional): If True, marks depndency as missing. Defaults to False.
        """
        filename = os.path.abspath(filename)
        if filename in self.deps:
            return
        depinfo = OmegaConf.create()
        depinfo.update(**extra_attrs)
        if origin is not None:
            depinfo.origin = origin
        else:
            if missing or not os.path.exists(filename):
                depinfo.mtime     = 0 
                depinfo.mtime_str = "n/a"
                self.deps[filename] = depinfo
                return
            # get mtime and hash
            depinfo.mtime     = os.path.getmtime(filename) 
            depinfo.mtime_str = datetime.datetime.fromtimestamp(depinfo.mtime).strftime('%c')
            if not os.path.isdir(filename):
                depinfo.md5hash   = hashlib.md5(open(filename, "rb").read()).hexdigest()
            # add git info
            dirname = os.path.realpath(filename)
            if not os.path.isdir(dirname):
                dirname = os.path.dirname(dirname) 
            gitinfo = self._get_git_info(dirname)
            if gitinfo:
                depinfo.git = gitinfo
        self.deps[filename] = depinfo

    def add_fail(self, fail: FailRecord):
        self.fails[fail.filename] = OmegaConf.structured(fail)

    def replace(self, globs: List[str], dirname: str, **extra_attrs):
        remove = set()
        for glob in globs:
            remove.update(fnmatch.filter(self.deps, glob))
        if remove:
            for name in remove:
                self.deps[name] = OmegaConf.create(dict(origin=dirname))
        # add directory
        if dirname not in self.deps:
            self.add(dirname, **extra_attrs)

    def update(self, other):
        for name in other.deps:
            if name not in self.deps:
                self.deps[name] = other.deps[name]
        self.fails = OmegaConf.unsafe_merge(self.fails, other.fails)
        # self.provides = OmegaConf.unsafe_merge(self.provides, other.provides)
        # self.requires = OmegaConf.unsafe_merge(self.requires, other.requires)

    def save(self, filename):
        OmegaConf.save(self.deps, filename)

    def _get_git_info(self, dirname: str):
        """Returns git info structure for a directory, or None if not under git control

        Args:
            dirname (str): path

        Returns:
            DictConfig: directory info
        """
        # check cache first
        if dirname in self._git_cache:
            return self._git_cache[dirname]
        if not self._git:
            return None
        try:
            branches = subprocess.check_output("git -c color.ui=never branch -a -v -v".split(), 
                                                cwd=dirname, 
                                                stderr=subprocess.DEVNULL)
        except subprocess.CalledProcessError as exc:
            self._git_cache[dirname] = None
            return None
        # use git to get the info
        gitinfo = OmegaConf.create()
        for line in branches.decode().split("\n"):
            line = line.strip()
            if line.startswith("*"):
                gitinfo.branch = line[1:].strip().replace("${", "\\${")
                break
        # get description
        try:
            describe = subprocess.check_output("git describe --abbrev=16 --always --long --all".split(), cwd=dirname)
            gitinfo.describe = describe.decode().strip()
        except subprocess.CalledProcessError as exc:
            pass
        # get remote info
        try:
            remotes = subprocess.check_output("git remote -v".split(), cwd=dirname)
            gitinfo.remotes = remotes.decode().strip().split('\n')
        except subprocess.CalledProcessError as exc:
            pass

        self._git_cache[dirname] = gitinfo
        return gitinfo

    def get_description(self):
        desc = OrderedDict()
        for filename, attrs in self.deps.items():
            attrs_items = attrs.items() if attrs else [] 
            attrs_str = [f"mtime: {datetime.datetime.fromtimestamp(value).strftime('%c')}" 
                            if attr == "mtime" else f"{attr}: {value}"
                            for attr, value in attrs_items]
            desc[filename] = attrs_str
        return desc

    def have_deps_changed(self, mtime, verbose=False):
        # check that all dependencies are older than the cache
        for f in self.deps.keys():
            if not os.path.exists(f):
                if verbose:
                    print(f"Dependency {f} doesn't exist, forcing reload")
                return True
            if os.path.getmtime(f) > mtime:
                if verbose:
                    print(f"Dependency {f} is newer than the cache, forcing reload")
                return True
        # check that previously failing includes are not now succeeding (because that's also reason to reload cache)
        for filename, dep in self.fails.items():
            if dep.modulename:
                try:
                    mod = importlib.import_module(dep.modulename)
                    fname = os.path.join(os.path.dirname(mod.__file__), dep.fname)
                    if os.path.exists(fname):
                        return True
                except ImportError as exc:
                    pass
            elif not dep.missing_parent:
                if os.path.exists(filename):
                    return True
        return False

    # def add_provision_record(self, loc, filename):
    #     if loc not in self.provides:
    #         self.provides[loc] = []
    #     if filename not in self.provides[loc]:
    #         self.provides[loc].append(filename)

    # def scan_requirements(self, conf: DictConfig, location: Optional[str], filename: str):
    #     # build requirements map first using recursive helper
    #     def _scan(conf, loc, filename):
    #         if isinstance(conf, DictConfig):
    #             # add to requirements map, if this has requirements
    #             for optional, keyword in (False, "_requires"), (True, "_contingent"):
    #                 reqs = pop_conf(conf, keyword, [])
    #                 reqs = [reqs] if type(reqs) is str else reqs
    #                 # make list of unmet requirements
    #                 reqs = [req for req in reqs if (loc if req == "_base" else req) not in self.provides]
    #                 # save remaining unresolved reqs
    #                 if reqs:
    #                     if loc not in self.requires:
    #                         self.requires[loc] = OmegaConf.create()
    #                     for req in reqs:
    #                         reqloc = loc if req == "_base" else req
    #                         self.requires[loc][req] = RequirementRecord(loc, reqloc, filename, optional=optional)
    #             # If all resolved, add to provision record
    #             if loc not in self.requires and loc not in self.provides:
    #                 self.provides[loc] = filename
    #             # recurse into content
    #             for name, value in conf.items_ex(resolve=False):
    #                 _scan(value, f"{loc}.{name}" if loc else name, filename)

    #     _scan(conf, location or "", filename)

    # def check_requirements(self, conf: DictConfig, strict=True):
    #     """Checks remaining unmet requirements"""
    #     unmet = []
    #     optional = {}
    #     for loc, reqs in self.requires.items_ex(resolve=False):
    #         for name, req in reqs.items_ex(resolve=False):
    #             if req.requires not in self.provides:
    #                 if not strict or req.optional:
    #                     optional[loc, name] = req
    #                 else:
    #                     unmet.append(ConfigurattError(f"requirement '{name}' not met for section '{loc}' in {req.filename}"))
    #     if unmet:
    #         raise ConfigurattError("configuration has missing requirements", nested=unmet)

    #     for loc, _ in optional.keys():
    #         section = conf
    #         loc_elems = loc.split(".")
    #         try:
    #             for loc_elem in loc_elems[:-1]:
    #                 section = section[loc_elem]
    #             del section[loc_elem[-1]]
    #         except KeyError as exc:
    #             pass
        
    #     return optional
