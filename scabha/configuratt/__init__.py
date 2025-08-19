from typing import List

from omegaconf.omegaconf import DictConfig, ListConfig

from .cache import load_cache as load_cache
from .cache import save_cache as save_cache
from .common import PATH as PATH
from .common import ConfigurattError, pop_conf
from .core import load as load
from .core import load_nested as load_nested

# * as * used to re-export symbols and satisfy linter.
from .deps import ConfigDependencies as ConfigDependencies


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
                except Exception:
                    break
            # got to the end? Success
            else:
                return True
        # nothing found -- return False
        return False

    def _scan(section, location=""):
        # Short-circuit out if section is empty. Seems both DictConfig and ListConfig can be a kind of None (funny
        # OmegeConf API feature), where they're an instance of DictConfig or ListConfig, but don't support __getitem__.
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
                    unresolved.append(
                        (location, req, ConfigurattError(f"section '{location}' has missing requirement '{req}'"))
                    )
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
