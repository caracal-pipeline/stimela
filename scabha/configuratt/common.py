import os
from omegaconf import DictConfig
from scabha.exceptions import ScabhaBaseException


class ConfigurattError(ScabhaBaseException):
    pass


# paths to search for _include statements
PATH = ['.']

# package version info stored with code dependencies
PACKAGE_VERSION = None

# extensions to look for implicitly, if not supplied
IMPLICIT_EXTENSIONS = (".yml", ".yaml")


# DictConfig doesn't support pop(), so here's a quick replacement
def pop_conf(conf: DictConfig, key: str, default=None):
    value = conf.get(key, default)
    if key in conf:
        del conf[key]
    return value
