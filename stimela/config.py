import glob
import os, os.path, time, re, logging
from typing import Any, List, Dict, Optional, Union
from enum import Enum
from dataclasses import dataclass, field
from omegaconf.omegaconf import MISSING, OmegaConf
from omegaconf.errors import OmegaConfBaseException
from collections import OrderedDict

from yaml.error import YAMLError
import stimela
from stimela.exceptions import *

CONFIG_FILE = os.path.expanduser("~/.config/stimela.conf")

from scabha import configuratt
from scabha.cargo import ListOrString, EmptyDictDefault, EmptyListDefault, Parameter, Cab, CabManagement 


## schema for a stimela image

@dataclass
class ImageBuildInfo:
    info: Optional[str] = ""
    dockerfile: Optional[str] = "Dockerfile"
    production: Optional[bool] = True          # False can be used to mark test (non-production) images 


@dataclass
class StimelaImage:
    name: str = MISSING
    info: str = "image description"
    images: Dict[str, ImageBuildInfo] = MISSING
    path: str = ""          # path to image definition yaml file

    # optional library of common parameter sets
    params: Dict[str, Any] = EmptyDictDefault()

    # optional library of common management settings
    management: Dict[str, CabManagement] = EmptyDictDefault()


@dataclass 
class StimelaLogConfig(object):
    enable: bool = True                          
    name: str = "log-{info.fqname}.txt"          # Default name for log file. {fqname} and {config.x.y} is substituted.
    
    dir: str = "."                               # Default directory for log files

    symlink: Optional[str] = None                # Will make named symlink to the log directory. A useful pattern is e.g. dir="logs-{config.run.datetime}", symlink="logs",
                                                 # then each run has its own log dir, and "logs" always points to the latest one

    # how deep to nest individual log files. 0 means one log per recipe, 1 means one per step, 2 per each substep, etc. 
    nest: int = 999                             
    
    level: str = "INFO"                          # level at which we log



## overall Stimela config schema


import stimela.backends.docker
import stimela.backends.singularity
import stimela.backends.podman
import stimela.backends.native

Backend = Enum("Backend", "docker singularity podman native", module=__name__)

@dataclass
class StimelaOptions(object):
    backend: Backend = "native" #TODO(Sphe):: Maybe docker/singularity makes more sense
    registry: str = "quay.io"
    basename: str = "stimela/v2-"
    singularity_image_dir: str = "~/.singularity"
    log: StimelaLogConfig = StimelaLogConfig()
    ## For distributed computes and cpu allocation
    dist: Dict[str, Any] = EmptyDictDefault()  

@dataclass
class StimelaLibrary(object):
    params: Dict[str, Any] = EmptyDictDefault()
    recipes: Dict[str, Any] = EmptyDictDefault()
    steps: Dict[str, Any] = EmptyDictDefault()

def DefaultDirs():
    return field(default_factory=lambda:dict(indir='.', outdir='.'))

_CONFIG_BASENAME = "stimela.conf"
_STIMELA_CONFDIR = os.path.os.path.expanduser("~/.stimela")

# dict of config file locations to check, in order of preference
CONFIG_LOCATIONS = OrderedDict(
    local   = _CONFIG_BASENAME,
    venv    = os.environ.get('VIRTUAL_ENV', None) and os.path.join(os.environ['VIRTUAL_ENV'], _CONFIG_BASENAME),
    stimela = os.path.isdir(_STIMELA_CONFDIR) and os.path.join(_STIMELA_CONFDIR, _CONFIG_BASENAME),
    user    = os.path.join(os.path.os.path.expanduser("~/.config"), _CONFIG_BASENAME),
)

if 'VIRTUAL_ENV' in os.environ:
    configuratt.PATH.append(os.environ['VIRTUAL_ENV'])
if os.path.isdir(_STIMELA_CONFDIR):
    configuratt.PATH.append(_STIMELA_CONFDIR)
configuratt.PATH += os.environ.get("STIMELA_INCLUDE", '').split(':')

# set to the config file that was actually found
CONFIG_LOADED = None


def merge_extra_config(conf, newconf):
    from stimela import logger

    if 'cabs' in newconf:
        for cab in newconf.cabs:
            if cab in conf.cabs:
                logger().warning(f"changing definition of cab '{cab}'")
    return OmegaConf.merge(conf, newconf)


StimelaConfig = None

ConfigExceptionTypes = (configuratt.ConfigurattError, OmegaConfBaseException, YAMLError)

def get_config_class():
    return StimelaConfig

def load_config(extra_configs=List[str]):
    log = stimela.logger()

    stimela_dir = os.path.dirname(stimela.__file__)
    from stimela.kitchen.recipe import Recipe, Cab

    global StimelaConfig
    @dataclass 
    class StimelaConfig:
        base: Dict[str, StimelaImage] = EmptyDictDefault()
        lib: StimelaLibrary = StimelaLibrary()
        cabs: Dict[str, Cab] = MISSING
        opts: StimelaOptions = StimelaOptions()
        vars: Dict[str, Any] = EmptyDictDefault()
        run:  Dict[str, Any] = EmptyDictDefault()

    # start with empty structured config containing schema
    base_schema = OmegaConf.structured(StimelaImage) 
    cab_schema = OmegaConf.structured(Cab)
    opts_schema = OmegaConf.structured(StimelaOptions)

    conf = OmegaConf.structured(StimelaConfig)

    # merge base/*/*yaml files into the config, under base.imagename
    base_configs = glob.glob(f"{stimela_dir}/cargo/base/*/*.yaml")
    try:
        conf.base = configuratt.load_nested(base_configs, use_sources=[conf], structured=base_schema, nameattr='name', include_path='path', location='base')
    except ConfigExceptionTypes as exc:
        log.error(f"failed to build base configuration: {exc}")
        return None

    # merge base/*/*yaml files into the config, under base.imagename
    for path in glob.glob(f"{stimela_dir}/cargo/lib/params/*.yaml"):
        name = os.path.splitext(os.path.basename(path))[0]
        try:
            conf.lib.params[name] = OmegaConf.load(path)
        except ConfigExceptionTypes as exc:
            log.error(f"error loading {path}: {exc}")
            return None

    # merge all cab/*/*yaml files into the config, under cab.taskname
    cab_configs = glob.glob(f"{stimela_dir}/cargo/cab/*.yaml")
    try:
        conf.cabs = configuratt.load_nested(cab_configs, structured=cab_schema, nameattr='name', location='cabs', use_sources=[conf])
    except ConfigExceptionTypes as exc:
        log.error(f"failed to build cab configuration: {exc}")
        return None

    conf.opts = opts_schema

    def _load(conf, config_file):
        global CONFIG_LOADED
        log.info(f"loading config from {config_file}")
        try:
            newconf = configuratt.load_using(config_file, conf)
            conf = merge_extra_config(conf, newconf)
            if not CONFIG_LOADED:
                CONFIG_LOADED = config_file
        except ConfigExceptionTypes as exc:
            log.error(f"error reading {config_file}: {exc}")
        return conf

    # find standard config file to use
    if not any(path.startswith("=") for path in extra_configs):
        # merge global config into opts
        for _, config_file in CONFIG_LOCATIONS.items():
            if config_file and os.path.exists(config_file):
                conf = _load(conf, config_file)

    # add local configs
    for path in extra_configs:
        if path.startswith("="):
            path = path[1:]
        log.info("loading config from {path}")
        conf = _load(conf, config_file)

    if not CONFIG_LOADED:
        log.info("no configuration files, so using defaults")

    # add runtime info
    _ds = time.strftime("%Y%m%d")
    _ts = time.strftime("%H%M%S")
    runtime = dict(date=_ds, time=_ts, datetime=f"{_ds}-{_ts}")

    conf.run = OmegaConf.create(runtime)
    
    return conf

