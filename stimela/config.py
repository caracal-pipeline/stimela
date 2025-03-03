import importlib
import os, os.path, time, platform, traceback
from typing import Any, List, Dict, Optional
from dataclasses import dataclass, field
from omegaconf.omegaconf import OmegaConf
from omegaconf.errors import OmegaConfBaseException
from collections import OrderedDict
import psutil

from yaml.error import YAMLError
import stimela
from stimela.exceptions import *
from stimela import log_exception

from scabha import configuratt
from scabha.basetypes import EmptyDictDefault, EmptyListDefault, EmptyClassDefault
from stimela.backends import StimelaBackendOptions

@dataclass 
class StimelaLogConfig(object):
    enable: bool = True                          
    name: str = "log-{info.fqname}"          # Default name for log file. info dict and {config.x.y} is substituted.
    ext: str = ".txt"                        # Default extension for log file.
    dir: str = "."                               # Default directory for log files

    symlink: Optional[str] = None                # Will make named symlink to the log directory. A useful pattern is e.g. dir="logs-{config.run.datetime}", symlink="logs",
                                            # then each run has its own log dir, and "logs" always points to the latest one

    # how deep to nest individual log files. 0 means one log per recipe, 1 means one per step, 2 per each substep, etc. 
    nest: int = 999                             
    
    level: str = "INFO"                          # level at which we log
    

## overall Stimela config schema

import stimela.backends


@dataclass
class StimelaProfilingOptions(object):
    print_depth: int = 9999
    unroll_loops: bool = False

@dataclass
class StimelaDisableSkipOptions(object):
    fresh: bool = False
    exist: bool = False

@dataclass
class StimelaOptions(object):
    backend: StimelaBackendOptions = EmptyClassDefault(StimelaBackendOptions)
    log: StimelaLogConfig = EmptyClassDefault(StimelaLogConfig)
    ## list of paths to search with _include
    include: List[str] = EmptyListDefault()
    ## Miscellaneous runtime options (runtime.casa, etc.)     
    runtime: Dict[str, Any] = EmptyDictDefault()    
    ## Profiling options
    profile: StimelaProfilingOptions = EmptyClassDefault(StimelaProfilingOptions)
    ## Disables skip_if_outputs checks
    disable_skips: StimelaDisableSkipOptions = EmptyClassDefault(StimelaDisableSkipOptions)

def DefaultDirs():
    return field(default_factory=lambda:dict(indir='.', outdir='.'))

_CONFIG_BASENAME = "stimela.conf"
_STIMELA_CONFDIR = os.path.os.path.expanduser("~/.stimela")

# check for cultcargo module
try:
    ccmod = importlib.import_module("cultcargo")
except ImportError:
    ccmod = None

# dict of config file locations to check, in order of preference
CONFIG_LOCATIONS = OrderedDict(
    package = os.path.join(os.path.dirname(__file__), _CONFIG_BASENAME),
    local   = _CONFIG_BASENAME,
    venv    = os.environ.get('VIRTUAL_ENV', None) and os.path.join(os.environ['VIRTUAL_ENV'], _CONFIG_BASENAME),
    stimela = os.path.isdir(_STIMELA_CONFDIR) and os.path.join(_STIMELA_CONFDIR, _CONFIG_BASENAME),
    cultcargo = ccmod and os.path.join(os.path.dirname(ccmod.__file__), _CONFIG_BASENAME),
    user    = os.path.join(os.path.expanduser("~/.config"), _CONFIG_BASENAME),
)

# set to the config file that was actually found
CONFIG_LOADED = None

# set to the set of config dependencies
CONFIG_DEPS = None

# stimela base directory
STIMELA_DIR = os.path.dirname(stimela.__file__)


def merge_extra_config(conf, newconf):
    from stimela import logger

    if 'cabs' in newconf:
        for cab in newconf.cabs:
            if cab in conf.cabs:
                logger().warning(f"changing definition of cab '{cab}'")
    return OmegaConf.unsafe_merge(conf, newconf)


StimelaConfigSchema = None

ConfigExceptionTypes = (configuratt.ConfigurattError, OmegaConfBaseException, YAMLError)

def get_initial_deps():
    dependencies = configuratt.ConfigDependencies()

    # add ourselves to dependencies
    dependencies.add(STIMELA_DIR, version=stimela.__version__)              
    dependencies.add(__file__, origin=STIMELA_DIR)              
    dependencies.add(configuratt.__file__, origin=STIMELA_DIR)  

    return dependencies


def load_config(extra_configs: List[str], extra_dotlist: List[str] = [], include_paths: List[str] = [],
                verbose: bool = False, use_sys_config: bool = True):

    # # disable OmegaConf resolvers
    # for name in "oc.create", "oc.decode", "oc.deprecated", "oc.env", "oc.select", "oc.dict.keys", "oc.dict.values":
    #     print(f"clearing {name}")
    #     print(OmegaConf.clear_resolver(name))

    log = stimela.logger()

    configuratt.common.PACKAGE_VERSION = f"stimela=={stimela.__version__}"
    # set up include paths

    # stadard system paths
    configuratt.PATH[0:0] = [os.path.expanduser("~/lib/stimela"), "/usr/lib/stimela", "/usr/local/lib/stimela"]
    if 'VIRTUAL_ENV' in os.environ:
        configuratt.PATH.insert(0, os.environ['VIRTUAL_ENV'])
        configuratt.PATH.insert(0, os.path.join(os.environ['VIRTUAL_ENV'], "lib/stimela"))
    if os.path.isdir(_STIMELA_CONFDIR):
        configuratt.PATH.insert(0, _STIMELA_CONFDIR)

    # add paths from command line and environment variable
    paths = [os.path.expanduser(path) for path in include_paths]
    envpaths = os.environ.get("STIMELA_INCLUDE")
    if envpaths:
        paths += envpaths.split(':')

    if paths:
        log.info(f"added include paths: {' '.join(paths)}")
        configuratt.PATH[0:0] = paths

    if verbose:
        log.info(f"include paths are {':'.join(configuratt.PATH)}")

    extra_cache_keys = list(extra_dotlist) + configuratt.PATH

    STIMELA_DIR = os.path.dirname(stimela.__file__)
    from stimela.kitchen.cab import Cab, ImageInfo

    global StimelaConfigSchema, StimelaLibrary, StimelaConfig
    @dataclass
    class StimelaLibrary(object):
        params: Dict[str, Any] = EmptyDictDefault()
        recipes: Dict[str, Any] = EmptyDictDefault()
        steps: Dict[str, Any] = EmptyDictDefault()
        misc: Dict[str, Any] = EmptyDictDefault()
        wisdom: Dict[str, Any] = EmptyDictDefault()
        

    @dataclass 
    class StimelaConfig:
        images: Dict[str, ImageInfo] = EmptyDictDefault()
        lib: StimelaLibrary = EmptyClassDefault(StimelaLibrary)
        cabs: Dict[str, Cab] = EmptyDictDefault()
        opts: StimelaOptions = EmptyClassDefault(StimelaOptions)
        vars: Dict[str, Any] = EmptyDictDefault()
        run:  Dict[str, Any] = EmptyDictDefault()
        

    base_configs = lib_configs = cab_configs = []

    if use_sys_config:
        sys_configs = [config_file for config_file in CONFIG_LOCATIONS.values()
                        if config_file and os.path.exists(config_file)]
    else:
        sys_configs = []

    all_configs = base_configs + lib_configs + cab_configs + sys_configs + list(extra_configs)

    conf, dependencies = configuratt.load_cache(all_configs, extra_keys=extra_cache_keys, verbose=verbose) 

    if conf is not None:
        log.info("loaded full configuration from cache")
    else:
        log.info("loading configuration")
        dependencies = get_initial_deps()

        # start with empty structured config containing schema
        cab_schema = OmegaConf.structured(Cab)
        opts_schema = OmegaConf.structured(StimelaOptions)

        StimelaConfigSchema = OmegaConf.structured(StimelaConfig)

        conf = StimelaConfigSchema.copy()

        # merge lib/params/*yaml files into the config
        try:
            conf.lib.params, deps = configuratt.load_nested(lib_configs,  use_sources=[conf], 
                                                                location='lib.params', include_path='_path') 
            dependencies.update(deps)
        except Exception as exc:
            if verbose:
                traceback.print_exc()
            log_exception(ConfigError("error loading lib.params configuration", exc))
            return None

        # merge all cab/*/*yaml files into the config, under cab.taskname
        try:
            conf.cabs, deps = configuratt.load_nested(cab_configs, use_sources=[conf], structured=cab_schema, 
                                                        nameattr='name', include_path='_path', location='cabs', 
                                                        use_cache=False, verbose=verbose)
            dependencies.update(deps)
        except Exception as exc:
            if verbose:
                traceback.print_exc()
            log_exception(ConfigError("error loading cabs configuration", exc))
            return None

        conf.opts = opts_schema

        def _load(conf, config_file):
            global CONFIG_LOADED
            log.info(f"loading config from {config_file}")
            try:
                newconf, deps = configuratt.load(config_file, use_sources=[conf], verbose=verbose, use_cache=False)
                dependencies.update(deps)
                conf = merge_extra_config(conf, newconf)
                if not CONFIG_LOADED:
                    CONFIG_LOADED = config_file
            except ConfigExceptionTypes as exc:
                if verbose:
                    traceback.print_exc()
                log_exception(ConfigError("error reading {config_file}", exc))
            return conf

        # add standard configs 
        for config_file in sys_configs:
            conf = _load(conf, config_file)

        # add local configs
        for path in extra_configs:
            conf = _load(conf, path)

        if not CONFIG_LOADED:
            log.info("no user-supplied configuration files given, using defaults")

        # dependencies.replace((base_configs_glob, cab_configs_glob, lib_configs_glob), STIMELA_DIR)

        configuratt.save_cache(all_configs, conf, dependencies, extra_keys=extra_cache_keys, verbose=verbose)

    # add dotlist settings
    if extra_dotlist:
        try:
            dotlist_conf = OmegaConf.from_dotlist(extra_dotlist)
            conf = OmegaConf.unsafe_merge(conf, dotlist_conf)
        except Exception as exc:
            if verbose:
                traceback.print_exc()
            log_exception(f"error applying command-line config settings", exc)
            return None

    # add runtime info
    _ds = time.strftime("%Y%m%d")
    _ts = time.strftime("%H%M%S")
    runtime = dict(
        date=_ds, 
        time=_ts, datetime=f"{_ds}-{_ts}", 
        ncpu=psutil.cpu_count(logical=True),
        node=platform.node().split('.', 1)[0],
        hostname=platform.node(), 
        env={key: value.replace('${', r'\${') for key, value in os.environ.items()})
    runtime['ncpu-logical'] = psutil.cpu_count(logical=True)
    runtime['ncpu-physical'] = psutil.cpu_count(logical=False)

    conf.run = OmegaConf.create(runtime)

    # add include paths
    if conf.opts.include:
        configuratt.PATH += list(conf.opts.include)
        log.info(f"added include paths: {' '.join(conf.opts.include)}")

    global CONFIG_DEPS
    CONFIG_DEPS = dependencies

    # check for missing requirements
    missing = configuratt.check_requirements(conf, [], strict=True)
    for (loc, name, _) in missing:
        log.warn(f"optional config section '{loc}' omitted due to unmet requirement '{name}'")

    return conf

