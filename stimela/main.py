import os
import logging
import glob
import sys
import click
import datetime
from dataclasses import dataclass
from omegaconf import OmegaConf
import stimela
from stimela import config, stimelogging

UID = stimela.UID
GID = stimela.GID
LOG_HOME = stimela.LOG_HOME
LOG_FILE = stimela.LOG_FILE

log = None

class RunExecGroup(click.Group):
    """ Makes the run and exec commands point to the same thing

    Args:
        click (_type_): _description_
    """
    def get_command(self, ctx, cmd_name):
        rv = click.Group.get_command(self, ctx, cmd_name)
        if rv is not None:
            return rv

        alias = "exec"
        if cmd_name == alias :  
            return click.Group.get_command(self, ctx, "run")
        ctx.fail("Uknown command or alias")

    def resolve_command(self, ctx, args):
        # always return the full command name
        _, cmd, args = super().resolve_command(ctx, args)
        return cmd.name, cmd, args


@click.group(cls=RunExecGroup)
@click.option('--backend', '-b', type=click.Choice(config.Backend._member_names_), 
                help="Backend to use (for containerization).")
@click.option('--config', '-c', 'config_files', metavar='FILE', multiple=True,
                help="Extra user-defined config file(s) to load.")
@click.option('--set', '-s', 'config_dotlist', metavar='SECTION.VAR=VALUE', multiple=True,
                help="Extra user-defined config settings to apply.")
@click.option('--no-sys-config', is_flag=True, 
                help="Do not load standard config files.")
@click.option('-I', '--include', metavar="DIR", multiple=True, 
                help="Add directory to _include paths. Can be given multiple times.")
@click.option('--clear-cache', '-C', is_flag=True, 
                help="Reset the configuration cache. First thing to try in case of strange configuration errors.")
@click.option('--verbose', '-v', is_flag=True, help='Be extra verbose in output.')
@click.version_option(str(stimela.__version__))
def cli(backend, config_files=[], config_dotlist=[], include=[], verbose=False, no_sys_config=False, clear_cache=False):
    global log
    log = stimela.logger(loglevel=logging.DEBUG if verbose else logging.INFO)
    log.info(f"starting")        # remove this eventually, but it's handy for timing things right now

    stimela.VERBOSE = verbose
    if verbose:
        log.debug("verbose output enabled")

    # use this logger for exceptions
    import scabha.exceptions
    scabha.exceptions.set_logger(log)
    if verbose:
        scabha.exceptions.ALWAYS_REPORT_TRACEBACK = True

    import scabha.configuratt.cache
    scabha.configuratt.cache.set_cache_dir(os.path.expanduser("~/.cache/stimela-configs"))
    # clear cache if requested
    if clear_cache:
        scabha.configuratt.cache.clear_cache(log)

    # load config files
    stimela.CONFIG = config.load_config(extra_configs=config_files, extra_dotlist=config_dotlist, include_paths=include,
                                        verbose=verbose, use_sys_config=not no_sys_config)
    if stimela.CONFIG is None:
        log.error("failed to load configuration, exiting")
        sys.exit(1)

    if config.CONFIG_LOADED:
        log.info(f"loaded config from {config.CONFIG_LOADED}") 

    # enable logfiles and such
    if stimela.CONFIG.opts.log.enable:
        if verbose:
            stimela.CONFIG.opts.log.level = "DEBUG"
        # setup file logging
        subst = OmegaConf.create(dict(
                    info=OmegaConf.create(dict(fqname='stimela')), 
                    config=stimela.CONFIG))
        stimelogging.update_file_logger(log, stimela.CONFIG.opts.log, nesting=-1, subst=subst)


    # print backends
    log.info(f"available backends: {' '.join(config.AVAILABLE_BACKENDS)}")

    # get default backend module
    global BACKEND 
    if backend:
        stimela.CONFIG.opts.backend = backend

    if stimela.CONFIG.opts.backend.name not in config.AVAILABLE_BACKENDS:
        status = config.get_backend_status(stimela.CONFIG.opts.backend.name)
        log.error(f"the backend '{stimela.CONFIG.opts.backend.name}' is not available: {status}")
        sys.exit(1)

    log.info(f"default backend is {stimela.CONFIG.opts.backend.name}")

    # report dependencies
    for filename, attrs in config.CONFIG_DEPS.get_description().items():
        log.debug(f"config dependency {', '.join([filename] + attrs)}")

    # dump dependencies
    filename = os.path.join(stimelogging.get_logger_file(log) or '.', "stimela.config.deps")
    log.info(f"saving config dependencies to {filename}")
    config.CONFIG_DEPS.save(filename)


# import commands
from stimela.commands import run, images, build, push, save_config, help

## the ones not listed above haven't been converted to click yet. They are:
# cabs, clean, containers, kill, ps, pull
