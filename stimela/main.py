import os
import logging
import glob
import sys
import click
import datetime
from dataclasses import dataclass
from omegaconf import OmegaConf
import stimela
from stimela import config, stimelogging, backends

UID = stimela.UID
GID = stimela.GID
LOG_HOME = stimela.LOG_HOME
LOG_FILE = stimela.LOG_FILE

log = None

_command_aliases = dict(exec="run", help="doc")

class RunExecGroup(click.Group):
    """ Makes the run and exec commands point to the same thing

    Args:
        click (_type_): _description_
    """
    def get_command(self, ctx, cmd_name):
        rv = click.Group.get_command(self, ctx, cmd_name)
        if rv is not None:
            return rv

        if cmd_name in _command_aliases:  
            return click.Group.get_command(self, ctx, _command_aliases[cmd_name])
        ctx.fail("Uknown command or alias")

    def resolve_command(self, ctx, args):
        # always return the full command name
        _, cmd, args = super().resolve_command(ctx, args)
        return cmd.name, cmd, args


@click.group(cls=RunExecGroup)
@click.option('--backend', '-b', type=click.Choice(backends.SUPPORTED_BACKENDS), 
                 help="Backend to use.")
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
@click.option('--boring', '-B', is_flag=True, 
                help="Disables progress bar and any other fancy console outputs.")
@click.option('--verbose', '-v', is_flag=True, 
              help='Be extra verbose in output.')
@click.version_option(str(stimela.__version__))
def cli(config_files=[], config_dotlist=[], include=[], backend=None, 
        verbose=False, no_sys_config=False, clear_cache=False, boring=False):
    global log
    log = stimela.logger(loglevel=logging.DEBUG if verbose else logging.INFO, boring=boring)
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

    # select backend, passing it any config options that have been set up
    if backend:
        if backends.get_backend(backend, getattr(stimela.CONFIG.opts.backend, backend, None)) is None:
            log.error(f"backend '{backend}' not available: {backends.get_backend_status(backend)}")
            sys.exit(1)

        stimela.CONFIG.opts.backend.select = [backend]

    # enable logfiles and such
    if stimela.CONFIG.opts.log.enable:
        if verbose:
            stimela.CONFIG.opts.log.level = "DEBUG"
        # setup file logging
        subst = OmegaConf.create(dict(
                    info=OmegaConf.create(dict(fqname='stimela', taskname='stimela')), 
                    config=stimela.CONFIG))
        stimelogging.update_file_logger(log, stimela.CONFIG.opts.log, nesting=-1, subst=subst)

    # report dependencies
    for filename, attrs in config.CONFIG_DEPS.get_description().items():
        log.debug(f"config dependency {', '.join([filename] + attrs)}")

    # dump dependencies
    filename = os.path.join(stimelogging.get_logfile_dir(log) or '.', "stimela.config.deps")
    log.info(f"saving config dependencies to {filename}")
    config.CONFIG_DEPS.save(filename)


# import commands
from stimela.commands import doc, run, build, save_config, cleanup

## These one needs to be reimplemented, current backed auto-pulls and auto-builds:
# images, pull, build, clean

## this one is deprecated, stimela doc does the trick
# cabs

## the ones below should be deprecated, since we don't do async containers anymore
# containers, kill, ps
