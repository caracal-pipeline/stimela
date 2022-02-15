# -*- coding: future_fstrings -*-
import os
import logging
import re
import time
import sys
import click
import stimela
from omegaconf import OmegaConf
from stimela import config, stimelogging
from dataclasses import dataclass

BASE = stimela.BASE
CAB = stimela.CAB
USER = stimela.USER
UID = stimela.UID
GID = stimela.GID
LOG_HOME = stimela.LOG_HOME
LOG_FILE = stimela.LOG_FILE
GLOBALS = stimela.GLOBALS
CAB_USERNAME = stimela.CAB_USERNAME

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
                help="Extra config file(s) to load. Prefix with '=' to override standard config files.")
@click.option('--verbose', '-v', is_flag=True, help='Be extra verbose in output.')
@click.version_option(str(stimela.__version__))
def cli(backend, config_files=[], verbose=False):
    global log
    log = stimela.logger(loglevel=logging.DEBUG if verbose else logging.INFO)
    log.info(f"starting")        # remove this eventually, but it's handy for timing things right now

    if verbose:
        log.debug("verbose output enabled")

    # use this logger for exceptions
    import scabha.exceptions
    scabha.exceptions.set_logger(log)

    # load config files
    stimela.CONFIG = config.load_config(extra_configs=config_files)
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

    # set backend module
    global BACKEND 
    if backend:
        stimela.CONFIG.opts.backend = backend
    BACKEND = getattr(stimela.backends, stimela.CONFIG.opts.backend.name)
    log.info(f"backend is {stimela.CONFIG.opts.backend.name}")



# import commands
from stimela.commands import run, images, build, push, save_config

## the ones not listed above haven't been converted to click yet. They are:
# cabs, clean, containers, kill, ps, pull
