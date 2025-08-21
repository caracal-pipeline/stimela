from typing import List

import click
from omegaconf import OmegaConf

import stimela
from stimela import log_exception, logger
from stimela.exceptions import BackendError
from stimela.main import cli

from .run import load_recipe_files


@cli.command(
    "cleanup",
    help="""
    Cleans up backend resources associated with recipe(s).
    """,
)
@click.argument("items", nargs=-1, metavar="filename.yml...")
def cleanup(items: List[str] = []):
    log = logger()

    # load all recipe/config files
    # load config and recipes from all given files
    load_recipe_files(items)

    # now cleanup backend
    backends_list = stimela.CONFIG.opts.backend.select
    if type(backends_list) is str:
        backends_list = [backends_list]
    if not backends_list:
        log.info("configuration does not specify any backends, nothing to clean up")
    else:
        log.info(f"invoking cleanup procedure, selected backends: {', '.join(backends_list)}")
        try:
            backend = OmegaConf.to_object(stimela.CONFIG.opts.backend)
            stimela.backends.cleanup_backends(backend, log)
        except BackendError as exc:
            log_exception(exc)
            return 1
