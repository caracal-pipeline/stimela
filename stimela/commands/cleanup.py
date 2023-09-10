import os, sys
import click
from omegaconf import OmegaConf
from typing import Dict, List

import stimela
from stimela import logger, log_exception
from stimela.main import cli
from stimela.kitchen.recipe import Recipe
from stimela.exceptions import RecipeValidationError, BackendError

from .run import load_recipe_file

@cli.command("cleanup",
    help="""
    Cleans up backend resources associated with recipe(s).
    """)
@click.argument("items", nargs=-1, metavar="filename.yml|recipe name|...") 
def cleanup(items: List[str] = []):
    
    log = logger()
        
    for item in items:
        # a filename -- treat it as a config
        if os.path.isfile(item):
            log.info(f"loading recipe/config {item}")

            # if file contains a recipe entry, treat it as a full config (that can include cabs etc.)
            conf, recipe_deps = load_recipe_file(item)

            # anything that is not a standard config section will be treated as a recipe
            recipes = [name for name in conf if name not in stimela.CONFIG]

            if len(recipes) == 1:
                default_recipe = recipes[0]

            for name in recipes:
                try:
                    # cast section to Recipe and remove from loaded conf
                    recipe = OmegaConf.structured(Recipe)
                    recipe = OmegaConf.unsafe_merge(recipe, conf[name])
                except Exception as exc:
                    log.error(f"recipe '{name}': {exc}")
                    sys.exit(2)
                del conf[name]
                # add to global namespace
                stimela.CONFIG.lib.recipes[name] = recipe

            # the rest is safe to merge into config as is
            stimela.CONFIG = OmegaConf.unsafe_merge(stimela.CONFIG, conf)
        
    # now cleanup backend
    try:
        backend = OmegaConf.to_object(stimela.CONFIG.opts.backend)
        stimela.backends.cleanup_backends(backend, log)
    except BackendError as exc:
        log_exception(exc)
        return 1

