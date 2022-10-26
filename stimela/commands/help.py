import fnmatch
import os, sys
import click
from typing import *
from rich.tree import Tree
from rich.table import Table
from rich import box
from rich import print as rich_print
from omegaconf import OmegaConf

import stimela
from scabha import configuratt
from stimela import logger, log_exception
from stimela.main import cli
from scabha.cargo import ParameterCategory
from stimela.kitchen.recipe import Recipe
from stimela.kitchen.cab import Cab
from stimela.config import ConfigExceptionTypes
from stimela.exceptions import RecipeValidationError

from .run import load_recipe_file

@cli.command("help",
    help="""
    Print help on a cab or a recipe.
    """)
@click.option("do_list", "-l", "--list", is_flag=True,
                help="""Lists the available cabs and recipes, including custom-defined ones.""")
@click.option("-I", "--implicit", is_flag=True,
                help="""Increases level of detail to include implicit inputs/outputs.""")
@click.option("-O", "--obscure", is_flag=True,
                help="""Increases level of detail to include implicit and obscure inputs/outputs.""")
@click.option("-A", "--all", is_flag=True,
                help="""Increases level of detail to include all inputs/outputs.""")
@click.option("-R", "--required", is_flag=True,
                help="""Decreases level of detail to include required inputs/outputs only.""")
@click.argument("items", nargs=-1, metavar="filename.yml|cab name|recipe name|...") 
def help(items: List[str] = [], do_list=False, implicit=False, obscure=False, all=False, required=False):

    log = logger()
    top_tree = Tree(f"stimela help {' '.join(items)}", guide_style="dim")
    found_something = False
    default_recipe = None

    if required:
        max_category = ParameterCategory.Required
    else:
        max_category = ParameterCategory.Optional
    if all:
        max_category = ParameterCategory.Hidden
    elif obscure:
        max_category = ParameterCategory.Obscure
    elif implicit:
        max_category = ParameterCategory.Implicit
        
    def load_recipe(name: str, section: Dict):
        try:
            if not section.get('name'):
                section.name = name
            recipe = Recipe(**section)
            recipe.finalize(fqname=name)
            return recipe
        except Exception as exc:
            if not isinstance(exc, RecipeValidationError):
                exc = RecipeValidationError(f"error loading recipe '{name}'", exc)
            log_exception(exc)
            sys.exit(2)


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
        
        # else treat as a wildcard for recipe names or cab names
        else:
            recipe_names = fnmatch.filter(stimela.CONFIG.lib.recipes.keys(), item)
            cab_names = fnmatch.filter(stimela.CONFIG.cabs.keys(), item)
            if not recipe_names and not cab_names:
                log.error(f"'{item}' does not match any files, recipes or cab names. Try -l/--list")
                sys.exit(2)

            for name in recipe_names:
                recipe = load_recipe(name, stimela.CONFIG.lib.recipes[name])
                tree = top_tree.add(f"Recipe: [bold]{name}[/bold]")
                recipe.rich_help(tree, max_category=max_category)
            
            for name in cab_names:
                cab = Cab(**stimela.CONFIG.cabs[name])
                cab.finalize(config=stimela.CONFIG)
                tree = top_tree.add(f"Cab: [bold]{name}[/bold]")
                cab.rich_help(tree, max_category=max_category)

            found_something = True

    if do_list or (not found_something and not default_recipe):

        if stimela.CONFIG.lib.recipes:
            subtree = top_tree.add("Recipes:")
            table = Table.grid("", "", padding=(0,2))
            for name, recipe in stimela.CONFIG.lib.recipes.items():
                table.add_row(f"[bold]{name}[/bold]", recipe.info)
            subtree.add(table)
        elif not do_list and not found_something:
            log.error(f"nothing particular to help on, perhaps specify a recipe name or a cab name, or use -l/--list")
            sys.exit(2)

    if default_recipe and not found_something:
        recipe = load_recipe(name, stimela.CONFIG.lib.recipes[default_recipe])
        tree = top_tree.add(f"Recipe: [bold]{default_recipe}[/bold]")
        recipe.rich_help(tree, max_category=max_category)

    if do_list:
        subtree = top_tree.add("Cabs:")
        table = Table.grid("", "", padding=(0,2))
        for name, cab in stimela.CONFIG.cabs.items():
            table.add_row(f"[bold]{name}[/bold]", cab.info)
        subtree.add(table)            
        
    rich_print(top_tree)


