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
from stimela import logger, log_exception
from stimela.main import cli
from scabha.cargo import ParameterCategory
from stimela.kitchen.recipe import Recipe
from stimela.kitchen.cab import Cab
from stimela.exceptions import RecipeValidationError
from stimela.task_stats import destroy_progress_bar

from .run import load_recipe_files

@cli.command("doc",
    help="""
    Print documentation on a cab or a recipe.
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
@click.argument("what", nargs=-1, metavar="filename.yml ... [recipe name] [cab name]", required=True) 
def doc(what: List[str] = [], do_list=False, implicit=False, obscure=False, all=False, required=False):

    log = logger()
    top_tree = Tree(f"stimela doc {' '.join(what)}", guide_style="dim")

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

    # load all recipe/config files
    files_to_load = []
    names_to_document = []
    for item in what:
        if os.path.splitext(item)[1].lower() in (".yml", ".yaml"):
            files_to_load.append(item)
            log.info(f"will load recipe/config file '{item}'")
        else:
            names_to_document.append(item)
    
    # load config and recipes from all given files
    if files_to_load:
        load_recipe_files(files_to_load)

    destroy_progress_bar()
    
    log.info(f"loaded {len(stimela.CONFIG.cabs)} cab definition(s) and {len(stimela.CONFIG.lib.recipes)} recipe(s)")

    if not stimela.CONFIG.lib.recipes and not stimela.CONFIG.cabs:
        log.error(f"Nothing to document")
        sys.exit(2)

    recipes_to_document = set()
    cabs_to_document = set()

    for item in names_to_document:
        recipe_names = fnmatch.filter(stimela.CONFIG.lib.recipes.keys(), item)
        cab_names = fnmatch.filter(stimela.CONFIG.cabs.keys(), item)
        if not recipe_names and not cab_names:
            log.error(f"'{item}' does not match any recipe or cab names. Try -l/--list")
            sys.exit(2)
        recipes_to_document.update(recipe_names)
        cabs_to_document.update(cab_names)

    # if nothing was specified, and only one cab/only one recipe is defined, print that
    if not names_to_document:
        if len(stimela.CONFIG.lib.recipes) == 1 and not stimela.CONFIG.cabs:
            recipes_to_document.update(stimela.CONFIG.lib.recipes.keys())
        elif len(stimela.CONFIG.cabs) == 1 and not stimela.CONFIG.lib.recipes:
            cabs_to_document.update(stimela.CONFIG.cabs.keys())

    if recipes_to_document or cabs_to_document:
        for name in recipes_to_document:
            recipe = load_recipe(name, stimela.CONFIG.lib.recipes[name])
            tree = top_tree.add(f"Recipe: [bold]{name}[/bold]")
            recipe.rich_help(tree, max_category=max_category)
        
        for name in cabs_to_document:
            cab = Cab(**stimela.CONFIG.cabs[name])
            cab.finalize(config=stimela.CONFIG)
            tree = top_tree.add(f"Cab: [bold]{name}[/bold]")
            cab.rich_help(tree, max_category=max_category)

    # list recipes and cabs -- also do this by default if nothing explicit was documented    
    if do_list or not (recipes_to_document or cabs_to_document):
        if stimela.CONFIG.lib.recipes:
            subtree = top_tree.add("Recipes:")
            table = Table.grid("", "", padding=(0,2))
            for name, recipe in stimela.CONFIG.lib.recipes.items():
                table.add_row(f"[bold]{name}[/bold]", recipe.info)
            subtree.add(table)

        if stimela.CONFIG.cabs:
            subtree = top_tree.add("Cabs:")
            table = Table.grid("", "", padding=(0,2))
            for name, cab in stimela.CONFIG.cabs.items():
                table.add_row(f"[bold]{name}[/bold]", cab.info)
            subtree.add(table)            
        
    rich_print(top_tree)


