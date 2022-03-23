import fnmatch
import os, sys
import click
from omegaconf import OmegaConf

import stimela
from scabha import configuratt
from stimela import logger
from stimela.main import cli
from scabha.cargo import Cab
from stimela.kitchen.recipe import Recipe, Step, join_quote
from stimela.config import ConfigExceptionTypes
from typing import *
from rich.tree import Tree
from rich.table import Table
from rich import box
from rich import print as rich_print

def print_schema_help(cargo_type, name, cargo, extra_defaults={}, omit_params={}):
    print(
f"""
    {cargo_type} name: {name}
    Info: {cargo.info}
""")
    def print_inputs_outputs(inputs_outputs):
        for name, schema in inputs_outputs.items():
            line = f"        {name}: {schema.dtype}"
            default = extra_defaults.get(name)
            if default is None:
                default = schema.default
            if default is not None:
                line += f" = {default}"
            line += "\n"
            if schema.info:
                line = f"          {schema.info}\n"
            print(line)
    if cargo.inputs:
        print("    Inputs:\n")
        print_inputs_outputs(cargo.inputs)
    else:
        print("    Inputs: none\n")
    if cargo.outputs:
        print("    Outputs:\n")
        print_inputs_outputs(cargo.outputs)
    else:
        print("    Outputs: none\n")
    if type(cargo) is Recipe:
        print("\n    Steps")
        if any(step.skip for step in cargo.steps.values()):
            print(" including [skipped] steps")
        step_names = []
        for label, step in cargo.steps.items():
            step_names.append(f"[{label}]" if step.skip else label)
        print(f":\n        {' '.join(step_names)}")


@cli.command("help",
    help="""
    Print help on a cab or a recipe.
    """)
@click.option("do_list", "-l", "--list", is_flag=True,
                help="""Lists the available cabs and recipes, including custom-defined ones.""")
@click.argument("items", nargs=-1, metavar="filename.yml|cab name|recipe name|...") 
def help(items: List[str] = [], do_list=False):

    log = logger()
    top_tree = Tree(f"stimela help {' '.join(items)}", guide_style="dim")
    found_something = False

    for item in items:
        # a filename -- treat it as a config
        if os.path.isfile(item):
            log.info(f"loading recipe/config {item}")

            # if file contains a recipe entry, treat it as a full config (that can include cabs etc.)
            try:
                conf = configuratt.load(item, use_sources=[stimela.CONFIG])
            except ConfigExceptionTypes as exc:
                log.error(f"error loading {item}: {exc}")
                sys.exit(2)

            # anything that is not a standard config section will be treated as a recipe
            recipes = [name for name in conf if name not in stimela.CONFIG]

            for name in recipes:
                # cast section to Recipe and remove from loaded conf
                recipe = OmegaConf.create(Recipe)
                recipe = OmegaConf.unsafe_merge(recipe, conf[name])
                del conf[name]
                # add to global namespace
                stimela.CONFIG.lib.recipes[name] = recipe

            # the rest is safe to merge into config as is
            stimela.CONFIG = OmegaConf.merge(stimela.CONFIG, conf)
        
        # else treat as a wildcard for recipe names or cab names
        else:
            recipe_names = fnmatch.filter(stimela.CONFIG.lib.recipes.keys(), item)
            cab_names = fnmatch.filter(stimela.CONFIG.cabs.keys(), item)
            if not recipe_names and not cab_names:
                log.error(f"'{item}' does not match any files, recipes or cab names")
                sys.exit(2)

            for name in recipe_names:
                recipe = Recipe(**stimela.CONFIG.lib.recipes[name])
                recipe.finalize()
                tree = top_tree.add(f"Recipe: [bold]{name}[/bold]")
                recipe.rich_help(tree)
            
            for name in cab_names:
                cab = Cab(**stimela.CONFIG.cabs[name])
                cab.finalize()
                tree = top_tree.add(f"Cab: [bold]{name}[/bold]")
                cab.rich_help(tree)

            found_something = True

    if do_list or not found_something:
        if stimela.CONFIG.lib.recipes:
            subtree = top_tree.add("Recipes:")
            table = Table.grid("", "", padding=(0,2))
            for name, recipe in stimela.CONFIG.lib.recipes.items():
                table.add_row(f"[bold]{name}[/bold]", recipe.info)
            subtree.add(table)
        elif not do_list and not found_something:
            log.error(f"nothing particular to help on, perhaps specify a recipe name or a cab name, or use -l/--list")
            sys.exit(2)

    if do_list:
        subtree = top_tree.add("Cabs:")
        table = Table.grid("", "", padding=(0,2))
        for name, cab in stimela.CONFIG.cabs.items():
            table.add_row(f"[bold]{name}[/bold]", cab.info)
        subtree.add(table)            
        
    rich_print(top_tree)


