import click
from typing import List
from stimela.main import cli

import click
from typing import List, Optional, Tuple
from .run import run

from stimela.main import cli

@cli.command("build",
    help="""
    Builds singularity images required by the recipe. Only available if the singularity backend is selected.
    """,
    no_args_is_help=True)
@click.option("-r", "--rebuild", is_flag=True,
                help="""rebuilds all images from scratch. Default builds missing images only.""")
@click.option("-a", "--all-steps", is_flag=True,
                help="""builds images for all steps. Default is to omit explicitly skipped steps.""")
@click.option("-s", "--step", "step_ranges", metavar="STEP(s)", multiple=True,
                help="""only build images for specific step(s) from the recipe. Use commas, or give multiple times to cherry-pick steps.
                Use [BEGIN]:[END] to specify a range of steps. Note that cherry-picking an individual step via this option
                also impies --enable-step.""")
@click.option("-t", "--tags", "tags", metavar="TAG(s)", multiple=True,
                help="""only build images for steps wth the given tags (and also steps tagged as "always"). 
                Use commas, or give multiple times for multiple tags.""")
@click.option("--skip-tags", "skip_tags", metavar="TAG(s)", multiple=True,
                help="""explicitly skips steps wth the given tags. 
                Use commas, or give multiple times for multiple tags.""")
@click.option("-e", "--enable-step", "enable_steps", metavar="STEP(s)", multiple=True,
                help="""Build image for step(s) even if the recipe marks them as skipped. Use commas, or give multiple times 
                for multiple steps.""")
@click.option("-c", "--config", "config_equals", metavar="X.Y.Z=VALUE", nargs=1, multiple=True,
                help="""tweak configuration options.""")
@click.option("-C", "--config-assign", metavar="X.Y.Z VALUE", nargs=2, multiple=True,
                help="""tweak configuration options: same function -c/--config, but plays nicer with the shell's 
                tab completion feature.""")
@click.option("-l", "--last-recipe", is_flag=True,
                help="""if multiple recipes are defined, selects the last one for building.""")
@click.option("-S", "--singularity", "enable_singularity", is_flag=True,
                help="""Selects the singularity backend (shortcut for -C opts.backend.select=singularity)""")
@click.option("--slurm", "enable_slurm", is_flag=True,
                help="""Enables the slurm backend wrapper (shortcut for -C backend.slurm.enable=True)""")
@click.argument("what", metavar="filename.yml ... [recipe name] [PARAM=VALUE] ...", nargs=-1, required=True) 
def build(what: str, last_recipe: bool = False, rebuild: bool = False, all_steps: bool=False,
            config_equals: List[str] = [],
            config_assign: List[Tuple[str, str]] = [],
            step_ranges: List[str] = [], tags: List[str] = [], skip_tags: List[str] = [], enable_steps: List[str] = [],
            enable_singularity=False,
            enable_slurm=False):
    return run.callback(what, last_recipe=last_recipe, step_ranges=step_ranges, 
        tags=tags, skip_tags=skip_tags, enable_steps=enable_steps,
        config_equals=config_equals, config_assign=config_assign,
        build=True, rebuild=rebuild, build_skips=all_steps,
        enable_singularity=enable_singularity,
        enable_slurm=enable_slurm)
