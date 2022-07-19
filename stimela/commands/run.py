import itertools
import click
import logging
import os.path
import yaml
import sys
import traceback

from datetime import datetime
from typing import List, Optional
from collections import OrderedDict
from omegaconf.omegaconf import OmegaConf, OmegaConfBaseException

import stimela
from scabha import configuratt
from scabha.exceptions import ScabhaBaseException
from stimela import stimelogging
import stimela.config
from stimela.config import ConfigExceptionTypes
from stimela import logger, log_exception
from stimela.exceptions import RecipeValidationError, StimelaRuntimeError
from stimela.main import cli
from stimela.kitchen.recipe import Recipe, Step, RecipeSchema, join_quote

def load_recipe_file(filename: str):
    dependencies = stimela.config.get_initial_deps()

    # if file contains a recipe entry, treat it as a full config (that can include cabs etc.)
    try:
        conf, deps = configuratt.load(filename, use_sources=[stimela.CONFIG])
    except ConfigExceptionTypes as exc:
        log_exception(f"error loading {filename}", exc)
        sys.exit(2)

    # warn user if any includes failed
    if deps.fails:
        logger().warning(f"{len(deps.fails)} optional includes were not found, some cabs may not be available")
        for path, dep in deps.fails.items():
            logger().warning(f"    {path} (from {dep.origin})")

    dependencies.update(deps)

    return conf, dependencies


@cli.command("run",
    help="""
    Execute a single cab, or a recipe from a YML file. 
    If the YML files contains multiple recipes, specify the recipe name as an extra argument.
    Use PARAM=VALUE to specify parameters for the recipe or cab. You can also use X.Y.Z=FOO to
    change any and all config and/or recipe settings.
    """,
    no_args_is_help=True)
@click.option("-s", "--step", "step_names", metavar="STEP(s)", multiple=True,
                help="""only runs specific step(s) from the recipe. Use commas, or give multiple times to cherry-pick steps.
                Use [BEGIN]:[END] to specify a range of steps. Note that enabling an individual step via this option
                force-enables even steps with skip=true.""")
@click.option("-t", "--tags", "tags", metavar="TAG(s)", multiple=True,
                help="""only runs steps wth the given tags (and also steps tagged as "always"). 
                Use commas, or give multiple times for multiple tags.""")
@click.option("--skip-tags", "skip_tags", metavar="TAG(s)", multiple=True,
                help="""explicitly skips steps wth the given tags. 
                Use commas, or give multiple times for multiple tags.""")
@click.option("-e", "--enable-step", "enable_steps", metavar="STEP(s)", multiple=True,
                help="""Sets skip=false on the given step(s). Use commas, or give multiple times for multiple steps.""")
@click.option("-d", "--dry-run", is_flag=True,
                help="""Doesn't actually run anything, only prints the selected steps.""")
@click.argument("what", metavar="filename.yml|cab name") 
@click.argument("parameters", nargs=-1, metavar="[recipe name] [PARAM=VALUE] [X.Y.Z=FOO] ...", required=False) 
def run(what: str, parameters: List[str] = [], dry_run: bool = False, help: bool = False,
    step_names: List[str] = [], tags: List[str] = [], skip_tags: List[str] = [], enable_steps: List[str] = []):

    log = logger()
    params = OrderedDict()
    dotlist = OrderedDict()
    errcode = 0
    recipe_name = None

    # parse arguments as recipe name, parameter assignments, or dotlist for OmegaConf    
    for pp in parameters:
        if "=" not in pp:
            if recipe_name is not None:
                log_exception(f"multiple recipe names given")
                errcode = 2
            recipe_name = pp
        else:
            key, value = pp.split("=", 1)
            # dotlist
            if '.' in key:
                dotlist[key] = pp
            # else param=value
            else:
                # parse string as yaml value
                try:
                    params[key] = yaml.safe_load(value)
                except Exception as exc:
                    log_exception(f"error parsing '{pp}'", exc)
                    errcode = 2

    if errcode:
        sys.exit(errcode)

    # load extra config settigs from dotkey arguments, to be merged in below
    # (when loading a recipe file, we want to merge these in AFTER the recipe is loaded, because the arguments
    # might apply to the recipe)
    try:
        extra_config = OmegaConf.from_dotlist(dotlist.values()) if dotlist else OmegaConf.create()
    except OmegaConfBaseException as exc:
        log_exception(f"error loading command-line dotlist", exc)
        sys.exit(2)

    if what in stimela.CONFIG.cabs:
        cabname = what

        try:
            stimela.CONFIG = OmegaConf.unsafe_merge(stimela.CONFIG, extra_config)
        except OmegaConfBaseException as exc:
            log_exception(f"error applying command-line dotlist", exc)
            sys.exit(2)

        log.info(f"setting up cab {cabname}")

        # create step config by merging in settings (var=value pairs from the command line) 
        outer_step = Step(cab=cabname, params=params)

        # prevalidate() is done by run() automatically if not already done, but it does set up the recipe's logger, so do it anyway
        try:
            outer_step.prevalidate(root=True)
        except ScabhaBaseException as exc:
            log_exception(exc)
            sys.exit(1)

    else:
        if not os.path.isfile(what):
            log_exception(f"'{what}' is neither a recipe file nor a known stimela cab")
            sys.exit(2)

        log.info(f"loading recipe/config {what}")

        conf, recipe_deps = load_recipe_file(what)

        # anything that is not a standard config section will be treated as a recipe
        all_recipe_names = [name for name in conf if name not in stimela.CONFIG]
        if not all_recipe_names:
            log_exception(f"{what} does not contain any recipes")
            sys.exit(2)

        # split content into config sections, and recipes:
        # config secions are merged into the config namespace, while recipes go under
        # lib.recipes
        update_conf = OmegaConf.create()
        for name, value in conf.items():
            if name in stimela.CONFIG:
                update_conf[name] = value
            else:
                try:
                    stimela.CONFIG.lib.recipes[name] = OmegaConf.merge(RecipeSchema, value)
                except Exception as exc:
                    log_exception("error loading recipe '{name}'", exc)
                    sys.exit(2)
        
        try:
            stimela.CONFIG = OmegaConf.unsafe_merge(stimela.CONFIG, update_conf)
        except Exception as exc:
            log_exception(f"error applying configuration from {what}", exc)
            sys.exit(2)

        log.info(f"{what} contains the following recipe sections: {join_quote(all_recipe_names)}")

        if recipe_name:
            if recipe_name not in conf:
                log_exception(f"{what} does not contain recipe '{recipe_name}'")
                sys.exit(2)
        else:
            if len(all_recipe_names) > 1: 
                print(f"This file contains the following recipes: {', '.join(all_recipe_names)}")
                log_exception(f"multiple recipes found, please specify one on the command line")
                sys.exit(2)
            recipe_name = all_recipe_names[0]
        
        log.info(f"selected recipe is '{recipe_name}'")

        # create recipe object from the config
        kwargs = dict(**stimela.CONFIG.lib.recipes[recipe_name])
        kwargs.setdefault('name', recipe_name)
        try:
            recipe = Recipe(**kwargs)
        except Exception as exc:
            log_exception(f"error loading recipe '{recipe_name}'", exc)
            sys.exit(2)

        # force name, if not set
        if not recipe.name:
            recipe.name = recipe_name

        # wrap it in an outer step and prevalidate (to set up loggers etc.)
        recipe.fqname = recipe_name

        # protect dotlisted arguments from being assignedby recipe.assignb and recipe.assign_based_on
        recipe.protect_from_assignments(dotlist.keys())
        recipe.protect_from_assignments(params.keys())

        log.info("pre-validating the recipe")
        outer_step = Step(recipe=recipe, name=f"{recipe_name}", info=what, params=params)
        try:
            params = outer_step.prevalidate(root=True)
        except Exception as exc:
            log_exception(RecipeValidationError(f"pre-validation of recipe '{recipe_name}' failed", exc))
            for line in traceback.format_exc().split("\n"):
                log.debug(line)
            sys.exit(1)        

        # select recipe substeps based on command line

        tags = set(itertools.chain(*(tag.split(",") for tag in tags)))
        skip_tags = set(itertools.chain(*(tag.split(",") for tag in skip_tags))) 
        step_names = list(itertools.chain(*(step.split(',') for step in step_names)))
        enable_steps = set(itertools.chain(*(step.split(",") for step in enable_steps)))

        for name in enable_steps:
            if name in recipe.steps:
                recipe.enable_step(name)  # config file may have skip=True, but we force-enable here
            else:
                log_exception(f"no such recipe step: '{name}'")
                sys.exit(2)

        # select subset based on tags/skip_tags, this will be a list of names
        tagged_steps = set()

        # if tags are given, only use steps with (tags+{"always"}-skip_tags)
        if tags:
            tags.add("always")
            tags.difference_update(skip_tags)
            for step_name, step in recipe.steps.items():
                if (tags & step.tags):
                    tagged_steps.add(step_name)
            log.info(f"{len(tagged_steps)} of {len(recipe.steps)} steps selected via tags ({', '.join(tags)})")
        # else, use steps without any tag in (skip_tags + {"never"})
        else:
            skip_tags.add("never")
            for step_name, step in recipe.steps.items():
                if not (skip_tags & step.tags):
                    tagged_steps.add(step_name)
            if len(recipe.steps) != len(tagged_steps):
                log.info(f"{len(recipe.steps) - len(tagged_steps)} steps skipped due to tags ({', '.join(skip_tags)})")

        # add steps explicitly enabled by --step
        if step_names:
            all_step_names = list(recipe.steps.keys())
            step_subset = set()
            for name in step_names:
                if ':' in name:
                    begin, end = name.split(':', 1)
                    if begin:
                        try:
                            first = all_step_names.index(begin)
                        except ValueError as exc:
                            log_exception(f"No such recipe step: '{begin}")
                            sys.exit(2)
                    else:
                        first = 0
                    if end:
                        try:
                            last = all_step_names.index(end)
                        except ValueError as exc:
                            log_exception(f"No such recipe step: '{end}")
                            sys.exit(2)
                    else:
                        last = len(recipe.steps)-1
                    step_subset.update(name for name in all_step_names[first:last+1] if name in tagged_steps)
                # explicit step name: enable, and add to tagged_steps
                else:
                    if name not in all_step_names:
                        log_exception(f"No such recipe step: '{name}")
                        sys.exit(2)
                    recipe.enable_step(name)  # config file may have skip=True, but we force-enable here
                    step_subset.add(name)
            # specified subset becomes *the* subset
            log.info(f"{len(step_subset)} steps selected by name")
            tagged_steps = step_subset

        if not tagged_steps:
            log.info("specified tags and/or step names select no steps")
            sys.exit(0)

        # apply restrictions, if any
        recipe.restrict_steps(tagged_steps, force_enable=False)

        steps = [name for name, step in recipe.steps.items() if not step._skip]
        log.info(f"will run the following recipe steps:")
        log.info(f"    {' '.join(steps)}", extra=dict(color="GREEN"))

        # warn user if som steps remain explicitly disabled
        if any(recipe.steps[name]._skip for name in tagged_steps):
            log.warning("note that some steps remain explicitly skipped")

        filename = os.path.join(stimelogging.get_logger_file(recipe.log) or '.', "stimela.recipe.deps")
        stimela.config.CONFIG_DEPS.update(recipe_deps)
        stimela.config.CONFIG_DEPS.save(filename)
        log.info(f"saved recipe dependencies to {filename}")

    # in debug mode, pretty-print the recipe
    if log.isEnabledFor(logging.DEBUG):
        log.debug("---------- prevalidated step follows ----------")
        for line in outer_step.summary(params=params):
            log.debug(line)

    if dry_run:
        log.info("dry run was requested, exiting")
        sys.exit(0)

    start_time = datetime.now()
    def elapsed():
        return str(datetime.now() - start_time).split('.', 1)[0]

    try:
        outputs = outer_step.run()
    except Exception as exc:
        if not isinstance(exc, ScabhaBaseException) or not exc.logged:
            log_exception(StimelaRuntimeError(f"run failed after {elapsed()}", exc))
        for line in traceback.format_exc().split("\n"):
            log.debug(line)
        sys.exit(1)

    if outputs and step.log.isEnabledFor(logging.DEBUG):
        outer_step.log.debug(f"run successful after {elapsed()}, outputs follow:")
        for name, value in outputs.items():
            if name in recipe.outputs:
                outer_step.log.debug(f"  {name}: {value}")
    else:
        outer_step.log.info(f"run successful after {elapsed()}")

    return 0