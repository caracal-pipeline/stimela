import itertools
import click
import logging
import os.path
import yaml
import sys
import traceback

from datetime import datetime
from typing import List, Optional, Tuple
from collections import OrderedDict
from omegaconf.omegaconf import OmegaConf, OmegaConfBaseException

import stimela
from scabha import configuratt
from scabha.exceptions import ScabhaBaseException
from stimela import stimelogging
import stimela.config
from stimela.config import ConfigExceptionTypes
from stimela import logger, log_exception
from stimela.exceptions import RecipeValidationError, StimelaRuntimeError, StepSelectionError
from stimela.main import cli
from stimela.kitchen.recipe import Recipe, Step, RecipeSchema, join_quote
from stimela import task_stats

def load_recipe_file(filename: str):
    dependencies = stimela.config.get_initial_deps()

    # if file contains a recipe entry, treat it as a full config (that can include cabs etc.)
    try:
        conf, deps = configuratt.load(filename, use_sources=[stimela.CONFIG], no_toplevel_cache=True)
    except ConfigExceptionTypes as exc:
        log_exception(f"error loading {filename}", exc)
        sys.exit(2)

    # warn user if any includes failed
    if deps.fails:
        logger().warning(f"{len(deps.fails)} optional includes were not found, some cabs may not be available")
        for path, dep in deps.fails.items():
            logger().warning(f"    {path} (from {dep.origin})")

    dependencies.update(deps)

    # check for missing requirements
    missing = configuratt.check_requirements(conf, [stimela.CONFIG], strict=True)
    for (loc, name, _) in missing:
        logger().warning(f"optional config section '{loc}' omitted due to missing requirement '{name}'")


    return conf, dependencies


@cli.command("run",
    help="""
    Execute a single cab, or a recipe from a YML file. 
    If the YML files contains multiple recipes, specify the recipe name as an extra argument.
    Use PARAM=VALUE to specify parameters for the recipe or cab. You can also use X.Y.Z=FOO to
    change any and all config and/or recipe settings.
    """,
    no_args_is_help=True)
@click.option("-s", "--step", "step_ranges", metavar="STEP(s)", multiple=True,
                help="""only runs specific step(s) from the recipe. Use commas, or give multiple times to cherry-pick steps.
                Use [BEGIN]:[END] to specify a range of steps. Note that cherry-picking an individual step via this option
                also impies --enable-step.""")
@click.option("-t", "--tags", "tags", metavar="TAG(s)", multiple=True,
                help="""only runs steps wth the given tags (and also steps tagged as "always"). 
                Use commas, or give multiple times for multiple tags.""")
@click.option("--skip-tags", "skip_tags", metavar="TAG(s)", multiple=True,
                help="""explicitly skips steps wth the given tags. 
                Use commas, or give multiple times for multiple tags.""")
@click.option("-e", "--enable-step", "enable_steps", metavar="STEP(s)", multiple=True,
                help="""Force-enable steps even if the recipe marks them as skipped. Use commas, or give multiple times 
                for multiple steps.""")
@click.option("-a", "--assign", metavar="PARAM VALUE", nargs=2, multiple=True,
                help="""assigns values to parameters: equivalent to PARAM=VALUE, but plays nicer with the shell's 
                tab completion.""")
@click.option("-l", "--last-recipe", is_flag=True,
                help="""if multiple recipes are defined, selects the last one for execution.""")
@click.option("-d", "--dry-run", is_flag=True,
                help="""Doesn't actually run anything, only prints the selected steps.""")
@click.option("-p", "--profile", metavar="DEPTH", type=int,
                help="""Print per-step profiling stats to this depth. 0 disables.""")
@click.argument("what", metavar="filename.yml|cab name") 
@click.argument("parameters", nargs=-1, metavar="[recipe name] [PARAM=VALUE] [X.Y.Z=FOO] ...", required=False) 
def run(what: str, parameters: List[str] = [], dry_run: bool = False, last_recipe: bool = False, profile: Optional[int] = None,
    assign: List[Tuple[str, str]] = [],
    step_ranges: List[str] = [], tags: List[str] = [], skip_tags: List[str] = [], enable_steps: List[str] = []):

    log = logger()
    params = OrderedDict()
    dotlist = OrderedDict()
    errcode = 0
    recipe_name = None

    # parse assign values as YaML
    for key, value in assign:
        # parse string as yaml value
        try:
            params[key] = yaml.safe_load(value)
        except Exception as exc:
            log_exception(f"error parsing value for --assign {key} {value}", exc)
            errcode = 2

    # parse arguments as recipe name, parameter assignments, or dotlist for OmegaConf    
    for pp in parameters:
        if "=" not in pp:
            if recipe_name is not None:
                log_exception(f"multiple recipe names given")
                errcode = 2
            recipe_name = pp
        else:
            key, value = pp.split("=", 1)
            # parse string as yaml value
            try:
                params[key] = yaml.safe_load(value)
            except Exception as exc:
                log_exception(f"error parsing {pp}", exc)
                errcode = 2

    if errcode:
        sys.exit(errcode)

    # load extra config settigs from dotkey arguments, to be merged in below
    # (when loading a recipe file, we want to merge these in AFTER the recipe is loaded, because the arguments
    # might apply to the recipe)
    try:
        extra_config = OmegaConf.from_dotlist(list(dotlist.values())) if dotlist else OmegaConf.create()
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
        conf.merge_with(extra_config)

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
                    log_exception(f"error in definition of recipe '{name}'", exc)
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
        elif last_recipe or len(all_recipe_names) == 1:
            recipe_name = all_recipe_names[-1]
        else:
            print(f"This file contains the following recipes: {', '.join(all_recipe_names)}")
            log_exception(f"multiple recipes found, please specify one on the command line")
            sys.exit(2)
        
        log.info(f"selected recipe is '{recipe_name}'")

        # create recipe object from the config
        kwargs = dict(**stimela.CONFIG.lib.recipes[recipe_name])
        kwargs.setdefault('name', recipe_name)
        try:
            recipe = Recipe(**kwargs)
        except Exception as exc:
            traceback.print_exc()
            log_exception(f"error loading recipe '{recipe_name}'", exc)
            sys.exit(2)

        # force name, if not set
        if not recipe.name:
            recipe.name = recipe_name

        # wrap it in an outer step and prevalidate (to set up loggers etc.)
        recipe.fqname = recipe_name
        recipe.finalize()
        
        for key, value in params.items():
            recipe.assign_value(key, value, override=True)

        # split out parameters
        params = {key: value for key, value in params.items() if key in recipe.inputs_outputs}

        stimelogging.declare_chapter("prevalidation")
        log.info("pre-validating the recipe")
        outer_step = Step(recipe=recipe, name=f"{recipe_name}", info=what, params=params)
        try:
            params = outer_step.prevalidate(root=True)
        except Exception as exc:
            log_exception(RecipeValidationError(f"pre-validation of recipe '{recipe_name}' failed", exc))
            for line in traceback.format_exc().split("\n"):
                log.debug(line)
            sys.exit(1)        

        # select recipe substeps based on command line, and exit if nothing to run
        selection_options = []
        for opts in (tags, skip_tags, step_ranges, enable_steps):
            selection_options.append(set(itertools.chain(*(opt.split(",") for opt in opts))))
        
        try:
            if not recipe.restrict_steps(*selection_options):
                sys.exit(0)
        except StepSelectionError as exc:
            log_exception(exc)
            sys.exit(2)

        logdir = stimelogging.get_logfile_dir(recipe.log) or '.'
        log.info(f"recipe logs will be saved under {logdir}")

        filename = os.path.join(logdir, "stimela.recipe.deps")
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
        task_stats.save_profiling_stats(outer_step.log, 
            print_depth=profile if profile is not None else stimela.CONFIG.opts.profile.print_depth,
            unroll_loops=stimela.CONFIG.opts.profile.unroll_loops)
        if not isinstance(exc, ScabhaBaseException) or not exc.logged:
            log_exception(StimelaRuntimeError(f"run failed after {elapsed()}", exc, 
                tb=not isinstance(exc, ScabhaBaseException)))
        else:
            log.error("run failed, exiting with error code 1")
        for line in traceback.format_exc().split("\n"):
            log.debug(line)
        outer_step.log.info(f"last log directory was [bold green]{stimelogging.get_logfile_dir(outer_step.log) or '.'}[/bold green]")
        sys.exit(1)

    if outputs and outer_step.log.isEnabledFor(logging.DEBUG):
        outer_step.log.debug(f"run successful after {elapsed()}, outputs follow:")
        for name, value in outputs.items():
            if name in recipe.outputs:
                outer_step.log.debug(f"  {name}: {value}")
    else:
        outer_step.log.info(f"run successful after {elapsed()}")

    task_stats.save_profiling_stats(outer_step.log, 
            print_depth=profile if profile is not None else stimela.CONFIG.opts.profile.print_depth,
            unroll_loops=stimela.CONFIG.opts.profile.unroll_loops)
    
    outer_step.log.info(f"last log directory was [bold green]{stimelogging.get_logfile_dir(outer_step.log) or '.'}[/bold green]")
    return 0