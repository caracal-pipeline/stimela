import itertools
import click
import logging
import os.path
import yaml
import sys
import traceback
import re
import importlib
from datetime import datetime
from typing import List, Optional, Tuple
from collections import OrderedDict
from omegaconf.omegaconf import OmegaConf, OmegaConfBaseException


import stimela
from scabha import configuratt
from scabha.basetypes import UNSET
from scabha.exceptions import ScabhaBaseException
from scabha.substitutions import SubstitutionNS
from stimela import stimelogging
import stimela.config
from stimela.config import ConfigExceptionTypes
from stimela import logger, log_exception
from stimela.exceptions import RecipeValidationError, StimelaRuntimeError, StepSelectionError, StepValidationError
from stimela.main import cli
from stimela.kitchen.recipe import Recipe, Step, RecipeSchema, join_quote
from stimela import task_stats
import stimela.backends

_yaml_extensions = {".yml", ".yaml", ".YML", ".YAML"}

def resolve_recipe_file(filename: str):
    """
    Resolves a recipe file, which may be specified as (module)recipe.yml or module::recipe.yml, with
    the suffix being optional.

    Returns real path if file resolved, or None if filename should not be treated as a recipe file.

    Raises FileNotFoundError if filename is a recipe file that doesn't exist.
    """
    ext = os.path.splitext(filename)[1].lower()
    # unrecognized extension -- treat as non-filename
    if ext and ext not in _yaml_extensions:
        return None

    # check for (location)filename.yml or (location)/filename.yml style
    match1 = re.fullmatch("^\\((.+)\\)/?(.+)$", filename)
    match2 = re.fullmatch("^([\w.]+)::(.+)$", filename)
    if match1 or match2:
        modulename, fname = (match1 or match2).groups()
        try:
            mod = importlib.import_module(modulename)
        except ImportError as exc:
            raise FileNotFoundError(f"{filename} not found ({exc})")
        # get filename
        fname = os.path.join(os.path.dirname(mod.__file__), fname)
        if ext:
            if os.path.exists(fname):
                return fname
            else:
                raise FileNotFoundError(f"{filename} resolves to {fname}, which doesn't exist")
        # else check for implicit extension        
        else:
            for ext in _yaml_extensions:
                path = f"{fname}{ext}"
                if os.path.exists(path):
                    return path
            else:
                raise FileNotFoundError(f"{filename} resolves to {fname}, which doesn't match any YaML files")
    # no match and no extension, treat as non-filename
    if not ext:
        return None
    if os.path.exists(filename):
        return filename
    raise FileNotFoundError(f"{filename} doesn't exist")


def load_recipe_files(filenames: List[str]):
    """Loads a set of config or recipe files. Returns list of recipes loaded."""

    full_conf = OmegaConf.create()
    full_deps = configuratt.ConfigDependencies()
    for filename in filenames:
        # check for (location)filename.yaml or (location)/filename.yaml style
        match1 = re.fullmatch("^\\((.+)\\)/?(.+)$", filename)
        match2 = re.fullmatch("^([\w.]+)::(.+)$", filename)
        if match1 or match2:
            modulename, filename = (match1 or match2).groups()
            try:
                mod = importlib.import_module(modulename)
            except ImportError as exc:
                log_exception(f"error importing {modulename}", exc)
                sys.exit(2)
            filename = os.path.join(os.path.dirname(mod.__file__), filename)
        # try loading
        try:
            conf, deps = configuratt.load(filename, use_sources=[stimela.CONFIG, full_conf], no_toplevel_cache=True)
        except FileNotFoundError as exc:
            log_exception(exc)
            sys.exit(2)
        except ConfigExceptionTypes as exc:
            log_exception(f"error loading {filename}", exc)
            sys.exit(2)
        # accumulate loaded config
        full_conf.merge_with(conf)
        full_deps.update(deps)

    # warn user if any includes failed
    if full_deps.fails:
        logger().warning(f"{len(full_deps.fails)} optional includes were not found, some cabs may not be available")
        for path, dep in full_deps.fails.items():
            logger().warning(f"    {path} (from {dep.origin})")

    # merge into full config dependencies
    dependencies = stimela.config.get_initial_deps()
    dependencies.update(full_deps)
    stimela.config.CONFIG_DEPS.update(dependencies)

    # check for missing requirements
    missing = configuratt.check_requirements(full_conf, [stimela.CONFIG], strict=True)
    for (loc, name, _) in missing:
        logger().warning(f"optional config section '{loc}' omitted due to missing requirement '{name}'")

    # split content into config sections, and recipes:
    # config secions are merged into the config namespace, while recipes go under
    # lib.recipes
    recipe_names = []
    update_conf = OmegaConf.create()
    for name, value in full_conf.items():
        if name in stimela.CONFIG:
            update_conf[name] = value
        else:
            try:
                stimela.CONFIG.lib.recipes[name] = OmegaConf.merge(RecipeSchema, value)
            except Exception as exc:
                log_exception(f"error in definition of recipe '{name}'", exc)
                sys.exit(2)
            recipe_names.append(name)
    
    try:
        stimela.CONFIG.merge_with(update_conf)
    except Exception as exc:
        log_exception(f"error applying configuration from {' ,'.join(filenames)}", exc)
        sys.exit(2)

    return recipe_names

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
@click.option("-c", "--config", "config_equals", metavar="X.Y.Z=VALUE", nargs=1, multiple=True,
                help="""tweak configuration options.""")
@click.option("-a", "--assign", metavar="PARAM VALUE", nargs=2, multiple=True,
                help="""assigns values to parameters: equivalent to PARAM=VALUE, but plays nicer with the shell's 
                tab completion feature.""")
@click.option("-C", "--config-assign", metavar="X.Y.Z VALUE", nargs=2, multiple=True,
                help="""tweak configuration options: same function -c/--config, but plays nicer with the shell's 
                tab completion feature.""")
@click.option("-l", "--last-recipe", is_flag=True,
                help="""if multiple recipes are defined, selects the last one for execution.""")
@click.option("-d", "--dry-run", is_flag=True,
                help="""Doesn't actually run anything, only prints the selected steps.""")
@click.option("-p", "--profile", metavar="DEPTH", type=int,
                help="""Print per-step profiling stats to this depth. 0 disables.""")
@click.option("-N", "--native", "enable_native", is_flag=True,
                help="""Selects the native backend (shortcut for -C opts.backend.select=native)""")
@click.option("-S", "--singularity", "enable_singularity", is_flag=True,
                help="""Selects the singularity backend (shortcut for -C opts.backend.select=singularity)""")
@click.option("-K", "--kube", "enable_kube", is_flag=True,
                help="""Selects the kubernetes backend (shortcut for -C opts.backend.select=kube)""")
@click.option("--slurm", "enable_slurm", is_flag=True,
                help="""Enables the slurm backend wrapper (shortcut for -C backend.slurm.enable=True)""")
@click.argument("parameters", nargs=-1, metavar="filename.yml ... [recipe or cab name] [PARAM=VALUE] ...", required=True) 
def run(parameters: List[str] = [], dry_run: bool = False, last_recipe: bool = False, profile: Optional[int] = None,
    assign: List[Tuple[str, str]] = [],
    config_equals: List[str] = [],
    config_assign: List[Tuple[str, str]] = [],
    step_ranges: List[str] = [], tags: List[str] = [], skip_tags: List[str] = [], enable_steps: List[str] = [],
    build=False, rebuild=False, build_skips=False,
    enable_native=False,
    enable_singularity=False,
    enable_kube=False,
    enable_slurm=False):

    log = logger()
    params = OrderedDict()
    errcode = 0
    recipe_or_cab = None
    files_to_load = []

    def convert_value(value):
        if value == "=UNSET":
            return UNSET
        else:
            return yaml.safe_load(value)

    # parse assign values as YaML
    for key, value in assign:
        # parse string as yaml value
        try:
            params[key] = convert_value(value)
        except Exception as exc:
            log_exception(f"error parsing value for --assign {key} {value}", exc)
            errcode = 2

    # parse arguments as recipe name, parameter assignments, or dotlist for OmegaConf    
    for pp in parameters:
        if "=" in pp:
            key, value = pp.split("=", 1)
            # parse string as yaml value
            try:
                params[key] = convert_value(value)
            except Exception as exc:
                log_exception(f"error parsing {pp}", exc)
                errcode = 2
        else:
            try:
                filename = resolve_recipe_file(pp)
            except FileNotFoundError as exc:
                log_exception(exc)
                errcode = 2
                continue
            if filename:
                files_to_load.append(filename)
                log.info(f"will load recipe/config file {filename}")
            elif recipe_or_cab is not None:
                log_exception(f"multiple recipe/cab names given")
                errcode = 2
            else:
                recipe_or_cab = pp
                log.info(f"treating '{pp}' as a recipe or cab name")

    if errcode:
        sys.exit(errcode)

    # load config and recipes from all given files
    if files_to_load:
        available_recipes = load_recipe_files(files_to_load)
    else:
        available_recipes = []

    # load config settigs from --config arguments
    try:
        stimela.CONFIG.merge_with(OmegaConf.from_dotlist(config_equals))
    except OmegaConfBaseException as exc:
        log_exception(f"error loading -c/--config assignments", exc)
        sys.exit(2)
    try:
        dotlist = [f"{key}={value}" for key, value in config_assign]
        stimela.CONFIG.merge_with(OmegaConf.from_dotlist(dotlist))
    except OmegaConfBaseException as exc:
        log_exception(f"error loading -C/--config-assign assignments", exc)
        sys.exit(2)

    # enable backends
    if enable_native:
        log.info("selecting the native backend")
        stimela.CONFIG.opts.backend.select = 'native'
    elif enable_singularity:
        log.info("selecting the singularity backend")
        stimela.CONFIG.opts.backend.select = 'singularity'
    elif enable_kube:
        log.info("selecting the kube backend")
        stimela.CONFIG.opts.backend.select = 'kube'
    if enable_slurm:
        log.info("enabling the slurm backend wrapper")
        stimela.CONFIG.opts.backend.slurm.enable = True

    def log_available_runnables():
        """Helper function to list available recipes or cabs"""
        if available_recipes:
            log.info(f"available recipes: {' '.join(available_recipes)}")
        if stimela.CONFIG.cabs:
            log.info(f"available cabs: {' '.join(stimela.CONFIG.cabs.keys())}")

    # figure out what we're running, recipe or cab
    recipe_name = cab_name = None
    # do we need to make an implicit choice?
    if recipe_or_cab is None:
        # -l specified, pick the last recipe
        if last_recipe:
            if not available_recipes:
                log.error(f"-l/--last-recipe specified, but no valid recipes were loaded")
                sys.exit(2)
            else:
                recipe_name = available_recipes[-1]
                log.info(f"-l/--last-recipe specified, selecting '{recipe_name}'")
        # nothing specified, either we have exactly 1 recipe defined (pick that), or 0 recipes and 1 cab 
        elif len(available_recipes) == 1:
            recipe_name = available_recipes[0]
            log.info(f"found single recipe '{recipe_name}', selecting it implicitly")
        elif len(stimela.CONFIG.cabs) == 1 and not available_recipes:
            cab_name = next(iter(stimela.CONFIG.cabs))
            log.info(f"found single cab '{cab_name}', selecting it implicitly")
        else:
            log.error("found multiple recipes or cabs, please specify one on the command line")
            log_available_runnables()
            sys.exit(2)
    # else something was specified
    elif recipe_or_cab in available_recipes:
        recipe_name = recipe_or_cab
        log.info(f"selected recipe is '{recipe_name}'")
    elif recipe_or_cab in stimela.CONFIG.cabs:
        cab_name = recipe_or_cab
        log.info(f"selected cab is '{cab_name}'")
    else:
        if not available_recipes and not stimela.CONFIG.cabs:
            log.error("no valid recipes or cabs were loaded")
        else:
            log.error(f"'{recipe_or_cab}' does not refer to a recipe or a cab")
            log_available_runnables()
        sys.exit(2)

    # are we running a standalone cab?
    if cab_name is not None:
        # create step config by merging in settings (var=value pairs from the command line) 
        outer_step = Step(cab=cab_name, params=params)
        outer_step.name = cab_name
        # provide basic substitutions for running the step below
        subst = SubstitutionNS()
        info = SubstitutionNS(fqname=cab_name, label=cab_name, label_parts=[], suffix='', taskname=cab_name)
        subst._add_('info', info, nosubst=True)
        subst._add_('config', stimela.CONFIG, nosubst=True) 
        subst._add_('current', SubstitutionNS(**params))
        # create step logger manually, since we won't be doing the normal recipe-level log management
        step_logger = stimela.logger().getChild(cab_name)
        step_logger.propagate = True
        try:
            outer_step.finalize(fqname=cab_name, log=step_logger)
            outer_step.prevalidate(root=True, subst=subst)
        except ScabhaBaseException as exc:
            log_exception(exc)
            sys.exit(1)
        # check for missing parameters
        if not build and (outer_step.missing_params or outer_step.unresolved_params):
            missing = {}
            for name in outer_step.missing_params:
                missing[name] = outer_step.inputs_outputs[name].info
            # don't report unresolved implicits, since that's just a consequence of a missing input
            for name in outer_step.unresolved_params: 
                if not outer_step.inputs_outputs[name].implicit:
                    missing[name] = outer_step.inputs_outputs[name].info
            #
            if missing:
                log_exception(StepValidationError(f"cab '{cab_name}' is missing required parameter(s)", missing))
                sys.exit(1)

    # else run a recipe
    else:
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
        try:
            recipe.finalize()
        except Exception as exc:
            log_exception(RecipeValidationError(f"error validating recipe '{recipe_name}'", exc))
            for line in traceback.format_exc().split("\n"):
                log.debug(line)
            sys.exit(1)        
        
        for key, value in params.items():
            try:
                recipe.assign_value(key, value, override=True)
            except ScabhaBaseException as exc:
                log_exception(exc)
                sys.exit(1)

        # split out parameters
        params = {key: value for key, value in params.items() if key in recipe.inputs_outputs}

        stimelogging.declare_chapter("prevalidation")
        log.info("pre-validating the recipe")
        outer_step = Step(recipe=recipe, name=f"{recipe_name}", info=recipe_name, params=params)
        try:
            params = outer_step.prevalidate(root=True)
        except Exception as exc:
            log_exception(RecipeValidationError(f"pre-validation of recipe '{recipe_name}' failed", exc))
            for line in traceback.format_exc().split("\n"):
                log.debug(line)
            sys.exit(1)        

        # select recipe substeps based on command line, and exit if nothing to run
        if not build_skips: 
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
        stimela.config.CONFIG_DEPS.save(filename)
        log.info(f"saved recipe dependencies to {filename}")

        # no substitutions provided, recipe initializes its own
        subst = None

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

    # build the images
    if build:
        try:
            outer_step.build(rebuild=rebuild, build_skips=build_skips, log=log)
        except Exception as exc:
            stimela.backends.close_backends(log)

            if not isinstance(exc, ScabhaBaseException) or not exc.logged:
                log_exception(StimelaRuntimeError(f"build failed after {elapsed()}", exc, 
                    tb=not isinstance(exc, ScabhaBaseException)))
            else:
                log.error("build failed, exiting with error code 1")
            for line in traceback.format_exc().split("\n"):
                log.debug(line)
            last_log_dir = stimelogging.get_logfile_dir(outer_step.log) or '.'
            outer_step.log.info(f"last log directory was {stimelogging.apply_style(last_log_dir, 'bold green')}")
            sys.exit(1)

    # else run the recipe
    else:
        try:
            outputs = outer_step.run(is_outer_step=True, subst=subst)
        except Exception as exc:
            stimela.backends.close_backends(log)

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
            last_log_dir = stimelogging.get_logfile_dir(outer_step.log) or '.'
            outer_step.log.info(f"last log directory was {stimelogging.apply_style(last_log_dir, 'bold green')}")
            sys.exit(1)

        if outputs and outer_step.log.isEnabledFor(logging.DEBUG):
            outer_step.log.debug(f"run successful after {elapsed()}, outputs follow:")
            for name, value in outputs.items():
                if name in recipe.outputs:
                    outer_step.log.debug(f"  {name}: {value}")
        else:
            outer_step.log.info(f"run successful after {elapsed()}")

    stimela.backends.close_backends(log)

    if not build:
        task_stats.save_profiling_stats(outer_step.log, 
                print_depth=profile if profile is not None else stimela.CONFIG.opts.profile.print_depth,
                unroll_loops=stimela.CONFIG.opts.profile.unroll_loops)
    
    last_log_dir = stimelogging.get_logfile_dir(outer_step.log) or '.'
    outer_step.log.info(f"last log directory was {stimelogging.apply_style(last_log_dir, 'bold green')}")
    return 0
    