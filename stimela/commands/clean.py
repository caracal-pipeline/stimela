import os
import shutil
import sys
import traceback
from collections import OrderedDict
from pathlib import Path
from typing import List, Tuple

import click
import yaml
from benedict import benedict
from scabha.basetypes import UNSET
from scabha.exceptions import ScabhaBaseException
from scabha.substitutions import SubstitutionNS
from scabha.validate import Unresolved

import stimela
from stimela import log_exception, logger
from stimela.exceptions import RecipeValidationError
from stimela.kitchen.recipe import Recipe, Step

from .run import load_recipe_files, resolve_recipe_files


def _collect_output_files(step, files):
    """Recursively collect output file paths from a step and its sub-steps.

    Args:
        step: A finalized and prevalidated Step object.
        files: A list to append (path_string, step_fqname) tuples to.
    """
    if step.cargo is None:
        return

    # Check this step's own output parameters
    params = step.validated_params or step.params or {}
    for name, schema in step.cargo.outputs.items():
        if schema.is_file_type or schema.is_file_list_type:
            value = params.get(name)
            if value is None or isinstance(value, (Unresolved, UNSET.__class__)):
                continue
            if isinstance(value, (list, tuple)):
                for v in value:
                    if isinstance(v, str) and v:
                        files.append((v, step.fqname))
            elif isinstance(value, str) and value:
                files.append((value, step.fqname))

    # Recurse into sub-steps if this is a recipe
    if isinstance(step.cargo, Recipe):
        for label, substep in step.cargo.steps.items():
            _collect_output_files(substep, files)


@click.command(
    "clean",
    help="""
    Remove output files produced by a recipe, so it can be re-run from scratch.
    Loads the recipe, resolves output file parameters, and deletes them.
    Use --dry-run to list files without deleting.
    """,
    no_args_is_help=True,
)
@click.option(
    "-d",
    "--dry-run",
    is_flag=True,
    help="List output files without deleting them.",
)
@click.option(
    "-l",
    "--last-recipe",
    is_flag=True,
    help="If multiple recipes are defined, selects the last one.",
)
@click.option(
    "-a",
    "--assign",
    metavar="PARAM VALUE",
    nargs=2,
    multiple=True,
    help="Assigns values to parameters: equivalent to PARAM=VALUE.",
)
@click.option(
    "-pf",
    "--parameter-file",
    metavar="filename.yml",
    multiple=True,
    help="Use parameter values from the specified parameter file.",
)
@click.argument("parameters", nargs=-1, metavar="filename.yml ... [recipe name] [PARAM=VALUE] ...", required=True)
def clean(
    parameters: List[str] = [],
    dry_run: bool = False,
    last_recipe: bool = False,
    assign: List[Tuple[str, str]] = [],
    parameter_file: List[str] = [],
):
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

    # Load parameter files
    for pf in parameter_file:
        if not Path(pf).exists():
            raise FileNotFoundError(f"{pf} is not a path to a valid parameter file.")
        pf_dict = benedict.from_yaml(pf, keypath_separator=">>").flatten(separator=".")
        for key, value in pf_dict.items():
            try:
                params[key] = value
            except Exception as exc:
                log_exception(f"error parsing {key} {value} in parameter file {pf}", exc)
                errcode = 2

    # parse assign values as YAML
    for key, value in assign:
        try:
            params[key] = convert_value(value)
        except Exception as exc:
            log_exception(f"error parsing value for --assign {key} {value}", exc)
            errcode = 2

    # parse arguments as recipe name, parameter assignments, or files
    for pp in parameters:
        if "=" in pp:
            key, value = pp.split("=", 1)
            try:
                params[key] = convert_value(value)
            except Exception as exc:
                log_exception(f"error parsing {pp}", exc)
                errcode = 2
        else:
            try:
                filenames = resolve_recipe_files(pp, log=log)
            except FileNotFoundError as exc:
                log_exception(exc)
                errcode = 2
                continue
            if filenames is not None:
                files_to_load += filenames
                log.info(f"will load recipe/config file(s) {', '.join(filenames)}")
            elif recipe_or_cab is not None:
                log_exception("multiple recipe names given")
                errcode = 2
            else:
                recipe_or_cab = pp
                log.info(f"treating '{pp}' as a recipe name")

    if errcode:
        sys.exit(errcode)

    # load config and recipes from all given files
    if files_to_load:
        available_recipes, default_name = load_recipe_files(files_to_load)
    else:
        available_recipes, default_name = [], None

    if recipe_or_cab is None and default_name is not None and not last_recipe:
        log.info(f"using '{default_name}' as the default recipe")
        recipe_or_cab = default_name

    # figure out which recipe to clean
    recipe_name = None
    if recipe_or_cab is None:
        if last_recipe:
            if not available_recipes:
                log.error("-l/--last-recipe specified, but no valid recipes were loaded")
                sys.exit(2)
            else:
                recipe_name = available_recipes[-1]
                log.info(f"-l/--last-recipe specified, selecting '{recipe_name}'")
        elif len(available_recipes) == 1:
            recipe_name = available_recipes[0]
            log.info(f"found single recipe '{recipe_name}', selecting it implicitly")
        else:
            log.error("found multiple recipes, please specify one on the command line")
            if available_recipes:
                log.info(f"available recipes: {' '.join(available_recipes)}")
            sys.exit(2)
    elif recipe_or_cab in stimela.CONFIG.lib.recipes:
        recipe_name = recipe_or_cab
        log.info(f"selected recipe is '{recipe_name}'")
    else:
        if not available_recipes:
            log.error("no valid recipes were loaded")
        else:
            log.error(f"'{recipe_or_cab}' does not refer to a recipe")
            log.info(f"available recipes: {' '.join(available_recipes)}")
        sys.exit(2)

    # create recipe object and finalize
    kwargs = dict(**stimela.CONFIG.lib.recipes[recipe_name])
    kwargs.setdefault("name", recipe_name)
    try:
        recipe = Recipe(**kwargs)
    except Exception as exc:
        traceback.print_exc()
        log_exception(f"error loading recipe '{recipe_name}'", exc)
        sys.exit(2)

    if not recipe.name:
        recipe.name = recipe_name

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

    # create subst namespace and prevalidate
    subst = SubstitutionNS()
    info = SubstitutionNS(
        fqname=recipe_name, label=recipe_name, label_parts=[recipe_name], suffix="", taskname=recipe_name
    )
    subst._add_("info", info)
    subst._add_("self", info)
    subst._add_("config", stimela.CONFIG, nosubst=True)
    subst._add_("current", SubstitutionNS(**params))

    recipe_params = {key: value for key, value in params.items() if key in recipe.inputs_outputs}
    outer_step = Step(recipe=recipe, name=recipe_name, info=recipe_name, params=recipe_params)
    try:
        outer_step.prevalidate(root=True, subst=subst)
    except Exception as exc:
        log_exception(RecipeValidationError(f"pre-validation of recipe '{recipe_name}' failed", exc))
        for line in traceback.format_exc().split("\n"):
            log.debug(line)
        sys.exit(1)

    # collect output files from all steps
    output_files = []
    _collect_output_files(outer_step, output_files)

    if not output_files:
        log.info("no output files found in recipe")
        return 0

    # remove duplicates while preserving order
    seen = set()
    unique_files = []
    for filepath, step_fqname in output_files:
        if filepath not in seen:
            seen.add(filepath)
            unique_files.append((filepath, step_fqname))

    if dry_run:
        log.info(f"would remove {len(unique_files)} output file(s):")
        for filepath, step_fqname in unique_files:
            exists = os.path.exists(filepath)
            status = "" if exists else " [not found]"
            log.info(f"  {filepath} (from {step_fqname}){status}")
        return 0

    # reject dangerous paths
    _dangerous_paths = {"", ".", "..", "/"}

    # delete output files
    removed = 0
    for filepath, step_fqname in unique_files:
        if filepath in _dangerous_paths or os.path.normpath(filepath) in _dangerous_paths:
            log.warning(f"skipping dangerous path {filepath!r} (from {step_fqname})")
            continue
        if os.path.islink(filepath):
            # remove symlinks directly, never follow into directories
            try:
                os.remove(filepath)
                log.info(f"removed symlink {filepath} (from {step_fqname})")
                removed += 1
            except OSError as exc:
                log.warning(f"failed to remove symlink {filepath}: {exc}")
        elif os.path.isdir(filepath):
            try:
                shutil.rmtree(filepath)
                log.info(f"removed directory {filepath} (from {step_fqname})")
                removed += 1
            except OSError as exc:
                log.warning(f"failed to remove directory {filepath}: {exc}")
        elif os.path.isfile(filepath):
            try:
                os.remove(filepath)
                log.info(f"removed {filepath} (from {step_fqname})")
                removed += 1
            except OSError as exc:
                log.warning(f"failed to remove {filepath}: {exc}")
        else:
            log.debug(f"skipping {filepath} (from {step_fqname}): does not exist")

    log.info(f"removed {removed} of {len(unique_files)} output file(s)")
    return 0
