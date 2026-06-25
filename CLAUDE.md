# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Stimela 2.x is a workflow management framework for radio interferometry data processing pipelines. It orchestrates execution of processing steps (cabs) within recipes, supporting multiple container backends (Singularity/Apptainer, Kubernetes, SLURM) and native execution.

## Build & Development Commands

```bash
# Install (dev environment with uv)
uv sync --group dev
uv run pre-commit install

# Lint and format (ruff)
uv run ruff check                          # lint
uv run ruff check --fix                    # lint with auto-fix
uv run ruff format                         # format

# Tests
uv run pytest tests/                       # all tests
uv run pytest tests/scabha_tests/          # scabha tests only
uv run pytest tests/stimela_tests/         # stimela tests only
uv run pytest tests/stimela_tests/test_recipe.py           # single file
uv run pytest tests/stimela_tests/test_recipe.py::test_test_aliasing  # single test

# CLI entry point
stimela -v -b native exec <recipe.yml> [recipe_name] [param=value ...]
```

**Ruff config** is in `ruff.toml`: line-length 120, import sorting enabled (isort). Pre-commit hooks run ruff-check and ruff-format automatically.

**Known test issue**: `test_test_recipe` fails on macOS because it references `/bin/true` which lives at `/usr/bin/true` on macOS. This is a pre-existing issue, not caused by your changes.

## Architecture

### Package Relationship

- **scabha** — separate PyPI package (`caracal-pipeline/scabha`), low-level parameter handling, validation, and substitution engine
- **stimela** — this repo, workflow orchestration built on top of scabha

Scabha is a dependency, not part of this repo. Changes to scabha require a separate PR at https://github.com/caracal-pipeline/scabha. The "divorce" between the two packages is structurally complete (no circular imports), though some naming vestiges remain.

### Core Abstractions (in `stimela/kitchen/`)

- **Cargo** (`scabha/cargo.py`): Base class defining parameter schemas (inputs/outputs), defaults, and validation. Both Cab and Recipe inherit from it.
- **Cab** (`stimela/kitchen/cab.py`): Atomic processing unit — wraps a shell command or Python callable with typed input/output parameters.
- **Recipe** (`stimela/kitchen/recipe.py`): Ordered sequence of Steps with parameter aliasing, variable assignment, for-loops (scatter/gather), and conditional execution.
- **Step** (`stimela/kitchen/step.py`): Wrapper that binds a Cab or nested Recipe into a recipe, handling parameter passing, skip logic, and backend selection.

### Execution Flow

1. **Finalize**: Steps resolve cab/recipe references, flatten parameters, apply dynamic schemas
2. **Prevalidate**: Parameter types checked, defaults applied, substitutions partially resolved
3. **Run**: Inputs validated → backend executes cab → outputs validated. Variable substitution uses `{name}` syntax via `SubstitutionNS`.

### Backends (`stimela/backends/`)

Pluggable execution engines: `native`, `apptainer` (formerly `singularity`), `kube` (Kubernetes), `slurm`. Each provides `run()` and `build()` methods. Backend selection cascades: global config → recipe → step.

All backends must apply `cab.management.environment` variables. The native backend passes them via subprocess `env=`, singularity/apptainer via `--env` flags, and kube merges them into `kube.env` before pod creation.

### Configuration

Uses **OmegaConf** structured configs extensively. Config loaded from (in order): bundled `stimela.conf`, local `./stimela.conf`, virtualenv, `~/.stimela/`, cultcargo package, `~/.config/stimela.conf`.

### Key Patterns

- All core classes are `@dataclass` with OmegaConf structured config integration. Field defaults use `EmptyDictDefault()` / `EmptyListDefault()` from `stimela/config.py`.
- Parameter substitution uses `{recipe.param}` / `{.local}` syntax, evaluated by `scabha/substitutions.py` and `scabha/evaluator.py`.
- `evaluate_and_substitute_object()` in `scabha/validate.py` recursively resolves substitutions and formulas in OmegaConf trees.
- When converting between OmegaConf DictConfig and plain Python objects, use `OmegaConf.structured()` for dataclass instances and `OmegaConf.create()` for plain dicts. Objects must be DictConfig before calling `OmegaConf.merge()`.
- Exception hierarchy rooted at `ScabhaBaseException` (scabha) and `StimelaBaseException` (stimela), with specific subclasses for validation, backend, and runtime errors.
- Rich library used for terminal display and progress tracking.

## File Dependency Map for Parallel Work

When fixing multiple issues in parallel, avoid editing the same file from different branches. Key file groupings:

| Group | Files | Safe to parallelize with other groups |
|-------|-------|---------------------------------------|
| Recipe | `kitchen/recipe.py` | Yes, but serialize changes within this file |
| Step | `kitchen/step.py` | Yes |
| Cab/Flavours | `kitchen/cab.py`, `backends/flavours/python_flavours.py`, `backends/flavours/__init__.py` | Yes |
| Native backend | `backends/native/run_native.py` | Yes |
| Singularity/Apptainer | `backends/singularity.py` | Yes |
| Kube backend | `backends/kube/*.py` | Yes |
| CLI commands | `commands/*.py`, `main.py` | Yes |
| Logging | `stimelogging.py` | Yes |
| Config | `config.py` | Yes |
| Scabha (separate repo) | `scabha/validate.py`, `scabha/evaluator.py`, `scabha/substitutions.py`, `scabha/schema_utils.py` | Requires separate PR to caracal-pipeline/scabha |

## Issue Triage Labels

The project uses these labels for issue triage (established June 2026 sprint):

- **sprint** — active working set, implementable
- **death row** — stale or invalid, should be closed
- **pink pony** — too vague or under-specified to implement
- **humans help!** — needs human decision-making, Claude can't resolve alone

## Working with Claude Code on This Project

### Sprint Workflow

Follow the methodology in Discussion #566:

1. **Triage**: For each unmilestoned issue, assign a label (sprint/death row/pink pony/humans help!) and comment explaining the decision.
2. **For sprint issues**, decide approach:
   - Simple fix + test coverage → branch `issue-XYZ`, PR
   - Simple fix + no tests → tests first on `issue-XYZ-tests`, then fix
   - Needs rearchitecting → propose plan on issue, label "humans help!"
3. **Always add tests** for new features and bug fixes.
4. **Request Copilot review** after creating PRs (if enabled on repo), then address comments.

### Parallel Agent Best Practices

- **Use worktree isolation** (`isolation: "worktree"`) for parallel agents that edit code — prevents file conflicts.
- **Group by file**: Issues touching the same file must be serialized in one agent. Independent files can run in parallel.
- **Don't use forks for narrow tasks** when the conversation has broad context. Forks inherit your full context and may go beyond their brief. Use `general-purpose` agents with specific prompts instead.
- **Include tests in initial instructions**. Don't send agents back for tests — it wastes a round trip.
- **Include review workflow in instructions**: "After creating PR, reply to any Copilot review comments."
- **Clean up worktrees** after agents finish: `git worktree remove <path> --force`.

### Scabha Changes

Many stimela issues require changes in scabha (the parameter validation library). Since scabha is a separate repo:

1. Check if scabha is installed as editable: `pip show scabha` — look at the Location field
2. If editable, make changes directly and create a PR on `caracal-pipeline/scabha`
3. If not editable, document the needed scabha changes on the stimela issue and create a corresponding scabha issue
4. Some agents have used monkey-patching as a workaround — this works but the proper fix should go into scabha

### Common Pitfalls

- **`recipe.py` is a god class** (1476 lines). Many issues touch it. Serialize changes to avoid merge conflicts.
- **OmegaConf everywhere**: Remember to check whether you have a `DictConfig` or a plain dict. Use `OmegaConf.structured()` for dataclass instances.
- **Sentinel types**: The codebase has multiple "not set" sentinels (`UNSET`, `Unresolved`, `Placeholder`, `SkippedOutput`). `UNSET` is a subclass of `Unresolved`. Use `isinstance(value, Unresolved)` to catch all, or `type(value) is Unresolved` for exact match.
- **The substitution engine has known complexity**: `{recipe.param}` interpolation, `=formula` evaluation, and `{{` escaping interact in subtle ways. Test thoroughly.

## Sprint Progress (June 2026)

A comprehensive bugfix sprint was conducted in June 2026. See Discussion #566 (bugfix sprint) and #567 (prototyping sprint) for context. Key outcomes:

- 68 issues triaged with labels and comments
- 40+ issues addressed across 23 PRs (stimela) and 4 PRs (scabha)
- 150+ new tests added
- All milestoned issues (R2.2, R2.2.1, R2.3) addressed
- Architecture analysis documents at https://github.com/gijzelaerr/agentic-astro/tree/main/stimela-analysis
