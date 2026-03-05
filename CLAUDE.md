# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Stimela 2.x is a workflow management framework for radio interferometry data processing pipelines. It orchestrates execution of processing steps (cabs) within recipes, supporting multiple container backends (Singularity, Kubernetes, SLURM) and native execution.

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

## Architecture

Two packages live in this repo:

- **scabha** — low-level parameter handling, validation, and substitution engine
- **stimela** — workflow orchestration built on top of scabha

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

Pluggable execution engines: `native`, `singularity`, `kube` (Kubernetes), `slurm`. Each provides `run()` and `build()` methods. Backend selection cascades: global config → recipe → step.

### Configuration

Uses **OmegaConf** structured configs extensively. Config loaded from (in order): bundled `stimela.conf`, local `./stimela.conf`, virtualenv, `~/.stimela/`, cultcargo package, `~/.config/stimela.conf`.

### Key Patterns

- All core classes are `@dataclass` with OmegaConf structured config integration. Field defaults use `EmptyDictDefault()` / `EmptyListDefault()` from `stimela/config.py`.
- Parameter substitution uses `{recipe.param}` / `{.local}` syntax, evaluated by `scabha/substitutions.py` and `scabha/evaluator.py`.
- `evaluate_and_substitute_object()` in `scabha/validate.py` recursively resolves substitutions and formulas in OmegaConf trees.
- When converting between OmegaConf DictConfig and plain Python objects, use `OmegaConf.structured()` for dataclass instances and `OmegaConf.create()` for plain dicts. Objects must be DictConfig before calling `OmegaConf.merge()`.
- Exception hierarchy rooted at `ScabhaBaseException` (scabha) and `StimelaBaseException` (stimela), with specific subclasses for validation, backend, and runtime errors.
- Rich library used for terminal display and progress tracking.
