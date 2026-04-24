# Pydantic v1 → v2 Migration Plan

Branch: `pydanticv2`. Supersedes PR #511.

## 1. Context

Stimela currently pins `pydantic>=1.10.2,<2`. Pydantic v1 is EOL and conflicts
with modern dependencies; the upgrade is long overdue. A prior attempt
(PR #511) landed a ~200-line hand-rolled pre-validation coercion layer
that partially reimplements pydantic v2's built-in lax-mode coercion, has
unresolved review comments, and caused at least one behavioural regression
(breifast #187: `ParameterValidationError` downgrade-to-warning on skipped
steps stopped working because `TypeError` was raised outside the existing
catch).

The real pydantic surface in this codebase is remarkably small:

- **Only `scabha/validate.py`** imports `pydantic` — two call sites
  (`pydantic.dataclasses.dataclass` at line 208, `pydantic.ValidationError`
  at line 289).
- **Custom types** `URI`, `File`, `Directory`, `MS` in `scabha/basetypes.py`
  are `str` subclasses that pydantic needs a schema hook for.
- **CLI/YAML inputs** are already YAML-parsed at the command boundary
  (`stimela/commands/run.py:385` `convert_value → yaml.safe_load`), so
  pydantic receives proper Python types (bool, int, float, None) for
  scalar params — not raw strings.

With that shape, the clean migration is ~50 lines, not 200.

## 2. Design decisions

Capturing the decisions so they don't get re-litigated mid-migration.

### D1. Delegate coercion to pydantic v2's native config

Use `pydantic.ConfigDict` on `pydantic.dataclasses.dataclass`:

```python
_VALIDATION_CONFIG = pydantic.ConfigDict(
    strict=False,                 # lax mode: "1"→1, 1→1.0, "true"→True etc.
    arbitrary_types_allowed=True, # allow Parameter, Unresolved, URI-family
    coerce_numbers_to_str=True,   # 1→"1" for str-dtype fields
)
pcls = pydantic.dataclasses.dataclass(dcls, config=_VALIDATION_CONFIG)
```

**Rationale.** `strict=False` is pydantic v2's lax-coercion mode — the
closest analog to v1's default behaviour. `coerce_numbers_to_str=True`
closes the one meaningful gap in that mode (int/float → str).
`arbitrary_types_allowed=True` lets non-pydantic types (e.g.
`Unresolved`) flow through without schema hooks. This is the path JSKenyon
proposed on PR #511 and it is exactly what pydantic v2 was designed for.

We do **not** re-introduce any of the following from PR #511:

- `COERCERS` registry, `register_coercer` decorator
- `BOOL_TRUTHY`/`BOOL_FALSY`/`NONE_STRINGS` tables
- `maybe_coerce_value`, `coerce_scalar`, `coerce_homogeneous_sequence`,
  `coerce_heterogeneous_tuple`, `is_simple_dtype`, `is_any_dtype`,
  `str_to_bool`, `str_to_int`, `str_to_none`, `float_to_int`

If a specific v1→v2 behavioural gap survives the config change, handle
it with a single, targeted `BeforeValidator` on `_dtype` (see D3) — not a
bespoke parallel coercion system.

### D2. Keep URI's `__get_pydantic_core_schema__`

Port (don't copy-paste — write fresh and verify) the URI core-schema hook
from PR #511:

```python
class URI(str):
    ...
    @classmethod
    def __get_pydantic_core_schema__(cls, source, handler):
        return core_schema.no_info_after_validator_function(
            cls,
            core_schema.str_schema(),
            serialization=core_schema.plain_serializer_function_ser_schema(str),
        )
```

`File`, `Directory`, `MS` inherit the hook (classmethod binds `cls`
correctly to each subclass), so `File("path")`, `Directory("path")`,
`MS("path")` all construct the right runtime type. Add a direct test for
this (see Step 6).

**On `URI.__new__`.** PR #511 added an explicit `__new__` that just calls
`super().__new__(cls, value)`. That's a no-op over the inherited
`str.__new__` but is harmless and defensive against pydantic invoking
construction with extra kwargs in some path. **Default:** include it;
drop it only if tests pass without it.

### D3. Preserve skip-to-warning error flow

`scabha/validate.py:289–297` catches `pydantic.ValidationError`, maps each
entry to a `ParameterValidationError`, and re-raises. `stimela/kitchen/
step.py:542–563` catches `ScabhaBaseException` and downgrades to a warning
when the step is being skipped (`skip=True`).

**Rule:** every error path out of validation MUST raise
`ParameterValidationError` (or a `ScabhaBaseException` subclass), never
a bare `TypeError`/`ValueError`/`pydantic.ValidationError`. This is what
PR #511 broke in the TRON/breifast case.

Pydantic v2's `ValidationError.errors()` still exists; the dict keys
shifted slightly (`msg`, `loc`, `type`, `input`). Update the unpacking
accordingly.

### D4. Python 3.9 floor is fixed

`pyproject.toml` declares `requires-python = ">=3.9,<3.14"`. Do not use
`types.NoneType` (3.10+), `type | None` syntax, or `from __future__
import annotations` tricks that depend on PEP 604. Stick to `Optional[T]`
and `Union[...]` from `typing`. This is where PR #511's first iteration
broke 3.9 compatibility.

### D5. Bump pydantic floor to `>=2.7,<3`

`2.7` gives a stable surface (validators, `TypeAdapter`, serialisers, bool
string set). Matches the PR #511 revision after its early 3.9 fix.

### D6. Tests first, then code

Before changing any pydantic-touching code, land a characterisation test
file (`tests/scabha_tests/test_validate.py`) that pins current master
behaviour for:

- scalar coercion (bool, int, float, str, numeric ↔ str)
- Optional / Union / Literal
- `List[T]`, `List[File]`, `Tuple[...]`, `Dict[K, V]`
- URI / File / Directory / MS construction and isinstance checks
- The skipped-step downgrade path (what gets logged vs. raised)
- `Unresolved` / `UNSET` sentinels flow through without validation
- Error reporting: `ParameterValidationError` structure preserved

These tests run green against master, then must run green post-migration.
Any divergence is a conscious decision recorded in this plan.

## 3. Non-goals

Intentionally out of scope — don't get tempted:

1. **Refactoring `Parameter.__post_init__`'s `eval(self.dtype, ...)`**
   in `scabha/cargo.py`. It's a latent wart but orthogonal to pydantic.
2. **Rewriting `Cargo`, `Parameter`, `Step`, `Recipe` as pydantic
   `BaseModel`s.** They are plain `@dataclass` + OmegaConf-structured and
   should stay that way. The migration is surgical, not architectural.
3. **Relative → absolute import style cleanup.** PR #511 quietly changed
   `from .basetypes import ...` to `from scabha.basetypes import ...`
   across validate.py. Skip this — it's churn, not value.
4. **`type(x) is str` → `isinstance(x, str)` cleanups** outside
   `scabha/validate.py`. If we touch a line for pydantic reasons, we can
   modernise it; we don't go hunting.
5. **Whitespace changes in `stimela/stimela.conf`** and other touched-
   incidentally files. Do not include.
6. **Any "extended bool parsing" table** (`yes`/`no`/`on`/`off`).
   Pydantic v2's built-in bool set is what we use; CLI inputs are already
   YAML-parsed upstream so this is mostly a non-issue.

## 4. Risks and open questions

### R1. Dead-code paths surface as breaks

Some paths covered by the test suite may rely on pydantic v1's more
permissive coercion (e.g. silent list→tuple, empty-string→None, int→bool
for an int field). Expected mitigation: characterisation tests in Step 1
will catch these before the migration flips, and targeted
`BeforeValidator`s at the Parameters dataclass level resolve them.

### R2. `get_filelikes` / typeguard interaction

`scabha/basetypes.py:216–267` uses `typeguard` to probe Union branches
when recursively extracting File-likes. Pydantic v2 plays fine with
`typeguard`, but confirm `List[Union[File, Directory]]` still works after
the URI schema hook is added. Test this.

### R3. `arbitrary_types_allowed=True` with `Unresolved`

`Unresolved` / `UNSET` values bypass pydantic validation at
`validate.py:217` today (skipped from the `inputs` dict before the
dataclass is instantiated). Confirm the bypass still holds and
`arbitrary_types_allowed` is only doing work for the URI-family fallback
path.

### R4. `pydantic.ValidationError.errors()` format change

v1 → v2 keys changed. The failure message template at `validate.py:291`
reads `err["loc"]` and `err["msg"]`. Those keys both still exist in v2;
the loc tuple format shifts slightly (includes integer indexes for list
elements). Verify message output is still legible.

### R5. TRON regression

PR #511 broke skipped-step warning downgrade because
`maybe_coerce_value` raised `TypeError`. Our approach deletes that layer
entirely, so the failure mode cannot recur. Still: the breifast `tron-pfb`
recipe is the canonical smoke test; run it before declaring done
(see Step 8).

### R6. What the CLI parser does NOT coerce

`convert_value` in `stimela/commands/run.py:381` yaml-parses each assign
value. That means scalars ARE pre-typed, but nested substitutions
(`param="{recipe.foo}"`) expand to strings at substitution time, post-
CLI-parse. So pydantic still needs to coerce string → int/float for
substituted values. `strict=False` handles this natively.

## 5. Migration steps

Each step is an atomic commit. Run the full test suite after each.

### Step 0 — Setup

- Confirm branch is `pydanticv2`, rebase on latest master.
- `uv sync --group dev` to install dev deps.
- `uv run pytest tests/` to confirm master tests pass as a baseline.
- `uv run ruff check` and `uv run ruff format --check` to confirm clean.

### Step 1 — Characterisation tests (v1 baseline)

Create `tests/scabha_tests/test_validate.py`. Target coverage:

| Case                                                | Why |
|-----------------------------------------------------|-----|
| `int` dtype, value `5`                              | happy path  |
| `int` dtype, value `"5"`                            | str→int coercion |
| `int` dtype, value `"1.5"`                          | error path — not lossless |
| `float` dtype, value `1`                            | int→float coercion |
| `str` dtype, value `1`                              | int→str coercion |
| `bool` dtype, value `True`/`False`                  | happy path |
| `bool` dtype, value `"true"`/`"false"`              | str→bool coercion |
| `bool` dtype, value `"yes"`                         | document whatever v1 does (pin behaviour) |
| `Optional[int]` dtype, value `None`                 | happy path |
| `Optional[int]` dtype, value `"null"`               | document whatever v1 does |
| `List[int]` dtype, value `[1, "2", 3]`              | element coercion |
| `Tuple[int, str]` dtype, value `[1, 2]`             | heterogeneous coercion |
| `Tuple[int, ...]` dtype, value `[1, "2", 3]`        | homogeneous variadic |
| `Dict[str, int]` dtype, value `{"a": "1"}`          | dict value coercion |
| `File` dtype, value `"path/to/x"`                   | File construction |
| `List[File]` dtype, value `["a", "b"]`              | List[File] construction + isinstance |
| `Union[str, int]` dtype                             | Union handling |
| `Unresolved` value with any dtype                   | bypass validation |
| `UNSET` value with any dtype                        | bypass validation |
| Missing required param                              | `ParameterValidationError` raised |
| Failed validation with `skip=True` upstream         | downgrades to warning, no raise |
| `ValidationError` message format                    | mkname(...), value shown, msg present |

These tests run green against master HEAD. Commit them as a separate
commit so the baseline is bisectable.

### Step 2 — Bump the pydantic floor

One-line change in `pyproject.toml`:

```
pydantic>=2.7,<3
```

Regenerate lock (`uv lock`) if the project uses one. Run tests — expect
many failures; that's the point.

### Step 3 — URI pydantic core schema hook

In `scabha/basetypes.py`:

1. Add `from pydantic_core import core_schema` at the top.
2. Add defensive `URI.__new__` (see D2).
3. Add `URI.__get_pydantic_core_schema__` classmethod.
4. Nothing on File/Directory/MS — they inherit.

Verify by instantiating `File("x")`, `Directory("x")`, `MS("x")`
directly — they should return instances of the right class.

### Step 4 — Migrate `scabha/validate.py`

**In place, no file reorganisation.** Specific edits:

a. Module-level constant near the imports:
```python
_VALIDATION_CONFIG = pydantic.ConfigDict(
    strict=False,
    arbitrary_types_allowed=True,
    coerce_numbers_to_str=True,
)
```

b. `validate_parameters` — replace line 208:
```python
pcls = pydantic.dataclasses.dataclass(dcls, config=_VALIDATION_CONFIG)
```

c. Update the `ValidationError` unpacking at line 289 for v2's format:
```python
except pydantic.ValidationError as exc:
    errors = []
    for err in exc.errors():
        loc_parts = [field2name.get(x, str(x)) for x in err["loc"]]
        loc = ".".join(loc_parts)
        if loc in inputs:
            errors.append(ParameterValidationError(f"{loc} = {inputs[loc]}: {err['msg']}"))
        else:
            errors.append(ParameterValidationError(f"{loc}: {err['msg']}"))
    raise ParameterValidationError(
        f"{len(errors)} parameter(s) failed validation:", errors
    )
```
Only change: the loop body may need to coerce integer list indexes to
strings (v2's `loc` tuple can contain ints for list positions). Covered
by `str(x)` already — verify with a test case.

d. Minor: `Optional[callable]` → `Optional[Callable]` in two function
signatures (a latent bug the PR #511 fix happened to identify — it's a
legitimate fix and sits in this file).

**No new helper functions. No coercion layer. No registry.**

### Step 5 — Verify characterisation tests

Run `uv run pytest tests/scabha_tests/test_validate.py -v`. Expected:

- 80-90% pass immediately.
- A small number of edge cases (e.g. `"null"` → `None`, exotic bool
  strings) will fail because pydantic v2 is stricter than v1 in those
  corners.

For each failure: **decide, don't patch reflexively.**

- **If the behaviour was a v1 quirk no recipe depends on** → update the
  test to document v2's stricter behaviour. Note the divergence in
  §7 References.
- **If real recipes depend on it** (confirmed by grepping cult-cargo /
  breifast) → add a single targeted `BeforeValidator` on the dataclass
  field via `typing.Annotated[..., BeforeValidator(fn)]` inside the
  `make_dataclass` fields list. Keep it minimal and per-dtype.

### Step 6 — Targeted pydantic v2 tests

Add to `tests/scabha_tests/test_validate.py`:

- `File("/a/b")` is an instance of `File` (not just `URI`), after
  pydantic round-trip through `List[File]` / `Optional[File]`.
- Serialisation hook: `TypeAdapter(File).dump_python(File("/a"))` returns
  the string form.
- `ValidationError` raised from v2 surface is still caught and
  re-raised as `ParameterValidationError` with correct location strings
  including list indices.
- Skipped-step downgrade: pass a deliberately-invalid input through a
  step with `skip=True` and assert the log captures a WARNING, not an
  ERROR, and no exception propagates.

### Step 7 — Full suite + lint

```
uv run pytest tests/ -v
uv run ruff check
uv run ruff format --check
```

Both must be clean. No `# noqa` bodges.

### Step 8 — Live smoke test

Run the breifast/TRON recipe JSKenyon named in PR #511 discussion
(`/net/janis/home/kenyon/testing/breifast`, `stimela -C run
breifast.recipes::tron-pfb.yml tron -pf parameters-tron-pfb.yml`). Must
complete in ~5 min with no validation errors.

Also run the simpler test recipes the user typically exercises if any
are convenient.

### Step 9 — PR

- Title: `Pydantic v1 → v2 migration (clean)`
- Body: brief — point at this plan, call out the explicit decision to
  delegate coercion to pydantic v2 `ConfigDict` rather than a bespoke
  pre-coercion layer.
- Reference PR #511 as superseded.
- Credit JSKenyon's suggestion.

## 6. Rollback

If post-merge a critical recipe breaks that wasn't caught:

1. The migration is small (one file + one basetype + tests). A single
   `git revert` of the merge returns master to pydantic v1.
2. The characterisation tests stay as debt payment regardless of which
   direction we go.

## 7. References

- PR #511 (superseded): https://github.com/caracal-pipeline/stimela/pull/511
- Pydantic v2 migration guide: https://docs.pydantic.dev/latest/migration/
- `ConfigDict` reference: https://docs.pydantic.dev/latest/api/config/
- Relevant breifast regression: https://github.com/ratt-ru/breifast/issues/187

## 8. Acceptance checklist

- [ ] All master tests green on pydantic v2.
- [ ] `tests/scabha_tests/test_validate.py` lands in Step 1 and stays
      green through Step 5.
- [ ] New v2-specific tests in Step 6 all green.
- [ ] `ruff check` / `ruff format --check` clean.
- [ ] TRON breifast recipe runs end-to-end.
- [ ] No new module-level helper functions in `scabha/validate.py`.
      Net line count delta should be roughly zero or slightly negative.
- [ ] `pyproject.toml` has `pydantic>=2.7,<3`.
- [ ] `scabha/basetypes.py` has the URI core-schema hook, no other
      changes.
- [ ] Every v1→v2 behavioural divergence is either reflected in an
      updated test (documented quirk) or handled by a single targeted
      `BeforeValidator` (documented need). No catch-all coercion layer.
