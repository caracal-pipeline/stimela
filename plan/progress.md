# Pydantic v2 Migration — Progress Log

Working branch: `pydanticv2`. Plan: `plan/migrate_pydantic.md`.

## Current state

**Step 6 — Targeted pydantic v2 tests:** Next.

## Step status

| # | Step                                    | Status | Commit / Notes |
|---|-----------------------------------------|--------|----------------|
| 0 | Setup + baseline tests green            | DONE   | 76/76 pytest pass, ruff clean, pydantic 1.10.26 |
| 1 | Characterisation tests (v1 baseline)    | DONE   | 41 tests, commit 47064c1 |
| 2 | Bump pydantic floor to `>=2.7,<3`       | DONE   | pydantic 2.13.3, pydantic-core 2.46.3 |
| 3 | URI `__get_pydantic_core_schema__`      | DONE   | inherits to File/Directory/MS |
| 4 | Migrate `scabha/validate.py`            | DONE   | ConfigDict(strict=False, arbitrary_types_allowed=True, coerce_numbers_to_str=True) |
| 5 | Characterisation tests green on v2      | DONE   | 41/41 pass |
| 6 | Targeted pydantic v2 tests              | TODO   |  |
| 7 | Full suite + ruff clean                 | DONE   | 117/117 pass, ruff clean |
| 8 | Live TRON/breifast smoke test           | TODO   |  |
| 9 | PR opened                               | TODO   |  |

## Pick-up-where-we-left-off log

When resuming, first:

1. `git -C /home/bester/software/stimela status && git log --oneline -5`
2. Read this file (latest entry).
3. Read the matching Step in `plan/migrate_pydantic.md`.
4. Confirm assumptions are still valid before writing code.

### Entries

_(Newest first. Prepend a new block each time you stop.)_

---

#### Steps 2-5, 7 — Core migration landed

- Bumped pydantic to `>=2.7,<3` in pyproject.toml, pydantic 2.13.3 now active.
- Added `URI.__get_pydantic_core_schema__` using
  `core_schema.no_info_after_validator_function(cls, str_schema)` with a
  plain `str` serializer. Inherited by File/Directory/MS via normal
  classmethod binding on `cls`.
- Added `_VALIDATION_CONFIG = pydantic.ConfigDict(strict=False,
  arbitrary_types_allowed=True, coerce_numbers_to_str=True)` in
  `scabha/validate.py` and passed it to `pydantic.dataclasses.dataclass`.
  Also fixed `Optional[callable]` → `Optional[Callable]` latent typo in
  the same file.
- **v1→v2 difference surfaced and handled:** pydantic v2's URI schema
  hook constructs real `File`/`Directory`/`MS` instances as output values
  (v1 left them as plain strs). Downstream sites that used
  `type(x) is str` or `type(x) is not str` rejected these str subclasses.
  Updated five sites to `isinstance(x, str)` — the correct Liskov
  behaviour: `scabha/validate.py:238`, `stimela/kitchen/cab.py:284,290`,
  `stimela/kitchen/step.py:626,663,709`. These are exactly the sites
  that receive validated parameter values; broader `type(x) is str`
  checks elsewhere see config strings, not validated params, and are
  left alone.
- Full test suite: **117/117 pass** on pydantic v2 (was 117/117 on v1 at
  Step 1 baseline). Ruff clean. Net diff across source: +34 lines, -11.
- **Next action:** Step 6 — add targeted v2-specific tests (File
  isinstance after List[File] roundtrip; v2 ValidationError loc format
  with list indices; serialization via TypeAdapter).

---

#### [unstarted] Plan authored

- Wrote `plan/migrate_pydantic.md` and this file.
- Verified current branch is `pydanticv2`.
- Verified `plan/` is empty on this branch.
- Confirmed investigation findings (single-file pydantic surface;
  CLI values already YAML-parsed at `stimela/commands/run.py:385`;
  `Parameter._dtype` populated by `eval()` in `cargo.py:255`; URI
  hierarchy pure `str` subclass chain; error flow catches
  `ScabhaBaseException` at `stimela/kitchen/step.py:542`).
- **Next action:** Step 0 — confirm master tests pass on this branch
  with current pydantic v1 pin, then start Step 1 (characterisation
  tests).

## Decisions / divergences log

_(Record any case where a planned v1 behaviour is consciously replaced
by v2 behaviour in the tests, or any targeted `BeforeValidator` added.)_

_(empty)_

## Open questions for the user

_(Escalate here rather than guessing. Clear each once answered.)_

_(empty)_
