# Pydantic v2 Migration — Progress Log

Working branch: `pydanticv2`. Plan: `plan/migrate_pydantic.md`.

## Current state

**Step 1 — Characterisation tests:** In progress.

## Step status

| # | Step                                    | Status | Commit / Notes |
|---|-----------------------------------------|--------|----------------|
| 0 | Setup + baseline tests green            | DONE   | 76/76 pytest pass, ruff clean, pydantic 1.10.26 |
| 1 | Characterisation tests (v1 baseline)    | TODO   |  |
| 2 | Bump pydantic floor to `>=2.7,<3`       | TODO   |  |
| 3 | URI `__get_pydantic_core_schema__`      | TODO   |  |
| 4 | Migrate `scabha/validate.py`            | TODO   |  |
| 5 | Characterisation tests green on v2      | TODO   |  |
| 6 | Targeted pydantic v2 tests              | TODO   |  |
| 7 | Full suite + ruff clean                 | TODO   |  |
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
