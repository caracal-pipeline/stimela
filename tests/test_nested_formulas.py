"""Tests for deeply nested formula evaluation (GitHub issue #462).

Users hit 'maximum recursion depth exceeded' when using complex skip conditions
with nested IF() calls. This test verifies that deeply nested formulas parse
and evaluate correctly after the recursion limit fix in stimela/__init__.py.
"""

# Importing stimela triggers the recursion limit increase
from scabha.evaluator import Evaluator, parse_string
from scabha.substitutions import SubstitutionNS, substitutions_from

import stimela  # noqa: F401


def test_parse_nested_if_four_levels():
    """Test parsing a 4-level nested IF formula (the exact case from issue #462)."""
    formula = (
        "IF(recipe.selfcal.enable==false, true, "
        "IF(recipe.iter==steps.max-iter.max-iter, true, "
        "IF(steps.time_freq_intervals.jones_type=='k', "
        "IF(recipe.iter==0, true, false), true)))"
    )
    result = parse_string(formula, location=["test"])
    assert result is not None


def test_parse_nested_if_six_levels():
    """Test parsing a 6-level nested IF formula."""
    formula = (
        "IF(1==1, true, IF(2==2, true, IF(3==3, IF(4==4, IF(5==5, IF(6==6, true, false), false), false), true), false))"
    )
    result = parse_string(formula, location=["test"])
    assert result is not None


def test_evaluate_nested_if_from_issue_462():
    """Test full evaluation of the formula from issue #462 with a mock namespace."""
    ns = SubstitutionNS(
        recipe=SubstitutionNS(
            selfcal=SubstitutionNS(enable=True),
            iter=1,
        ),
        steps=SubstitutionNS(
            **{
                "max-iter": SubstitutionNS(**{"max-iter": 5}),
                "time_freq_intervals": SubstitutionNS(
                    jones_type="k",
                    time_int=[10],
                    freq_int=[0],
                ),
            }
        ),
    )

    formula = (
        "=IF(recipe.selfcal.enable==false, true, "
        "IF(recipe.iter==steps.max-iter.max-iter, true, "
        "IF(steps.time_freq_intervals.jones_type=='k', "
        "IF(recipe.iter==0, true, false), true)))"
    )

    with substitutions_from(ns, raise_errors=True) as context:
        evaluator = Evaluator(ns, context, allow_unresolved=False, location=["test"])
        result = evaluator.evaluate(formula)

    # enable=True, iter=1 != max-iter=5, jones_type=='k', iter=1 != 0 => false
    assert result is False


def test_evaluate_nested_if_skip_true():
    """Test that nested IF returns true (skip) when enable is false."""
    ns = SubstitutionNS(
        recipe=SubstitutionNS(
            selfcal=SubstitutionNS(enable=False),
            iter=0,
        ),
        steps=SubstitutionNS(
            **{
                "max-iter": SubstitutionNS(**{"max-iter": 5}),
                "time_freq_intervals": SubstitutionNS(
                    jones_type="k",
                    time_int=[10],
                    freq_int=[0],
                ),
            }
        ),
    )

    formula = (
        "=IF(recipe.selfcal.enable==false, true, "
        "IF(recipe.iter==steps.max-iter.max-iter, true, "
        "IF(steps.time_freq_intervals.jones_type=='k', "
        "IF(recipe.iter==0, true, false), true)))"
    )

    with substitutions_from(ns, raise_errors=True) as context:
        evaluator = Evaluator(ns, context, allow_unresolved=False, location=["test"])
        result = evaluator.evaluate(formula)

    # enable=False => first IF condition is true => return true
    assert result is True


def test_recursion_limit_is_raised():
    """Verify that importing stimela raises the recursion limit."""
    import sys

    assert sys.getrecursionlimit() >= stimela._MINIMUM_RECURSION_LIMIT
