import sys

from .test_recipe import change_test_dir as change_test_dir
from .test_recipe import run, verify_output


def test_recursion_limit_is_raised():
    """Verify stimela raises the recursion limit above the default (issue #462)."""
    import stimela  # noqa: F401 — import for side-effect

    assert sys.getrecursionlimit() >= 10000


def test_deeply_nested_formula():
    """A recipe with 4-level nested IFs in a skip condition must not crash (issue #462)."""
    retcode, output = run("stimela -v -b native run test_deeply_nested_formula.yml")
    assert retcode == 0
    assert verify_output(output, "deeply nested skip evaluated successfully")
