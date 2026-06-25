from .test_recipe import change_test_dir as change_test_dir
from .test_recipe import run, verify_output


def test_slice_function():
    """Test that SLICE() function works for list slicing (issue #449)."""
    print("===== testing SLICE function =====")
    retcode, output = run("stimela -v -b native run test_issue449_slicing.yml")
    assert retcode == 0, f"stimela run failed:\n{output}"
    print(output)
    # Check first-n step produces first 3 elements
    assert verify_output(output, "echo-first-n", "a", "b", "c")
    # Check mid step produces elements 1-3
    assert verify_output(output, "echo-mid", "b", "c", "d")
    # Check to-end step produces elements from index 2 onward
    assert verify_output(output, "echo-to-end", "c", "d", "e")


def test_slice_direct():
    """Test SLICE function directly via the evaluator."""
    from scabha.evaluator import Evaluator
    from scabha.substitutions import SubstitutionNS

    import stimela  # noqa: F401 - ensure patches are applied

    ns = SubstitutionNS(recipe=dict(mylist=["a", "b", "c", "d", "e"]))
    evaluator = Evaluator(ns, location=["test"])

    # SLICE(list, stop)
    result = evaluator.evaluate("=SLICE(recipe.mylist, 3)")
    assert result == ["a", "b", "c"], f"Expected ['a', 'b', 'c'], got {result}"

    # SLICE(list, start, stop)
    result = evaluator.evaluate("=SLICE(recipe.mylist, 1, 4)")
    assert result == ["b", "c", "d"], f"Expected ['b', 'c', 'd'], got {result}"

    # SLICE(list, start, UNSET) => list[start:]
    result = evaluator.evaluate("=SLICE(recipe.mylist, 2, UNSET)")
    assert result == ["c", "d", "e"], f"Expected ['c', 'd', 'e'], got {result}"

    # SLICE(list, UNSET, stop) => list[:stop]
    result = evaluator.evaluate("=SLICE(recipe.mylist, UNSET, 2)")
    assert result == ["a", "b"], f"Expected ['a', 'b'], got {result}"

    # SLICE(list, start, stop, step)
    result = evaluator.evaluate("=SLICE(recipe.mylist, 0, 5, 2)")
    assert result == ["a", "c", "e"], f"Expected ['a', 'c', 'e'], got {result}"

    print("All direct SLICE tests passed")
