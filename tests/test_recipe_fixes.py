"""Tests for recipe.py fixes (issues #349, #317, #362, #460, #433, #307)."""

import re
import subprocess

import pytest


@pytest.fixture(autouse=True)
def change_test_dir(request, monkeypatch):
    monkeypatch.chdir(request.fspath.dirname)


def run(command):
    """Runs command, returns tuple of exit code, output (combined stdout+stderr)."""
    print(f"running: {command}")
    try:
        output = subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT).strip().decode()
        return 0, output
    except subprocess.CalledProcessError as exc:
        return exc.returncode, exc.output.strip().decode()


def verify_output(output, *regexes):
    """Check that regexes appear in order in the output string."""
    output = re.sub(r"\s+", " ", output)
    regex = "(.*?)".join(regexes)
    count = len(re.findall(regex, output))
    if not count:
        print("Error, expected regex pattern did not appear in the output:")
        print(f"  {regex}")
        return 0
    return count


# --- Issue #349: check input/output direction for aliases ---


def test_349_input_aliased_to_step_output():
    """A recipe input aliased to a step output should raise an error."""
    retcode, output = run("stimela -v -b native exec test_recipe_fixes.yml test-349-input-to-output")
    print(output)
    assert retcode != 0
    assert verify_output(output, "input.*aliased to step output")


def test_349_output_aliased_to_step_input():
    """A recipe output aliased to a step input should raise an error."""
    retcode, output = run("stimela -v -b native exec test_recipe_fixes.yml test-349-output-to-input")
    print(output)
    assert retcode != 0
    assert verify_output(output, "output.*aliased to step input")


# --- Issue #362: assign to an input should be prohibited ---


def test_362_assign_to_input_deprecated():
    """Having an input in the assign section should produce a deprecation warning."""
    retcode, output = run("stimela -v -b native exec test_recipe_fixes.yml test-362-assign-to-input")
    print(output)
    # Should succeed but produce a deprecation warning
    assert retcode == 0
    assert verify_output(output, "assign.*input.*deprecated")


# --- Issue #460: improved error for alias/step name collision ---


def test_460_alias_step_collision_message():
    """When an alias shares a name with a step input, a warning should explain the collision."""
    retcode, output = run("stimela -v -b native exec test_recipe_fixes.yml test-460-alias-step-collision")
    print(output)
    # Should produce a warning about the alias name colliding with a step parameter
    assert verify_output(output, "alias.*bar.*same name.*step.*input.*sleep-1.bar")


# --- Issue #307: recipe summary_message ---


def test_317_assign_propagates_to_alias():
    """A recipe input set via assign should propagate its value to step aliases."""
    retcode, output = run("stimela -v -b native exec test_recipe_fixes.yml test-317-assign-alias-propagation")
    print(output)
    assert retcode == 0
    # The assigned value should appear in the step's command line
    assert verify_output(output, "b=assigned_value")


def test_433_subrecipe_namespace_scrubbed():
    """Sub-recipe defaults should not resolve parent recipe namespace during prevalidation."""
    retcode, output = run("stimela -v -b native exec test_recipe_fixes.yml test-433-subrecipe-namespace")
    print(output)
    # The sub-recipe input x defaults to {recipe.parent_var}. During prevalidation,
    # the parent recipe's namespace should be scrubbed, so the default should be
    # Unresolved (not resolved to the parent's value).
    assert verify_output(output, "Unresolved.*recipe.parent_var.*invalid key")


def test_307_summary_message():
    """A recipe with summary_message should print it after successful execution."""
    retcode, output = run("stimela -v -b native run test_recipe_fixes_307.yml test-307-summary-message")
    print(output)
    assert retcode == 0
    assert verify_output(output, "Recipe completed with a=success")


def test_307_no_summary_message():
    """A recipe without summary_message should run without issues."""
    retcode, output = run("stimela -v -b native run test_recipe_fixes_307.yml test-307-no-summary")
    print(output)
    assert retcode == 0
