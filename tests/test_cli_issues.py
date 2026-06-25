"""Tests for CLI/parameter issues #490 and #438."""

from .test_recipe import change_test_dir as change_test_dir
from .test_recipe import run, verify_output

# === Issue #490: assigning foo=bar from CLI only works if foo is a proper input ===


def test_cli_assign_valid_input():
    """Valid input assignment from CLI should succeed."""
    retcode, output = run("stimela -v -b native exec test_cli_assign.yml test-cli-assign x=hello")
    assert retcode == 0
    assert verify_output(output, "x=hello")


def test_cli_assign_valid_step_param():
    """Assignment to a step parameter (step.param=value) should succeed."""
    retcode, output = run("stimela -v -b native exec test_cli_assign.yml test-cli-assign x=hello echo.y=world")
    assert retcode == 0
    assert verify_output(output, "x=hello, y=world")


def test_cli_assign_unknown_variable():
    """Assignment to an unknown variable should produce a clear error."""
    retcode, output = run("stimela -v -b native exec test_cli_assign.yml test-cli-assign x=hello nonexistent=bar")
    assert retcode != 0
    assert verify_output(output, "not a known input")


def test_cli_assign_typo_in_input_name():
    """A typo in an input name should produce a clear error, not be silently ignored."""
    retcode, output = run("stimela -v -b native exec test_cli_assign.yml test-cli-assign xx=hello")
    assert retcode != 0
    assert verify_output(output, "not a known input")


# === Issue #438: more elegant handling of boolean flags ===


def test_bool_flags_true():
    """Boolean flags set to True should produce correct command-line arguments."""
    retcode, output = run("stimela -v -b native exec test_bool_flags.yml test-bool-flags-true")
    assert retcode == 0
    # All True: should have --default-flag, --is-flag-param, --dual-flag, --explicit-param yes
    assert verify_output(output, "--default-flag")
    assert verify_output(output, "--is-flag-param")
    assert verify_output(output, "--dual-flag")
    assert verify_output(output, "--explicit-param yes")


def test_bool_flags_false():
    """Boolean flags set to False should produce correct command-line arguments.
    In particular, explicit_flag policy should emit --no-<name> for False values.
    """
    retcode, output = run("stimela -v -b native exec test_bool_flags.yml test-bool-flags-false")
    assert retcode == 0
    # default-flag=False: should be omitted (no --default-flag in output)
    # is-flag-param=False: should be omitted (no --is-flag-param)
    # dual-flag=False with explicit_flag: should produce --no-dual-flag
    assert verify_output(output, "--no-dual-flag")
    # explicit-param=False with explicit_false="no": should produce --explicit-param no
    assert verify_output(output, "--explicit-param no")
    # Verify that default-flag and is-flag-param are NOT in the command line
    assert not verify_output(output, "--default-flag")
    assert not verify_output(output, "--is-flag-param")
