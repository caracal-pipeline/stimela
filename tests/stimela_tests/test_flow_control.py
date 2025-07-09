import pytest
from .test_recipe import run, verify_output

# TODO(JSKenyon): This should be made programmatic at some point.
ALL_STEPS = {
    "s1",
    "s2",
    "s2.t1-always",
    "s2.t2-never-foo",
    "s2.t3",
    "s2.t4-foo-bar",
    "s2.t5-bar",
    "s2.t6-skip",
    "s2.t7",
    "s2.t7.ssr-1-always",
    "s2.t7.ssr-2-never",
    "s2.t7.ssr-3-foo",
    "s2.t7.ssr-4-skip",
    "s3",
    "s3.t1-always",
    "s3.t2-never-foo",
    "s3.t3",
    "s3.t4-foo-bar",
    "s3.t5-bar",
    "s3.t6-skip",
    "s3.t7",
    "s3.t7.ssr-1-always",
    "s3.t7.ssr-2-never",
    "s3.t7.ssr-3-foo",
    "s3.t7.ssr-4-skip",
    "s4",
    "s4.t1-always",
    "s4.t2-never-foo",
    "s4.t3",
    "s4.t4-foo-bar",
    "s4.t5-bar",
    "s4.t6-skip",
    "s4.t7",
    "s4.t7.ssr-1-always",
    "s4.t7.ssr-2-never",
    "s4.t7.ssr-3-foo",
    "s4.t7.ssr-4-skip",
    "s5"
}

@pytest.fixture
def base_command():
    return "stimela -b native run test_subrecipes.yml recipe"


def validate_run(test_output, run_steps):

    skip_steps = ALL_STEPS - run_steps

    for s in run_steps:
        try:
            assert verify_output(test_output, f"running step recipe.{s}")
        except AssertionError:
            raise AssertionError(f"Step {s} failed to run.")

    for s in skip_steps:
        try:
            assert not verify_output(test_output, f"running step recipe.{s}")
        except AssertionError:
            raise AssertionError(f"Step {s} was run unexpectedly.")

def test_no_cli_options(base_command):
    """Select tag on step/s of root recipe"""
    retcode, output = run(f"{base_command}")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t3",
        "s2.t4-foo-bar",
        "s2.t5-bar",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s2.t7.ssr-3-foo",
        "s4",
        "s4.t1-always",
        "s4.t3",
        "s4.t4-foo-bar",
        "s4.t5-bar",
        "s4.t7",
        "s4.t7.ssr-1-always",
        "s4.t7.ssr-3-foo"
    }

    validate_run(output, run_steps)


def test_select_by_tag_case_a(base_command):
    """Select tag on step/s of root recipe."""
    retcode, output = run(f"{base_command} -t foo")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s3",
        "s3.t1-always",
        "s3.t3",
        "s3.t4-foo-bar",
        "s3.t5-bar",
        "s3.t7",
        "s3.t7.ssr-1-always",
        "s3.t7.ssr-3-foo",
        "s4",
        "s4.t1-always",
        "s4.t3",
        "s4.t4-foo-bar",
        "s4.t5-bar",
        "s4.t7",
        "s4.t7.ssr-1-always",
        "s4.t7.ssr-3-foo",
    }

    validate_run(output, run_steps)

def test_select_by_tag_case_b(base_command):
    """Select tag on step/s of subrecipe."""
    retcode, output = run(f"{base_command} -t s2.bar")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t4-foo-bar",
        "s2.t5-bar",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s4",
        "s4.t1-always",
        "s4.t7",
        "s4.t7.ssr-1-always"
    }

    validate_run(output, run_steps)

def test_select_by_tag_case_c(base_command):
    """Select tag on step/s of subsubrecipe."""
    retcode, output = run(f"{base_command} -t s2.t7.foo")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s2.t7.ssr-3-foo",
        "s4",
        "s4.t1-always",
        "s4.t7",
        "s4.t7.ssr-1-always"
    }

    validate_run(output, run_steps)

def test_select_by_tag_case_d(base_command):
    """Select tag on step/s of subrecipe which are also never tagged."""
    retcode, output = run(f"{base_command} -t s2.foo")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t2-never-foo",
        "s2.t4-foo-bar",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s4",
        "s4.t1-always",
        "s4.t7",
        "s4.t7.ssr-1-always"
    }

    validate_run(output, run_steps)

def test_skip_by_tag_case_a(base_command):
    """Skip tag on step/s of root recipe."""
    retcode, output = run(f"{base_command} --skip-tags foo")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t3",
        "s2.t4-foo-bar",
        "s2.t5-bar",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s2.t7.ssr-3-foo"
    }

    validate_run(output, run_steps)

def test_skip_by_tag_case_b(base_command):
    """Skip tag on step/s of subrecipe."""
    retcode, output = run(f"{base_command} --skip-tags s2.bar")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t3",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s2.t7.ssr-3-foo",
        "s4",
        "s4.t1-always",
        "s4.t3",
        "s4.t4-foo-bar",
        "s4.t5-bar",
        "s4.t7",
        "s4.t7.ssr-1-always",
        "s4.t7.ssr-3-foo"
    }

    validate_run(output, run_steps)

def test_skip_by_tag_case_c(base_command):
    """Skip tag on step/s of subsubrecipe."""
    retcode, output = run(f"{base_command} --skip-tags s2.t7.foo")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t3",
        "s2.t4-foo-bar",
        "s2.t5-bar",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s4",
        "s4.t1-always",
        "s4.t3",
        "s4.t4-foo-bar",
        "s4.t5-bar",
        "s4.t7",
        "s4.t7.ssr-1-always",
        "s4.t7.ssr-3-foo"
    }

    validate_run(output, run_steps)

def test_select_by_step_case_a(base_command):
    """Select step of root recipe."""
    retcode, output = run(f"{base_command} -s s3")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s3",
        "s3.t1-always",
        "s3.t3",
        "s3.t4-foo-bar",
        "s3.t5-bar",
        "s3.t7",
        "s3.t7.ssr-1-always",
        "s3.t7.ssr-3-foo",
        "s4",
        "s4.t1-always",
        "s4.t7",
        "s4.t7.ssr-1-always"
    }

    validate_run(output, run_steps)

def test_select_by_step_case_b(base_command):
    """Select step of subrecipe."""
    retcode, output = run(f"{base_command} -s s3.t3")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s3",
        "s3.t1-always",
        "s3.t3",
        "s3.t7",
        "s3.t7.ssr-1-always",
        "s4",
        "s4.t1-always",
        "s4.t7",
        "s4.t7.ssr-1-always"
    }

    validate_run(output, run_steps)

def test_select_by_step_case_c(base_command):
    """Select step of subsubrecipe."""
    retcode, output = run(f"{base_command} -s s2.t7.ssr-2-never")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s2.t7.ssr-2-never",
        "s4",
        "s4.t1-always",
        "s4.t7",
        "s4.t7.ssr-1-always"
    }

    validate_run(output, run_steps)

def test_select_by_step_case_d(base_command):
    """Select step of subrecipe which has been marked as skip."""
    retcode, output = run(f"{base_command} -s s3.t6-skip")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s3",
        "s3.t1-always",
        "s3.t6-skip",
        "s3.t7",
        "s3.t7.ssr-1-always",
        "s4",
        "s4.t1-always",
        "s4.t7",
        "s4.t7.ssr-1-always",
    }

    validate_run(output, run_steps)

def test_select_by_step_range_case_a(base_command):
    """Select steps of root recipe."""
    retcode, output = run(f"{base_command} -s s2:s3")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t3",
        "s2.t4-foo-bar",
        "s2.t5-bar",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s2.t7.ssr-3-foo",
        "s4",
        "s4.t1-always",
        "s4.t7",
        "s4.t7.ssr-1-always"
    }

    validate_run(output, run_steps)

def test_select_by_step_range_case_b(base_command):
    """Select steps of subrecipe."""
    retcode, output = run(f"{base_command} -s s2.t2-never-foo:t6-skip")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t3",
        "s2.t4-foo-bar",
        "s2.t5-bar",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s4",
        "s4.t1-always",
        "s4.t7",
        "s4.t7.ssr-1-always"
    }

    validate_run(output, run_steps)

def test_select_by_step_range_case_c(base_command):
    """Select steps of subsubrecipe."""
    retcode, output = run(f"{base_command} -s s2.t7.ssr-2-never:ssr-4-skip")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s2.t7.ssr-3-foo",
        "s4",
        "s4.t1-always",
        "s4.t7",
        "s4.t7.ssr-1-always"
    }

    validate_run(output, run_steps)

def test_select_by_unbounded_step_range_case_a(base_command):
    """Select steps of recipe with no upper bound."""
    retcode, output = run(f"{base_command} -s s3:")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s4",
        "s4.t1-always",
        "s4.t3",
        "s4.t4-foo-bar",
        "s4.t5-bar",
        "s4.t7",
        "s4.t7.ssr-1-always",
        "s4.t7.ssr-3-foo"
    }

    validate_run(output, run_steps)

def test_select_by_unbounded_step_range_case_b(base_command):
    """Select steps of recipe with no lower bound."""
    retcode, output = run(f"{base_command} -s :s2")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t3",
        "s2.t4-foo-bar",
        "s2.t5-bar",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s2.t7.ssr-3-foo",
        "s4",
        "s4.t1-always",
        "s4.t7",
        "s4.t7.ssr-1-always",
    }

    validate_run(output, run_steps)

def test_select_by_unbounded_step_range_case_c(base_command):
    """Select steps of subrecipe with no upper bound."""
    retcode, output = run(f"{base_command} -s s3.t5-bar:")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s4",
        "s4.t1-always",
        "s4.t3",
        "s4.t4-foo-bar",
        "s4.t5-bar",
        "s4.t7",
        "s4.t7.ssr-1-always",
        "s4.t7.ssr-3-foo"
    }

    validate_run(output, run_steps)

def test_select_by_unbounded_step_range_case_d(base_command):
    """Select steps of subrecipe with no lower bound."""
    retcode, output = run(f"{base_command} -s s2.:t4-foo-bar")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t3",
        "s2.t4-foo-bar",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s4",
        "s4.t1-always",
        "s4.t7",
        "s4.t7.ssr-1-always",
    }

    validate_run(output, run_steps)

def test_select_by_unbounded_step_range_case_e(base_command):
    """Select steps of sebsubrecipe with no upper bound."""
    retcode, output = run(f"{base_command} -s s2.t7.ssr-3-foo:")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s2.t7.ssr-3-foo",
        "s4",
        "s4.t1-always",
        "s4.t3",
        "s4.t4-foo-bar",
        "s4.t5-bar",
        "s4.t7",
        "s4.t7.ssr-1-always",
        "s4.t7.ssr-3-foo"
    }

    validate_run(output, run_steps)

def test_select_by_unbounded_step_range_case_f(base_command):
    """Select steps of subsubrecipe with no lower bound."""
    retcode, output = run(f"{base_command} -s s2.t7.:ssr-1-always")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t3",
        "s2.t4-foo-bar",
        "s2.t5-bar",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s4",
        "s4.t1-always",
        "s4.t7",
        "s4.t7.ssr-1-always",
    }

    validate_run(output, run_steps)


def test_skip_by_step_case_a(base_command):
    """Skip step of root recipe."""
    retcode, output = run(f"{base_command} -k s2")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s4",
        "s4.t1-always",
        "s4.t3",
        "s4.t4-foo-bar",
        "s4.t5-bar",
        "s4.t7",
        "s4.t7.ssr-1-always",
        "s4.t7.ssr-3-foo"
    }

    validate_run(output, run_steps)

def test_skip_by_step_case_b(base_command):
    """Skip step of subrecipe."""
    retcode, output = run(f"{base_command} -k s2.t3")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t4-foo-bar",
        "s2.t5-bar",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s2.t7.ssr-3-foo",
        "s4",
        "s4.t1-always",
        "s4.t3",
        "s4.t4-foo-bar",
        "s4.t5-bar",
        "s4.t7",
        "s4.t7.ssr-1-always",
        "s4.t7.ssr-3-foo"
    }

    validate_run(output, run_steps)

def test_skip_by_step_case_c(base_command):
    """Skip step of subsubrecipe."""
    retcode, output = run(f"{base_command} -k s2.t7.ssr-1-always")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t4-foo-bar",
        "s2.t3",
        "s2.t5-bar",
        "s2.t7",
        "s2.t7.ssr-3-foo",
        "s4",
        "s4.t1-always",
        "s4.t3",
        "s4.t4-foo-bar",
        "s4.t5-bar",
        "s4.t7",
        "s4.t7.ssr-1-always",
        "s4.t7.ssr-3-foo"
    }

    validate_run(output, run_steps)

def test_skip_by_step_range_case_a(base_command):
    """Skip steps of root recipe."""
    retcode, output = run(f"{base_command} -k s1:s2")
    assert retcode == 0
    print(output)
    run_steps = {
        "s4",
        "s4.t1-always",
        "s4.t3",
        "s4.t4-foo-bar",
        "s4.t5-bar",
        "s4.t7",
        "s4.t7.ssr-1-always",
        "s4.t7.ssr-3-foo"
    }

    validate_run(output, run_steps)

def test_skip_by_step_range_case_b(base_command):
    """Skip steps of subrecipe."""
    retcode, output = run(f"{base_command} -k s2.t3:t4-foo-bar")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t5-bar",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s2.t7.ssr-3-foo",
        "s4.t1-always",
        "s4",
        "s4.t3",
        "s4.t4-foo-bar",
        "s4.t5-bar",
        "s4.t7",
        "s4.t7.ssr-1-always",
        "s4.t7.ssr-3-foo"
    }

    validate_run(output, run_steps)

def test_skip_by_step_range_case_c(base_command):
    """Skip steps of subsubrecipe."""
    retcode, output = run(f"{base_command} -k s4.t7.ssr-1-always:ssr-4-skip")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t5-bar",
        "s2.t3",
        "s2.t4-foo-bar",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s2.t7.ssr-3-foo",
        "s4.t1-always",
        "s4",
        "s4.t3",
        "s4.t4-foo-bar",
        "s4.t5-bar"
    }

    validate_run(output, run_steps)

def test_skip_by_unbounded_step_range_case_a(base_command):
    """Skip steps of recipe with no upper bound."""
    retcode, output = run(f"{base_command} -k s2:")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1"
    }

    validate_run(output, run_steps)

def test_skip_by_unbounded_step_range_case_b(base_command):
    """Skip steps of recipe with no lower bound."""
    retcode, output = run(f"{base_command} -k :s2")
    assert retcode == 0
    print(output)
    run_steps = {
        "s4",
        "s4.t1-always",
        "s4.t3",
        "s4.t4-foo-bar",
        "s4.t5-bar",
        "s4.t7",
        "s4.t7.ssr-1-always",
        "s4.t7.ssr-3-foo"
    }

    validate_run(output, run_steps)

def test_skip_by_unbounded_step_range_case_c(base_command):
    """Skip steps of subrecipe with no upper bound."""
    retcode, output = run(f"{base_command} -k s2.t4-foo-bar:")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t3"
    }

    validate_run(output, run_steps)

def test_skip_by_unbounded_step_range_case_d(base_command):
    """Skip steps of subrecipe with no lower bound."""
    retcode, output = run(f"{base_command} -k s2.:t3")
    assert retcode == 0
    print(output)
    run_steps = {
        "s2",
        "s2.t4-foo-bar",
        "s2.t5-bar",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s2.t7.ssr-3-foo",
        "s4",
        "s4.t1-always",
        "s4.t3",
        "s4.t4-foo-bar",
        "s4.t5-bar",
        "s4.t7",
        "s4.t7.ssr-1-always",
        "s4.t7.ssr-3-foo"
    }

    validate_run(output, run_steps)

def test_skip_by_unbounded_step_range_case_e(base_command):
    """Skip steps of subsubrecipe with no upper bound."""
    retcode, output = run(f"{base_command} -k s2.t7.ssr-3-foo:")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t3",
        "s2.t4-foo-bar",
        "s2.t5-bar",
        "s2.t7",
        "s2.t7.ssr-1-always"
    }

    validate_run(output, run_steps)

def test_skip_by_unbounded_step_range_case_f(base_command):
    """Skip steps of subsubrecipe with no lower bound."""
    retcode, output = run(f"{base_command} -k s2.t7.:ssr-2-never")
    assert retcode == 0
    print(output)
    run_steps = {
        "s2",
        "s2.t7",
        "s2.t7.ssr-3-foo",
        "s4",
        "s4.t1-always",
        "s4.t3",
        "s4.t4-foo-bar",
        "s4.t5-bar",
        "s4.t7",
        "s4.t7.ssr-1-always",
        "s4.t7.ssr-3-foo"
    }

    validate_run(output, run_steps)

def test_unskip_steps_case_a(base_command):
    """Unskip step of recipe."""
    retcode, output = run(f"{base_command} -e s5")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t3",
        "s2.t4-foo-bar",
        "s2.t5-bar",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s2.t7.ssr-3-foo",
        "s4",
        "s4.t1-always",
        "s4.t3",
        "s4.t4-foo-bar",
        "s4.t5-bar",
        "s4.t7",
        "s4.t7.ssr-1-always",
        "s4.t7.ssr-3-foo",
        "s5"
    }

    validate_run(output, run_steps)

def test_unskip_steps_case_b(base_command):
    """Unskip step of subrecipe."""
    retcode, output = run(f"{base_command} -e s4.t6-skip")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t3",
        "s2.t4-foo-bar",
        "s2.t5-bar",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s2.t7.ssr-3-foo",
        "s4",
        "s4.t1-always",
        "s4.t3",
        "s4.t4-foo-bar",
        "s4.t5-bar",
        "s4.t6-skip",
        "s4.t7",
        "s4.t7.ssr-1-always",
        "s4.t7.ssr-3-foo"
    }

    validate_run(output, run_steps)

def test_unskip_steps_case_c(base_command):
    """Unskip step of subsubrecipe."""
    retcode, output = run(f"{base_command} -e s4.t7.ssr-4-skip")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t3",
        "s2.t4-foo-bar",
        "s2.t5-bar",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s2.t7.ssr-3-foo",
        "s4",
        "s4.t1-always",
        "s4.t3",
        "s4.t4-foo-bar",
        "s4.t5-bar",
        "s4.t7",
        "s4.t7.ssr-1-always",
        "s4.t7.ssr-3-foo",
        "s4.t7.ssr-4-skip"
    }

    validate_run(output, run_steps)

def test_unskip_steps_case_d(base_command):
    """Unskip steps of subrecipes with no upper bound."""
    retcode, output = run(f"{base_command} -e s2.t6-skip:")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t3",
        "s2.t4-foo-bar",
        "s2.t5-bar",
        "s2.t6-skip",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s2.t7.ssr-3-foo",
        "s2.t7.ssr-4-skip",
        "s4",
        "s4.t1-always",
        "s4.t3",
        "s4.t4-foo-bar",
        "s4.t5-bar",
        "s4.t6-skip",
        "s4.t7",
        "s4.t7.ssr-1-always",
        "s4.t7.ssr-3-foo",
        "s4.t7.ssr-4-skip",
        "s5"
    }

    validate_run(output, run_steps)

def test_unskip_steps_case_e(base_command):
    """Unskip steps of subrecipes with no lower bound."""
    retcode, output = run(f"{base_command} -e s3.:t3")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t3",
        "s2.t4-foo-bar",
        "s2.t5-bar",
        "s2.t6-skip",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s2.t7.ssr-3-foo",
        "s2.t7.ssr-4-skip",
        "s4",
        "s4.t1-always",
        "s4.t3",
        "s4.t4-foo-bar",
        "s4.t5-bar",
        "s4.t7",
        "s4.t7.ssr-1-always",
        "s4.t7.ssr-3-foo",
    }

    validate_run(output, run_steps)

def test_unskip_steps_case_f(base_command):
    """Unskip steps of subsubrecipes with no upper bound."""
    retcode, output = run(f"{base_command} -e s2.t7.ssr-4-skip:")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t3",
        "s2.t4-foo-bar",
        "s2.t5-bar",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s2.t7.ssr-3-foo",
        "s2.t7.ssr-4-skip",
        "s4",
        "s4.t1-always",
        "s4.t3",
        "s4.t4-foo-bar",
        "s4.t5-bar",
        "s4.t6-skip",
        "s4.t7",
        "s4.t7.ssr-1-always",
        "s4.t7.ssr-3-foo",
        "s4.t7.ssr-4-skip",
        "s5"
    }

    validate_run(output, run_steps)

def test_unskip_steps_case_g(base_command):
    """Unskip steps of subsubrecipes with no lower bound."""
    retcode, output = run(f"{base_command} -e s4.t7.:ssr-1-always")
    assert retcode == 0
    print(output)
    run_steps = {
        "s1",
        "s2",
        "s2.t1-always",
        "s2.t3",
        "s2.t4-foo-bar",
        "s2.t5-bar",
        "s2.t6-skip",
        "s2.t7",
        "s2.t7.ssr-1-always",
        "s2.t7.ssr-3-foo",
        "s2.t7.ssr-4-skip",
        "s4",
        "s4.t1-always",
        "s4.t3",
        "s4.t4-foo-bar",
        "s4.t5-bar",
        "s4.t6-skip",
        "s4.t7",
        "s4.t7.ssr-1-always",
        "s4.t7.ssr-3-foo",
    }

    validate_run(output, run_steps)