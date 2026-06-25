import subprocess

from .test_recipe import change_test_dir as change_test_dir
from .test_recipe import run, verify_output


def run_with_stderr(command):
    """Runs command, returns tuple of exit code, combined stdout+stderr output"""
    print(f"running: {command}")
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    combined = result.stdout + result.stderr
    return result.returncode, combined.strip()


def test_param_file_input():
    print("===== expecting no errors =====")
    retcode, output = run("stimela -b native run test_param_file.yml test-param-file -pf param_file.yml")
    assert retcode == 0
    print(output)
    assert verify_output(output, "A string!", "['A', 'list', '!']")
    assert not verify_output(output, "gremlin")
    assert verify_output(output, "Found a unicorn!")


def test_param_file_none_section_error_message():
    """Test that a parameter file with an empty section (None value) followed by a
    CLI assignment to a nested key under that section produces a clear error message
    rather than a cryptic 'not a nested namespace' TypeError. (Issue #552)
    """
    print("===== expecting a clear error about None section =====")
    retcode, output = run_with_stderr(
        "stimela -b native run test_param_file.yml test-param-file "
        "-pf param_file_none_section.yml echo-inputs.extra=unicorn"
    )
    assert retcode != 0
    print(output)
    # The error message should mention the empty section / None value problem
    assert verify_output(output, "empty section") or verify_output(output, "set to None")
