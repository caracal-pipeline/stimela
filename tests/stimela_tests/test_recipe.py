import os
import re
import subprocess
import pytest


# Change into directory where test_recipy.py lives
# As suggested by https://stackoverflow.com/questions/62044541/change-pytest-working-directory-to-test-case-directory
@pytest.fixture(autouse=True)
def change_test_dir(request, monkeypatch):
    monkeypatch.chdir(request.fspath.dirname)


def callable_function(a: int, b: str):
    print(f"callable_function({a},'{b}')")


def run(command):
    """Runs command, returns tuple of exit code, output"""
    print(f"running: {command}")
    try:
        return 0, subprocess.check_output(command, shell=True).strip().decode()
    except subprocess.CalledProcessError as exc:
        return exc.returncode, exc.output.strip().decode()


def verify_output(output, *regexes):
    """Returns True if the regexes appear in sequence in the output.

    Given an output string, returns True if the regexes appear in order in
    the output string, with any number of characters between the regex strings.

    Args:
        output:
            An output string as returned by 'run'.
        regexes:
            One or more regex strings to match in the output.
    """
    # Replace all whitespace characters with a single space to avoid dependence
    # on terminal width when performing these tests.
    output = re.sub(r"\s+", " ", output)
    # Match the regex strings with any number of characters between them.
    regex = "(.*)".join(regexes)
    if not re.search(regex, output):
        print("Error, expected regex pattern did not appear in the output:")
        print(f"  {regex}")
        return False
    return True


def test_test_aliasing():
    print("===== expecting an error since required parameters are missing =====")
    retcode, _ = run("stimela -v -b native exec test_aliasing.yml")
    assert retcode != 0

    print("===== expecting no errors now =====")
    retcode, output = run("stimela -v doc test_aliasing.yml")

    print("===== expecting no errors now =====")
    retcode, output = run("stimela -v -b native exec test_aliasing.yml a=1 s3.a=1 s4.a=1 e=e f=f")
    assert retcode == 0
    print(output)
    assert verify_output(output, "DEBUG: ### validated outputs", "DEBUG: recipe 'alias test recipe'", "DEBUG: out: 1")


def test_test_nesting():
    print("===== expecting no errors =====")
    retcode, output = run("stimela -v -b native exec test_nesting.yml demo_recipe")
    assert retcode == 0
    print(output)


def test_test_recipe():
    print("===== expecting an error since 'msname' parameter is missing =====")
    retcode = os.system("stimela -v -b native exec test_recipe.yml selfcal.image_name=bar")
    assert retcode != 0

    print("===== expecting an error due to elem-xyz choices wrong =====")
    retcode = os.system(
        "stimela -v -b native exec test_recipe.yml selfcal.image_name=bar msname=foo elem-xyz=t elemlist-xyz=[x,y]"
    )
    assert retcode != 0

    print("===== expecting an error due to elem-xyz choices wrong =====")
    retcode = os.system(
        "stimela -v -b native exec test_recipe.yml selfcal.image_name=bar msname=foo elem-xyz=x elemlist-xyz=[x,t]"
    )
    assert retcode != 0

    print("===== expecting no errors now =====")
    retcode = os.system(
        "stimela -v -b native exec test_recipe.yml selfcal.image_name=bar msname=foo elem-xyz=x elemlist-xyz=[x,y]"
    )
    assert retcode == 0

    print("===== expecting no errors now =====")
    retcode = os.system(
        "stimela -v -b native exec test_recipe.yml selfcal.image_name=bar msname=foo elem-xyz=x elemlist-xyz=x"
    )
    assert retcode == 0


def test_test_loop_recipe():
    print("===== expecting an error since 'ms' parameter is missing =====")
    retcode = os.system("stimela -v -b native exec test_loop_recipe.yml cubical_image_loop")
    assert retcode != 0

    print("===== expecting no errors now =====")
    retcode = os.system("stimela -v -b native exec test_loop_recipe.yml cubical_image_loop ms=foo")
    assert retcode == 0

    print("===== expecting no errors now =====")
    retcode = os.system("stimela -v -b native exec test_loop_recipe.yml same_as_cubical_image_loop ms=foo")
    assert retcode == 0

    for name in "abc":
        msname = f"test-{name}.ms"
        if not os.path.exists(msname):
            os.mkdir(msname)
    print("===== expecting no errors now =====")
    retcode = os.system("stimela -v -b native exec test_loop_recipe.yml loop_recipe")
    assert retcode == 0


def test_scatter():
    print("===== expecting no errors now =====")
    retcode = os.system("stimela -v -b native exec test_scatter.yml basic_loop")
    assert retcode == 0

    print("===== expecting no errors now =====")
    retcode = os.system("stimela -v -b native exec test_scatter.yml nested_loop")
    assert retcode == 0
