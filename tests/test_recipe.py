import os
import re
import stat
import subprocess
import sys

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
    """Given an output string, returns the number of times the regexes appear in order in
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
    regex = "(.*?)".join(regexes)
    count = len(re.findall(regex, output))
    if not count:
        print("Error, expected regex pattern did not appear in the output:")
        print(f"  {regex}")
        return 0
    return count


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


def test_issue527_1():
    """Test that circular references are ignored in prevalidation"""
    print("===== expecting no errors =====")
    retcode, output = run("stimela -v -b native run test_issue527.yml outer")
    assert retcode == 0
    print(output)
    assert verify_output(output, "recipe 'outer' executed successfully")


def test_issue527_2():
    """Test that circular references are caught in validation"""
    print("===== expecting an error =====")
    retcode, output = run("stimela -v -b native run test_issue527.yml circular")
    assert retcode == 1
    print(output)
    assert verify_output(output, "self-referencing formula or substitution")


def test_issue527_3():
    """Test that circular references are caught in validation"""
    print("===== expecting an error =====")
    retcode, output = run("stimela -v -b native run test_issue527.yml circular circular==recipe.circular")
    assert retcode == 1
    print(output)
    assert verify_output(output, "invalid inputs")


def test_scatter():
    print("===== expecting no errors now =====")
    retcode = os.system("stimela -v -b native exec test_scatter.yml basic_loop")
    assert retcode == 0

    print("===== expecting no errors now =====")
    retcode = os.system("stimela -v -b native exec test_scatter.yml nested_loop")
    assert retcode == 0


def _run_stderr(command):
    """Runs command, captures stderr+stdout, returns tuple of exit code, output"""
    import shlex

    print(f"running: {command}")
    result = subprocess.run(shlex.split(command), capture_output=True, text=True)
    return result.returncode, (result.stderr or "") + (result.stdout or "")


@pytest.mark.skipif(
    sys.platform != "linux" or os.geteuid() == 0,
    reason="POSIX permission bits do not restrict root; chmod semantics differ on non-Linux",
)
def test_permission_check(tmp_path):
    """Test that writable inputs with insufficient file permissions are caught before execution."""
    # create a read-only directory (simulating a read-only MS)
    ro_dir = tmp_path / "readonly.ms"
    ro_dir.mkdir()
    os.chmod(ro_dir, stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
    assert not os.access(ro_dir, os.W_OK), f"chmod did not make {ro_dir} read-only"

    recipe_path = os.path.join(os.path.dirname(__file__), "test_permissions.yml")

    print("===== expecting error for read-only writable input =====")
    retcode, output = _run_stderr(f"stimela -b native exec {recipe_path} ms-in={ro_dir}")
    assert retcode != 0
    assert "not writable" in output

    # make it writable, should succeed
    os.chmod(ro_dir, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
    print("===== expecting success with writable directory =====")
    retcode, output = _run_stderr(f"stimela -b native exec {recipe_path} ms-in={ro_dir}")
    assert retcode == 0

    # test check_permissions: false opt-out using nocheck recipe
    recipe_nocheck_path = os.path.join(os.path.dirname(__file__), "test_permissions_nocheck.yml")
    os.chmod(ro_dir, stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
    assert not os.access(ro_dir, os.W_OK), f"chmod did not make {ro_dir} read-only"
    print("===== expecting success with check_permissions opt-out =====")
    retcode, output = _run_stderr(f"stimela -b native exec {recipe_nocheck_path} ms-in={ro_dir}")
    assert retcode == 0


@pytest.mark.skipif(
    sys.platform != "linux" or os.geteuid() == 0,
    reason="POSIX permission bits do not restrict root; chmod semantics differ on non-Linux",
)
def test_permission_check_outputs(tmp_path):
    """Test that non-writable output paths are caught before execution."""
    # create a read-only file as an existing output
    ro_file = tmp_path / "existing_output.txt"
    ro_file.touch()
    os.chmod(ro_file, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
    assert not os.access(ro_file, os.W_OK), f"chmod did not make {ro_file} read-only"

    recipe_path = os.path.join(os.path.dirname(__file__), "test_permissions_output.yml")

    print("===== expecting error for read-only existing output =====")
    retcode, output = _run_stderr(f"stimela -b native exec {recipe_path} out-file={ro_file}")
    assert retcode != 0
    assert "not writable" in output

    # make it writable, should succeed
    os.chmod(ro_file, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)
    print("===== expecting success with writable output =====")
    retcode, output = _run_stderr(f"stimela -b native exec {recipe_path} out-file={ro_file}")
    assert retcode == 0

    # test non-writable parent directory for a new output
    ro_parent = tmp_path / "ro_parent"
    ro_parent.mkdir()
    os.chmod(ro_parent, stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
    assert not os.access(ro_parent, os.W_OK), f"chmod did not make {ro_parent} read-only"

    new_file = ro_parent / "new_output.txt"
    print("===== expecting error for non-writable output parent =====")
    retcode, output = _run_stderr(f"stimela -b native exec {recipe_path} out-file={new_file}")
    assert retcode != 0
    assert "not writable" in output
