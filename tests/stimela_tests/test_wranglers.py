import os, re, subprocess, pytest


# Change into directory where the test script lives
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
    """Returns true if the output contains lines matching the regexes in sequence (possibly with other lines in between)"""
    regexes = list(regexes[::-1])
    for line in output.split("\n"):
        if regexes and re.search(regexes[-1], line):
            regexes.pop()
    if regexes:
        print("Error, the following regexes did not match the output:")
        for regex in regexes:
            print(f"  {regex}")
        return False
    return True


def test_wrangler_replace_suppress():
    print("===== expecting no errors =====")
    retcode, output = run("stimela run test_wranglers.yml test_replace_suppress")
    assert retcode == 0
    print(output)
    assert verify_output(output, "Michael J. Fox", "don't need roads!")
    assert not verify_output(output, "cheetah")


def test_wrangler_force_success():
    print("===== expecting no errors =====")
    retcode, output = run("stimela run test_wranglers.yml test_force_success")
    assert retcode == 0
    print(output)
    assert verify_output(output, "deliberately declared")


def test_wrangler_force_failure():
    print("===== expecting an error =====")
    retcode, output = run("stimela run test_wranglers.yml test_force_failure")
    assert retcode != 0
    print(output)
    assert verify_output(output, "cab marked as failed")

    print("===== expecting an error =====")
    retcode, output = run("stimela run test_wranglers.yml test_force_failure2")
    assert retcode != 0
    print(output)
    assert verify_output(output, "Nobody expected the fox!")

def test_wrangler_parse():
    print("===== expecting no errors =====")
    retcode, output = run("stimela run test_wranglers.yml test_parse")
    assert retcode == 0
    print(output)
    assert verify_output(output, "The bloody cheetah ate 22 dogs!")

    print("===== expecting no errors =====")
    retcode, output = run("stimela run test_wranglers.yml test_parse2")
    assert retcode == 0
    print(output)
    assert verify_output(output, "The bloody cheetah ate 22 dogs!")

    print("===== expecting no errors =====")
    retcode, output = run("stimela run test_wranglers.yml test_parse3")
    assert retcode == 0
    print(output)
    assert verify_output(output, "The bloody cheetah ate 22 dogs!")

