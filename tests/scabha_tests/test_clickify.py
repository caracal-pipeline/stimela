#!/usr/bin/env python
import os.path
import re
import subprocess
import sys
from typing import List, Optional, Tuple

import click
import pytest

from scabha.schema_utils import clickify_parameters


# Change into directory where the test lives
# As suggested by https://stackoverflow.com/questions/62044541/change-pytest-working-directory-to-test-case-directory
@pytest.fixture(autouse=True)
def change_test_dir(request, monkeypatch):
    monkeypatch.chdir(request.fspath.dirname)


schema_file = os.path.join(os.path.dirname(__file__), "test_clickify.yaml")


def run(command):
    """Runs command, returns tuple of exit code, output"""
    print(f"running: {command}")
    try:
        return 0, subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT).strip().decode()
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


@click.command()
@clickify_parameters(schema_file)
def func(
    name: str,
    i: int,
    j: Optional[float] = 1,
    remainder: Optional[List[str]] = None,
    k: float = 2,
    nosubst_val: Optional[str] = None,
    unset_val: Optional[str] = None,
    unresolved_val: Optional[str] = None,
    tup: Optional[Tuple[int, str]] = None,
    files1: Optional[List[str]] = None,
    files2: Optional[List[str]] = None,
    files3: Optional[List[str]] = None,
    output: str = None,
):
    print(
        f"name:{name} i:{i} j:{j} k:{k} nosubst_val:{nosubst_val} "
        f"unset_val:{unset_val} unresolved_val:{unresolved_val} tup:{tup}"
    )
    print(f"remainder: {remainder}")
    print(f"files1: {files1}")
    print(f"files2: {files2}")
    print(f"files3: {files3}")
    print(f"output: {output}")


def test_clickify():
    retcode, output = run("./test_clickify.py xxx 10 yyy")
    assert retcode == 0
    print(output)
    assert verify_output(output, "doesn't resolve, skipping the default")
    assert verify_output(
        output, "name:xxx i:10 j:None k:2.0 nosubst_val:hello unset_val:None unresolved_val:None tup:None"
    )


if __name__ == "__main__":
    sys.exit(func())
