from pathlib import Path

import pytest

TESTDIR = Path(__file__).resolve().parent

@pytest.fixture(scope="package")
def testdir():
    return TESTDIR

# Change into directory where test_recipy.py lives
# As suggested by https://stackoverflow.com/questions/62044541/change-pytest-working-directory-to-test-case-directory
@pytest.fixture(autouse=True)
def change_test_dir(request, monkeypatch):
    monkeypatch.chdir(request.fspath.dirname)


