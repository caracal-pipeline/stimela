"""Tests for the deprecation warning mechanism (#371), deprecated cab
property (#313), and parameter alias / nom_de_guerre deprecation (#444).
"""

import re
import subprocess
import warnings

import pytest

from stimela.deprecation import (
    clear_deprecation_warnings,
    deprecation_warning,
    get_deprecation_summary,
    has_deprecation_warnings,
)


# Change into directory where this test file lives
@pytest.fixture(autouse=True)
def change_test_dir(request, monkeypatch):
    monkeypatch.chdir(request.fspath.dirname)


@pytest.fixture(autouse=True)
def _clear_warnings():
    """Clear the deprecation registry before each test."""
    clear_deprecation_warnings()
    yield
    clear_deprecation_warnings()


def run(command):
    """Runs command, returns tuple of exit code, output."""
    print(f"running: {command}")
    try:
        return 0, subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT).strip().decode()
    except subprocess.CalledProcessError as exc:
        return exc.returncode, exc.output.strip().decode()


def verify_output(output, *regexes):
    """Check that the given regexes appear in order in the output."""
    output = re.sub(r"\s+", " ", output)
    regex = "(.*?)".join(regexes)
    count = len(re.findall(regex, output))
    if not count:
        print("Error, expected regex pattern did not appear in the output:")
        print(f"  {regex}")
        return 0
    return count


# ---- Unit tests for the deprecation module (#371) ----


class TestDeprecationModule:
    """Unit tests for stimela.deprecation."""

    def test_deprecation_warning_registers(self):
        """deprecation_warning registers warnings and they show up in the summary."""
        assert not has_deprecation_warnings()

        deprecation_warning("feature X is deprecated", category="test")
        assert has_deprecation_warnings()

        lines = get_deprecation_summary()
        assert len(lines) == 1
        assert "feature X is deprecated" in lines[0]
        assert "[test]" in lines[0]

    def test_duplicate_suppression(self):
        """Identical warnings are suppressed after the first occurrence."""
        deprecation_warning("same warning", category="test")
        deprecation_warning("same warning", category="test")
        deprecation_warning("same warning", category="test")

        lines = get_deprecation_summary()
        assert len(lines) == 1
        assert "x3" in lines[0]

    def test_different_categories(self):
        """Warnings with different categories are tracked separately."""
        deprecation_warning("msg", category="cat_a")
        deprecation_warning("msg", category="cat_b")

        lines = get_deprecation_summary()
        assert len(lines) == 2

    def test_different_messages(self):
        """Warnings with different messages are tracked separately."""
        deprecation_warning("msg1", category="test")
        deprecation_warning("msg2", category="test")

        lines = get_deprecation_summary()
        assert len(lines) == 2

    def test_clear(self):
        """clear_deprecation_warnings empties the registry."""
        deprecation_warning("to be cleared", category="test")
        assert has_deprecation_warnings()
        clear_deprecation_warnings()
        assert not has_deprecation_warnings()

    def test_future_warning_issued(self):
        """A Python FutureWarning is issued for programmatic callers."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            deprecation_warning("test future warning", category="test")

        future_warnings = [x for x in w if issubclass(x.category, FutureWarning)]
        assert len(future_warnings) == 1
        assert "test future warning" in str(future_warnings[0].message)


# ---- Integration tests for deprecated cab (#313) ----


class TestDeprecatedCab:
    """Integration tests for the 'deprecated' property on cab definitions."""

    def test_deprecated_cab_warns(self):
        """Running a recipe that uses a deprecated cab should produce a deprecation warning."""
        retcode, output = run("stimela -v -b native exec test_deprecation.yml test_deprecated_cab")
        assert retcode == 0
        assert verify_output(output, "Deprecation warning", "old_cab.*deprecated", "echo_cab")

    def test_non_deprecated_cab_no_warning(self):
        """A non-deprecated cab should not trigger any deprecation warning."""
        retcode, output = run("stimela -v -b native exec test_deprecation.yml test_ndg_cab")
        assert retcode == 0
        # Should not contain a "cab.*deprecated" warning -- only the nom_de_guerre warning
        assert not verify_output(output, "cab.*is deprecated")


# ---- Integration tests for nom_de_guerre deprecation (#444) ----


class TestNomDeGuerreDeprecation:
    """Integration tests for nom_de_guerre deprecation warnings."""

    def test_nom_de_guerre_warns(self):
        """Using a parameter with nom_de_guerre should produce a deprecation warning."""
        retcode, output = run("stimela -v -b native exec test_deprecation.yml test_ndg_cab")
        assert retcode == 0
        assert verify_output(output, "Deprecation warning", "nom_de_guerre")


# ---- Unit tests for Cab.deprecated field (#313) ----


class TestCabDeprecatedField:
    """Tests that the deprecated field is properly part of the Cab dataclass."""

    def test_cab_has_deprecated_field(self):
        """Cab should have an optional 'deprecated' field."""
        import dataclasses

        from stimela.kitchen.cab import Cab

        fields = {f.name: f for f in dataclasses.fields(Cab)}
        assert "deprecated" in fields
        assert fields["deprecated"].default is None


# ---- Unit tests for cab-level alias resolution (#444) ----


class TestCabAliasMap:
    """Tests for cab-level alias mapping."""

    def test_cab_alias_resolution(self):
        """Cab.resolve_cab_aliases should remap deprecated parameter names."""
        from collections import OrderedDict

        from stimela.kitchen.cab import Cab

        cab = Cab(
            command="echo",
            inputs=OrderedDict(
                output_column={
                    "dtype": "str",
                    "required": True,
                    "aliases": ["out_col"],
                }
            ),
        )

        params = OrderedDict(out_col="DATA")
        resolved = cab.resolve_cab_aliases(params)

        assert "output_column" in resolved
        assert "out_col" not in resolved
        assert resolved["output_column"] == "DATA"

    def test_cab_no_alias_needed(self):
        """When canonical name is used, no remapping happens."""
        from collections import OrderedDict

        from stimela.kitchen.cab import Cab

        cab = Cab(
            command="echo",
            inputs=OrderedDict(
                output_column={
                    "dtype": "str",
                    "required": True,
                    "aliases": ["out_col"],
                }
            ),
        )

        params = OrderedDict(output_column="DATA")
        resolved = cab.resolve_cab_aliases(params)

        assert "output_column" in resolved
        assert resolved["output_column"] == "DATA"
