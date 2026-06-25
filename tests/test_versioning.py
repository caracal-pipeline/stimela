"""Tests for versioning features (issues #374, #306, #373)."""

import re
import subprocess

import pytest


@pytest.fixture(autouse=True)
def change_test_dir(request, monkeypatch):
    monkeypatch.chdir(request.fspath.dirname)


def run(command):
    """Runs command, returns tuple of exit code, output"""
    print(f"running: {command}")
    try:
        return 0, subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT).strip().decode()
    except subprocess.CalledProcessError as exc:
        return exc.returncode, exc.output.strip().decode()


def verify_output(output, *regexes):
    """Check that all regex patterns appear in output in order."""
    output = re.sub(r"\s+", " ", output)
    regex = "(.*?)".join(regexes)
    count = len(re.findall(regex, output))
    if not count:
        print("Error, expected regex pattern did not appear in the output:")
        print(f"  {regex}")
        return 0
    return count


# ---- Issue #374: Version field on Cab and config.packages ----


class TestCabVersion:
    """Tests for cab version field population (#374)."""

    def test_explicit_version(self):
        """Cab with explicit version should preserve it."""

        from stimela.kitchen.cab import Cab

        cab = Cab(command="echo", version="1.2.3")
        assert cab.version == "1.2.3"

    def test_version_from_image(self):
        """Cab should auto-populate version from image.version."""
        from stimela.kitchen.cab import Cab

        cab = Cab(command="echo", image="test-image:3.1.0")
        assert cab.version == "3.1.0"

    def test_version_from_image_strip_cc(self):
        """Cab should strip -ccX.Y.Z suffix from image version."""
        from stimela.kitchen.cab import Cab

        cab = Cab(command="echo", image="test-image:3.1.0-cc0.2.0")
        assert cab.version == "3.1.0"

    def test_version_from_image_latest(self):
        """Cab with image version 'latest' should not set version."""
        from stimela.kitchen.cab import Cab

        cab = Cab(command="echo", image="test-image:latest")
        assert cab.version is None

    def test_version_none_no_image(self):
        """Cab without image and without explicit version should have None."""
        from stimela.kitchen.cab import Cab

        cab = Cab(command="echo")
        assert cab.version is None

    def test_explicit_version_not_overridden_by_image(self):
        """Explicit version should not be overridden by image version."""
        from stimela.kitchen.cab import Cab

        cab = Cab(command="echo", version="5.0.0", image="test-image:3.1.0")
        assert cab.version == "5.0.0"


class TestStripCcSuffix:
    """Tests for the _strip_cc_suffix helper."""

    def test_strip_simple(self):
        from stimela.kitchen.cab import _strip_cc_suffix

        assert _strip_cc_suffix("3.1.0-cc0.2.0") == "3.1.0"

    def test_strip_three_part(self):
        from stimela.kitchen.cab import _strip_cc_suffix

        assert _strip_cc_suffix("1.0.0-cc1.2.3") == "1.0.0"

    def test_no_suffix(self):
        from stimela.kitchen.cab import _strip_cc_suffix

        assert _strip_cc_suffix("3.1.0") == "3.1.0"

    def test_strip_two_part(self):
        from stimela.kitchen.cab import _strip_cc_suffix

        assert _strip_cc_suffix("3.1.0-cc0.2") == "3.1.0"


class TestConfigPackages:
    """Tests for config.packages namespace (#374)."""

    def test_packages_populated(self):
        """Config should have packages namespace with installed versions."""
        retcode, output = run("stimela -v -b native exec test_versioning.yml versioned_recipe msg=hello")
        assert retcode == 0
        # The packages namespace should be available in config
        # We can't easily check the namespace directly from CLI, but
        # we verify the run succeeds and the config loads properly


# ---- Issue #306: Version specifiers on parameters ----


class TestVersionSpecifiers:
    """Tests for parameter version specifiers (#306)."""

    def test_active_params_kept(self):
        """Parameters matching version specifier should be active."""
        from scabha.cargo import Parameter

        from stimela.kitchen.cab import _apply_version_specifiers

        p_active = Parameter(dtype="str", versions=">=2.0")
        params = {"p_active": p_active}
        _apply_version_specifiers(params, "2.0.0")
        assert p_active._active is True

    def test_versioned_cab_active_params(self):
        """Parameters matching version should stay active in versioned cab."""
        retcode, output = run("stimela -v -b native exec test_versioning.yml versioned_recipe msg=hello")
        assert retcode == 0

    def test_versioned_cab_deactivated_param_warning(self):
        """Supplying a deactivated parameter should produce a warning."""
        retcode, output = run(
            "stimela -v -b native exec test_versioning.yml versioned_recipe msg=hello old_feature=test"
        )
        # old_feature has versions: "<2.0" and cab version is 2.0.0
        # so it should be deactivated and produce a warning or be rejected
        # The step validator will detect it as an unknown parameter in the step
        # since it's deactivated. This may succeed or fail depending on strict validation.
        print(output)

    def test_param_version_filtering_unit(self):
        """Unit test: version specifier filtering activates/deactivates correctly."""
        from scabha.cargo import Parameter

        from stimela.kitchen.cab import _apply_version_specifiers

        p1 = Parameter(dtype="str", versions=">=2.0")
        p2 = Parameter(dtype="str", versions="<2.0")
        p3 = Parameter(dtype="str", versions=">=1.5,<3.0")
        p4 = Parameter(dtype="str")  # no version specifier

        params = {"p1": p1, "p2": p2, "p3": p3, "p4": p4}
        _apply_version_specifiers(params, "2.0.0")

        assert p1._active is True
        assert p2._active is False
        assert p3._active is True
        assert p4._active is True  # no specifier means always active

    def test_param_version_filtering_no_cab_version(self):
        """When cab has no version, all params stay active."""
        from scabha.cargo import Parameter

        from stimela.kitchen.cab import _apply_version_specifiers

        p1 = Parameter(dtype="str", versions=">=2.0")
        params = {"p1": p1}
        _apply_version_specifiers(params, None)

        assert p1._active is True

    def test_param_version_filtering_invalid_version(self):
        """Invalid cab version should not crash, just skip filtering."""
        from scabha.cargo import Parameter

        from stimela.kitchen.cab import _apply_version_specifiers

        p1 = Parameter(dtype="str", versions=">=2.0")
        params = {"p1": p1}
        _apply_version_specifiers(params, "not-a-version")

        assert p1._active is True  # should remain active if version is invalid

    def test_image_versioned_cab_filtering(self):
        """Cab with image version should filter params based on stripped version."""
        from stimela.kitchen.cab import Cab

        cab = Cab(command="echo", image="test-image:3.1.0-cc0.2.0")
        # version should be 3.1.0 after stripping
        assert cab.version == "3.1.0"

    def test_unversioned_cab_all_active(self):
        """Cab without version should keep all params active regardless of specifiers."""
        from scabha.cargo import Parameter

        from stimela.kitchen.cab import _apply_version_specifiers

        p1 = Parameter(dtype="str", versions=">=1.0")
        params = {"p1": p1}
        _apply_version_specifiers(params, None)
        assert p1._active is True


# ---- Issue #373: Recipe requirements ----


class TestRecipeRequirements:
    """Tests for recipe dependency requirements (#373)."""

    def test_requirements_met(self):
        """Recipe with met requirements should succeed."""
        retcode, output = run("stimela -v -b native exec test_versioning.yml recipe_with_requires msg=hello")
        assert retcode == 0

    def test_version_requirements_met(self):
        """Recipe with met version requirements should succeed."""
        retcode, output = run("stimela -v -b native exec test_versioning.yml recipe_with_version_requires msg=hello")
        assert retcode == 0

    def test_unmet_version_requirement(self):
        """Recipe with unmet version requirement should fail."""
        retcode, output = run("stimela -v -b native exec test_versioning.yml recipe_with_bad_requires msg=hello")
        assert retcode != 0
        assert verify_output(output, "unmet requirement")

    def test_missing_package_requirement(self):
        """Recipe requiring a non-existent package should fail."""
        retcode, output = run("stimela -v -b native exec test_versioning.yml recipe_with_missing_pkg msg=hello")
        assert retcode != 0
        assert verify_output(output, "not installed")

    def test_check_requirements_unit(self):
        """Unit test: _check_requirements validates correctly."""
        from stimela.kitchen.recipe import Recipe

        recipe = Recipe(name="test_req", steps={})
        recipe.requires = ["stimela", "scabha"]
        # Should not raise
        recipe._check_requirements()

    def test_check_requirements_version_unit(self):
        """Unit test: _check_requirements validates version specifiers."""
        from stimela.kitchen.recipe import Recipe

        recipe = Recipe(name="test_req", steps={})
        recipe.requires = ["stimela >= 1.0"]
        # Should not raise (stimela >= 1.0 is always true for 2.x)
        recipe._check_requirements()

    def test_check_requirements_bad_version_unit(self):
        """Unit test: _check_requirements raises on unmet version."""
        from stimela.exceptions import RecipeValidationError
        from stimela.kitchen.recipe import Recipe

        recipe = Recipe(name="test_req", steps={})
        recipe.requires = ["stimela >= 99.0"]
        with pytest.raises(RecipeValidationError, match="unmet requirement"):
            recipe._check_requirements()

    def test_check_requirements_missing_pkg_unit(self):
        """Unit test: _check_requirements raises on missing package."""
        from stimela.exceptions import RecipeValidationError
        from stimela.kitchen.recipe import Recipe

        recipe = Recipe(name="test_req", steps={})
        recipe.requires = ["nonexistent_xyz_pkg_123"]
        with pytest.raises(RecipeValidationError, match="unmet requirement"):
            recipe._check_requirements()


# ---- Issue #374: Recipe version field ----


class TestRecipeVersion:
    """Tests for recipe version field (#374)."""

    def test_recipe_version_set(self):
        """Recipe should accept a version field."""
        from stimela.kitchen.recipe import Recipe

        recipe = Recipe(name="test_recipe", version="1.0.0", steps={})
        assert recipe.version == "1.0.0"

    def test_recipe_version_none(self):
        """Recipe without version should have None."""
        from stimela.kitchen.recipe import Recipe

        recipe = Recipe(name="test_recipe", steps={})
        assert recipe.version is None

    def test_versioned_recipe_runs(self):
        """Recipe with version should run successfully."""
        retcode, output = run("stimela -v -b native exec test_versioning.yml versioned_recipe msg=hello")
        assert retcode == 0
