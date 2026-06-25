import copy
import logging
import warnings
from dataclasses import dataclass
from typing import Any, Dict

import pytest
from scabha.basetypes import File
from scabha.schema_utils import paramfile_loader

from stimela.backends import StimelaBackendSchema, runner
from stimela.exceptions import BackendError
from stimela.kitchen.cab import Cab

from . import testdir

recipe_file = File(f"{testdir}/test_recipe.yml")


@dataclass
class SchemaSpec:
    cabs: Dict[str, Cab]
    recipe: Any
    opts: Any


cabs = paramfile_loader(recipe_file, schema_spec=SchemaSpec)[recipe_file.BASENAME].cabs
backend_opts = StimelaBackendSchema


# fool the backend validator on systems without target backend
# this is OK since we are just testing the backend.validate_backend_settings_function
def set_fake_backend(backend_name, off=False):
    """Creates a fake backend getter for testing.

    For 'apptainer' (and the deprecated 'singularity' alias), the actual module
    is 'singularity', so we import that module when either name is requested.
    """

    def set_fake_backend(func):
        def inner(name, opts={}):
            name, opts = func(name, opts)
            if name == backend_name:
                if off:
                    return None
                else:
                    # Both 'apptainer' and 'singularity' map to the singularity module
                    module_name = "singularity" if backend_name in ("apptainer", "singularity") else backend_name
                    this_runner = __import__("stimela.backends", fromlist=[module_name])
                    return getattr(this_runner, module_name)
            else:
                this_runner = __import__("stimela.backends", fromlist=["runner"])
                return this_runner.get_backend(name, opts)

        return inner

    return set_fake_backend


@set_fake_backend("apptainer", off=False)
def fake_apptainer_on(name, opts={}):
    return name, opts


@set_fake_backend("apptainer", off=True)
def fake_apptainer_off(name, opts={}):
    return name, opts


def test_default_select():
    print(
        "===== expecting no errors when using default backend selection ('apptainer,native')"
        "and cab.image is not set ====="
    )

    simms_cab = Cab(**cabs.simms)
    backend_runner = runner.validate_backend_settings(backend_opts, logging.Logger, simms_cab)

    assert isinstance(backend_runner, runner.BackendRunner)
    assert backend_runner.backend_name == "native"


def test_native_priority_backend():
    print("===== expecting no errors when backend selection is 'native,apptainer' and cab.image is not set =====")

    simms_cab = Cab(**cabs.simms)
    backend_opts.select = ["native", "apptainer"]
    backend_runner = runner.validate_backend_settings(backend_opts, logging.Logger, simms_cab)

    assert backend_runner.backend_name == "native"


def test_container_backend_no_image():
    print("===== expecting errors when using a container backend and cab.image is not set =====")

    # import runner for this function context
    runner = __import__("stimela.backends", fromlist=["runner"]).runner
    runner.get_backend = fake_apptainer_on

    simms_cab = Cab(**cabs.simms)
    backend_opts.select = ["apptainer"]
    with pytest.raises(BackendError) as exception:
        runner.validate_backend_settings(backend_opts, logging.Logger, simms_cab)

    assert "container image not specified by cab" in str(exception.value)


def test_no_container_backend_yes_image():
    print("===== expecting errors when container backend is not available =====")

    # import runner for this function context
    runner = __import__("stimela.backends", fromlist=["runner"]).runner
    runner.get_backend = fake_apptainer_off

    simms_cab = Cab(**cabs.simms)
    backend_opts.select = ["apptainer"]
    with pytest.raises(BackendError) as exception:
        runner.validate_backend_settings(backend_opts, logging.Logger, simms_cab)

    assert "unable to select a backend" in str(exception.value)
    assert "apptainer" in str(exception.value)


def test_container_backend_yes_image():
    print("===== expecting no errors when using a container backend and cab.image is set =====")

    # import runner for this function context
    runner = __import__("stimela.backends", fromlist=["runner"]).runner
    runner.get_backend = fake_apptainer_on

    simms_cab = Cab(**cabs.simms)
    backend_opts.select = ["apptainer"]
    simms_cab.image = "foo-bar"
    backend_runner = runner.validate_backend_settings(backend_opts, logging.Logger, simms_cab)

    assert backend_runner.backend is runner.get_backend("apptainer")


def test_native_backend_no_image():
    print("===== expecting no errors when using the native backend and cab.image is not set =====")

    simms_cab = Cab(**cabs.simms)
    backend_opts.select = ["native"]
    backend_runner = runner.validate_backend_settings(backend_opts, logging.Logger, simms_cab)

    assert backend_runner.backend is runner.get_backend("native")


def test_apptainer_priority_no_native_no_apptainer():
    print("===== expecting errors since both backends are not available =====")

    simms_cab = Cab(**cabs.simms)
    backend_opts2 = copy.deepcopy(backend_opts)
    backend_opts2.select = ["apptainer", "native"]
    backend_opts2.native.enable = False

    with pytest.raises(BackendError) as exception:
        runner.validate_backend_settings(backend_opts2, logging.Logger, simms_cab)

    assert "unable to select a backend" in str(exception.value)
    assert "apptainer" in str(exception.value)
    assert "native: disabled" in str(exception.value)


def test_apptainer_priority_no_native():
    print("===== expecting no errors since apptainer is available =====")

    # import runner for this function context
    runner = __import__("stimela.backends", fromlist=["runner"]).runner
    runner.get_backend = fake_apptainer_on

    backend_opts2 = copy.deepcopy(backend_opts)
    backend_opts2.native.enable = False
    simms_cab = Cab(**cabs.simms)
    backend_opts2.select = ["apptainer", "native"]
    simms_cab.image = "foo-bar"
    backend_runner = runner.validate_backend_settings(backend_opts2, logging.Logger, simms_cab)

    assert backend_runner.backend is runner.get_backend("apptainer")


def test_apptainer_priority_no_native_no_image():
    print("===== expecting errors since container image is not set and native is disabled =====")

    # import runner for this function context
    runner = __import__("stimela.backends", fromlist=["runner"]).runner
    runner.get_backend = fake_apptainer_on

    simms_cab = Cab(**cabs.simms)
    backend_opts2 = copy.deepcopy(backend_opts)
    backend_opts2.select = ["apptainer", "native"]
    backend_opts2.native.enable = False

    with pytest.raises(BackendError) as exception:
        runner.validate_backend_settings(backend_opts2, logging.Logger, simms_cab)

    assert "container image not specified by cab" in str(exception.value)
    assert "native: disabled" in str(exception.value)


def test_singularity_alias_resolves_to_apptainer():
    """Test that using the deprecated 'singularity' name in select resolves to 'apptainer'."""
    from stimela.backends import StimelaBackendOptions, _resolve_backend_name

    # Test the name resolution function directly
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        resolved = _resolve_backend_name("singularity")
        assert resolved == "apptainer"
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)

    # Test that __post_init__ resolves 'singularity' in the select list
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        opts = StimelaBackendOptions(select="singularity,native")
    assert "apptainer" in opts.select
    assert "singularity" not in opts.select

    # Verify validate_backend_settings still works with the resolved config
    simms_cab = Cab(**cabs.simms)
    backend_opts2 = copy.deepcopy(backend_opts)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        backend_opts2.select = ["singularity", "native"]
    backend_runner = runner.validate_backend_settings(backend_opts2, logging.Logger, simms_cab)
    assert isinstance(backend_runner, runner.BackendRunner)
