import logging
import os.path
from dataclasses import dataclass
from typing import Any, Dict

import pytest

import stimela_tests
from scabha.basetypes import File
from scabha.schema_utils import paramfile_loader
from stimela.backends import StimelaBackendSchema, runner
from stimela.exceptions import BackendError
from stimela.kitchen.cab import Cab

testdir = os.path.abspath(os.path.dirname(stimela_tests.__file__))
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
    def set_fake_backend(func):
        def inner(name, opts={}):
            name, opts = func(name, opts)
            if name == backend_name:
                if off:
                    return None
                else:
                    this_runner = __import__("stimela.backends", fromlist=[backend_name])
                    return getattr(this_runner, backend_name)
            else:
                this_runner = __import__("stimela.backends", fromlist=["runner"])
                return this_runner.get_backend(name, opts)

        return inner

    return set_fake_backend


@set_fake_backend("singularity", off=False)
def fake_singularity_on(name, opts={}):
    return name, opts


@set_fake_backend("singularity", off=True)
def fake_singularity_off(name, opts={}):
    return name, opts


def test_default_select():
    print(
        "===== expecting no errors when using default backend selection ('singularity,native')"
        "and cab.image is not set ====="
    )

    simms_cab = Cab(**cabs.simms)
    backend_runner = runner.validate_backend_settings(backend_opts, logging.Logger, simms_cab)

    assert isinstance(backend_runner, runner.BackendRunner)
    assert backend_runner.backend_name == "native"


def test_native_priority_backend():
    print("===== expecting no errors when backend selection is 'native,singularity' and cab.image is not set =====")

    simms_cab = Cab(**cabs.simms)
    backend_opts.select = ["native", "singularity"]
    backend_runner = runner.validate_backend_settings(backend_opts, logging.Logger, simms_cab)

    assert backend_runner.backend_name == "native"


def test_container_backend_no_image():
    print("===== expecting errors when using a container backend and cab.image is not set =====")

    # import runner for this function context
    runner = __import__("stimela.backends", fromlist=["runner"]).runner
    runner.get_backend = fake_singularity_on

    simms_cab = Cab(**cabs.simms)
    backend_opts.select = ["singularity"]
    with pytest.raises(BackendError) as exception:
        runner.validate_backend_settings(backend_opts, logging.Logger, simms_cab)

    assert "require a container image" in str(exception.value)


def test_no_container_backend_yes_image():
    print("===== expecting errors when using a container backend and cab.image is not set =====")

    # import runner for this function context
    runner = __import__("stimela.backends", fromlist=["runner"]).runner
    runner.get_backend = fake_singularity_off

    simms_cab = Cab(**cabs.simms)
    backend_opts.select = ["singularity"]
    with pytest.raises(BackendError) as exception:
        runner.validate_backend_settings(backend_opts, logging.Logger, simms_cab)

    assert "not available" in str(exception.value)


def test_container_backend_yes_image():
    print("===== expecting no errors when using a container backend and cab.image is set =====")

    # import runner for this function context
    runner = __import__("stimela.backends", fromlist=["runner"]).runner
    runner.get_backend = fake_singularity_on

    simms_cab = Cab(**cabs.simms)
    backend_opts.select = ["singularity"]
    simms_cab.image = "foo-bar"
    backend_runner = runner.validate_backend_settings(backend_opts, logging.Logger, simms_cab)

    assert backend_runner.backend is runner.get_backend("singularity")


def test_native_backend_no_image():
    print("===== expecting no errors when using the native backend and cab.image is not set =====")

    simms_cab = Cab(**cabs.simms)
    backend_opts.select = ["native"]
    backend_runner = runner.validate_backend_settings(backend_opts, logging.Logger, simms_cab)

    assert backend_runner.backend is runner.get_backend("native")
