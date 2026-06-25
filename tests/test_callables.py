from .test_recipe import change_test_dir as change_test_dir
from .test_recipe import run, verify_output


def callable_function(a: int, b: str):
    print(f"callable_function({a},'{b}')")
    return a * 2


def callable_function_dict(a: int, b: str):
    print(f"callable_function_dict({a},'{b}')")
    return dict(x=a * 2, y=b + b)


def test_wrangler_replace_suppress():
    print("===== expecting no errors =====")
    retcode, output = run("stimela -v -b native run test_callables.yml")
    assert retcode == 0
    print(output)
    assert verify_output(output, "y = 46barbar")
    # verify that step s6_implicit (implicit output, no flavour.output specified) produced x = 14
    assert verify_output(output, "s6_implicit.*x = 14")


def test_implicit_output_single():
    """When a python callable has exactly one output and flavour.output is not set,
    the output should be implicitly selected."""
    from collections import OrderedDict
    from unittest.mock import MagicMock

    from scabha.cargo import Parameter

    from stimela.backends.flavours import _CallableFlavour

    flavour = _CallableFlavour()
    cab = MagicMock()
    cab.name = "test_cab"
    cab.outputs = OrderedDict({"x": Parameter(dtype="int")})
    flavour.finalize(cab)
    assert flavour.output == "x"


def test_explicit_output_still_works():
    """When flavour.output is explicitly set, it should be used as-is."""
    from collections import OrderedDict
    from unittest.mock import MagicMock

    from scabha.cargo import Parameter

    from stimela.backends.flavours import _CallableFlavour

    flavour = _CallableFlavour(output="x")
    cab = MagicMock()
    cab.name = "test_cab"
    cab.outputs = OrderedDict({"x": Parameter(dtype="int"), "y": Parameter(dtype="str")})
    flavour.finalize(cab)
    assert flavour.output == "x"


def test_no_implicit_output_multiple():
    """When a python callable has multiple outputs and flavour.output is not set,
    the output should NOT be implicitly selected (remains None)."""
    from collections import OrderedDict
    from unittest.mock import MagicMock

    from scabha.cargo import Parameter

    from stimela.backends.flavours import _CallableFlavour

    flavour = _CallableFlavour()
    cab = MagicMock()
    cab.name = "test_cab"
    cab.outputs = OrderedDict({"x": Parameter(dtype="int"), "y": Parameter(dtype="str")})
    flavour.finalize(cab)
    assert flavour.output is None


def test_no_implicit_output_zero():
    """When a python callable has no outputs, flavour.output should remain None."""
    from collections import OrderedDict
    from unittest.mock import MagicMock

    from stimela.backends.flavours import _CallableFlavour

    flavour = _CallableFlavour()
    cab = MagicMock()
    cab.name = "test_cab"
    cab.outputs = OrderedDict()
    flavour.finalize(cab)
    assert flavour.output is None
