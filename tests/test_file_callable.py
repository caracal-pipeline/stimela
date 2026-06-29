from .test_recipe import change_test_dir as change_test_dir
from .test_recipe import run, verify_output


def test_file_callable():
    """Test python flavour cab with (path/file.py)function syntax."""
    print("===== testing (path/file.py)function syntax for python callable =====")
    retcode, output = run("stimela -v -b native run test_file_callable.yml test_file_callables")
    assert retcode == 0
    print(output)
    # s2 should produce x=21 (7*3) and y=worldworldworld (world*3)
    assert verify_output(output, "y = worldworldworld")


def test_file_callable_dynschema():
    """Test dynamic_schema with (path/file.py)function syntax.

    This only tests that the YAML loads and the dynamic schema is applied
    (via doc command which triggers finalization).
    """
    print("===== testing (path/file.py)function syntax for dynamic schema =====")
    retcode, output = run("stimela -v doc test_file_callable.yml")
    assert retcode == 0
    print(output)
