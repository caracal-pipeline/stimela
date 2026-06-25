import os
import tempfile
from unittest.mock import MagicMock

from .test_recipe import change_test_dir as change_test_dir
from .test_recipe import run, verify_output


def test_skip_expression_outputs_exist():
    """Test skip_if_outputs with expression using outputs_exist (issue #491)."""
    os.system("rm -f test_skip_cond[1234].tmp test_skip_cond4b.tmp test_skip_cond_input.tmp")

    # Create input file for freshness test (touch3 needs it to compare mtimes)
    with open("test_skip_cond_input.tmp", "w") as f:
        f.write("input")

    print("===== all files should be touched =====")
    retcode, output = run("stimela -b native run test_issue491_skip_conditionals.yml")
    assert retcode == 0, f"Failed:\n{output}"
    print(output)
    assert verify_output(output, "---INVOKING---", "touch test_skip_cond1.tmp")
    assert verify_output(output, "---INVOKING---", "touch test_skip_cond2.tmp")
    assert verify_output(output, "---INVOKING---", "touch test_skip_cond3.tmp")
    assert verify_output(output, "---INVOKING---", "touch test_skip_cond4.tmp")

    print("===== steps 1, 2, 4 should be skipped (outputs exist) =====")
    retcode, output = run("stimela -b native run test_issue491_skip_conditionals.yml")
    assert retcode == 0, f"Failed:\n{output}"
    print(output)
    # touch1 uses legacy "exist" mode
    assert not verify_output(output, "---INVOKING---", "touch test_skip_cond1.tmp")
    # touch2 uses expression mode with outputs_exist
    assert not verify_output(output, "---INVOKING---", "touch test_skip_cond2.tmp")
    # touch3 uses freshness check -- outputs should be fresh after first run
    assert not verify_output(output, "---INVOKING---", "touch test_skip_cond3.tmp")
    # touch4 uses compound AND expression with outputs_exist
    assert not verify_output(output, "---INVOKING---", "touch test_skip_cond4.tmp")

    print("===== cleaning up =====")
    os.system("rm -f test_skip_cond[1234].tmp test_skip_cond4b.tmp test_skip_cond_input.tmp")


def test_check_output_existence():
    """Test _check_output_existence helper function."""
    from stimela.kitchen.step import _check_output_existence

    with tempfile.TemporaryDirectory() as tmpdir:
        existing = os.path.join(tmpdir, "exists.txt")
        missing = os.path.join(tmpdir, "missing.txt")
        with open(existing, "w") as f:
            f.write("test")

        schema_file = MagicMock()
        schema_file.is_file_type = True
        schema_file.is_file_list_type = False

        schema_missing = MagicMock()
        schema_missing.is_file_type = True
        schema_missing.is_file_list_type = False

        outputs = {"out1": schema_file, "out2": schema_missing}
        params = {"out1": existing, "out2": missing}

        result = _check_output_existence(params, outputs)
        assert result["out1"] is True, "Existing file should return True"
        assert result["out2"] is False, "Missing file should return False"

    print("_check_output_existence test passed")


def test_check_output_freshness():
    """Test _check_output_freshness helper function."""
    import time

    from stimela.kitchen.step import _check_output_freshness

    with tempfile.TemporaryDirectory() as tmpdir:
        # Create an input file
        infile = os.path.join(tmpdir, "input.txt")
        with open(infile, "w") as f:
            f.write("input")

        time.sleep(0.05)

        # Create a fresh output (newer than input)
        fresh_out = os.path.join(tmpdir, "fresh_output.txt")
        with open(fresh_out, "w") as f:
            f.write("fresh")

        # Set a stale output (older than input) by backdating it
        stale_out = os.path.join(tmpdir, "stale_output.txt")
        with open(stale_out, "w") as f:
            f.write("stale")
        # Set mtime to 1 second before input
        input_mtime = os.path.getmtime(infile)
        os.utime(stale_out, (input_mtime - 1, input_mtime - 1))

        input_schema = MagicMock()
        input_schema.is_input = True
        input_schema.is_file_type = True
        input_schema.is_file_list_type = False
        input_schema.skip_freshness_checks = False

        fresh_schema = MagicMock()
        fresh_schema.is_file_type = True
        fresh_schema.is_file_list_type = False
        fresh_schema.skip_freshness_checks = False

        stale_schema = MagicMock()
        stale_schema.is_file_type = True
        stale_schema.is_file_list_type = False
        stale_schema.skip_freshness_checks = False

        inputs = {"infile": input_schema}
        outputs = {"fresh": fresh_schema, "stale": stale_schema}
        params = {"infile": infile, "fresh": fresh_out, "stale": stale_out}

        result = _check_output_freshness(params, inputs, outputs)
        assert result["fresh"] is True, "Fresh output should return True"
        assert result["stale"] is False, "Stale output should return False"

    print("_check_output_freshness test passed")
