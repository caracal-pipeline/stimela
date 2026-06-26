import os
import shutil

from .test_recipe import change_test_dir as change_test_dir
from .test_recipe import run, verify_output


def test_clean_dry_run():
    """Test that --dry-run lists output files without deleting them."""
    # Create files that the recipe's outputs would reference
    for path in ("clean_test_output.txt", "clean_test.ms"):
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.isfile(path):
            os.remove(path)

    retcode, output = run("stimela -v clean -d test_clean.yml clean-recipe")
    assert retcode == 0
    # Should list the output files
    assert verify_output(output, "would remove")
    assert verify_output(output, "clean_test_output.txt")
    assert verify_output(output, "clean_test.ms")


def test_clean_removes_files():
    """Test that clean actually removes output files."""
    # Create dummy output files that the recipe would produce
    with open("clean_test_output.txt", "w") as f:
        f.write("test data")
    os.makedirs("clean_test.ms", exist_ok=True)
    os.makedirs("clean_test_outdir", exist_ok=True)

    assert os.path.exists("clean_test_output.txt")
    assert os.path.isdir("clean_test.ms")
    assert os.path.isdir("clean_test_outdir")

    retcode, output = run("stimela -v clean test_clean.yml clean-recipe")
    assert retcode == 0
    assert verify_output(output, "removed")

    # Files should be gone
    assert not os.path.exists("clean_test_output.txt")
    assert not os.path.exists("clean_test.ms")
    assert not os.path.exists("clean_test_outdir")


def test_clean_nonexistent_files():
    """Test that clean handles missing files gracefully."""
    # Make sure the files don't exist
    for path in ("clean_test_output.txt", "clean_test.ms", "clean_test_outdir"):
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.isfile(path):
            os.remove(path)

    retcode, output = run("stimela -v clean test_clean.yml clean-recipe")
    assert retcode == 0
    # Should report 0 removed
    assert verify_output(output, "removed 0")
