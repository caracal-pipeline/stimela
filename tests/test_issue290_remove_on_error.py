import os
import tempfile
from unittest.mock import MagicMock

from .test_recipe import change_test_dir as change_test_dir


def test_remove_outputs_on_error():
    """Test that _remove_outputs_on_error cleans up files marked with remove_on_error (issue #290)."""
    from stimela.kitchen.step import _remove_outputs_on_error

    log = MagicMock()

    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test files
        file1 = os.path.join(tmpdir, "output1.zarr")
        os.makedirs(file1)  # directory output (like zarr)
        file2 = os.path.join(tmpdir, "output2.txt")
        with open(file2, "w") as f:
            f.write("test")
        file3 = os.path.join(tmpdir, "keep_this.txt")
        with open(file3, "w") as f:
            f.write("keep")

        assert os.path.exists(file1)
        assert os.path.exists(file2)
        assert os.path.exists(file3)

        # Create mock schemas
        schema_remove_dir = MagicMock()
        schema_remove_dir.metadata = {"remove_on_error": True}
        schema_remove_dir.is_file_type = True
        schema_remove_dir.is_file_list_type = False

        schema_remove_file = MagicMock()
        schema_remove_file.metadata = {"remove_on_error": True}
        schema_remove_file.is_file_type = True
        schema_remove_file.is_file_list_type = False

        schema_keep = MagicMock()
        schema_keep.metadata = {}
        schema_keep.is_file_type = True
        schema_keep.is_file_list_type = False

        outputs_schema = {
            "zarr-out": schema_remove_dir,
            "text-out": schema_remove_file,
            "keep-out": schema_keep,
        }
        params = {
            "zarr-out": file1,
            "text-out": file2,
            "keep-out": file3,
        }

        _remove_outputs_on_error(params, outputs_schema, log)

        # zarr directory should be removed
        assert not os.path.exists(file1), "Directory with remove_on_error should have been removed"
        # text file should be removed
        assert not os.path.exists(file2), "File with remove_on_error should have been removed"
        # file without remove_on_error should be kept
        assert os.path.exists(file3), "File without remove_on_error should be kept"

    print("remove_on_error test passed")


def test_remove_outputs_on_error_list():
    """Test remove_on_error with file list-type outputs."""
    from stimela.kitchen.step import _remove_outputs_on_error

    log = MagicMock()

    with tempfile.TemporaryDirectory() as tmpdir:
        file1 = os.path.join(tmpdir, "out1.txt")
        file2 = os.path.join(tmpdir, "out2.txt")
        with open(file1, "w") as f:
            f.write("1")
        with open(file2, "w") as f:
            f.write("2")

        schema = MagicMock()
        schema.metadata = {"remove_on_error": True}
        schema.is_file_type = False
        schema.is_file_list_type = True

        outputs_schema = {"outputs": schema}
        params = {"outputs": [file1, file2]}

        _remove_outputs_on_error(params, outputs_schema, log)

        assert not os.path.exists(file1), "List file 1 should be removed"
        assert not os.path.exists(file2), "List file 2 should be removed"

    print("remove_on_error list test passed")
