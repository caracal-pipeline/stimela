"""Integration test for the output cache feature (issue #369)."""

import glob
import os

from .test_recipe import change_test_dir as change_test_dir
from .test_recipe import run, verify_output

_call_count = 0


def cached_function(x: int, y: str):
    """A function whose outputs we want to cache."""
    global _call_count
    _call_count += 1
    print(f"cached_function({x}, '{y}') call #{_call_count}")
    return dict(result=x * 2, message=y + y)


def test_cache_integration():
    """Test that output caching works end-to-end via the CLI."""
    # clean up any leftover cache files
    for f in glob.glob(".test-stimela-cache*"):
        os.unlink(f)

    print("===== first run: should execute and cache outputs =====")
    retcode, output = run("stimela -v -b native run test_cache_integration.yml")
    assert retcode == 0
    print(output)
    assert verify_output(output, "result = 42")
    assert verify_output(output, "message = hellohello")

    # verify cache files were created
    cache_files = glob.glob(".test-stimela-cache*")
    assert len(cache_files) > 0, "cache database files should have been created"

    print("===== second run: should find cached outputs =====")
    retcode, output = run("stimela -v -b native run test_cache_integration.yml")
    assert retcode == 0
    print(output)
    # should still produce the correct outputs
    assert verify_output(output, "result = 42")
    assert verify_output(output, "message = hellohello")
    # should mention cached outputs were found
    assert verify_output(output, "cached non-file output")
    # verify the number of cached outputs matches what we expect (result + message)
    assert verify_output(output, "2 cached non-file output")

    # clean up
    for f in glob.glob(".test-stimela-cache*"):
        os.unlink(f)
