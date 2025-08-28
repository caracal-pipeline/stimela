from pathlib import Path
from shutil import rmtree

# List of file patterns for removal during clean up.
PATTERNS = ["log-*.txt", "stimela.*.deps", "stimela.stats.*"]


def pytest_sessionfinish(session, exitstatus):
    """Runs just before pytest returns the exit status to the system."""
    if exitstatus != 0:  # Tests were not successful - don't clean up.
        return

    test_folder = Path(__file__).parent
    log_folder = test_folder.joinpath("stimela_tests/test-logs")

    if log_folder.exists():
        rmtree(log_folder)

    for pattern in PATTERNS:
        for file_path in test_folder.rglob(pattern):
            file_path.unlink()
