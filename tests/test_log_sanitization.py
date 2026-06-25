"""Tests for log path sanitization in stimelogging.py (issue #513)."""

from stimela.stimelogging import _RE_LOG_SANITIZE


def sanitize_path(path: str) -> str:
    """Replicate the log path sanitization logic from stimelogging.update_file_logger."""
    return _RE_LOG_SANITIZE.sub("_", path)


def test_plus_preserved_in_log_path():
    """The '+' character in J2000 coordinate names should not be replaced (issue #513)."""
    path = "logs/log-J1939+4540.txt"
    assert sanitize_path(path) == "logs/log-J1939+4540.txt"


def test_at_sign_preserved_in_log_path():
    """The '@' character should be preserved in log paths."""
    path = "logs/log-target@field.txt"
    assert sanitize_path(path) == "logs/log-target@field.txt"


def test_basic_alphanumeric_preserved():
    """Standard alphanumeric characters, dots, underscores, hyphens, and slashes are preserved."""
    path = "logs-20240101/log-my_recipe.step1.txt"
    assert sanitize_path(path) == "logs-20240101/log-my_recipe.step1.txt"


def test_special_chars_replaced():
    """Characters outside the allowed set are still replaced with underscore."""
    path = "logs/log-name with spaces.txt"
    assert sanitize_path(path) == "logs/log-name_with_spaces.txt"


def test_shell_metacharacters_replaced():
    """Shell metacharacters like $, !, &, etc. are replaced."""
    path = "logs/log-$var!.txt"
    assert sanitize_path(path) == "logs/log-_var_.txt"


def test_j2000_coordinate_full():
    """A realistic J2000 coordinate target name is preserved correctly."""
    path = "./logs-2024/log-recipe.J0521+1638.calibrate.txt"
    assert sanitize_path(path) == "./logs-2024/log-recipe.J0521+1638.calibrate.txt"


def test_negative_declination_preserved():
    """J2000 coordinates with negative declination (using '-') are preserved."""
    path = "logs/log-J1939-4540.txt"
    assert sanitize_path(path) == "logs/log-J1939-4540.txt"
