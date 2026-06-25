"""Tests for log path sanitization (issue #513).

Verifies that the regex in stimelogging.update_file_logger preserves
characters that are safe in filenames (like + and @) while still
replacing truly unsafe characters.
"""

import inspect
import re

from stimela.stimelogging import update_file_logger

_source = inspect.getsource(update_file_logger)
_match = re.search(r're\.sub\(r"(\[\^[^"]+\])"', _source)
assert _match, "Could not find sanitization regex in update_file_logger source"
_PATTERN = _match.group(1)


def _sanitize(path):
    return re.sub(_PATTERN, "_", path)


def test_plus_preserved_in_log_name():
    assert _sanitize("target+03.log") == "target+03.log"
    assert _sanitize("J0538+2817.log") == "J0538+2817.log"


def test_at_preserved_in_log_name():
    assert _sanitize("user@host.log") == "user@host.log"


def test_unsafe_chars_replaced():
    assert _sanitize("log dir/name with spaces.log") == "log_dir/name_with_spaces.log"
    assert _sanitize("name!special#chars.log") == "name_special_chars.log"


def test_path_separators_preserved():
    assert _sanitize("logs/sub/file.log") == "logs/sub/file.log"
    assert _sanitize("./relative/path.log") == "./relative/path.log"


def test_dots_dashes_underscores_preserved():
    assert _sanitize("my-step.sub-step.log") == "my-step.sub-step.log"
    assert _sanitize("step_name.log") == "step_name.log"
