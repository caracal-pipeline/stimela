import os
import sys
from pathlib import Path

# pyparsing's infix_notation uses deep recursion; nested formulas (e.g. 4+ nested IFs)
# exceed Python's default limit of 1000. See https://github.com/caracal-pipeline/stimela/issues/462
_MIN_RECURSION_LIMIT = 10000
if sys.getrecursionlimit() < _MIN_RECURSION_LIMIT:
    try:
        sys.setrecursionlimit(_MIN_RECURSION_LIMIT)
    except ValueError:
        pass

# -----------
# knicked from https://github.com/python-poetry/poetry/issues/273#issuecomment-1103812336
try:
    from importlib import metadata
except ImportError:  # for Python<3.8
    import importlib_metadata as metadata
__version__ = metadata.version(__package__)
del metadata  # optional, avoids polluting the results of dir()
# ------------

CONFIG = None

UID = os.getuid()
GID = os.getgid()

root = os.path.dirname(__file__)

CAB_PATH = os.path.join(root, "cargo/cab")
BASE_PATH = os.path.join(root, "cargo/base")

# Set up logging infrastructure
LOG_HOME = os.path.expanduser("~/.stimela")
# make sure directory exists
Path(LOG_HOME).mkdir(exist_ok=True)
# This is is the default log file. It logs stimela images, containers and processes
LOG_FILE = "{0:s}/stimela_logfile.json".format(LOG_HOME)

from .stimelogging import logger, log_exception  # noqa
