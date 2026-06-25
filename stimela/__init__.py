import os
from pathlib import Path

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

# Apply scabha patches for complex type coercion (issue #364)
from ._patches import apply_patches as _apply_patches  # noqa: E402

_apply_patches()
del _apply_patches
