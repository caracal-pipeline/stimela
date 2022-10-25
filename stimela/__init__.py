import os
from pathlib import Path

__author__ = """Sphesihle Makhathini, Oleg Smirnov and RATT"""
__email__ = "sphemakh@gmail.com"
__version__ = "2.0rc2"

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

from .stimelogging import logger, log_exception
