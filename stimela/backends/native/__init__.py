import logging
from dataclasses import dataclass
from typing import Optional

import stimela

# add these as module attributes - use * as * to satisfy linter.
from .run_native import build_command_line as build_command_line
from .run_native import run as run
from .run_native import update_rlimits as update_rlimits


def is_available(opts=None):
    return True


def get_status():
    return "OK"


def is_remote():
    return False


@dataclass
class NativeBackendOptions(object):
    enable: bool = True
    virtual_env: Optional[str] = None


def init(backend: "stimela.backend.StimelaBackendOptions", log: logging.Logger):
    pass
