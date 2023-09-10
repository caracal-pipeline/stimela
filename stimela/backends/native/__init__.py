from dataclasses import dataclass
from typing import Optional
import logging
import stimela

def is_available():
    return True

def get_status():
    return "OK"

def is_remote():
    return False

from .run_native import run, build_command_line, update_rlimits

@dataclass
class NativeBackendOptions(object):
    enable: bool = True
    virtual_env: Optional[str] = None

def init(backend: 'stimela.backend.StimelaBackendOptions', log: logging.Logger):
    pass
