from dataclasses import dataclass
from typing import Optional

def is_available():
    return True

def get_status():
    return "OK"

from .run_native import run, build_command_line, update_rlimits

@dataclass
class NativeBackendOptions(object):
    enable: bool = True
    virtual_env: Optional[str] = None
