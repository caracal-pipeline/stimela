"""Bind-mount specifications shared by the container backends (singularity, podman).

Kept free of stimela.kitchen imports, since the backend option dataclasses need these at import time.
"""

import shutil
from dataclasses import dataclass
from enum import Enum
from tempfile import mkdtemp
from typing import Callable, Optional, Union

ReadWrite = Enum("BindMode", "ro rw", module=__name__)


@dataclass
class BindDir(object):
    """A bind-mount specification for container backends (singularity, podman)"""

    host: Optional[str] = None  # host path, default uses label, or else "ephemeral[:class]" for a temporary directory
    target: Optional[str] = None  # container path: ==host by default
    mode: ReadWrite = "rw"
    mkdir: bool = False  # create host directory if it doesn't exist
    conditional: Union[bool, str] = True  # bind conditionally (will be formula-evaluated)


class CustomTemporaryDirectory(object):
    """Custom context manager for tempfile.mkdtemp()."""

    def __init__(self, clean_up=True, dir=None, remover: Optional[Callable[[str], None]] = None):
        self.name = mkdtemp(dir=dir)
        self.clean_up = clean_up  # Workaround for < Python3.12
        self.remover = remover or shutil.rmtree  # function used to remove the directory

    def __enter__(self):
        return self.name

    def __exit__(self, exc_type, exc_value, traceback):
        if self.clean_up:
            self.remover(self.name)
