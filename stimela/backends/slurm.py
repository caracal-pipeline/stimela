import subprocess
import os
import re
import logging
from stimela import utils
import stimela
from shutil import which
from dataclasses import dataclass, make_dataclass
from omegaconf import OmegaConf
from typing import Dict, List, Any, Optional, Tuple
from contextlib import ExitStack
from scabha.basetypes import EmptyListDefault
import datetime
from stimela.utils.xrun_asyncio import xrun

from stimela.exceptions import BackendError


# path to default srun binary
_default_srun_path = None

# map from SlurmOptions attributes to srun options

# Dictionary of supported srun options -- these directly map to fields of the SlurmOptions dataclass
# Just keep on adding them here as needed
_srun_options = dict(
    account=str,
    chdir=str,
    clusters=str,
    constraint=str,
    mem=str,
    mem_per_cpu=str,
    mincpus=int,
    partition=int
)

# create the basic options dataclass
_BaseSlurmOptions = make_dataclass("BaseSlurmOptions", 
                    [(name, Optional[dtype], None) for name, dtype in _srun_options.items()]
)

@dataclass
class SlurmOptions(_BaseSlurmOptions):
    enable: bool = True
    srun: Optional[str] = None              # path to executable

    def get_executable(self):
        global _default_srun_path
        if self.srun is None:
            if _default_srun_path is None:
                _default_srun_path = which("srun")
                if not _default_srun_path:
                    _default_srun_path = False
            if _default_srun_path is False:
                raise BackendError(f"slurm 'srun' binary not found")
            return _default_srun_path
        else:
            if not os.access(self.srun, os.X_OK):
                raise BackendError(f"slurm.srun path '{self.srun}' is not an executable")
            return self.srun
        
    def command_wrapper(self, args: List[str]):
        output_args = [self.get_executable()]

        # add all base options that have been specified
        for name in _srun_options.keys():
            value = getattr(self, name)
            if value is not None:
                output_args += ["--" + name.replace("_", "-"), value]

        output_args += args
        return args


SlurmOptionsSchema = OmegaConf.structured(SlurmOptions)

