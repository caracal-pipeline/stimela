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
from scabha.basetypes import EmptyListDefault, EmptyDictDefault, ListDefault
import datetime
from stimela.utils.xrun_asyncio import xrun

from stimela.exceptions import BackendError


# path to default srun binary
_default_srun_path = None

@dataclass
class SlurmOptions(object):
    enable: bool = False                            # enables passing off jobs to slurm via srun
    srun_path: Optional[str] = None                 # path to srun executable
    srun_opts: Dict[str, str] = EmptyDictDefault()  # extra options passed to srun. "--" prepended, and "_" replaced by "-"
    build_local = True                              # if True, images will be built locally (i.e. on the head node) even when slurm is enabled
    # these will be checked for
    required_mem_opts: Optional[List[str]] = ListDefault("mem", "mem-per-cpu", "mem-per-gpu")

    def get_executable(self):
        global _default_srun_path
        if self.srun_path is None:
            if _default_srun_path is None:
                _default_srun_path = which("srun")
                if not _default_srun_path:
                    _default_srun_path = False
            if _default_srun_path is False:
                raise BackendError(f"slurm 'srun' binary not found")
            return _default_srun_path
        else:
            if not os.access(self.srun_path, os.X_OK):
                raise BackendError(f"slurm.srun_path '{self.srun}' is not an executable")
            return self.srun
        
    def run_command_wrapper(self, args: List[str], fqname: Optional[str]=None, log: Optional[logging.Logger]=None) -> List[str]:
        output_args = [self.get_executable()]

        if fqname is not None:
            output_args += ["-J", fqname]

        # add all base options that have been specified
        for name, value in self.srun_opts.items():
            output_args += ["--" + name, value]

        output_args += args
        return output_args
    
    def build_command_wrapper(self, args: List[str], fqname: Optional[str]=None, log: Optional[logging.Logger]=None) -> List[str]:
        if self.build_local:
            return args
        return self.run_command_wrapper(args, fqname=fqname, log=log)
    
    def validate(self, log: logging.Logger):
        if self.required_mem_opts:
            if not set(self.srun_opts.keys()).intersection(self.required_mem_opts):
                raise BackendError(f"slurm.srun_opts must set one of the following: {', '.join(self.required_mem_opts)}")




SlurmOptionsSchema = OmegaConf.structured(SlurmOptions)

