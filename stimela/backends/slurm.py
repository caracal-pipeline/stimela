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
from scabha.basetypes import EmptyListDefault, EmptyDictDefault
import datetime
from stimela.utils.xrun_asyncio import xrun

from stimela.exceptions import BackendError


# path to default srun binary
_default_srun_path = None


@dataclass
class SlurmOptions(object):
    enable: bool = True
    srun_path: Optional[str] = None              # path to executable
    srun_opts: Dict[str, str] = EmptyDictDefault()
    build_local = True

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
        
    def run_command_wrapper(self, args: List[str], fqname: Optional[str]=None) -> List[str]:
        output_args = [self.get_executable()]

        if fqname is not None:
            output_args += ["-J", fqname]

        # add all base options that have been specified
        for name, value in self.srun_opts.items():
            output_args += ["--" + name.replace("_", "-"), value]

        output_args += args
        return output_args
    
    def build_command_wrapper(self, args: List[str], fqname: Optional[str]=None) -> List[str]:
        if self.build_local:
            return args
        return self.run_command_wrapper(args, fqname=fqname)



SlurmOptionsSchema = OmegaConf.structured(SlurmOptions)

