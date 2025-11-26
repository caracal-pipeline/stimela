import logging
import os
from dataclasses import dataclass
from shutil import which
from typing import Any, Dict, List, Optional

from omegaconf import OmegaConf

from scabha.basetypes import EmptyDictDefault
from stimela.exceptions import BackendError

# path to default srun binary
_default_srun_path = None


@dataclass
class SlurmOptions(object):
    # enables passing off jobs to slurm via srun
    enable: bool = False
    # path to srun executable
    srun_path: Optional[str] = None
    # extra options passed to srun. "--" prepended, and "_" replaced by "-"
    srun_opts: Dict[str, str] = EmptyDictDefault()
    # extra options passed to srun for build commands. If None, use srun_opts
    srun_opts_build: Optional[Dict[str, str]] = None
    # if True, images will be built locally (i.e. on the head node) even when slurm is enabled
    build_local: bool = True

    # ## disabling this for now
    # # these will be checked for
    # required_mem_opts: Optional[List[str]] = ListDefault("mem", "mem-per-cpu", "mem-per-gpu")
    # # this will be applied if the required above are missing
    # default_mem_opt: str = "8GB"

    def get_executable(self):
        global _default_srun_path
        if self.srun_path is None:
            if _default_srun_path is None:
                _default_srun_path = which("srun")
                if not _default_srun_path:
                    _default_srun_path = False
            if _default_srun_path is False:
                raise BackendError("slurm 'srun' binary not found")
            return _default_srun_path
        else:
            if not os.access(self.srun_path, os.X_OK):
                raise BackendError(f"slurm.srun_path '{self.srun}' is not an executable")
            return self.srun

    def _wrap(
        self,
        srun_opts: Dict[str, Any],
        args: List[str],
        log_args: List[str],
        ephem_binds: Dict[str, str] = {},
        fqname: Optional[str] = None,
    ) -> List[str]:
        output_args = [self.get_executable()]

        # reverse fqname to make job name (more informative that way)
        if fqname is not None:
            output_args += ["-J", ".".join(fqname.split(".")[::-1])]

        # add all base options that have been specified
        for name, value in srun_opts.items():
            output_args += ["--" + name, value]

        # use wrapper script if ephemeral bindings are required
        if ephem_binds:
            output_args.append(os.path.join(os.path.dirname(__file__), "slurm_runner.sh"))
            for name, basedir in ephem_binds.items():
                output_args.append(f"{basedir}::{name}")
            output_args.append("--")

        return output_args + args, output_args + log_args

    def wrap_run_command(
        self,
        args: List[str],
        log_args: List[str],
        ephem_binds: Dict[str, str] = {},
        fqname: Optional[str] = None,
        log: Optional[logging.Logger] = None,
    ) -> List[str]:
        return self._wrap(self.srun_opts, args, log_args, ephem_binds=ephem_binds, fqname=fqname)

    def wrap_build_command(
        self, args: List[str], fqname: Optional[str] = None, log: Optional[logging.Logger] = None
    ) -> List[str]:
        if self.build_local:
            return args, args
        return self._wrap(
            self.srun_opts_build if self.srun_opts_build is not None else self.srun_opts,
            args,
            log_args=args,
            fqname=fqname,
        )

    def validate(self, log: logging.Logger):
        pass
        # if self.required_mem_opts:
        #     if not set(self.srun_opts.keys()).intersection(self.required_mem_opts):
        #         self.srun_opts['mem'] = self.default_mem_opt


SlurmOptionsSchema = OmegaConf.structured(SlurmOptions)
