import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

from omegaconf import OmegaConf


@dataclass
class SSHOptions(object):
    # enables passing off jobs to slurm via srun
    enable: bool = False
    # path to srun executable
    user: Optional[str] = None
    # path to srun executable
    host: Optional[str] = None
    # extra options passed to srun. "--" prepended, and "_" replaced by "-"
    remote_env: Optional[str] = None
    remote_singularity: Optional[str] = None

    def _wrap(
        self,
        args: List[str],
        log_args: List[str],
    ) -> List[str]:
        output_args = ["ssh", f"{self.user}@{self.host}"]

        output_args.append("VIRTUAL_ENV=/home/kenyon/git_packages/stimela/.venv")

        return output_args + args, output_args + log_args

    def wrap_run_command(
        self,
        args: List[str],
        log_args: List[str],
        ephem_binds: Dict[str, str] = {},
        fqname: Optional[str] = None,
        log: Optional[logging.Logger] = None,
    ) -> List[str]:
        return self._wrap(args, log_args)

    def wrap_build_command(
        self, args: List[str], fqname: Optional[str] = None, log: Optional[logging.Logger] = None
    ) -> List[str]:
        return self._wrap(args, args)


SSHSchema = OmegaConf.structured(SSHOptions)
