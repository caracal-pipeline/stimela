from dataclasses import dataclass

from .local import local_reporter
from .slurm import slurm_reporter

REPORTERS = {
    "native": local_reporter,
    "singularity": local_reporter,
    "slurm": slurm_reporter,
    "kube": local_reporter,  # For now - this needs testing. Slurm may be more appropriate.
}


@dataclass
class EmptyReport:
    @property
    def profiling_results(self):
        return vars(self)


def empty_reporter(now, task_info):
    return EmptyReport()
