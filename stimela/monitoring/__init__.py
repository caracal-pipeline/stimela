from dataclasses import dataclass

from .local import local_reporter
from .slurm import slurm_reporter


@dataclass
class DummyReport:
    @property
    def profiling_results(self):
        return vars(self)


def dummy_reporter(now, task_info):
    return DummyReport()


REPORTERS = {
    "native": local_reporter,
    "singularity": local_reporter,
    "slurm": slurm_reporter,
    "kube": local_reporter,  # For now - this needs testing. Slurm may be more appropriate.
    "dummy": dummy_reporter,
}
