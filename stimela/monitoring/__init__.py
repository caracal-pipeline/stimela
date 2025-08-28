from .local import local_reporter
from .slurm import slurm_reporter

REPORTERS = {
    "native": local_reporter,
    "singularity": local_reporter,
    "slurm": slurm_reporter,
    "kube": local_reporter,  # For now - this needs testing. Slurm may be more appropriate.
}
