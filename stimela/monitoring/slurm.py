from dataclasses import dataclass


# NOTE(JSKenyon): Currently the SlurmReport contains no information. It is possible that we may be
# able to add some reporting using the `sacct` command - investigate.
@dataclass
class SlurmReport:
    @property
    def profiling_results(self):
        return vars(self)


def slurm_reporter(now, task_info):
    return SlurmReport()
