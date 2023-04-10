import shlex
from typing import Dict, Optional, Any 
from stimela.kitchen.batch import Batch
from stimela import logger
from stimela.utils.xrun_poll import xrun
from stimela.exceptions import StimelaCabRuntimeError
import subprocess


binary = "srun"

class SlurmBatch(Batch):

    def exists(self):
        try:
            subprocess.check_output(["which", binary])
        except subprocess.CalledProcessError:
            raise SystemError(f"The '{binary}' tool could not be found on the system. Is 'slurm' installed?")

    # This funtion can only be called after the __init_cab__()
    def submit(self, jobfile, runcmd, *args):
        self.exists()
        jobname = f"{self.name}.job"
        with open(jobfile, "w") as fh:
            fh.writelines("#!/bin/bash\n")
            fh.writelines(f"#SBATCH --job-name={jobname}\n")
            fh.writelines(f"#SBATCH --qos=normal\n")
            fh.writelines(f"#SBATCH --mail-type=ALL\n")
            fh.writelines(f"#SBATCH --error=stimela_slurm_{self.name}.err\n")
            #fh.writelines(f"#SBATCH --time=2-00:00\n")
            if self.mem:
                fh.writelines(f"#SBATCH --mem={self.mem}\n")
            if self.cpus:
                fh.writelines(f"#SBATCH --cpus-per-task={self.cpus}\n")
            if self.email:
                fh.writelines(f"#SBATCH --mail-user={self.email}\n")
            for arg in args:
                fh.writelines(f"{arg}\n")

            fh.writelines("\n")
            fh.writelines(f"{runcmd}\n")

        self.log.info(f"Submiting job to schedular. The job name is {jobname}")
        retcode = xrun(binary, [jobfile], shell=False, log=self.log, 
                    output_wrangler=self.cab.apply_output_wranglers, 
                    return_errcode=True, command_name=binary)

        # if retcode is not 0, and cab didn't declare itself a success,
        if retcode:
            if not self.cab.runtime_success:
                raise StimelaCabRuntimeError(f"{binary} returned non-zero exit status {retcode}", log=self.log)
        else:
            if self.cab.runtime_success is False:
                raise StimelaCabRuntimeError(f"{binary} was marked as failed based on its output", log=self.log)

        return retcode
