import shlex
from typing import Dict, Optional, Any 
from scabha.cargo import Cab 
from stimela import logger
from stimela.utils.xrun_poll import xrun
from stimela.exceptions import StimelaCabRuntimeError
from dataclasses import dataclass

@dataclass
class Batch(object):
    self.name = name
    self.cpus = cpus
    self.mem = mem
    self.email = email

    def __init_cab(cab: Cab, subst: Optional[Dict[str, Any]], log)
        self.cab = cab
        self.log = log
        self.args, self.venv = self.cab.build_command_line(subst)

class SlurmBatch(Batch):

    # This funtion can only be called after the __init_cab()
    def submit(self, jobfile, runcmd, *args):
        jobname = f"{self.name}.job"
        with open(jobfile) as fh:
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

        command_name = "srun"
        self.log.info("Submiting job to schedular. The job name is {jobname}")
        retcode = xrun(command_name, [jobfile], shell=False, log=self.log, 
                    output_wrangler=self.cab.apply_output_wranglers, 
                    return_errcode=True, command_name=command_name)

        # if retcode is not 0, and cab didn't declare itself a success,
        if retcode:
            if not cab.runtime_status:
                raise StimelaCabRuntimeError(f"{command_name} returned non-zero exit status {retcode}", log=self.log)
        else:
            if cab.runtime_status is False:
                raise StimelaCabRuntimeError(f"{command_name} was marked as failed based on its output", log=self.log)

        return retcode
