import shlex
from typing import Dict, Optional, Any
from scabha.cargo import Cab
from stimela import logger, schedulers
from stimela.utils.xrun_poll import xrun
from stimela.exceptions import StimelaCabRuntimeError
from stimela.schedulers import slurm


def run(cab: Cab, log, subst: Optional[Dict[str, Any]] = None,
        batch: schedulers.Batch = None):

    if batch:
        batch.__init_cab(cab, subst, log)
        batch.submit()

    #-------------------------------------------------------
    args, venv = cab.build_command_line(subst)

    command_name = args[0]

    if venv:
        args = ["/bin/bash", "--rcfile", f"{venv}/bin/activate", "-c", " ".join(shlex.quote(arg) for arg in args)]

    log.debug(f"command line is {args}")
    
    cab.reset_runtime_status()

    retcode = xrun(args[0], args[1:], shell=False, log=log, 
                output_wrangler=cab.apply_output_wranglers, 
                return_errcode=True, command_name=command_name)

    # if retcode is not 0, and cab didn't declare itself a success,
    if retcode:
        if not cab.runtime_status:
            raise StimelaCabRuntimeError(f"{command_name} returned non-zero exit status {retcode}", log=log)
    else:
        if cab.runtime_status is False:
            raise StimelaCabRuntimeError(f"{command_name} was marked as failed based on its output", log=log)

    return retcode
