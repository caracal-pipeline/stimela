import logging, datetime

from typing import Dict, Optional, Any

from stimela.kitchen.cab import Cab
from stimela.utils.xrun_asyncio import xrun
from stimela.schedulers.slurm import SlurmBatch
from stimela.schedulers import SlurmBatch


def run(cab: Cab, params: Dict[str, Any], runtime: Dict[str, Any], fqname: str,
        log: logging.Logger, subst: Optional[Dict[str, Any]] = None, batch=None):
    """Runs cab contents

    Args:
        cab (Cab): cab object
        log (logger): logger to use
        subst (Optional[Dict[str, Any]]): Substitution dict for commands etc., if any.

    Returns:
        Any: return value (e.g. exit code) of content
    """

    args = cab.flavour.get_arguments(cab, params, subst)

    log.debug(f"command line is {args}")

    cabstat = cab.reset_status()

    command_name = cab.flavour.command_name

    if batch:
        batch = SlurmBatch(**batch)
        batch.__init_cab__(cabstat.cab, params, subst, log)
        runcmd = "/bin/bash -c" + " ".join(args)
        jobfile = "foo-bar.job"
        batch.name = "foo-bar"
        batch.submit(jobfile=jobfile, runcmd=runcmd)

        return

    #-------------------------------------------------------
    # run command
    start_time = datetime.datetime.now()
    def elapsed(since=None):
        """Returns string representing elapsed time"""
        return str(datetime.datetime.now() - (since or start_time)).split('.', 1)[0]

    retcode = xrun(args[0], args[1:], shell=False, log=log,
                output_wrangler=cabstat.apply_wranglers,
                return_errcode=True, command_name=command_name, 
                log_command=True, log_result=False)

    # check if output marked it as a fail
    if cabstat.success is False:
        log.error(f"{command_name} was marked as failed based on its output")

    # if retcode != 0 and not explicitly marked as success, mark as failed
    if retcode and cabstat.success is not True:
        cabstat.declare_failure(f"{command_name} returns error code {retcode} after {elapsed()}")
    else:
        log.info(f"{command_name} returns exit code {retcode} after {elapsed()}")

    return cabstat
