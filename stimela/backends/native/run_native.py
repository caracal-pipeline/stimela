import logging, datetime, resource

from typing import Dict, Optional, Any

import stimela
import stimela.kitchen
from stimela.utils.xrun_asyncio import xrun
from stimela.exceptions import StimelaProcessRuntimeError


def update_rlimits(rlimits: Dict[str, Any], log: logging.Logger):
    for name, limit in rlimits.items():
        rname = f"RLIMIT_{name}"
        if not hasattr(resource, rname):
            raise StimelaProcessRuntimeError(f"unknown resource limit 'opts.rlimits.{name}'")
        rconst = getattr(resource, rname)
        # get current limits
        soft, hard = resource.getrlimit(rconst)
        # check for unlimited
        if limit is None:
            limit = resource.RLIM_INFINITY
            if hard != resource.RLIM_INFINITY:
                raise StimelaProcessRuntimeError(f"can't set opts.rlimits.{name}=unlimited: hard limit is {hard}")
        else:
            if limit > hard:
                raise StimelaProcessRuntimeError(f"can't set opts.rlimits.{name}={limit}: hard limit is {hard}")
        resource.setrlimit(rconst, (limit, hard))
        log.debug(f"setting soft limit {name}={limit} (hard limit is {hard})")



def build_command_line(cab: 'stimela.kitchen.cab.Cab', params: Dict[str, Any], subst: Optional[Dict[str, Any]] = None):
    return cab.flavour.get_arguments(cab, params, subst)


def run(cab: 'stimela.kitchen.cab.Cab', params: Dict[str, Any], fqname: str,
        log: logging.Logger, subst: Optional[Dict[str, Any]] = None):
    """Runs cab contents

    Args:
        cab (Cab): cab object
        log (logger): logger to use
        subst (Optional[Dict[str, Any]]): Substitution dict for commands etc., if any.

    Returns:
        Any: return value (e.g. exit code) of content
    """
    update_rlimits(stimela.CONFIG.opts.rlimits, log)

    args = build_command_line(cab, params, subst)

    log.debug(f"command line is {args}")

    cabstat = cab.reset_status()

    command_name = cab.flavour.command_name

    # run command
    start_time = datetime.datetime.now()
    def elapsed(since=None):
        """Returns string representing elapsed time"""
        return str(datetime.datetime.now() - (since or start_time)).split('.', 1)[0]

    # log.info(f"argument lengths are {[len(a) for a in args]}")

    retcode = xrun(args[0], args[1:], shell=False, log=log,
                output_wrangler=cabstat.apply_wranglers,
                return_errcode=True, command_name=command_name, 
                gentle_ctrl_c=True,
                log_command=True if cab.flavour.log_full_command else command_name, 
                log_result=False)

    # check if output marked it as a fail
    if cabstat.success is False:
        log.error(f"declaring '{command_name}' as failed based on its output")

    # if retcode != 0 and not explicitly marked as success, mark as failed
    if retcode and cabstat.success is not True:
        cabstat.declare_failure(f"{command_name} returns error code {retcode} after {elapsed()}")
    else:
        log.info(f"{command_name} returns exit code {retcode} after {elapsed()}")

    return cabstat
