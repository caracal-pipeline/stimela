import logging, datetime, resource, os.path

from typing import Dict, Optional, Any

import stimela
import stimela.kitchen
from stimela.utils.xrun_asyncio import xrun
from stimela.exceptions import StimelaProcessRuntimeError, BackendSpecificationError
from scabha.substitutions import substitutions_from


def update_rlimits(rlimits: Dict[str, Any], log: logging.Logger):
    for name, limit in rlimits.items():
        rname = f"RLIMIT_{name}"
        if not hasattr(resource, rname):
            raise StimelaProcessRuntimeError(f"unknown resource limit 'backend.rlimits.{name}'")
        rconst = getattr(resource, rname)
        # get current limits
        soft, hard = resource.getrlimit(rconst)
        # check for unlimited
        if limit is None:
            limit = resource.RLIM_INFINITY
            if hard != resource.RLIM_INFINITY:
                raise StimelaProcessRuntimeError(f"can't set backend.rlimits.{name}=unlimited: hard limit is {hard}")
        else:
            if limit > hard:
                raise StimelaProcessRuntimeError(f"can't set backend.rlimits.{name}={limit}: hard limit is {hard}")
        resource.setrlimit(rconst, (limit, hard))
        log.debug(f"setting soft limit {name}={limit} (hard limit is {hard})")



def build_command_line(cab: 'stimela.kitchen.cab.Cab', params: Dict[str, Any], subst: Optional[Dict[str, Any]] = None,
                        virtual_env: Optional[str] = None, log: Optional[logging.Logger] = None):
    return cab.flavour.get_arguments(cab, params, subst, virtual_env=virtual_env, log=log)


def run(cab: 'stimela.kitchen.cab.Cab', params: Dict[str, Any], fqname: str,
        backend: 'stimela.backend.StimelaBackendOptions',
        log: logging.Logger, subst: Optional[Dict[str, Any]] = None,
        wrapper: Optional['stimela.backends.runner.BackendWrapper'] = None):
    """
    Runs cab contents

    Args:
        cab: cab object
        params: cab parameters
        backend: backed settings object
        log (logger): logger to use
        subst (Optional[Dict[str, Any]]): Substitution dict for commands etc., if any.
        wrapper (BackendWrapper): wrapper for command line
    Returns:
        Any: return value (e.g. exit code) of content
    """
    update_rlimits(backend.rlimits, log)

    venv = search = None
    if backend.native and backend.native.virtual_env:
        try:
            with substitutions_from(subst, raise_errors=True) as context:
                venv = context.evaluate(backend.native.virtual_env, 
                                        location=["backend.native.virtual_env"])
        except Exception as exc:
            raise BackendSpecificationError(f"error evaluating backend.native.virtual_env", exc)
        if venv:
            venv = os.path.expanduser(venv)
            if not os.path.isfile(f"{venv}/bin/activate"):
                raise BackendSpecificationError(f"virtual environment {venv} doesn't exist")
            log.debug(f"virtual environment is {venv}")

    args, log_args = build_command_line(cab, params, subst, virtual_env=venv, log=log)

    cabstat = cab.reset_status()

    command_name = cab.flavour.command_name

    # run command
    start_time = datetime.datetime.now()
    def elapsed(since=None):
        """Returns string representing elapsed time"""
        return str(datetime.datetime.now() - (since or start_time)).split('.', 1)[0]

    # log.info(f"argument lengths are {[len(a) for a in args]}")
    
    if wrapper:
        args, log_args = wrapper.wrap_run_command(args, log_args, fqname=fqname, log=log)
        
    log.debug(f"command line is {' '.join(log_args)}")

    retcode = xrun(args[0], args[1:], shell=False, log=log,
                output_wrangler=cabstat.apply_wranglers,
                return_errcode=True, command_name=command_name, 
                gentle_ctrl_c=True,
                log_command=' '.join(log_args), 
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
