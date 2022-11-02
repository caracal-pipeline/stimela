import shlex, os.path, logging, datetime

from typing import Dict, Optional, Any
from collections import OrderedDict

from stimela.kitchen.cab import Cab
from stimela.utils.xrun_asyncio import xrun, dispatch_to_log
from stimela.exceptions import StimelaCabRuntimeError, CabValidationError
from stimela.schedulers.slurm import SlurmBatch
from stimela.schedulers import SlurmBatch

from io import TextIOBase


class LoggerIO(TextIOBase):
    """This is a stream class which captures text stream output into a logger, applying an optional output wrangler"""
    def __init__(self, log, command_name, stream_name, output_wrangler=None):
        self.log = log
        self.command_name = command_name
        self.stream_name = stream_name
        self.output_wrangler = output_wrangler

    def write(self, s):
        if s != "\n":
            for line in s.rstrip().split("\n"):
                dispatch_to_log(self.log, line, self.command_name, self.stream_name,
                                output_wrangler=self.output_wrangler)
        return len(s)


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

    if cab.flavour == "python":
        return run_callable(cab.py_module, cab.py_function, cab, params, log, subst)
    elif cab.flavour == "python-code":
        return _run_external_python(code, "python", cab, params, log, subst)
    elif cab.flavour == "binary":
        return run_command(cab, params, log, subst)
    else:
        raise StimelaCabRuntimeError(f"{cab.flavour} flavour cabs not yet supported by native backend")


def run_callable(modulename: str, funcname: str,  cab: Cab, params: Dict[str, Any], log: logging.Logger, subst: Optional[Dict[str, Any]] = None):
    """Runs a cab corresponding to an external Python callable. Intercepts stdout/stderr into the logger.

    Args:
        modulename (str): module name to import
        funcname (str): name of function in module to call
        cab (Cab): cab object
        log (logger): logger to use
        subst (Optional[Dict[str, Any]]): Substitution dict for commands etc., if any.

    Raises:
        StimelaCabRuntimeError: if any errors arise resolving the module or calling the function

    Returns:
        Any: return value (e.g. exit code) of content
    """
    # form up arguments
    arguments = []
    for key, schema in cab.inputs_outputs.items():
        if not schema.policies.skip:
            if key in params:
                arguments.append(f"{key}={repr(params[key])}")
            elif cab.get_schema_policy(schema, 'pass_missing_as_none'):
                arguments.append(f"{key}=None")

    # form up command string
    command = f"""
import sys
sys.path.append('.')
from {modulename} import {funcname}
try:
    from click import Command
except ImportError:
    Command = None
if Command is not None and isinstance({funcname}, Command):
    print("invoking callable {modulename}.{funcname}() (as click command) using external interpreter")
    {funcname} = {funcname}.callback
else:
    print("invoking callable {modulename}.{funcname}() using external interpreter")

retval = {funcname}({','.join(arguments)})
print("Return value is", repr(retval))
    """
    return _run_external_python(command, funcname, cab, params, log, subst)

def _run_external_python(command: str, funcname: str, cab: Cab, params: Dict[str, Any], log: logging.Logger, subst: Optional[Dict[str, Any]] = None):
    log.debug(f"python command is: {command}")

    # get virtual env, if specified
    from scabha.substitutions import substitutions_from
    with substitutions_from(subst, raise_errors=True) as context:
        venv = context.evaluate(cab.virtual_env, location=["virtual_env"])

    if venv:
        venv = os.path.expanduser(venv)
        interpreter = f"{venv}/bin/python"
        if not os.path.isfile(interpreter):
            raise CabValidationError(f"virtual environment {venv} doesn't exist")
        log.debug(f"virtual environment is {venv}")
    else:
        interpreter = "python"

    args = [interpreter, "-c", command]

    return _run_external(args, funcname, cab, params, subst, log, log_command=False)



def run_command(cab: Cab, params: Dict[str, Any], log: logging.Logger, subst: Optional[Dict[str, Any]] = None, batch=None):
    """Runs command represented by cab.

    Args:
        cab (Cab): cab object
        log (logger): logger to use
        subst (Optional[Dict[str, Any]]): Substitution dict for commands etc., if any.

    Raises:
        StimelaCabRuntimeError: if any errors arise during the command

    Returns:
        int: return value (e.g. exit code) of command
    """
    # build command line from parameters
    args, venv = cab.build_command_line(params, subst)
    command_name = args[0]

    # prepend virtualennv invocation, if asked
    if venv:
        args = ["/bin/bash", "--rcfile", f"{venv}/bin/activate", "-c", " ".join(shlex.quote(arg) for arg in args)]

    log.debug(f"command line is {args}")

    return _run_external(args, command_name, cab, params, subst, log, batch, log_command=True)



def _run_external(args, command_name, cab, params, subst, log, batch=None, log_command=True):

    if batch:
        batch = SlurmBatch(**batch)
        batch.__init_cab__(cab, params, subst, log)
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

    cabstat = cab.reset_status()

    retcode = xrun(args[0], args[1:], shell=False, log=log,
                output_wrangler=cabstat.apply_wranglers,
                return_errcode=True, command_name=command_name, log_command=log_command)

    # check if output marked it as a fail
    if cabstat.success is False:
        log.error(f"{command_name} was marked as failed based on its output")

    # if retcode != 0 and not explicitly marked as success, mark as failed
    if retcode and cabstat.success is not True:
        cabstat.declare_failure(f"{command_name} returns error code {retcode} after {elapsed()}")
    else:
        log.info(f"{command_name} returns exit code {retcode} after {elapsed()}")

    return cabstat
