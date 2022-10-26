import shlex, os.path
import importlib, traceback, sys, logging, uuid

from typing import Dict, Optional, Any
from collections import OrderedDict
from contextlib import redirect_stderr, redirect_stdout

from stimela.kitchen.cab import Cab
from stimela import logger
from stimela.utils.xrun_asyncio import xrun, dispatch_to_log
from stimela.exceptions import StimelaCabRuntimeError, CabValidationError
from stimela.schedulers.slurm import SlurmBatch
import click
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
    if cab.flavour == "python-ext":
        return run_external_callable(cab.py_module, cab.py_function, cab, params, log, subst)
    elif cab.flavour == "binary":
        return run_command(cab, params, log, subst)
    else:
        raise StimelaCabRuntimeError(f"{cab.flavour} flavour cabs not yet supported by native backend")


def run_callable(modulename: str, funcname: str,  cab: Cab, params: Dict[str, Any], log, subst: Optional[Dict[str, Any]] = None):
    """Runs a cab corresponding to a Python callable. Intercepts stdout/stderr into the logger.

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

    # import module and get function object
    path0 = sys.path.copy()
    sys.path.append('.')
    try:
        mod = importlib.import_module(modulename)
    except ImportError as exc:
        raise StimelaCabRuntimeError(f"can't import {modulename}: {exc}", log=log)
    finally:
        sys.path = path0

    func = getattr(mod, funcname, None)

    if not callable(func):
        raise StimelaCabRuntimeError(f"{modulename}.{funcname} is not a valid callable", log=log)

    # for functions wrapped in a @click.command decorator, get the underlying function itself
    if isinstance(func, click.Command):
        log.info(f"invoking callable {modulename}.{funcname}() (as click command)")
        func = func.callback
    else:
        log.info(f"invoking callable {modulename}.{funcname}()")

    args = OrderedDict()
    for key, schema in cab.inputs_outputs.items():
        if not schema.policies.skip:
            if key in params:
                args[key] = params[key]
            elif cab.get_schema_policy(schema, 'pass_missing_as_none'):
                args[key] = None

    # redirect and call
    cab.reset_runtime_status()
    try:
        with redirect_stdout(LoggerIO(log, funcname, "stdout", output_wrangler=cab.apply_output_wranglers)), \
                redirect_stderr(LoggerIO(log, funcname, "stderr", output_wrangler=cab.apply_output_wranglers)):
            retval = func(**args)
    except Exception as exc:
        for line in traceback.format_exception(*sys.exc_info()):
            log.error(line.rstrip())
        raise StimelaCabRuntimeError(f"{modulename}.{funcname}() threw exception: {exc}'", log=log)

    log.info(f"{modulename}.{funcname}() returns {retval}")

    # check if command was marked as failed by the output wrangler
    if cab.runtime_status is False:
        raise StimelaCabRuntimeError(f"{modulename}.{funcname} was marked as failed based on its output", log=log)

    return retval


def run_external_callable(modulename: str, funcname: str,  cab: Cab, params: Dict[str, Any], log: logging.Logger, subst: Optional[Dict[str, Any]] = None):
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

    cab.reset_runtime_status()

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

    retcode = xrun(args[0], args[1:], shell=False, log=log,
                output_wrangler=cab.apply_output_wranglers,
                return_errcode=True, command_name=command_name, log_command=log_command)

    # if retcode is not zero, raise error, unless cab declared itself a success (via the wrangler)
    if retcode:
        if not cab.runtime_status:
            raise StimelaCabRuntimeError(f"{command_name} returned non-zero exit status {retcode}", log=log)
    # if retcode is zero, check that cab didn't declare itself a failure
    else:
        if cab.runtime_status is False:
            raise StimelaCabRuntimeError(f"{command_name} was marked as failed based on its output", log=log)

    return retcode