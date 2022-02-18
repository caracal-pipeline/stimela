import shlex, re
import importlib, traceback, sys

from typing import Dict, Optional, Any
from collections import OrderedDict
from contextlib import redirect_stderr, redirect_stdout

from scabha.cargo import Cab
from stimela import logger
from stimela.utils.xrun_poll import xrun, dispatch_to_log
from stimela.exceptions import StimelaCabRuntimeError
import click

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


def run(cab: Cab, log, subst: Optional[Dict[str, Any]] = None, batch=None):
    """Runs cab contents

    Args:
        cab (Cab): cab object
        log (logger): logger to use
        subst (Optional[Dict[str, Any]]): Substitution dict for commands etc., if any.

    Returns:
        Any: return value (e.g. exit code) of content
    """

    # commands of form "(module)function" are a Python call
    match = re.match("^\((.+)\)(.+)$", cab.command)
    if match:
        return run_callable(match.group(1), match.group(2), cab, log, subst)
    # everything else is a shell command
    else:
        return run_command(cab, log, subst)


def run_callable(modulename: str, funcname: str, cab: Cab, log, subst: Optional[Dict[str, Any]] = None):
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
    try:
        mod = importlib.import_module(modulename)
    except ImportError as exc:
        raise StimelaCabRuntimeError(f"can't import {modulename}: {exc}", log=log)

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
            if key in cab.params:
                args[key] = cab.params
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


def run_command(cab: Cab, log, subst: Optional[Dict[str, Any]] = None, batch=None):
    """Runns command represented by cab.

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
    args, venv = cab.build_command_line(subst)

    if batch:
        batch = SlurmBatch(**batch)
        batch.__init_cab__(cab, subst, log)
        runcmd = "/bin/bash -c" + " ".join(args)
        jobfile = "foo-bar.job"
        batch.name = "foo-bar"
        batch.submit(jobfile=jobfile, runcmd=runcmd)

        return
    #-------------------------------------------------------

    command_name = args[0]

    # prepend virtualennv invocation, if asked
    if venv:
        args = ["/bin/bash", "--rcfile", f"{venv}/bin/activate", "-c", " ".join(shlex.quote(arg) for arg in args)]

    log.debug(f"command line is {args}")
    
    # run command
    cab.reset_runtime_status()

    retcode = xrun(args[0], args[1:], shell=False, log=log, 
                output_wrangler=cab.apply_output_wranglers, 
                return_errcode=True, command_name=command_name)

    # if retcode is not zero, raise error, unless cab declared itself a success (via the wrangler)
    if retcode:
        if not cab.runtime_status:
            raise StimelaCabRuntimeError(f"{command_name} returned non-zero exit status {retcode}", log=log)
    # if retcode is zero, check that cab didn't declare itself a failure
    else:
        if cab.runtime_status is False:
            raise StimelaCabRuntimeError(f"{command_name} was marked as failed based on its output", log=log)

    return retcode
