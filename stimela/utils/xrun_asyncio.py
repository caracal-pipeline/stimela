import traceback
import os
import signal
import datetime
import asyncio
import logging
import re
from rich.markup import escape

from stimela import stimelogging, task_stats

from stimela.exceptions import StimelaCabRuntimeError, StimelaProcessRuntimeError

DEBUG = 0

log = None

def get_stimela_logger():
    """Returns Stimela's logger, or None if no Stimela installed"""
    try:
        import stimela
        return stimela.logger()
    except ImportError:
        return None


def dispatch_to_log(log, line, command_name, stream_name, output_wrangler, style=None, prefix=None):
    # dispatch output to log
    extra = dict()
    # severity = logging.WARNING if fobj is proc.stderr else logging.INFO
    severity = logging.INFO
    if style is not None:
        extra['style'] = style
    if prefix is not None:
        extra['prefix'] = prefix
    extra.setdefault('style', 'dim' if stream_name == 'stdout' else 'white')
    extra.setdefault('prefix', task_stats.get_subprocess_id() + "#")
    # feed through wrangler to adjust severity and content
    if output_wrangler is not None:
        line, severity = output_wrangler(escape(line), severity)
    else:
        line = escape(line)
    # escape emojis. Check that it's a str -- wranglers can return FunkyMessages instead of strings, in which case the 
    # escaping is aleady done for us
    if type(line) is str:
        line = re.sub(r":(\w+):", r":[bold][/bold]\1:", line)
    # dispatch to log
    if line is not None:
        if severity >= logging.ERROR:
            extra['prefix'] = stimelogging.FunkyMessage("[red]:error:[/red]", "!")
        if isinstance(line, stimelogging.FunkyMessage) and line.prefix:
            extra['prefix'] = line.prefix
        log.log(severity, line, extra=extra)


def xrun(command, options, log=None, env=None, timeout=-1, kill_callback=None, output_wrangler=None, shell=True, 
            return_errcode=False, command_name=None, progress_bar=False, 
            gentle_ctrl_c=False,
            log_command=True, log_result=True):
    
    command_name = command_name or command

    # this part could be inside the container
    command_line = " ".join([command] + list(map(str, options)))
    if shell:
        command_line = " ".join([command] + list(map(str, options)))
        command = [command_line]
    else:
        command = [command] + list(map(str, options))
        command_line = " ".join(command)

    log = log or get_stimela_logger()

    if log is None:
        from .xrun_poll import xrun_nolog
        return xrun_nolog(command, name=command_name, shell=shell)

    # this part is never inside the container
    import stimela

    log = log or stimela.logger()

    if log_command:
        # NOTE(JSKenyon): These must be logged with soft wrapping to make
        # copy pasting the output simple.
        extras = dict(custom_console_print=True, style="dim", soft_wrap=True)
        if log_command is True:
            log.info("---INVOKING---", extra=dict(**extras, justify="center"))
            log.info(f"{command_line}\n", extra=extras)
        else:
            log.info("---INVOKING---", extra=dict(**extras, justify="center"))
            log.info(f"{log_command}\n", extra=extras)
            log.debug(f"full command line is {command_line}", extra=extras)

    with task_stats.declare_subcommand(os.path.basename(command_name)) as command_context:

        start_time = datetime.datetime.now()
        def elapsed():
            """Returns string representing elapsed time"""
            return str(datetime.datetime.now() - start_time).split('.', 1)[0]
        
        loop = asyncio.get_event_loop()
        
        proc = loop.run_until_complete(
                asyncio.create_subprocess_exec(*command,
                    limit=1024**3,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE))

        async def stream_reader(stream, stream_name):
            while not stream.at_eof():
                line = await stream.readline()
                line = (line.decode('utf-8') if type(line) is bytes else line).rstrip()
                if line or not stream.at_eof():
                    dispatch_to_log(log, line, command_name, stream_name, output_wrangler=output_wrangler)

        async def proc_awaiter(proc, *cancellables):
            await proc.wait()
            for task in cancellables:
                task.cancel()

        reporter = asyncio.Task(task_stats.run_process_status_update())
        ctrl_c_caught = job_interrupted = False
        try:
            job = asyncio.gather(
                proc_awaiter(proc, reporter),
                stream_reader(proc.stdout, "stdout"),
                stream_reader(proc.stderr, "stderr"),
                reporter
            )
            results = loop.run_until_complete(job)
            status = proc.returncode
            if log_result:
                log.info(f"{command_name} exited with code {status} after {elapsed()}")
        except SystemExit as exc:
            loop.run_until_complete(proc.wait())
        except KeyboardInterrupt:
            if callable(kill_callback):
                command_context.ctrl_c()
                log.warning(f"Ctrl+C caught after {elapsed()}, shutting down {command_name} process, please give it a few moments")
                kill_callback() 
                log.info(f"the {command_name} process was shut down successfully",
                        extra=dict(stimela_subprocess_output=(command_name, "status")))
                loop.run_until_complete(proc.wait())
            else:
                try:
                    raise KeyboardInterrupt
                    # below doesn't work -- figure out later
                    if not gentle_ctrl_c or ctrl_c_caught:
                        raise KeyboardInterrupt
                    command_context.ctrl_c()
                    ctrl_c_caught = True
                    log.warning(f"Ctrl+C caught after {elapsed()}, job will error out when {command_name} process {proc.pid} completes")
                    log.warning(f"Use Ctrl+C again to interrupt the job")
                    results = loop.run_until_complete(job)
                except KeyboardInterrupt:
                    log.warning(f"Ctrl+C caught after {elapsed()}, interrupting {command_name} process {proc.pid}")
                    job_interrupted = True
                    proc.send_signal(signal.SIGINT)

                    async def wait_on_process(proc):
                        for retry in range(10):
                            await asyncio.sleep(1)
                            if proc.returncode is not None:
                                log.info(f"Process {proc.pid} has exited with return code {proc.returncode}")
                                break
                            if retry == 5:
                                log.warning(f"Process {proc.pid} not exited after {retry} seconds, will try to terminate it")
                                proc.terminate()
                            else:
                                log.info(f"Process {proc.pid} not exited after {retry} seconds, waiting a bit longer...")
                        else:
                            log.warning(f"Killing process {proc.pid}")
                            proc.kill()
                    
                    loop.run_until_complete(wait_on_process(proc))
            if job_interrupted:
                raise StimelaCabRuntimeError(f"{command_name} interrupted with Ctrl+C")
            else:
                raise StimelaCabRuntimeError(f"{command_name} complete, but received a Ctrl+C during the run")

        except Exception as exc:
            loop.run_until_complete(proc.wait())
            traceback.print_exc()
            raise StimelaCabRuntimeError(f"{command_name} threw exception: {exc} after {elapsed()}'", log=log)

        if status and not return_errcode:
            raise StimelaCabRuntimeError(f"{command_name} returns error code {status} after {elapsed()}")
    
    return status
    
