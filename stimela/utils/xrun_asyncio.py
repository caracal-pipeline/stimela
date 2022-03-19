import traceback, subprocess, errno, re, time, logging, os, sys, signal
import asyncio
from .xrun_poll import get_stimela_logger, dispatch_to_log, xrun_nolog
from . import StimelaCabRuntimeError, StimelaProcessRuntimeError

DEBUG = 0

log = None


async def stream_reader(stream, line_handler, exit_handler):
    while not stream.at_eof():
        line = await stream.readline()
        line = (line.decode('utf-8') if type(line) is bytes else line).rstrip()
        line_handler(line)
    # Finished
    exit_handler()


async def xrun_impl(command, options, log=None, env=None, timeout=-1, kill_callback=None, output_wrangler=None, shell=True, return_errcode=False, command_name=None):
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
        return xrun_nolog(command, name=command_name, shell=shell)

    # this part is never inside the container
    import stimela

    log = log or stimela.logger()

    log.info("running " + command_line, extra=dict(stimela_subprocess_output=(command_name, "start")))

    start_time = time.time()

    proc = await asyncio.create_subprocess_exec(*command,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE)

    def line_dispatcher(line, stream_name):
        dispatch_to_log(log, line, command_name, stream_name="stderr", output_wrangler=output_wrangler)

    results = await asyncio.gather(
        asyncio.create_task(proc.wait()),
        asyncio.create_task(stream_reader(proc.stdout, lambda line:line_dispatcher(line, "stdout"))),
        asyncio.create_task(stream_reader(proc.stderr, lambda line:line_dispatcher(line, "stderr"))),
        return_exceptions=True
    )
    status = proc.returncode

    for result in results:
        if isinstance(result, SystemExit):
            raise StimelaCabRuntimeError(f"{command_name}: SystemExit with code {status}", log=log)
        elif isinstance(result, KeyboardInterrupt):
            if callable(kill_callback):
                log.warning(f"Ctrl+C caught: shutting down {command_name} process, please give it a few moments")
                kill_callback() 
                log.info(f"the {command_name} process was shut down successfully",
                        extra=dict(stimela_subprocess_output=(command_name, "status")))
                await proc.wait()
            else:
                log.warning(f"Ctrl+C caught, interrupting {command_name} process {proc.pid}")
                proc.send_signal(signal.SIGINT)
                for retry in range(10):
                    if retry:
                        log.info(f"Process {proc.pid} not exited after {retry} seconds, waiting a bit longer...")
                    try:
                        await proc.wait(1)
                        log.info(f"Process {proc.pid} has exited with return code {proc.returncode}")
                        break
                    except subprocess.TimeoutExpired as exc:
                        if retry == 4:
                            log.warning(f"Terminating process {proc.pid}")
                            proc.terminate()
                else:
                    log.warning(f"Killing process {proc.pid}")
                    proc.kill()
                    await proc.wait()
            raise StimelaCabRuntimeError(f"{command_name} interrupted with Ctrl+C")

        elif isinstance(result,  Exception):
            traceback.print_exc()
            await proc.wait()
            raise StimelaCabRuntimeError(f"{command_name} threw exception: {exc}'", log=log)

    if status and not return_errcode:
        raise StimelaCabRuntimeError(f"{command_name} returns error code {status}")
    
    return status
    
async def xrun(command, options, log=None, env=None, timeout=-1, kill_callback=None, output_wrangler=None, shell=True, return_errcode=False, command_name=None):
    return asyncio.run(xrun_impl(command, options, log=log, env=env, 
                    timeout=timeout, kill_callback=kill_callback, output_wrangler=output_wrangler, 
                    shell=shell, return_errcode=return_errcode, command_name=command_name))
