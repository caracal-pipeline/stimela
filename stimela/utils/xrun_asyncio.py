import traceback, subprocess, errno, re, time, logging, os, sys, signal
import asyncio
import psutil
from .xrun_poll import get_stimela_logger, dispatch_to_log, xrun_nolog
from . import StimelaCabRuntimeError, StimelaProcessRuntimeError

DEBUG = 0

log = None



def xrun(command, options, log=None, env=None, timeout=-1, kill_callback=None, output_wrangler=None, shell=True, return_errcode=False, command_name=None):
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

    loop = asyncio.get_event_loop()
    
    proc = loop.run_until_complete(
            asyncio.create_subprocess_exec(*command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE))

    async def stream_reader(stream, stream_name):
        while not stream.at_eof():
            line = await stream.readline()
            line = (line.decode('utf-8') if type(line) is bytes else line).rstrip()
            if line or not stream.at_eof():
                dispatch_to_log(log, line, command_name, stream_name, output_wrangler=output_wrangler)

    async def cpu_reporter_impl(period):
        while True:
            await asyncio.sleep(period)
            usage = psutil.cpu_percent()
            log.info(f"Current CPU usage is {usage}%")

    async def wrap_cancellable(job):
        try:
            return await job 
        except asyncio.CancelledError as exc:
            return None

    async def proc_awaiter(proc, *cancellables):
        await proc.wait()
        for task in cancellables:
            task.cancel()

    reporter = asyncio.Task(cpu_reporter_impl(5))

    try:
        job = asyncio.gather(
            proc_awaiter(proc, reporter),
            stream_reader(proc.stdout, "stdout"),
            stream_reader(proc.stderr, "stderr"),
            wrap_cancellable(reporter)
        )
        results = loop.run_until_complete(job)
        status = proc.returncode
    except SystemExit as exc:
        loop.run_until_complete(proc.wait())
    except KeyboardInterrupt:
        if callable(kill_callback):
            log.warning(f"Ctrl+C caught: shutting down {command_name} process, please give it a few moments")
            kill_callback() 
            log.info(f"the {command_name} process was shut down successfully",
                    extra=dict(stimela_subprocess_output=(command_name, "status")))
            loop.run_until_complete(proc.wait())
        else:
            log.warning(f"Ctrl+C caught, interrupting {command_name} process {proc.pid}")
            proc.send_signal(signal.SIGINT)

            async def wait_on_process(proc):
                for retry in range(10):
                    await asyncio.sleep(1)
                    if proc.returncode is not None:
                        log.info(f"Process {proc.pid} has exited with return code {proc.returncode}")
                        break
                    if retry == 5:
                        log.warning(f"Process {proc.pid} not exited after {retry} seconds, will tyr to terminate it")
                        proc.terminate()
                    else:
                        log.info(f"Process {proc.pid} not exited after {retry} seconds, waiting a bit longer...")
                else:
                    log.warning(f"Killing process {proc.pid}")
                    proc.kill()
            
            loop.run_until_complete(wait_on_process(proc))

        raise StimelaCabRuntimeError(f"{command_name} interrupted with Ctrl+C")

    except Exception as exc:
        traceback.print_exc()
        loop.run_until_complete(proc.wait())
        raise StimelaCabRuntimeError(f"{command_name} threw exception: {exc}'", log=log)

    if status and not return_errcode:
        raise StimelaCabRuntimeError(f"{command_name} returns error code {status}")
    
    return status
    
