import select 
import traceback
import subprocess
import errno
import re
import time
import logging
import sys
import signal

DEBUG = 0
from stimela.exceptions import StimelaCabRuntimeError, StimelaProcessRuntimeError

log = None

from .xrun_asyncio import get_stimela_logger, dispatch_to_log

def global_logger():
    """Returns Stimela logger if running in stimela, else inits a global logger"""
    global log
    if log is None:
        log = get_stimela_logger()
        if log is None:
            # no stimela => running payload inside a cab -- just use the global logger and make it echo everything to the console
            logging.basicConfig(format="%(message)s", level=logging.INFO, stream=sys.stdout)
            log = logging.getLogger()
    return log

def xrun_nolog(command, name=None, shell=True):
    log = global_logger()
    name = name or command.split(" ", 1)[0]
    try:
        log.info("# running {}".format(command))
        status = subprocess.call(command, shell=shell)

    except KeyboardInterrupt:
        log.error("# {} interrupted by Ctrl+C".format(name))
        raise

    except Exception as exc:
        for line in traceback.format_exc():
            log.error("# {}".format(line.strip()))
        log.error("# {} raised exception: {}".format(name, str(exc)))
        raise

    if status:
        raise StimelaProcessRuntimeError("{} returns error code {}".format(name, status))

    return 0


class SelectPoller(object):
    """Poller class. Poor man's select.poll(). Damn you OS/X and your select.poll will-you-won'y-you bollocks"""
    def __init__ (self, log):
        self.fdlabels = {}
        self.log = log

    def register_file(self, fobj, label):
        self.fdlabels[fobj.fileno()] = label, fobj

    def register_process(self, po, label_stdout='stdout', label_stderr='stderr'):
        self.fdlabels[po.stdout.fileno()] = label_stdout, po.stdout
        self.fdlabels[po.stderr.fileno()] = label_stderr, po.stderr

    def poll(self, timeout=5, verbose=False):
        while True:
            try:
                to_read, _, _ = select.select(self.fdlabels.keys(), [], [], timeout)
                self.log.debug("poll(): ready to read: {}".format(to_read))
                # return on success or timeout
                return [self.fdlabels[fd] for fd in to_read]
            except (select.error, IOError) as ioerr:
                if verbose:
                    self.log.debug("poll() exception: {}".format(traceback.format_exc()))
                if hasattr(ioerr, 'args'):
                    err = ioerr.args[0]  # py2
                else:
                    err = ioerr.errno    # py3
                # catch interrupted system call -- return if we have a timeout, else
                # loop again
                if err == errno.EINTR:
                    if timeout is not None:
                        if verbose:
                            self.log.debug("poll(): returning")
                        return []
                    if verbose:
                        self.log.debug("poll(): retrying")
                else:
                    raise ioerr

    def unregister_file(self, fobj):
        if fobj.fileno() in self.fdlabels:
            del self.fdlabels[fobj.fileno()]

    def __contains__(self, fobj):
        return fobj.fileno() in self.fdlabels

class Poller(object):
    """Poller class. Wraps select.poll()."""
    def __init__ (self, log):
        self.fdlabels = {}
        self.log = log
        self._poll = select.poll()

    def register_file(self, fobj, label):
        self.fdlabels[fobj.fileno()] = label, fobj
        self._poll.register(fobj.fileno(), select.POLLIN)

    def register_process(self, po, label_stdout='stdout', label_stderr='stderr'):
        self.fdlabels[po.stdout.fileno()] = label_stdout, po.stdout
        self.fdlabels[po.stderr.fileno()] = label_stderr, po.stderr
        self._poll.register(po.stdout.fileno(), select.POLLIN)
        self._poll.register(po.stderr.fileno(), select.POLLIN)

    def poll(self, timeout=5, verbose=False):
        try:
            to_read = self._poll.poll(timeout*1000)
            if verbose:
                self.log.debug("poll(): ready to read: {}".format(to_read))
            return [self.fdlabels[fd] for (fd, ev) in to_read]
        except Exception:
            if verbose:
                self.log.debug("poll() exception: {}".format(traceback.format_exc()))
            raise

    def unregister_file(self, fobj):
        if fobj.fileno() in self.fdlabels:
            self._poll.unregister(fobj.fileno())
            del self.fdlabels[fobj.fileno()]

    def __contains__(self, fobj):
        return fobj.fileno() in self.fdlabels



def xrun(command, options, log=None, env=None, timeout=-1, kill_callback=None, output_wrangler=None, shell=True, return_errcode=False, command_name=None, log_command=True):
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

    if log_command:
        log.info("running " + command_line, extra=dict(stimela_subprocess_output=(command_name, "start")))

    start_time = time.time()

    proc = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            env=env, bufsize=1, universal_newlines=True, shell=shell)

    poller = Poller(log=log)
    poller.register_process(proc)

    proc_running = True

    try:
        while proc_running and poller.fdlabels:
            fdlist = poller.poll(verbose=DEBUG>0)
#            print(f"fdlist is {fdlist}")
            for fname, fobj in fdlist:
                try:
                    line = fobj.readline()
                except EOFError:
                    line = b''
#                print("read {} from {}".format(line, fname))
                empty_line = not line
                line = (line.decode('utf-8') if type(line) is bytes else line).rstrip()
                # break out if process closes
                if empty_line:
                    poller.unregister_file(fobj)
                    if proc.stdout not in poller and proc.stderr not in poller:
                        log.debug(f"the {command_name} process has exited")
                        proc_running = None
                        break
                    continue
                # dispatch output to log
                dispatch_to_log(log, line, command_name, 
                                stream_name="stderr" if fobj is proc.stderr else "stdout", 
                                output_wrangler=output_wrangler)
            if timeout > 0 and time.time() > start_time + timeout:
                log.error(f"timeout, killing {command_name} process")
                kill_callback() if callable(kill_callback) else proc.kill()
                proc_running = False

        proc.wait()
        status = proc.returncode

    except SystemExit as exc:
        proc.wait()
        status = exc.code
        raise StimelaCabRuntimeError(f"{command_name}: SystemExit with code {status}", log=log)

    except KeyboardInterrupt:
        if callable(kill_callback):
            log.warning(f"Ctrl+C caught: shutting down {command_name} process, please give it a few moments")
            kill_callback() 
            log.info(f"the {command_name} process was shut down successfully",
                     extra=dict(stimela_subprocess_output=(command_name, "status")))
            proc.wait()
        else:
            log.warning(f"Ctrl+C caught, interrupting {command_name} process {proc.pid}")
            proc.send_signal(signal.SIGINT)
            for retry in range(10):
                if retry:
                    log.info(f"Process {proc.pid} not exited after {retry} seconds, waiting a bit longer...")
                try:
                    proc.wait(1)
                    log.info(f"Process {proc.pid} has exited with return code {proc.returncode}")
                    break
                except subprocess.TimeoutExpired as exc:
                    if retry == 4:
                        log.warning(f"Terminating process {proc.pid}")
                        proc.terminate()
            else:
                log.warning(f"Killing process {proc.pid}")
                proc.kill()
                proc.wait()
    
        raise StimelaCabRuntimeError(f"{command_name} interrupted with Ctrl+C")

    except Exception as exc:
        traceback.print_exc()
        proc.wait()
        raise StimelaCabRuntimeError(f"{command_name} threw exception: {exc}'", log=log)

    if status and not return_errcode:
        raise StimelaCabRuntimeError(f"{command_name} returns error code {status}")
    
    return status
    
