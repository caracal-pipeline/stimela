import sys, os.path, re
import logging
from typing import Optional, Dict, Any, Union
from omegaconf import DictConfig
from scabha.substitutions import SubstitutionNS, forgiving_substitutions_from

class MultiplexingHandler(logging.Handler):
    """handler to send INFO and below to stdout, everything above to stderr"""
    def __init__(self, info_stream=sys.stdout, err_stream=sys.stderr):
        super(MultiplexingHandler, self).__init__()
        self.info_handler = logging.StreamHandler(info_stream)
        self.err_handler = logging.StreamHandler(err_stream)
        self.multiplex = True

    def emit(self, record):
        handler = self.err_handler if record.levelno > logging.INFO and self.multiplex else self.info_handler
        handler.emit(record)
        # ignore broken pipes, this often happens when cleaning up and exiting
        try:
            handler.flush()
        except BrokenPipeError:
            pass

    def flush(self):
        try:
            self.err_handler.flush()
            self.info_handler.flush()
        except BrokenPipeError:
            pass

    def close(self):
        self.err_handler.close()
        self.info_handler.close()

    def setFormatter(self, fmt):
        self.err_handler.setFormatter(fmt)
        self.info_handler.setFormatter(fmt)

class ConsoleColors():
    WARNING = '\033[93m' if sys.stdin.isatty() else ''
    ERROR   = '\033[91m' if sys.stdin.isatty() else ''
    BOLD    = '\033[1m'  if sys.stdin.isatty() else ''
    DIM     = '\033[2m'  if sys.stdin.isatty() else ''
    GREEN   = '\033[92m' if sys.stdin.isatty() else ''
    ENDC    = '\033[0m'  if sys.stdin.isatty() else ''

    BEGIN = "<COLORIZE>"
    END   = "</COLORIZE>"

    @staticmethod
    def colorize(msg, *styles):
        style = "".join(styles)
        return msg.replace(ConsoleColors.BEGIN, style).replace(ConsoleColors.END, ConsoleColors.ENDC if style else "")

class ColorizingFormatter(logging.Formatter):
    """This Formatter inserts color codes into the string according to severity"""
    def __init__(self, fmt=None, datefmt=None, style="%", default_color=None):
        super(ColorizingFormatter, self).__init__(fmt, datefmt, style)
        self._default_color = default_color or ""

    def format(self, record):
        style = ConsoleColors.BOLD if hasattr(record, 'boldface') else ""
        if hasattr(record, 'color'):
            style += getattr(ConsoleColors, record.color or "None", "")
        elif record.levelno >= logging.ERROR:
            style += ConsoleColors.ERROR
        elif record.levelno >= logging.WARNING:
            style += ConsoleColors.WARNING
        return ConsoleColors.colorize(super(ColorizingFormatter, self).format(record), style or self._default_color)


class SelectiveFormatter(logging.Formatter):
    """Selective formatter. if condition(record) is True, invokes other formatter"""
    def __init__(self, default_formatter, dispatch_list):
        self._dispatch_list = dispatch_list
        self._default_formatter = default_formatter

    def format(self, record):
        for condition, formatter in self._dispatch_list:
            if condition(record):
                return formatter.format(record)
        else:
            return self._default_formatter.format(record)


_logger = None
log_console_handler = log_formatter = log_boring_formatter = log_colourful_formatter = None


def is_logger_initialized():
    return _logger is not None


def logger(name="STIMELA", propagate=False, console=True, boring=False,
           fmt="{asctime} {name} {levelname}: {message}",
           col_fmt="{asctime} {name} %s{levelname}: {message}%s"%(ConsoleColors.BEGIN, ConsoleColors.END),
           sub_fmt="# {message}",
           col_sub_fmt="%s# {message}%s"%(ConsoleColors.BEGIN, ConsoleColors.END),
           datefmt="%Y-%m-%d %H:%M:%S", loglevel="INFO"):
    """Returns the global Stimela logger (initializing if not already done so, with the given values)"""
    global _logger
    if _logger is None:
        _logger = logging.getLogger(name)
        if type(loglevel) is str:
            loglevel = getattr(logging, loglevel)
        _logger.setLevel(loglevel)
        _logger.propagate = propagate

        global log_console_handler, log_formatter, log_boring_formatter, log_colourful_formatter

        # this function checks if the log record corresponds to stdout/stderr output from a cab
        def _is_from_subprocess(rec):
            return hasattr(rec, 'stimela_subprocess_output')

        log_boring_formatter = SelectiveFormatter(
                    logging.Formatter(fmt, datefmt, style="{"),
                    [(_is_from_subprocess, logging.Formatter(sub_fmt, datefmt, style="{"))])

        log_colourful_formatter = SelectiveFormatter(
                    ColorizingFormatter(col_fmt, datefmt, style="{"),
                    [(_is_from_subprocess, ColorizingFormatter(fmt=col_sub_fmt, datefmt=datefmt, style="{",
                                                               default_color=ConsoleColors.DIM))])

        log_formatter = log_boring_formatter if boring else log_colourful_formatter

        if console:
            if "SILENT_STDERR" in os.environ and os.environ["SILENT_STDERR"].upper()=="ON":
                log_console_handler = logging.StreamHandler(stream=sys.stdout)
            else:  
                log_console_handler = MultiplexingHandler()
            log_console_handler.setFormatter(log_formatter)
            log_console_handler.setLevel(loglevel)
            _logger.addHandler(log_console_handler)
            _logger_console_handlers[_logger.name] = log_console_handler

        import scabha
        scabha.set_logger(_logger)

    return _logger


_logger_file_handlers = {}
_logger_console_handlers = {}

def has_file_logger(log: logging.Logger):
    return log.name in _logger_file_handlers


def disable_file_logger(log: logging.Logger):
    current_logfile, fh = _logger_file_handlers.get(log.name, (None, None))
    if fh is not None:
        fh.close()
        log.removeHandler(fh)
        del _logger_file_handlers[log.name]


def setup_file_logger(log: logging.Logger, logfile: str, level: Optional[Union[int, str]] = logging.INFO, symlink: Optional[str] = None):
    """Sets up logging to file

    Args:
        log (logging.Logger): Logger object
        logfile (str): logfile. May contain dirname, which will be created as needed.
        level (Optional[Union[int, str]], optional): Logging level, defaults to logging.INFO.
        symlink (Optional[str], optional): if set, and logfile contains a dirname that is created, sets named symlink to point to it
            (This is useful for patterns such as logfile="logs-YYMMDD/logfile.txt", then logs -> logs-YYMMDD)

    Returns:
        [logging.Logger]: logger object
    """
    current_logfile, fh = _logger_file_handlers.get(log.name, (None, None))
    
    # does the logger need a new FileHandler created
    if current_logfile != logfile:
        log.debug(f"will switch to logfile {logfile} (previous was {current_logfile})")

        # remove old FH if so
        if fh is not None:
            fh.close()
            log.removeHandler(fh)

        # create new one
        logdir = os.path.dirname(logfile)
        if logdir and not os.path.exists(logdir):            
            os.makedirs(logdir)
            if symlink:
                symlink_path = os.path.join(os.path.dirname(logdir.rstrip("/")) or ".", symlink)
                # remove existing symlink
                if os.path.islink(symlink_path):
                    os.unlink(symlink_path)
                # Make symlink to logdir. If name exists and is not a symlink, we'll do nothing
                if not os.path.exists(symlink_path):
                    os.symlink(os.path.basename(logdir), symlink_path)

        
        fh = logging.FileHandler(logfile, 'w', delay=True)
        fh.setFormatter(log_boring_formatter)
        log.addHandler(fh)

        _logger_file_handlers[log.name] = logfile, fh

        # if logging to console, disable propagation from this sub-logger, and add a console handler
        # This ensures that parent loggers that log to files to not get repeated messages
        if log_console_handler:
            log.propagate = False
            if log.name not in _logger_console_handlers:
                _logger_console_handlers[log.name] = log_console_handler
                log.addHandler(log_console_handler)


    # resolve level
    if level is not None:
        if type(level) is str:
            level = getattr(logging, level, logging.INFO)
        fh.setLevel(level)

    return log


def update_file_logger(log: logging.Logger, logopts: Union["StimelaLogConfig", DictConfig], nesting: int = 0, subst: Optional[SubstitutionNS] = None):
    """Updates logfiles associated with given logger based on option settings

    Args:
        log (logging.Logger):                          Logger object
        nesting (int):                                 nesting level of this logger
        logopts (Union[StimelaLogConfig, DictConfig]): config settings
        subst (Dict[str, Any]):                        dictionary of substitutions for pathnames in logopts

    Returns:
        [type]: [description]
    """
    if logopts.enable and logopts.nest > nesting:

        path = os.path.join(logopts.dir or ".", logopts.name)
        
        # {}-substitutions

        if subst is not None:
            with forgiving_substitutions_from(subst, raise_errors=False) as context: 
                path = context.evaluate(path, location=["log file"])
                if context.errors:
                    for err in context.errors:
                        log.error(f"bad substitution in log file: {err}")
                    return None

        # substitute non-filename characters for _
        path = re.sub(r'[^a-zA-Z0-9_./-]', '_', path)

        # setup the logger
        setup_file_logger(log, path, level=logopts.level, symlink=logopts.symlink)

    else:
        disable_file_logger(log)


