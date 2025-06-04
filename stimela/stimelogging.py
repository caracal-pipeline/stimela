import  os.path
import  re
import logging
import traceback
import copy
from types import TracebackType
from typing import Optional, OrderedDict, Union, Any
from omegaconf import DictConfig
from scabha.exceptions import ScabhaBaseException, FormattedTraceback
from scabha.substitutions import SubstitutionNS, forgiving_substitutions_from
import rich.progress
import rich.logging
from rich.tree import Tree
from rich import print as rich_print
from rich.markup import escape
from rich.padding import Padding
from rich.syntax import Syntax
from rich.pretty import Pretty
from rich.errors import MarkupError
from warnings import warn

from . import task_stats

CONSOLE_PRINT_OPTIONS = [
    "sep",
    "end",
    "style",
    "justify",
    "overflow",
    "no_wrap",
    "emoji",
    "markup",
    "highlight",
    "width",
    "height",
    "crop",
    "soft_wrap",
    "new_line_start"
]


class FunkyMessage(object):
    """Class representing a message with two versions: funky (with markup), and boring (no markup)"""
    def __init__(self, funky, boring=None, prefix=None, escape_emojis=True):
        self.boring = boring if boring is not None else funky
        if escape_emojis:
            self.funky = re.sub(r":(\w+):", r":[bold][/bold]\1:", funky)
        self.funky = funky
        self.prefix = prefix
    def __str__(self):
        return self.funky
    def __add__(self, other):
        if isinstance(other, FunkyMessage):
            return FunkyMessage(f"{self}{other}", f"{self.boring}{other.boring}")
        else:
            return FunkyMessage(f"{self}{other}", f"{self.boring}{other}")

def defunkify(arg: Union[str, FunkyMessage]):
    return arg.boring if isinstance(arg, FunkyMessage) else arg

class StimelaConsoleHander(rich.logging.RichHandler):
    def __init__(self, console):
        rich.logging.RichHandler.__init__(self, console=console,
                    highlighter=rich.highlighter.NullHighlighter(), 
                    markup=True,
                    show_level=False, show_path=False, show_time=False, keywords=[])
        self._console = console

    def emit(self, record):
        # NOTE(JSKenyon): If a message requires a custom console print,
        # forward all known arguments to the _console.print method.
        if getattr(record, "custom_console_print", False):
            self._console.print(
                record.msg,
                **{k: getattr(record, k) for k in CONSOLE_PRINT_OPTIONS if hasattr(record, k)}
            )
            return
        try:
            rich.logging.RichHandler.emit(self, record)
        # backstop -- message should have been properly markup-escaped
        except MarkupError:
            record.msg = escape(record.msg)
            self._console.print(f"Malformed markup in log message: {record.msg}", markup=False, style="red")
            self._console.print(f"This is a (probably harmless) bug -- but please report", markup=False, style="red")
            try:
                rich.logging.RichHandler.emit(self, record)
            except MarkupError:
                self._console.print(f"Malformed markup after escaping -- this is surely a bug -- please report", markup=False, style="red")

        if hasattr(record, 'console_payload'):
            self._console.print(record.console_payload, highlight=getattr(record, 'console_highlight', None))

class StimelaLogFormatter(logging.Formatter):
    DEBUG_STYLE   = "dim", ""
    WARNING_STYLE = "", "yellow"
    ERROR_STYLE   = "bold", "red"
    CRITICAL_STYLE  = "bold", "red"

    _ansi_escape = re.compile(r'\x1B[@-_][0-?]*[ -/]*[@-~]')

    def __init__(self, boring=False, override_message_attr=None):
        datefmt = "%Y-%m-%d %H:%M:%S"
        if not boring:
            super().__init__("{asctime} {name} [{style}]{levelname}: {message}[/{style}]", datefmt, style="{")
            self._prefix_fmt = logging.Formatter("[{style}]{prefix} {message}[/{style}]", style="{")
        else:
            super().__init__("{asctime} {name} {levelname}: {message}", datefmt, style="{")
            self._prefix_fmt = logging.Formatter("{prefix} {message}", style="{")
        self.boring = boring
        self._override_attr = override_message_attr

    def format(self, record):
        record = copy.copy(record)
        # override message, if a specific override attribute is present
        if self._override_attr is not None and hasattr(record, self._override_attr):
            record.msg = getattr(record, self._override_attr)
        # apply styling
        if not hasattr(record, 'style'):
            if record.levelno <= logging.DEBUG:
                font, color = self.DEBUG_STYLE
            elif record.levelno >= logging.CRITICAL:
                font, color = self.SEVERE_STYLE
            elif record.levelno >= logging.ERROR:
                font, color = self.ERROR_STYLE
            elif record.levelno >= logging.WARNING:
                font, color = self.WARNING_STYLE
            else:
                font = color = ""
            if getattr(record, 'color', None):
                color = record.color.lower()
            if hasattr(record, 'bold') or hasattr(record, 'boldface'):
                if "bold" not in font:
                    font = f"bold {font}"
            setattr(record, 'style', f"{font} {color}".strip() or "normal")
        # in boring mode, make funky messages boring, and strip ANSI codes
        if self.boring:
            record.msg = self._ansi_escape.sub('', defunkify(record.msg))
        # select format based on whether we have prefix or not
        if hasattr(record, 'prefix'):
            if self.boring:
                record.prefix = defunkify(record.prefix)
            return self._prefix_fmt.format(record)
        else:
            return super().format(record)

_logger = None
_log_console_handler = _log_formatter = _log_file_formatter = _log_boring_formatter = _log_colourful_formatter = None

_boring = False

LOG_DIR = '.'

def is_logger_initialized():
    return _logger is not None


def declare_chapter(title: str, **kw):
    if not _boring:
        progress_console.rule(title, **kw)

def apply_style(text: str, style: str):
    if _boring:
        return text
    else:
        return f"[{style}]{text}[/{style}]"


def logger(name="STIMELA", propagate=False, boring=False, loglevel="INFO"):
    """Returns the global Stimela logger (initializing if not already done so, with the given values)"""
    global _logger, _boring
    if _logger is None:
        _logger = logging.getLogger(name)
        _boring = boring
        if type(loglevel) is str:
            loglevel = getattr(logging, loglevel)
        _logger.setLevel(loglevel)
        _logger.propagate = propagate

        global _log_console_handler, _log_formatter, _log_file_formatter, _log_boring_formatter, _log_colourful_formatter
        global progress_console, progress_bar

        _log_file_formatter = StimelaLogFormatter(boring=True, override_message_attr='logfile_message')
        _log_boring_formatter = StimelaLogFormatter(boring=True)
        _log_colourful_formatter = StimelaLogFormatter(boring=False)

        _log_formatter = _log_boring_formatter if boring else _log_colourful_formatter

        progress_bar, progress_console = task_stats.init_progress_bar(boring=boring)

        _log_console_handler = StimelaConsoleHander(console=progress_console)

        _log_console_handler.setFormatter(_log_formatter)
        _log_console_handler.setLevel(loglevel)

        _logger.addHandler(_log_console_handler)
        _logger_console_handlers[_logger.name] = _log_console_handler

        import scabha
        scabha.set_logger(_logger)

    return _logger

_logger_file_handlers = {}
_logger_console_handlers = {}

# keep track of all log files opened
_previous_logfiles = set()


def has_file_logger(log: logging.Logger):
    return log.name in _logger_file_handlers


def disable_file_logger(log: logging.Logger):
    current_logfile, fh = _logger_file_handlers.get(log.name, (None, None))
    if fh is not None:
        fh.close()
        log.removeHandler(fh)
        del _logger_file_handlers[log.name]


class DelayedFileHandler(logging.FileHandler):
    """A version of FileHandler that also handles directory and symlink creation in a delayed way"""
    def __init__(self, logfile, symlink, mode):
        self.symlink, self.logfile = symlink, logfile
        self.is_open = False
        super().__init__(logfile, mode, delay=True)

    def get_logfile_dir(self):
        """Gets name of logfile and ensures the directory exists"""
        if not self.is_open:
            self.is_open = True
            logdir = os.path.dirname(self.logfile)
            if logdir and not os.path.exists(logdir):            
                os.makedirs(logdir)
                if self.symlink:
                    symlink_path = os.path.join(os.path.dirname(logdir.rstrip("/")) or ".", self.symlink)
                    # remove existing symlink
                    if os.path.islink(symlink_path):
                        os.unlink(symlink_path)
                    # Make symlink to logdir. If name exists and is not a symlink, we'll do nothing
                    if not os.path.exists(symlink_path):
                        os.symlink(os.path.basename(logdir), symlink_path)
        return os.path.dirname(self.logfile)

    def emit(self, record):
        self.get_logfile_dir()
        return super().emit(record)

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
        # if file was previously open, append, else overwrite
        if logfile in _previous_logfiles:
            mode = 'a'
        else:
            mode = 'w'
            _previous_logfiles.add(logfile)
        # create new FH
        fh = DelayedFileHandler(logfile, symlink, mode)
        fh.setFormatter(_log_file_formatter)
        log.addHandler(fh)

        _logger_file_handlers[log.name] = logfile, fh

        # if logging to console, disable propagation from this sub-logger, and add a console handler
        # This ensures that parent loggers that log to files to not get repeated messages
        if _log_console_handler:
            log.propagate = False
            if log.name not in _logger_console_handlers:
                _logger_console_handlers[log.name] = _log_console_handler
                log.addHandler(_log_console_handler)


    # resolve level
    if level is not None:
        if type(level) is str:
            level = getattr(logging, level, logging.INFO)
        fh.setLevel(level)

    return log


def update_file_logger(log: logging.Logger, logopts: DictConfig, nesting: int = 0, subst: Optional[SubstitutionNS] = None, location=[]):
    """Updates logfiles associated with given logger based on option settings

    Args:
        log (logging.Logger):                          Logger object
        nesting (int):                                 nesting level of this logger
        logopts (Union[StimelaLogConfig, DictConfig]): config settings
        subst (Dict[str, Any]):                        dictionary of substitutions for pathnames in logopts
        location (List[str]):                          location of this logger in the hierarchy  

    Returns:
        [type]: [description]
    """

    if logopts.enable and logopts.nest >= nesting:
        path = os.path.join(logopts.dir or ".", logopts.name + logopts.ext)

        if subst is not None:
            with forgiving_substitutions_from(subst, raise_errors=False) as context: 
                path = context.evaluate(path, location=location + ["log"])
                if context.errors:
                    for err in context.errors:
                        log.error(f"bad substitution in log path: {err}")
                    return None

        # substitute non-filename characters for _
        path = re.sub(r'[^a-zA-Z0-9_./-]', '_', path)

        # setup the logger
        setup_file_logger(log, path, level=logopts.level, symlink=logopts.symlink)
    else:
        disable_file_logger(log)
        log.propagate = True


def get_logfile_dir(log: logging.Logger):
    """Returns filename associated with the logger, or None if not logging to file"""
    logfile, fh = _logger_file_handlers.get(log.name, (None, None))
    if logfile is None:
        return None
    return fh.get_logfile_dir()


def log_exception(*errors, severity="error", log=None):
    """Logs one or more error messages or exceptions (unless they are marked as already logged), and 
    pretty-prints them to the console  as appropriate.
    """
    def exc_message(e):
        if isinstance(e, ScabhaBaseException):
            return escape(e.message)
        elif type(e) is str:
            return escape(e)
        else:
            return escape(f"{type(e).__name__}: {e}")

    if severity == "error":
        colour = "bold red"
        message_dispatch = (log or logger()).error
    else:
        colour = "yellow"
        message_dispatch = (log or logger()).warning
    
    trees = []
    do_log = False
    messages = []

    def add_dict(dd, tree):
        for field, value in dd.items():
            if isinstance(value, (dict, OrderedDict, DictConfig)):
                subtree = tree.add(escape(f"{field}:"))
                add_dict(value, subtree)
            else:
                tree.add(escape(f"{field}: {value}"))

    def add_nested(excs, tree):
        for exc in excs:
            if isinstance(exc, Exception):
                subtree = tree.add(f"{exc_message(exc)}")
                # tbtree = subtree.add("Traceback:")
                # for line in traceback.format_exception(exc):
                #     tbtree.add(line)
                if isinstance(exc, ScabhaBaseException) and exc.nested:
                    add_nested(exc.nested, subtree)
            elif type(exc) is TracebackType:
                subtree = tree.add(apply_style("Traceback:", "dim"))
                for line in traceback.format_tb(exc):
                    subtree.add(apply_style(escape(line.rstrip()), "dim"))
            elif type(exc) is FormattedTraceback:
                subtree = tree.add(apply_style("Traceback:", "dim"))
                for line in exc.lines:
                    subtree.add(apply_style(escape(line), "dim"))
            elif isinstance(exc, (dict, OrderedDict, DictConfig)):
                add_dict(exc, tree)
            else:
                tree.add(str(exc))

    has_nesting = False

    for exc in errors:
        if isinstance(exc, ScabhaBaseException):
            messages.append(exc.message)
            if not exc.logged:
                do_log = exc.logged = True
            tree = Tree(exc_message(exc) if _boring else 
                            f"[{colour}]{exc_message(exc)}[/{colour}]", 
                        guide_style="" if _boring else "dim")
            trees.append(tree)
            if exc.nested:
                add_nested(exc.nested, tree)
                has_nesting = True
        else:
            tree = Tree(exc_message(exc) if _boring else 
                            f"[{colour}]{exc_message(exc)}[/{colour}]", 
                        guide_style="" if _boring else "dim")
            trees.append(tree)
            do_log = True
            messages.append(str(exc))

    if do_log:
        message_dispatch(": ".join(messages))

    printfunc = task_stats.progress_bar.console.print if task_stats.progress_bar is not None else rich_print

    if has_nesting:
        declare_chapter("detailed error report follows", style="red")
        for tree in trees:
            printfunc(Padding(tree, pad=(0,0,0,8)))

def log_rich_payload(log: logging.Logger, message: str, payload: Any, 
                     console_payload: Optional[Any] = None, syntax: Optional[str] = None, 
                     level: int = logging.INFO):
    """Logs a message with a rich payload. File logger will get plain text version ("message: \npayload"), 
    console logger will get a rich renderable console_payload object instead, or a Syntax object. 

    Args:
        log (logging.Logger): logger to use
        message (str): message
        payload (Any): payload string
        console_payload (Any): rich renderable object corresponding to the payload. If not set, uses Pretty(payload)
        syntax (str, optional): if set, uses Syntax(payload, syntax) as the console_payload
        level: (int, optional): logging level, defaults to INFO
    """
    if syntax is not None:
        console_payload = Syntax(payload, syntax, padding=(0, 0, 0, 8))
    elif console_payload is None:
        console_payload = Pretty(payload, indent_size=8)
    log.log(level, f"{message}:", 
            extra=dict(logfile_message=f"{message}: \n{str(payload)}",
                        console_payload=console_payload))
