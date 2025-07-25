import atexit
from dataclasses import dataclass, fields
from collections import defaultdict
import sys
import os.path
from datetime import datetime, timedelta
import contextlib
import asyncio
from typing import OrderedDict, Any, List, Callable, Optional
from scabha.basetypes import EmptyListDefault
from omegaconf import OmegaConf
import psutil

import rich.logging
from rich.progress import (
    Progress,
    SpinnerColumn,
    TimeElapsedColumn,
    TextColumn
)
from rich.console import Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table, Column
from rich.text import Text

from stimela import stimelogging


progress_console = rich.console.Console(
    file=sys.stdout,
    highlight=False,
    emoji=False
)
class Display:

    progress_console = progress_console

    progress_fields = {
        "cpu_usage": "CPU",
        "ram_usage": "RAM",
        "system_load": "Load",
        "disk_read": "Read",
        "disk_write": "Write",
        "task_name": "Step",
        "task_status": "Status",
        "task_command": "Command",
        "task_cpu_usage": "CPU",
        "task_ram_usage": "RAM",
        "task_peak_ram_usage": "Peak",
        "task_peak_cpu_usage": "Peak",
    }

    styles = {"fancy", "simple"}

    def __init__(self):

        self.display_style = "default"
        self.style_override = None

        self.total_elapsed = self._timer_element()
        self.total_elapsed_id = self.total_elapsed.add_task("", start=True)

        self.task_elapsed = self._timer_element()
        self.task_elapsed_id = self.task_elapsed.add_task("")

        for k, v in self.progress_fields.items():
            status = self._status_element()
            status_id = status.add_task(v, value=None)
            setattr(self, k, status)
            setattr(self, f"{k}_id", status_id)

        msg = Text("DISPLAY HAS NOT BEEN CONFIGURED", justify="center")
        msg.stylize("bold red")

        self.live_display = Live(
            msg,
            refresh_per_second=5,
            console=progress_console,
            transient=True
        )

        self.task_maxima = defaultdict(float)

    def _timer_element(self, width=None):
        return Progress(
            SpinnerColumn(),
            TextColumn(
                "[yellow][bold]{task.description}[/bold][/yellow]",
                table_column=Column(no_wrap=True, width=width)
            ),
            TimeElapsedColumn(),
            refresh_per_second=2,
            console=self.progress_console,
            transient=True
        )

    def _status_element(self, width=None):
        return Progress(
            TextColumn(
                "[bold]{task.description}[/bold]",
                table_column=Column(no_wrap=True, width=width)
            ),
            TextColumn(
                "[bold]{task.fields[value]}[/bold]",
                table_column=Column(no_wrap=True)
            ),
            refresh_per_second=2,
            console=progress_console,
            transient=True
        )

    def set_display_style(self, style="simple"):

        if self.style_override:  # If set, ignore style argument.
            style = self.style_override

        if self.display_style == style:
            return  # Already configured.

        if style == "fancy":
            self._configure_fancy_display()
        elif style == "simple":
            self._configure_simple_display()
        elif style == "default":
            self.__init__()
        else:
            raise ValueError(f"Unrecognised style: {style}")

    def set_display_style_override(self, style=None):

        if style in self.styles or style is None:
            self.style_override = style
        else:
            raise ValueError(f"Unrecognised style: {style}")
        self.set_display_style()

    def _configure_fancy_display(self):

        self.display_style = "fancy"

        progress_fields = self.progress_fields

        width = max(len(fn) for fn in progress_fields.values() if fn)

        # Set the width of the text column in the recipe timer.
        self.total_elapsed.columns[1].get_table_column().width = width - 1
        # Set the description recipe timer task.
        self.total_elapsed.update(self.total_elapsed_id, description="")

        self.task_elapsed = self._timer_element(width=width - 1)
        self.task_elapsed_id = self.task_elapsed.add_task("")

        for k, v in progress_fields.items():
            status = self._status_element(width=width)
            status_id = status.add_task(v, value=None)
            setattr(self, k, status)
            setattr(self, f"{k}_id", status_id)

        ram_table = Table.grid(expand=True, padding=(0,1))
        ram_table.add_column(ratio=1)
        ram_table.add_column(ratio=1)
        ram_table.add_row(
            self.task_ram_usage,
            self.task_peak_ram_usage,
        )

        cpu_table = Table.grid(expand=True, padding=(0,1))
        cpu_table.add_column(ratio=1)
        cpu_table.add_column(ratio=1)
        cpu_table.add_row(
            self.task_cpu_usage,
            self.task_peak_cpu_usage,
        )

        task_group = Group(
            self.task_elapsed,
            self.task_name,
            self.task_status,
            self.task_command,
            cpu_table,
            ram_table
        )

        system_group = Group(
            self.total_elapsed,
            self.cpu_usage,
            self.ram_usage,
            self.disk_read,
            self.disk_write,
            self.system_load
        )

        table = Table.grid(expand=True)
        table.add_column()
        table.add_column(ratio=1)
        table.add_column(ratio=1)
        table.add_column()
        table.add_row(
            " ",  # Spacer.
            Panel(
                system_group,
                title="System",
                border_style="green",
                expand=True
            ),
            Panel(
                task_group,
                title="Task",
                border_style="green",
                expand=True,

            ),
            " "  # Spacer.
        )

        self.live_display.update(table)

    def _configure_simple_display(self):

        self.display_style = "simple"

        progress_fields = self.progress_fields | {
            "disk_read": "R",
            "disk_write": "W",
        }

        # Set the width of the text column in the recipe timer.
        self.total_elapsed.columns[1].get_table_column().width = None
        # Set the description recipe timer task.
        self.total_elapsed.update(self.total_elapsed_id, description="R")

        self.task_elapsed = self._timer_element()
        self.task_elapsed_id = self.task_elapsed.add_task("S")

        for k, v in progress_fields.items():
            status = self._status_element()
            status_id = status.add_task(v, value=None)
            setattr(self, k, status)
            setattr(self, f"{k}_id", status_id)

        # Drop the description column of name and status for brevity.
        self.task_name.columns = self.task_name.columns[1:]
        self.task_status.columns = self.task_status.columns[1:]

        table = Table.grid(expand=False, padding=(0, 1))
        table.add_row(
            self.total_elapsed,
            self.task_elapsed,
            self.task_name,
            self.task_status,
            self.task_cpu_usage,
            self.task_ram_usage,
            self.disk_read,
            self.disk_write
        )

        self.live_display.update(table)

    def reset_current_task(self):
        self.task_elapsed.reset(self.task_elapsed_id)
        self.task_maxima = defaultdict(float)

    def enable(self):
        def destructor():
            self.live_display.__exit__(None, None, None)
        atexit.register(destructor)
        self.live_display.__enter__()

    def disable(self):
        self.live_display.__exit__(None, None, None)

    def update(self, sys_stats, task_stats, task_info):

        self.cpu_usage.update(
            self.cpu_usage_id,
            value=f"[green]{sys_stats.cpu}[/green]%"
        )

        used = sys_stats.mem_used
        total = sys_stats.mem_total
        percent = (used / total) * 100

        self.ram_usage.update(
            self.ram_usage_id,
            value=(
                f"[green]{used}/{total}[/green]GB "
                f"([green]{percent:.2f}[/green]%)"
            )
        )

        self.system_load.update(
            self.system_load_id,
            value=(
                f"[green]{task_stats.load_1m:.2f}[/green]%/"
                f"[green]{task_stats.load_5m:.2f}[/green]%/"
                f"[green]{task_stats.load_15m:.2f}[/green]% "
                f"(1/5/15 min)"
            )
        )

        self.disk_read.update(
            self.disk_read_id,
            value=(
                f"[green]{task_stats.read_gbps:2.2f}[/green]GB/s "
                f"[green]{task_stats.read_ms:4}[/green]ms "
                f"[green]{task_stats.read_count:-4}[/green] reads"
            )
        )
        self.disk_write.update(
            self.disk_write_id,
            value=(
                f"[green]{task_stats.write_gbps:2.2f}[/green]GB/s "
                f"[green]{task_stats.write_ms:4}[/green]ms "
                f"[green]{task_stats.write_count:-4}[/green] writes"
            )
        )

        if task_info is not None:
            self.task_name.update(
                self.task_name_id,
                value=f"[bold]{task_info.description}[/bold]"
            )

            self.task_status.update(
                self.task_status_id,
                value=f"[dim]{task_info.status or 'N/A'}[/dim]"
            )
            # Sometimes the command contains square brackets which rich
            # interprets as formatting. Remove them. # TODO: Figure out
            # why the command has square brackets in the first place.
            self.task_command.update(
                self.task_command_id,
                value=f"{(task_info.command or 'N/A').strip('[]')}"
            )

        self.task_cpu_usage.update(
            self.task_cpu_usage_id,
            value=f"[green]{task_stats.cpu:2.1f}[/green]%"
        )

        max_cpu = max(
            task_stats.cpu, self.task_maxima["task_peak_cpu_usage"]
        )
        self.task_maxima["task_peak_cpu_usage"] = max_cpu
        self.task_peak_cpu_usage.update(
            self.task_peak_cpu_usage_id,
            value=f"[green]{max_cpu:2.1f}[/green]%"
        )

        self.task_ram_usage.update(
            self.task_ram_usage_id,
            value=f"[green]{task_stats.mem_used}[/green]GB"
        )

        max_ram = max(
            task_stats.mem_used, self.task_maxima["task_peak_ram_usage"]
        )
        self.task_maxima["task_peak_ram_usage"] = max_ram
        self.task_peak_ram_usage.update(
            self.task_peak_ram_usage_id,
            value=f"[green]{max_ram}[/green]GB"
        )

display = Display()

# this is "" for the main process, ".0", ".1", for subprocesses, ".0.0" for nested subprocesses
_subprocess_identifier = ""

def get_subprocess_id():
    return _subprocess_identifier

def add_subprocess_id(num: int):
    global _subprocess_identifier
    _subprocess_identifier += f".{num}"

_prev_disk_io = None, None

@dataclass
class TaskInformation(object):
    names: List[str]
    status: str = ""
    task_attrs: List[str] = EmptyListDefault()
    command: Optional[str] = None
    status_reporter: Optional[Callable] = None
    hide_local_metrics: bool = False

    def __post_init__(self):
        self.names_orig = list(self.names)

    @property
    def description(self):
        name = '.'.join(self.names)
        # if self.status:
        #     name += f": [dim]{self.status}[/dim]"
        ## OMS: omit attributes from task status for now
        # if self.task_attrs:
        #     name += f"\[{', '.join(self.task_attrs)}]"
        return name

# stack of task information -- most recent subtask is at the end
_task_stack = []
stimela_process = psutil.Process()
child_processes = {}

@contextlib.contextmanager
def declare_subtask(subtask_name, status_reporter=None, hide_local_metrics=False):
    task_names = []
    if _task_stack:
        task_names = _task_stack[-1].names + \
                    (_task_stack[-1].task_attrs or [])
    task_names.append(subtask_name)
    _task_stack.append(
        TaskInformation(task_names, status_reporter=status_reporter, hide_local_metrics=hide_local_metrics)
    )
    update_process_status()
    try:
        yield subtask_name
    finally:
        _task_stack.pop(-1)
        update_process_status()


def declare_subtask_status(status):
    _task_stack[-1].status = status
    update_process_status()


def declare_subtask_attributes(*args, **kw):
    _task_stack[-1].task_attrs = [str(x) for x in args] + \
                                 [f"{key} {value}" for key, value in kw.items()]
    update_process_status()


class _CommandContext(object):
    def __init__(self, command):
        self.command = command
        _task_stack[-1].command = command
        update_process_status()
    def ctrl_c(self):
        _task_stack[-1].command = f"{self.command}(^C)"
        update_process_status()
    def update_status(self, status):
        _task_stack[-1].command = f"{self.command} ({status})"
        update_process_status()


@contextlib.contextmanager
def declare_subcommand(command):
    display.reset_current_task()
    try:
        yield _CommandContext(command)
    finally:
        _task_stack[-1].command = None
        update_process_status()

@dataclass
class SystemStatsDatum:
    n_cpu: int = psutil.cpu_count()
    cpu: float = psutil.cpu_percent()
    mem_used: float = round(psutil.virtual_memory().used / (2 ** 30))
    mem_total: float = round(psutil.virtual_memory().total / (2 ** 30))

@dataclass
class TaskStatsDatum(object):
    cpu: float          = 0
    mem_used: float     = 0
    load_1m: float      = 0
    load_5m: float      = 0
    load_15m: float     = 0
    read_count: int     = 0
    read_gb: float      = 0
    read_gbps: float    = 0
    read_ms: float      = 0
    write_count: int    = 0
    write_gb: float     = 0
    write_gbps: float   = 0
    write_ms: float     = 0
    num_samples: int    = 0

    def __post_init__(self):
        self.extras = []

    def insert_extra_stats(self, **kw):
        for name, value in kw.items():
            self.extras.append(name)
            setattr(self, name, value)

    def add(self, other: "TaskStatsDatum"):
        for f in _taskstats_sample_names + other.extras:
            if not hasattr(self, f):
                self.extras.append(f)
            setattr(self, f, getattr(self, f, 0) + getattr(other, f))

    def peak(self, other: "TaskStatsDatum"):
        for f in _taskstats_sample_names + other.extras:
            if hasattr(self, f):
                setattr(self, f, max(getattr(self, f, -1e-9999), getattr(other, f)))
            else:
                self.extras.append(f)
                setattr(self, f, getattr(other, f))

    def averaged(self):
        avg = TaskStatsDatum(num_samples=1)
        for f in _taskstats_sample_names:
            setattr(avg, f, getattr(self, f) / self.num_samples)
        avg.insert_extra_stats(**{name: getattr(self, name) / self.num_samples for name in self.extras})
        return avg


_taskstats_sample_names = [f.name for f in fields(TaskStatsDatum)]

_taskstats = OrderedDict()
_task_start_time = OrderedDict()


def collect_stats():
    """Returns dictionary of per-task stats (elapsed time, sums, peaks)"""
    # cumulative add -- substeps contribute to parent steps
    for key in list(_taskstats.keys())[::-1]:
        _, sum, peak = _taskstats[key]
        key1 = tuple(key[:-1])
        if key1 in _taskstats:
            _, sum1, peak1 = _taskstats[key1]
            sum1.add(sum)
            peak1.peak(peak)
    return _taskstats


def add_missing_stats(stats):
    """Adds stats that weren't recorded into dictionary"""
    for key, value in stats.items():
        if key not in _taskstats:
            _taskstats[key] = value


def stats_field_names():
    return _taskstats_sample_names


def update_stats(now: datetime, sample: TaskStatsDatum):
    if _task_stack:
        ti = _task_stack[-1]
        keys = [tuple(ti.names)]
        if ti.task_attrs:
            keys.append(tuple(ti.names + ti.task_attrs))
    else:
        keys = [()]

    for key in keys:
        _, sum, peak = _taskstats.setdefault(key, [0, TaskStatsDatum(), TaskStatsDatum()])
        sum.add(sample)
        peak.peak(sample)
        start = _task_start_time.setdefault(key, now)
        _taskstats[key][0] = (now - start).total_seconds()

def update_children():
    """Update the module level dictionary mapping child pid to process.

    This is necessary as calling Process.children will return different
    Process objects each time. These then fail to report CPU stats unless
    we make them block which has a large impact on performance.
    """
    current_children = stimela_process.children(recursive=True)
    current_pids = {proc.pid for proc in current_children}
    child_processes.update(
        {c.pid: c for c in current_children if c.pid not in child_processes}
    )
    dropped_pids = {c for c in child_processes.keys() if c not in current_pids}

    for pid in dropped_pids:
        del child_processes[pid]

def update_process_status():
    # current subtask info
    task_info = _task_stack[-1] if _task_stack else None

    # elapsed time since start
    now = datetime.now()

    # form up sample datum
    task_stats = TaskStatsDatum(num_samples=1)
    sys_stats = SystemStatsDatum()

    update_children()
    # Assume that all child processes belong to the same task.
    # TODO(JSKenyon): Handling of children is rudimentary at present.
    # How would this work for scattered/parallel steps?
    if child_processes and task_info:
        processes = list(child_processes.values())
    else:
        processes = []  # Don't bother with cpu and mem for stimela itself.

    # CPU and memory
    for p in processes:
        try:
            task_stats.cpu += p.cpu_percent()
            task_stats.mem_used += p.memory_info().rss
        except psutil.NoSuchProcess:
            pass  # Process ended before we could gather its stats.

    task_stats.mem_used = round(task_stats.mem_used  / (2 ** 30))

    # load
    load = [l/sys_stats.n_cpu * 100 for l in psutil.getloadavg()]
    task_stats.load_1m, task_stats.load_5m, task_stats.load_15m = load

    # get disk I/O stats
    disk_io = psutil.disk_io_counters()
    global _prev_disk_io
    prev_io, prev_time = _prev_disk_io
    if prev_io is not None:
        delta = (now - prev_time).total_seconds()
        io = {}
        io_fields = (
            'read_bytes',
            'read_count',
            'read_time',
            'write_bytes',
            'write_count',
            'write_time'
        )
        for key in io_fields:
            io[key] = getattr(disk_io, key) - getattr(prev_io, key)
        task_stats.read_count = io['read_count']
        task_stats.write_count = io['write_count']
        task_stats.read_gb = io['read_bytes']/2**30
        task_stats.write_gb = io['write_bytes']/2**30
        task_stats.read_gbps = task_stats.read_gb / delta
        task_stats.write_gbps = task_stats.write_gb / delta
        task_stats.read_ms = io['read_time']
        task_stats.write_ms = io['write_time']
    else:
        io = None
    _prev_disk_io = disk_io, now

    # call extra status reporter
    # TODO(JSKenyon): I have broken this code while updating taskstats.py to
    # use a live display. This will need to be fixed at some point, ideally
    # when we have access to a kubenetes cluster.
    if task_info and task_info.status_reporter:
        extra_metrics, extra_stats = task_info.status_reporter()
        if extra_stats:
            task_stats.insert_extra_stats(**extra_stats)
    else:
        extra_metrics = None

    # TODO(JSKenyon): This is not correct and needs to be handled elsewhere.
    # When using a remote backend, local metrics and not useful and should
    # be hidden.
    if not any(ti.hide_local_metrics for ti in _task_stack):
        display.update(sys_stats, task_stats, task_info)

    # update stats
    update_stats(now, task_stats)


async def run_process_status_update():
    if display.live_display.is_started:
        with contextlib.suppress(asyncio.CancelledError):
            while True:
                update_process_status()
                await asyncio.sleep(1)

_printed_stats = dict(
    k8s_cores="k8s cores",
    k8s_mem="k8s mem GB",
    cpu="CPU %",
    mem_used="Mem GB",
    load_1m="Load %",
    read_gbps="R GB/s",
    write_gbps="W GB/s",
)

# these stats are written as sums
_sum_stats = ("read_count", "read_gb", "read_ms", "write_count", "write_gb", "write_ms")

def render_profiling_summary(stats: TaskStatsDatum, max_depth, unroll_loops=False):

    table_avg = Table(title=Text("\naverages & total I/O", style="bold"))
    table_avg.add_column("")
    table_avg.add_column("time hms", justify="right")

    table_peak = Table(title=Text("\npeaks", style="bold"))
    table_peak.add_column("")
    table_peak.add_column("time hms", justify="right")

    # accumulate set of all stats available
    available_stats = set(_taskstats_sample_names)
    for name, (elapsed, sum, peak) in stats.items():
        available_stats.update(sum.extras)
        available_stats.update(peak.extras)

    for f, label in _printed_stats.items():
        if f in available_stats:
            table_avg.add_column(label, justify="right")
            table_peak.add_column(label, justify="right")

    table_avg.add_column("R GB", justify="right")
    table_avg.add_column("W GB", justify="right")

    for name_tuple, (elapsed, sum, peak) in stats.items():
        if name_tuple and len(name_tuple) <= max_depth and elapsed > 0:
            # skip loop iterations, if not unrolling loops
            if not unroll_loops and any(n.endswith("]") for n in name_tuple):
                continue
            secs, mins, hours = elapsed % 60, int(elapsed // 60) % 60, int(elapsed // 3600)
            tstr = f"{hours:d}:{mins:02d}:{secs:04.1f}"
            avg = sum.averaged()
            indentation_level = len(name_tuple) - 1
            avg_row = ["  " * indentation_level + name_tuple[-1], tstr]
            peak_row = avg_row.copy()
            for f, label in _printed_stats.items():
                if f in available_stats:
                    avg_row.append(f"{getattr(avg, f):.2f}" if hasattr(avg, f) else "")
                    peak_row.append(f"{getattr(peak, f):.2f}" if hasattr(peak, f) else "")

            avg_row += [f"{sum.read_gb:.2f}", f"{sum.write_gb:.2f}"]
            table_avg.add_row(*avg_row)
            table_peak.add_row(*peak_row)

    stimelogging.declare_chapter("profiling results")
    # Disable display - ensures that it doesn't appear below the profiling.
    display.disable()
    from rich.columns import Columns
    # progress_console.print(table_avg, justify="center")
    # progress_console.print(table_peak, justify="center")
    # progress_console.print(Columns((table_avg, table_peak)), justify="center")

    with progress_console.capture() as capture:
        progress_console.print(Columns((table_avg, table_peak)), justify="center")

    text = capture.get()

    return text


# from rich.console import Console
# console = Console()
# with console.capture() as capture:
#     console.print("[bold red]Hello[/] World")
# str_output = capture.get()

def save_profiling_stats(log, print_depth=2, unroll_loops=False):
    from . import stimelogging

    stats = collect_stats()
    summary = render_profiling_summary(stats, print_depth, unroll_loops=unroll_loops)
    if print_depth:
        print(summary)

    filename = os.path.join(stimelogging.get_logfile_dir(log) or '.', "stimela.stats.full")

    stats_dict = OmegaConf.create()

    for name, (elapsed, sum, peak) in stats.items():
        if name:
            name = '.'.join(name)
            avg = sum.averaged()
            davg = {f: getattr(avg, f) for f in _taskstats_sample_names}
            dpeak = {f: getattr(peak, f) for f in _taskstats_sample_names}
            dsum = {f: getattr(sum, f) for f in _sum_stats}

            stats_dict[name] = dict(elapsed=elapsed, avg=davg, peak=dpeak, total=dsum)

    OmegaConf.save(stats_dict, filename)

    log.info(f"saved full profiling stats to {filename}")

    filename = os.path.join(stimelogging.get_logfile_dir(log) or '.', "stimela.stats.summary.txt")
    open(filename, "wt").write(summary)

    log.info(f"saved summary to {filename}")
