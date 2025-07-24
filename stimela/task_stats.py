import atexit
from dataclasses import dataclass, fields
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
    }

    def __init__(self):

        self.display_style = "default"

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

    def set_display_style(self, style):

        if self.display_style == style:
            return  # Already configured.

        if style == "fancy":
            self._configure_fancy_display()
        elif style == "simple":
            self._configure_simple_display()
        elif style == "default":
            self.__init__()
        else:
            raise ValueError(f"Unrecognised style when configuring display.")

    def _configure_fancy_display(self):

        self.display_style = "fancy"

        progress_fields = self.progress_fields

        width = max(len(fn) for fn in progress_fields.values() if fn)

        self.total_elapsed = self._timer_element(width=width - 1)
        self.total_elapsed_id = self.total_elapsed.add_task("", start=True)

        self.task_elapsed = self._timer_element(width=width - 1)
        self.task_elapsed_id = self.task_elapsed.add_task("")

        for k, v in progress_fields.items():
            status = self._status_element(width=width)
            status_id = status.add_task(v, value=None)
            setattr(self, k, status)
            setattr(self, f"{k}_id", status_id)

        task_group = Group(
            self.task_elapsed,
            self.task_name,
            self.task_status,
            self.task_command,
            self.task_cpu_usage,
            self.task_ram_usage
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

        self.total_elapsed = self._timer_element()
        self.total_elapsed_id = self.total_elapsed.add_task("R", start=True)

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

    def enable(self, style="fancy"):
        self.set_display_style(style)
        def destructor():
            self.live_display.__exit__(None, None, None)
        atexit.register(destructor)
        self.live_display.__enter__()

    def disable(self):
        self.live_display.__exit__(None, None, None)

display = Display()

# this is "" for the main process, ".0", ".1", for subprocesses, ".0.0" for nested subprocesses
_subprocess_identifier = ""

def get_subprocess_id():
    return _subprocess_identifier

def add_subprocess_id(num: int):
    global _subprocess_identifier
    _subprocess_identifier += f".{num}"

_start_time = datetime.now()
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
    display.task_elapsed.reset(display.task_elapsed_id)
    try:
        yield _CommandContext(command)
    finally:
        _task_stack[-1].command = None
        update_process_status()


@dataclass
class TaskStatsDatum(object):
    cpu: float          = 0
    mem_used: float     = 0
    mem_total: float    = 0
    load: float         = 0
    read_count: int     = 0
    read_gb: float      = 0
    read_gbps: float    = 0
    read_ms: float      = 0
    write_count: int    = 0
    write_gb: float     = 0
    write_gbps: float   = 0
    write_ms: float     = 0
    num_samples: int    = 0
    sys_cpu: float      = 0

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
    ti = _task_stack[-1] if _task_stack else None

    # elapsed time since start
    now = datetime.now()

    # form up sample datum
    s = TaskStatsDatum(num_samples=1)

    # System wide cpu and RAM.
    ncpu = psutil.cpu_count()
    s.sys_cpu = psutil.cpu_percent()
    sys_mem = psutil.virtual_memory()

    update_children()
    # Assume that all child processes belong to the same task.
    # TODO(JSKenyon): Handling of children is rudimentary at present.
    # How would this work for scattered/parallel steps?
    if child_processes and ti:
        processes = list(child_processes.values())
    else:
        processes = []  # Don't bother with cpu and mem for stimela itself.

    # CPU and memory
    for p in processes:
        try:
            s.cpu += p.cpu_percent()
            s.mem_used += p.memory_info().rss
        except psutil.NoSuchProcess:
            pass  # Process ended before we could gather its stats.

    s.mem_used = round(s.mem_used  / (2 ** 30))
    s.mem_total = round(sys_mem.total / (2 ** 30))

    # load
    s.load, load_5m, load_15m = psutil.getloadavg()

    # get disk I/O stats
    disk_io = psutil.disk_io_counters()
    global _prev_disk_io
    prev_io, prev_time = _prev_disk_io
    if prev_io is not None:
        delta = (now - prev_time).total_seconds()
        io = {}
        for key in 'read_bytes', 'read_count', 'read_time', 'write_bytes', 'write_count', 'write_time':
            io[key] = getattr(disk_io, key) - getattr(prev_io, key)
        s.read_count = io['read_count']
        s.write_count = io['write_count']
        s.read_gb = io['read_bytes']/2**30
        s.write_gb = io['write_bytes']/2**30
        s.read_gbps = s.read_gb / delta
        s.write_gbps = s.write_gb / delta
        s.read_ms = io['read_time']
        s.write_ms = io['write_time']
    else:
        io = None
    _prev_disk_io = disk_io, now

    # call extra status reporter
    # TODO(JSKenyon): I have broken this code while updating taskstats.py to
    # use a live display. This will need to be fixed at some point, ideally
    # when we have access to a kubenetes cluster.
    if ti and ti.status_reporter:
        extra_metrics, extra_stats = ti.status_reporter()
        if extra_stats:
            s.insert_extra_stats(**extra_stats)
    else:
        extra_metrics = None

    if not any(t.hide_local_metrics for t in _task_stack):

        display.cpu_usage.update(
            display.cpu_usage_id,
            value=f"[green]{s.sys_cpu}[/green]%"
        )

        used = round(sys_mem.used / 2 ** 30)
        total = round(sys_mem.total / 2 ** 30)
        percent = (used / total) * 100

        display.ram_usage.update(
            display.ram_usage_id,
            value=(
                f"[green]{used}/{total}[/green]GB "
                f"([green]{percent:.2f}[/green]%)"
            )
        )

        display.system_load.update(
            display.system_load_id,
            value=(
                f"[green]{s.load/ncpu * 100:.2f}[/green]%/"
                f"[green]{load_5m/ncpu * 100:.2f}[/green]%/"
                f"[green]{load_15m/ncpu * 100:.2f}[/green]% "
                f"(1/5/15 min)"
            )
        )

        if io is not None:

            display.disk_read.update(
                display.disk_read_id,
                value=(
                    f"[green]{s.read_gbps:2.2f}[/green]GB/s "
                    f"[green]{s.read_ms:4}[/green]ms "
                    f"[green]{s.read_count:-4}[/green] reads"
                )
            )
            display.disk_write.update(
                display.disk_write_id,
                value=(
                    f"[green]{s.write_gbps:2.2f}[/green]GB/s "
                    f"[green]{s.write_ms:4}[/green]ms "
                    f"[green]{s.write_count:-4}[/green] writes"
                )
            )

        if ti is not None:
            display.task_name.update(
                display.task_name_id,
                value=f"[bold]{ti.description}[/bold]"
            )

            display.task_status.update(
                display.task_status_id,
                value=f"[dim]{ti.status or 'N/A'}[/dim]"
            )
            # Sometimes the command contains square brackets which rich
            # interprets as formatting. Remove them. # TODO: Figure out
            # why the command has square brackets in the first place.
            display.task_command.update(
                display.task_command_id,
                value=f"{(ti.command or 'N/A').strip('[]')}"
            )

        display.task_cpu_usage.update(
            display.task_cpu_usage_id,
            value=f"[green]{s.cpu:2.1f}[/green]%"
        )

        display.task_ram_usage.update(
            display.task_ram_usage_id,
            value=f"[green]{s.mem_used}[/green]GB"
        )

    # update stats
    update_stats(now, s)


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
    load="Load",
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
