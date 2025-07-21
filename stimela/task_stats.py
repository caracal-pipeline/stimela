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

import rich.progress
import rich.logging
from rich.console import Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from stimela import stimelogging


progress_console = rich.console.Console(
    file=sys.stdout,
    highlight=False,
    emoji=False
)

total_elapsed = rich.progress.Progress(
    rich.progress.SpinnerColumn(),
    f"[yellow][bold]{'Elapsed':<10}[/bold][/yellow]",
    rich.progress.TimeElapsedColumn(),
    refresh_per_second=2,
    console=progress_console,
    transient=True
)
total_elapsed_task = total_elapsed.add_task("Run Time")

sys_usage = rich.progress.Progress(
    "[bold]{task.description:<12} {task.fields[resource]} [/bold]",
    refresh_per_second=2,
    console=progress_console,
    transient=True
)

cpu_usage_task = sys_usage.add_task("CPU", resource="Pending...")
ram_usage_task = sys_usage.add_task("RAM", resource="Pending...")
disk_read_task = sys_usage.add_task("Disk Read", resource="Pending...")
disk_write_task = sys_usage.add_task("Disk Write", resource="Pending...")

task_usage = rich.progress.Progress(
    "[bold]{task.description:<12} {task.fields[resource]} [/bold]",
    refresh_per_second=2,
    console=progress_console,
    transient=True
)

task_name_task = task_usage.add_task("Name", resource="Pending...")
task_status_task = task_usage.add_task("Status", resource="Pending...")
task_command_task = task_usage.add_task("Command", resource="Pending...")
task_cpu_usage_task = task_usage.add_task("CPU", resource="Pending...")
task_ram_usage_task = task_usage.add_task("RAM", resource="Pending...")

task_elapsed = rich.progress.Progress(
    rich.progress.SpinnerColumn(),
    f"[yellow][bold]{'Elapsed':<10}[/bold][/yellow]",
    rich.progress.TimeElapsedColumn(),
    refresh_per_second=2,
    console=progress_console,
    transient=True
)
task_elapsed_task = task_elapsed.add_task("Run Time")

task_state = Group(task_elapsed, task_usage)
system_state = Group(total_elapsed, sys_usage)

progress_table = Table.grid(expand=True)
progress_table.add_column()
progress_table.add_column(ratio=1)
progress_table.add_column(ratio=1)
progress_table.add_column()
progress_table.add_row(
    " ",  # Spacer.
    Panel(
        task_state,
        title="Task",
        border_style="green",
        expand=True
    ),
    Panel(
        system_state,
        title="System",
        border_style="green",
        expand=True,
        padding=(0,1,1,1)  # (T, R, B, L) - defaults are (R, L) = (1, 1).
    ),
    " "  # Spacer.
)

live_display = Live(
    progress_table,
    refresh_per_second=5,
    console=progress_console,
    transient=True
)

def enable_progress_display():
    def destructor():
        live_display.__exit__(None, None, None)
    atexit.register(destructor)
    live_display.__enter__()

def disable_progress_display():
    live_display.__exit__(None, None, None)

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
    task_elapsed.reset(task_elapsed_task)
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
    elapsed = str(now - _start_time).split('.', 1)[0]

    # form up sample datum
    s = TaskStatsDatum(num_samples=1)

    # System wide cpu and RAM.
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
    s.load, _, _ = psutil.getloadavg()

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

    if not sys_usage.disable:
        if not any(t.hide_local_metrics for t in _task_stack):
            sys_usage.update(
                cpu_usage_task,
                resource=f"[green]{s.sys_cpu}[/green]%"
            )

            used = round(sys_mem.used / 2 ** 30)
            total = round(sys_mem.total / 2 ** 30)
            percent = (used / total) * 100

            sys_usage.update(
                ram_usage_task,
                resource=(
                    f"[green]{used}/{total}[/green]GB "
                    f"([green]{percent:.2f}[/green]%)"
                )
            )

            if io is not None:

                sys_usage.update(
                    disk_read_task,
                    resource=(
                        f"[green]{s.read_gbps:2.2f}[/green]GB/s "
                        f"[green]{s.read_ms:4}[/green]ms "
                        f"[green]{s.read_count:-4}[/green] reads"
                    )
                )
                sys_usage.update(
                    disk_write_task,
                    resource=(
                        f"[green]{s.write_gbps:2.2f}[/green]GB/s "
                        f"[green]{s.write_ms:4}[/green]ms "
                        f"[green]{s.write_count:-4}[/green] writes"
                    )
                )

    if not task_usage.disable:
        if not any(t.hide_local_metrics for t in _task_stack):

            if ti is not None:
                task_usage.update(
                    task_name_task,
                    resource=f"[bold]{ti.description}[/bold]"
                )

                task_usage.update(
                    task_status_task,
                    resource=f"[dim]{ti.status or 'N/A'}[/dim]"
                )
                # Sometimes the command contains square brackets which rich
                # interprets as formatting. Remove them. # TODO: Figure out
                # why the command has square brackets in the first place.
                task_usage.update(
                    task_command_task,
                    resource=f"{(ti.command or 'N/A').strip('[]')}"
                )

            task_usage.update(
                task_cpu_usage_task,
                resource=f"[green]{s.cpu:2.1f}[/green]%"
            )

            task_usage.update(
                task_ram_usage_task,
                resource=f"[green]{s.mem_used}[/green]GB"
            )

    # update stats
    update_stats(now, s)


async def run_process_status_update():
    if live_display.is_started:
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
        if name_tuple and len(name_tuple) <= max_depth:
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
    disable_progress_display()
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
