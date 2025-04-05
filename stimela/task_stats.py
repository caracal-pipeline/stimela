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
from rich.table import Table
from rich.text import Text

from stimela import stimelogging

# this is "" for the main process, ".0", ".1", for subprocesses, ".0.0" for nested subprocesses
_subprocess_identifier = ""

def get_subprocess_id():
    return _subprocess_identifier

def add_subprocess_id(num: int):
    global _subprocess_identifier
    _subprocess_identifier += f".{num}"

progress_bar = progress_task = None

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

def init_progress_bar(boring=False):
    global progress_console, progress_bar, progress_task
    progress_console = rich.console.Console(file=sys.stdout, highlight=False, emoji=False)
    progress_bar = rich.progress.Progress(
        rich.progress.SpinnerColumn(),
        "[yellow]{task.fields[elapsed_time]}[/yellow]",
        "[bold]{task.description}[/bold]",
        rich.progress.SpinnerColumn(),
        "[dim]{task.fields[status]}[/dim]",
        "{task.fields[command]}",
        rich.progress.TimeElapsedColumn(),
        "{task.fields[cpu_info]}",
        refresh_per_second=2,
        console=progress_console,
        transient=True,
        disable=boring)

    progress_task = progress_bar.add_task("stimela", status="", command="starting", cpu_info=" ", elapsed_time="", start=True)
    progress_bar.__enter__()
    atexit.register(destroy_progress_bar)
    return progress_bar, progress_console

def destroy_progress_bar():
    global progress_bar
    if progress_bar is not None:
        progress_bar.__exit__(None, None, None)
        progress_bar = None

def restate_progress():
    """Renders a snapshot of the progress bar onto the console"""
    if progress_bar is not None:
        progress_console.print(progress_bar.get_renderable())
        progress_console.rule()


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
    progress_bar and progress_bar.reset(progress_task)
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


def update_process_status():
    # current subtask info
    ti = _task_stack[-1] if _task_stack else None

    # elapsed time since start
    now = datetime.now()
    elapsed = str(now - _start_time).split('.', 1)[0]

    # form up sample datum
    s = TaskStatsDatum(num_samples=1)
    # CPU and memory
    s.cpu = psutil.cpu_percent()
    mem = psutil.virtual_memory()
    s.mem_used = round(mem.total*mem.percent/100 / 2**30)
    s.mem_total = round(mem.total / 2**30)
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
    if ti and ti.status_reporter:
        extra_metrics, extra_stats = ti.status_reporter()
        if extra_stats:
            s.insert_extra_stats(**extra_stats)
    else:
        extra_metrics = None

    # if a progress bar exists, update it
    if progress_bar is not None:
        cpu_info = []
        # add local metering, if not diabled by a task in the stack
        if not any(t.hide_local_metrics for t in _task_stack):
            cpu_info = [
                f"CPU [green]{s.cpu:2.1f}%[/green]",
                f"RAM [green]{round(s.mem_used):3}[/green]/[green]{round(s.mem_total)}[/green]G",
                f"Load [green]{s.load:2.1f}[/green]" 
            ]

            if io is not None:
                cpu_info += [
                    f"R [green]{s.read_count:-4}[/green] [green]{s.read_gbps:2.2f}[/green]G [green]{s.read_ms:4}[/green]ms",
                    f"W [green]{s.write_count:-4}[/green] [green]{s.write_gbps:2.2f}[/green]G [green]{s.write_ms:4}[/green]ms "
                ]
        # add extra metering
        cpu_info += extra_metrics or []

        updates = dict(elapsed_time=elapsed, cpu_info="|".join(cpu_info))

        if ti is not None:
            updates['description'] = ti.description
            updates['status'] = ti.status or ''
            updates['command'] = ti.command or ''

        progress_bar.update(progress_task, **updates)

    # update stats
    update_stats(now, s)


async def run_process_status_update():
    if progress_bar:
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

    for name, (elapsed, sum, peak) in stats.items():
        if name and len(name) <= max_depth:
            # skip loop iterations, if not unrolling loops 
            if not unroll_loops and any(n.endswith("]") for n in name):
                continue
            secs, mins, hours = elapsed % 60, int(elapsed // 60) % 60, int(elapsed // 3600)
            tstr = f"{hours:d}:{mins:02d}:{secs:04.1f}"
            avg = sum.averaged()
            avg_row = [".".join(name), tstr]
            peak_row = avg_row.copy()
            for f, label in _printed_stats.items():
                if f in available_stats:
                    avg_row.append(f"{getattr(avg, f):.2f}" if hasattr(avg, f) else "")
                    peak_row.append(f"{getattr(peak, f):.2f}" if hasattr(peak, f) else "")

            avg_row += [f"{sum.read_gb:.2f}", f"{sum.write_gb:.2f}"]
            table_avg.add_row(*avg_row)
            table_peak.add_row(*peak_row)

    stimelogging.declare_chapter("profiling results")
    destroy_progress_bar()
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
