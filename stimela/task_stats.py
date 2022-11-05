import atexit
from dataclasses import dataclass, fields
import sys
import os.path
from datetime import datetime, timedelta
import contextlib
import asyncio
from typing import OrderedDict
from omegaconf import OmegaConf
import psutil
import rich.progress
import rich.logging
from rich.tree import Tree
from rich.table import Table


progress_bar = progress_task = None

_progress_task_names = []
_progress_task_names_orig = []

_start_time = datetime.now()
_prev_disk_io = None, None


def init_progress_bar():
    global progress_console, progress_bar, progress_task
    progress_console = rich.console.Console(file=sys.stdout, highlight=False)
    progress_bar = rich.progress.Progress(
        rich.progress.SpinnerColumn(),
        "[yellow]{task.fields[elapsed_time]}[/yellow]",
        "[bold]{task.description}[/bold]",
        rich.progress.SpinnerColumn(),
        "{task.fields[command]}",
        rich.progress.TimeElapsedColumn(),
        "{task.fields[cpu_info]}",
        refresh_per_second=2,
        console=progress_console,
        transient=True)

    progress_task = progress_bar.add_task("stimela", command="starting", cpu_info=" ", elapsed_time="", start=True)
    progress_bar.__enter__()
    atexit.register(destroy_progress_bar)
    return progress_bar, progress_console

def destroy_progress_bar():
    global progress_bar
    if progress_bar is not None:
        progress_bar.__exit__(None, None, None)
        progress_bar = None

@contextlib.contextmanager
def declare_subtask(subtask_name):
    _progress_task_names.append(subtask_name)
    _progress_task_names_orig.append(subtask_name)
    update_process_status(description='.'.join(_progress_task_names))
    try:
        yield subtask_name
    finally:
        _progress_task_names.pop(-1)
        _progress_task_names_orig.pop(-1)
        update_process_status(progress_task,
                              description='.'.join(_progress_task_names))


def declare_subtask_attributes(*args, **kw):
    attrs = [str(x) for x in args] + \
        [f"{key} {value}" for key, value in kw.items()]
    attrs = ', '.join(attrs)
    _progress_task_names[-1] = f"{_progress_task_names_orig[-1]}\[{attrs}]"
    update_process_status(description='.'.join(_progress_task_names))


@contextlib.contextmanager
def declare_subcommand(command):
    update_process_status(command=command)
    progress_bar and progress_bar.reset(progress_task)
    try:
        yield command
    finally:
        update_process_status(command="")


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

    def add(self, other: "TaskStatsDatum"):
        for f in _taskstats_sample_names:
            setattr(self, f, getattr(self, f) + getattr(other, f))

    def peak(self, other: "TaskStatsDatum"):
        for f in _taskstats_sample_names:
            setattr(self, f, max(getattr(self, f), getattr(other, f)))

    def averaged(self):
        avg = TaskStatsDatum(num_samples=1)
        for f in _taskstats_sample_names:
            setattr(avg, f, getattr(self, f) / self.num_samples)
        return avg


_taskstats_sample_names = [f.name for f in fields(TaskStatsDatum)]

_taskstats = OrderedDict()
_task_start_time = OrderedDict()


def collect_stats():
    """Returns dictionary of per-task stats (elapsed time, sums, peaks)"""
    return _taskstats


def add_missing_stats(stats):
    """Adds stats that wren't recorded into dictionary"""
    for key, value in stats.items():
        if key not in _taskstats:
            _taskstats[key] = value


def stats_field_names():
    return _taskstats_sample_names


def update_stats(now: datetime, sample: TaskStatsDatum):
    key1, key2 = tuple(_progress_task_names_orig), tuple(_progress_task_names)

    _, sum, peak = _taskstats.setdefault(key1, [0, TaskStatsDatum(), TaskStatsDatum()])
    sum.add(sample)
    peak.peak(sample)
    start = _task_start_time.setdefault(key1, now)
    _taskstats[key1][0] = (now - start).total_seconds()

    if key2 != key1:
        _, sum, peak = _taskstats.setdefault(key2, [0, TaskStatsDatum(), TaskStatsDatum()])
        sum.add(sample)
        peak.peak(sample)
        start = _task_start_time.setdefault(key2, now)
        _taskstats[key2][0] = (now - start).total_seconds()


def update_process_status(command=None, description=None):
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

    # if a progress bar exists, update it
    if progress_bar is not None:
        if io is not None:
            ioinfo = f"|R [green]{s.read_count:-4}[/green] [green]{s.read_gbps:2.2f}[/green]G [green]{s.read_ms:4}[/green]ms" + \
                    f"|W [green]{s.write_count:-4}[/green] [green]{s.write_gbps:2.2f}[/green]G [green]{s.write_ms:4}[/green]ms "
        else:
            ioinfo = ""

        updates = dict(elapsed_time=elapsed,
                    cpu_info=f"CPU [green]{s.cpu:2.1f}%[/green]|RAM [green]{round(s.mem_used):3}[/green]/[green]{round(s.mem_total)}[/green]G|Load [green]{s.load:2.1f}[/green]{ioinfo}")
        if command is not None:
            updates['command'] = command
        if description is not None:
            updates['description'] = description
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
    cpu="CPU %",
    mem_used="Mem GB",
    load="Load",
    read_gbps="R GB/s",
    write_gbps="W GB/s")

# these stats are written as sums
_sum_stats = ("read_count", "read_gb", "read_ms", "write_count", "write_gb", "write_ms")

def render_profiling_summary(stats):

    table_avg = Table(title="averages + total I/O")
    table_avg.add_column("")
    table_avg.add_column("time hms", justify="right")

    table_peak = Table(title="peaks")
    table_peak.add_column("")
    table_peak.add_column("time hms", justify="right")
    
    for f, label in _printed_stats.items():
        table_avg.add_column(label, justify="right")
        table_peak.add_column(label, justify="right")

    table_avg.add_column("R GB", justify="right")
    table_avg.add_column("W GB", justify="right")

    for name, (elapsed, sum, peak) in stats.items():
        if name:
            secs, mins, hours = elapsed % 60, int(elapsed // 60) % 60, int(elapsed // 3600)
            tstr = f"{hours:d}:{mins:02d}:{secs:04.1f}"
            avg = sum.averaged()
            avg_row = [".".join(name), tstr]
            peak_row = avg_row.copy()
            for f, label in _printed_stats.items():
                if f != "num_samples":
                    avg_row.append(f"{getattr(avg, f):.2f}")
                    peak_row.append(f"{getattr(peak, f):.2f}")
            avg_row += [f"{sum.read_gb:.2f}", f"{sum.write_gb:.2f}"]
            table_avg.add_row(*avg_row)
            table_peak.add_row(*peak_row)

    progress_console.rule("profiling results")
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

def save_profiling_stats(log, print_stats=True):
    from . import stimelogging
    
    stats = collect_stats()
    summary = render_profiling_summary(stats)
    if print_stats:
        print(summary)

    filename = os.path.join(stimelogging.get_logger_file(log) or '.', "stimela.stats.full")

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

    filename = os.path.join(stimelogging.get_logger_file(log) or '.', "stimela.stats.summary.txt")
    open(filename, "wt").write(summary)

    log.info(f"saved summary to {filename}")
