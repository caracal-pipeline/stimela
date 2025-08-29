import asyncio
import contextlib
import os.path
import threading
import time
from dataclasses import dataclass, fields
from datetime import datetime
from typing import Callable, List, Optional, OrderedDict

from omegaconf import OmegaConf
from rich.table import Table
from rich.text import Text

from scabha.basetypes import EmptyListDefault
from stimela import stimelogging
from stimela.display.display import display, rich_console
from stimela.monitoring import empty_reporter

# this is "" for the main process, ".0", ".1", for subprocesses, ".0.0" for nested subprocesses
_subprocess_identifier = ""


def get_subprocess_id():
    return _subprocess_identifier


def add_subprocess_id(num: int):
    global _subprocess_identifier
    _subprocess_identifier += f".{num}"


@dataclass
class TaskInformation(object):
    names: List[str]
    status: str = ""
    task_attrs: List[str] = EmptyListDefault()
    command: Optional[str] = None
    status_reporter: Optional[Callable] = None

    def __post_init__(self):
        self.names_orig = list(self.names)

    @property
    def description(self):
        name = ".".join(self.names)
        # if self.status:
        #     name += f": [dim]{self.status}[/dim]"
        ## OMS: omit attributes from task status for now
        # if self.task_attrs:
        #     name += f"\[{', '.join(self.task_attrs)}]"
        return name


# stack of task information -- most recent subtask is at the end
_task_stack = []


@contextlib.contextmanager
def declare_subtask(subtask_name, status_reporter=empty_reporter):
    task_names = []
    if _task_stack:
        task_names = _task_stack[-1].names + (_task_stack[-1].task_attrs or [])
    task_names.append(subtask_name)
    _task_stack.append(TaskInformation(task_names, status_reporter=status_reporter))
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
    _task_stack[-1].task_attrs = [str(x) for x in args] + [f"{key} {value}" for key, value in kw.items()]
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
class TaskStatsDatum(object):
    num_samples: int = 0

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
    task_info = _task_stack[-1] if _task_stack else None

    # elapsed time since start
    now = datetime.now()

    # Call extra status reporter which should return stats which can be added
    # to the task_stats object as well as a list of strings which can be used
    # in the display. TODO(JSKenyon): All backends should produce a Report
    # object which can be used here. At present, this only applies to the
    # kube backend.
    if task_info is None:
        report = empty_reporter(now, task_info)
    elif task_info and task_info.status_reporter:
        report = task_info.status_reporter(now, task_info)
    else:
        raise ValueError("No reporter avaialable.")

    task_stats = TaskStatsDatum(num_samples=1)
    task_stats.insert_extra_stats(**report.profiling_results)

    # Update the display using the stats and info objects.
    if display.is_enabled:
        display.update(task_info, report)

    # update stats
    update_stats(now, task_stats)


async def run_process_status_update():
    with contextlib.suppress(asyncio.CancelledError):
        while True:
            update_process_status()
            await asyncio.sleep(1)


class MonitorThread:
    """Starts a thread to monitor resource usage.

    Attributes:
        thread: A thread object.
        event: An event object which can break the monitor loop.
        interval: Wait interval in seconds between updates.
    """

    def __init__(self, interval: float = 1):
        self.thread = threading.Thread(target=self._update)
        self.event = threading.Event()
        self.interval = interval

    def _update(self):
        """Code run in the thread. Updates resource usage based on interval."""
        half_interval = self.interval / 2
        # Sleep on either side to prevent rapid calls to psutil.
        while not self.event.is_set():
            time.sleep(half_interval)
            update_process_status()
            time.sleep(half_interval)

    def start(self):
        """Starts the monitor thread."""
        self.event.clear()
        self.thread.start()

    def stop(self):
        """Breaks the monitor loop and joins the thread."""
        self.event.set()
        self.thread.join()


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

    if "read_gb" in available_stats:
        table_avg.add_column("R GB", justify="right")
    if "write_gb" in available_stats:
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
                    avg_row.append(f"{getattr(avg, f):.2f}" if hasattr(avg, f) else "--")
                    peak_row.append(f"{getattr(peak, f):.2f}" if hasattr(peak, f) else "--")

            if "read_gb" in available_stats:
                avg_row.append(f"{getattr(sum, 'read_gb'):.2f}" if hasattr(sum, "read_gb") else "--")
            if "write_gb" in available_stats:
                avg_row.append(f"{getattr(sum, 'write_gb'):.2f}" if hasattr(sum, "write_gb") else "--")

            table_avg.add_row(*avg_row)
            table_peak.add_row(*peak_row)

    stimelogging.declare_chapter("profiling results")
    # Disable display - ensures that it doesn't appear below the profiling.
    display.disable()
    from rich.columns import Columns
    # rich_console.print(table_avg, justify="center")
    # rich_console.print(table_peak, justify="center")
    # rich_console.print(Columns((table_avg, table_peak)), justify="center")

    with rich_console.capture() as capture:
        rich_console.print(Columns((table_avg, table_peak)), justify="center")

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

    filename = os.path.join(stimelogging.get_logfile_dir(log) or ".", "stimela.stats.full")

    stats_dict = OmegaConf.create()

    for name, (elapsed, sum, peak) in stats.items():
        if name:
            name = ".".join(name)
            avg = sum.averaged()
            davg = {f: getattr(avg, f, "--") for f in _taskstats_sample_names}
            dpeak = {f: getattr(peak, f, "--") for f in _taskstats_sample_names}
            dsum = {f: getattr(sum, f, "--") for f in _sum_stats}

            stats_dict[name] = dict(elapsed=elapsed, avg=davg, peak=dpeak, total=dsum)

    OmegaConf.save(stats_dict, filename)

    log.info(f"saved full profiling stats to {filename}")

    filename = os.path.join(stimelogging.get_logfile_dir(log) or ".", "stimela.stats.summary.txt")
    open(filename, "wt").write(summary)

    log.info(f"saved summary to {filename}")
