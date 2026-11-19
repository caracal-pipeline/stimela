from dataclasses import dataclass

import psutil

_prev_disk_io = None, None
_child_processes = {}

# pidfiles of processes to be tracked (along with their descendants) in addition to our own descendants. Some
# container runtimes (podman) run the workload under a daemonized monitor process (conmon) that is reparented
# away from Stimela, so it would otherwise not be seen.
_external_pidfiles = set()


def register_external_pidfile(path: str):
    """Starts tracking the process whose PID is (or will be) written to the given file, and its descendants"""
    _external_pidfiles.add(path)


def unregister_external_pidfile(path: str):
    """Stops tracking the process given by register_external_pidfile()"""
    _external_pidfiles.discard(path)


def _external_processes():
    """Returns list of processes given by registered pidfiles, plus their descendants"""
    processes = []
    for path in list(_external_pidfiles):
        try:
            with open(path) as f:
                proc = psutil.Process(int(f.read().strip()))
            processes.append(proc)
            processes += proc.children(recursive=True)
        except (OSError, ValueError, psutil.Error):
            continue  # pidfile not yet written, or process gone
    return processes


def update_children():
    """Update the module level dictionary mapping child pid to process.

    This is necessary as calling Process.children will return different Process objects each time.
    These then fail to report CPU stats unless we make them block which has a large impact on
    performance.
    """
    current_children = psutil.Process().children(recursive=True) + _external_processes()
    current_pids = {proc.pid for proc in current_children}
    _child_processes.update({c.pid: c for c in current_children if c.pid not in _child_processes})
    dropped_pids = {c for c in _child_processes.keys() if c not in current_pids}

    for pid in dropped_pids:
        del _child_processes[pid]


@dataclass
class LocalReport:
    cpu: float = 0
    mem_used: float = 0
    load_1m: float = 0
    load_5m: float = 0
    load_15m: float = 0
    read_count: int = 0
    read_gb: float = 0
    read_gbps: float = 0
    read_ms: float = 0
    write_count: int = 0
    write_gb: float = 0
    write_gbps: float = 0
    write_ms: float = 0
    sys_n_cpu: int = 0
    sys_cpu: float = 0
    sys_mem_used: int = 0
    sys_mem_total: int = 0

    def __post_init__(self):
        self.sys_n_cpu = psutil.cpu_count()
        self.sys_cpu = psutil.cpu_percent()
        self.sys_mem_used = round(psutil.virtual_memory().used / (2**30))
        self.sys_mem_total = round(psutil.virtual_memory().total / (2**30))

    @property
    def profiling_results(self):
        return vars(self)


def local_reporter(now, task_info):
    # form up sample datum
    local_stats = LocalReport()

    # Track the child processes (and retain their Process objects).
    update_children()

    if _child_processes and task_info:
        processes = list(_child_processes.values())
    else:
        processes = []  # Don't bother with cpu and mem for stimela itself.

    # CPU and memory
    for p in processes:
        try:
            local_stats.cpu += p.cpu_percent()
            local_stats.mem_used += p.memory_info().rss
        except psutil.NoSuchProcess:
            pass  # Process ended before we could gather its stats.

    local_stats.mem_used = round(local_stats.mem_used / (2**30))

    # load
    load = [la / local_stats.sys_n_cpu * 100 for la in psutil.getloadavg()]
    local_stats.load_1m, local_stats.load_5m, local_stats.load_15m = load

    # get disk I/O stats
    disk_io = psutil.disk_io_counters()
    global _prev_disk_io
    prev_io, prev_time = _prev_disk_io
    if prev_io is not None:
        delta = (now - prev_time).total_seconds()
        io = {}
        io_fields = ("read_bytes", "read_count", "read_time", "write_bytes", "write_count", "write_time")
        for key in io_fields:
            io[key] = getattr(disk_io, key) - getattr(prev_io, key)
        local_stats.read_count = io["read_count"]
        local_stats.write_count = io["write_count"]
        local_stats.read_gb = io["read_bytes"] / 2**30
        local_stats.write_gb = io["write_bytes"] / 2**30
        local_stats.read_gbps = local_stats.read_gb / delta
        local_stats.write_gbps = local_stats.write_gb / delta
        local_stats.read_ms = io["read_time"]
        local_stats.write_ms = io["write_time"]
    else:
        io = None
    _prev_disk_io = disk_io, now

    return local_stats
