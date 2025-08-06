from __future__ import annotations
from typing import Optional, List, TYPE_CHECKING
from collections import defaultdict

from rich.console import Group
from rich.panel import Panel
from rich.table import Table, Column

from .base import DisplayStyle
from .elements import timer_element, status_element

if TYPE_CHECKING:
    from stimela.task_stats import (
        TaskInformation,
        TaskStatsDatum,
        SystemStatsDatum
    )

class FancyDisplay(DisplayStyle):

    tracked_values = {
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
        "task_peak_cpu_usage": "Peak"
    }

    def __init__(self, run_timer):
        """Configures the display in simple mode."""

        super().__init__(run_timer)

        self.task_maxima = defaultdict(float)

        width = max(len(fn) for fn in self.tracked_values.values() if fn)

        # Set the width of the text column in the recipe timer.
        self.run_elapsed.columns[1].get_table_column().width = width - 1
        # Set the description recipe timer task.
        self.run_elapsed.update(self.run_elapsed_id, description="")

        self.task_elapsed = timer_element(width=width - 1)
        self.task_elapsed_id = self.task_elapsed.add_task("")

        for k, v in self.tracked_values.items():
            status = status_element(width=width)
            status_id = status.add_task(v, value="Pending...")
            setattr(self, k, status)
            setattr(self, f"{k}_id", status_id)

        ram_columns = [Column(ratio=1), Column(ratio=1)]
        ram_table = Table.grid(*ram_columns, expand=True, padding=(0,1))
        ram_table.add_row(self.task_ram_usage, self.task_peak_ram_usage)

        cpu_columns = [Column(ratio=1), Column(ratio=1)]
        cpu_table = Table.grid(*cpu_columns, expand=True, padding=(0,1))
        cpu_table.add_row(self.task_cpu_usage, self.task_peak_cpu_usage)

        task_group = Group(
            self.task_elapsed,
            self.task_name,
            self.task_status,
            self.task_command,
            cpu_table,
            ram_table
        )

        system_group = Group(
            self.run_elapsed,
            self.cpu_usage,
            self.ram_usage,
            self.disk_read,
            self.disk_write,
            self.system_load
        )

        group_columns = [Column(ratio=1), Column(ratio=1)]
        table = Table.grid(*group_columns, expand=True)
        table.add_row(
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
        )

        self.render_target = table

    def reset(self):
        super().reset()
        self.task_maxima.clear()

    def update(
        self,
        sys_stats: SystemStatsDatum,
        task_stats: TaskStatsDatum,
        task_info: TaskInformation,
        extra_info: Optional[List[str]] = None
    ):
        """Updates the progress elements using the provided values.

        Args:
            sys_stats:
                An object containing the current system status.
            task_stats:
                An object containing the current task stats.
            task_info:
                An object containing information about the current task.
            extra_info:
                A list of strings which will be combined and added to the
                extra_info field. Typically used for non-standard information
                originating from a backend.
        """
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
