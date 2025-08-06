from __future__ import annotations
from typing import Optional, List, TYPE_CHECKING
from rich.table import Table

from .base import DisplayStyle
from .elements import status_element

if TYPE_CHECKING:
    from stimela.task_stats import (
        TaskInformation,
        TaskStatsDatum,
        SystemStatsDatum
    )


class SimpleDisplay(DisplayStyle):

    progress_fields = {
        "task_name": None,
        "task_status": None,
        "task_cpu_usage": "CPU",
        "task_ram_usage": "RAM",
        "disk_read": "R",
        "disk_write": "W",
    }

    def __init__(self, recipe_timer):
        """Configures the display in simple mode."""

        super().__init__(recipe_timer)

        self.total_elapsed.update(self.total_elapsed_id, description="R")
        self.task_elapsed.update(self.task_elapsed_id, description="S")

        for k, v in self.progress_fields.items():
            status = status_element(has_description=v is not None)
            status_id = status.add_task(v, value="Pending...")
            setattr(self, k, status)
            setattr(self, f"{k}_id", status_id)

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

        self.render_target = table

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

        self.task_cpu_usage.update(
            self.task_cpu_usage_id,
            value=f"[green]{task_stats.cpu:2.1f}[/green]%"
        )

        self.task_ram_usage.update(
            self.task_ram_usage_id,
            value=f"[green]{task_stats.mem_used}[/green]GB"
        )
