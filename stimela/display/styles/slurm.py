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


class SlurmDisplay(DisplayStyle):

    progress_fields = {
        "task_name": None,
        "task_status": None,
        "task_command": None,
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
            self.task_command
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
