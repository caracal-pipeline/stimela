from __future__ import annotations
from typing import Optional, TYPE_CHECKING
from rich.table import Table
from rich.progress import Progress

from .base import DisplayStyle
from .elements import status_element

if TYPE_CHECKING:
    from stimela.task_stats import (
        TaskInformation,
        TaskStatsDatum,
        SystemStatsDatum
    )


class SimpleSlurmDisplay(DisplayStyle):
    """Styles a rich live display appropriately for slurm.

    In addition to the attributes below, attributes will be added for each
    key in tracked_values. A *_id attribute will also be added for each key.

    Attributes:
        tracked_values:
            A mapping from name to description for quantities present in
            this display stlye.
        run_elapsed:
            A rich.Progress object tracking total time elapsed.
        run_elapsed_id:
            The task ID of the task associcated with run_elapsed.
        task_elapsed:
            A rich.Progress object tracking time elapsed in the current task.
        task_elapsed_id:
            The task ID of the task associcated with task_elapsed.
        task_maxima:
            A dict for tracking the peak values of certain progress elements.
    """

    tracked_values = {
        "task_name": None,
        "task_status": None,
        "task_command": None,
    }

    def __init__(self, run_timer: Progress):
        """Configures the display in slurm mode.

        Args:
            run_timer:
                Progress object which tracks total run time.
        """

        super().__init__(run_timer)

        self.run_elapsed.update(self.run_elapsed_id, description="R")
        self.task_elapsed.update(self.task_elapsed_id, description="S")

        for k, v in self.tracked_values.items():
            status = status_element(has_description=v is not None)
            status_id = status.add_task(v, value="--")
            setattr(self, k, status)
            setattr(self, f"{k}_id", status_id)

        table = Table.grid(expand=False, padding=(0, 1))
        table.add_row(
            self.run_elapsed,
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
        extra_info: Optional[object] = None
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
                A Report object containing additional information. Typically
                used for information originating from a backend.
        """
        if task_info is not None:
            self.task_name.update(
                self.task_name_id,
                value=f"[bold]{task_info.description}[/bold]"
            )

            self.task_status.update(
                self.task_status_id,
                value=f"[dim]{task_info.status or '--'}[/dim]"
            )
            # Sometimes the command contains square brackets which rich
            # interprets as formatting. Remove them. # TODO: Figure out
            # why the command has square brackets in the first place.
            self.task_command.update(
                self.task_command_id,
                value=f"{(task_info.command or '--').strip('[]')}"
            )
