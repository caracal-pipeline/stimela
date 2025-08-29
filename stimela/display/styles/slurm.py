from __future__ import annotations

from typing import TYPE_CHECKING

from rich.progress import Progress, SpinnerColumn, TimeElapsedColumn

from stimela.monitoring.slurm import SlurmReport

from .base import DisplayStyle

if TYPE_CHECKING:
    from stimela.task_stats import TaskInformation


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

        columns = []
        columns.append(SpinnerColumn())
        columns.append("[yellow][bold]R[/bold][/yellow]")
        columns.append("[yellow]{task.fields[elapsed]}[/yellow]")
        columns.append(SpinnerColumn())
        columns.append("[yellow][bold]S[/bold][/yellow]")
        columns.append(TimeElapsedColumn())
        columns.append("[bold]{task.fields[name]}[/bold]")
        columns.append("[dim]{task.fields[status]}[/dim]")
        columns.append("{task.fields[command]}")

        self.progress = Progress(*columns, auto_refresh=False, transient=True)
        self.progress_id = self.progress.add_task("", start=True)

        self.render_target = self.progress

    def update(
        self,
        task_info: TaskInformation,
        report: SlurmReport,
    ):
        """Updates the progress elements using the provided values.

        Args:
            task_info:
                An object containing information about the current task.
            report:
                A Report object containing resource monitoring.
        """
        if task_info is not None:
            self.progress.update(
                self.progress_id,
                elapsed=0,
                name=task_info.description,
                status=task_info.status or "running",
                command=(task_info.command or "--").strip("([])"),
            )
