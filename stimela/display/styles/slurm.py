from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from rich.progress import Progress, SpinnerColumn, TimeElapsedColumn
from rich.text import Text

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

        # NOTE(JSKenyon): This display has diverged from the others as it has
        # been optimised for minimum CPU usage.
        self.task_elapsed = Progress(
            SpinnerColumn(),
            "[yellow][bold]R[/bold][/yellow]",
            "[yellow]{task.fields[elapsed]}[/yellow]",
            SpinnerColumn(),
            "[yellow][bold]S[/bold][/yellow]",
            TimeElapsedColumn(),
            "[bold]{task.fields[name]}[/bold]",
            "[dim]{task.fields[status]}[/dim]",
            "{task.fields[command]}",
            auto_refresh=False,
            transient=True,
        )
        self.task_elapsed_id = self.task_elapsed.add_task(
            "stimela", name="--", status="--", command="--", elapsed="00:00:00", start=True
        )

        self.render_target = self.task_elapsed

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
        updates = {}

        run_elapsed_task = self.run_elapsed.tasks[self.run_elapsed_id]
        elapsed = timedelta(seconds=int(run_elapsed_task.elapsed))
        updates["elapsed"] = Text(str(elapsed), style="progress.elapsed")

        if task_info is not None:
            updates["name"] = task_info.description
            updates["status"] = task_info.status or "running"
            updates["command"] = (task_info.command or "--").strip("([])")

        self.task_elapsed.update(self.task_elapsed_id, **updates)
