from __future__ import annotations
from typing import Optional, TYPE_CHECKING
from copy import copy
from rich.progress import (
    Progress,
    SpinnerColumn,
    TimeElapsedColumn,
    TextColumn
)
from rich.table import Column

if TYPE_CHECKING:
    from stimela.task_stats import (
        TaskInformation,
        TaskStatsDatum,
        SystemStatsDatum
    )


class DisplayStyle:

    def __init__(self, recipe_timer, console):
        self.console = console
        self.total_elapsed = copy(recipe_timer)
        self.total_elapsed_id = 0  # We only ever add a single task.
        self.task_elapsed = self._timer_element()
        self.task_elapsed_id = self.task_elapsed.add_task("")

    def reset(self):
        self.task_elapsed.reset(self.task_elapsed_id)

    def _timer_element(self, width: Optional[int] = None):
        """Return a timer progress element consisting of some columns.

        Args:
            width: Column width for text column.
        """
        return Progress(
            SpinnerColumn(),
            TextColumn(
                "[yellow][bold]{task.description}[/bold][/yellow]",
                table_column=Column(no_wrap=True, width=width)
            ),
            TimeElapsedColumn(),
            refresh_per_second=2,
            console=self.console,
            transient=True
        )

    def _status_element(
        self,
        has_description: bool = True,
        width: Optional[int] = None
    ):
        """Return a status progress element consisting of some columns.

        Args:
            width: Column width for text column.
        """

        columns = []

        if has_description:
            columns.append(
                TextColumn(
                    "[bold]{task.description}[/bold]",
                    table_column=Column(no_wrap=True, width=width)
                ),
            )

        columns.append(
            TextColumn(
                "[bold]{task.fields[value]}[/bold]",
                table_column=Column(no_wrap=True)
            )
        )

        return Progress(
            *columns,
            refresh_per_second=2,
            console=self.console,
            transient=True
        )
