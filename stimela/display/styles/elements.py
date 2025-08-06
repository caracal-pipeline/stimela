from typing import Optional
from rich.progress import (
    Progress,
    SpinnerColumn,
    TimeElapsedColumn,
    TextColumn
)
from rich.table import Column


def timer_element(width: Optional[int] = None):
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
        transient=True
    )

def status_element(
    has_description: bool = True,
    width: Optional[int] = None
):
    """Return a status progress element consisting of some columns.

    Args:
        has_description:
            Determines whether description column is added.
        width:
            Column width for text column.
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
        transient=True
    )
