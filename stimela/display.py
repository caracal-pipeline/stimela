import atexit
from typing import Optional
from collections import defaultdict
from rich.progress import (
    Progress,
    SpinnerColumn,
    TimeElapsedColumn,
    TextColumn
)
from rich.console import Group, Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table, Column
from rich.text import Text

from stimela.stimelogging import rich_console

class Display:
    """Manages a rich live display.

    This class manages and configures a rich live display object for tracking
    the progress of a Stimela recipe as well as its resource usage.

    In addition to the attributes below, for each key in progress_fields, this
    class will add an attribute of the same name as well as a corresponding
    id attribute.

    Attributes:
        progress_fields:
            A key-vlaue mapping between resource field name and
            its description in the display.
        styles:
            Available styles for configuring the live display.
        display_style:
            The currently configured display style.
        style_override:
            An override which supersedes display_style.
        live_display:
            A rich live display which can be rendered to the console.
        task_maxima:
            Tracks the maxima of the current task's displayed values.
        total_elapsed:
            A rich progress object which tracks total elapsed time.
        total_elapsed_id:
            The task id associated with total_elapsed.
        task_elapsed:
            A rich progress object which tracks the elapsed time in a task.
        task_elapsed_id:
            The task id associated with task_elapsed.
    """

    progress_fields = {
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
        "task_peak_cpu_usage": "Peak",
    }

    styles = {"fancy", "simple", "remote"}

    def __init__(self, console: Console):
        """Initializes an instance of the Display object.

        Args:
            console: A rich console to which the display will render.
        """

        self.console = console

        self.display_style = "default"
        self.style_override = None

        self.total_elapsed = self._timer_element()
        self.total_elapsed_id = self.total_elapsed.add_task("", start=True)

        self.task_elapsed = self._timer_element()
        self.task_elapsed_id = self.task_elapsed.add_task("")

        self.task_maxima = defaultdict(float)

        for k, v in self.progress_fields.items():
            status = self._status_element()
            status_id = status.add_task(v, value="Pending...")
            setattr(self, k, status)
            setattr(self, f"{k}_id", status_id)

        msg = Text("DISPLAY HAS NOT BEEN CONFIGURED", justify="center")
        msg.stylize("bold red")

        self.live_display = Live(
            msg,
            refresh_per_second=5,
            console=self.console,
            transient=True
        )

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

    def _status_element(self, width: Optional[int] = None):
        """Return a status progress element consisting of some columns.

        Args:
            width: Column width for text column.
        """
        return Progress(
            TextColumn(
                "[bold]{task.description}[/bold]",
                table_column=Column(no_wrap=True, width=width)
            ),
            TextColumn(
                "[bold]{task.fields[value]}[/bold]",
                table_column=Column(no_wrap=True)
            ),
            refresh_per_second=2,
            console=self.console,
            transient=True
        )

    def reset_current_task(self):
        """Reset both the timer and the maxima for the current task."""
        self.task_elapsed.reset(self.task_elapsed_id)
        self.task_maxima = defaultdict(float)

    def enable(self):
        """Start rendering the live display."""
        atexit.register(self.disable)
        self.live_display.start(True)

    def disable(self):
        """Stop rendering the live display."""
        self.live_display.stop()

    @property
    def is_enabled(self):
        return self.live_display.is_started

    def set_display_style(self, style: str = "simple"):
        """Reconfigures the display style based on the provided string.

        Args:
            style:
                Specifies which display style should be applied.
        """
        if self.style_override:  # If set, ignore style argument.
            style = self.style_override

        if self.display_style == style:
            return  # Already configured.

        if style == "fancy":
            self._configure_fancy_display()
        elif style == "simple":
            self._configure_simple_display()
        elif style == "remote":
            self._configure_remote_display()
        elif style == "default":
            self.__init__()
        else:
            raise ValueError(f"Unrecognised style: {style}")

    def set_display_style_override(self, style: Optional[str] = None):
        """Sets the display style override and applies the display style.

        Args:
            style:
                Specifies which display style should be applied.
        """
        if style in self.styles or style is None:
            self.style_override = style
        else:
            raise ValueError(f"Unrecognised style: {style}")
        self.set_display_style()

    def _configure_fancy_display(self):
        """Configures the display in fancy mode."""

        self.display_style = "fancy"

        progress_fields = self.progress_fields

        width = max(len(fn) for fn in progress_fields.values() if fn)

        # Set the width of the text column in the recipe timer.
        self.total_elapsed.columns[1].get_table_column().width = width - 1
        # Set the description recipe timer task.
        self.total_elapsed.update(self.total_elapsed_id, description="")

        self.task_elapsed = self._timer_element(width=width - 1)
        self.task_elapsed_id = self.task_elapsed.add_task("")

        for k, v in progress_fields.items():
            status = self._status_element(width=width)
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
            self.total_elapsed,
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

        self.live_display.update(table)

    def _configure_simple_display(self):
        """Configures the display in simple mode."""

        self.display_style = "simple"

        progress_fields = self.progress_fields | {
            "disk_read": "R",
            "disk_write": "W",
        }

        # Set the width of the text column in the recipe timer.
        self.total_elapsed.columns[1].get_table_column().width = None
        # Set the description recipe timer task.
        self.total_elapsed.update(self.total_elapsed_id, description="R")

        self.task_elapsed = self._timer_element()
        self.task_elapsed_id = self.task_elapsed.add_task("S")

        for k, v in progress_fields.items():
            status = self._status_element()
            status_id = status.add_task(v, value="Pending...")
            setattr(self, k, status)
            setattr(self, f"{k}_id", status_id)

        # Drop the description column of name and status for brevity.
        self.task_name.columns = self.task_name.columns[1:]
        self.task_status.columns = self.task_status.columns[1:]

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

        self.live_display.update(table)

    def _configure_remote_display(self):
        """Configures the display in remote mode."""

        self.display_style = "remote"

        progress_fields = self.progress_fields

        # Set the width of the text column in the recipe timer.
        self.total_elapsed.columns[1].get_table_column().width = None
        # Set the description recipe timer task.
        self.total_elapsed.update(self.total_elapsed_id, description="R")

        self.task_elapsed = self._timer_element()
        self.task_elapsed_id = self.task_elapsed.add_task("S")

        for k, v in progress_fields.items():
            status = self._status_element()
            status_id = status.add_task(v, value="Pending...")
            setattr(self, k, status)
            setattr(self, f"{k}_id", status_id)

        # Drop the description column of name, status and command for brevity.
        self.task_name.columns = self.task_name.columns[1:]
        self.task_status.columns = self.task_status.columns[1:]
        self.task_command.columns = self.task_command.columns[1:]

        table = Table.grid(expand=False, padding=(0, 1))
        table.add_row(
            self.total_elapsed,
            self.task_elapsed,
            self.task_name,
            self.task_status,
            self.task_command
        )

        self.live_display.update(table)

    def update(self, sys_stats, task_stats, task_info):
        """Updates the progress elements using the provided values.

        Args:
            sys_stats:
                An object containing the current system status.
            task_stats:
                An object containing the current task stats.
            task_info:
                An object containing information about the current task.
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

display = Display(rich_console)
