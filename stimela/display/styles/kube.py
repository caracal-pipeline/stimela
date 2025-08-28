from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING

from rich.console import Group
from rich.panel import Panel
from rich.progress import Progress
from rich.table import Column, Table
from rich.text import Text

from .base import DisplayStyle
from .elements import status_element, timer_element

if TYPE_CHECKING:
    from stimela.task_stats import TaskInformation


class KubeDisplay(DisplayStyle):
    """Styles a rich live display appropriately for kube backend.

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
        "task_name": "Step",
        "task_status": "Status",
        "task_command": "Command",
        "kube_connection_status": "Status",
        "running_pods": "Running",
        "pending_pods": "Pending",
        "terminating_pods": "Terminating",
        "successful_pods": "Successful",
        "failed_pods": "Failed",
        "stateless_pods": "Stateless",
        "kube_core_usage": "CPU",
        "kube_ram_usage": "RAM",
        "kube_peak_ram_usage": "Peak",
        "kube_peak_core_usage": "Peak",
    }

    def __init__(self, run_timer: Progress):
        """Configures the display in kube fancy mode.

        Args:
            run_timer:
                Progress object which tracks total run time.
        """

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
            status_id = status.add_task(v, value="--")
            setattr(self, k, status)
            setattr(self, f"{k}_id", status_id)

        ram_columns = [Column(ratio=1), Column(ratio=1)]
        ram_table = Table.grid(*ram_columns, expand=True, padding=(0, 1))
        ram_table.add_row(self.kube_ram_usage, self.kube_peak_ram_usage)

        cpu_columns = [Column(ratio=1), Column(ratio=1)]
        cpu_table = Table.grid(*cpu_columns, expand=True, padding=(0, 1))
        cpu_table.add_row(self.kube_core_usage, self.kube_peak_core_usage)

        task_group = Group(self.task_elapsed, self.task_name, self.task_command, cpu_table, ram_table)

        pod_table_title = Text("Pods")
        pod_table_title.pad(10, "-")
        pod_table_title.stylize("bold green")

        pod_columns = [Column(ratio=1), Column(ratio=1)]
        pod_table = Table(*pod_columns, title=pod_table_title, show_header=False, expand=True, padding=(0, 0), box=None)
        pod_table.add_row(self.pending_pods, self.terminating_pods)
        pod_table.add_row(self.running_pods, self.successful_pods)
        pod_table.add_row(self.failed_pods, self.stateless_pods)

        status_columns = [Column(ratio=1), Column(ratio=1)]
        status_table = Table.grid(*status_columns, expand=True, padding=(0, 1))
        status_table.add_row(self.run_elapsed, self.kube_connection_status)

        session_group = Group(status_table, pod_table)

        group_columns = [Column(ratio=1), Column(ratio=1)]
        table = Table.grid(*group_columns, expand=True)
        table.add_row(
            Panel(session_group, title="[bold]Session[/bold]", border_style="green", expand=True),
            Panel(
                task_group,
                title="[bold]Task[bold]",
                border_style="green",
                expand=True,
            ),
        )

        self.render_target = table

    def reset(self):
        """Reset elements of the display e.g. time elapsed in a task."""
        super().reset()
        self.task_maxima.clear()

    def update(
        self,
        task_info: TaskInformation,
        report: object,
    ):
        """Updates the progress elements using the provided values.

        Args:
            task_info:
                An object containing information about the current task.
            report:
                A Report object containing resource monitoring.
        """

        if task_info is not None:
            self.task_name.update(self.task_name_id, value=f"[bold]{task_info.description}[/bold]")

            self.task_status.update(self.task_status_id, value=f"[dim]{task_info.status or 'running'}[/dim]")
            # Sometimes the command contains square brackets which rich
            # interprets as formatting. Remove them. # TODO: Figure out
            # why the command has square brackets in the first place.
            self.task_command.update(self.task_command_id, value=f"{(task_info.command or '--').strip('([])')}")

        if report:
            self.kube_connection_status.update(self.kube_connection_status_id, value=f"{report.connection_status}")

            cpu_percent = report.total_cores * 100

            self.kube_core_usage.update(self.kube_core_usage_id, value=f"[green]{cpu_percent:2.1f}[/green]%")

            max_cpu_percent = max(cpu_percent, self.task_maxima["kube_peak_core_usage"])
            self.task_maxima["kube_peak_core_usage"] = max_cpu_percent
            self.kube_peak_core_usage.update(
                self.kube_peak_core_usage_id, value=f"[green]{max_cpu_percent:2.1f}[/green]%"
            )

            self.kube_ram_usage.update(self.kube_ram_usage_id, value=f"[green]{report.total_memory}[/green]GB")

            max_ram = max(report.total_memory, self.task_maxima["kube_peak_ram_usage"])
            self.task_maxima["kube_peak_ram_usage"] = max_ram
            self.kube_peak_ram_usage.update(self.kube_peak_ram_usage_id, value=f"[green]{max_ram}[/green]GB")

            self.pending_pods.update(self.pending_pods_id, value=f"[yellow]{report.pending_pods}[/yellow]")

            self.running_pods.update(self.running_pods_id, value=f"[green]{report.running_pods}[/green]")

            self.terminating_pods.update(self.terminating_pods_id, value=f"[blue]{report.terminating_pods}[/blue]")

            self.successful_pods.update(self.successful_pods_id, value=f"[green]{report.successful_pods}[/green]")

            self.failed_pods.update(self.failed_pods_id, value=f"[red]{report.failed_pods}[/red]")

            self.stateless_pods.update(self.stateless_pods_id, value=f"[red]{report.stateless_pods}[/red]")


class SimpleKubeDisplay(DisplayStyle):
    """Styles a rich live display appropriately for kube backend.

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
        "task_command": None,
        "kube_connection_status": None,
        "running_pods": "[cyan]R[/cyan]",
        "pending_pods": "[yellow]P[/yellow]",
        "successful_pods": "[green]S[/green]",
        "failed_pods": "[red]F[/red]",
        "kube_core_usage": "CPU",
        "kube_ram_usage": "RAM",
    }

    def __init__(self, run_timer: Progress):
        """Configures the display in kube simple mode.

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
            self.task_command,
            self.pending_pods,
            "|",
            self.running_pods,
            "|",
            self.successful_pods,
            "|",
            self.failed_pods,
            self.kube_core_usage,
            self.kube_ram_usage,
            self.kube_connection_status,
        )

        self.render_target = table

    def update(
        self,
        task_info: TaskInformation,
        report: object,
    ):
        """Updates the progress elements using the provided values.

        Args:
            task_info:
                An object containing information about the current task.
            report:
                A Report object containing resource monitoring.
        """

        if task_info is not None:
            self.task_name.update(self.task_name_id, value=f"[bold]{task_info.description}[/bold]")
            # Sometimes the command contains square brackets which rich
            # interprets as formatting. Remove them. # TODO: Figure out
            # why the command has square brackets in the first place.
            self.task_command.update(self.task_command_id, value=f"{(task_info.command or '--').strip('([])')}")

        if report:
            self.kube_connection_status.update(self.kube_connection_status_id, value=f"{report.connection_status}")

            cpu_percent = report.total_cores * 100

            self.kube_core_usage.update(self.kube_core_usage_id, value=f"[green]{cpu_percent:2.1f}[/green]%")

            self.kube_ram_usage.update(self.kube_ram_usage_id, value=f"[green]{report.total_memory}[/green]GB")

            self.pending_pods.update(self.pending_pods_id, value=f"[yellow]{report.pending_pods}[/yellow]")

            self.running_pods.update(self.running_pods_id, value=f"[cyan]{report.running_pods}[/cyan]")

            self.successful_pods.update(self.successful_pods_id, value=f"[green]{report.successful_pods}[/green]")

            self.failed_pods.update(self.failed_pods_id, value=f"[red]{report.failed_pods}[/red]")
