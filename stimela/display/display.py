from __future__ import annotations
import atexit
from typing import Optional, List, TYPE_CHECKING
from rich.progress import (
    Progress,
    SpinnerColumn,
    TimeElapsedColumn,
    TextColumn
)
from rich.console import Console
from rich.live import Live
from rich.table import Column
from rich.text import Text

from stimela.stimelogging import rich_console
from stimela.display.styles import (
    FancyDisplay,
    SimpleDisplay,
    KubeDisplay,
    SlurmDisplay
)

if TYPE_CHECKING:
    from stimela.task_stats import (
        TaskInformation,
        TaskStatsDatum,
        SystemStatsDatum
    )

class Display:
    """Manages a rich live display.

    This class manages and configures a rich live display object for tracking
    the progress of a Stimela recipe as well as its resource usage.

    Attributes:
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
        run_elapsed:
            A rich progress object which tracks total elapsed time.
        run_elapsed_id:
            The task id associated with run_elapsed.
    """

    run_elapsed = Progress(
        SpinnerColumn(),
        TextColumn(
            "[yellow][bold]{task.description}[/bold][/yellow]",
            table_column=Column(no_wrap=True)
        ),
        TimeElapsedColumn(),
        refresh_per_second=2,
        transient=True
    )
    run_elapsed_id = run_elapsed.add_task("", start=True)

    styles = {"fancy", "simple", "slurm", "kube"}

    def __init__(self, console: Console):
        """Initializes an instance of the Display object.

        Args:
            console: A rich console to which the display will render.
        """

        self.console = console

        self.display_style = "default"
        self.style_override = None

        msg = Text("DISPLAY HAS NOT BEEN CONFIGURED", justify="center")
        msg.stylize("bold red")

        self.live_display = Live(
            msg,
            refresh_per_second=5,
            console=self.console,
            transient=True
        )

    def reset_current_task(self):
        """Reset both the timer and the maxima for the current task."""
        self.current_display.reset()

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
            self.display_style = "fancy"
            self.current_display = FancyDisplay(self.run_elapsed)
        elif style == "simple":
            self.display_style = "simple"
            self.current_display = SimpleDisplay(self.run_elapsed)
        elif style == "kube":
            self.display_style = "kube"
            self.current_display = KubeDisplay(self.run_elapsed)
        elif style == "slurm":
            self.display_style = "slurm"
            self.current_display = SlurmDisplay(self.run_elapsed)
        else:
            raise ValueError(f"Unrecognised style: {style}")

        self.live_display.update(self.current_display.render_target)

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
        return self.current_display.update(
            sys_stats,
            task_stats,
            task_info,
            extra_info
        )

display = Display(rich_console)
