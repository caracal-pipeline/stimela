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
from rich.control import Control

from stimela.stimelogging import rich_console
from stimela.display.styles import (
    SimpleLocalDisplay,
    LocalDisplay,
    SimpleKubeDisplay,
    KubeDisplay,
    SimpleSlurmDisplay
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
        run_elapsed:
            A rich progress object which tracks total elapsed time.
        run_elapsed_id:
            The task id associated with run_elapsed.
        style_map:
            Mapping from (name, variant) to DisplayStyle.
        console:
            A rich Console object with which the display is associated.
        variant_override:
            An override which supersedes the variant specified in
            set_display_style.
        current_display:
            The currently active DisplayStyle object.
        live_display:
            A rich live display which can be rendered to the console.
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

    style_map = {
        ("local", "simple"): SimpleLocalDisplay,
        ("local", "fancy"): LocalDisplay,
        ("kube", "simple"): SimpleKubeDisplay,
        ("kube", "fancy"): KubeDisplay,
        ("slurm", "simple"): SimpleSlurmDisplay,
        ("slurm", "fancy"): SimpleSlurmDisplay  # No fancy variant as yet.
    }

    def __init__(self, console: Console):
        """Initializes an instance of the Display object.

        Args:
            console: A rich console to which the display will render.
        """

        self.console = console
        self.variant_override = None
        self.current_display = None

        msg = Text("DISPLAY HAS NOT BEEN CONFIGURED", justify="center")
        msg.stylize("bold red")

        self.live_display = Live(
            msg,
            refresh_per_second=5,
            console=self.console,
            transient=True
        )

        # Configure a simple, local display as the default.
        self.set_display_style(variant="simple")

    def reset_current_task(self):
        """Calls the reset method of the current DisplayStyle object."""
        self.current_display.reset()

    def enable(self):
        """Start rendering the live display."""
        atexit.register(self.disable)
        self.live_display.start(True)

    def disable(self, reset_cursor=False):
        """Stop rendering the live display.

        Disabling the display will leave the cursor above the bottom of the
        terminal for multiline displays. Re-enabling the display will then
        result in it being rendered higher up. The cursor can be reset to
        avoid this problem.

        Args:
            reset_cursor:
                Determines whether the cursor will be reset to the bottom
                left corner of the terminal.
        """
        self.live_display.stop()

        if reset_cursor:
            self.console.control(Control.move_to(0, self.console.height - 1))

    @property
    def is_enabled(self):
        return self.live_display.is_started

    def set_display_style(self, style: str = "local", variant: str = "fancy"):
        """Reconfigures the display style based on the provided string.

        Args:
            style:
                Specifies which display style should be applied. Current
                options are 'local', 'slurm' and 'kube'.
            variant:
                Specifies which variant of the display to use. May be ignored
                if variant_override is set on this object. Current options are
                'simple' and 'fancy'.
        """
        # If the variant override has been set, prefer it over the input.
        variant = self.variant_override or variant

        new_display_type = self.style_map.get((style, variant))

        if new_display_type is None:
            raise ValueError(
                f"Unrecognised style ({style}) or variant ({variant}) when "
                f"configuring display."
            )

        if isinstance(self.current_display, new_display_type):
            return  # Already configured.

        self.current_display = new_display_type(self.run_elapsed)
        self.live_display.update(self.current_display.render_target)

    def set_variant_override(self, override: Optional[str] = None):
        """Configures a global override for the display variant."""
        self.variant_override = override

    def update(
        self,
        sys_stats: SystemStatsDatum,
        task_stats: TaskStatsDatum,
        task_info: TaskInformation,
        extra_info: Optional[object] = None
    ):
        """Calls the update method on current_display.

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
        return self.current_display.update(
            sys_stats,
            task_stats,
            task_info,
            extra_info
        )

display = Display(rich_console)
