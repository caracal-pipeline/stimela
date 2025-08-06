from abc import ABC, abstractmethod
from rich.progress import Progress
from .elements import timer_element


class BaseDisplayStyle(ABC):
    """Abstract base class for DisplayStyle objects.

    This is the abstract base class for a number of display styles which can be
    rendered to the active console.
    """
    @abstractmethod
    def reset(self):
        pass

    @abstractmethod
    def update(self):
        pass


class DisplayStyle(BaseDisplayStyle):
    """Styles a rich live display.

    This is the base class for a number of display styles which can be
    rendered to the active console.

    Attributes:
        run_elapsed:
            A rich.Progress object tracking total time elapsed.
        run_elapsed_id:
            The task ID of the task associcated with run_elapsed.
        task_elapsed:
            A rich.Progress object tracking time elapsed in the current task.
        task_elapsed_id:
            The task ID of the task associcated with task_elapsed.
    """
    def __init__(self, run_timer: Progress):
        """Instantiates a DisplayStyle using the run_timer.

        Args:
            run_timer:
                rich.Progress object tracking total run time.
        """
        self.run_elapsed = run_timer
        self.run_elapsed_id = 0  # We only ever add a single task.

        # NOTE(JSKenyon): Ideally, we would copy the timer Progress object but
        # it is a complex object which includes a lock. This code instead
        # 'resets' the properties of the progress object which we mutute in
        # the subclasses.
        self.run_elapsed.update(self.run_elapsed_id, description="")
        self.run_elapsed.columns[1].get_table_column().width = None

        self.task_elapsed = timer_element()
        self.task_elapsed_id = self.task_elapsed.add_task("")

    def reset(self):
        """Reset elements of the display e.g. time elapsed in a task."""
        self.task_elapsed.reset(self.task_elapsed_id)
