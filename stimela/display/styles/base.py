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
        # Create a new run timer which shares a start time with the global
        # run timer. This means each display can mutate their version of the
        # run timer without affecting the other display types.
        run_start_time = run_timer.tasks[0].start_time
        self.run_elapsed = timer_element()
        self.run_elapsed_id = self.run_elapsed.add_task("", start=False)
        self.run_elapsed.tasks[self.run_elapsed_id].start_time = run_start_time
        self.run_elapsed.start_task(self.run_elapsed_id)

        self.task_elapsed = timer_element()
        self.task_elapsed_id = self.task_elapsed.add_task("")

    def reset(self):
        """Reset elements of the display e.g. time elapsed in a task."""
        self.task_elapsed.reset(self.task_elapsed_id)
