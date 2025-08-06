from copy import copy
from abc import ABC, abstractmethod
from rich.progress import Progress
from .elements import timer_element


class DisplayStyle(ABC):

    def __init__(self, recipe_timer: Progress):
        self.total_elapsed = copy(recipe_timer)
        self.total_elapsed_id = 0  # We only ever add a single task.
        self.task_elapsed = timer_element()
        self.task_elapsed_id = self.task_elapsed.add_task("")

    def reset(self):
        self.task_elapsed.reset(self.task_elapsed_id)

    @abstractmethod
    def update(self):
        pass
