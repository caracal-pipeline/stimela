import logging
import threading

from stimela import task_stats
from stimela.utils.xrun_asyncio import xrun


def test_xrun_without_event_loop():
    """xrun must work in a thread without a current event loop: the main thread on
    Python >= 3.14, or any other thread on every Python version."""
    result = {}

    def target():
        try:
            with task_stats.declare_subtask("test_xrun"):
                result["status"] = xrun("true", [], log=logging.getLogger("test_xrun"), log_command=False)
        except Exception as exc:
            result["exception"] = exc

    thread = threading.Thread(target=target)
    thread.start()
    thread.join()
    assert result == {"status": 0}
