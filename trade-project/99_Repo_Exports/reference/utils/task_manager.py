import asyncio
import logging
import time
from typing import Coroutine, Set, Optional

logger = logging.getLogger("task_manager")

class BackgroundTaskManager:
    """
    Manages background fire-and-forget asyncio tasks.
    Prevents unbounded memory growth by limiting concurrent background tasks.
    Holds a strong reference to tasks to prevent garbage collection in Python 3.7+.
    """
    def __init__(self, limit: int = 10000):
        self.limit = limit
        self._tasks: Set[asyncio.Task] = set()
        self._dropped_count = 0
        self._last_log_ts = 0

    def submit(self, coro: Coroutine, name: Optional[str] = None) -> Optional[asyncio.Task]:
        """
        Submits a coroutine as a background task. 
        If the internal queue limit is reached, the task is dropped and NOT executed.
        Returns the Task if it was scheduled, None if it was dropped.
        """
        if len(self._tasks) >= self.limit:
            self._dropped_count += 1
            now = time.time()
            if now - self._last_log_ts > 5.0:
                logger.warning(f"BackgroundTaskManager hit limit ({self.limit}). "
                               f"Dropped {self._dropped_count} tasks in the last interval.")
                self._dropped_count = 0
                self._last_log_ts = now
            # Prevent execution of the coroutine (Python will warn about unawaited coroutine)
            # To avoid the ResourceWarning, we can close it:
            coro.close()
            return None

        # Schedule execution
        task = asyncio.create_task(coro, name=name)
        
        # Keep a strong reference
        self._tasks.add(task)
        
        # Remove reference when done
        task.add_done_callback(self._tasks.discard)
        return task

# Singleton instance to be used across the worker
background_tasks = BackgroundTaskManager(limit=10000)

def safe_create_task(coro: Coroutine, name: Optional[str] = None) -> Optional[asyncio.Task]:
    """
    A drop-in replacement for asyncio.create_task for fire-and-forget operations.
    Bounds maximum concurrent tasks to avoid OOM issues.
    """
    return background_tasks.submit(coro, name=name)
