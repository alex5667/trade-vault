import asyncio
import logging
import time
from collections.abc import Coroutine, Awaitable
import contextlib
from typing import Any

logger = logging.getLogger("task_manager")

try:
    from services.orderflow.metrics import task_drop_total, task_error_total
    _METRICS_AVAILABLE = True
except Exception:
    task_drop_total = task_error_total = None
    _METRICS_AVAILABLE = False

class BackgroundTaskManager:
    """
    Manages background fire-and-forget asyncio tasks.
    Prevents unbounded memory growth by limiting concurrent background tasks.
    Holds a strong reference to tasks to prevent garbage collection in Python 3.7+.
    """
    def __init__(self, limit: int = 10000):
        self.limit = limit
        self._tasks: set[asyncio.Task] = set()
        self._dropped_count = 0
        self._last_log_ts = 0

    def _task_done_callback(self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        try:
            exc = task.exception()
            if exc:
                import redis
                name = task.get_name() or ""
                name_prefix = name[:32]
                if isinstance(exc, (redis.exceptions.TimeoutError, redis.exceptions.ConnectionError, asyncio.TimeoutError)):
                    coro_name = getattr(task.get_coro(), '__qualname__', str(task.get_coro()))
                    logger.warning(f"Background task '{name}' ({coro_name}) failed with Redis timeout/connection error: {exc}")
                    if task_error_total:
                        with contextlib.suppress(Exception):
                            task_error_total.labels(name_prefix=name_prefix, exc_type=type(exc).__name__).inc()
                elif isinstance(exc, TypeError) and "'NoneType' object is not callable" in str(exc):
                    coro_name = getattr(task.get_coro(), '__qualname__', str(task.get_coro()))
                    logger.warning(f"Background task '{name}' ({coro_name}) failed with asyncio closed transport error (TypeError): {exc}")
                    if task_error_total:
                        with contextlib.suppress(Exception):
                            task_error_total.labels(name_prefix=name_prefix, exc_type="ClosedTransportError").inc()
                elif not isinstance(exc, asyncio.CancelledError):
                    logger.error(f"Background task '{name}' raised an exception: {exc}", exc_info=exc)
                    if task_error_total:
                        with contextlib.suppress(Exception):
                            task_error_total.labels(name_prefix=name_prefix, exc_type=type(exc).__name__).inc()
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    def submit(self, coro: Coroutine | Awaitable | Any, name: str | None = None) -> asyncio.Task | None:
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
            if task_drop_total:
                try:
                    name_prefix = (name or "")[:32]
                    task_drop_total.labels(name_prefix=name_prefix).inc()
                except Exception:
                    pass
            if hasattr(coro, "close"):
                coro.close()
            return None

        # Schedule execution
        task = asyncio.create_task(coro, name=name)

        # Keep a strong reference
        self._tasks.add(task)

        # Remove reference when done and retrieve exceptions safely
        task.add_done_callback(self._task_done_callback)
        return task

# Singleton instance to be used across the worker
background_tasks = BackgroundTaskManager(limit=10000)

def safe_create_task(coro: Coroutine | Awaitable | Any, name: str | None = None) -> asyncio.Task | None:
    """
    A drop-in replacement for asyncio.create_task for fire-and-forget operations.
    Bounds maximum concurrent tasks to avoid OOM issues.
    """
    return background_tasks.submit(coro, name=name)
