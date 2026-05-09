"""
State and lifecycle management for handlers.

Extracted from BaseOrderFlowHandler to follow Single Responsibility Principle.
Manages:
- Running state (is_running flag)
- Stop event coordination
- Thread lifecycle
- Start time tracking
"""

import threading
import time


class HandlerStateManager:
    """
    Manages lifecycle state for a handler.
    
    Responsibilities:
    - Track running state
    - Coordinate shutdown via threading.Event
    - Manage thread reference
    - Track uptime
    
    Thread-safe for concurrent access.
    """

    def __init__(self):
        """Initialize state manager with default stopped state."""
        self.is_running: bool = False
        self._stop_event: threading.Event = threading.Event()
        self._lock: threading.Lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._start_time: float = time.time()

    def start(self) -> None:
        """
        Mark handler as running.
        
        Thread-safe. Can be called multiple times (idempotent).
        """
        with self._lock:
            self.is_running = True
            self._stop_event.clear()

    def stop(self) -> None:
        """
        Signal handler to stop.
        
        Sets stop event and marks as not running.
        Thread-safe.
        """
        with self._lock:
            self.is_running = False
            self._stop_event.set()

    def is_stopped(self) -> bool:
        """
        Check if stop has been requested.
        
        Returns:
            True if stop event is set, False otherwise
        """
        return self._stop_event.is_set()

    def wait_for_stop(self, timeout: float | None = None) -> bool:
        """
        Wait for stop signal.
        
        Args:
            timeout: Maximum time to wait in seconds (None = wait forever)
            
        Returns:
            True if stop was signaled, False if timeout occurred
        """
        return self._stop_event.wait(timeout)

    def set_thread(self, thread: threading.Thread | None) -> None:
        """
        Set the handler's thread reference.
        
        Args:
            thread: Thread object or None to clear
        """
        with self._lock:
            self._thread = thread

    def get_thread(self) -> threading.Thread | None:
        """
        Get the handler's thread reference.
        
        Returns:
            Thread object or None if not set
        """
        with self._lock:
            return self._thread

    def get_uptime(self) -> float:
        """
        Get handler uptime in seconds.
        
        Returns:
            Seconds since initialization
        """
        return time.time() - self._start_time

    def reset_start_time(self) -> None:
        """Reset start time to current time (for restart scenarios)."""
        with self._lock:
            self._start_time = time.time()
