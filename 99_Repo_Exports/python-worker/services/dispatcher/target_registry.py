import os
from typing import Optional

class TargetRegistry:
    """
    Centralized registry for resolving target logical names to physical delivery configurations.
    Eliminates magic ENV strings from the Outbox Dispatcher hot path.
    """
    
    @staticmethod
    def get_task_stream(target_name: str) -> str:
        """Returns the Redis Stream name for queueing tasks to a Target Worker"""
        return os.getenv(f"SIGNAL_TASKS_STREAM_{target_name.upper()}", f"stream:signals:tasks:{target_name}")

    @staticmethod
    def get_http_url(target_name: str) -> Optional[str]:
        """Returns the HTTP URL for a given target, if configured."""
        return os.getenv(f"SIGNAL_TARGET_URL_{target_name.upper()}")

    @staticmethod
    def get_http_timeout(target_name: str, default: float = 10.0) -> float:
        """Returns the HTTP timeout for the target."""
        val = os.getenv(f"SIGNAL_TARGET_TIMEOUT_{target_name.upper()}")
        if not val:
            return default
        try:
            return float(val)
        except ValueError:
            return default
