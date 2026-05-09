"""
ErrorHandler: Centralizes error handling and metric counting.
"""
from typing import Any

from common.transient import is_transient_error


class ErrorHandler:
    def __init__(self, logger: Any, counters: dict[str, int]):
        self.logger = logger
        self.counters = counters

    def handle(
        self,
        exc: Exception,
        *,
        context: str,
        msg_id: str = "",
        ctr_transient: str = "transient_error",
        ctr_fatal: str = "fatal_error",
        log_transient: bool = False,
    ) -> bool:
        """
        Handle exception: increment counters, log if needed.
        
        Args:
            exc: The exception caught.
            context: Context string for logging (e.g. "dispatch_one").
            msg_id: Associated message ID (if any).
            ctr_transient: Counter key for transient errors.
            ctr_fatal: Counter key for fatal errors.
            log_transient: Whether to log transient errors as warnings.
            
        Returns:
            True if transient, False if fatal.
        """
        if is_transient_error(exc):
            self.counters[ctr_transient] += 1
            if log_transient:
               self.logger.warning("Transient error %s msg=%s: %s", context, msg_id, exc)
            return True

        self.counters[ctr_fatal] += 1
        self.logger.error("Fatal error %s msg=%s: %s", context, msg_id, exc, exc_info=True)
        return False
