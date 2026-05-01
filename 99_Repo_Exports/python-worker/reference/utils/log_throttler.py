from __future__ import annotations
"""utils.log_throttler — Rate-limit repeated log messages.

Allows emitting only every N-th occurrence of a keyed message to avoid
log spam in hot signal/volume loops.

Usage::

    from utils.log_throttler import log_throttler

    if log_throttler.should_log("expired_ticker", 10_000):
        logger.warning("Stale ticker detected")
"""

import logging
import threading
from collections import defaultdict

_log = logging.getLogger(__name__)


class LogThrottler:
    """Thread-safe counter that gates repeated log messages.

    Example::

        throttler = LogThrottler()
        if throttler.should_log("my_key", every_n=10_000):
            logger.info("event happened")
    """

    def __init__(self) -> None:
        self._counters: dict[str, int] = defaultdict(int)
        self._lock = threading.Lock()

    def should_log(self, message_key: str, every_n: int = 10_000) -> bool:
        """Return ``True`` on the 1st call and every *every_n*-th call after.

        Args:
            message_key: Unique string key for the message type.
            every_n:     Emit every N-th occurrence (default 10 000).
        """
        with self._lock:
            self._counters[message_key] += 1
            count = self._counters[message_key]
        return count == 1 or count % every_n == 0

    def get_count(self, message_key: str) -> int:
        """Return the current invocation counter for *message_key*."""
        with self._lock:
            return self._counters[message_key]

    def reset_counter(self, message_key: str) -> None:
        """Reset the counter for *message_key* to zero."""
        with self._lock:
            self._counters[message_key] = 0

    def log_with_count(
        self,
        message_key: str,
        message: str,
        every_n: int = 10_000,
    ) -> bool:
        """Log *message* via ``logging.info`` if this occurrence should be emitted.

        Appends a ``[shown N/N, next every K]`` suffix on repeated emissions.

        Args:
            message_key: Unique key for the message type.
            message:     Message text to emit.
            every_n:     Throttle threshold.

        Returns:
            ``True`` if the message was logged, ``False`` if suppressed.
        """
        if not self.should_log(message_key, every_n):
            return False
        count = self.get_count(message_key)
        if count == 1:
            _log.info(message)
        else:
            _log.info("%s [shown %d/%d, next every %d]", message, count, count, every_n)
        return True


# Module-level singleton for convenient cross-module reuse.
log_throttler = LogThrottler()
