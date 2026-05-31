"""
Observability helpers for SignalDispatcher.

Provides fail-open logging and metrics increment functions to ensure
observability code does not crash the main application logic.
"""

from collections.abc import Callable
from typing import Any


def sd_sampled_debug(logger: Any, key: str, msg: str, *args: Any) -> None:
    """
    Sampled debug logging to avoid flooding logs.
    
    This is a simplified standalone version. In a full system, this might track
    frequency per key. For now, we rely on the logger's own filtering or
    just log at DEBUG level.
    """
    if logger:
        # In a real high-throughput system we might sample here based on 'key'
        logger.debug(msg, *args)


def sd_try_incr(logger: Any, incr_fn: Callable[[str], Any] | None, metric_key: str) -> None:
    """
    Best-effort metrics increment; never raises.
    
    Args:
        logger: Logger instance
        incr_fn: Function to increment metric (e.g. statsd.incr)
        metric_key: Metric key name
    """
    if incr_fn is None:
        return
    try:
        incr_fn(metric_key)
    except Exception as e:
        # Do not recurse: only debug log
        sd_sampled_debug(logger, "metrics_incr_failed", "metrics incr failed key=%s err=%r", metric_key, e)


def sd_fail_open(
    logger: Any,
    *,
    key: str,
    err: Exception,
    incr_fn: Callable[[str], Any] | None = None,
    metric_key: str = ""
) -> None:
    """
    Unified "fail-open but observable" handler.

    Args:
        logger: Logger instance
        key: Logical key of operation (for sampling/context)
        err: The exception that occurred
        incr_fn: Optional metric increment function
        metric_key: Full metric key (with prefix) to increment on error
    """
    if metric_key:
        sd_try_incr(logger, incr_fn, metric_key)
    sd_sampled_debug(logger, key, "fail-open: %s err=%r", key, err)
