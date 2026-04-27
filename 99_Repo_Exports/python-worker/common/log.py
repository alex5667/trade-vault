"""
Common logging utilities for the scanner infrastructure.

Provides standardized logger setup, configuration, and trace_id propagation.

#18: trace_id is stored in a contextvars.ContextVar so it is automatically
included in every log record produced within the same async task or thread,
without needing to explicit pass it through every call frame.

Usage:
    from common.log import set_trace_id, clear_trace_id, get_logger

    set_trace_id("abc-123")
    logger = get_logger(__name__)
    logger.info("processing signal")  # record carries trace_id="abc-123"
    clear_trace_id()

FastAPI / asyncio: set_trace_id() at the entry point of each request/task.
Go-originated trace_id: read from Redis payload field and call set_trace_id()
before processing the tick.
"""
from __future__ import annotations

import logging
import sys
from contextvars import ContextVar, Token
from typing import Optional

# ---------------------------------------------------------------------------
# trace_id context variable
# ---------------------------------------------------------------------------
_TRACE_ID_VAR: ContextVar[str] = ContextVar("trace_id", default="")


def set_trace_id(trace_id: str) -> Token[str]:
    """Set the current trace_id for this async task / thread.

    Returns the Token so callers can reset() to the previous value if needed.
    Prefer using a context-manager in long-running code:

        token = set_trace_id("abc-123")
        try:
            ...
        finally:
            _TRACE_ID_VAR.reset(token)
    """
    return _TRACE_ID_VAR.set(str(trace_id or ""))


def get_trace_id() -> str:
    """Return the current trace_id, or empty string if not set."""
    return _TRACE_ID_VAR.get("")


def clear_trace_id() -> None:
    """Reset trace_id to empty string (default)."""
    _TRACE_ID_VAR.set("")


# ---------------------------------------------------------------------------
# Logging filter that injects trace_id into every LogRecord
# ---------------------------------------------------------------------------
class _TraceIdFilter(logging.Filter):
    """Inject current trace_id into every LogRecord as record.trace_id."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.trace_id = _TRACE_ID_VAR.get("")  # type: ignore[attr-defined]
        return True


# ---------------------------------------------------------------------------
# Logger factory
# ---------------------------------------------------------------------------
_TRACE_FILTER = _TraceIdFilter()


def setup_logger(name: str = "app", level: Optional[str] = "INFO") -> logging.Logger:
    """Setup and return a configured logger.

    Args:
        name: Logger name
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

    Returns:
        Configured logger instance
    """
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)

    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(level)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)

    # Include trace_id in the format.  Falls back gracefully to "" when not set.
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | trace=%(trace_id)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    handler.addFilter(_TRACE_FILTER)

    logger.addHandler(handler)
    logger.addFilter(_TRACE_FILTER)

    return logger


def get_logger(name: str = "app") -> logging.Logger:
    """Get a logger by name.

    Args:
        name: Logger name

    Returns:
        Logger instance
    """
    return logging.getLogger(name)
