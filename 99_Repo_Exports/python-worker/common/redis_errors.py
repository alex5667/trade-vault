from __future__ import annotations

import logging
import time
from typing import Callable, Optional, TypeVar

try:
    import redis
    from redis import exceptions as rex
except Exception:  # pragma: no cover
    redis = None  # type: ignore
    rex = None  # type: ignore

from common.backoff import Backoff

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Existing: is_transient_error(...)
# Keep it untouched; add helpers on top for more granular routing.

def _exc_name(e: BaseException) -> str:
    try:
        return type(e).__name__
    except Exception:
        return "Exception"


def _msg_upper(e: BaseException) -> str:
    try:
        return (str(e) or "").upper()
    except Exception:
        return ""


def is_redis_timeout_error(e: BaseException) -> bool:
    """
    Timeout errors (network read/write timeout).
    """
    try:
        if rex:
            cls = getattr(rex, "TimeoutError", None)
            if cls is not None and isinstance(e, cls):
                return True
    except Exception:
        pass
    
    if _exc_name(e) in ("TimeoutError",):
        return True

    s = _msg_upper(e)
    return ("TIMED OUT" in s) or ("TIMEOUT" in s)


def is_redis_busy_loading_error(e: BaseException) -> bool:
    """
    Redis is loading dataset into memory (BUSY LOADING).
    """
    try:
        if rex:
            cls = getattr(rex, "BusyLoadingError", None)
            if cls is not None and isinstance(e, cls):
                return True
    except Exception:
        pass
    s = _msg_upper(e)
    return ("BUSY LOADING" in s) or ("LOADING" in s)


def is_redis_readonly_error(e: BaseException) -> bool:
    """
    READONLY errors (writing to replica / failover window).
    Often transient depending on topology.
    """
    try:
        if rex:
            cls = getattr(rex, "ReadOnlyError", None)
            if cls is not None and isinstance(e, cls):
                return True
    except Exception:
        pass
    s = _msg_upper(e)
    return "READONLY" in s


def is_redis_connection_error(e: BaseException) -> bool:
    """
    Connection class errors (socket-level) - usually transient.
    Fail-open: if redis-py classes missing, fall back to message heuristics.
    """
    try:
        if rex:
            classes = tuple(
                c for c in (
                    getattr(rex, "ConnectionError", None),
                    getattr(rex, "TimeoutError", None),
                ) if c is not None
            )
            if classes and isinstance(e, classes):
                return True
    except Exception:
        pass
    s = _msg_upper(e)
    # keep conservative: socket/connect/reset/refused
    # "Buffer is closed" / "Socket closed" are asyncio-transport errors that redis-py
    # wraps as ConnectionError when the server drops an idle TCP connection or restarts.
    return any(tok in s for tok in (
        "ECONN",
        "CONNECTION REFUSED",
        "CONNECTION RESET",
        "BROKEN PIPE",
        "CONNECTION CLOSED",
        "BUFFER IS CLOSED",
        "SOCKET CLOSED",
        "SOCKET IS CLOSED",
    ))


def is_redis_key_error(exc: BaseException) -> bool:
    """
    Key/type/schema errors (usually non-transient; indicates wrong command/key type).
    """
    msg = (str(exc) or "").upper()
    # Check for specific Redis error patterns regardless of exception type
    if "WRONGTYPE" in msg:
        return True
    if "UNKNOWN COMMAND" in msg:
        return True
    if "NOSCRIPT" in msg:
        return True
    if "SYNTAX" in msg:
        return True

    # If redis is available, also check exception types
    if redis is not None:
        Resp = getattr(redis.exceptions, "ResponseError", None)
        Data = getattr(redis.exceptions, "DataError", None)
        if Resp is not None and isinstance(exc, Resp):
            if "WRONGTYPE" in msg or "wrong kind of value" in msg.lower():
                return True
            if "NOSCRIPT" in msg:
                return True
        if Data is not None and isinstance(exc, Data):
            return True
    return False


def is_redis_stream_error(exc: BaseException) -> bool:
    """
    Stream/group-specific errors (often configuration or race).
    """
    msg = (str(exc) or "").upper()
    # Check for specific Redis stream error patterns regardless of exception type
    if "NOGROUP" in msg:
        return True
    if "BUSYGROUP" in msg:
        return True
    if "XREADGROUP" in msg:
        return True
    if "XGROUP" in msg:
        return True
    if "XAUTOCLAIM" in msg:
        return True

    # If redis is available, also check exception types
    if redis is not None:
        Resp = getattr(redis.exceptions, "ResponseError", None)
        if Resp is not None and isinstance(exc, Resp):
            return (
                "NOGROUP" in msg
                or "BUSYGROUP" in msg
                or "XREADGROUP" in msg
                or "XGROUP" in msg
                or "XAUTOCLAIM" in msg
            )
    return False


def is_transient_error(exc: BaseException) -> bool:
    """
    Should we retry? Conservative: only clearly transient categories.
    """
    try:
        # Handle known redis-py/asyncio bug where dropped transports raise TypeError
        if isinstance(exc, TypeError) and "'NoneType' object is not callable" in str(exc):
            return True

        if is_redis_connection_error(exc):
            return True
        if is_redis_timeout_error(exc):
            return True
        if is_redis_busy_loading_error(exc):
            return True
        if is_redis_readonly_error(exc):
            return True

        # Special case: stream errors.
        # NOGROUP/BUSYGROUP are NOT transient (coding/config error).
        # Other stream errors (like brief connection loss during XACK/XREAD)
        # can be retried as they often resolve after reconnection.
        if is_redis_stream_error(exc):
            msg = (str(exc) or "").upper()
            if "NOGROUP" in msg or "BUSYGROUP" in msg:
                return False
            return True
    except Exception:
        pass

    return False


def get_redis_error_category(exc: BaseException) -> str:
    """
    Low-cardinality category for DLQ/metrics tags.
    Returns one of:
      - connection | timeout | busy | readonly
      - stream | key
      - auth | data | memory | script | response
      - transient | unknown
    """
    try:
        if is_redis_busy_loading_error(exc):
            return "busy"
        if is_redis_connection_error(exc):
            return "connection"
        if is_redis_timeout_error(exc):
            return "timeout"
        if is_redis_readonly_error(exc):
            return "readonly"
        if is_redis_stream_error(exc):
            return "stream"
        if is_redis_key_error(exc):
            return "key"
        if rex:
            Auth = getattr(rex, "AuthenticationError", None)
            Data = getattr(rex, "DataError", None)
            Resp = getattr(rex, "ResponseError", None)
            if Auth is not None and isinstance(exc, Auth):
                return "auth"
            if Data is not None and isinstance(exc, Data):
                return "data"
            if Resp is not None and isinstance(exc, Resp):
                s = _msg_upper(exc)
                if "OOM" in s or "MAXMEMORY" in s:
                    return "memory"
                if "NOSCRIPT" in s:
                    return "script"
                return "response"

        # If it didn't match any specific category but is still transient
        if is_transient_error(exc):
            return "transient"
    except Exception:
        pass
    return "unknown"


def retry_redis_operation(
    operation: Callable[[], T],
    operation_name: str = "Redis operation",
    max_retries: int = 10,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    on_final_failure: Optional[Callable[[Exception], T]] = None,
    logger_instance: Optional[logging.Logger] = None,
) -> T:
    """
    Retry a Redis operation with exponential backoff and jitter.
    
    Prevents thundering herd by adding jitter to retry delays. Handles both
    BusyLoadingError exceptions and generic exceptions that match busy loading patterns.
    
    Args:
        operation: Callable that performs the Redis operation
        operation_name: Name for logging (e.g., "xreadgroup", "xrevrange")
        max_retries: Maximum number of retry attempts
        base_delay: Base delay in seconds for exponential backoff
        max_delay: Maximum delay in seconds
        on_final_failure: Optional callback if all retries fail. If provided and returns
                         a value, that value is returned instead of raising.
        logger_instance: Optional logger instance (defaults to module logger)
    
    Returns:
        Result of the operation
    
    Raises:
        Exception: If all retries fail and on_final_failure is not provided or returns None
    """
    log = logger_instance or logger
    backoff = Backoff(
        base_delay=base_delay,
        max_delay=max_delay,
        multiplier=2.0,
        jitter=True,  # Add jitter to prevent thundering herd
        max_attempts=max_retries,
    )

    last_exception: Optional[Exception] = None

    for attempt in range(max_retries):
        try:
            return operation()
        except Exception as e:
            last_exception = e

            # Check if this is a transient error (busy loading, connection, timeout)
            is_transient = is_transient_error(e)

            if not is_transient:
                # Non-retryable error, raise immediately
                raise

            # Retryable error
            if attempt < max_retries - 1:
                delay = backoff.get_delay()
                error_category = get_redis_error_category(e)
                log.warning(
                    "%s: Redis transient error [%s] (attempt %d/%d), retrying in %.1fs...",
                    operation_name,
                    error_category,
                    attempt + 1,
                    max_retries,
                    delay,
                )
                time.sleep(delay)
            else:
                # Final attempt failed
                error_category = get_redis_error_category(e)
                log.error(
                    "%s: Redis still failing [%s] after %d attempts",
                    operation_name,
                    error_category,
                    max_retries,
                )
                if on_final_failure is not None:
                    result = on_final_failure(e)
                    if result is not None:
                        return result
                raise

    # Should not reach here, but handle edge case
    if last_exception is not None:
        if on_final_failure is not None:
            result = on_final_failure(last_exception)
            if result is not None:
                return result
        raise last_exception

    raise RuntimeError(f"{operation_name}: Retry logic failed unexpectedly")
