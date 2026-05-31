
import logging
import os
import time
from collections.abc import Callable
from typing import Any

# Common throttle map for sampled logs
_DBG_LAST: dict[str, float] = {}

def get_debug_interval() -> float:
    """Get the sampled debug interval from environment variable or default."""
    try:
        return float(os.getenv("SAMPLED_DEBUG_INTERVAL_SEC", "30.0") or 30.0)
    except Exception:
        return 30.0

def sampled_debug(logger: Any, key: str, msg: str, *args: Any) -> None:
    """
    Fail-open debug logging with sampling to prevent log spam.
    Logs once every `SAMPLED_DEBUG_INTERVAL_SEC` (default 30s) per key.
    """
    try:
        interval = get_debug_interval()
        now = time.time()
        last = _DBG_LAST.get(key, 0.0)

        if (now - last) < interval:
            return

        _DBG_LAST[key] = now

        lg = logger if logger is not None else logging.getLogger("resiliency")
        lg.debug(msg, *args)
    except Exception:
        # Absolute fail-open: never raise here
        return

def safe_call_fail_open(
    logger: Any,
    *,
    key: str,
    fn: Callable[..., Any],
    args: tuple[Any, ...] = (),
    kwargs: dict[str, Any] | None = None,
    ctx: Any = None,
    dq_flag: str = "",
    append_flag_fn: Callable[[Any, str], None] | None = None,
) -> bool:
    """
    Unified fail-open wrapper for critical paths.
    
    Policy:
      - NEVER crash the pipeline (catch all exceptions).
      - Make issues observable via sampled logging.
      - (Optional) Mark data quality flags on context.
      
    Returns:
        True if successful, False if exception caught.
    """
    if kwargs is None:
        kwargs = {}

    try:
        fn(*args, **kwargs)
        return True
    except Exception as e:
        # 1) DQ flag if applicable
        if ctx is not None and dq_flag and append_flag_fn is not None:
            try:
                append_flag_fn(ctx, dq_flag)
            except Exception:
                pass

        # 2) Sampled observer log
        sampled_debug(logger, key, "fail-open: %s err=%r", key, e)
        return False
