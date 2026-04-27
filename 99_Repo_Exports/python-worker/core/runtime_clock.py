"""Process runtime clock (monotonic uptime + derived start timestamp).

Why this exists:
- Warmup/observe-only windows must not depend on wall clock (NTP can jump).
- Downstream wants to export `uptime_sec` and optionally `runtime_start_ts_ms`
  for debugging and decision records.

The `runtime_start_ts_ms` is *derived* from the provided event timestamp to
avoid relying on wall-clock time.
"""

from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Optional, Any


_START_MONO = time.monotonic()


@dataclass(frozen=True)
class RuntimeClock:
    uptime_sec: int
    runtime_start_ts_ms: Optional[int] = None


def snapshot(event_ts_ms: Optional[Any] = None) -> RuntimeClock:
    """Take a monotonic runtime snapshot.

    Args:
        event_ts_ms: Optional event timestamp (epoch ms). If provided and valid,
            runtime_start_ts_ms is derived as (event_ts_ms - uptime_ms).

    Returns:
        RuntimeClock(uptime_sec, runtime_start_ts_ms?)
    """
    try:
        up = time.monotonic() - _START_MONO
        if up < 0:
            up = 0.0
    except Exception:
        up = 0.0

    uptime_sec = int(up)

    runtime_start_ts_ms: Optional[int] = None
    if event_ts_ms is not None:
        try:
            et = int(event_ts_ms)
            if et > 0:
                runtime_start_ts_ms = int(et - (uptime_sec * 1000))
        except Exception:
            runtime_start_ts_ms = None

    return RuntimeClock(uptime_sec=uptime_sec, runtime_start_ts_ms=runtime_start_ts_ms)
