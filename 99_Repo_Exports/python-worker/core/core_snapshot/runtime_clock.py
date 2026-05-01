from __future__ import annotations
"""tick_flow_full.core.runtime_clock

Monotonic runtime clock helpers.

Why this exists
---------------
For rollout guards like "observe-only 24–48h" we must NOT use wall-clock time.
Wall-clock can jump (NTP, VM resume, clock skew), which makes "uptime" non-
deterministic and can accidentally enable veto too early (or never).

We therefore base uptime on time.monotonic().

We also optionally expose runtime_start_ts_ms as an *estimate* of process start
in epoch-ms derived from an *event time* (preferred) minus monotonic uptime.
This makes the value deterministic with respect to the input event timeline.
If you do not have an event timestamp, prefer leaving runtime_start_ts_ms=None.
"""


import time
from dataclasses import dataclass
from typing import Optional


_MONO_START = time.monotonic()


def uptime_sec() -> float:
    """Process uptime in seconds, based on a monotonic clock."""
    u = time.monotonic() - _MONO_START
    # Defensive guard: monotonic should not go backwards, but do not let
    # negative values leak into rollout logic.
    return 0.0 if u < 0.0 else float(u)


def runtime_start_ts_ms(event_ts_ms: Optional[int] = None) -> Optional[int]:
    """Best-effort epoch-ms process start timestamp.

    Preferred usage: pass an *event timestamp* (e.g., current tick ts in ms)
    so the result is deterministic against the input stream timeline.

    If event_ts_ms is not provided, returns None by design, because using
    wall-clock for start time reintroduces NTP/jump risk.
    """
    if event_ts_ms is None:
        return None
    # NOTE: uptime_sec() is monotonic; deriving start_ts from event time makes
    # the value stable even if wall-clock changes.
    return int(int(event_ts_ms) - uptime_sec() * 1000.0)


@dataclass(frozen=True)
class RuntimeClockSnapshot:
    """Convenience bundle for downstream code (DQ gate, decision records)."""

    uptime_sec: float
    runtime_start_ts_ms: Optional[int]


def snapshot(event_ts_ms: Optional[int] = None) -> RuntimeClockSnapshot:
    """Return (uptime_sec, runtime_start_ts_ms) in one call."""
    return RuntimeClockSnapshot(
        uptime_sec=uptime_sec(),
        runtime_start_ts_ms=runtime_start_ts_ms(event_ts_ms=event_ts_ms),
    )
