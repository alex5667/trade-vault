from __future__ import annotations

"""Book derivative features.

These helpers are intentionally dependency-free so they can be unit-tested without
bringing up the full runtime stack (redis/asyncpg/etc.).

Naming:
  - `*_rate_*` features are expressed in 1/sec.

Time determinism:
  - `*_ts_ms` are epoch milliseconds.
  - dt guards must treat non-monotonic timestamps as bad_time (fail-open).
"""

import math
from typing import Optional


def compute_book_imbalance_rate_10(
    prev_imb10: Optional[float],
    prev_ts_ms: Optional[int],
    cur_imb10: float,
    cur_ts_ms: int,
    *,
    clip_abs_per_s: float = 50.0,
) -> tuple[float, int]:
    """Compute book_imbalance_rate_10 = d(depth_imbalance_10)/dt (1/sec), guarded.

    Returns (rate_per_s, bad_dt_flag).

    Guard rules:
      - If prev_* is None (first observation): rate=0.0, bad_dt=0 (not an error).
      - If dt_ms <= 0 (out-of-order or duplicate book snapshot): rate=0.0, bad_dt=1.
      - Safety clip by abs value (default 50.0 1/sec) prevents "explosion" on tiny dt.
      - Non-finite rate (inf/nan) is clamped to 0.0, bad_dt=0 (fail-open).
    """
    if prev_ts_ms is None or prev_imb10 is None:
        # First observation: initialize state on caller side, no error.
        return 0.0, 0

    try:
        dt_ms = int(cur_ts_ms) - int(prev_ts_ms)
    except Exception:
        return 0.0, 1

    if dt_ms <= 0:
        # Out-of-order or duplicate snapshot: do NOT advance state.
        return 0.0, 1

    dt_sec = float(dt_ms) / 1000.0
    if dt_sec <= 0.0:
        return 0.0, 1

    try:
        rate = (float(cur_imb10) - float(prev_imb10)) / dt_sec
    except Exception:
        return 0.0, 0

    if not math.isfinite(rate):
        return 0.0, 0

    # Safety clip: prevents explosion on very small dt (e.g., 1ms tick bursts).
    if rate > clip_abs_per_s:
        rate = clip_abs_per_s
    elif rate < -clip_abs_per_s:
        rate = -clip_abs_per_s

    return float(rate), 0
