"""Rolling momentum trackers (price + spread) deterministic by ts_ms.

We intentionally compute momentum from bar-close observations:
  - price_momentum_bps: (px_now - px_first_in_window)/px_now * 1e4
  - spread_momentum_bps_per_s: d(spread_bps)/dt_sec between the last 2 accepted points

No-data contract
----------------
- If window has <2 points or px_now<=0 => price_momentum_bps=0, price_momentum_no_data=1
- If we don't have 2 spread points with dt>0 => spread_momentum_bps_per_s=0, spread_momentum_no_data=1

Time safety
-----------
- Non-monotonic ts_ms is rejected and counted (fail-open).
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict

from core.rolling_window import RollingWindow


def _clip(x: float, lo: float, hi: float) -> float:
    try:
        if x < lo:
            return lo
        if x > hi:
            return hi
        return hi if x > hi else x
    except Exception:
        return 0.0


@dataclass
class RollingMomentumTracker:
    horizon_ms: int = 60_000
    maxlen: int = 512

    _px: RollingWindow[float] = None  # type: ignore
    _sp: RollingWindow[float] = None  # type: ignore
    last_snapshot: Dict[str, float] = None  # type: ignore

    def __post_init__(self) -> None:
        self._px = RollingWindow[float](horizon_ms=int(self.horizon_ms), maxlen=int(self.maxlen))
        self._sp = RollingWindow[float](horizon_ms=int(self.horizon_ms), maxlen=int(self.maxlen))
        self.last_snapshot = {
            "price_momentum_bps": 0.0,
            "price_momentum_no_data": 1.0,
            "spread_momentum_bps_per_s": 0.0,
            "spread_momentum_no_data": 1.0,
        }

    @property
    def bad_time_total(self) -> int:
        return int(getattr(self._px, "bad_time_total", 0) or 0) + int(getattr(self._sp, "bad_time_total", 0) or 0)

    def apply_config(self, *, horizon_ms: int, maxlen: int) -> None:
        self._px.apply_config(horizon_ms=int(horizon_ms or 0), maxlen=int(maxlen or 0))
        self._sp.apply_config(horizon_ms=int(horizon_ms or 0), maxlen=int(maxlen or 0))

    def update(self, *, ts_ms: int, px: float, spread_bps: float) -> Dict[str, float]:
        ts_ms = int(ts_ms or 0)
        px = float(px or 0.0)
        spread_bps = float(spread_bps or 0.0)

        ok1 = self._px.push(ts_ms, px)
        ok2 = self._sp.push(ts_ms, spread_bps)
        if not ok1 or not ok2:
            return dict(self.last_snapshot)

        snap: Dict[str, float] = {
            "price_momentum_bps": 0.0,
            "price_momentum_no_data": 1.0,
            "spread_momentum_bps_per_s": 0.0,
            "spread_momentum_no_data": 1.0,
        }

        if len(self._px) >= 2 and px > 0 and math.isfinite(px):
            first = self._px.first()
            if first is not None:
                px0 = float(first[1] or 0.0)
                if px0 > 0 and math.isfinite(px0):
                    mom_bps = ((px - px0) / px) * 10_000.0
                    snap["price_momentum_bps"] = _clip(float(mom_bps), -50_000.0, 50_000.0)
                    snap["price_momentum_no_data"] = 0.0

        if len(self._sp) >= 2:
            last = self._sp.last()
            items = self._sp.items()
            prev = items[-2] if len(items) >= 2 else None
            if last is not None and prev is not None:
                t1, s1 = int(prev[0]), float(prev[1] or 0.0)
                t2, s2 = int(last[0]), float(last[1] or 0.0)
                dt_ms = t2 - t1
                if dt_ms > 0:
                    rate = (s2 - s1) / (dt_ms / 1000.0)
                    snap["spread_momentum_bps_per_s"] = _clip(float(rate), -50_000.0, 50_000.0)
                    snap["spread_momentum_no_data"] = 0.0

        self.last_snapshot = snap
        return dict(snap)
