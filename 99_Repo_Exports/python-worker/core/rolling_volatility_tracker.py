from __future__ import annotations

"""Realized volatility tracker (std of log-returns) deterministic by ts_ms.

Definition
----------
realized_vol_bps = std( log(px_t / px_{t-1}) ) * 1e4

No-data contract
----------------
If we have fewer than 3 prices (fewer than 2 log-returns):
  - realized_vol_bps = 0.0
  - realized_vol_no_data = 1
"""


import math
from dataclasses import dataclass

from core.rolling_window import RollingWindow


def _clip(x: float, lo: float, hi: float) -> float:
    try:
        if x < lo:
            return lo
        if x > hi:
            return hi
        return x
    except Exception:
        return 0.0


@dataclass
class RollingVolatilityTracker:
    horizon_ms: int = 120_000
    maxlen: int = 512

    _px: RollingWindow[float] = None  # type: ignore
    last_snapshot: dict[str, float] = None  # type: ignore

    def __post_init__(self) -> None:
        self._px = RollingWindow[float](horizon_ms=int(self.horizon_ms), maxlen=int(self.maxlen))
        self.last_snapshot = {
            "realized_vol_bps": 0.0,
            "realized_vol_no_data": 1.0,
        }

    @property
    def bad_time_total(self) -> int:
        return int(getattr(self._px, "bad_time_total", 0) or 0)

    def apply_config(self, *, horizon_ms: int, maxlen: int) -> None:
        self._px.apply_config(horizon_ms=int(horizon_ms or 0), maxlen=int(maxlen or 0))

    def update(self, *, ts_ms: int, px: float) -> dict[str, float]:
        ts_ms = int(ts_ms or 0)
        px = float(px or 0.0)
        ok = self._px.push(ts_ms, px)
        if not ok:
            return dict(self.last_snapshot)

        if len(self._px) < 3:
            snap = {"realized_vol_bps": 0.0, "realized_vol_no_data": 1.0}
            self.last_snapshot = snap
            return dict(snap)

        prices: list[float] = [float(v) for _, v in self._px.items() if float(v) > 0 and math.isfinite(float(v))]
        if len(prices) < 3:
            snap = {"realized_vol_bps": 0.0, "realized_vol_no_data": 1.0}
            self.last_snapshot = snap
            return dict(snap)

        rets: list[float] = []
        for i in range(1, len(prices)):
            p0 = prices[i - 1]
            p1 = prices[i]
            if p0 > 0 and p1 > 0:
                rets.append(math.log(p1 / p0))

        if len(rets) < 2:
            snap = {"realized_vol_bps": 0.0, "realized_vol_no_data": 1.0}
            self.last_snapshot = snap
            return dict(snap)

        m = sum(rets) / float(len(rets))
        var = sum((r - m) ** 2 for r in rets) / float(len(rets))
        std = math.sqrt(max(0.0, var))
        vol_bps = _clip(float(std * 10_000.0), 0.0, 50_000.0)
        snap = {"realized_vol_bps": float(vol_bps), "realized_vol_no_data": 0.0}
        self.last_snapshot = snap
        return dict(snap)
