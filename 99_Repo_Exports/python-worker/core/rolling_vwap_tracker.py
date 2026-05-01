from __future__ import annotations
"""Rolling VWAP tracker (deterministic by ts_ms)."""

import math
from dataclasses import dataclass
from typing import Dict

from core.rolling_window import WeightedRollingWindow


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
class RollingVWAPTracker:
    horizon_ms: int = 120_000
    maxlen: int = 512

    _w: WeightedRollingWindow = None  # type: ignore
    last_snapshot: Dict[str, float] = None  # type: ignore

    def __post_init__(self) -> None:
        self._w = WeightedRollingWindow(horizon_ms=int(self.horizon_ms), maxlen=int(self.maxlen))
        self.last_snapshot = {
            "roll_vwap_px": 0.0,
            "vwap_roll_diff_bps": 0.0,
            "vwap_roll_no_data": 1.0,
        }

    @property
    def bad_time_total(self) -> int:
        return int(getattr(self._w, "bad_time_total", 0) or 0)

    def apply_config(self, *, horizon_ms: int, maxlen: int) -> None:
        self._w.apply_config(horizon_ms=int(horizon_ms or 0), maxlen=int(maxlen or 0))

    def update(self, *, ts_ms: int, vwap: float, vol: float, ref_px: float) -> Dict[str, float]:
        ts_ms = int(ts_ms or 0)
        vwap = float(vwap or 0.0)
        vol = float(vol or 0.0)
        ref_px = float(ref_px or 0.0)

        if vol > 0 and vwap > 0 and math.isfinite(vwap):
            accepted = self._w.push(ts_ms, vwap, vol)
        else:
            accepted = self._w.push(ts_ms, 0.0, 0.0)

        if not accepted:
            return dict(self.last_snapshot)

        sum_pv = 0.0
        sum_v = 0.0
        for _, px, w in self._w.items():
            if w <= 0:
                continue
            if px <= 0 or not math.isfinite(px):
                continue
            sum_pv += px * w
            sum_v += w

        if sum_v <= 0 or ref_px <= 0 or not math.isfinite(ref_px):
            snap = {
                "roll_vwap_px": 0.0,
                "vwap_roll_diff_bps": 0.0,
                "vwap_roll_no_data": 1.0,
            }
            self.last_snapshot = snap
            return dict(snap)

        roll_vwap = sum_pv / sum_v
        diff_bps = ((ref_px - roll_vwap) / ref_px) * 10_000.0
        roll_vwap = _clip(float(roll_vwap), 0.0, 1e12)
        diff_bps = _clip(float(diff_bps), -50_000.0, 50_000.0)

        snap = {
            "roll_vwap_px": float(roll_vwap),
            "vwap_roll_diff_bps": float(diff_bps),
            "vwap_roll_no_data": 0.0,
        }
        self.last_snapshot = snap
        return dict(snap)
