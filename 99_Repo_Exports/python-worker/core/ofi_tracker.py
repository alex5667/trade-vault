from __future__ import annotations

"""
OFIStabilityTracker (Order Flow Imbalance) — L1 incremental flow.
Modified to satisfy runtime.py and test_ofi_tracker.py expectations.
"""

from dataclasses import dataclass, asdict
from typing import Optional, Tuple

from core.robust_stats import RollingRobustZ


@dataclass(frozen=True)
class OFIEvent:
    ts_ms: int
    ofi: float       # units (base)
    ofi_z: float
    stable_secs: float
    stability_score: float  # 0..1
    stable: int

    def to_dict(self) -> dict:
        return asdict(self)


class OFIStabilityTracker:
    def __init__(
        self,
        window_ms: int = 3000,
        z_window: int = 128,
    ) -> None:
        self.window_ms = int(window_ms)
        self.stats = RollingRobustZ(window=max(32, int(z_window)))

        self._dir: int = 0
        self._dir_start_ts_ms: Optional[int] = None
        self._last_ts_ms: Optional[int] = None
        self._last_non_zero_ts_ms: Optional[int] = None

    def reset(self) -> None:
        self._dir = 0
        self._dir_start_ts_ms = None
        self._last_ts_ms = None
        self._last_non_zero_ts_ms = None
        self.stats.reset()

    def compute_ofi_best_level(
        self,
        prev_bid_px: float, prev_bid_qty: float,
        prev_ask_px: float, prev_ask_qty: float,
        bid_px: float, bid_qty: float,
        ask_px: float, ask_qty: float
    ) -> float:
        # Cont OFI definition (L1)
        # Bid contribution
        if bid_px > prev_bid_px:
            eb = bid_qty
        elif bid_px == prev_bid_px:
            eb = bid_qty - prev_bid_qty
        else:
            eb = -prev_bid_qty

        # Ask contribution (note: ask improving is pa1 < pa0)
        if ask_px < prev_ask_px:
            ea = ask_qty
        elif ask_px == prev_ask_px:
            ea = ask_qty - prev_ask_qty
        else:
            ea = -prev_ask_qty

        return float(eb - ea)

    def update(
        self,
        ts_ms: int,
        ofi: float,
        depth_qty: float,
        deadband_abs: float = 0.0,
        deadband_frac_depth: float = 0.02,
        z_full: float = 3.0,
    ) -> Tuple[float, float, float]:
        ts_ms = int(ts_ms)
        ofi = float(ofi or 0.0)
        depth_qty = float(depth_qty or 1.0)

        if self._last_ts_ms is not None and ts_ms < self._last_ts_ms:
            # немонотонное время: игнорируем sample
            try:
                ofi_z = float(self.stats.z(ofi))
            except Exception:
                ofi_z = 0.0
            return ofi_z, 0.0, 0.0
        
        self._last_ts_ms = ts_ms

        # Robust stats
        try:
            self.stats.update(ofi)
            ofi_z = float(self.stats.z(ofi))
        except Exception:
            ofi_z = 0.0

        # Direction persistence
        threshold = max(deadband_abs, deadband_frac_depth * depth_qty)
        s = 0
        if ofi >= threshold and threshold >= 0:
            s = 1
        elif ofi <= -threshold and threshold >= 0:
            s = -1

        if s != 0:
            self._last_non_zero_ts_ms = ts_ms
            if self._dir != s:
                self._dir = s
                self._dir_start_ts_ms = ts_ms
            elif self._dir_start_ts_ms is None:
                self._dir_start_ts_ms = ts_ms
        else:
            # Hold direction for up to 500ms grace if in deadband
            if self._dir != 0 and self._last_non_zero_ts_ms is not None:
                if (ts_ms - self._last_non_zero_ts_ms) > 500:
                    self._dir = 0
                    self._dir_start_ts_ms = None

        stable_secs = 0.0
        if self._dir != 0 and self._dir_start_ts_ms is not None:
            stable_secs = max(0.0, (ts_ms - self._dir_start_ts_ms) / 1000.0)

        # stability_score: relative to z_full
        score = min(1.0, abs(ofi_z) / max(0.1, z_full))

        return ofi_z, stable_secs, score
