# tick_flow_full/core/vol_regime_tracker.py
# -*- coding: utf-8 -*-
from __future__ import annotations
"""
Volatility regime tracker (fast/slow realized vol ratio + robust z-score).

Two public APIs — both maintained for backward compatibility:

1. Legacy tick-close API (used by bar_processor.py callers):
       update(ts_ms, close)          — log-return realized vol, dict snapshot()
       update(ts_ms, close=px)       — keyword alias

2. Bar-driven API (new, from diff Stage-4 recommendations):
       update_bar(bar)               — uses bar.open / bar.close, abs(close/open-1)*1e4
       snapshot_typed()              — returns VolRegimeSnapshot dataclass

Both APIs keep the same EMA + RollingRobustZ underneath.
The regime label (shock / calm / normal / na) is written on every update.
"""


import math
from dataclasses import dataclass, field
from collections import deque
from typing import Any, Dict, Optional

from core.robust_stats import RollingRobustZ


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clamp(x: float, lo: float, hi: float) -> float:
    """Clamp float to [lo, hi]; returns lo on non-finite / conversion errors."""
    try:
        xf = float(x)
    except Exception:
        return lo
    if xf < lo:
        return lo
    if xf > hi:
        return hi
    return xf


def _safe_f(x: Any, default: float = 0.0) -> float:
    """Convert to finite float or return *default*."""
    try:
        v = float(x)
        if not math.isfinite(v):
            return default
        return v
    except Exception:
        return default


# ---------------------------------------------------------------------------
# Public dataclass (new API — typed snapshot)
# ---------------------------------------------------------------------------

@dataclass
class VolRegimeSnapshot:
    """Typed snapshot returned by snapshot_typed() / update_bar()."""
    ts_ms: int
    # Realized vol (bps) for the last bar — legacy close-log-return or abs(c/o-1)*1e4
    realized_bps: float
    # Fast & slow EWMA of realized volatility (bps)
    short_ema_bps: float   # "vol_fast_bps" alias
    long_ema_bps: float    # "vol_slow_bps" alias
    # short_ema / long_ema ratio
    ratio: float           # "vol_ratio" alias
    # Robust-z of ratio over rolling window
    ratio_z: float         # "vol_ratio_z" alias
    # Regime label: "shock" | "normal" | "calm" | "na"
    regime: str


# ---------------------------------------------------------------------------
# Internal state
# ---------------------------------------------------------------------------

@dataclass
class VolRegimeState:
    """Mutable internal state shared by both APIs."""
    last_close: float = 0.0
    vol_fast: float = 0.0   # fast EWMA of realized bps
    vol_slow: float = 0.0   # slow EWMA of realized bps
    ratio: float = 0.0
    ratio_z: float = 0.0
    last_realized_bps: float = 0.0
    regime: str = "na"
    ts_ms: int = 0


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class VolRegimeTracker:
    """Tracks short/long realized volatility + robust z-score of their ratio.

    Design goals
    ------------
    - Bar-driven, deterministic, no hidden random state.
    - Two calling conventions for backward compatibility:

        # Legacy (bar_processor.py):
        tracker.update(ts_ms, close=100.0)
        snap = tracker.snapshot()   # returns dict (backward compat)

        # New bar-driven API:
        snap_typed = tracker.update_bar(bar)   # returns VolRegimeSnapshot
        snap_typed = tracker.snapshot_typed()

    Regime classification thresholds (all configurable)
    ---------------------------------------------------
    - shock : ratio_z >= shock_z           (default 3.0)
    - calm  : ratio <= calm_ratio AND ratio_z < 1.0  (default 0.9)
    - normal: everything else
    """

    def __init__(
        self,
        # Legacy parameter names kept for backward compat
        fast_alpha: float = 0.25,
        slow_alpha: float = 0.03,
        z_window: int = 240,
        eps: float = 1e-9,
        # New parameters from diff (also exposed as short_alpha / long_alpha)
        short_alpha: Optional[float] = None,
        long_alpha: Optional[float] = None,
        ratio_z_window: Optional[int] = None,
        # Regime thresholds
        shock_z: float = 3.0,
        calm_ratio: float = 0.9,
    ) -> None:
        # Allow both old and new parameter names; new ones take precedence
        self.fast_alpha = _clamp(short_alpha if short_alpha is not None else fast_alpha, 0.01, 0.99)
        self.slow_alpha = _clamp(long_alpha if long_alpha is not None else slow_alpha, 0.001, 0.2)
        self.eps = float(eps)
        self.shock_z = float(shock_z)
        self.calm_ratio = float(calm_ratio)

        win = ratio_z_window if ratio_z_window is not None else z_window
        self._z = RollingRobustZ(window=max(8, int(win)))
        self._s = VolRegimeState()

    # ------------------------------------------------------------------
    # Legacy API (backward-compat with existing bar_processor.py callers)
    # ------------------------------------------------------------------

    def update(self, ts_ms: int, close: float = 0.0, **kwargs) -> None:
        """Update from a close price (legacy tick-close log-return method).

        Accepts both positional ``update(ts, px)`` and keyword
        ``update(ts, close=px)`` calling conventions so existing
        bar_processor callers keep working without changes.

        Computes realized_bps as abs(log(close / prev_close)) * 1e4.
        """
        # Keyword alias: update(ts_ms=X, close=Y)
        if "close" in kwargs and close == 0.0:
            close = float(kwargs["close"])
        ts_ms = int(ts_ms or 0)
        px = float(close or 0.0)
        if px <= self.eps or ts_ms <= 0:
            return

        # Log-return realized vol (bps)
        if self._s.last_close > self.eps:
            r = math.log(px / self._s.last_close)
            realized_bps = abs(r) * 10_000.0
        else:
            realized_bps = 0.0

        self._s.last_close = px
        self._update_emas(realized_bps, ts_ms)

    def snapshot(self) -> Dict[str, float]:
        """Return dict snapshot — backward compat for bar_processor.py callers.

        Keys: vol_fast_bps, vol_slow_bps, vol_ratio, vol_ratio_z,
              vol_ts_ms, vol_regime_label.
        """
        s = self._s
        return {
            "vol_fast_bps":    float(s.vol_fast),
            "vol_slow_bps":    float(s.vol_slow),
            "vol_ratio":       float(s.ratio),
            "vol_ratio_z":     float(s.ratio_z),
            "vol_ts_ms":       int(s.ts_ms),
            "vol_regime_label": str(s.regime),
        }

    # ------------------------------------------------------------------
    # New bar-driven API (from Stage-4 diff)
    # ------------------------------------------------------------------

    def update_bar(self, bar: Any, ts_ms: Optional[int] = None) -> VolRegimeSnapshot:
        """Update from a MicroBar object (open/close) and return typed snapshot.

        Realized vol = abs(close/open - 1) * 1e4   (bar-range method).
        This avoids tick noise and is deterministic on bar close.

        Parameters
        ----------
        bar     : Any object with .open, .close, .end_ts_ms / .ts_ms attrs.
        ts_ms   : Override timestamp; falls back to bar.end_ts_ms / bar.ts_ms.
        """
        open_px  = _safe_f(getattr(bar, "open",  None), 0.0)
        close_px = _safe_f(getattr(bar, "close", None), 0.0)
        if ts_ms is None:
            ts_ms = int(getattr(bar, "end_ts_ms", 0) or getattr(bar, "ts_ms", 0) or 0)
        return self._update_ohlc(open_px=open_px, close_px=close_px, ts_ms=int(ts_ms))

    def update_ohlc(self, open_px: float, close_px: float, ts_ms: int) -> VolRegimeSnapshot:
        """Update from explicit open/close prices; returns typed snapshot.

        Realized vol = abs(close/open - 1) * 1e4.
        This is the named alias used by the diff for clarity.
        """
        return self._update_ohlc(open_px=open_px, close_px=close_px, ts_ms=ts_ms)

    def snapshot_typed(self) -> VolRegimeSnapshot:
        """Return typed VolRegimeSnapshot (new API)."""
        if self._s.ts_ms == 0:
            return VolRegimeSnapshot(
                ts_ms=0,
                realized_bps=0.0,
                short_ema_bps=0.0,
                long_ema_bps=0.0,
                ratio=0.0,
                ratio_z=0.0,
                regime="na",
            )
        s = self._s
        return VolRegimeSnapshot(
            ts_ms=int(s.ts_ms),
            realized_bps=float(s.last_realized_bps),
            short_ema_bps=float(s.vol_fast),
            long_ema_bps=float(s.vol_slow),
            ratio=float(s.ratio),
            ratio_z=float(s.ratio_z),
            regime=str(s.regime),
        )

    # ------------------------------------------------------------------
    # Internal shared update logic
    # ------------------------------------------------------------------

    def _update_ohlc(self, open_px: float, close_px: float, ts_ms: int) -> VolRegimeSnapshot:
        """Bar-range realized vol: abs(close/open - 1) * 1e4."""
        realized_bps = 0.0
        if open_px > 0 and close_px > 0:
            realized_bps = abs((close_px / open_px) - 1.0) * 1e4
        self._update_emas(realized_bps, ts_ms)
        return self.snapshot_typed()

    def _update_emas(self, realized_bps: float, ts_ms: int) -> None:
        """Update fast/slow EMAs, ratio, robust-z, and regime classification."""
        s = self._s
        s.last_realized_bps = float(realized_bps)
        s.ts_ms = int(ts_ms)

        # Fast EMA (short_alpha)
        if s.vol_fast <= 0.0:
            s.vol_fast = realized_bps
        else:
            s.vol_fast = self.fast_alpha * realized_bps + (1.0 - self.fast_alpha) * s.vol_fast

        # Slow EMA (slow_alpha)
        if s.vol_slow <= 0.0:
            s.vol_slow = realized_bps
        else:
            s.vol_slow = self.slow_alpha * realized_bps + (1.0 - self.slow_alpha) * s.vol_slow

        # Ratio: fast / slow (clamped to prevent runaway)
        ratio = s.vol_fast / max(s.vol_slow, self.eps) if s.vol_slow > self.eps else 0.0
        ratio = _clamp(ratio, 0.0, 100.0)

        # Robust z-score of ratio
        self._z.update(float(ratio))
        ratio_z = float(self._z.z(float(ratio)))

        s.ratio   = float(ratio)
        s.ratio_z = float(ratio_z)

        # Regime classification
        if ratio_z >= self.shock_z:
            s.regime = "shock"
        elif ratio <= self.calm_ratio and ratio_z < 1.0:
            s.regime = "calm"
        elif s.ts_ms == 0:
            s.regime = "na"
        else:
            s.regime = "normal"
