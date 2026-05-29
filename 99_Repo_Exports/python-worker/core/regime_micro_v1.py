from __future__ import annotations

"""
Fast micro-regime classifier (no hysteresis, no min_hold).

Runs on every 1m bar close in bar_processor.py and provides a regime label
that is semantically current (5-bar window ≈ 5 minutes), unlike the slow
regime which has min_hold=180s + 3-bar confirm and lags 3-10 minutes.

Slow regime (MarketRegimeService) → structural gating (entry veto).
Micro regime → adaptive TP/SL/scoring adjustment within the slow regime.
Both are preserved independently; neither replaces the other.
"""

import math
import os
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Literal

RegimeMicroLabel = Literal[
    "trend_micro_up",
    "trend_micro_down",
    "range_micro",
    "shock_micro",
    "squeeze_micro",
    "mixed_micro",
]

_VALID_LABELS: frozenset[str] = frozenset({
    "trend_micro_up", "trend_micro_down", "range_micro",
    "shock_micro", "squeeze_micro", "mixed_micro",
})


def _safe_f(x: object, default: float = 0.0) -> float:
    try:
        v = float(x)  # type: ignore[arg-type]
        return v if math.isfinite(v) else default
    except Exception:
        return default


@dataclass(frozen=True)
class RegimeMicroConfig:
    enabled: bool = True
    window_bars: int = 5
    trend_ret_bps: float = 15.0
    range_ret_bps: float = 5.0
    range_pct_bps: float = 20.0
    squeeze_pct_bps: float = 10.0
    shock_abs_z: float = 3.0

    @staticmethod
    def from_env() -> "RegimeMicroConfig":
        return RegimeMicroConfig(
            enabled=os.getenv("REGIME_MICRO_ENABLED", "1") != "0",
            window_bars=int(os.getenv("REGIME_MICRO_WINDOW_BARS", "5")),
            trend_ret_bps=float(os.getenv("REGIME_MICRO_TREND_RET_BPS", "15")),
            range_ret_bps=float(os.getenv("REGIME_MICRO_RANGE_RET_BPS", "5")),
            range_pct_bps=float(os.getenv("REGIME_MICRO_RANGE_PCT_BPS", "20")),
            squeeze_pct_bps=float(os.getenv("REGIME_MICRO_SQUEEZE_PCT_BPS", "10")),
            shock_abs_z=float(os.getenv("REGIME_MICRO_SHOCK_ABS_Z", "3.0")),
        )


def classify_regime_micro(
    ret_5_bps: float,
    vol_state: str,
    range_pct_5_bps: float,
    abs_ret_z: float,
    cfg: RegimeMicroConfig | None = None,
) -> str:
    """Pure stateless classifier. No I/O, no side effects.

    Priority order (first match wins):
      shock_micro > trend_micro_up > trend_micro_down > squeeze_micro > range_micro > mixed_micro
    """
    if cfg is None:
        cfg = RegimeMicroConfig()

    ret = _safe_f(ret_5_bps)
    range_pct = _safe_f(range_pct_5_bps)
    z = _safe_f(abs_ret_z)
    vol = str(vol_state or "na").strip().lower()

    if vol == "shock" or z >= cfg.shock_abs_z:
        return "shock_micro"

    if ret >= cfg.trend_ret_bps:
        return "trend_micro_up"

    if ret <= -cfg.trend_ret_bps:
        return "trend_micro_down"

    if vol == "calm" and range_pct <= cfg.squeeze_pct_bps:
        return "squeeze_micro"

    if abs(ret) <= cfg.range_ret_bps and range_pct <= cfg.range_pct_bps:
        return "range_micro"

    return "mixed_micro"


@dataclass
class RegimeMicroState:
    """Per-symbol rolling state for micro regime computation.

    Call push_bar() on every 1m close, then read label / age_ms.
    Thread-unsafe (single-symbol, single-thread design).
    """

    cfg: RegimeMicroConfig = field(default_factory=RegimeMicroConfig.from_env)

    # Rolling window of (close_px, high_px, low_px) per bar
    _closes: Deque[float] = field(default_factory=lambda: deque(maxlen=6))
    _highs: Deque[float] = field(default_factory=lambda: deque(maxlen=6))
    _lows: Deque[float] = field(default_factory=lambda: deque(maxlen=6))

    # Rolling std of 1m returns for abs_ret_z (window=20)
    _ret1m_history: Deque[float] = field(default_factory=lambda: deque(maxlen=20))

    # Last computed label
    label: str = "na"
    label_ts_ms: int = 0

    def push_bar(
        self,
        close: float,
        high: float,
        low: float,
        vol_state: str,
        ts_ms: int,
    ) -> str:
        """Update rolling state and return new label. Returns 'na' if not enabled."""
        if not self.cfg.enabled:
            return "na"

        c = _safe_f(close)
        h = _safe_f(high)
        lo = _safe_f(low)
        if c <= 0:
            return self.label

        # 1m return vs previous close (bps)
        ret1m_bps = 0.0
        if self._closes and self._closes[-1] > 0:
            ret1m_bps = (c - self._closes[-1]) / self._closes[-1] * 10_000.0

        self._closes.append(c)
        self._highs.append(h if h > 0 else c)
        self._lows.append(lo if lo > 0 else c)
        self._ret1m_history.append(ret1m_bps)

        n = len(self._closes)
        w = min(n, self.cfg.window_bars)
        if w < 2:
            return self.label

        window_closes = list(self._closes)[-w:]
        window_highs = list(self._highs)[-w:]
        window_lows = list(self._lows)[-w:]

        oldest = window_closes[0]
        newest = window_closes[-1]
        ret_5_bps = (newest - oldest) / oldest * 10_000.0 if oldest > 0 else 0.0

        hi = max(window_highs)
        lo_w = min(window_lows)
        mid = (hi + lo_w) / 2.0
        range_pct_5_bps = (hi - lo_w) / mid * 10_000.0 if mid > 0 else 0.0

        # Robust abs_ret_z: |ret1m| / rolling_std(ret1m, 20)
        hist = list(self._ret1m_history)
        abs_z = 0.0
        if len(hist) >= 5:
            mean = sum(hist) / len(hist)
            var = sum((x - mean) ** 2 for x in hist) / len(hist)
            std = math.sqrt(var) if var > 0 else 0.0
            if std > 0:
                abs_z = abs(ret1m_bps) / std

        lbl = classify_regime_micro(
            ret_5_bps=ret_5_bps,
            vol_state=vol_state,
            range_pct_5_bps=range_pct_5_bps,
            abs_ret_z=abs_z,
            cfg=self.cfg,
        )

        self.label = lbl
        self.label_ts_ms = ts_ms
        return lbl
