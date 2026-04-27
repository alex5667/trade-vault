# -*- coding: utf-8 -*-
"""core.slippage_model

Expected slippage (bps) model for adverse-selection filtering.

Goal:
  Reject or shadow-only entries with high likelihood of adverse selection.

World-practice rationale:
  In fast markets, the quality loss often comes from fill quality (slippage),
  not from entry timing. Modeling expected slippage as a function of spread,
  book churn, book update rate, pressure, and volatility is a robust first
  line of defense.

Design constraints:
  - No L2 reconstruction required.
  - Uses already available microstructure proxies.
  - Deterministic and cheap.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Tuple


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return d


@dataclass
class SlippageEstimate:
    expected_bps: float
    ok: bool
    reason: str


def expected_slippage_bps(*, spread_bps: float, churn_score: float, book_rate_z: float, pressure_sps: float, atr_bps: float, cfg: Dict[str, Any]) -> SlippageEstimate:
    """Compute expected slippage in basis points.

    Inputs:
      spread_bps: current spread proxy (bps)
      churn_score: 0..inf (higher => more churn / flicker)
      book_rate_z: robust z-score of instantaneous book update rate
      pressure_sps: signal pressure proxy (signals per second EMA)
      atr_bps: volatility proxy

    Config (optional):
      slippage_spread_w (default 0.60)
      slippage_churn_w  (default 0.25)
      slippage_press_w  (default 0.15)
      slippage_rate_penalty_z (default -2.0)  # low-rate is dangerous
      slippage_max_bps (default 18)
      slippage_shadow_only_bps (default 12)

    Returns:
      SlippageEstimate(expected_bps, ok, reason)
    """

    spr = max(0.0, _f(spread_bps, 0.0))
    churn = max(0.0, _f(churn_score, 0.0))
    press = max(0.0, _f(pressure_sps, 0.0))
    atr = max(0.0, _f(atr_bps, 0.0))
    z = _f(book_rate_z, 0.0)

    w_spr = _f(cfg.get("slippage_spread_w", 0.60), 0.60)
    w_churn = _f(cfg.get("slippage_churn_w", 0.25), 0.25)
    w_press = _f(cfg.get("slippage_press_w", 0.15), 0.15)

    # penalty: low book rate (negative z) increases expected slippage
    z_pen = _f(cfg.get("slippage_rate_penalty_z", -2.0), -2.0)
    low_rate_mult = 1.0
    try:
        if z < z_pen:
            # e.g. z=-4, z_pen=-2 => multiplier ~1.25
            low_rate_mult = 1.0 + min(1.0, abs(z - z_pen) / 8.0)
    except Exception:
        low_rate_mult = 1.0

    # pressure saturating transform: 0..1
    # by default treat 0.5 sps as "high" in orderflow entry pipeline
    press_scale = _f(cfg.get("slippage_pressure_scale_sps", 0.5), 0.5)
    press_n = min(1.0, press / max(1e-9, press_scale))

    # churn saturating transform: 0..1 (churn_score is already non-negative)
    churn_scale = _f(cfg.get("slippage_churn_scale", 2.0), 2.0)
    churn_n = min(1.0, churn / max(1e-9, churn_scale))

    # volatility affects how much spread matters (in low ATR, spread hurts more)
    # factor in [1.0 .. 1.5]
    vol_floor = _f(cfg.get("slippage_vol_floor_bps", 6.0), 6.0)
    vol_mult = 1.0
    if atr > 0 and atr < vol_floor:
        vol_mult = 1.0 + min(0.5, (vol_floor - atr) / max(1e-9, vol_floor))

    expected = (w_spr * spr * vol_mult + w_churn * 10.0 * churn_n + w_press * 10.0 * press_n) * low_rate_mult
    expected = max(0.0, float(expected))

    max_bps = _f(cfg.get("slippage_max_bps", 18.0), 18.0)
    shadow_bps = _f(cfg.get("slippage_shadow_only_bps", 12.0), 12.0)
    if expected >= max_bps:
        return SlippageEstimate(expected_bps=expected, ok=False, reason="SLIPPAGE_TOO_HIGH")
    if expected >= shadow_bps:
        return SlippageEstimate(expected_bps=expected, ok=True, reason="SLIPPAGE_SHADOW_ONLY")
    return SlippageEstimate(expected_bps=expected, ok=True, reason="OK")
