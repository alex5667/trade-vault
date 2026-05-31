"""
kelly_sizing.py — Phase 3.2 fractional Kelly position sizing.

Pure-function module. Provides:

  kelly_fraction(p, b)
    Classical Kelly formula f* = (p*b - (1-p)) / b for binary outcome
    (win = +b, loss = -1, on per-unit-risk basis).

  size_position(...)
    Translates calibrated P(win) and payoff ratio into a position size,
    applying:
        * fraction multiplier (default 0.25 — fractional Kelly)
        * hard cap (max % of equity / max R per trade)
        * floor (no-trade band) when EV is too small
        * safety: collapses to ZERO when calibration is suspect
          (ECE-regression guard exposed via `calib_ok` boolean — owner
          of the gate must pass the live ECE/Brier check result).

Reasonable for adoption AFTER:
  * meta-labeling delivers a calibrated `p_win`
  * `signal_outcome` confirms realised vs expected EV is non-negative
  * SHADOW window has gathered sufficient samples (the ENV master
    switch KELLY_SIZING_ENABLED defaults to 0 — caller side flag).

No I/O — pure math, deterministic, fully unit-tested.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SizingResult:
    fraction: float       # final equity fraction applied (0..max_pct)
    kelly_raw: float      # raw Kelly fraction (can be negative)
    kelly_clamped: float  # raw clamped to >= 0
    used_fraction: float  # kelly_fraction multiplier applied (e.g., 0.25)
    rejected_reason: str  # "" if accepted; else why fraction=0


def kelly_fraction(p_win: float, payoff_ratio: float) -> float:
    """Raw Kelly fraction for binary bet with payoff b = tp_r / sl_r.

    f* = (p * b - (1-p)) / b
       = p - (1-p)/b
    """
    if payoff_ratio <= 0:
        return 0.0
    p = max(0.0, min(1.0, float(p_win)))
    b = float(payoff_ratio)
    return (p * b - (1.0 - p)) / b


def size_position(
    p_win: float,
    payoff_ratio: float,
    kelly_mult: float = 0.25,
    max_position_pct: float = 0.02,
    min_edge_bps: float = 5.0,
    sl_bps: float = 10.0,
    calib_ok: bool = True,
    fallback_fixed_pct: float = 0.0,
) -> SizingResult:
    """Returns SizingResult.fraction in [0, max_position_pct].

    Args:
      p_win:            calibrated P(TP hit before SL) in [0,1]
      payoff_ratio:     tp_r / sl_r (typically >= 1 for trend-following)
      kelly_mult:       fractional Kelly multiplier (default 0.25)
      max_position_pct: hard cap on equity fraction (default 2%)
      min_edge_bps:     no-trade band on expected edge per bet
      sl_bps:           |SL| in bps — converts payoff to bps for edge check
      calib_ok:         False = ECE regressed; fall back to fallback_fixed_pct
      fallback_fixed_pct: position size when calib_ok=False (default 0 = no trade)
    """
    if not calib_ok:
        return SizingResult(
            fraction=max(0.0, min(max_position_pct, fallback_fixed_pct)),
            kelly_raw=0.0,
            kelly_clamped=0.0,
            used_fraction=kelly_mult,
            rejected_reason="calib_not_ok" if fallback_fixed_pct == 0 else "",
        )

    raw = kelly_fraction(p_win, payoff_ratio)
    clamped = max(0.0, raw)

    # No-trade band: expected_edge_bps = sl_bps * (p*b - (1-p))
    # If below min_edge_bps, skip.
    edge_per_bet_bps = sl_bps * (p_win * payoff_ratio - (1.0 - p_win))
    if edge_per_bet_bps < min_edge_bps:
        return SizingResult(
            fraction=0.0,
            kelly_raw=raw,
            kelly_clamped=clamped,
            used_fraction=kelly_mult,
            rejected_reason=f"edge_below_min:{edge_per_bet_bps:.2f}<{min_edge_bps:.2f}",
        )

    f = clamped * max(0.0, kelly_mult)
    f = min(f, max_position_pct)
    if f <= 0.0:
        return SizingResult(
            fraction=0.0,
            kelly_raw=raw,
            kelly_clamped=clamped,
            used_fraction=kelly_mult,
            rejected_reason="kelly_non_positive",
        )

    return SizingResult(
        fraction=f,
        kelly_raw=raw,
        kelly_clamped=clamped,
        used_fraction=kelly_mult,
        rejected_reason="",
    )
