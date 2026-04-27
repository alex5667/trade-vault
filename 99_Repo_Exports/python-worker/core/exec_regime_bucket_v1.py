from __future__ import annotations

"""Execution-regime bucket classifier (liq × vol).

Goal
----
Provide a single deterministic mapping used across the hot-path
components (tick_processor / orderflow_strategy / gates) so that:
  - bucket naming is consistent (observability + enforcement allowlists)
  - minor label variations don't silently break regime logic

Buckets
-------
  NORMAL
  LOW_LIQ
  HIGH_VOL
  HIGH_VOL_LOW_LIQ

Inputs are *labels* (strings) produced by the liquidity guard and
volatility regime tracker.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class ExecRegimeBucketResult:
    bucket: str
    low_liq: int
    high_vol: int


def _norm(s: str) -> str:
    return str(s or "").strip().lower().replace("-", "_").replace(" ", "_")


def compute_exec_regime_bucket(
    *,
    liq_regime_label: str,
    vol_regime_label: str,
) -> ExecRegimeBucketResult:
    """Compute execution-regime bucket from liquidity/volatility labels.

    Liquidity regimes are intentionally permissive on naming:
      low / very_low / stressed / illiquid / low_liq / extreme_low

    Volatility regimes come from VolRegimeTracker (shock/normal/calm/na),
    but we also accept broader aliases (high/extreme/high_vol).
    """
    lr = _norm(liq_regime_label)
    vr = _norm(vol_regime_label)

    low_liq = 1 if lr in (
        "low", "very_low", "stressed", "illiquid",
        "low_liq", "extreme_low", "thin", "news",
    ) else 0

    high_vol = 1 if vr in (
        "shock", "high", "extreme", "high_vol", "extreme_vol",
    ) else 0

    if low_liq and high_vol:
        b = "HIGH_VOL_LOW_LIQ"
    elif high_vol:
        b = "HIGH_VOL"
    elif low_liq:
        b = "LOW_LIQ"
    else:
        b = "NORMAL"

    return ExecRegimeBucketResult(bucket=b, low_liq=low_liq, high_vol=high_vol)
