"""
calibration/promotion_gate.py — Plan 3 / Step 4 PBO/DSR/ECE promotion gate.

Pure-Python validation that a candidate model/config has cleared the
out-of-sample quality bars before it is allowed to move to SHADOW → CANARY → ENFORCE.

Hard-rule gates only — no ranking, no auto-promote. Returns (passed, reasons)
so an external workflow (or the user) makes the actual promotion call.

References:
  * Bailey & López de Prado (2014/2016) — Deflated Sharpe Ratio.
  * Bailey, Borwein, López de Prado, Zhu (2015) — Probability of Backtest Overfitting.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


# ─── ENV defaults (overridable per-run) ──────────────────────────────────────


def _env_int(k: str, d: int) -> int:
    try:
        return int(os.environ.get(k, str(d)))
    except Exception:
        return d


def _env_float(k: str, d: float) -> float:
    try:
        return float(os.environ.get(k, str(d)))
    except Exception:
        return d


@dataclass(frozen=True)
class PromotionThresholds:
    """Tunable bars; defaults come from ENV (operator can override per-run)."""

    min_oos_trades: int = field(default_factory=lambda: _env_int("PROMOTION_MIN_OOS_TRADES", 300))
    min_oos_days: int = field(default_factory=lambda: _env_int("PROMOTION_MIN_OOS_DAYS", 14))
    max_pbo: float = field(default_factory=lambda: _env_float("PROMOTION_MAX_PBO", 0.25))
    min_deflated_sharpe: float = field(default_factory=lambda: _env_float("PROMOTION_MIN_DEFLATED_SHARPE", 0.0))
    max_ece: float = field(default_factory=lambda: _env_float("PROMOTION_MAX_ECE", 0.07))
    min_pass_rate: float = field(default_factory=lambda: _env_float("PROMOTION_MIN_PASS_RATE", 0.02))
    # Optional slippage residual gate: skipped when None
    max_slippage_residual_p95_bps: float | None = None


@dataclass(frozen=True)
class PromotionMetrics:
    """OOS evidence collected by the candidate evaluation pipeline."""

    n_oos_trades: int
    n_oos_days: int
    mean_oos_profit_factor: float
    mean_oos_sharpe: float
    deflated_sharpe: float
    pbo: float
    ece: float
    brier: float
    pass_rate: float
    slippage_residual_p95_bps: float | None = None


def can_promote(
    m: PromotionMetrics,
    thr: PromotionThresholds | None = None,
) -> tuple[bool, list[str]]:
    """Return (passed, list-of-failed-reason-codes).

    Empty list ↔ all gates passed. Each reason code is a stable identifier
    suitable for log/alert label values. The intent is hard reject — any
    failure blocks promotion; the caller decides whether to retry.
    """
    thr = thr or PromotionThresholds()
    reasons: list[str] = []

    if m.n_oos_trades < thr.min_oos_trades:
        reasons.append("oos_trades_too_low")
    if m.n_oos_days < thr.min_oos_days:
        reasons.append("oos_days_too_low")
    if m.pbo > thr.max_pbo:
        reasons.append("pbo_too_high")
    if m.deflated_sharpe <= thr.min_deflated_sharpe:
        reasons.append("deflated_sharpe_non_positive")
    if m.ece > thr.max_ece:
        reasons.append("ece_too_high")
    if m.pass_rate < thr.min_pass_rate:
        reasons.append("pass_rate_too_low")
    if thr.max_slippage_residual_p95_bps is not None and m.slippage_residual_p95_bps is not None:
        if m.slippage_residual_p95_bps > thr.max_slippage_residual_p95_bps:
            reasons.append("slippage_residual_too_high")

    return (len(reasons) == 0, reasons)
