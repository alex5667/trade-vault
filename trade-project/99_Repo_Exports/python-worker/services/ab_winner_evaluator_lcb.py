from __future__ import annotations

"""services.ab_winner_evaluator_lcb

LCB evaluator upgraded:
  - per-regime confidence parameters (z, min_n, min_lcb_r)
  - deterministic and stable tie-breaking (prefer A when uncertain)
  - returns rich diagnostics for audit/proposal layer
  - cost-aware LCB (r_adj = r_mult - slip_R - fees_R)

This file is used by ABWinnerSuggesterV2.
"""


import math
import os
from dataclasses import dataclass
from typing import Any


def _rg(x: Any) -> str:
    try:
        return (x or "na").strip().lower() or "na"
    except Exception:
        return "na"


def _arm(x: Any) -> str:
    v = (x or "").strip().upper()
    return v if v in ("A", "B", "C") else ""


def _f(x: Any, d: float = 0.0) -> float:
    try:
        if x is None:
            return d
        return float(x)
    except Exception:
        return d


def compute_r_adj(payload: dict) -> float:
    """
    r_adj = r_mult - slip_R - fees_R
    slip_usd = turnover_roundtrip * (expected_slippage_bps/10000)
    """
    r_mult = _f(payload.get("r_mult"), 0.0)
    risk_usd = max(1e-9, _f(payload.get("risk_usd"), 0.0))
    turn = _f(payload.get("turnover_roundtrip"), 0.0)
    slip_bps = _f(payload.get("p0_slippage_bps_est"), _f(payload.get("expected_slippage_bps_at_entry"), 0.0))
    fees_usd = _f(payload.get("fees_usd"), 0.0)

    slip_usd = max(0.0, turn) * (max(0.0, slip_bps) / 10000.0)
    slip_R = slip_usd / risk_usd

    fees_already_net = os.getenv("LCB_FEES_ALREADY_NET", "1") == "1"
    subtract_fees = os.getenv("LCB_SUBTRACT_FEES", "0") == "1"
    fees_R = (fees_usd / risk_usd) if (subtract_fees and not fees_already_net) else 0.0

    return float(r_mult - slip_R - fees_R)


@dataclass
class LCBPick:
    arm: str
    regime: str
    mean_r: float
    std_r: float
    n: int
    lcb_r: float
    z: float
    min_n: int
    min_lcb_r: float
    reason: str


class LCBEvaluatorPerRegime:
    """Lower Confidence Bound (LCB) evaluator per regime.

    rows: list of dicts, each row must contain at least:
      - ab_arm: "A"|"B"|"C"
      - r_mult: float (R multiple)
      - regime: string

    Policy:
      - compute mean/std per arm
      - LCB = mean - z * std/sqrt(n)
      - select arm with max LCB, but only if it clears min_n and min_lcb_r
      - otherwise pick "A" (safe default) and mark reason as "insufficient_evidence".
    """

    def __init__(self, cfg: dict[str, Any] | None = None) -> None:
        self.cfg = cfg or {}

    def _params(self, regime: str) -> tuple[float, int, float]:
        rg = _rg(regime)
        # Defaults: 80% one-sided (~1.28) for normal regimes,
        #           90% (~1.64) for thin/news where variance and slippage are higher.
        z_default = _f(self.cfg.get("lcb_z_default"), 1.28)
        z_thin = _f(self.cfg.get("lcb_z_thin"), 1.64)
        min_n_default = int(self.cfg.get("min_n_default", 60) or 60)
        min_n_thin = int(self.cfg.get("min_n_thin", 120) or 120)
        min_lcb_default = _f(self.cfg.get("min_lcb_r_default"), 0.05)
        min_lcb_thin = _f(self.cfg.get("min_lcb_r_thin"), 0.10)

        if rg in ("thin", "news", "illiquid"):
            return z_thin, min_n_thin, min_lcb_thin
        return z_default, min_n_default, min_lcb_default

    @staticmethod
    def _mean_std(xs: list[float]) -> tuple[float, float]:
        if not xs:
            return 0.0, 0.0
        n = len(xs)
        mu = sum(xs) / float(n)
        if n < 2:
            return mu, 0.0
        var = sum((x - mu) ** 2 for x in xs) / float(n - 1)
        return mu, math.sqrt(max(0.0, var))

    def pick_winner(self, rows: list[dict[str, Any]]) -> dict[str, Any] | None:
        if not rows:
            return None
        # regime is constant per key in suggester, but tolerate mixed input (take majority).
        regs = [_rg(r.get("regime")) for r in rows]
        regime = max(set(regs), key=regs.count) if regs else "na"
        z, min_n, min_lcb_r = self._params(regime)

        by_arm: dict[str, list[float]] = {"A": [], "B": [], "C": []}
        cost_aware = os.getenv("LCB_COST_AWARE_ENABLE", "0") == "1"
        for r in rows:
            arm = _arm(r.get("ab_arm") or r.get("arm") or "")
            if not arm:
                continue
            # Use r_adj (cost-aware) if enabled, otherwise r_mult
            if cost_aware:
                rr = compute_r_adj(r)
            else:
                rr = _f(r.get("r_mult"), None)  # type: ignore
            if rr is None or not math.isfinite(rr):
                continue
            by_arm[arm].append(float(rr))

        picks: list[LCBPick] = []
        for arm, xs in by_arm.items():
            n = len(xs)
            mu, sd = self._mean_std(xs)
            se = (sd / math.sqrt(n)) if (n > 0 and sd > 0) else 0.0
            lcb = mu - z * se
            picks.append(
                LCBPick(
                    arm=arm,
                    regime=regime,
                    mean_r=float(mu),
                    std_r=float(sd),
                    n=int(n),
                    lcb_r=float(lcb),
                    z=float(z),
                    min_n=int(min_n),
                    min_lcb_r=float(min_lcb_r),
                    reason="",
                )
            )

        # Choose best by LCB, tie-break by n then mean.
        picks_sorted = sorted(picks, key=lambda p: (p.lcb_r, p.n, p.mean_r), reverse=True)
        best = picks_sorted[0]

        # Safety gates (avoid flapping and false positives)
        if best.n < min_n:
            best = LCBPick(**{**best.__dict__, "arm": "A", "reason": "insufficient_n"})
        elif best.lcb_r < min_lcb_r:
            best = LCBPick(**{**best.__dict__, "arm": "A", "reason": "lcb_below_floor"})
        else:
            best = LCBPick(**{**best.__dict__, "reason": "ok"})

        # return dict (backward-friendly)
        return {
            "winner_arm": best.arm,
            "regime": best.regime,
            "lcb_r": best.lcb_r,
            "mean_r": best.mean_r,
            "std_r": best.std_r,
            "n": best.n,
            "z": best.z,
            "min_n": best.min_n,
            "min_lcb_r": best.min_lcb_r,
            "reason": best.reason,
            "arms": [
                {
                    "arm": p.arm,
                    "n": p.n,
                    "mean_r": p.mean_r,
                    "std_r": p.std_r,
                    "lcb_r": p.lcb_r,
                }
                for p in picks_sorted
            ],
        }


# Backward-compatible alias
def choose_winner(rows: list[dict[str, Any]], cfg: dict[str, Any] | None = None) -> tuple[str, str]:
    """Legacy interface: returns (winner_arm, reason)."""
    out = LCBEvaluatorPerRegime(cfg=cfg).pick_winner(rows) or {}
    return (out.get("winner_arm") or "A"), (out.get("reason") or "")

