# -*- coding: utf-8 -*-
"""
AB Winner Evaluator (LCB, per-regime)
====================================

Goal
----
Select winner arm (A/B/C) conservatively using Lower Confidence Bound (LCB)
on mean R-multiple (r_mult). This reduces the chance of switching to a noisy arm.

Why LCB
-------
Mean-only selection overfits in small samples. LCB = mean - z * stderr
is a simple, robust, production-friendly conservative estimate.

Design rules
------------
- Deterministic inputs: only events:trades POSITION_CLOSED fields.
- Fail-open to A when insufficient data / invalid.
- Per-regime policy: different confidence / min samples / min edge.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass
class RegimePolicy:
    """
    v1 Policy knobs per regime label.
    """
    conf: float = 0.95
    min_n: int = 80
    min_edge_lcb: float = 0.10  # in R


@dataclass
class RegimeThresholds:
    """
    v2 Policy knobs for AB evaluation.
    """
    min_n: int
    min_lcb_r: float
    min_lcb_wr: float
    min_delta_lcb_vs_a: float
    z: float


@dataclass
class ArmScore:
    arm: str
    n: int
    mean: float
    stdev: float
    stderr: float
    lcb: float


@dataclass
class WinnerDecision:
    winner: str
    ok: bool
    reason: str
    lcb_r: Dict[str, float]
    lcb_wr: Dict[str, float]
    n: Dict[str, int]
    baseline_a_lcb_r: float
    delta_lcb_vs_a: float


def _z_for_conf(conf: float) -> float:
    """
    Map common confidence levels to z-values (normal approximation).
    We avoid scipy dependency in production.
    """
    c = float(conf)
    if c >= 0.995:
        return 2.807
    if c >= 0.990:
        return 2.576
    if c >= 0.975:
        return 2.241
    if c >= 0.950:
        return 1.960
    if c >= 0.900:
        return 1.645
    if c >= 0.850:
        return 1.440
    return 1.282


def _safe_mean(xs: List[float]) -> float:
    if not xs:
        return 0.0
    return float(sum(xs) / float(len(xs)))


def _safe_stdev(xs: List[float]) -> float:
    n = len(xs)
    if n < 2:
        return 0.0
    mu = _safe_mean(xs)
    v = sum((float(x) - mu) ** 2 for x in xs)
    return float(math.sqrt(max(0.0, v / (n - 1))))


def lcb_mean(xs: List[float], *, conf: Optional[float] = None, z: Optional[float] = None) -> Tuple[float, float, float, float]:
    """
    Returns (mean, stdev, stderr, lcb) for list xs.
    """
    n = len(xs)
    if n <= 0:
        return 0.0, 0.0, 0.0, -1e9
    mu = _safe_mean(xs)
    sd = _safe_stdev(xs)
    se = sd / math.sqrt(float(n))
    z_val = z if z is not None else (_z_for_conf(conf) if conf is not None else 1.96)
    lcb = mu - z_val * se
    return float(mu), float(sd), float(se), float(lcb)


def eval_winner_lcb(
    *,
    samples_by_arm: Dict[str, List[float]],
    regime: str,
    group: str = "default",
    scenario: str = "default",
    thr_by_regime: Dict[str, RegimeThresholds],
    default_z: float = 1.96,
    allow_abc: Tuple[str, ...] = ("A", "B", "C"),
) -> WinnerDecision:
    """
    v2 API for selecting the winner arm.
    """
    thr = thr_by_regime.get(regime) or thr_by_regime.get("default")
    if not thr:
        # Fallback if no threshold map provided
        thr = RegimeThresholds(min_n=80, min_lcb_r=0.0, min_lcb_wr=0.0, min_delta_lcb_vs_a=0.0, z=default_z)

    lcb_r = {}
    lcb_wr = {}
    n_map = {}
    
    for arm in allow_abc:
        xs = samples_by_arm.get(arm, []) or []
        n = len(xs)
        n_map[arm] = n
        mu, sd, se, lcb = lcb_mean(xs, z=thr.z)
        lcb_r[arm] = lcb
        
        # Win rate LCB (simplified Bernoulli LCB)
        wins = [1.0 if x > 0 else 0.0 for x in xs]
        wr_mu, wr_sd, wr_se, wr_lcb = lcb_mean(wins, z=thr.z)
        lcb_wr[arm] = wr_lcb

    base_lcb_r = lcb_r.get("A", -1e9)
    best_arm = "A"
    best_lcb_r = base_lcb_r
    
    eligible_arms = []
    for arm in allow_abc:
        if n_map[arm] >= thr.min_n:
            if lcb_r[arm] >= thr.min_lcb_r and lcb_wr[arm] >= thr.min_lcb_wr:
                eligible_arms.append(arm)

    reason = "eligible: " + ",".join(eligible_arms)
    if not eligible_arms:
        return WinnerDecision(winner="A", ok=False, reason="no_eligible_arms", 
                             lcb_r=lcb_r, lcb_wr=lcb_wr, n=n_map, 
                             baseline_a_lcb_r=base_lcb_r, delta_lcb_vs_a=0.0)

    # Choose best among eligible
    for arm in eligible_arms:
        if lcb_r[arm] > best_lcb_r:
            best_lcb_r = lcb_r[arm]
            best_arm = arm

    delta = best_lcb_r - base_lcb_r
    if best_arm != "A" and delta < thr.min_delta_lcb_vs_a:
        return WinnerDecision(winner="A", ok=False, reason=f"delta_below_thr: {delta:.4f} < {thr.min_delta_lcb_vs_a}", 
                             lcb_r=lcb_r, lcb_wr=lcb_wr, n=n_map, 
                             baseline_a_lcb_r=base_lcb_r, delta_lcb_vs_a=delta)

    return WinnerDecision(winner=best_arm, ok=True, reason="winner_selected", 
                         lcb_r=lcb_r, lcb_wr=lcb_wr, n=n_map, 
                         baseline_a_lcb_r=base_lcb_r, delta_lcb_vs_a=delta)


def default_regime_policy(regime: str) -> RegimePolicy:
    from contexts import normalize_regime_label, MARKET_REGIME_NA
    rg = normalize_regime_label(regime)
    if rg in ("trend", "trending_bull", "trending_bear"):
        return RegimePolicy(conf=0.90, min_n=60, min_edge_lcb=0.07)
    if rg in ("range", "mixed"):
        return RegimePolicy(conf=0.95, min_n=80, min_edge_lcb=0.10)
    if rg in ("thin", "news", "illiquid"):
        return RegimePolicy(conf=0.975, min_n=120, min_edge_lcb=0.15)
    return RegimePolicy(conf=0.95, min_n=80, min_edge_lcb=0.10)


def choose_winner_lcb(
    *,
    samples_by_arm: Dict[str, List[float]],
    regime: str,
    policy: Optional[RegimePolicy] = None,
    baseline_arm: str = "A",
    allow_abc: Tuple[str, ...] = ("A", "B", "C"),
) -> Tuple[str, Dict[str, ArmScore], str]:
    pol = policy or default_regime_policy(regime)
    conf = float(pol.conf)
    min_n = int(pol.min_n)
    min_edge = float(pol.min_edge_lcb)

    scores: Dict[str, ArmScore] = {}
    for arm in allow_abc:
        xs = samples_by_arm.get(arm, []) or []
        mu, sd, se, lcb = lcb_mean(xs, conf=conf)
        scores[arm] = ArmScore(arm=arm, n=len(xs), mean=mu, stdev=sd, stderr=se, lcb=lcb)

    base = scores.get(baseline_arm) or ArmScore(arm=baseline_arm, n=0, mean=0, stdev=0, stderr=0, lcb=-1e9)
    if base.n < max(10, min_n // 2):
        return baseline_arm, scores, f"baseline_n_too_small n={base.n}"

    best_arm = baseline_arm
    best_lcb = base.lcb

    for arm, sc in scores.items():
        if arm != baseline_arm and sc.n >= min_n and sc.lcb > best_lcb:
            best_lcb = sc.lcb
            best_arm = arm

    if best_arm == baseline_arm:
        return baseline_arm, scores, "baseline_best_lcb"

    if best_lcb >= (base.lcb + min_edge):
        return best_arm, scores, f"switch lcb={best_lcb:.4f}"

    return baseline_arm, scores, f"no_switch best_lcb={best_lcb:.4f}"

