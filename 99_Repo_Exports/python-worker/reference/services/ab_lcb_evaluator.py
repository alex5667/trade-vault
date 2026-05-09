from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import Any


@dataclass
class ArmMoments:
    """
    Minimal sufficient statistics for LCB(mean R):
      - n
      - sum_r
      - sum_r2
    From these we can compute mean and (sample) variance.

    Notes:
      - This is robust for hourly evaluation; do not store raw trades.
      - If upstream only provides mean/std, we can still accept that (see from_stats()).
    """
    n: int = 0
    sum_r: float = 0.0
    sum_r2: float = 0.0

    def mean(self) -> float:
        return (self.sum_r / self.n) if self.n > 0 else 0.0

    def var(self) -> float:
        # sample variance (unbiased) if n>=2, else 0
        if self.n < 2:
            return 0.0
        m = self.mean()
        # E[x^2] - mean^2
        ex2 = self.sum_r2 / float(self.n)
        v = max(0.0, ex2 - m * m)
        # unbiased-ish: scale by n/(n-1)
        return v * float(self.n) / float(self.n - 1)

    def std(self) -> float:
        return math.sqrt(max(0.0, self.var()))


@dataclass
class LCBEvalConfig:
    """
    Per-regime LCB configuration.
    - z: confidence multiplier (Normal approx)
    - min_n: minimum samples per arm to be eligible
    - min_edge_lcb: minimum improvement in LCB vs baseline to switch
    - r_clip: winsorize R values at [-r_clip, +r_clip] if we build moments from raw
    """
    z: float = 1.64
    min_n: int = 200
    min_edge_lcb: float = 0.00
    r_clip: float = 5.0

    @staticmethod
    def for_regime(regime: str) -> LCBEvalConfig:
        rg = (regime or "na").strip().lower()

        # Defaults (safe, conservative):
        # - trend: allow faster switching (lower z, lower min_n)
        # - range/mixed: medium
        # - thin/news/illiquid: strict (higher z, higher min_n)
        if rg in ("trend", "trending_bull", "trending_bear"):
            z = float(os.getenv("AB_LCB_Z_TREND", "1.28"))          # ~90%
            min_n = int(os.getenv("AB_LCB_MIN_N_TREND", "120"))
        elif rg in ("thin", "news", "illiquid"):
            z = float(os.getenv("AB_LCB_Z_THIN", "1.96"))           # ~97.5%
            min_n = int(os.getenv("AB_LCB_MIN_N_THIN", "220"))
        else:
            # range/mixed/na
            z = float(os.getenv("AB_LCB_Z_RANGE", "1.64"))          # ~95%
            min_n = int(os.getenv("AB_LCB_MIN_N_RANGE", "180"))

        min_edge_lcb = float(os.getenv("AB_LCB_MIN_EDGE_LCB", "0.00"))
        r_clip = float(os.getenv("AB_LCB_R_CLIP", "5.0"))
        return LCBEvalConfig(z=z, min_n=min_n, min_edge_lcb=min_edge_lcb, r_clip=r_clip)


def _clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def lcb_mean(m: float, s: float, n: int, z: float) -> float:
    """
    Lower Confidence Bound for mean with Normal approximation:
      LCB = mean - z * (std / sqrt(n))
    If n<2 or std==0 => LCB == mean (fail-open).
    """
    if n <= 0:
        return float("-inf")
    if n < 2 or s <= 0:
        return float(m)
    se = s / math.sqrt(float(n))
    return float(m - z * se)


def moments_from_stats(d: dict[str, Any]) -> ArmMoments:
    """
    Accept multiple upstream formats:
      - {n, sum_r, sum_r2}
      - {n, mean_r, std_r} or {n, mean_r, var_r}
    """
    n = int(d.get("n", 0) or 0)
    if n <= 0:
        return ArmMoments(n=0, sum_r=0.0, sum_r2=0.0)

    if "sum_r" in d and "sum_r2" in d:
        return ArmMoments(
            n=n,
            sum_r=float(d.get("sum_r", 0.0) or 0.0),
            sum_r2=float(d.get("sum_r2", 0.0) or 0.0),
        )

    mean_r = float(d.get("mean_r", 0.0) or d.get("avg_r", 0.0) or 0.0)
    if "var_r" in d:
        var_r = max(0.0, float(d.get("var_r", 0.0) or 0.0))
        # reconstruct sum_r2: var = (sum(x^2) - n*mean^2)/(n-1)
        # => sum(x^2) = (n-1)*var + n*mean^2
        sum_r2 = float(max(0.0, (n - 1) * var_r + n * mean_r * mean_r))
        return ArmMoments(n=n, sum_r=float(n) * mean_r, sum_r2=sum_r2)

    std_r = float(d.get("std_r", 0.0) or 0.0)
    if std_r > 0 and n >= 2:
        var_r = std_r * std_r
        sum_r2 = float(max(0.0, (n - 1) * var_r + n * mean_r * mean_r))
        return ArmMoments(n=n, sum_r=float(n) * mean_r, sum_r2=sum_r2)

    # Fallback: no dispersion info => treat variance=0 (LCB==mean). Still deterministic.
    return ArmMoments(n=n, sum_r=float(n) * mean_r, sum_r2=float(n) * mean_r * mean_r)


@dataclass
class WinnerDecision:
    winner: str
    ok: bool
    reason: str
    baseline: str = "A"
    z: float = 0.0
    min_n: int = 0
    min_edge_lcb: float = 0.0
    # diagnostics
    lcb_by_arm: dict[str, float] = None
    mean_by_arm: dict[str, float] = None
    n_by_arm: dict[str, int] = None


def choose_winner_lcb(
    *,
    stats_by_arm: dict[str, dict[str, Any]],
    regime: str,
    baseline_arm: str = "A",
) -> WinnerDecision:
    """
    Choose winner using LCB(mean R) per regime.

    Rules:
      1) Arms must have n >= min_n to be eligible (baseline may be eligible with lower n? -> no, keep consistent).
      2) Compute LCB for each eligible arm.
      3) Winner = argmax LCB.
      4) Switch away from baseline only if LCB(winner) - LCB(baseline) >= min_edge_lcb.

    Fail-open:
      - If baseline not eligible => keep baseline, ok=False, reason="baseline_not_ready".
      - If no arm eligible => keep baseline, ok=False.
    """
    cfg = LCBEvalConfig.for_regime(regime)
    base = (baseline_arm or "A").strip().upper()

    moms: dict[str, ArmMoments] = {}
    for arm, d in (stats_by_arm or {}).items():
        a = (arm or "").strip().upper()
        if a not in ("A", "B", "C", "D"):
            continue
        if not isinstance(d, dict):
            continue
        moms[a] = moments_from_stats(d)

    lcb_map: dict[str, float] = {}
    mean_map: dict[str, float] = {}
    n_map: dict[str, int] = {}

    eligible = []
    for a, m in moms.items():
        n_map[a] = int(m.n)
        mean_map[a] = float(m.mean())
        # eligibility
        if m.n >= cfg.min_n:
            eligible.append(a)
            lcb_map[a] = lcb_mean(m.mean(), m.std(), m.n, cfg.z)

    if base not in eligible:
        return WinnerDecision(
            winner=base,
            ok=False,
            reason="baseline_not_ready",
            baseline=base,
            z=cfg.z,
            min_n=cfg.min_n,
            min_edge_lcb=cfg.min_edge_lcb,
            lcb_by_arm=lcb_map,
            mean_by_arm=mean_map,
            n_by_arm=n_map,
        )

    if not eligible:
        return WinnerDecision(
            winner=base,
            ok=False,
            reason="no_eligible_arms",
            baseline=base,
            z=cfg.z,
            min_n=cfg.min_n,
            min_edge_lcb=cfg.min_edge_lcb,
            lcb_by_arm=lcb_map,
            mean_by_arm=mean_map,
            n_by_arm=n_map,
        )

    # winner among eligible
    best = base
    best_lcb = lcb_map.get(base, float("-inf"))
    for a in eligible:
        v = float(lcb_map.get(a, float("-inf")))
        if v > best_lcb:
            best = a
            best_lcb = v

    base_lcb = float(lcb_map.get(base, float("-inf")))
    if best != base:
        edge = best_lcb - base_lcb
        if edge < float(cfg.min_edge_lcb):
            return WinnerDecision(
                winner=base,
                ok=False,
                reason=f"edge_below_min_lcb edge={edge:.4f}",
                baseline=base,
                z=cfg.z,
                min_n=cfg.min_n,
                min_edge_lcb=cfg.min_edge_lcb,
                lcb_by_arm=lcb_map,
                mean_by_arm=mean_map,
                n_by_arm=n_map,
            )

    return WinnerDecision(
        winner=best,
        ok=True,
        reason="lcb_winner",
        baseline=base,
        z=cfg.z,
        min_n=cfg.min_n,
        min_edge_lcb=cfg.min_edge_lcb,
        lcb_by_arm=lcb_map,
        mean_by_arm=mean_map,
        n_by_arm=n_map,
    )
