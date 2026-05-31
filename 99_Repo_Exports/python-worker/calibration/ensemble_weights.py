"""
ensemble_weights.py — Phase 3.1 dynamic source weighting.

For each (symbol, source) computes a "skill" score from rolling OOS metrics
in signal_outcome / so_daily, then blends sources via softmax with an
exponential time decay.

Skill metrics supported:
  * neg_log_loss : -E[ y * log(p) + (1-y) * log(1-p) ] (lower is better → flip sign)
  * sharpe       : mean(realized_r) / std(realized_r), annualized factor optional

Guards:
  * MIN_SAMPLES per source → no weight if below
  * temperature clamp on softmax to avoid degenerate concentration
  * equal-weight fallback when every source is below MIN_SAMPLES

Pure-Python; service wrapper in
`orderflow_services/ensemble_weights_publisher_v1.py`.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable

_MIN_SAMPLES = 100
_TEMPERATURE = 1.0
_HALFLIFE_DAYS = 10.0
_EPS = 1e-9


@dataclass(frozen=True)
class SourceSkill:
    source: str
    n: int
    skill: float          # higher is better
    weight: float         # 0..1, sum to 1 within (symbol)
    avg_calib_prob: float
    avg_realized_r: float


def _clip01(x: float) -> float:
    return max(_EPS, min(1.0 - _EPS, float(x)))


def neg_log_loss(probs: list[float], labels: list[int]) -> float:
    """Returns mean cross-entropy loss. Lower = better.

    label encoding: 1=win, 0=loss/vertical."""
    if not probs:
        return float("inf")
    total = 0.0
    n = 0
    for p, y in zip(probs, labels):
        p = _clip01(p)
        if y == 1:
            total += -math.log(p)
        else:
            total += -math.log(1.0 - p)
        n += 1
    return total / max(n, 1)


def sharpe(returns: list[float]) -> float:
    if len(returns) < 2:
        return 0.0
    mean = sum(returns) / len(returns)
    var = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
    if var <= 0:
        return 0.0
    return mean / math.sqrt(var)


def _softmax(scores: list[float], temperature: float) -> list[float]:
    if not scores:
        return []
    t = max(temperature, _EPS)
    m = max(scores)
    exps = [math.exp((s - m) / t) for s in scores]
    z = sum(exps)
    if z <= 0:
        return [1.0 / len(scores)] * len(scores)
    return [e / z for e in exps]


def _decay_weight(age_days: float, halflife_days: float) -> float:
    if halflife_days <= 0:
        return 1.0
    return math.exp(-math.log(2.0) * max(age_days, 0.0) / halflife_days)


def compute_weights(
    rows: Iterable[dict],
    metric: str = "neg_log_loss",
    min_samples: int = _MIN_SAMPLES,
    temperature: float = _TEMPERATURE,
    halflife_days: float = _HALFLIFE_DAYS,
    now_ms: int | None = None,
) -> dict[str, list[SourceSkill]]:
    """rows: dicts with keys symbol, source, calib_prob, realized_r, label,
                                  decision_time_ms.

    Returns dict[symbol] -> list[SourceSkill] (weights sum to 1 within symbol).
    """
    by_sym_src: dict[tuple[str, str], list[dict]] = {}
    for r in rows:
        sym = str(r.get("symbol", "")).upper()
        src = str(r.get("source", ""))
        if not sym or not src:
            continue
        by_sym_src.setdefault((sym, src), []).append(r)

    if now_ms is None:
        import time
        now_ms = int(time.time() * 1000)

    out: dict[str, list[SourceSkill]] = {}
    grouped_by_sym: dict[str, list[tuple[str, list[dict]]]] = {}
    for (sym, src), recs in by_sym_src.items():
        grouped_by_sym.setdefault(sym, []).append((src, recs))

    for sym, src_groups in grouped_by_sym.items():
        skills: list[tuple[str, int, float, float, float]] = []
        for src, recs in src_groups:
            n = len(recs)
            if n < min_samples:
                continue
            # Apply time decay per-sample
            wsum = 0.0
            wn = 0.0
            wprobs = []
            wlabels = []
            wreturns = []
            avg_cp = 0.0
            avg_rr = 0.0
            for r in recs:
                age_days = max(0.0, (now_ms - int(r.get("decision_time_ms") or now_ms)) / 86_400_000.0)
                w = _decay_weight(age_days, halflife_days)
                wsum += w
                wn += 1
                # collect for metric
                if r.get("calib_prob") is not None:
                    wprobs.append(float(r["calib_prob"]))
                    wlabels.append(1 if int(r.get("label") or 0) == 1 else 0)
                if r.get("realized_r") is not None:
                    wreturns.append(float(r["realized_r"]))
                avg_cp += float(r.get("calib_prob") or 0.0) * w
                avg_rr += float(r.get("realized_r") or 0.0) * w

            avg_cp = avg_cp / wsum if wsum > 0 else 0.0
            avg_rr = avg_rr / wsum if wsum > 0 else 0.0

            if metric == "sharpe":
                skill = sharpe(wreturns)
            else:  # neg_log_loss
                if wprobs:
                    skill = -neg_log_loss(wprobs, wlabels)
                else:
                    # No calibrated probs → use avg realized_r as a proxy
                    skill = avg_rr
            skills.append((src, n, skill, avg_cp, avg_rr))

        if not skills:
            # No source passes — fallback to equal weight across all observed
            seen_src = [src for src, _ in src_groups]
            if seen_src:
                w = 1.0 / len(seen_src)
                out[sym] = [
                    SourceSkill(source=s, n=0, skill=0.0, weight=w,
                                avg_calib_prob=0.0, avg_realized_r=0.0)
                    for s in seen_src
                ]
            continue

        score_vec = [s[2] for s in skills]
        weights = _softmax(score_vec, temperature)
        out[sym] = [
            SourceSkill(
                source=src, n=n, skill=skill, weight=w,
                avg_calib_prob=avg_cp, avg_realized_r=avg_rr,
            )
            for (src, n, skill, avg_cp, avg_rr), w in zip(skills, weights)
        ]
    return out


def to_redis_payload(weights: dict[str, list[SourceSkill]]) -> dict[str, dict[str, str]]:
    """Returns {symbol: {source: "weight"}} ready for HSET per-symbol."""
    out: dict[str, dict[str, str]] = {}
    for sym, items in weights.items():
        out[sym] = {it.source: f"{it.weight:.6f}" for it in items}
    return out
