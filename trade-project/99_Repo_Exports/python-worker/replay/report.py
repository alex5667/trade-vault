from __future__ import annotations

"""
Replay report builder:
  - counts_by_kind
  - score percentiles by kind (final_score)
  - optional sanity checks (NaN/Inf)
"""

import math
from dataclasses import dataclass
from typing import Any


def _isfinite(x: float) -> bool:
    return not (math.isnan(x) or math.isinf(x))


def _pct(xs: list[float], q: float) -> float:
    if not xs:
        return 0.0
    ys = sorted(xs)
    if q <= 0:
        return float(ys[0])
    if q >= 1:
        return float(ys[-1])
    i = int(round((len(ys) - 1) * q))
    i = max(0, min(len(ys) - 1, i))
    return float(ys[i])


@dataclass
class Report:
    counts_by_kind: dict[str, int]
    score_p50_by_kind: dict[str, float]
    score_p95_by_kind: dict[str, float]


def normalize_signal_payload(p: dict[str, Any]) -> dict[str, Any]:
    """
    Нормализация для golden comparisons:
      - выкидываем поля, которые могут дрейфовать (uuid, ts fine-grain и т.п.)
      - оставляем "смысловые" поля
    """
    keep = (
        "kind",
        "side",
        "symbol",
        "ts",
        "price",
        "level_price",
        "raw_score",
        "final_score",
        "confidence",
        "reasons",
        "qf",
        "qf16",
    )
    out: dict[str, Any] = {}
    for k in keep:
        if k in p:
            out[k] = p[k]
    # enforce sane types for golden stability
    if "reasons" in out and not isinstance(out["reasons"], list):
        out["reasons"] = [str(out["reasons"])]
    return out


def build_report(signals: list[dict[str, Any]]) -> Report:
    counts: dict[str, int] = {}
    scores: dict[str, list[float]] = {}

    for p in signals:
        k = (p.get("kind", "") or "")
        counts[k] = counts.get(k, 0) + 1
        s = p.get("final_score", None)
        try:
            sf = float(s) if s is not None else 0.0
        except Exception:
            sf = 0.0
        if not _isfinite(sf):
            sf = 0.0
        scores.setdefault(k, []).append(sf)

    p50: dict[str, float] = {}
    p95: dict[str, float] = {}
    for k, xs in scores.items():
        p50[k] = _pct(xs, 0.50)
        p95[k] = _pct(xs, 0.95)

    return Report(counts_by_kind=counts, score_p50_by_kind=p50, score_p95_by_kind=p95)
