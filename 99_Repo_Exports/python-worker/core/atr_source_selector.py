# -*- coding: utf-8 -*-
from __future__ import annotations
"""
ATR Source Selector (ATR TF Calibrator core)
==========================================

Цель
----
Детерминированно выбрать "лучший" ATR-источник из нескольких Redis ключей:
  - ATR:{SYM}:{TF} (tracker hash)
  - atr:{sym}:{tf} (string)
  - atr:val:{sym}:{tf} (legacy mirror)
  - atr:json:{sym}:{tf} (json + ts)
  - ta:last:atr:{sym} (json, возможно cross-tf)

Критерии выбора
--------------
1) Freshness: чем свежее (age_ms меньше), тем лучше.
2) TF match: совпадает ли TF с запрошенным (строго по умолчанию).
3) Consistency: если есть несколько кандидатов, штрафуем выбросы относительно медианы.

Важно
-----
* Fail-open: если ничего адекватного нет — возвращаем None.
* Детерминизм времени: используйте now_ms из сигнала/бара, а не wall-clock.
"""


import math
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class AtrCandidate:
    src: str
    key: str
    tf: str
    atr: float
    ts_ms: int = 0
    age_ms: int = 0
    tf_match: int = 1
    score: float = 0.0
    penalty_consistency: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _freshness_score(age_ms: int, *, half_life_ms: int) -> float:
    """
    Freshness in (0..1]. Exponential decay with half-life.
    age_ms<=0 => 1.0
    """
    try:
        a = int(age_ms or 0)
        hl = int(half_life_ms or 0)
        if a <= 0:
            return 1.0
        if hl <= 0:
            # If not configured, degrade gently:
            return 0.5
        # exp(-ln(2)*age/hl)
        return float(math.exp(-0.69314718056 * (float(a) / float(hl))))
    except Exception:
        return 0.0


def _median(xs: List[float]) -> float:
    ys = sorted(xs)
    n = len(ys)
    if n == 0:
        return 0.0
    mid = n // 2
    return float(ys[mid]) if (n % 2 == 1) else float(0.5 * (ys[mid - 1] + ys[mid]))


def _consistency_penalty(cands: List[AtrCandidate]) -> Dict[str, float]:
    """
    Returns per-key penalty in [0..1], where 0 = no penalty, 1 = huge penalty.
    Robust: compare ratio to median ATR among candidates (only finite >0).
    """
    vals: List[float] = []
    for c in cands:
        if math.isfinite(c.atr) and c.atr > 0:
            vals.append(float(c.atr))
    if len(vals) < 2:
        return {}
    med = _median(vals)
    if med <= 0:
        return {}
    out: Dict[str, float] = {}
    for c in cands:
        if not (math.isfinite(c.atr) and c.atr > 0):
            continue
        r = float(c.atr) / float(med)
        # 0 penalty if within ±30% of median.
        # linear penalty grows beyond that, capped at 1.0.
        dev = abs(r - 1.0)
        if dev <= 0.30:
            p = 0.0
        else:
            p = min(1.0, (dev - 0.30) / 0.70)
        out[c.key] = float(p)
    return out


def select_best_atr_candidate(
    *,
    desired_tf: str,
    candidates: List[AtrCandidate],
    now_ms: int,
    allow_tf_mismatch: bool = False,
    half_life_ms: int = 10 * 60 * 1000,  # 10 min
    src_priority: Optional[Dict[str, float]] = None,
) -> Tuple[Optional[AtrCandidate], Dict[str, Any]]:
    """
    Returns (best_candidate, meta).
    meta includes: chosen_src, chosen_key, chosen_tf, reasons, candidates[]
    """
    tf0 = str(desired_tf or "").upper().strip()
    nm = int(now_ms or 0)
    if nm <= 0:
        nm = 0

    # Default priorities: higher is better.
    pr = src_priority or {
        "tracker": 1.00,
        "atr_json": 0.90,
        "ta_last": 0.80,
        "atr_str": 0.60,
        "atr_val": 0.55,
    }

    # Normalize and pre-filter
    good: List[AtrCandidate] = []
    for c in candidates:
        try:
            if not (math.isfinite(c.atr) and c.atr > 0):
                continue
            c.tf = str(c.tf or "").upper().strip()
            if tf0 and c.tf and (c.tf != tf0):
                c.tf_match = 0
            else:
                c.tf_match = 1
            # Compute age if possible
            if nm > 0 and int(c.ts_ms or 0) > 0:
                c.age_ms = int(max(0, nm - int(c.ts_ms)))
            else:
                c.age_ms = int(c.age_ms or 0)
            good.append(c)
        except Exception:
            continue

    # TF strict by default: keep only matching TF
    filtered = [c for c in good if c.tf_match == 1] if not allow_tf_mismatch else list(good)
    if not filtered:
        return None, {
            "ok": 0,
            "reason": "no_candidates",
            "desired_tf": tf0,
            "allow_tf_mismatch": int(1 if allow_tf_mismatch else 0),
            "candidates": [c.to_dict() for c in good],
        }

    # Consistency penalties
    pen = _consistency_penalty(filtered)

    # Score candidates
    for c in filtered:
        f = _freshness_score(c.age_ms, half_life_ms=half_life_ms) if c.ts_ms > 0 else 0.20
        p_src = float(pr.get(str(c.src), 0.50))
        p_cons = float(pen.get(c.key, 0.0))
        c.penalty_consistency = p_cons
        # Score: (priority * freshness) * (1 - penalty)
        c.score = float((p_src * f) * (1.0 - 0.70 * p_cons))

    filtered.sort(key=lambda x: x.score, reverse=True)
    best = filtered[0]

    meta = {
        "ok": 1,
        "desired_tf": tf0,
        "chosen": best.to_dict(),
        "half_life_ms": int(half_life_ms),
        "allow_tf_mismatch": int(1 if allow_tf_mismatch else 0),
        "candidates": [c.to_dict() for c in filtered[:8]],  # keep small
    }
    return best, meta
