from __future__ import annotations

"""
common/reason_normalizer.py
--------------------------
"Последняя гайка" против кардинальности и спама:

1) Нормализуем reason -> reason_norm (малый фиксированный словарь).
   Это:
     - делает signals_veto{reason} пригодным для дашборда,
     - позволяет топ-N агрегацию без explosion по тегам,
     - упрощает алерты в Telegram (вместо тысячи уникальных reason'ов).

2) Правила intentionally "lossy":
   - объединяем bo_l2_missing + bo_l2_stale -> bo_l2_fail_closed
   - все touch/spread/cooldown -> стабильные ключи
   - остаточные причины "сжимаем" (токены/обрезка/снятие чисел)
"""

import re

_RE_NUM = re.compile(r"\b\d+(\.\d+)?\b")
_RE_MULTI_SEP = re.compile(r"[:\s]+")


def _clean(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("-", "_")
    s = _RE_NUM.sub("", s)          # remove naked numbers (thresholds, ids)
    s = _RE_MULTI_SEP.sub("_", s)   # unify separators
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def normalize_reason(reason: str, *, kind: str | None = None) -> str:
    r = _clean(reason)
    k = _clean(kind or "")

    if not r:
        return "unknown_veto"

    # ---- Global "protective" buckets (shared across kinds) ----
    if "cooldown" in r:
        return "cooldown"
    if "spread" in r:
        return "spread_filter_veto"
    if "touch" in r:
        return "touch_suppressed"

    # ---- Confidence / thresholds ----
    if "conf_below_min" in r:
        return "conf_below_min_veto"

    # ---- L2 gating: breakout must be fail-closed ----
    # We intentionally collapse many variants into a stable bucket.
    if k == "breakout" or r.startswith("bo_") or "breakout" in k:
        if "l2_missing" in r or "l2_stale" in r:
            return "bo_l2_fail_closed"
        if r.startswith("bo_l2_") or ("l2" in r and "bo" in r):
            return "bo_l2_veto"

    # ---- Absorption / L3 buckets (stable) ----
    if "l3_missing" in r or "l3_unavailable" in r:
        return "l3_missing"
    if r.startswith("l3_") and "veto" in r:
        return "l3_veto"
    if r.startswith("l2_") and "veto" in r:
        return "l2_veto"

    # ---- Fallback: compress to first tokens to avoid cardinality ----
    # Example: "bo_l2_wall_distance_too_far_veto" -> "bo_l2_wall_distance"
    toks = [t for t in r.split("_") if t]
    if not toks:
        return "unknown_veto"
    # drop trailing "veto/fail/closed/open" noise if present
    while toks and toks[-1] in {"veto", "fail", "closed", "open", "reject", "drop"}:
        toks.pop()
    # cap length
    toks = toks[:4] if len(toks) > 4 else toks
    out = "_".join(toks).strip("_")
    return out or "unknown_veto"


def reason_family(reason_norm: str) -> str:
    """
    Второй уровень нормализации (ещё один "¼ гайки"):
    - reason_norm = конкретная стабильная причина (bo_l2_fail_closed, conf_below_min_veto, ...)
    - reason_family = ещё более грубый "класс" (book_l2_gate, confidence_gate, spread_gate, ...)
    Это позволяет:
      - строить дешёвые дашборды по family (низкая кардинальность),
      - алертить "поменялась доминирующая семейство/причина" после релиза.
    """
    r = _clean(reason_norm or "")
    if not r or r == "unknown_veto":
        return "unknown"
    if r == "bo_l2_fail_closed" or r.startswith("bo_l2_"):
        return "book_l2_gate"
    if r.startswith("l2_"):
        return "book_l2_quality"
    if r.startswith("l3_") or r == "l3_missing":
        return "l3_quality"
    if "conf_below_min" in r or r.startswith("conf_"):
        return "confidence_gate"
    if r == "spread_filter_veto" or "spread" in r:
        return "spread_gate"
    if r == "cooldown" or "cooldown" in r:
        return "cooldown_gate"
    if r == "touch_suppressed" or "touch" in r:
        return "touch_gate"
    return "other"
