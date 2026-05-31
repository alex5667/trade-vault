from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


def _clamp01(x: float) -> float:
    if x != x:  # NaN
        return 0.5
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return float(x)


DEFAULT_L3_SCORE_NEUTRAL = 0.5
DEFAULT_GEOMETRY_SCORE_NEUTRAL = 0.1  # "HTF levels missing" -> neutral-but-low, no veto
DEFAULT_L2_STALE_MS = 1500


def _ensure_list_attr(obj: Any, name: str) -> list[str]:
    cur = getattr(obj, name, None)
    if isinstance(cur, list):
        return cur
    cur = []
    setattr(obj, name, cur)
    return cur


def get_l2_ts_ms(ctx: Any) -> int | None:
    """
    Унифицированно достаём timestamp книги:
      - ctx.l2_ts_ms
      - ctx.l2.ts_ms (если snapshot)
    """
    v = getattr(ctx, "l2_ts_ms", None)
    if isinstance(v, (int, float)):
        return int(v)
    l2 = getattr(ctx, "l2", None)
    if l2 is not None:
        v2 = getattr(l2, "ts_ms", None) or getattr(l2, "ts", None)
        if isinstance(v2, (int, float)):
            return int(v2)
    return None


def ensure_dependency_defaults(ctx: Any) -> None:
    """
    Заполняет "безопасные" дефолты для зависимостей и маркирует качество данных.
    Ничего не veto'ит — только нормализует контекст.
    """
    flags = _ensure_list_attr(ctx, "data_quality_flags")

    # L3: если нет данных — не veto, но помечаем и ставим нейтраль
    l3_score = getattr(ctx, "l3_score", None)
    if l3_score is None:
        ctx.l3_score = DEFAULT_L3_SCORE_NEUTRAL
        if "l3_missing" not in flags:
            flags.append("l3_missing")
    else:
        ctx.l3_score = _clamp01(float(l3_score))

    # HTF/Geometry: если геометрия/уровни недоступны — не veto, ставим нейтраль и помечаем
    geo_score = getattr(ctx, "geometry_score", None)
    if geo_score is None:
        ctx.geometry_score = DEFAULT_GEOMETRY_SCORE_NEUTRAL
        if "htf_missing" not in flags:
            flags.append("htf_missing")
    else:
        ctx.geometry_score = _clamp01(float(geo_score))

    # Candles/HLC fallback: только маркируем (само использование зависит от вашего ATR/HLC fallback)
    if bool(getattr(ctx, "hlc_fallback_used", False)):
        if "hlc_fallback" not in flags:
            flags.append("hlc_fallback")


@dataclass(frozen=True)
class DependencyDecision:
    veto: bool
    conf_multiplier: float = 1.0
    parts: dict[str, Any] = field(default_factory=dict)


def dependency_decision_for_kind(
    kind: str,
    ctx: Any,
    now_ms: int | None = None,
    l2_stale_ms: int = DEFAULT_L2_STALE_MS,
) -> DependencyDecision:
    """
    Политики fail-open / fail-closed:

    - L2 stale:
        breakout -> fail-closed (veto)
        extreme  -> fail-open (штраф к confidence)
        прочие   -> fail-open (мягкий штраф)
    - L3 недоступен:
        не veto, l3_score=0.5, пометка l3_missing (см. ensure_dependency_defaults)
    - HTF уровни недоступны:
        не veto, geometry_score=0.1, пометка htf_missing (см. ensure_dependency_defaults)
    - candles fallback:
        только флаг hlc_fallback (см. ensure_dependency_defaults)
    """
    # Обязательная нормализация дефолтов (важно для консистентных частей/метрик)
    ensure_dependency_defaults(ctx)

    if now_ms is None:
        # чаще всего ctx.ts — это ms на bucket boundary
        v = getattr(ctx, "ts", None)
        now_ms = int(v) if isinstance(v, (int, float)) else None

    flags = _ensure_list_attr(ctx, "data_quality_flags")

    l2_ts = get_l2_ts_ms(ctx)
    l2_age = None
    l2_stale = False
    if now_ms is not None:
        if l2_ts is None:
            l2_stale = True
        else:
            l2_age = int(max(0, now_ms - l2_ts))
            l2_stale = l2_age > int(max(l2_stale_ms, 1))
    else:
        # если даже now_ms нет — считаем книгу "сомнительной", но не veto здесь
        l2_stale = (l2_ts is None)

    if l2_stale and "l2_stale" not in flags:
        flags.append("l2_stale")

    parts: dict[str, Any] = {
        "l2": {"ts_ms": l2_ts, "age_ms": l2_age, "stale": l2_stale},
        "l3_score": float(getattr(ctx, "l3_score", DEFAULT_L3_SCORE_NEUTRAL)),
        "geometry_score": float(getattr(ctx, "geometry_score", DEFAULT_GEOMETRY_SCORE_NEUTRAL)),
        "hlc_fallback": bool(getattr(ctx, "hlc_fallback_used", False)),
    }

    k = (kind or "").lower()

    # --- L2 stale policy ---
    if l2_stale:
        if k == "breakout":
            # fail-closed: без актуальной книги breakout не торгуем
            return DependencyDecision(veto=True, conf_multiplier=0.0, parts=parts)
        if k == "extreme":
            # fail-open: разрешаем, но штрафуем (book quality неизвестен)
            return DependencyDecision(veto=False, conf_multiplier=0.75, parts=parts)
        # прочие кандидаты: мягче
        return DependencyDecision(veto=False, conf_multiplier=0.85, parts=parts)

    return DependencyDecision(veto=False, conf_multiplier=1.0, parts=parts)
