from __future__ import annotations

import json
import os
from dataclasses import asdict, is_dataclass
from typing import Any, Optional


def _env_bool(name: str, default: str = "1") -> bool:
    v = str(os.getenv(name, default)).strip().lower()
    return v not in {"0", "false", "no", "off", ""}


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _pick_cancel_to_trade(ctx: Any) -> Optional[float]:
    """
    cancel_to_trade в проекте обычно бывает в нескольких окнах/сторонах.
    Для лога берём "наихудшее"/максимальное из доступных — это самый полезный one-liner индикатор спуф-риска.
    """
    cands = [
        getattr(ctx, "cancel_to_trade_bid_5s", None),
        getattr(ctx, "cancel_to_trade_ask_5s", None),
        getattr(ctx, "cancel_to_trade_bid_20s", None),
        getattr(ctx, "cancel_to_trade_ask_20s", None),
    ]
    vals = [v for v in (_to_float(v) for v in cands) if v is not None]
    return max(vals) if vals else None


def _missing_l3(ctx: Any) -> bool:
    """
    L3 может быть "частично": например есть spread, но нет cancel_to_trade.
    Для data-quality флага считаем L3 missing, если нет ключевых L3-lite полей.
    """
    has_ctt = _pick_cancel_to_trade(ctx) is not None
    has_mps = _to_float(getattr(ctx, "microprice_shift_bps_20", None)) is not None
    has_spread = _to_float(getattr(ctx, "spread_bps", None)) is not None
    # достаточно 2 из 3, чтобы считать "L3 есть"
    return (int(has_ctt) + int(has_mps) + int(has_spread)) < 2


def _missing_htf(ctx: Any) -> bool:
    """
    HTF/geometry могут быть представлены по-разному:
      - ctx.geometry (snapshot объект)
      - ctx.geometry_score / ctx.geo_zone_hits
      - ctx.htf_level_dist_bps и т.п.
    Для флага missing_htf используем мягкую эвристику: если нет вообще ничего из geometry/htf.
    """
    if getattr(ctx, "geometry", None) is not None:
        return False
    if _to_float(getattr(ctx, "geometry_score", None)) is not None:
        return False
    if getattr(ctx, "geo_zone_hits", None) is not None:
        return False
    if _to_float(getattr(ctx, "htf_level_dist_bps", None)) is not None:
        return False
    return True


def _used_fallback_hlc(ctx: Any) -> bool:
    """
    По вашей 4.1 политике: candles fallback помечаем через ctx.data_quality_flags += ["hlc_fallback"].
    """
    flags = getattr(ctx, "data_quality_flags", None)
    if isinstance(flags, (list, tuple, set)):
        return "hlc_fallback" in flags
    return False


def _l2_is_stale(ctx: Any, parts: Optional[dict[str, Any]] = None) -> bool:
    """
    L2 stale может быть рассчитан в confirmations и сохранён в parts.
    Если parts нет — смотрим на ctx (если поля уже проставляются где-то в пайплайне).
    """
    if isinstance(parts, dict):
        v = parts.get("l2_is_stale", None)
        if v is None:
            v = parts.get("l2_stale", None)
        if v is not None:
            return bool(v)
    v2 = getattr(ctx, "l2_is_stale", None)
    if v2 is not None:
        return bool(v2)
    return False


def _extract_level_key(payload: dict[str, Any]) -> Optional[str]:
    """
    Для логов предпочтительнее level_key (строковый/стабильный),
    но если его нет — используем level_price.
    """
    lk = payload.get("level_key", None)
    if lk is not None:
        try:
            s = str(lk).strip()
            return s or None
        except Exception:
            pass
    lp = payload.get("level_price", None)
    if lp is not None:
        try:
            return str(lp)
        except Exception:
            return None
    return None


def _dataclass_to_dict(obj: Any) -> Optional[dict[str, Any]]:
    if obj is None:
        return None
    if is_dataclass(obj):
        try:
            return asdict(obj)
        except Exception:
            return None
    return None


def build_signal_json_log(
    *,
    payload: dict[str, Any],
    ctx: Any,
    parts: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """
    ### 5.3 Логи: "1 сигнал = 1 JSON"
    Возвращает *готовый* dict для json.dumps (без сериализации).

    ВАЖНО:
      - тут не должно быть тяжёлых объектов (L2 книги/большие массивы) -> только top-features
      - missing_* флаги считаем fail-open эвристиками (чтобы лог был всегда)
    """
    conf_factor = _to_float(payload.get("conf_factor", None))
    raw_score = _to_float(payload.get("raw_score", None))
    final_score = _to_float(payload.get("final_score", None))

    # regime_score — стараемся взять "одну ось": trend-range, как вы делали в ctx.market_regime_score
    regime_score = _to_float(getattr(ctx, "market_regime_score", None))
    if regime_score is None:
        rt = _to_float(getattr(ctx, "regime_trend_score", None))
        rr = _to_float(getattr(ctx, "regime_range_score", None))
        if rt is not None and rr is not None:
            regime_score = rt - rr

    # geometry_score — по плану 3.4: монотонный скор. Если пока хранится как snapshot — пробуем вытащить.
    geometry_score = _to_float(getattr(ctx, "geometry_score", None))
    if geometry_score is None:
        g = getattr(ctx, "geometry", None)
        if g is not None:
            # поддерживаем разные форматы snapshot (dataclass или объект с атрибутом score)
            geometry_score = _to_float(getattr(g, "geometry_score", None))
            if geometry_score is None:
                geometry_score = _to_float(getattr(g, "score", None))

    # microprice_shift — в проекте у вас microprice_shift_bps_20
    microprice_shift = _to_float(getattr(ctx, "microprice_shift_bps_20", None))

    # taker_rate — название может отличаться (taker_rate_ema / taker_rate_ema_5s / taker_rate)
    taker_rate = _to_float(getattr(ctx, "taker_rate_ema", None))
    if taker_rate is None:
        taker_rate = _to_float(getattr(ctx, "taker_rate", None))
    if taker_rate is None:
        taker_rate = _to_float(getattr(ctx, "taker_rate_ema_5s", None))

    # cancel_to_trade — агрегируем
    cancel_to_trade = _pick_cancel_to_trade(ctx)

    # spread_bps / obi_avg — уже есть в ctx
    spread_bps = _to_float(getattr(ctx, "spread_bps", None))
    obi_avg = _to_float(getattr(ctx, "obi_avg", None))

    # data quality
    dq = {
        "l2_is_stale": bool(_l2_is_stale(ctx, parts)),
        "used_fallback_hlc": bool(_used_fallback_hlc(ctx)),
        "missing_htf": bool(_missing_htf(ctx)),
        "missing_l3": bool(_missing_l3(ctx)),
    }

    # компактный snapshot геометрии (для дебага, если это dataclass)
    # НЕ добавляем большие поля (списки зон) — только если snapshot маленький
    geometry_snapshot = _dataclass_to_dict(getattr(ctx, "geometry", None))

    return {
        # идентификация сигнала
        "signal_id": payload.get("signal_id", None),
        "kind": payload.get("kind", None),
        "side": payload.get("side", None),
        "symbol": payload.get("symbol", None),
        "ts": payload.get("ts", None),
        "price": payload.get("price", None),
        "level_key": _extract_level_key(payload),
        # scoring axis
        "raw_score": raw_score,
        "conf_factor": conf_factor,
        "final_score": final_score,
        # top features (для калибровки/отладки)
        "features": {
            "spread_bps": spread_bps,
            "obi_avg": obi_avg,
            "microprice_shift": microprice_shift,
            "cancel_to_trade": cancel_to_trade,
            "taker_rate": taker_rate,
            "regime_score": regime_score,
            "geometry_score": geometry_score,
        },
        # data quality flags
        "data_quality": dq,
        # optional: parts-lite (если хотите видеть, чем именно conf_factor был получен)
        # ВНИМАНИЕ: parts должен быть небольшой (числа/флаги), без вложенных книг.
        "parts": parts or {},
        # optional: geometry snapshot if tiny dataclass
        "geometry": geometry_snapshot,
    }


def log_signal_one_json(
    logger: Any,
    *,
    payload: dict[str, Any],
    ctx: Any,
    parts: Optional[dict[str, Any]] = None,
) -> None:
    """
    Пишем ровно одну строку JSON в logger.info.
    Логгер/хэндлер пусть добавляет timestamp/level как ему нужно,
    но сообщение должно быть чистым JSON (1 сигнал = 1 JSON).
    """
    if not _env_bool("SIGNAL_ONE_JSON_LOG", "1"):
        return
    obj = build_signal_json_log(payload=payload, ctx=ctx, parts=parts)
    try:
        # separators -> компактно; ensure_ascii=False -> читабельные unicode поля (если будут)
        msg = json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)
    except Exception:
        # fail-open: если внезапно сериализация сломалась — логируем деградированно
        msg = json.dumps(
            {"kind": "signal_log_encode_fail_open", "signal_id": payload.get("signal_id"), "payload_kind": payload.get("kind")},
            ensure_ascii=False,
            separators=(",", ":"),
        )
    try:
        logger.info(msg)
    except Exception:
        # logging не должен ломать сигналинг
        pass
