# domain/calculators.py
from __future__ import annotations
import math
from typing import Optional, Sequence, Tuple

from domain.models import PositionState, Side


def round_to_point(value: float, point: float, mode: str) -> float:
    if point <= 0:
        return value
    q = value / point
    if mode == "floor":
        return math.floor(q) * point
    if mode == "ceil":
        return math.ceil(q) * point
    return round(q) * point


def calc_trailing_sl(
    side: Side
    current_price: float
    trailing_distance: float
    point: float
    prev_sl: float
) -> Optional[float]:
    if trailing_distance <= 0:
        return None

    if side == "LONG":
        candidate = current_price - trailing_distance
        candidate = round_to_point(candidate, point, "floor")  # для LONG SL вниз по тику
        if candidate <= prev_sl:
            return None
        return candidate

    candidate = current_price + trailing_distance
    candidate = round_to_point(candidate, point, "ceil")       # для SHORT SL вверх по тику
    if candidate >= prev_sl:
        return None
    return candidate



def update_excursions(pos: Any, price: float, ts_ms: int) -> None:
    """
    Обновляет favorable/adverse "экскурсии" (выходы цены).
    Существующее поведение:
      - обновляет максимальные/минимальные увиденные цены
      - обновляет max_favorable_price + max_favorable_ts
    Улучшение (критично для "MFE/MAE before TP1"):
      - также отслеживать max_adverse_price + max_adverse_ts
        чтобы мы могли позже детерминированно заморозить "MAE_BEFORE_TP1".
    """
    # --- существующий код вычисляет/обновляет favorable ---
    # Код репозитория указывает:
    #   pos.max_favorable_price / pos.max_favorable_ts пишутся здесь.
    # Мы сохраняем этот контракт и расширяем его max_adverse_price / max_adverse_ts.

    # Fail-open: никогда не падать на странных структурах pos.
    try:
        p = float(price)
        if not math.isfinite(p) or p <= 0:
            return
    except Exception:
        return

    # Определить направление. Мы не полагаемся на конкретную типизацию класса.
    # LONG: favorable=max, adverse=min
    # SHORT: favorable=min, adverse=max
    try:
        is_long = bool(getattr(pos, "is_long")() if callable(getattr(pos, "is_long", None)) else (str(getattr(pos, "direction", "")).lower() in {"long", "buy"}))
    except Exception:
        is_long = True

    # Текущие экстремумы. Critical Fix: handle 0.0 initialization properly.
    try:
        cur_max = float(getattr(pos, "max_price_seen", 0.0) or 0.0)
    except Exception:
        cur_max = 0.0

    try:
        cur_min = float(getattr(pos, "min_price_seen", 0.0) or 0.0)
    except Exception:
        cur_min = 0.0

    # Обновить глобальные экстремумы (общие для обеих сторон)
    # Если 0.0 -> неинициализировано -> обновляем безусловно
    if cur_max == 0.0 or p > cur_max:
        try:
            setattr(pos, "max_price_seen", p)
        except Exception:
            pass
        max_seen = p
    else:
        max_seen = cur_max

    if cur_min == 0.0 or p < cur_min:
        try:
            setattr(pos, "min_price_seen", p)
        except Exception:
            pass
        min_seen = p
    else:
        min_seen = cur_min

    # Цены favorable/adverse для стороны
    favorable = max_seen if is_long else min_seen
    adverse = min_seen if is_long else max_seen

    # Update favorable snapshot + timestamp (existing behavior)
    # Обновить снимок favorable + timestamp (существующее поведение)
    try:
        prev_fav_raw = getattr(pos, "max_favorable_price", 0.0)
        prev_fav = float(prev_fav_raw or favorable)
    except Exception:
        prev_fav = favorable
        prev_fav_raw = 0.0
    
    # Если 0.0/None, принудительно обновить
    if not prev_fav_raw:
        fav_updated = True
    else:
        fav_updated = (favorable > prev_fav) if is_long else (favorable < prev_fav)
    
    if fav_updated:
        try:
            setattr(pos, "max_favorable_price", float(favorable))
            setattr(pos, "max_favorable_ts", int(ts_ms))
        except Exception:
            pass

    # NEW: обновление снимка adverse + timestamp
    # Требуется для вычисления "MAE before TP1" без неоднозначности.
    try:
        prev_adv_raw = getattr(pos, "max_adverse_price", 0.0)
        prev_adv = float(prev_adv_raw or adverse)
    except Exception:
        prev_adv = adverse
        prev_adv_raw = 0.0
        
    if not prev_adv_raw:
        adv_updated = True
    else:
        adv_updated = (adverse < prev_adv) if is_long else (adverse > prev_adv)
        
    if adv_updated:
        try:
            setattr(pos, "max_adverse_price", float(adverse))
            setattr(pos, "max_adverse_ts", int(ts_ms))
        except Exception:
            pass


def snapshot_tp1_excursions(pos: Any, ts_ms: int) -> None:
    """
    Заморозить метрики "экскурсий" точно при первом касании TP1.

    Зачем:
      Global pos.mfe_pnl/pos.mae_pnl продолжают меняться после TP1 (особенно если сделка scale out / тралится).
      Для эмпирической калибровки нам нужны:
        - MFE_AT_TP1      (вылет цены до первого тейк-профита)
        - MAE_BEFORE_TP1  (просадка, пересиженная до TP1)

    Контракт:
      - идемпотентность: первый вызов выигрывает, последующие ничего не делают.
      - fail-open: никогда не вызывает исключений.
    """
    try:
        if getattr(pos, "tp1_hit_ts_ms", None):
            return
    except Exception:
        # если доступ к атрибуту не удался, всё равно попытаться установить один раз
        pass

    try:
        setattr(pos, "tp1_hit_ts_ms", int(ts_ms))
    except Exception:
        return

    # Снимок денежных экскурсий в момент TP1
    try:
        mfe_pnl = float(getattr(pos, "mfe_pnl", 0.0) or 0.0)
        mae_pnl = float(getattr(pos, "mae_pnl", 0.0) or 0.0)
    except Exception:
        mfe_pnl = 0.0
        mae_pnl = 0.0

    try:
        setattr(pos, "mfe_pnl_at_tp1", float(mfe_pnl))
        setattr(pos, "mae_pnl_before_tp1", float(mae_pnl))
    except Exception:
        pass

    # Снимок экстремумов цены/времени (диагностика; полезно для проверки "до TP1")
    for src, dst in [
        ("max_favorable_price", "mfe_price_at_tp1")
        ("max_favorable_ts", "mfe_ts_at_tp1")
        ("max_adverse_price", "mae_price_before_tp1")
        ("max_adverse_ts", "mae_ts_before_tp1")
    ]:
        try:
            v = getattr(pos, src, None)
            if v is not None:
                setattr(pos, dst, v)
        except Exception:
            pass


def pnl_pct_simple(side: Side, entry_price: float, exit_price: float) -> float:
    if entry_price <= 0:
        return 0.0
    if side == "LONG":
        return (exit_price - entry_price) / entry_price * 100.0
    return (entry_price - exit_price) / entry_price * 100.0


def duration_ms(entry_ts_ms: int, exit_ts_ms: int) -> int:
    return max(0, int(exit_ts_ms - entry_ts_ms))


def calc_missed_profit(
    pos: PositionState
    spec
    tp_ratios: Sequence[float]
) -> float:
    """
    Если была ситуация TP→SL, считаем "упущенную прибыль":
    сценарий: закрыть остаток в момент последнего TP vs факт.
    """
    tp_before_sl = pos.tp_hits
    if tp_before_sl <= 0:
        return 0.0

    tp_idx = min(3, tp_before_sl)
    tp_price = pos.tp_fill_prices.get(tp_idx)
    if tp_price is None:
        # fallback на уровни
        if 1 <= tp_idx <= len(pos.tp_levels):
            tp_price = pos.tp_levels[tp_idx - 1]
        else:
            return 0.0

    # сколько закрыли до этого TP по ratios
    closed_before = 0.0
    for i in range(tp_idx):
        if i < len(tp_ratios):
            closed_before += pos.lot * float(tp_ratios[i])

    remaining_at_tp = max(0.0, pos.lot - closed_before)

    hypothetical_rest = spec.pnl_money(pos.entry_price, float(tp_price), remaining_at_tp, pos.direction, symbol=pos.symbol)

    actual = pos.realized_pnl_gross  # факт по gross
    # pnl уже содержит "то, что случилось с остатком после TP" → заменяем на hypothetical_rest
    # для этого оценим pnl остатка как (actual - pnl частей TP по уровням)
    pnl_tp_parts = 0.0
    for i in range(min(3, len(pos.tp_levels))):
        lvl = i + 1
        if lvl in pos.tp_fill_prices and i < len(tp_ratios):
            pnl_tp_parts += spec.pnl_money(pos.entry_price, pos.tp_fill_prices[lvl], pos.lot * float(tp_ratios[i]), pos.direction, symbol=pos.symbol)

    pnl_rest_fact = actual - pnl_tp_parts
    hypothetical_total = (actual - pnl_rest_fact) + hypothetical_rest
    return hypothetical_total - actual

