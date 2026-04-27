# risk_position_sizer.py
"""
Risk Position Sizer - позиционирование по ATR и балансу счёта.
Учитывает шаг лота, минимумы/максимумы и стоимость тика.

ENV:
    MAX_LOT_FALLBACK  (float, default 5.0)  — hard cap лота когда SymbolSpecs не задаёт max_lot.
    EQUITY_LEVERAGE_CAP (float, default 0.0) — если > 0, ограничивает лот по notional =
                              balance * EQUITY_LEVERAGE_CAP / (entry * contract_size).
                              0.0 = отключено (legacy-режим).
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Tuple, Optional
import math
import os

# ---------------------------------------------------------------------------
# ENV-driven defaults (всегда перекрывают hardcode)
# ---------------------------------------------------------------------------
_DEFAULT_MAX_LOT: float = float(os.getenv("MAX_LOT_FALLBACK", "5.0"))
_EQUITY_LEVERAGE_CAP: float = float(os.getenv("EQUITY_LEVERAGE_CAP", "0.0"))


@dataclass
class SymbolSpecs:
    """
    Спецификации торгового инструмента.

    Attributes:
        symbol: Trading symbol (e.g., "XAUUSD")
        point: Размер шага цены (e.g., 0.01 для XAUUSD)
        tick_value_per_lot: $ за 1 пункт (point) на 1.0 lot
        lot_step: Минимальный шаг размера лота
        min_lot: Минимальный размер лота
        max_lot: Максимальный размер лота.  Берётся из ENV MAX_LOT_FALLBACK (default 5.0);
                 брокерский cap должен передаваться явно при инициализации.
        contract_size: Количество единиц базового актива на 1 лот (для leverage cap).
    """
    symbol: str = "XAUUSD"
    point: float = 0.01          # размер шага цены
    tick_value_per_lot: float = 1.0  # $ за 1 пункт (point) на 1.0 lot
    lot_step: float = 0.01
    min_lot: float = 0.01
    max_lot: float = field(default_factory=lambda: _DEFAULT_MAX_LOT)
    contract_size: float = 1.0   # единиц базового актива на лот (напр. 100 для CFD на нефть)

def _round_step(x: float, step: float) -> float:
    """Округляет значение до шага."""
    return math.floor(x / step) * step if step > 0 else x

def risk_amount(balance: float, risk_pct: float) -> float:
    """
    Рассчитывает сумму риска в долларах.
    
    Args:
        balance: Баланс счёта в USD
        risk_pct: Процент риска от баланса
    
    Returns:
        Сумма риска в USD
    """
    return max(0.0, balance * (risk_pct / 100.0))

def pip_cost_per_lot(spec: SymbolSpecs) -> float:
    """
    Стоимость одного "point" на 1.0 lot.
    
    Args:
        spec: SymbolSpecs объект
    
    Returns:
        Стоимость в USD
    """
    return spec.tick_value_per_lot

# ---------------------------------------------------------------------------
# Equity-aware leverage cap (P2-3)
# ---------------------------------------------------------------------------

def equity_leverage_cap(
    lot: float,
    entry: float,
    balance: float,
    spec: SymbolSpecs,
    leverage_cap: Optional[float] = None,
) -> float:
    """
    Ограничивает размер лота так, чтобы notional-exposure не превышал
    balance * leverage_cap.

    Args:
        lot: Расчётный лот (до cap'а).
        entry: Цена входа.
        balance: Баланс счёта в USD.
        spec: SymbolSpecs (используется contract_size).
        leverage_cap: Максимально допустимый leverage (кратность баланса).
                      None → берётся из ENV EQUITY_LEVERAGE_CAP.
                      0.0 → cap отключён (legacy-режим).

    Returns:
        Лот после применения cap'а (>= spec.min_lot).
    """
    cap = _EQUITY_LEVERAGE_CAP if leverage_cap is None else float(leverage_cap)
    if cap <= 0.0 or entry <= 0.0 or balance <= 0.0:
        return lot  # отключено или невалидные данные
    notional_per_lot = entry * spec.contract_size
    if notional_per_lot <= 0.0:
        return lot
    max_notional = balance * cap
    max_lot_by_leverage = max_notional / notional_per_lot
    capped = _round_step(min(lot, max_lot_by_leverage), spec.lot_step)
    return max(spec.min_lot, capped)


def lot_by_risk(
    balance: float,
    risk_pct: float,
    entry: float,
    sl: float,
    spec: SymbolSpecs
) -> float:
    """
    Рассчитывает размер лота на основе риска и расстояния до SL.
    
    Args:
        balance: Баланс счёта в USD
        risk_pct: Процент риска
        entry: Цена входа
        sl: Цена Stop Loss
        spec: SymbolSpecs объект
    
    Returns:
        Размер лота (rounded to lot_step)
    """
    ra = risk_amount(balance, risk_pct)
    dist_points = abs(entry - sl) / spec.point
    if dist_points <= 0:
        return spec.min_lot
    cost_point_per_lot = pip_cost_per_lot(spec)   # $/point/lot
    raw = ra / (dist_points * cost_point_per_lot)
    lot = max(spec.min_lot, min(spec.max_lot, _round_step(raw, spec.lot_step)))
    return lot

def lot_by_risk_with_leverage(
    balance: float,
    risk_pct: float,
    entry: float,
    sl: float,
    spec: SymbolSpecs,
    leverage_cap: Optional[float] = None,
) -> float:
    """
    lot_by_risk + equity_leverage_cap в одном вызове.
    Рекомендуется использовать вместо lot_by_risk напрямую.
    """
    lot = lot_by_risk(balance, risk_pct, entry, sl, spec)
    return equity_leverage_cap(lot, entry, balance, spec, leverage_cap=leverage_cap)


def apply_volatility_scaler(
    lot: float,
    atr: float,
    atr_baseline: float = 3.0,
    bounds=(0.5, 1.5)
) -> float:
    """
    Применяет масштабирование на основе волатильности (ATR).
    При высокой волатильности уменьшает лот, при низкой — увеличивает.
    
    Args:
        lot: Исходный размер лота
        atr: Текущий ATR
        atr_baseline: Базовый ATR для сравнения
        bounds: Минимальный и максимальный множитель
    
    Returns:
        Отмасштабированный лот
    """
    if atr <= 0:
        return lot
    scale = max(bounds[0], min(bounds[1], atr_baseline / atr))
    return max(0.0, lot * scale)

def kelly_fraction(win_rate: float, rr: float) -> float:
    """
    Рассчитывает долю Kelly Criterion для позиционирования.
    Келли (упрощённо): f* = p - (1-p)/RR
    
    Args:
        win_rate: Винрейт (0.0-1.0)
        rr: Средний Risk/Reward ratio
    
    Returns:
        Доля Kelly (0.0-1.0)
    """
    p = max(0.0, min(1.0, win_rate))
    if rr <= 0:
        return 0.0
    return max(0.0, p - (1.0 - p) / rr)

def build_sl_tp(
    side: str,
    entry: float,
    atr: float,
    sl_mult: float,
    tp_mults: List[float]
) -> Tuple[float, List[float]]:
    """
    Строит SL и TP уровни на основе ATR.
    
    Args:
        side: "LONG" | "SHORT"
        entry: Цена входа
        atr: ATR значение
        sl_mult: Множитель ATR для SL
        tp_mults: Список множителей ATR для TP уровней
    
    Returns:
        Tuple[sl_price, [tp1, tp2, ...]]
    """
    if atr <= 0:
        atr = float(os.getenv("ATR_FALLBACK", "1.0"))
    
    if side.upper() == "LONG":
        sl = entry - atr * sl_mult
        tps = [entry + atr * m for m in tp_mults]
    else:
        sl = entry + atr * sl_mult
        tps = [entry - atr * m for m in tp_mults]
    
    return (round(sl, 2), [round(x, 2) for x in tps])

def size_and_bracket(
    side: str,
    entry: float,
    atr: float,
    balance: float,
    spec: SymbolSpecs,
    risk_pct: float = 1.0,
    sl_mult: float = 1.5,
    tp_mults: Optional[List[float]] = None,
    use_volatility_scaler: bool = True,
    kelly: Optional[Tuple[float, float]] = None  # (win_rate, avg_rr) -> fraction
) -> Tuple[float, float, List[float]]:
    """
    Рассчитывает размер позиции и уровни SL/TP с учётом риска и волатильности.
    
    Args:
        side: "LONG" | "SHORT"
        entry: Цена входа
        atr: ATR значение
        balance: Баланс счёта в USD
        spec: SymbolSpecs объект
        risk_pct: Процент риска от баланса
        sl_mult: Множитель ATR для SL
        tp_mults: Список множителей ATR для TP
        use_volatility_scaler: Применять ли масштабирование по волатильности
        kelly: Опционально (win_rate, avg_rr) для Kelly Criterion
    
    Returns:
        Tuple[lot, sl_price, [tp1, tp2, ...]]
    """
    if tp_mults is None:
        tp_mults = [2.0, 3.0, 4.0]

    sl, tps = build_sl_tp(side, entry, atr, sl_mult, tp_mults)
    lot = lot_by_risk(balance, risk_pct, entry, sl, spec)

    if use_volatility_scaler:
        lot = apply_volatility_scaler(
            lot,
            atr,
            atr_baseline=float(os.getenv("ATR_BASELINE", "3.0"))
        )

    if kelly:
        f = kelly_fraction(*kelly)
        lot = max(spec.min_lot, min(spec.max_lot, _round_step(lot * f, spec.lot_step)))

    return (round(lot, 2), sl, tps)

