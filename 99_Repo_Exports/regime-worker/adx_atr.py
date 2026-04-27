"""
adx_atr.py — Wilder's ADX/ATR incremental calculation.

Stateful per-(symbol, timeframe) computation using WilderState.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class WilderState:
    """Мутируемое состояние фильтра Уайлдера для одной пары (symbol, timeframe)."""

    __slots__ = ("atr", "plus_dm", "minus_dm", "adx", "initialized")

    atr: Optional[float]
    plus_dm: Optional[float]
    minus_dm: Optional[float]
    adx: Optional[float]
    initialized: bool

    def __init__(self) -> None:
        self.atr = None
        self.plus_dm = None
        self.minus_dm = None
        self.adx = None
        self.initialized = False


def wilder_update(prev: float, new: float, n: int) -> float:
    """Exponential smoothing по Уайлдеру: EMA с alpha=1/n."""
    return (prev * (n - 1) + new) / n


def true_range(h: float, lo: float, pc: float) -> float:
    """True Range = max(H-L, |H-PC|, |L-PC|)."""
    return max(h - lo, abs(h - pc), abs(lo - pc))


def directional_moves(
    h: float, lo: float, ph: float, pl: float
) -> tuple[float, float]:
    """Рассчитывает +DM и -DM от текущей и предыдущей свечи."""
    up = h - ph
    dn = pl - lo
    plus_dm = up if (up > dn and up > 0) else 0.0
    minus_dm = dn if (dn > up and dn > 0) else 0.0
    return plus_dm, minus_dm


def update_adx_atr(
    state: WilderState,
    h: float, lo: float, c: float,
    ph: float, pl: float, pc: float,
    n: int = 14,
) -> tuple[WilderState, Optional[dict[str, float]]]:
    """
    Обновляет состояние Уайлдера для одной свечи.

    Args:
        state: текущее накопленное состояние.
        h, lo, c: high/low/close текущей свечи.
        ph, pl, pc: high/low/close предыдущей свечи.
        n: период сглаживания (14 по умолчанию).

    Returns:
        (state, result_dict) или (state, None) при первой инициализации.
    """
    tr = true_range(h, lo, pc)
    p_dm, m_dm = directional_moves(h, lo, ph, pl)

    if not state.initialized:
        state.atr = tr
        state.plus_dm = p_dm
        state.minus_dm = m_dm
        state.adx = None
        state.initialized = True
        return state, None

    atr = wilder_update(state.atr, tr, n)
    p_dms = wilder_update(state.plus_dm, p_dm, n)
    m_dms = wilder_update(state.minus_dm, m_dm, n)

    if atr == 0:
        return state, None

    plus_di = 100.0 * (p_dms / atr)
    minus_di = 100.0 * (m_dms / atr)
    denom = plus_di + minus_di
    dx = 100.0 * abs(plus_di - minus_di) / denom if denom > 0 else 0.0
    adx = dx if state.adx is None else wilder_update(state.adx, dx, n)

    state.atr = atr
    state.plus_dm = p_dms
    state.minus_dm = m_dms
    state.adx = adx

    return state, {"atr": atr, "plusDI": plus_di, "minusDI": minus_di, "adx": adx}
