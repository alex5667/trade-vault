"""
classify.py — Классификация рыночного режима на основе ADX/ATR-квантилей.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Dict


def classify_regime(
    adx: float,
    adx_prev: float | None,
    atr_pct: float,
    atr_pct_prev: float | None,
    plus_di: float,
    minus_di: float,
    q: Dict[str, float],
) -> tuple[str, float, float]:
    """
    Определяет режим рынка по ADX, ATR% и их динамике.

    Args:
        adx: текущее значение ADX.
        adx_prev: предыдущее ADX (None при первом вызове).
        atr_pct: ATR как доля от цены.
        atr_pct_prev: предыдущий ATR% (None при первом вызове).
        plus_di: +DI компонент.
        minus_di: -DI компонент.
        q: квантили (adx_p40, adx_p60, adx_p75, atrp_p25, atrp_p50).

    Returns:
        (regime, adx_slope, atrp_slope)
    """
    adx_slope = (adx - adx_prev) if adx_prev is not None else 0.0
    atrp_slope = (atr_pct - atr_pct_prev) if atr_pct_prev is not None else 0.0

    high_adx = adx >= q["adx_p60"]
    low_adx = adx < q["adx_p40"]
    low_atr = atr_pct <= q["atrp_p25"]
    mid_atr = atr_pct <= q["atrp_p50"]

    if low_atr and low_adx and adx_slope <= 0:
        regime = "squeeze"
    elif high_adx and adx_slope >= 0:
        regime = "trending_bull" if plus_di > minus_di else "trending_bear"
    elif low_adx and mid_atr:
        regime = "range"
    elif adx_slope > 0 and atrp_slope > 0:
        regime = "expansion"
    else:
        regime = "range"

    return regime, adx_slope, atrp_slope


def confidence(regime: str, adx: float, q: Dict[str, float]) -> float:
    """
    Возвращает уверенность классификации [0..1].

    Trending: 0.9 при ADX>=p75, 0.7 при ADX>=p60, иначе 0.55.
    Squeeze:  0.7
    Expansion: 0.65
    Range/default: 0.5
    """
    if regime.startswith("trending"):
        if adx >= q["adx_p75"]:
            return 0.9
        if adx >= q["adx_p60"]:
            return 0.7
        return 0.55
    if regime == "squeeze":
        return 0.7
    if regime == "expansion":
        return 0.65
    return 0.5
