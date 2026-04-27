# core/regime.py
from __future__ import annotations
from typing import Literal

Regime = Literal["trend", "range", "mixed"]

import logging

logger = logging.getLogger("regime_classifier")

def classify_regime(ma_slope: float, atr_norm: float) -> Regime:
    """
    Классифицирует режим рынка на основе наклона MA и нормализованной ATR.

    ma_slope: нормализованный наклон MA (в bps за N баров)
    atr_norm: ATR / mid_price (нормализованная волатильность)

    Возвращает:
    - "trend": сильный тренд (большой наклон + высокая волатильность)
    - "range": рендж (маленький наклон + низкая волатильность)
    - "mixed": смешанный режим
    """
    if not ma_slope or not atr_norm:
        logger.warning(f"Regime classification received incomplete inputs: ma_slope={ma_slope}, atr_norm={atr_norm}")

    # Параметры можно настроить через ENV или константы
    trend_slope_threshold = 3e-4  # 0.03%
    range_slope_threshold = 1e-4  # 0.01%
    trend_atr_threshold = 0.002   # 0.2%
    range_atr_threshold = 0.001   # 0.1%

    slope_strong = abs(ma_slope) > trend_slope_threshold
    atr_high = atr_norm > trend_atr_threshold
    slope_weak = abs(ma_slope) < range_slope_threshold
    atr_low = atr_norm < range_atr_threshold

    if slope_strong and atr_high:
        return "trend"
    elif slope_weak and atr_low:
        return "range"
    else:
        return "mixed"
