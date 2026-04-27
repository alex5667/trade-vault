# -*- coding: utf-8 -*-
"""
PositionSizer — расчёт размера позиции от баланса, процента риска и ATR.

Public API (backward-compatible):
    SymbolSpecs   — dataclass со спецификациями инструмента
    PositionSizer — вычислитель размера позиции
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass

logger = logging.getLogger(__name__)

_EPSILON = 1e-9  # защита от деления на ноль при сравнениях


@dataclass
class SymbolSpecs:
    """Спецификации торгового инструмента.

    Attributes:
        point:               Размер тика в цене (> 0).
        tick_value_per_lot:  Денежная ценность 1 тика на 1 лот (> 0), USD.
        min_lot:             Минимальный размер лота (> 0).
        max_lot:             Максимальный размер лота (>= min_lot).
        lot_step:            Шаг лота (> 0).
    """

    point: float               # размер тика в цене
    tick_value_per_lot: float  # денежная ценность 1 тика на 1 лот, USD
    min_lot: float = 0.01
    max_lot: float = 10.0
    lot_step: float = 0.01

    def __post_init__(self) -> None:  # noqa: D401
        """Validate field constraints on creation."""
        errors: list[str] = []
        if self.point <= 0:
            errors.append(f"point must be > 0, got {self.point}")
        if self.tick_value_per_lot <= 0:
            errors.append(
                f"tick_value_per_lot must be > 0, got {self.tick_value_per_lot}"
            )
        if self.lot_step <= 0:
            errors.append(f"lot_step must be > 0, got {self.lot_step}")
        if self.min_lot <= 0:
            errors.append(f"min_lot must be > 0, got {self.min_lot}")
        if self.max_lot < self.min_lot:
            errors.append(
                f"max_lot ({self.max_lot}) must be >= min_lot ({self.min_lot})"
            )
        if errors:
            raise ValueError(f"SymbolSpecs validation failed: {'; '.join(errors)}")


class PositionSizer:
    """Вычислитель размера позиции по формуле ATR-риска.

    Формула (все аргументы валидны):
        money_risk   = balance * risk_pct / 100
        stop_dist    = atr * atr_sl_mult
        ticks        = max(stop_dist / point, 1)      # минимум 1 тик
        lot          = money_risk / (ticks * tick_value_per_lot)
        lot          = clamp(round_to_step(lot), min_lot, max_lot)

    В случае невалидных входных данных (atr <= 0, point <= 0 и т.п.)
    возвращает (min_lot, atr_sl_mult * max(atr, 1.0)) и пишет предупреждение.
    """

    def __init__(self, specs: SymbolSpecs) -> None:
        self.specs = specs

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _round_lot(self, lot: float) -> float:
        """Clamp *lot* to [min_lot, max_lot] and round down to lot_step."""
        step = self.specs.lot_step
        lot = max(self.specs.min_lot, min(self.specs.max_lot, lot))
        # Use integer arithmetic to avoid floating-point drift
        steps = math.floor(lot / step)
        return round(steps * step, 8)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def size_by_atr(
        self,
        balance: float,
        risk_pct: float,
        atr: float,
        atr_sl_mult: float,
    ) -> tuple[float, float]:
        """Рассчитать лот и расстояние до стоп-лосса.

        Args:
            balance:     Баланс счёта в USD.
            risk_pct:    Процент риска от баланса (например, 1.0 = 1 %).
            atr:         Текущее значение ATR в ценовых единицах.
            atr_sl_mult: Множитель ATR для расстояния до SL.

        Returns:
            (lot, stop_dist) — скругленный лот и расстояние до SL.

        Notes:
            При невалидных параметрах (atr <= 0, point <= 0, tvpl <= 0)
            возвращается (min_lot, atr_sl_mult * max(atr, 1.0)) и
            логируется предупреждение.
        """
        s = self.specs
        if atr <= 0 or s.point <= 0 or s.tick_value_per_lot <= 0:
            fallback_stop = atr_sl_mult * max(atr, 1.0)
            logger.warning(
                "PositionSizer fallback: atr=%.6f point=%.6f tvpl=%.6f "
                "→ lot=%.4f stop=%.6f",
                atr,
                s.point,
                s.tick_value_per_lot,
                s.min_lot,
                fallback_stop,
            )
            return (s.min_lot, fallback_stop)

        money_risk = balance * (risk_pct / 100.0)
        stop_dist = atr * atr_sl_mult
        ticks = max(stop_dist / s.point, 1.0)
        lot = money_risk / (ticks * s.tick_value_per_lot)
        lot = self._round_lot(lot)
        return (lot, stop_dist)
