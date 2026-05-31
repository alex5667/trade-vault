"""
Конфигурация TP (Take Profit) для торговых позиций.

Вынесено в отдельный модуль для избежания circular imports.
"""
from __future__ import annotations

import math
import os


def parse_tp_ratio(value: str | None = None) -> list[float]:
    """
    Парсинг переменной окружения TP_RATIO или переданной строки.

    Формат: "0.5,0.3,0.2" или "50,30,20" (проценты)
    По умолчанию: [0.50, 0.30, 0.20]

    Returns:
        Список долей закрытия для TP1, TP2, TP3
    """
    if not value:
        value = os.getenv("TP_RATIO")

    if not value:
        return [0.50, 0.30, 0.20]  # Значение по умолчанию

    try:
        parts = [part.strip() for part in value.split(",") if part.strip()]
        if len(parts) < 2:
            parts.extend(["0.0"] * (2 - len(parts)))

        ratios: list[float] = []
        for part in parts:
            ratio = float(part)
            # Если значение > 1, считаем это процентами и конвертируем
            if ratio > 1.0:
                ratio = ratio / 100.0
            ratios.append(ratio)

        # Нормализация: если сумма > 1, нормализуем
        total = sum(ratios)
        if total > 1.0:
            ratios = [r / total for r in ratios]

        return ratios
    except (ValueError, TypeError) as e:
        import logging
        logging.warning("⚠️ Invalid TP_RATIO format '%s', using default [0.50, 0.30, 0.20]: %s", value, e)
        return [0.50, 0.30, 0.20]


# Значение по умолчанию для использования в других модулях
TP_RATIOS_DEFAULT = tuple(parse_tp_ratio())


# ---------------------------------------------------------------------------
# Strategy-aware TP quantity computation
# ---------------------------------------------------------------------------

def _round_down(x: float, step: float) -> float:
    """Round x down to nearest multiple of step (LOT_SIZE quantisation)."""
    if step <= 0:
        return x
    return math.floor(x / step) * step


def compute_tp_qtys(
    total_qty: float,
    ratios: list[float] | tuple[float, ...],
    step_size: float = 0.0,
) -> list[float]:
    """Distribute total_qty across TP levels according to ratios.

    - Normalizes ratios to sum=1.0
    - Rounds each TP qty DOWN to step_size (Binance LOT_SIZE)
    - Last TP absorbs remainder to avoid dust

    Args:
        total_qty: Total position quantity
        ratios: Volume split per TP (e.g. (0.50, 0.50) or (0.40, 0.30, 0.30))
        step_size: Exchange LOT_SIZE step for quantisation (0=no rounding)

    Returns:
        list[float] — qty for each TP level, sum == total_qty
    """
    if not ratios or total_qty <= 0:
        return []

    # Normalise ratios to sum=1.0
    r_list = [max(0.0, r) for r in ratios]
    r_sum = sum(r_list)
    if r_sum <= 0:
        # Fallback: even split
        r_list = [1.0 / len(ratios)] * len(ratios)
        r_sum = 1.0
    if abs(r_sum - 1.0) > 1e-9:
        r_list = [r / r_sum for r in r_list]

    n = len(r_list)
    parts: list[float] = []
    remaining = total_qty

    for i in range(n):
        if i == n - 1:
            # Last TP gets remainder (no dust)
            q = remaining
        else:
            q = total_qty * r_list[i]
            if step_size > 0:
                q = _round_down(q, step_size)
            remaining -= q

        if q > 0:
            parts.append(q)
        else:
            parts.append(0.0)

    # Guard: if rounding pushed sum above total
    s = sum(parts)
    if s > total_qty and step_size > 0 and parts:
        parts[-1] = _round_down(parts[-1] - (s - total_qty), step_size)

    return [q for q in parts if q > 0]


def compute_even_split_tp_qtys(
    total_qty: float,
    n_tps: int,
    step_size: float = 0.0,
) -> list[float]:
    """Even split fallback — used when no tp_ratios are provided.

    Equivalent to old _split_tp_qtys from monolith executor.
    """
    if n_tps <= 0:
        return []
    if n_tps == 1:
        return [total_qty]

    ratios = [1.0 / n_tps] * n_tps
    return compute_tp_qtys(total_qty, ratios, step_size)
