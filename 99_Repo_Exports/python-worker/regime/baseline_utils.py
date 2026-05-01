from __future__ import annotations

import math
from typing import Iterable, Sequence, List, Dict

from .models import SignalExecRow, BaselineQuantiles


def sliding_windows(seq: Sequence[SignalExecRow], window_size: int) -> Iterable[Sequence[SignalExecRow]]:
    """
    Генерирует скользящие окна по N сигналов из последовательности.
    """
    n = len(seq)
    if window_size <= 0 or n < window_size:
        return
    for start in range(0, n - window_size + 1):
        yield seq[start:start + window_size]


def _quantile(values: List[float], q: float) -> float:
    """
    Вычисляет квантиль с линейной интерполяцией.
    q в [0,1]
    """
    if not values:
        return float("nan")
    xs = sorted(values)
    if len(xs) == 1:
        return xs[0]

    pos = q * (len(xs) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return xs[lo]
    w = pos - lo
    return xs[lo] * (1.0 - w) + xs[hi] * w


def compute_quantiles(values: List[float]) -> BaselineQuantiles:
    """Вычисляет все необходимые квантили для списка значений."""
    if not values:
        return BaselineQuantiles(
            p05=None,
            p10=None,
            p25=None,
            p50=None,
            p75=None,
            p90=None,
            p95=None,
            sample_size=0,
        )

    return BaselineQuantiles(
        p05=_quantile(values, 0.05),
        p10=_quantile(values, 0.10),
        p25=_quantile(values, 0.25),
        p50=_quantile(values, 0.50),
        p75=_quantile(values, 0.75),
        p90=_quantile(values, 0.90),
        p95=_quantile(values, 0.95),
        sample_size=len(values),
    )


def compute_family_baseline(
    rows: List[SignalExecRow],
    window_size: int,
) -> Dict[str, BaselineQuantiles]:
    """
    Вычисляет baseline для одной пары symbol+family.

    rows — сигналы по одному symbol+family (за horizon_days),
    отсортированные по opened_at.

    Возвращает квантили для 'hit_rate' и 'expectancy_R'.
    """
    rows = sorted(rows, key=lambda r: r.opened_at)

    hit_rates: List[float] = []
    expectancies: List[float] = []

    for win in sliding_windows(rows, window_size):
        # результаты по окну
        rs = [r.result_r for r in win]
        n = len(rs)
        if n == 0:
            continue

        # hit-rate
        wins = sum(1 for x in rs if x > 0.0)
        hit_rate = wins / n

        # expectancy
        expectancy_r = sum(rs) / n

        hit_rates.append(hit_rate)
        expectancies.append(expectancy_r)

    return {
        "hit_rate": compute_quantiles(hit_rates),
        "expectancy_R": compute_quantiles(expectancies),
    }
