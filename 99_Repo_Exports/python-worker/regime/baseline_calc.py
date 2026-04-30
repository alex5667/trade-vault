"""
Оффлайн-расчет baseline-квантилей для signal families.

Считает скользящие окна по истории сигналов и вычисляет квантили
для hit_rate и expectancy_R по каждому семейству сигналов.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Iterable, Sequence, Optional
import math


@dataclass
class SignalExecRow:
    """Строка из таблицы signal_exec_summary"""
    signal_id: int
    symbol: str
    family: str
    opened_at: datetime
    result_r: float


@dataclass
class BaselineQuantiles:
    """Квантили для одной метрики"""
    p05: Optional[float]
    p10: Optional[float]
    p25: Optional[float]
    p50: Optional[float]
    p75: Optional[float]
    p90: Optional[float]
    p95: Optional[float]
    sample_size: int  # сколько окон


def sliding_windows(seq: Sequence[SignalExecRow], window_size: int) -> Iterable[Sequence[SignalExecRow]]:
    """
    Генератор скользящих окон по N сигналов.
    Возвращает последовательности окон: [0:N], [1:N+1], [2:N+2], ...
    """
    n = len(seq)
    if window_size <= 0 or n < window_size:
        return
    for start in range(0, n - window_size + 1):
        yield seq[start:start + window_size]


def _quantile(values: List[float], q: float) -> float:
    """
    Вычисляет q-квантиль с линейной интерполяцией между соседними значениями.
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
    """Вычисляет все квантили для списка значений"""
    if not values:
        return BaselineQuantiles(
            p05=None
            p10=None
            p25=None
            p50=None
            p75=None
            p90=None
            p95=None
            sample_size=0
        )

    return BaselineQuantiles(
        p05=_quantile(values, 0.05)
        p10=_quantile(values, 0.10)
        p25=_quantile(values, 0.25)
        p50=_quantile(values, 0.50)
        p75=_quantile(values, 0.75)
        p90=_quantile(values, 0.90)
        p95=_quantile(values, 0.95)
        sample_size=len(values)
    )


def compute_family_baseline(
    rows: List[SignalExecRow]
    window_size: int
) -> Dict[str, BaselineQuantiles]:
    """
    Рассчитывает baseline для одного семейства сигналов (symbol + family).

    Args:
        rows: Сигналы за период horizon_days, отсортированные по opened_at
        window_size: Размер скользящего окна (количество сигналов)

    Returns:
        Словарь с квантилями для 'hit_rate' и 'expectancy_R'
    """
    # Сортируем по времени открытия (на всякий случай)
    rows = sorted(rows, key=lambda r: r.opened_at)

    hit_rates: List[float] = []
    expectancies: List[float] = []

    # Проходим по всем скользящим окнам
    for win in sliding_windows(rows, window_size):
        # Извлекаем результаты в R для окна
        rs = [r.result_r for r in win]
        n = len(rs)
        if n == 0:
            continue

        # Hit rate: доля прибыльных сигналов
        wins = sum(1 for x in rs if x > 0.0)
        hit_rate = wins / n

        # Expectancy: математическое ожидание в R
        expectancy_r = sum(rs) / n

        hit_rates.append(hit_rate)
        expectancies.append(expectancy_r)

    return {
        "hit_rate": compute_quantiles(hit_rates)
        "expectancy_R": compute_quantiles(expectancies)
    }
