"""
Burstiness Tracker - отслеживание кластеризации и взрывности торгов.

Метрики:
- rate_short / rate_long: EWMA интенсивности торгов (короткое/длинное окно)
- burst_ratio: отношение короткой к длинной интенсивности
- cv_dt: коэффициент вариации интервалов между сделками
- fano_counts: Fano factor (дисперсия/среднее) для количества сделок в бакетах
- flip_ratio: доля переключений направления (buy -> sell, sell -> buy)

O(1) на тик, без аллокаций на горячем пути.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from typing import Optional


@dataclass
class BurstStats:
    trade_count_bucket: int = 0
    rate_short: float = 0.0
    rate_long: float = 0.0
    burst_ratio: float = 0.0
    cv_dt: float = 0.0
    fano_counts: float = 0.0
    flip_ratio: float = 0.0


class BurstinessTracker:
    """
    Отслеживание взрывности и кластеризации торгов.
    
    Использует экспоненциальные скользящие средние (EWMA) для отслеживания
    интенсивности торгов и статистики по бакетам.
    """

    def __init__(
        self
        bucket_ms: int
        half_life_short_ms: int = 250
        half_life_long_ms: int = 2000
        fano_window_buckets: int = 60
        dt_alpha: float = 0.05
    ):
        """
        Args:
            bucket_ms: Размер бакета в миллисекундах
            half_life_short_ms: Полупериод распада для короткого окна (по умолчанию 250 мс)
            half_life_long_ms: Полупериод распада для длинного окна (по умолчанию 2000 мс)
            fano_window_buckets: Размер окна для Fano factor (по умолчанию 60 бакетов)
            dt_alpha: Альфа для EWMA интервалов между сделками (по умолчанию 0.05)
        """
        self.bucket_ms = max(1, int(bucket_ms))
        self.beta_s = math.log(2) / max(1.0, float(half_life_short_ms))
        self.beta_l = math.log(2) / max(1.0, float(half_life_long_ms))

        self.last_ts = 0
        self.lambda_s = 0.0  # EWMA интенсивность короткого окна
        self.lambda_l = 0.0  # EWMA интенсивность длинного окна

        # EWMA для dt mean/var
        self.dt_mean = 0.0
        self.dt_m2 = 0.0
        self.dt_alpha = dt_alpha

        # per-bucket counters
        self._bucket_id: Optional[int] = None
        self._bucket_trades = 0
        self._bucket_flips = 0
        self._last_side = 0  # -1, 1, or 0

        # rolling Fano for trade counts
        self.w = max(10, int(fano_window_buckets))
        self.counts = deque([0] * self.w, maxlen=self.w)
        self.sum_c = 0.0
        self.sumsq_c = 0.0

    def on_trade(self, ts: int, side: int) -> None:
        """
        Обработка сделки.
        
        Args:
            ts: Временная метка в миллисекундах
            side: Направление сделки (-1 для sell, +1 для buy, 0 для неизвестного)
        """
        if ts <= 0:
            return

        # Обновление EWMA интенсивности
        if self.last_ts > 0:
            dt = max(1, ts - self.last_ts)
            
            # Экспоненциальное затухание
            self.lambda_s = self.lambda_s * math.exp(-self.beta_s * dt) + 1.0
            self.lambda_l = self.lambda_l * math.exp(-self.beta_l * dt) + 1.0

            # EWMA для статистики интервалов между сделками
            a = self.dt_alpha
            d = float(dt)
            # EWMA mean
            self.dt_mean = (1 - a) * self.dt_mean + a * d
            # EWMA second moment
            self.dt_m2 = (1 - a) * self.dt_m2 + a * (d * d)

        self.last_ts = ts

        # Определение бакета
        b = ts // self.bucket_ms
        if self._bucket_id is None:
            self._bucket_id = b

        # Инкремент счетчика сделок в бакете
        # (переключение бакета обрабатывается в on_bucket_advance)
        self._bucket_trades += 1

        # Отслеживание переключений направления
        if side in (-1, 1):
            if self._last_side in (-1, 1) and side != self._last_side:
                self._bucket_flips += 1
            self._last_side = side

    def on_bucket_advance(self, bucket_id: int) -> BurstStats:
        """
        Вызывается при переходе на новый бакет.
        
        Args:
            bucket_id: ID нового бакета
            
        Returns:
            BurstStats: Статистика по предыдущему бакету
        """
        # Сохраняем данные предыдущего бакета
        prev_trades = self._bucket_trades
        prev_flips = self._bucket_flips

        # Обновление Fano rolling window
        old = self.counts[0] if len(self.counts) == self.w else 0
        if len(self.counts) == self.w:
            # Удаляем старое значение из сумм (deque автоматически удалит слева)
            self.sum_c -= old
            self.sumsq_c -= old * old

        # Добавляем новое значение
        self.counts.append(prev_trades)
        self.sum_c += prev_trades
        self.sumsq_c += prev_trades * prev_trades

        # Вычисление Fano factor (дисперсия / среднее)
        n = max(1, len(self.counts))
        mean = self.sum_c / n
        var = (self.sumsq_c / n) - mean * mean
        fano = (var / mean) if mean > 1e-9 else 0.0

        # CV (коэффициент вариации) интервалов между сделками из EWMA моментов
        var_dt = max(0.0, self.dt_m2 - self.dt_mean * self.dt_mean)
        cv = (math.sqrt(var_dt) / self.dt_mean) if self.dt_mean > 1e-9 else 0.0

        # Отношение короткой к длинной интенсивности
        burst_ratio = self.lambda_s / max(1e-9, self.lambda_l)

        # Доля переключений направления
        flip_ratio = (prev_flips / max(1, prev_trades - 1)) if prev_trades > 1 else 0.0

        # Сброс счетчиков для нового бакета
        self._bucket_id = bucket_id
        self._bucket_trades = 0
        self._bucket_flips = 0
        self._last_side = 0

        return BurstStats(
            trade_count_bucket=int(prev_trades)
            rate_short=float(self.lambda_s)
            rate_long=float(self.lambda_l)
            burst_ratio=float(burst_ratio)
            cv_dt=float(cv)
            fano_counts=float(fano)
            flip_ratio=float(flip_ratio)
        )

