from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Deque, Literal, Optional

ExtremumKind = Literal["high", "low"]

# Sentinel: bar count effectively meaning "never seen a previous extremum"
_LARGE_BAR_COUNT: int = 10**9
# Threshold below which we consider bars_since_prev meaningful (not sentinel)
_SENTINEL_THRESHOLD: int = 10**8


@dataclass
class LocalExtremeEvent:
    ts: int               # timestamp экстремума (из центра окна)
    price: float          # цена экстремума
    kind: ExtremumKind    # "high" или "low"
    move_from_prev_bps: Optional[float] = None  # ход в bps от предыдущего экстремума
    bars_since_prev: Optional[int] = None       # сколько баров прошло с прошлого экстремума


class LocalExtremaConfig:
    def __init__(
        self,
        lookback_left: int = 2,
        lookback_right: int = 2,
        min_bars_between_extremes: int = 5,
        min_move_bps: float = 20.0,   # минимальный ход от предыдущего экстремума
    ) -> None:
        self.lookback_left = lookback_left
        self.lookback_right = lookback_right
        self.window_size = lookback_left + 1 + lookback_right
        self.min_bars_between_extremes = min_bars_between_extremes
        self.min_move_bps = min_move_bps


class LocalExtremaService:
    """
    Реал-тайм детектор локальных экстремумов по последовательности баров (бакетов).

    Важно: экстремум подтверждается только когда окно полностью заполнено.
    То есть событие "новый локальный максимум" возникает спустя
    `lookback_right` баров после самой точки экстремума (симметричное окно).
    """

    def __init__(self, config: Optional[LocalExtremaConfig] = None) -> None:
        self.cfg = config or LocalExtremaConfig()
        self._window: Deque[tuple[int, float]] = deque(maxlen=self.cfg.window_size)

        # состояние последнего принятого экстремума
        self._last_extreme_price: Optional[float] = None
        self._last_extreme_ts: Optional[int] = None
        self._bars_since_last_extreme: int = _LARGE_BAR_COUNT

        # счётчик всех обработанных баров (для отладки/метрик)
        self._bars_total: int = 0

    def reset(self) -> None:
        self._window.clear()
        self._last_extreme_price = None
        self._last_extreme_ts = None
        self._bars_since_last_extreme = _LARGE_BAR_COUNT
        self._bars_total = 0

    def feed(self, ts_ms: int, price: float) -> Optional[LocalExtremeEvent]:
        """
        Кормим сервис одним баром (бакетом): close-price + ts.

        Возвращает:
          - LocalExtremeEvent, если на этом шаге подтверждён НОВЫЙ локальный максимум/минимум;
          - None, если экстремума нет.
        """
        if price <= 0.0:
            # игнорируем некорректные цены
            self._bars_total += 1
            self._bars_since_last_extreme += 1
            return None

        self._window.append((ts_ms, price))
        self._bars_total += 1
        self._bars_since_last_extreme += 1

        # пока окно не заполнено — ничего детектировать нельзя
        if len(self._window) < self.cfg.window_size:
            return None

        # центр окна — кандидат в экстремум
        mid_idx = self.cfg.lookback_left
        ts_mid, price_mid = self._window[mid_idx]

        # Извлекаем соседние цены без создания промежуточного списка окна
        win = self._window
        left_prices = [win[i][1] for i in range(mid_idx)]
        right_prices = [win[i][1] for i in range(mid_idx + 1, len(win))]
        neighbours = left_prices + right_prices

        is_local_max = all(price_mid >= p for p in neighbours) and any(
            price_mid > p for p in neighbours
        )
        is_local_min = all(price_mid <= p for p in neighbours) and any(
            price_mid < p for p in neighbours
        )

        if not (is_local_max or is_local_min):
            return None

        # фильтр по минимальному количеству баров между экстремумами
        if self._bars_since_last_extreme < self.cfg.min_bars_between_extremes:
            return None

        # фильтр по минимальному ходу в bps от предыдущего экстремума
        move_bps: Optional[float] = None
        if self._last_extreme_price is not None and self._last_extreme_price > 0:
            rel = (price_mid - self._last_extreme_price) / self._last_extreme_price
            move_bps = abs(rel) * 10_000.0
            if move_bps < self.cfg.min_move_bps:
                return None

        kind: ExtremumKind = "high" if is_local_max else "low"

        event = LocalExtremeEvent(
            ts=ts_mid,
            price=price_mid,
            kind=kind,
            move_from_prev_bps=move_bps,
            bars_since_prev=(
                self._bars_since_last_extreme
                if self._bars_since_last_extreme < _SENTINEL_THRESHOLD
                else None
            ),
        )

        # обновляем состояние
        self._last_extreme_price = price_mid
        self._last_extreme_ts = ts_mid
        self._bars_since_last_extreme = 0

        return event
