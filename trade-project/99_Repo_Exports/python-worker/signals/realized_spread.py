# signals/realized_spread.py
from __future__ import annotations

"""
Realized Spread Tracker для микроструктурного анализа.

Отслеживает "post-factum" follow-through относительно mid через lag_ms.
Используется для определения momentum vs absorption в криптовалютных рынках.

Без Redis/JSON: только арифметика + deque + EMA для максимальной производительности.
"""

from collections import deque
from dataclasses import dataclass


def _ema_update(prev: float | None, x: float, alpha: float) -> float:
    """EMA update: prev + alpha * (x - prev)"""
    if prev is None:
        return x
    return prev + alpha * (x - prev)


@dataclass(frozen=True)
class RealizedSpreadMetrics:
    """Метрики realized spread для микроструктурного анализа."""
    # "post-factum" follow-through относительно mid через lag_ms
    realized_bps: float
    realized_ema_bps: float

    # текущий спред (L1) и его EMA
    spread_bps: float
    spread_ema_bps: float

    # EMA доли "плохих" follow-through (mid пошёл против агрессора)
    adverse_ratio_ema: float

    # сколько сделок реализовали (matured) за всё время
    realized_count: int


@dataclass
class _PendingTrade:
    """Pending trade для отложенного расчета realized spread."""
    ts: int          # timestamp trade
    q: int           # +1 taker buy, -1 taker sell
    price: float     # trade price (last)
    ref: float       # reference (обычно mid в момент trade)


class RealizedSpreadTracker:
    """
    Лёгкий трекер realized spread / slippage "post-factum" относительно mid (нужен L1).

    Идея:
    - По trade (через is_buyer_maker или last vs mid) кладём в очередь (pending).
    - Через lag_ms, когда пришли новые котировки, считаем follow-through:
        realized_bps = q * (mid_now - trade_price) / trade_price * 10_000
      где q=+1 для taker-buy, q=-1 для taker-sell.
      Положительное => цена продолжила движение в сторону агрессора (momentum).
      Отрицательное => adverse selection / absorption (агрессор "впитался").

    Без Redis/JSON: только арифметика + deque + EMA.
    """

    def __init__(
        self,
        *,
        lag_ms: int = 2000,
        max_pending: int = 4096,
        ema_alpha: float = 0.12,
        spread_ema_alpha: float = 0.08,
        adverse_ema_alpha: float = 0.08,
        max_gap_ms: int = 30_000,  # если поток "прыгнул" — чистим pending
    ) -> None:
        self.lag_ms = int(lag_ms)
        self.max_gap_ms = int(max_gap_ms)

        self.ema_alpha = float(ema_alpha)
        self.spread_ema_alpha = float(spread_ema_alpha)
        self.adverse_ema_alpha = float(adverse_ema_alpha)

        self._pending: deque[_PendingTrade] = deque(maxlen=int(max_pending))

        self._last_ts: int = 0

        self._realized_last: float = 0.0
        self._realized_ema: float | None = None

        self._spread_ema: float | None = None

        self._adverse_ratio_ema: float | None = None

        self._realized_count: int = 0

    def reset(self) -> None:
        """Сброс всех состояний трекера."""
        self._pending.clear()
        self._last_ts = 0
        self._realized_last = 0.0
        self._realized_ema = None
        self._spread_ema = None
        self._adverse_ratio_ema = None
        self._realized_count = 0

    @staticmethod
    def _mid(bid: float, ask: float) -> float:
        """Вычисляет mid price."""
        if bid > 0 and ask > 0:
            return (bid + ask) * 0.5
        return 0.0

    @staticmethod
    def _spread_bps(bid: float, ask: float, mid: float) -> float:
        """Вычисляет спред в базисных пунктах."""
        if mid <= 0 or bid <= 0 or ask <= 0:
            return 0.0
        return (ask - bid) / mid * 10_000.0

    @staticmethod
    def _infer_q(is_buyer_maker: bool | None, last: float, mid: float) -> int:
        """
        Определяет направление агрессии (q).
        
        Binance: isBuyerMaker=true => buyer was maker => SELL was taker => q = -1
        """
        # Binance: isBuyerMaker=true => buyer was maker => SELL was taker => q = -1
        if is_buyer_maker is True:
            return -1
        if is_buyer_maker is False:
            return +1

        # fallback (если фида нет): last vs mid
        if last > 0 and mid > 0:
            return +1 if last > mid else -1
        return 0

    def update(
        self,
        *,
        ts: int,
        bid: float,
        ask: float,
        last: float,
        is_buyer_maker: bool | None = None,
        is_trade_hint: bool | None = None,
    ) -> RealizedSpreadMetrics:
        """
        Вызывать на каждом тике.

        Args:
            ts: timestamp в миллисекундах
            bid: bid price
            ask: ask price
            last: last trade price
            is_buyer_maker: True если buyer был maker (Binance aggTrade.m)
            is_trade_hint: True если точно известно что это trade

        Returns:
            RealizedSpreadMetrics с текущими метриками

        is_trade_hint:
          - если вы точно знаете, что этот tick == trade (например flags&1), передайте True
          - если None, определим эвристикой по last и наличию is_buyer_maker
        """
        ts = int(ts)
        if ts <= 0:
            return self._metrics(0.0, 0.0)

        # если поток "перепрыгнул" — чистим pending, чтобы не делать ложных realized
        if self._last_ts and ts - self._last_ts > self.max_gap_ms:
            self._pending.clear()
        self._last_ts = ts

        mid = self._mid(float(bid), float(ask))
        if mid <= 0:
            return self._metrics(0.0, 0.0)

        # Текущий спред
        spread_bps = self._spread_bps(float(bid), float(ask), mid)
        self._spread_ema = _ema_update(self._spread_ema, spread_bps, self.spread_ema_alpha)

        # Определяем, является ли это trade
        is_trade = False
        if is_trade_hint is True:
            is_trade = True
        elif is_buyer_maker is not None:
            # Если есть is_buyer_maker, значит это trade
            is_trade = True
        elif last > 0 and abs(last - mid) > 1e-9:
            # Эвристика: если last отличается от mid, возможно это trade
            is_trade = True

        # Если это trade, добавляем в pending
        if is_trade and last > 0:
            q = self._infer_q(is_buyer_maker, last, mid)
            if q != 0:
                self._pending.append(_PendingTrade(
                    ts=ts,
                    q=q,
                    price=last,
                    ref=mid
                ))

        # Матурим pending trades (старше lag_ms)
        cutoff = ts - self.lag_ms
        realized_this_tick: list[float] = []

        while self._pending and self._pending[0].ts <= cutoff:
            p = self._pending.popleft()
            # realized = q * (mid_now - trade_price) / trade_price * 10_000
            if p.price > 0:
                realized = float(p.q) * (mid - p.price) / p.price * 10_000.0
                realized_this_tick.append(realized)
                self._realized_count += 1

        # Обновляем EMA realized
        if realized_this_tick:
            avg_realized = sum(realized_this_tick) / len(realized_this_tick)
            self._realized_last = avg_realized
            self._realized_ema = _ema_update(self._realized_ema, avg_realized, self.ema_alpha)

            # Adverse ratio: доля отрицательных (против агрессора)
            adverse_count = sum(1 for r in realized_this_tick if r < 0)
            adverse_ratio = adverse_count / len(realized_this_tick)
            self._adverse_ratio_ema = _ema_update(
                self._adverse_ratio_ema,
                adverse_ratio,
                self.adverse_ema_alpha
            )

        return self._metrics(spread_bps, self._spread_ema or 0.0)

    def _metrics(self, spread_bps: float, spread_ema_bps: float) -> RealizedSpreadMetrics:
        """Формирует текущие метрики."""
        return RealizedSpreadMetrics(
            realized_bps=self._realized_last,
            realized_ema_bps=self._realized_ema or 0.0,
            spread_bps=spread_bps,
            spread_ema_bps=spread_ema_bps,
            adverse_ratio_ema=self._adverse_ratio_ema or 0.0,
            realized_count=self._realized_count,
        )

    def get_metrics(self) -> RealizedSpreadMetrics:
        """Возвращает текущие метрики без обновления."""
        return self._metrics(0.0, self._spread_ema or 0.0)


# Convenience functions для быстрого использования

def create_tracker(
    lag_ms: int = 2000,
    ema_alpha: float = 0.12,
) -> RealizedSpreadTracker:
    """
    Создает трекер с разумными дефолтными параметрами.
    
    Args:
        lag_ms: задержка для расчета realized spread (default: 2000ms)
        ema_alpha: alpha для EMA realized spread (default: 0.12)
    
    Returns:
        Настроенный RealizedSpreadTracker
    """
    return RealizedSpreadTracker(
        lag_ms=lag_ms,
        ema_alpha=ema_alpha,
        spread_ema_alpha=0.08,
        adverse_ema_alpha=0.08,
        max_pending=4096,
        max_gap_ms=30_000,
    )


def interpret_metrics(metrics: RealizedSpreadMetrics) -> str:
    """
    Интерпретирует метрики для логирования/дебага.
    
    Returns:
        Строка с интерпретацией (momentum/absorption/mixed)
    """
    if metrics.realized_count < 10:
        return "warming_up"

    realized = metrics.realized_ema_bps
    adverse = metrics.adverse_ratio_ema

    if realized > 2.0 and adverse < 0.3:
        return "strong_momentum"
    elif realized > 0.5 and adverse < 0.4:
        return "momentum"
    elif realized < -1.0 or adverse > 0.6:
        return "absorption"
    elif adverse > 0.5:
        return "weak_absorption"
    else:
        return "mixed"

