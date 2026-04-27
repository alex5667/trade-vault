from __future__ import annotations

from collections import deque
from typing import Optional, Tuple

from .l3_lite_models import (
    L3LiteEvent,
    BookSnapshot,
    L3LiteFeatures,
    CancelTradeBuffers,
    MicropriceHistoryPoint,
)


class L3LiteMetricsAggregator:
    """
    Агрегатор L3-Lite метрик для расчета фич из потока событий.
    """

    def __init__(self, microprice_horizon_sec: int = 20, obi_persistence_sec: int = 30) -> None:
        self.buffers = CancelTradeBuffers()

        self._book: Optional[BookSnapshot] = None
        self._microprice_history: deque[MicropriceHistoryPoint] = deque()
        self._microprice_horizon_ms = microprice_horizon_sec * 1000
        self._obi_persistence_ms = obi_persistence_sec * 1000

        # для вычисления persistence по знаку OBI (например, по obi_5)
        self._obi_sign_history: deque[Tuple[int, int]] = deque()  # (ts_ms, sign), sign ∈ {-1, 0, +1}

    # === input methods ===

    def on_l3_event(self, ev: L3LiteEvent) -> None:
        """
        Подключить в место, где ты обрабатываешь L3-Lite события.
        """
        if ev.kind == "cancel":
            if ev.side == "bid":
                self.buffers.cancels_bid.append((ev.ts_ms, ev.qty))
            else:
                self.buffers.cancels_ask.append((ev.ts_ms, ev.qty))
        elif ev.kind == "trade":
            # предположим: trade side == 'bid' → агрессивные продажи, удар по bid
            # для cancel_to_trade_bid нам нужна "активность" по bid:
            if ev.side == "bid":
                self.buffers.trades_bid.append((ev.ts_ms, ev.qty))
            else:
                self.buffers.trades_ask.append((ev.ts_ms, ev.qty))

        # остальное ('new', 'replace') пока не используем

    def on_book_update(self, snap: BookSnapshot) -> None:
        """
        Вызывать при каждом обновлении книги (или на каждом тике).
        """
        self._book = snap
        mp = self._calc_microprice(snap)
        if mp is not None and mp > 0:
            self._microprice_history.append(MicropriceHistoryPoint(ts_ms=snap.ts_ms, microprice=mp))

    # === core calculation helpers ===

    def _calc_microprice(self, snap: BookSnapshot) -> Optional[float]:
        """Рассчитать микропрайс по снимку книги."""
        if not snap.bids or not snap.asks:
            return None

        best_bid, bid_qty = snap.bids[0]
        best_ask, ask_qty = snap.asks[0]
        if bid_qty <= 0 or ask_qty <= 0:
            return None

        # microprice = (A * Pb + B * Pa) / (A + B)
        # A = ask_qty, B = bid_qty
        A = ask_qty
        B = bid_qty
        return (A * best_bid + B * best_ask) / (A + B)

    def _cleanup_deque(self, dq: deque[Tuple[int, float]], now_ms: int, window_ms: int) -> None:
        """Очистить deque от записей старше window_ms."""
        cutoff = now_ms - window_ms
        while dq and dq[0][0] < cutoff:
            dq.popleft()

    def _sum_volume(self, dq: deque[Tuple[int, float]]) -> float:
        """Посчитать суммарный объем в deque."""
        return float(sum(v for _, v in dq))

    def _calc_cancel_to_trade(
        self,
        now_ms: int,
        window_ms: int,
    ) -> Tuple[float, float]:
        """Рассчитать cancel_to_trade для bid и ask."""
        # чистим старые записи
        self._cleanup_deque(self.buffers.cancels_bid, now_ms, window_ms)
        self._cleanup_deque(self.buffers.cancels_ask, now_ms, window_ms)
        self._cleanup_deque(self.buffers.trades_bid, now_ms, window_ms)
        self._cleanup_deque(self.buffers.trades_ask, now_ms, window_ms)

        canc_bid = self._sum_volume(self.buffers.cancels_bid)
        canc_ask = self._sum_volume(self.buffers.cancels_ask)
        tr_bid = self._sum_volume(self.buffers.trades_bid)
        tr_ask = self._sum_volume(self.buffers.trades_ask)

        eps = 1e-9
        c2t_bid = canc_bid / max(tr_bid, eps)
        c2t_ask = canc_ask / max(tr_ask, eps)
        return c2t_bid, c2t_ask

    def _calc_microprice_shift_bps_20(self, now_ms: int) -> Tuple[float, float]:
        """
        Рассчитать текущий микропрайс и сдвиг за 20 секунд в bps.
        Возвращает (microprice, shift_bps_20).
        shift_bps_20 = (mp_now - mp_20sec_ago)/mid_now * 1e4
        """
        if self._book is None:
            return 0.0, 0.0

        current_mp = self._calc_microprice(self._book) or 0.0
        if current_mp <= 0:
            return current_mp, 0.0

        # чистим историю дальше, чем horizon_ms
        cutoff = now_ms - self._microprice_horizon_ms
        while self._microprice_history and self._microprice_history[0].ts_ms < cutoff:
            self._microprice_history.popleft()

        # ищем точку, максимально близкую к cutoff
        if not self._microprice_history:
            return current_mp, 0.0

        # берём первую (самую старую) — она ок для "микро 20 сек назад"
        mp_old = self._microprice_history[0].microprice

        # midprice
        best_bid, _ = self._book.bids[0]
        best_ask, _ = self._book.asks[0]
        mid = 0.5 * (best_bid + best_ask)
        if mid <= 0:
            return current_mp, 0.0

        shift_bps = (current_mp - mp_old) / mid * 1e4
        return current_mp, shift_bps

    def _calc_spread_and_obi(self) -> Tuple[float, float, float, float]:
        """
        Рассчитать spread_bps, obi_5, obi_20, obi_50
        """
        if self._book is None or not self._book.bids or not self._book.asks:
            return 0.0, 0.0, 0.0, 0.0

        bids = self._book.bids
        asks = self._book.asks

        best_bid, _ = bids[0]
        best_ask, _ = asks[0]
        mid = 0.5 * (best_bid + best_ask)
        if mid <= 0:
            spread_bps = 0.0
        else:
            spread_bps = (best_ask - best_bid) / mid * 1e4

        def obi_for_L(L: int) -> float:
            b_slice = bids[:L]
            a_slice = asks[:L]
            v_bid = sum(q for _, q in b_slice)
            v_ask = sum(q for _, q in a_slice)
            total = v_bid + v_ask
            if total <= 0:
                return 0.0
            return (v_bid - v_ask) / total

        obi_5 = obi_for_L(5)
        obi_20 = obi_for_L(20)
        obi_50 = obi_for_L(50)
        return spread_bps, obi_5, obi_20, obi_50

    def _update_obi_sign(self, now_ms: int, obi_5: float, threshold: float = 0.2) -> None:
        """
        Сохраняем историю знака OBI_5 для последующей оценки persistence.
        |obi_5| < threshold → считаем sign = 0 (нет выраженного перекоса).
        """
        if abs(obi_5) < threshold:
            sign = 0
        else:
            sign = 1 if obi_5 > 0 else -1

        self._obi_sign_history.append((now_ms, sign))

        cutoff = now_ms - self._obi_persistence_ms
        while self._obi_sign_history and self._obi_sign_history[0][0] < cutoff:
            self._obi_sign_history.popleft()

    def _calc_obi_persistence_score(self) -> float:
        """
        Доля времени в последнем окне, когда sign ≠ 0 и не менялся.
        Пример: если 90% времени OBI стабильно положительный или отрицательный → score ~ 0.9.
        """
        if not self._obi_sign_history:
            return 0.0

        # считаем долю записей, где sign != 0 и sign одинаковый
        signs = [s for _, s in self._obi_sign_history]
        non_zero = [s for s in signs if s != 0]
        if not non_zero:
            return 0.0

        # проверим, насколько часто sign сохраняется
        base_sign = non_zero[0]
        stable_count = sum(1 for s in non_zero if s == base_sign)
        persistence = stable_count / len(signs)  # нормировка на общее кол-во точек в окне
        return float(persistence)

    def _calc_microprice_velocity(self, now_ms: int) -> float:
        """Рассчитать скорость изменения микропрайса (bps/сек)."""
        if len(self._microprice_history) < 2:
            return 0.0

        # Возьмем последние 2 точки для оценки скорости
        recent = list(self._microprice_history)[-2:]
        if len(recent) < 2:
            return 0.0

        dt_ms = recent[1].ts_ms - recent[0].ts_ms
        if dt_ms <= 0:
            return 0.0

        dmp = recent[1].microprice - recent[0].microprice
        velocity_bps_per_sec = (dmp / recent[0].microprice) * 1e4 / (dt_ms / 1000.0)

        return velocity_bps_per_sec

    def _calc_queue_pressure(self, cancel_to_trade: float, obi: float, side: str) -> float:
        """Рассчитать давление на очередь (комбинированная метрика)."""
        # Нормализуем cancel_to_trade (0-1 шкала)
        c2t_norm = min(1.0, cancel_to_trade / 5.0)  # max threshold = 5.0

        # Для bid side: давление растет при obi < 0 (продавцы доминируют)
        # Для ask side: давление растет при obi > 0 (покупатели доминируют)
        obi_pressure = abs(obi) if (side == "bid" and obi < 0) or (side == "ask" and obi > 0) else 0.0

        # Комбинированная метрика
        pressure = 0.6 * c2t_norm + 0.4 * min(1.0, obi_pressure)
        return pressure

    def _calc_market_depth_imbalance(self) -> float:
        """Рассчитать несбалансированность глубины книги."""
        if self._book is None or not self._book.bids or not self._book.asks:
            return 0.0

        bids = self._book.bids
        asks = self._book.asks

        # Возьмем топ-10 уровней или сколько есть
        depth_levels = 10
        bid_volume = sum(qty for _, qty in bids[:depth_levels])
        ask_volume = sum(qty for _, qty in asks[:depth_levels])

        total_volume = bid_volume + ask_volume
        if total_volume <= 0:
            return 0.0

        # Имбаланс: положительный = перевес bid, отрицательный = перевес ask
        imbalance = (bid_volume - ask_volume) / total_volume
        return imbalance

    # === public method ===

    def build_features(self, now_ms: int) -> Optional[L3LiteFeatures]:
        """
        Вызывается в момент оценки сигнала (или на каждом тике) для построения L3-фич.
        """
        if self._book is None:
            return None

        spread_bps, obi_5, obi_20, obi_50 = self._calc_spread_and_obi()
        self._update_obi_sign(now_ms, obi_5)
        obi_persistence_score = self._calc_obi_persistence_score()

        # cancel_to_trade по двум окнам: 5s и 20s
        c2t_bid_5, c2t_ask_5 = self._calc_cancel_to_trade(now_ms, window_ms=5_000)
        c2t_bid_20, c2t_ask_20 = self._calc_cancel_to_trade(now_ms, window_ms=20_000)

        microprice, mp_shift_bps_20 = self._calc_microprice_shift_bps_20(now_ms)

        # Дополнительные метрики
        microprice_velocity = self._calc_microprice_velocity(now_ms)
        queue_pressure_bid = self._calc_queue_pressure(c2t_bid_20, obi_5, side="bid")
        queue_pressure_ask = self._calc_queue_pressure(c2t_ask_20, obi_5, side="ask")
        market_depth_imbalance = self._calc_market_depth_imbalance()

        return L3LiteFeatures(
            cancel_to_trade_bid_5s=c2t_bid_5,
            cancel_to_trade_ask_5s=c2t_ask_5,
            cancel_to_trade_bid_20s=c2t_bid_20,
            cancel_to_trade_ask_20s=c2t_ask_20,
            microprice=microprice,
            microprice_shift_bps_20=mp_shift_bps_20,
            spread_bps=spread_bps,
            obi_5=obi_5,
            obi_20=obi_20,
            obi_50=obi_50,
            obi_persistence_score=obi_persistence_score,
            microprice_velocity_bps=microprice_velocity,
            queue_pressure_bid=queue_pressure_bid,
            queue_pressure_ask=queue_pressure_ask,
            market_depth_imbalance=market_depth_imbalance,
        )
