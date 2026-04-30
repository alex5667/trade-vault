# services/touch_level_tracker.py
from __future__ import annotations

from dataclasses import dataclass
from collections import deque
from typing import Deque, Tuple, Optional


@dataclass
class TouchSnapshot:
    ts: int
    bid_tag: str = "none"
    ask_tag: str = "none"
    bid_rho: float = 0.0
    ask_rho: float = 0.0
    bid_traded_w: float = 0.0
    ask_traded_w: float = 0.0
    bid_drop_w: float = 0.0
    ask_drop_w: float = 0.0
    bid_refill_lag_ms: int = 0
    ask_refill_lag_ms: int = 0
    bid_best_price: float = 0.0
    ask_best_price: float = 0.0
    bid_best_qty: float = 0.0
    ask_best_qty: float = 0.0
    is_stale: bool = True


class _RollingQtyWindow:
    def __init__(self, window_ms: int):
        self.window_ms = max(1, int(window_ms))
        self.q: Deque[Tuple[int, float]] = deque()
        self.sum = 0.0

    def add(self, ts: int, qty: float) -> None:
        ts = int(ts)
        qty = float(qty or 0.0)
        if ts <= 0 or qty <= 0:
            return
        self.q.append((ts, qty))
        self.sum += qty
        self.prune(ts)

    def prune(self, now_ts: int) -> None:
        now_ts = int(now_ts)
        cutoff = now_ts - self.window_ms
        while self.q and self.q[0][0] < cutoff:
            _, old = self.q.popleft()
            self.sum -= old

    def value(self, now_ts: int) -> float:
        self.prune(now_ts)
        return float(self.sum)


@dataclass
class _SideState:
    best_price: float = 0.0
    best_qty: float = 0.0
    last_ts: int = 0

    trades: Optional[_RollingQtyWindow] = None
    drops: Optional[_RollingQtyWindow] = None

    drop_started_ts: int = 0
    qty_before_drop: float = 0.0
    refill_lag_ms: int = 0

    def reset_level(self, price: float, qty: float, ts: int) -> None:
        self.best_price = float(price or 0.0)
        self.best_qty = float(qty or 0.0)
        self.last_ts = int(ts or 0)

        self.drop_started_ts = 0
        self.qty_before_drop = 0.0
        self.refill_lag_ms = 0


class TouchLevelTracker:
    """
    Touch-level depletion/refill (тонкий):
    - хранит только best_price/best_qty для bid/ask
    - считает Traded@touch по окну W
    - считает Drop@touch по окну W (включая ухудшение best-цены)
    - v2: trade match по band (0..N тиков от best), а не строго по best
    """

    def __init__(
        self
        window_ms: int = 500
        tau_refill_ms: int = 250
        recover_frac: float = 0.90
        rho_refill_min: float = 1.5
        rho_depletion_max: float = 1.5
        *
        tick_size: float = 0.0
        max_touch_ticks: int = 1
        book_fresh_ms: int = 250
    ):
        self.window_ms = max(1, int(window_ms))
        self.tau_refill_ms = max(0, int(tau_refill_ms))
        self.recover_frac = float(recover_frac)
        self.rho_refill_min = float(rho_refill_min)
        self.rho_depletion_max = float(rho_depletion_max)

        self.tick_size = float(tick_size or 0.0)
        self.max_touch_ticks = max(0, int(max_touch_ticks))
        self.book_fresh_ms = max(1, int(book_fresh_ms))

        self.bid = _SideState(trades=_RollingQtyWindow(self.window_ms), drops=_RollingQtyWindow(self.window_ms))
        self.ask = _SideState(trades=_RollingQtyWindow(self.window_ms), drops=_RollingQtyWindow(self.window_ms))

    def _eq_price(self, a: float, b: float) -> bool:
        if self.tick_size > 0:
            return abs(a - b) <= self.tick_size * 0.5
        return abs(a - b) <= 1e-9

    def _is_touch_trade(self, *, now_ts: int, price: float, side: int) -> bool:
        price = float(price or 0.0)
        if price <= 0:
            return False

        if side > 0:
            if self.ask.best_price <= 0:
                return False
            if (now_ts - int(self.ask.last_ts or 0)) > self.book_fresh_ms:
                return False
            best = float(self.ask.best_price)
            if self.tick_size > 0 and self.max_touch_ticks > 0:
                hi = best + self.tick_size * self.max_touch_ticks
                return (price >= best - self.tick_size * 0.501) and (price <= hi + self.tick_size * 0.501)
            return self._eq_price(price, best)

        if side < 0:
            if self.bid.best_price <= 0:
                return False
            if (now_ts - int(self.bid.last_ts or 0)) > self.book_fresh_ms:
                return False
            best = float(self.bid.best_price)
            if self.tick_size > 0 and self.max_touch_ticks > 0:
                lo = best - self.tick_size * self.max_touch_ticks
                return (price <= best + self.tick_size * 0.501) and (price >= lo - self.tick_size * 0.501)
            return self._eq_price(price, best)

        return False

    def on_trade(self, *, ts: int, price: float, qty: float, side: int) -> None:
        ts = int(ts)
        price = float(price or 0.0)
        qty = float(qty or 0.0)
        if ts <= 0 or price <= 0 or qty <= 0:
            return

        if side > 0:
            if self._is_touch_trade(now_ts=ts, price=price, side=side):
                self.ask.trades.add(ts, qty)
        elif side < 0:
            if self._is_touch_trade(now_ts=ts, price=price, side=side):
                self.bid.trades.add(ts, qty)

    def _update_side(self, st: _SideState, *, ts: int, best_p: float, best_q: float, is_bid: bool) -> None:
        best_p = float(best_p or 0.0)
        best_q = float(best_q or 0.0)

        if best_p <= 0 or ts <= 0:
            return

        if st.best_price <= 0:
            st.reset_level(best_p, best_q, ts)
            return

        # price changed
        if not self._eq_price(best_p, st.best_price):
            # WORSE move => previous level disappears => synthetic drop(prev_best_qty)
            if is_bid:
                # bid worsens when price goes DOWN
                if best_p < st.best_price and st.best_qty > 0:
                    st.drops.add(ts, float(st.best_qty))
                    if st.drop_started_ts == 0:
                        st.drop_started_ts = ts
                        st.qty_before_drop = float(st.best_qty)
                        st.refill_lag_ms = 0
            else:
                # ask worsens when price goes UP
                if best_p > st.best_price and st.best_qty > 0:
                    st.drops.add(ts, float(st.best_qty))
                    if st.drop_started_ts == 0:
                        st.drop_started_ts = ts
                        st.qty_before_drop = float(st.best_qty)
                        st.refill_lag_ms = 0

            st.best_price = best_p
            st.best_qty = best_q
            st.last_ts = ts
            return

        # same price => compute drop
        drop = max(0.0, st.best_qty - best_q)
        if drop > 0.0:
            st.drops.add(ts, drop)
            if st.drop_started_ts == 0:
                st.drop_started_ts = ts
                st.qty_before_drop = max(st.best_qty, best_q)
                st.refill_lag_ms = 0

        # refill lag: after drop started, detect recover
        if st.drop_started_ts > 0 and st.qty_before_drop > 0:
            target = st.qty_before_drop * max(0.0, min(1.0, self.recover_frac))
            if best_q >= target and st.refill_lag_ms == 0:
                st.refill_lag_ms = int(ts - st.drop_started_ts)

        st.best_qty = best_q
        st.last_ts = ts

    def on_book(self, *, ts: int, bid_p: float, bid_q: float, ask_p: float, ask_q: float) -> None:
        ts = int(ts)
        if ts <= 0:
            return
        self._update_side(self.bid, ts=ts, best_p=bid_p, best_q=bid_q, is_bid=True)
        self._update_side(self.ask, ts=ts, best_p=ask_p, best_q=ask_q, is_bid=False)

    def _tag(self, *, T: float, D: float, refill_lag_ms: int) -> Tuple[str, float]:
        eps = 1e-9
        rho = float(T) / (float(D) + eps)

        if T <= 0 and D > 0:
            return "cancel", rho

        if T > 0:
            # refill if "too much traded / too little drop" OR refill almost immediate
            if rho >= self.rho_refill_min or (refill_lag_ms > 0 and refill_lag_ms <= self.tau_refill_ms):
                return "refill", rho

            # depletion needs some visible drop + not instant refill
            if D > 0 and rho <= self.rho_depletion_max and (refill_lag_ms == 0 or refill_lag_ms > self.tau_refill_ms):
                return "depletion", rho

        return "none", rho

    def snapshot(self, *, ts: int) -> TouchSnapshot:
        ts = int(ts)
        if ts <= 0:
            return TouchSnapshot(ts=ts)

        bid_T = self.bid.trades.value(ts)
        ask_T = self.ask.trades.value(ts)
        bid_D = self.bid.drops.value(ts)
        ask_D = self.ask.drops.value(ts)

        bid_tag, bid_rho = self._tag(T=bid_T, D=bid_D, refill_lag_ms=int(self.bid.refill_lag_ms or 0))
        ask_tag, ask_rho = self._tag(T=ask_T, D=ask_D, refill_lag_ms=int(self.ask.refill_lag_ms or 0))

        is_stale = True
        if self.bid.last_ts and self.ask.last_ts:
            is_stale = (ts - min(self.bid.last_ts, self.ask.last_ts)) > self.book_fresh_ms

        return TouchSnapshot(
            ts=ts
            bid_tag=bid_tag
            ask_tag=ask_tag
            bid_rho=float(bid_rho)
            ask_rho=float(ask_rho)
            bid_traded_w=float(bid_T)
            ask_traded_w=float(ask_T)
            bid_drop_w=float(bid_D)
            ask_drop_w=float(ask_D)
            bid_refill_lag_ms=int(self.bid.refill_lag_ms or 0)
            ask_refill_lag_ms=int(self.ask.refill_lag_ms or 0)
            bid_best_price=float(self.bid.best_price)
            ask_best_price=float(self.ask.best_price)
            bid_best_qty=float(self.bid.best_qty)
            ask_best_qty=float(self.ask.best_qty)
            is_stale=bool(is_stale)
        )
