# services/l3_lite_tracker.py
from __future__ import annotations

from dataclasses import dataclass
import contextlib


def _ema(prev: float, x: float, a: float) -> float:
    return (1.0 - a) * prev + a * x


@dataclass(slots=True)
class L3LiteSnapshot:
    taker_buy_rate_ema: float = 0.0   # qty/sec (taker-buy -> hits ask)
    taker_sell_rate_ema: float = 0.0  # qty/sec (taker-sell -> hits bid)

    # Signed taker flow imbalance (buy vs sell), +1 = aggressive buy dominates
    taker_flow_imb: float = 0.0           # (buy - sell) / (buy + sell)
    taker_flow_imb_mean_ema: float = 0.0  # EMA mean of taker_flow_imb
    taker_flow_imb_mad_ema: float = 0.0   # EMA(|imb-mean|) as robust scale proxy
    taker_flow_imb_z: float = 0.0         # (imb - mean) / mad_ema

    cancel_bid_rate_ema: float = 0.0  # qty/sec
    cancel_ask_rate_ema: float = 0.0  # qty/sec

    cancel_to_trade_bid: float = 0.0  # cancel_bid_rate / taker_sell_rate
    cancel_to_trade_ask: float = 0.0  # cancel_ask_rate / taker_buy_rate

    eta_fill_bid_sec: float = 0.0     # depth_bid_5 / taker_sell_rate
    eta_fill_ask_sec: float = 0.0     # depth_ask_5 / taker_buy_rate


class L3LiteTracker:
    """
    L3-lite поверх Binance L2+trades:
    - копим taker-buy/taker-sell qty между book-снапшотами
    - на book-снапшоте:
        * считаем trade rates (qty/sec)
        * декомпозируем уменьшение depth_5 на "trades" и "cancels" (остаток)
        * считаем cancel-to-trade и ETA фила
    """

    __slots__ = (
        "alpha",
        "eps",
        "min_dt_ms",
        "enabled",

        "_last_book_ts",
        "_prev_depth_bid_5",
        "_prev_depth_ask_5",

        "_acc_buy_qty",
        "_acc_sell_qty",

        "snap",
    )

    def __init__(
        self,
        *,
        alpha: float = 0.08,
        eps: float = 1e-9,
        min_dt_ms: int = 80,
        enabled: bool = True,
    ):
        self.alpha = max(0.01, min(0.5, float(alpha)))
        self.eps = float(eps)
        self.min_dt_ms = max(10, int(min_dt_ms))
        self.enabled = bool(enabled)

        self._last_book_ts: int = 0
        self._prev_depth_bid_5: float = 0.0
        self._prev_depth_ask_5: float = 0.0

        self._acc_buy_qty: float = 0.0
        self._acc_sell_qty: float = 0.0

        self.snap = L3LiteSnapshot()

    def on_trade(self, *, ts: int, qty: float, side: int) -> None:
        """
        side: +1 taker-buy, -1 taker-sell
        """
        if not self.enabled:
            return
        if ts <= 0:
            return
        q = float(qty or 0.0)
        if q <= 0.0:
            return
        if side == 1:
            self._acc_buy_qty += q
        elif side == -1:
            self._acc_sell_qty += q

    def on_book(self, *, ts: int, depth_bid_5: float, depth_ask_5: float) -> None:
        if not self.enabled:
            return
        if ts <= 0:
            return

        bid5 = float(depth_bid_5 or 0.0)
        ask5 = float(depth_ask_5 or 0.0)

        # first snap: just initialize
        if self._last_book_ts <= 0:
            self._last_book_ts = ts
            self._prev_depth_bid_5 = bid5
            self._prev_depth_ask_5 = ask5
            self._acc_buy_qty = 0.0
            self._acc_sell_qty = 0.0
            self._recalc_eta(bid5, ask5)
            return

        dt_ms = ts - self._last_book_ts
        if dt_ms < self.min_dt_ms:
            # слишком частые дифы книги: не апдейтим статистику, только depth/ETA
            self._prev_depth_bid_5 = bid5
            self._prev_depth_ask_5 = ask5
            self._last_book_ts = ts
            self._recalc_eta(bid5, ask5)
            return

        dt = dt_ms / 1000.0

        # trade rate (qty/sec) between book updates
        buy_rate = self._acc_buy_qty / dt
        sell_rate = self._acc_sell_qty / dt

        a = self.alpha
        self.snap.taker_buy_rate_ema = _ema(self.snap.taker_buy_rate_ema, buy_rate, a)
        self.snap.taker_sell_rate_ema = _ema(self.snap.taker_sell_rate_ema, sell_rate, a)

        # Signed taker flow imbalance (buy vs sell) computed on the same dt window.
        # Deterministic & low-latency: robust-ish z via EMA mean + EMA abs-dev (MAD proxy).
        den = abs(self.snap.taker_buy_rate_ema) + abs(self.snap.taker_sell_rate_ema) + self.eps
        imb = (self.snap.taker_buy_rate_ema - self.snap.taker_sell_rate_ema) / den
        self.snap.taker_flow_imb = float(imb)
        self.snap.taker_flow_imb_mean_ema = _ema(self.snap.taker_flow_imb_mean_ema, imb, a)
        dev = abs(imb - self.snap.taker_flow_imb_mean_ema)
        self.snap.taker_flow_imb_mad_ema = _ema(self.snap.taker_flow_imb_mad_ema, dev, a)
        z_den = max(self.snap.taker_flow_imb_mad_ema, 1e-6)
        self.snap.taker_flow_imb_z = float((imb - self.snap.taker_flow_imb_mean_ema) / z_den)

        # depth deltas
        prev_bid5 = self._prev_depth_bid_5
        prev_ask5 = self._prev_depth_ask_5

        d_bid = bid5 - prev_bid5  # <0 => top5 depth decreased
        d_ask = ask5 - prev_ask5

        # "executed" proxy over interval:
        # taker-sell hits bid; taker-buy hits ask
        exec_on_bid = self._acc_sell_qty
        exec_on_ask = self._acc_buy_qty

        # cancellations are the part of depth-outflow not explained by executions
        out_bid = max(0.0, -d_bid)
        out_ask = max(0.0, -d_ask)

        canc_bid_qty = max(0.0, out_bid - exec_on_bid)
        canc_ask_qty = max(0.0, out_ask - exec_on_ask)

        canc_bid_rate = canc_bid_qty / dt
        canc_ask_rate = canc_ask_qty / dt

        self.snap.cancel_bid_rate_ema = _ema(self.snap.cancel_bid_rate_ema, canc_bid_rate, a)
        self.snap.cancel_ask_rate_ema = _ema(self.snap.cancel_ask_rate_ema, canc_ask_rate, a)

        # cancel-to-trade ratios
        self.snap.cancel_to_trade_bid = self.snap.cancel_bid_rate_ema / max(self.snap.taker_sell_rate_ema, self.eps)
        self.snap.cancel_to_trade_ask = self.snap.cancel_ask_rate_ema / max(self.snap.taker_buy_rate_ema, self.eps)

        # ETA fill (sec)
        self._recalc_eta(bid5, ask5)

        # reset accumulators for next interval
        self._acc_buy_qty = 0.0
        self._acc_sell_qty = 0.0
        self._prev_depth_bid_5 = bid5
        self._prev_depth_ask_5 = ask5
        self._last_book_ts = ts

    def _recalc_eta(self, bid5: float, ask5: float) -> None:
        # bid fill depends on taker-sell rate; ask fill depends on taker-buy rate
        sell_rate = max(self.snap.taker_sell_rate_ema, self.eps)
        buy_rate = max(self.snap.taker_buy_rate_ema, self.eps)

        self.snap.eta_fill_bid_sec = float(bid5) / sell_rate if bid5 > 0 else 0.0
        self.snap.eta_fill_ask_sec = float(ask5) / buy_rate if ask5 > 0 else 0.0

    def attach_to_context(self, ctx: object) -> None:
        """
        Мягко (через setattr): не ломает код, даже если ctx не имеет полей.
        """
        s = self.snap
        for k, v in (
            ("taker_buy_rate_ema", s.taker_buy_rate_ema),
            ("taker_sell_rate_ema", s.taker_sell_rate_ema),
            ("taker_flow_imb", s.taker_flow_imb),
            ("taker_flow_imb_mean_ema", s.taker_flow_imb_mean_ema),
            ("taker_flow_imb_mad_ema", s.taker_flow_imb_mad_ema),
            ("taker_flow_imb_z", s.taker_flow_imb_z),
            ("cancel_bid_rate_ema", s.cancel_bid_rate_ema),
            ("cancel_ask_rate_ema", s.cancel_ask_rate_ema),
            ("cancel_to_trade_bid", s.cancel_to_trade_bid),
            ("cancel_to_trade_ask", s.cancel_to_trade_ask),
            ("eta_fill_bid_sec", s.eta_fill_bid_sec),
            ("eta_fill_ask_sec", s.eta_fill_ask_sec),
        ):
            with contextlib.suppress(Exception):
                setattr(ctx, k, float(v))

