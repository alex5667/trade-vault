# -*- coding: utf-8 -*-
from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class L3BucketStats:
    """Stats snapshot for a closed bucket."""

    # raw bucket trade volumes (taker aggressor proxy)
    taker_buy_qty: float = 0.0
    taker_sell_qty: float = 0.0

    # EMA rates (qty/sec)
    taker_buy_rate_ema: float = 0.0
    taker_sell_rate_ema: float = 0.0

    # Reconciliation cancellation rates (qty/sec)
    cancel_bid_rate_ema: float = 0.0
    cancel_ask_rate_ema: float = 0.0

    # Reconciliation additions (qty/sec) inside tracked top-K depth
    added_bid_qty: float = 0.0
    added_ask_qty: float = 0.0
    added_bid_rate_ema: float = 0.0
    added_ask_rate_ema: float = 0.0

    # Added liquidity (limit-add) proxy rates (qty/sec)
    # Computed from top-K reconciliation: added = max(0, end_total - expected_total)
    limit_add_bid_rate_ema: float = 0.0
    limit_add_ask_rate_ema: float = 0.0
    limit_add_total_rate_ema: float = 0.0
    limit_add_imb: float = 0.0

    # VPIN-like toxicity proxy (0..1) + robust z-score
    # tox_raw = |buy - sell| / (buy + sell + eps)
    vpin_tox_ema: float = 0.0
    vpin_tox_z: float = 0.0


class L3QueueEventsProxy:
    """
    L3-lite proxy for Binance/public feeds:
      - Treat trade prints as "Trade" queue events.
      - Aggregate taker-buy / taker-sell qty per bucket.
      - Maintain EMA of taker absorption speed (qty/sec).
      - Track limit-add proxy (liquidity replenishment) rates.
      - Compute VPIN-like toxicity proxy (cheap O(1) per bucket).

    side convention: +1 taker-buy, -1 taker-sell
    """

    __slots__ = (
        "bucket_ms",
        "alpha",
        "eps",
        # Cancel rate EMA
        "_cancel_rate_alpha",
        "cancel_bid_rate_ema",
        "cancel_ask_rate_ema",
        # Trade rate EMAs
        "_bucket_buy",
        "_bucket_sell",
        "_rate_buy_ema",
        "_rate_sell_ema",
        # Added liquidity (limit-add) EMA rates
        "_add_rate_alpha",
        "_add_bid_rate_ema",
        "_add_ask_rate_ema",
        "_rate_add_bid_ema",
        "_rate_add_ask_ema",
        # VPIN-like toxicity proxy (robust z-score via MAD)
        "_vpin_alpha",
        "_vpin_mean_alpha",
        "_vpin_mad_alpha",
        "_vpin_tox_ema",
        "_vpin_mean_ema",
        "_vpin_mad_ema",
        "_vpin_tox_z",
        "_vpin_var_ema",
        "_last_vpin_z",
        # Bucket tracking
        "_last_bucket_id",
        # L3-lite reconciliation state (top-K totals)
        "_l2_bid_total",
        "_l2_ask_total",
        "_bucket_bid_start_total",
        "_bucket_ask_start_total",
        # Debug/observability (last computed)
        "_last_pulled_bid_qty",
        "_last_pulled_ask_qty",
        "_last_added_bid_qty",
        "_last_added_ask_qty",
        "_last_exec_overflow_bid",
        "_last_exec_overflow_ask",
    ),

    def __init__(self, *, bucket_ms: int, alpha: float = 0.12, eps: float = 1e-9) -> None:
        self.bucket_ms = max(50, int(bucket_ms))
        self.alpha = max(0.01, min(0.5, float(alpha)))
        self.eps = max(1e-12, float(eps))

        # Cancel rate EMA alpha
        self._cancel_rate_alpha = max(0.01, min(0.9, float(os.getenv("L3_CANCEL_RATE_EMA_ALPHA", "0.15"))))

        # Added liquidity rate EMA alpha
        self._add_rate_alpha = max(0.01, min(0.9, float(os.getenv("L3_ADD_RATE_EMA_ALPHA", "0.12"))))

        # VPIN-like toxicity proxy knobs
        self._vpin_alpha = max(0.01, min(0.9, float(os.getenv("VPIN_TOX_EMA_ALPHA", "0.05"))))
        self._vpin_mean_alpha = max(0.01, min(0.9, float(os.getenv("VPIN_TOX_MEAN_ALPHA", "0.03"))))
        self._vpin_mad_alpha = max(0.01, min(0.9, float(os.getenv("VPIN_TOX_MAD_ALPHA", "0.03"))))

        # Trade state
        self._bucket_buy = 0.0
        self._bucket_sell = 0.0
        self._rate_buy_ema = 0.0
        self._rate_sell_ema = 0.0

        # Cancel rate state
        self.cancel_bid_rate_ema = 0.0
        self.cancel_ask_rate_ema = 0.0

        # Added liquidity state (from on_bucket_close reconciliation)
        self._rate_add_bid_ema = 0.0
        self._rate_add_ask_ema = 0.0
        # Limit-add EMA rates (computed in on_bucket_advance)
        self._add_bid_rate_ema = 0.0
        self._add_ask_rate_ema = 0.0

        # VPIN toxicity state (new, via EMA+MAD robust z)
        self._vpin_tox_ema = 0.0
        self._vpin_mean_ema = 0.0
        self._vpin_mad_ema = 0.0
        self._vpin_tox_z = 0.0
        # Legacy VPIN state (keep for backward compat)
        self._vpin_var_ema: float = 1e-6
        self._last_vpin_z: float = 0.0

        self._last_bucket_id: Optional[int] = None

        # L3-lite totals state
        self._l2_bid_total = 0.0
        self._l2_ask_total = 0.0
        self._bucket_bid_start_total: Optional[float] = None
        self._bucket_ask_start_total: Optional[float] = None

        # Debug/metrics cache
        self._last_pulled_bid_qty = 0.0
        self._last_pulled_ask_qty = 0.0
        self._last_added_bid_qty = 0.0
        self._last_added_ask_qty = 0.0
        self._last_exec_overflow_bid = 0.0
        self._last_exec_overflow_ask = 0.0

    def on_trade(self, *, side: int, qty: float) -> None:
        """Accumulate qty into the current bucket."""
        q = float(qty or 0.0)
        if q <= 0.0:
            return
        if side == 1:
            self._bucket_buy += q
        elif side == -1:
            self._bucket_sell += q

    def on_bucket_advance(self, *, bucket_id: int) -> Optional[L3BucketStats]:
        """
        Call this when time moves to a new bucket.
        Returns stats for the bucket that just closed.
        """
        b = int(bucket_id)
        if self._last_bucket_id is None:
            self._last_bucket_id = b
            return None

        if b == self._last_bucket_id:
            return None

        # --- close previous bucket: update cancel EMAs + cache reconciliation ---
        self.on_bucket_close(bucket_ms=self.bucket_ms)

        # close previous bucket (note: if gap>1, we still close once with current collected qty)
        sec = self.bucket_ms / 1000.0
        buy_rate = self._bucket_buy / max(self.eps, sec)
        sell_rate = self._bucket_sell / max(self.eps, sec)

        a = self.alpha
        self._rate_buy_ema = (1.0 - a) * self._rate_buy_ema + a * buy_rate
        self._rate_sell_ema = (1.0 - a) * self._rate_sell_ema + a * sell_rate

        # limit-add proxy EMA rates (qty/sec) from reconciliation added_{bid,ask}
        add_bid_rate = float(self._last_added_bid_qty or 0.0) / max(self.eps, sec)
        add_ask_rate = float(self._last_added_ask_qty or 0.0) / max(self.eps, sec)
        aa = self._add_rate_alpha
        self._add_bid_rate_ema = (1.0 - aa) * self._add_bid_rate_ema + aa * add_bid_rate
        self._add_ask_rate_ema = (1.0 - aa) * self._add_ask_rate_ema + aa * add_ask_rate

        add_total = self._add_bid_rate_ema + self._add_ask_rate_ema
        add_den = add_total + self.eps
        add_imb = (self._add_bid_rate_ema - self._add_ask_rate_ema) / add_den

        # VPIN-like toxicity: |buy-sell| / (buy+sell)
        tox_raw = abs(self._bucket_buy - self._bucket_sell) / (abs(self._bucket_buy) + abs(self._bucket_sell) + self.eps)
        # EMA of raw toxicity (smoother)
        va = self._vpin_alpha
        self._vpin_tox_ema = (1.0 - va) * self._vpin_tox_ema + va * tox_raw
        # Robust-ish z: EMA mean + EMA abs-dev as MAD proxy
        vma = self._vpin_mean_alpha
        self._vpin_mean_ema = (1.0 - vma) * self._vpin_mean_ema + vma * tox_raw
        dev = abs(tox_raw - self._vpin_mean_ema)
        vda = self._vpin_mad_alpha
        self._vpin_mad_ema = (1.0 - vda) * self._vpin_mad_ema + vda * dev
        self._vpin_tox_z = (tox_raw - self._vpin_mean_ema) / max(self._vpin_mad_ema, 1e-6)

        out = L3BucketStats(
            taker_buy_qty=self._bucket_buy,
            taker_sell_qty=self._bucket_sell,
            taker_buy_rate_ema=self._rate_buy_ema,
            taker_sell_rate_ema=self._rate_sell_ema,
            cancel_bid_rate_ema=self.cancel_bid_rate_ema,
            cancel_ask_rate_ema=self.cancel_ask_rate_ema,
            added_bid_qty=float(self._last_added_bid_qty),
            added_ask_qty=float(self._last_added_ask_qty),
            added_bid_rate_ema=float(self._rate_add_bid_ema),
            added_ask_rate_ema=float(self._rate_add_ask_ema),
            limit_add_bid_rate_ema=self._add_bid_rate_ema,
            limit_add_ask_rate_ema=self._add_ask_rate_ema,
            limit_add_total_rate_ema=float(add_total),
            limit_add_imb=float(add_imb),
            vpin_tox_ema=float(self._vpin_tox_ema),
            vpin_tox_z=float(self._vpin_tox_z),
        ),

        # reset for new bucket
        self._bucket_buy = 0.0
        self._bucket_sell = 0.0
        self._last_bucket_id = b

        # New bucket start totals = last known L2 totals (so reconciliation is stable)
        self._bucket_bid_start_total = float(self._l2_bid_total or 0.0)
        self._bucket_ask_start_total = float(self._l2_ask_total or 0.0)

        # Note: if there was a gap > 1, intermediate buckets will have 0 trades
        # but we already updated totals for the next real bucket.

        return out

    def _ema(self, prev: float, x: float, alpha: float) -> float:
        if not math.isfinite(x):
            return prev
        if prev <= 0.0 or not math.isfinite(prev):
            return x
        return prev + alpha * (x - prev)

    def on_l2_totals(self, *, bid_total: float, ask_total: float) -> None:
        """
        Feed proxy with current L2 totals (qty) for tracked depth (top-K levels).
        Call this on each book update (or periodically).
        """
        bt = float(bid_total or 0.0)
        at = float(ask_total or 0.0)
        if bt < 0.0:
            bt = 0.0
        if at < 0.0:
            at = 0.0
        self._l2_bid_total = bt
        self._l2_ask_total = at

        # Initialize bucket start totals lazily if they were reset or never set.
        if self._bucket_bid_start_total is None:
            self._bucket_bid_start_total = bt
        if self._bucket_ask_start_total is None:
            self._bucket_ask_start_total = at

    def on_l2_levels(
        self,
        *,
        bids: list[list[float]] | list[tuple[float, float]],
        asks: list[list[float]] | list[tuple[float, float]],
    ) -> None:
        """
        Optional helper: accept top-K levels and sum totals internally.
        bids/asks: list of [price, qty] or (price, qty) for tracked levels.
        """
        bid_total = 0.0
        ask_total = 0.0
        for item in bids or []:
            try:
                q = float(item[1])
                if q > 0.0 and math.isfinite(q):
                    bid_total += q
            except (IndexError, TypeError, ValueError):
                continue
        for item in asks or []:
            try:
                q = float(item[1])
                if q > 0.0 and math.isfinite(q):
                    ask_total += q
            except (IndexError, TypeError, ValueError):
                continue
        self.on_l2_totals(bid_total=bid_total, ask_total=ask_total)

    def _compute_pulled_added(self) -> tuple[float, float, float, float, float, float]:
        """
        Reconcile bucket:
        - start totals: _bucket_*_start_total
        - end totals: _l2_*_total
        - exec proxy: taker_sell hits bids, taker_buy hits asks
        Returns:
          pulled_bid, pulled_ask, added_bid, added_ask, exec_overflow_bid, exec_overflow_ask
        """
        # If we never received L2, treat totals as 0 (and keep starts None).
        sb = float(self._bucket_bid_start_total or 0.0)
        sa = float(self._bucket_ask_start_total or 0.0)
        eb = float(self._l2_bid_total or 0.0)
        ea = float(self._l2_ask_total or 0.0)

        # Exec proxy per side:
        # taker SELL consumes bids (side -1), taker BUY consumes asks (side 1)
        exec_bid = float(self._bucket_sell or 0.0)
        exec_ask = float(self._bucket_buy or 0.0)

        # How much execution we expected to see WITHIN the top-K depth
        exec_in_depth_bid = min(exec_bid, sb)
        exec_in_depth_ask = min(exec_ask, sa)

        # How much execution went "beyond" our tracked top-K depth
        exec_overflow_bid = max(0.0, exec_bid - sb)
        exec_overflow_ask = max(0.0, exec_ask - sa)

        # What we expected to remain in L2 depth after execution
        expected_bid = max(0.0, sb - exec_in_depth_bid)
        expected_ask = max(0.0, sa - exec_in_depth_ask)

        # pulled: what disappeared without execution
        pulled_bid = max(0.0, expected_bid - eb)
        pulled_ask = max(0.0, expected_ask - ea)

        # added: what was added (stacking) beyond expectation
        added_bid = max(0.0, eb - expected_bid)
        added_ask = max(0.0, ea - expected_ask)

        return pulled_bid, pulled_ask, added_bid, added_ask, exec_overflow_bid, exec_overflow_ask

    def on_bucket_close(
        self,
        *,
        pulled_bid_qty_proxy: Optional[float] = None,
        pulled_ask_qty_proxy: Optional[float] = None,
        bucket_ms: int,
    ) -> None:
        """
        Обновляет EMA cancel_rate (qty/sec) на основании pulled liquidity за бакет.
        """
        # Преобразуем длительность бакета в секунды для получения рейта qty/sec
        bucket_sec = max(float(bucket_ms) / 1000.0, 1e-6)

        # If caller didn't provide pulled proxies, compute internally (L3-lite totals reconciliation).
        if pulled_bid_qty_proxy is None or pulled_ask_qty_proxy is None:
            pb, pa, ab, aa, ex_b, ex_a = self._compute_pulled_added()
            self._last_pulled_bid_qty = pb
            self._last_pulled_ask_qty = pa
            self._last_added_bid_qty = ab
            self._last_added_ask_qty = aa
            self._last_exec_overflow_bid = ex_b
            self._last_exec_overflow_ask = ex_a

            pulled_bid_qty_proxy = pb
            pulled_ask_qty_proxy = pa

        # Вычисляем мгновенные рейты отмен (proxy metrics)
        bid_rate = float(pulled_bid_qty_proxy or 0.0) / bucket_sec
        ask_rate = float(pulled_ask_qty_proxy or 0.0) / bucket_sec

        # Reconciliation additions rates (qty/sec) within tracked top-K depth
        add_bid_rate = float(self._last_added_bid_qty or 0.0) / bucket_sec
        add_ask_rate = float(self._last_added_ask_qty or 0.0) / bucket_sec

        a = self._cancel_rate_alpha
        self.cancel_bid_rate_ema = self._ema(self.cancel_bid_rate_ema, bid_rate, a)
        self.cancel_ask_rate_ema = self._ema(self.cancel_ask_rate_ema, ask_rate, a)

        a_add = self._add_rate_alpha
        self._rate_add_bid_ema = self._ema(self._rate_add_bid_ema, add_bid_rate, a_add)
        self._rate_add_ask_ema = self._ema(self._rate_add_ask_ema, add_ask_rate, a_add)

    def snapshot(self) -> Dict[str, float]:
        """Non-essential: quick current values (for observability)."""
        return {
            "taker_buy_qty_bucket": float(self._bucket_buy),
            "taker_sell_qty_bucket": float(self._bucket_sell),
            "taker_buy_rate_ema": float(self._rate_buy_ema),
            "taker_sell_rate_ema": float(self._rate_sell_ema),
            "cancel_bid_rate_ema": float(self.cancel_bid_rate_ema),
            "cancel_ask_rate_ema": float(self.cancel_ask_rate_ema),
            "added_bid_rate_ema": float(self._rate_add_bid_ema),
            "added_ask_rate_ema": float(self._rate_add_ask_ema),
            "limit_add_total_rate_ema": float(self._add_bid_rate_ema + self._add_ask_rate_ema),
            "limit_add_imb": float(
                (self._add_bid_rate_ema - self._add_ask_rate_ema)
                / (self._add_bid_rate_ema + self._add_ask_rate_ema + self.eps)
            ),
            "vpin_tox_ema": float(self._vpin_tox_ema),
            "vpin_tox_z": float(self._vpin_tox_z),
        }
