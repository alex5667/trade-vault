from __future__ import annotations

from ..models.data_models import _PendingMid


class RealizedSpreadTracker:
    """
    Very lightweight "post-factum vs mid" tracker:

    When a trade tick arrives:
      store (ts, mid_at_trade, side).

    When time advances beyond horizon_ms:
      realized_bps = side * (mid_now - mid_at_trade) / mid_at_trade * 10_000

    Maintains:
      - last_realized_bps
      - realized_ema_bps
      - adverse_ratio_ema : EMA of (realized_bps < 0)
      - spread_bps computed from bid/ask
    """

    __slots__ = (
        "horizon_ms",
        "alpha",
        "max_pending",
        "pending",
        "_head",
        "last_realized_bps",
        "realized_ema_bps",
        "adverse_ratio_ema",
        "dropped_pending",
        "settled",
    )

    def __init__(self, horizon_ms: int = 2000, alpha: float = 0.08, max_pending: int = 5000):
        self.horizon_ms = max(50, int(horizon_ms))
        self.alpha = max(0.01, min(0.5, float(alpha)))
        self.max_pending = max(100, int(max_pending))

        self.pending: list[_PendingMid] = []
        self._head = 0

        self.last_realized_bps: float = 0.0
        self.realized_ema_bps: float = 0.0
        self.adverse_ratio_ema: float = 0.0

        self.dropped_pending: int = 0
        self.settled: int = 0

    def update(
        self,
        *,
        ts: int,
        bid: float,
        ask: float,
        is_trade: bool,
        side: int,
    ) -> tuple[float, float, float, float]:
        """
        Returns:
          spread_bps, last_realized_bps, realized_ema_bps, adverse_ratio_ema
        """
        if ts <= 0 or bid <= 0 or ask <= 0 or ask < bid:
            return 0.0, self.last_realized_bps, self.realized_ema_bps, self.adverse_ratio_ema

        mid_now = 0.5 * (bid + ask)
        if mid_now <= 0:
            return 0.0, self.last_realized_bps, self.realized_ema_bps, self.adverse_ratio_ema

        spread_bps = (ask - bid) / mid_now * 10_000.0

        # settle old pending mids
        cutoff = ts - self.horizon_ms
        head = self._head
        pend = self.pending

        while head < len(pend):
            p = pend[head]
            if p.ts > cutoff:
                break

            if p.mid_at_trade > 0:
                realized = p.side * (mid_now - p.mid_at_trade) / p.mid_at_trade * 10_000.0
                self.last_realized_bps = float(realized)

                a = self.alpha
                self.realized_ema_bps = (1.0 - a) * self.realized_ema_bps + a * self.last_realized_bps

                adverse = 1.0 if realized < 0.0 else 0.0
                self.adverse_ratio_ema = (1.0 - a) * self.adverse_ratio_ema + a * adverse

            self.settled += 1
            head += 1

        self._head = head

        # append new trade
        if is_trade and side in (-1, 1):
            # bounded pending without O(n) popleft
            if (len(self.pending) - self._head) >= self.max_pending:
                self._head += 1
                self.dropped_pending += 1
            self.pending.append(_PendingMid(ts=ts, mid_at_trade=mid_now, side=side))

        # compaction occasionally
        if self._head > 2000 and self._head > (len(self.pending) // 2):
            self.pending = self.pending[self._head :]
            self._head = 0

        return float(spread_bps), self.last_realized_bps, self.realized_ema_bps, self.adverse_ratio_ema
