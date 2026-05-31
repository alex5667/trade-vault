from __future__ import annotations

import math
import os
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


def _clip01(x: float) -> float:
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    return float(x)


def _isfinite(x: float) -> bool:
    return not (math.isnan(x) or math.isinf(x))


def _ema(prev: float | None, x: float, alpha: float) -> float:
    """
    Стандартная EMA:
      ema := alpha*x + (1-alpha)*prev
    Если prev=None -> ema=x (детерминированный старт).
    """
    a = _clip01(alpha)
    if prev is None:
        return float(x)
    return float(a * x + (1.0 - a) * prev)


@dataclass
class PendingTrade:
    ts_ms: int
    side: int
    price: float
    qty: float


@dataclass(slots=True)
class PendingTradeHorizon:
    ts_ms: int
    entry_mid: float
    side: int         # +1 buy, -1 sell
    volume: float


class RealizedSpreadTracker:
    def __init__(
        self,
        max_pending: int = 20000,
        *,
        pending_pause_high: float = float(os.getenv("RS_PENDING_PAUSE_HIGH", "0.90")),
        pending_resume_low: float = float(os.getenv("RS_PENDING_RESUME_LOW", "0.80")),
        metrics: Any | None = None,
    ) -> None:
        self.max_pending = int(max_pending)
        self.pending_pause_high = float(pending_pause_high)
        self.pending_resume_low = float(pending_resume_low)
        self._pending_paused: bool = False
        self._pending_dropped: int = 0

        # metrics are optional; we use duck-typing and safe fallbacks
        self._m_inc: Callable[[str, int], None] = getattr(metrics, "inc", lambda _k, _v=1: None)
        self._m_gauge: Callable[[str, float], None] = getattr(metrics, "gauge", lambda _k, _v: None)

        self.pending: list[PendingTrade] = []
        self.pending_head: int = 0

        self._last_mid: float | None = None
        self._last_ts_ms: int = 0


@dataclass(slots=True)
class RealizedSpreadSnapshot:
    realized_bps_ema: float | None
    n_realized: int
    pending_len: int
    dropped_due_to_cap: int


class RealizedSpreadTrackerHorizon:
    """
    ### 6.1: RealizedSpreadTracker.update — детерминизм, pending compaction, max_pending

    Этот трекер intentionally "маленький":
      - хранит pending трейды (entry_mid, side, ts)
      - когда появляется новый mid и прошло horizon_ms -> реализуем spread (в bps)
      - EMA обновляется детерминированно
      - pending всегда ограничен max_pending (жёсткий лимит памяти)

    Если у вас уже есть RealizedSpreadTracker — можно механически перенести туда:
      - _drain_matured(...)
      - enforce max_pending + counters
    и подключить эти тесты.
    """

    def __init__(
        self,
        *,
        horizon_ms: int = 20_000,
        ema_alpha: float = 0.05,
        max_pending: int = 10_000,
    ) -> None:
        self.horizon_ms = int(max(1, horizon_ms))
        self.ema_alpha = float(ema_alpha)
        self.max_pending = int(max(1, max_pending))

        self.pending: deque[PendingTradeHorizon] = deque()
        self.realized_bps_ema: float | None = None
        self.n_realized: int = 0
        self.dropped_due_to_cap: int = 0

    def _isfinite(self, x: float) -> bool:
        return not (math.isnan(x) or math.isinf(x))

    def _mid(self, bid: float, ask: float) -> float:
        return float((bid + ask) / 2.0)

    def _enforce_cap(self) -> None:
        """
        Жёсткий лимит памяти: если pending переполнен — выкидываем самый старый.
        Это лучше, чем дать latency/ram распухнуть.
        """
        while len(self.pending) > self.max_pending:
            self.pending.popleft()
            self.dropped_due_to_cap += 1

    def _drain_matured(self, *, now_ms: int, mid_now: float) -> None:
        """
        Реализуем (compute realized spread) для всех pending, у которых now_ms - ts_ms >= horizon_ms.
        realized_bps = side * (mid_now - entry_mid) / entry_mid * 10_000

        Примечание:
          Это упрощённая модель "реализованного сдвига мида".
          Вам важно тут именно:
            - детерминизм
            - bounded pending
            - корректная EMA
        """
        if not self.pending:
            return
        # drain по времени (pending упорядочен по ts)
        while self.pending:
            p = self.pending[0]
            if (now_ms - int(p.ts_ms)) < self.horizon_ms:
                break
            self.pending.popleft()

            if p.entry_mid <= 0 or not self._isfinite(p.entry_mid) or not self._isfinite(mid_now):
                continue
            # signed move
            realized_bps = float(p.side) * (float(mid_now) - float(p.entry_mid)) / float(p.entry_mid) * 10_000.0
            if not self._isfinite(realized_bps):
                continue
            self.realized_bps_ema = _ema(self.realized_bps_ema, realized_bps, self.ema_alpha)
            self.n_realized += 1

    def update(
        self,
        *,
        ts_ms: int,
        bid: float,
        ask: float,
        trade_side: int,     # +1 buy, -1 sell, 0 => no trade
        trade_volume: float, # >=0
    ) -> RealizedSpreadSnapshot:
        """
        Обновление состояния.
        Детерминизм:
          - входной ts_ms обязателен
          - mid вычисляется однозначно из bid/ask
        """
        now_ms = int(ts_ms)
        mid_now = self._mid(float(bid), float(ask))

        # 1) Реализуем старые pending на текущем mid
        self._drain_matured(now_ms=now_ms, mid_now=mid_now)

        # 2) Если пришла сделка — добавим pending
        if int(trade_side) in (+1, -1):
            v = float(trade_volume or 0.0)
            if v > 0 and self._isfinite(v) and self._isfinite(mid_now) and mid_now > 0:
                self.pending.append(PendingTradeHorizon(ts_ms=now_ms, entry_mid=mid_now, side=int(trade_side), volume=v))
                self._enforce_cap()

        return RealizedSpreadSnapshot(
            realized_bps_ema=self.realized_bps_ema,
            n_realized=int(self.n_realized),
            pending_len=int(len(self.pending)),
            dropped_due_to_cap=int(self.dropped_due_to_cap),
        )

    def pending_active(self) -> int:
        n = len(self.pending) - int(self.pending_head)
        return int(n) if n > 0 else 0

    def pending_utilization(self) -> float:
        """
        Utilization definition requested:
          pending_utilization = (len(pending) - head) / max_pending
        """
        mp = max(int(self.max_pending), 1)
        return float(self.pending_active()) / float(mp)

    def _emit_pending_metrics(self) -> None:
        try:
            self._m_gauge("realized_spread.pending_utilization", float(self.pending_utilization()))
            self._m_gauge("realized_spread.pending_active", float(self.pending_active()))
            self._m_gauge("realized_spread.pending_paused", 1.0 if self._pending_paused else 0.0)
        except Exception:
            # never break pipeline on metrics
            pass

    def _should_accept_new_pending(self, projected_active: int) -> bool:
        mp = max(int(self.max_pending), 1)
        proj_util = float(projected_active) / float(mp)

        # hysteresis: once paused, require lower threshold to resume
        if self._pending_paused:
            if proj_util <= float(self.pending_resume_low):
                self._pending_paused = False
                self._m_inc("realized_spread.pending_resume", 1)
                return True
            return False

        # not paused yet -> pause if projected goes above high watermark
        if proj_util > float(self.pending_pause_high):
            self._pending_paused = True
            self._m_inc("realized_spread.pending_pause", 1)
            return False

        return True

    def _maybe_compact_pending(self) -> None:
        """
        Keep list memory stable: when head advanced far enough, compact.
        This also helps utilization drop (active=len-head) converge.
        """
        h = int(self.pending_head)
        n = len(self.pending)
        if h <= 0 or n <= 0:
            return
        # compact if head consumed more than 50% or list too large
        if h >= (n // 2) or n >= max(int(self.max_pending) * 2, 10_000):
            self.pending = self.pending[h:]
            self.pending_head = 0
            self._m_inc("realized_spread.pending_compact", 1)

    def mark_pending_consumed(self, n: int) -> None:
        """
        Advance head by n and compact opportunistically.
        Safe helper for engines that don't want to touch internal indices.
        """
        k = int(n)
        if k <= 0:
            return
        self.pending_head = min(int(self.pending_head) + k, len(self.pending))
        self._maybe_compact_pending()
        self._emit_pending_metrics()

    def reset(self) -> None:
        self.pending.clear()
        self.pending_head = 0
        self._pending_paused = False
        self._pending_dropped = 0
        self._last_mid = None
        self._last_ts_ms = 0

    def update_mid(self, mid: float, ts_ms: int) -> None:
        self._last_mid = float(mid)
        self._last_ts_ms = int(ts_ms)

    def append_pending(self, tr: PendingTrade) -> bool:
        """
        Append new pending trade with backpressure:
        - utilization = (len-head)/max_pending
        - if projected utilization > 0.9: pause & drop new pending (do NOT append)
        """
        # basic sanity to avoid NaN/Inf poisoning pending arrays
        try:
            if not (isinstance(tr.ts_ms, int) or isinstance(tr.ts_ms, float)):
                return False
            if not math.isfinite(float(tr.price)) or float(tr.price) <= 0.0:
                self._m_inc("realized_spread.pending_drop.bad_price", 1)
                return False
            if not math.isfinite(float(tr.qty)) or float(tr.qty) <= 0.0:
                self._m_inc("realized_spread.pending_drop.bad_qty", 1)
                return False
        except Exception:
            return False

        active = self.pending_active()
        projected_active = active + 1

        if not self._should_accept_new_pending(projected_active):
            self._pending_dropped += 1
            self._m_inc("realized_spread.pending_drop.backpressure", 1)
            self._emit_pending_metrics()
            return False

        # safety: keep head consistent and compact if needed
        self._maybe_compact_pending()

        # bounded growth: allow list size up to ~2x max_pending (compaction keeps it stable)
        # but never let active exceed max_pending; if it does (e.g. due to manual head), advance head
        mp = max(int(self.max_pending), 1)
        if active >= mp:
            # fail-open: advance head minimally to keep active == mp-1 then append
            # (still bounded; but backpressure should usually prevent reaching here)
            shift = (active - (mp - 1))
            self.pending_head = min(int(self.pending_head) + int(shift), len(self.pending))
            self._m_inc("realized_spread.pending_shift_to_fit", 1)

        self.pending.append(tr)
        self._emit_pending_metrics()
        return True
