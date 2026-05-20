"""core/live_triple_barrier.py — G6: Live Triple-Barrier Exit Tracker.

Per-position real-time wrapper around core.triple_barrier.label_path().

Design goals
------------
* Train/serve alignment: uses the *same* label_path() function that labels
  training data → live exit decisions are reproducible offline.
* Bounded memory: stores at most MAX_PATH_TICKS (ts_ms, price) pairs per
  position; older ticks are dropped from the front (deque maxlen).
* Immutable after close: once a barrier is hit the tracker is frozen; further
  push_tick() calls return the cached result immediately (O(1)).
* Pure: no IO, no Redis, no Prometheus — belongs in the core/ layer.

Env (read by callers, not here):
  TB_EXIT_ENABLED=0        master switch (int: 0=off, 1=on)
  TB_EXIT_MODE=shadow      shadow | enforce
  TB_COST_BPS=7.0          round-trip cost estimate
  TB_MAX_PATH_TICKS=2000   bounded deque size per tracker
  TB_HORIZON_H=4           default horizon in hours (used when pos has none)
"""

from __future__ import annotations

import os
from collections import deque
from dataclasses import dataclass, field
from typing import Deque

from core.triple_barrier import (
    BarrierOutcome,
    BarrierResult,
    BarrierSpec,
    label_path,
)

MAX_PATH_TICKS: int = int(os.getenv("TB_MAX_PATH_TICKS", "2000"))
_DEFAULT_HORIZON_H: float = float(os.getenv("TB_HORIZON_H", "4"))
_DEFAULT_COST_BPS: float = float(os.getenv("TB_COST_BPS", "7.0"))


# ---------------------------------------------------------------------------
# Public helpers — BarrierSpec derivation from a live position dict/object
# ---------------------------------------------------------------------------


def spec_from_pos(pos: object, *, cost_bps: float | None = None) -> BarrierSpec | None:
    """Derive BarrierSpec from a PositionState (or duck-type dict).

    Returns None if entry_price / SL are not usable (e.g. position not yet open).
    Callers should treat None as "skip tracking this position".

    Priority for horizon:
      1. pos.baseline_horizon_ms (populated by horizon-contract at entry)
      2. pos.hold_target_ms
      3. signal_payload["tb_horizon_h"] * 3600_000
      4. TB_HORIZON_H env * 3600_000
    """
    entry_px = float(getattr(pos, "entry_price", 0.0) or 0.0)
    if entry_px <= 1e-9:
        return None

    # TP barrier: use first TP level
    tp_levels = getattr(pos, "tp_levels", None) or []
    if tp_levels:
        tp1 = float(tp_levels[0])
        if tp1 <= 0:
            return None
        tp_bps = abs(tp1 - entry_px) / entry_px * 10_000.0
    else:
        return None  # no TP → cannot build spec

    # SL barrier: prefer pos.sl (current SL), fall back to baseline_sl
    sl_price = float(getattr(pos, "sl", 0.0) or getattr(pos, "baseline_sl", 0.0) or 0.0)
    if sl_price <= 0:
        return None
    sl_bps = abs(sl_price - entry_px) / entry_px * 10_000.0

    if tp_bps < 0.5 or sl_bps < 0.5:
        # degenerate spec — skip
        return None

    # Horizon
    h_ms: int = 0
    for attr in ("baseline_horizon_ms", "hold_target_ms"):
        v = int(getattr(pos, attr, 0) or 0)
        if v > 0:
            h_ms = v
            break
    if not h_ms:
        sp = getattr(pos, "signal_payload", None) or {}
        h_h = float(sp.get("tb_horizon_h", 0) or 0)
        h_ms = int(h_h * 3_600_000) if h_h > 0 else int(_DEFAULT_HORIZON_H * 3_600_000)

    effective_cost = cost_bps if cost_bps is not None else _DEFAULT_COST_BPS
    return BarrierSpec(h_ms=h_ms, tp_bps=tp_bps, sl_bps=sl_bps, cost_bps=effective_cost)


# ---------------------------------------------------------------------------
# LiveBarrierTracker
# ---------------------------------------------------------------------------


@dataclass
class LiveBarrierTracker:
    """Accumulates real-time price ticks and evaluates triple-barrier at each step.

    Usage::

        spec = spec_from_pos(pos)
        tracker = LiveBarrierTracker(
            sid=pos.sid,
            entry_px=pos.entry_price,
            entry_ts_ms=pos.entry_ts_ms,
            direction=pos.direction,
            spec=spec,
        )
        # on each tick:
        result = tracker.push_tick(ts_ms, mid_price)
        if result.outcome != BarrierOutcome.TIMEOUT:
            # barrier hit — handle accordingly

    Notes:
    * TIMEOUT in ``result.outcome`` means "no barrier hit yet in the current
      path window" — the position is still live.  The tracker distinguishes
      horizon expiry via ``is_horizon_expired(now_ms)``.
    * Path is bounded by MAX_PATH_TICKS deque (LRU drop from left).
    * Once ``done`` is True all push_tick() calls are O(1) no-ops.
    """

    sid: str
    entry_px: float
    entry_ts_ms: int
    direction: str
    spec: BarrierSpec

    _path: Deque[tuple[int, float]] = field(
        default_factory=lambda: deque(maxlen=MAX_PATH_TICKS), repr=False
    )
    _last_result: BarrierResult | None = field(default=None, repr=False)
    _done: bool = field(default=False, repr=False)

    # ---------- public API ----------

    def push_tick(self, ts_ms: int, price: float) -> BarrierResult:
        """Ingest one tick and return the current BarrierResult.

        - If already done (barrier previously hit): returns cached result.
        - If tick is beyond horizon: marks done, returns TIMEOUT result.
        - Otherwise: appends tick, re-evaluates label_path().
        """
        if self._done:
            assert self._last_result is not None
            return self._last_result

        # Enforce horizon: don't accept ticks past the window boundary
        if ts_ms > self.entry_ts_ms + self.spec.h_ms:
            result = self._evaluate()
            self._last_result = result
            self._done = True
            return result

        self._path.append((ts_ms, float(price)))
        result = self._evaluate()
        self._last_result = result

        if result.outcome != BarrierOutcome.TIMEOUT:
            self._done = True

        return result

    def is_horizon_expired(self, now_ms: int) -> bool:
        """True when current time exceeds entry_ts_ms + spec.h_ms."""
        return now_ms > self.entry_ts_ms + self.spec.h_ms

    def current_result(self) -> BarrierResult | None:
        """Latest computed BarrierResult, or None if no ticks yet."""
        return self._last_result

    @property
    def done(self) -> bool:
        return self._done

    @property
    def path_len(self) -> int:
        return len(self._path)

    # ---------- internals ----------

    def _evaluate(self) -> BarrierResult:
        return label_path(
            ts0_ms=self.entry_ts_ms,
            direction=self.direction,
            entry_px=self.entry_px,
            path=list(self._path),
            spec=self.spec,
        )
