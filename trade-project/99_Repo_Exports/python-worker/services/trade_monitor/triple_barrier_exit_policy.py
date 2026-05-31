"""services/trade_monitor/triple_barrier_exit_policy.py — G6: Triple-Barrier Live Exit Gate.

Shadow/enforce gate that wraps core.live_triple_barrier.LiveBarrierTracker.
Manages a per-position registry of trackers, updated on every tick.

Key design decisions
--------------------
* SHADOW (default): on every tick computes label_path() result, emits
  Prometheus metrics and structured logs, but does NOT close the position.
* ENFORCE: when the TIMEOUT barrier fires (horizon expired), requests a
  forced close via a callback.  TP/SL barriers in ENFORCE mode are
  deliberately NOT wired (they duplicate process_tick() level checks and
  would create double-exit races).
* Stateless per tick: no locks needed — caller must hold symbol-lock when
  calling push_tick() (same as the rest of on_tick()).
* Fail-open: any exception in G6 is logged and swallowed; never blocks
  the main trade flow.

Env
---
  TB_EXIT_ENABLED=0     master switch (0=off, 1=shadow/enforce, default off)
  TB_EXIT_MODE=shadow   shadow | enforce
  TB_COST_BPS=7.0       round-trip cost estimate (bps)

Prometheus counters (registered by caller, injected via constructor):
  g6_tb_tick_total{symbol, outcome}
  g6_tb_timeout_close_total{symbol, mode}
  g6_tb_horizon_expire_total{symbol}
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Callable

from core.live_triple_barrier import (
    LiveBarrierTracker,
    spec_from_pos,
)
from core.triple_barrier import BarrierOutcome, BarrierResult

logger = logging.getLogger("G6TripleBarrierExitPolicy")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_ENABLED: bool = os.getenv("TB_EXIT_ENABLED", "0") == "1"
_MODE: str = os.getenv("TB_EXIT_MODE", "shadow").lower().strip()  # shadow | enforce


def _is_enabled() -> bool:
    return _ENABLED


def _is_enforce() -> bool:
    return _MODE == "enforce"


# ---------------------------------------------------------------------------
# Decision dataclass (returned to caller)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TBExitDecision:
    """Describes the G6 decision for one tick on one position."""

    sid: str
    symbol: str
    outcome: str          # BarrierOutcome value string
    mode: str             # "shadow" | "enforce"
    should_close: bool    # True only for enforce + TIMEOUT
    close_reason: str     # e.g. "g6_tb_timeout" | "g6_tb_tp" | "g6_tb_sl" | ""
    result: BarrierResult | None = None


# ---------------------------------------------------------------------------
# G6 Gate
# ---------------------------------------------------------------------------


class G6TripleBarrierExitGate:
    """Registry + tick dispatcher for LiveBarrierTrackers.

    Lifecycle
    ---------
    * open_position(pos)       — called when a position is registered
    * push_tick(pos, ts_ms, price) — called for every on_tick price update
    * close_position(sid)      — called when position is closed (cleanup)

    The gate is stateless regarding mode — mode is read from env at each call
    so it can be toggled at runtime (restart required to pick up change).
    """

    def __init__(self) -> None:
        self._trackers: dict[str, LiveBarrierTracker] = {}

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    def open_position(self, pos: Any) -> bool:
        """Register a new position.  Returns True if tracker was created."""
        if not _is_enabled():
            return False
        sid = str(getattr(pos, "sid", "") or getattr(pos, "id", ""))
        if not sid or sid in self._trackers:
            return False
        try:
            spec = spec_from_pos(pos)
            if spec is None:
                logger.debug("G6: skip pos %s — cannot derive BarrierSpec", sid[:12])
                return False
            tracker = LiveBarrierTracker(
                sid=sid,
                entry_px=float(pos.entry_price),
                entry_ts_ms=int(pos.entry_ts_ms),
                direction=str(pos.direction),
                spec=spec,
            )
            self._trackers[sid] = tracker
            logger.debug(
                "G6: registered sid=%s dir=%s tp_bps=%.1f sl_bps=%.1f h_h=%.1f",
                sid[:12], pos.direction,
                spec.tp_bps, spec.sl_bps, spec.h_ms / 3_600_000,
            )
            return True
        except Exception as exc:
            logger.warning("G6: open_position failed sid=%s: %s", sid[:12], exc)
            return False

    def close_position(self, sid: str) -> None:
        """Clean up tracker when position is finalized."""
        self._trackers.pop(sid, None)

    # -------------------------------------------------------------------------
    # Per-tick entry point
    # -------------------------------------------------------------------------

    def push_tick(
        self,
        pos: Any,
        ts_ms: int,
        price: float,
        *,
        on_timeout_close: Callable[[str, BarrierResult], None] | None = None,
    ) -> TBExitDecision | None:
        """Evaluate barriers for one tick on one position.

        Parameters
        ----------
        pos              — PositionState (or duck-type object with .sid / .symbol)
        ts_ms            — tick timestamp (epoch ms)
        price            — mid price at tick
        on_timeout_close — callback(sid, result) invoked in ENFORCE mode when
                           TIMEOUT barrier fires; caller triggers the actual close

        Returns None if G6 is disabled, tracker not found, or an exception occurred.
        Returns TBExitDecision describing the outcome.
        """
        if not _is_enabled():
            return None

        sid = str(getattr(pos, "sid", "") or getattr(pos, "id", ""))
        if not sid:
            return None

        tracker = self._trackers.get(sid)
        if tracker is None:
            # Lazy registration (position opened before G6 was enabled)
            created = self.open_position(pos)
            if not created:
                return None
            tracker = self._trackers.get(sid)
            if tracker is None:
                return None

        try:
            result = tracker.push_tick(ts_ms=ts_ms, price=price)
        except Exception as exc:
            logger.warning("G6: push_tick failed sid=%s: %s", sid[:12], exc)
            return None

        symbol = str(getattr(pos, "symbol", "unknown"))
        outcome = result.outcome
        mode = "enforce" if _is_enforce() else "shadow"

        # NO_TICKS with an expired horizon is semantically identical to TIMEOUT:
        # the position held its full duration without producing any tracked ticks.
        if outcome == BarrierOutcome.NO_TICKS and tracker.is_horizon_expired(ts_ms):
            outcome = BarrierOutcome.TIMEOUT

        # --- Determine action ---
        should_close = False
        close_reason = ""

        if outcome == BarrierOutcome.TIMEOUT and tracker.is_horizon_expired(ts_ms):
            # Horizon expired with no barrier hit → true TIMEOUT
            if _is_enforce():
                should_close = True
                close_reason = "g6_tb_timeout"
                logger.info(
                    "G6 TIMEOUT ENFORCE: sid=%s sym=%s realized_close=%.1f bps "
                    "edge_after_cost=%.1f bps path_len=%d",
                    sid[:12], symbol,
                    result.realized_close_bps,
                    result.edge_after_cost_bps,
                    tracker.path_len,
                )
                if on_timeout_close is not None:
                    try:
                        on_timeout_close(sid, result)
                    except Exception as cb_err:
                        logger.warning("G6: on_timeout_close callback failed: %s", cb_err)
            else:
                logger.info(
                    "G6 TIMEOUT SHADOW: sid=%s sym=%s realized_close=%.1f bps "
                    "path_len=%d",
                    sid[:12], symbol, result.realized_close_bps, tracker.path_len,
                )

        elif outcome == BarrierOutcome.TP_HIT:
            # TP barrier hit by label_path() — process_tick() handles actual close,
            # we emit shadow metric only (avoid double-exit)
            close_reason = "g6_tb_tp"
            logger.debug(
                "G6 TP shadow: sid=%s sym=%s tp_bps=%.1f edge=%.1f",
                sid[:12], symbol, tracker.spec.tp_bps, result.edge_after_cost_bps,
            )

        elif outcome == BarrierOutcome.SL_HIT:
            close_reason = "g6_tb_sl"
            logger.debug(
                "G6 SL shadow: sid=%s sym=%s sl_bps=%.1f",
                sid[:12], symbol, tracker.spec.sl_bps,
            )

        return TBExitDecision(
            sid=sid,
            symbol=symbol,
            outcome=outcome.value,
            mode=mode,
            should_close=should_close,
            close_reason=close_reason,
            result=result,
        )

    # -------------------------------------------------------------------------
    # Introspection
    # -------------------------------------------------------------------------

    @property
    def tracker_count(self) -> int:
        return len(self._trackers)

    def tracker_sids(self) -> list[str]:
        return list(self._trackers)
