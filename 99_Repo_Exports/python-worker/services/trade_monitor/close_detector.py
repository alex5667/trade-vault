# services/trade_monitor/close_detector.py
"""
CloseDetector — on_tick fan-out and external event orchestration.

Extracted from TradeMonitorService.on_tick() (monolith lines 3903-4295)
and apply_external_* methods (4684-4959).

Design:
  - CloseDetector is a pure orchestrator: it holds no state of its own.
  - All state access happens via injected callbacks.
  - Threading / locking is the caller's responsibility (TradeMonitorService /
    monitor_app.py manages the global lock and symbol locks).
  - Fail-open on every path.
"""
from __future__ import annotations

import contextlib
import logging
from dataclasses import dataclass
from typing import Any, Callable

from utils.time_utils import get_ny_time_millis

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class TickResult:
    """Summary of a single tick's processing outcome."""

    symbol: str
    ts_ms: int
    positions_checked: int
    events_emitted: int
    did_close: bool


class CloseDetector:
    """
    On-tick position fan-out: checks every open position for close conditions.

    Injected callbacks:
      get_positions_for_symbol_fn — (symbol) -> list[PositionState]
      process_position_tick_fn   — (pos, tick, spec) -> list[TradeEvent]
      emit_event_fn              — (event) -> None
      get_spec_fn                — (symbol) -> SymbolSpec
      handle_close_events_fn     — (pos, events, tick) -> None  (does IO, emits metrics)
      handle_external_close_fn   — (pos, reason, price, ts_ms) -> None
      log                        — optional Logger
    """

    def __init__(
        self,
        *,
        get_positions_for_symbol_fn: Callable | None = None,
        process_position_tick_fn: Callable | None = None,
        emit_event_fn: Callable | None = None,
        get_spec_fn: Callable | None = None,
        handle_close_events_fn: Callable | None = None,
        handle_external_close_fn: Callable | None = None,
        log: logging.Logger | None = None,
    ) -> None:
        self._get_positions = get_positions_for_symbol_fn
        self._process_tick = process_position_tick_fn
        self._emit_event = emit_event_fn
        self._get_spec = get_spec_fn
        self._handle_close_events = handle_close_events_fn
        self._handle_external_close = handle_external_close_fn
        self._logger = log or logger

    # ------------------------------------------------------------------
    # On-tick fan-out (hot path)
    # ------------------------------------------------------------------

    def process_tick(self, tick: Any) -> TickResult:
        """
        Process one market tick: iterate all open positions for this symbol,
        check for close conditions, emit events.

        Returns a TickResult summary.
        """
        symbol = str(getattr(tick, "symbol", "") or "")
        ts_ms = int(getattr(tick, "ts_ms", 0) or 0)
        positions_checked = 0
        events_emitted = 0
        did_close = False

        if not symbol:
            return TickResult(symbol, ts_ms, 0, 0, False)

        positions = []
        if callable(self._get_positions):
            with contextlib.suppress(Exception):
                positions = self._get_positions(symbol)

        spec = None
        if callable(self._get_spec):
            with contextlib.suppress(Exception):
                spec = self._get_spec(symbol)

        for pos in positions:
            if getattr(pos, "closed", False):
                continue
            positions_checked += 1
            try:
                events: list[Any] = []
                if callable(self._process_tick):
                    with contextlib.suppress(Exception):
                        events = self._process_tick(pos, tick, spec) or []

                for ev in events:
                    events_emitted += 1
                    if callable(self._emit_event):
                        with contextlib.suppress(Exception):
                            self._emit_event(ev)
                    et = getattr(ev, "event_type", "")
                    if et in ("CLOSE", "TIME_BE_EXIT", "SL_HIT_VIRTUAL"):
                        did_close = True

                if events and callable(self._handle_close_events):
                    with contextlib.suppress(Exception):
                        self._handle_close_events(pos, events, tick)

            except Exception as e:
                self._logger.warning(
                    "CloseDetector.process_tick error for %s/%s: %s",
                    symbol, getattr(pos, "id", "?"), e,
                )

        return TickResult(
            symbol=symbol,
            ts_ms=ts_ms,
            positions_checked=positions_checked,
            events_emitted=events_emitted,
            did_close=did_close,
        )

    # ------------------------------------------------------------------
    # External close events (TP_HIT, SL_HIT, forced close)
    # ------------------------------------------------------------------

    def apply_external_tp_hit(
        self,
        sid: str,
        fill_price: float,
        fill_qty: float,
        ts_ms: int,
        tp_level: int = 1,
    ) -> bool:
        """
        Handle an external TP hit notification (from Binance user-stream).

        Returns True if the position was found and processed.
        """
        return self._apply_external(
            sid=sid,
            event_type="TP_HIT",
            price=fill_price,
            qty=fill_qty,
            ts_ms=ts_ms,
            extra={"tp_level": tp_level},
        )

    def apply_external_sl_hit(
        self,
        sid: str,
        fill_price: float,
        fill_qty: float,
        ts_ms: int,
    ) -> bool:
        """
        Handle an external SL hit notification.

        Returns True if the position was found and processed.
        """
        return self._apply_external(
            sid=sid,
            event_type="SL_HIT",
            price=fill_price,
            qty=fill_qty,
            ts_ms=ts_ms,
        )

    def apply_external_force_close(
        self,
        sid: str,
        fill_price: float,
        fill_qty: float,
        ts_ms: int,
        reason: str = "EXTERNAL_CLOSE",
    ) -> bool:
        """
        Handle an external forced close (e.g., liquidation, manual close).
        """
        return self._apply_external(
            sid=sid,
            event_type="EXTERNAL_CLOSE",
            price=fill_price,
            qty=fill_qty,
            ts_ms=ts_ms,
            extra={"reason": reason},
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _apply_external(
        self,
        sid: str,
        event_type: str,
        price: float,
        qty: float,
        ts_ms: int,
        extra: dict | None = None,
    ) -> bool:
        """Generic dispatcher for external close events."""
        if not callable(self._handle_external_close):
            return False
        try:
            self._handle_external_close(
                sid=sid,
                event_type=event_type,
                price=price,
                qty=qty,
                ts_ms=ts_ms,
                extra=extra or {},
            )
            return True
        except Exception as e:
            self._logger.warning(
                "CloseDetector._apply_external %s/%s failed: %s", event_type, sid, e
            )
            return False
