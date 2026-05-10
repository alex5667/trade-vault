"""order_modify_service.py — handle_modify logic for Binance executor.

Extracted from binance_executor.py (god-class decomposition).

Responsibilities:
- Modify SL/TP prices on an existing protected position
- Cancel old SL/TP orders and re-place with new prices
- Validate new prices vs current state
- Emit FSM events
"""
from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from services.execution.binance_order_mapper import (
    _f, _normalize_side, FSM_FAILED,
)

if TYPE_CHECKING:
    from services.binance_futures_client import BinanceFuturesClient
    from services.execution.binance_filters import FiltersCache
    from services.execution.execution_state_store import ExecutionStateStore
    from services.execution.execution_event_writer import ExecutionEventWriter
    from services.execution.protection_service import ProtectionService


def _ms_now() -> int:
    try:
        from utils.time_utils import get_ny_time_millis
        return get_ny_time_millis()
    except Exception:
        return int(time.time() * 1000)


class OrderModifyService:
    """Handles handle_modify: update SL/TP on an open protected position.

    Injected dependencies keep the class easily testable and free of god-class
    anti-patterns.
    """

    def __init__(
        self,
        *,
        position_mode: str = "oneway",
        sl_working_type: str = "MARK_PRICE",
        exec_modify_resize_strict_replace: bool = True,
        state_store: "ExecutionStateStore | None" = None,
        event_writer: "ExecutionEventWriter | None" = None,
        protection_service: "ProtectionService | None" = None,
        r: Any = None,
    ) -> None:
        self.position_mode = position_mode
        self.sl_working_type = sl_working_type
        self.exec_modify_resize_strict_replace = exec_modify_resize_strict_replace
        self._state = state_store
        self._events = event_writer
        self._protection = protection_service
        self.r = r

    def _write_event(self, fields: dict[str, Any]) -> None:
        if self._events:
            self._events.write(fields)

    def _load_state(self, sid: str) -> dict[str, Any]:
        if self._state:
            return self._state.load(sid)
        return {}

    def _save_state(self, sid: str, state: dict[str, Any]) -> None:
        if self._state:
            self._state.save(sid, state)

    def _transition(self, sid: str, *, symbol: str, action: str, next_state: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
        if self._state:
            return self._state.transition(sid, symbol=symbol, action=action, next_state=next_state, details=details)
        return {}

    # ------------------------------------------------------------------
    # handle_modify
    # ------------------------------------------------------------------

    def handle_modify(
        self,
        *,
        payload: dict[str, Any],
        client: "BinanceFuturesClient",
        filters: "FiltersCache",
        sid: str,
        ts_queue_ms: int,
        ts_exec_start_ms: int,
    ) -> dict[str, Any]:
        """Modify SL/TP prices on an existing protected position.

        Flow:
        1. Load current state
        2. Validate new prices
        3. Cancel old protection orders (strict-replace mode)
        4. Re-arm protection with new prices
        5. Update state + emit event

        Returns updated state dict.
        """
        symbol = (payload.get("symbol") or "").strip().upper()
        _, logical_side, _ = _normalize_side(payload)

        # Load current state
        state = self._load_state(sid)
        if not state:
            self._write_event({
                "sid": sid, "symbol": symbol, "action": "modify",
                "event_type": "MODIFY_STATE_MISS",
                "severity": "error",
            })
            return {"sid": sid, "symbol": symbol, "action": "modify_failed", "reason": "state_miss"}

        current_fsm = (state.get("fsm_state") or "").strip().upper()
        if current_fsm in {"EXIT_FILLED", "EMERGENCY_FLATTENED", "FAILED", "CANCELLED"}:
            self._write_event({
                "sid": sid, "symbol": symbol, "action": "modify",
                "event_type": "MODIFY_SKIPPED_TERMINAL",
                "severity": "info",
                "current_state": current_fsm,
            })
            return state

        new_sl = _f(payload.get("sl") or payload.get("new_sl"), 0.0)
        new_tp_levels = [_f(p) for p in (payload.get("tp_levels") or payload.get("new_tp_levels") or []) if p is not None]
        qty = _f(state.get("filled_qty") or state.get("qty") or payload.get("qty"), 0.0)

        # Validate prices
        if new_sl > 0 and self._protection:
            errors = self._protection.validate_protective_prices(
                sid=sid, symbol=symbol, logical_side=logical_side,
                entry_price=_f(state.get("entry_price") or state.get("avg_price"), 0.0),
                sl_price=new_sl,
                tp_levels=new_tp_levels,
            )
            if errors:
                self._write_event({
                    "sid": sid, "symbol": symbol, "action": "modify",
                    "event_type": "MODIFY_PRICE_VALIDATION_FAILED",
                    "severity": "error",
                    "errors": str(errors),
                })
                return self._transition(sid, symbol=symbol, action="modify", next_state=FSM_FAILED,
                                        details={"reason": "price_validation_failed", "errors": str(errors[:3])})

        # Cancel old protection (strict-replace)
        if self.exec_modify_resize_strict_replace and self._protection:
            self._protection.cancel_expected_protection_refs(
                sid=sid, symbol=symbol, state=state, client=client
            )

        # Re-arm protection
        new_prot: dict[str, Any] = {}
        if self._protection and new_sl > 0:
            new_prot = self._protection.place_protective(
                sid=sid, symbol=symbol, logical_side=logical_side,
                qty=qty, sl_price=new_sl, tp_levels=new_tp_levels,
                tp_qtys=[],
                client=client, filters=filters, r=self.r,
            )

        # Update state
        updated = dict(state)
        updated.update(new_prot)
        if new_sl > 0:
            updated["sl_price"] = new_sl
        if new_tp_levels:
            updated["tp_levels"] = new_tp_levels
        updated["modify_ts_ms"] = _ms_now()

        self._save_state(sid, updated)
        self._write_event({
            "sid": sid, "symbol": symbol, "action": "modify",
            "event_type": "POSITION_MODIFIED",
            "new_sl": new_sl,
            "new_tp_count": len(new_tp_levels),
            "ts_queue_ms": ts_queue_ms,
            "ts_exec_start_ms": ts_exec_start_ms,
        })
        return updated
