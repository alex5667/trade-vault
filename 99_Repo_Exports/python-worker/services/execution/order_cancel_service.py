"""order_cancel_service.py — handle_cancel and handle_resize logic.

Extracted from binance_executor.py (god-class decomposition).

Responsibilities:
- handle_cancel: cancel all orders for SID, optionally flatten position
- handle_resize: reduce position size by cancelling partial TPs + partial exit
- Emit FSM events at each step
"""
from __future__ import annotations

import contextlib
import time
from typing import TYPE_CHECKING, Any

from services.execution.binance_order_mapper import (
    _f, _make_cid, _format_float, _round_down, _normalize_side, FSM_EXIT_FILLED,
)

if TYPE_CHECKING:
    from services.binance_futures_client import BinanceFuturesClient
    from services.execution.binance_filters import FiltersCache
    from services.execution.execution_state_store import ExecutionStateStore
    from services.execution.execution_event_writer import ExecutionEventWriter
    from services.execution.protection_service import ProtectionService
    from services.execution.emergency_flatten_service import EmergencyFlattenService


def _ms_now() -> int:
    try:
        from utils.time_utils import get_ny_time_millis
        return get_ny_time_millis()
    except Exception:
        return int(time.time() * 1000)


class OrderCancelService:
    """Handles handle_cancel and handle_resize.

    Cancel closes a position by:
    1. Cancelling all known SL/TP orders
    2. Placing a MARKET reduce-only close for the remaining qty

    Resize reduces position size partially using a reduce-only MARKET or limit exit.
    """

    def __init__(
        self,
        *,
        position_mode: str = "oneway",
        cancel_mode: str = "market_exit",
        sl_working_type: str = "MARK_PRICE",
        exec_cancel_prefer_reduce_only: bool = True,
        state_store: "ExecutionStateStore | None" = None,
        event_writer: "ExecutionEventWriter | None" = None,
        protection_service: "ProtectionService | None" = None,
        flatten_service: "EmergencyFlattenService | None" = None,
        r: Any = None,
    ) -> None:
        self.position_mode = position_mode
        self.cancel_mode = cancel_mode
        self.sl_working_type = sl_working_type
        self.exec_cancel_prefer_reduce_only = exec_cancel_prefer_reduce_only
        self._state = state_store
        self._events = event_writer
        self._protection = protection_service
        self._flatten = flatten_service
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
    # handle_cancel
    # ------------------------------------------------------------------

    def handle_cancel(
        self,
        *,
        payload: dict[str, Any],
        client: "BinanceFuturesClient",
        filters: "FiltersCache",
        sid: str,
        ts_queue_ms: int,
        ts_exec_start_ms: int,
    ) -> dict[str, Any]:
        """Cancel position: cancel protection orders + close at market.

        cancel_mode options:
        - "market_exit": close position at MARKET (default)
        - "orders_only": cancel orders but leave position open
        - "emergency_flatten": use EmergencyFlattenService
        """
        symbol = (payload.get("symbol") or "").strip().upper()
        _, logical_side, _ = _normalize_side(payload)
        cancel_mode = (payload.get("cancel_mode") or self.cancel_mode).strip().lower()
        close_reason = (payload.get("reason") or payload.get("cancel_reason") or "manual_cancel").strip()

        state = self._load_state(sid)
        if not state:
            self._write_event({
                "sid": sid, "symbol": symbol, "action": "cancel",
                "event_type": "CANCEL_STATE_MISS",
                "severity": "warning",
                "cancel_mode": cancel_mode,
            })

        # Check already terminal
        current_fsm = (state.get("fsm_state") or "").strip().upper()
        if current_fsm in {"EXIT_FILLED", "EMERGENCY_FLATTENED", "FAILED"}:
            self._write_event({
                "sid": sid, "symbol": symbol, "action": "cancel",
                "event_type": "CANCEL_SKIPPED_TERMINAL",
                "severity": "info",
                "current_state": current_fsm,
            })
            return state

        qty = _f(state.get("filled_qty") or state.get("qty") or payload.get("qty"), 0.0)

        # Step 1: Cancel protection orders
        if self._protection and cancel_mode != "orders_only":
            self._protection.cancel_expected_protection_refs(
                sid=sid, symbol=symbol, state=state, client=client
            )

        # Step 2: Close position
        result: dict[str, Any] = {}
        if cancel_mode == "emergency_flatten" and self._flatten:
            result = self._flatten.emergency_flatten(
                sid=sid, symbol=symbol, logical_side=logical_side,
                qty=qty, client=client, filters=filters,
                reason=close_reason,
            )
        elif cancel_mode != "orders_only" and qty > 0:
            # Market reduce-only exit
            sf = filters.get(symbol)
            close_side = "SELL" if logical_side == "LONG" else "BUY"
            close_qty = _round_down(qty, sf.step_size)
            close_cid = _make_cid(sid, "cancel-exit", self.r)
            params: dict[str, Any] = {
                "symbol": symbol,
                "side": close_side,
                "type": "MARKET",
                "quantity": _format_float(close_qty, sf.step_size),
                "newClientOrderId": close_cid,
                "reduceOnly": "true",
            }
            if self.position_mode == "hedge":
                params.pop("reduceOnly", None)
                params["positionSide"] = logical_side

            with contextlib.suppress(Exception):
                close_resp = client.place_order(**params)
                result["exit_order_id"] = close_resp.get("orderId")
                result["exit_client_order_id"] = close_cid
                result["exit_avg_price"] = _f(close_resp.get("avgPrice"), 0.0)

        final_state = self._transition(
            sid, symbol=symbol, action="cancel", next_state=FSM_EXIT_FILLED,
            details={
                "cancel_mode": cancel_mode,
                "close_reason_tag": close_reason,
                "ts_queue_ms": ts_queue_ms,
                "ts_exec_start_ms": ts_exec_start_ms,
                **result,
            }
        )
        self._write_event({
            "sid": sid, "symbol": symbol, "action": "exit_filled",
            "event_type": "exit_filled",
            "cancel_mode": cancel_mode,
            "reason": close_reason,
            **result,
        })
        return final_state

    # ------------------------------------------------------------------
    # handle_resize
    # ------------------------------------------------------------------

    def handle_resize(
        self,
        *,
        payload: dict[str, Any],
        client: "BinanceFuturesClient",
        filters: "FiltersCache",
        sid: str,
        ts_queue_ms: int,
        ts_exec_start_ms: int,
    ) -> dict[str, Any]:
        """Reduce position size by partial market exit.

        Uses resize_qty from payload; if not present, uses resize_pct
        to calculate the reduction as a fraction of current filled_qty.
        """
        symbol = (payload.get("symbol") or "").strip().upper()
        _, logical_side, _ = _normalize_side(payload)

        state = self._load_state(sid)
        if not state:
            self._write_event({
                "sid": sid, "symbol": symbol, "action": "resize",
                "event_type": "RESIZE_STATE_MISS",
                "severity": "error",
            })
            return {"sid": sid, "symbol": symbol, "reason": "state_miss"}

        current_qty = _f(state.get("filled_qty") or state.get("qty") or 0, 0.0)
        resize_mode = (payload.get("resize_mode") or "reduce").strip().lower()

        # Determine target qty to exit
        resize_qty = _f(payload.get("resize_qty") or 0, 0.0)
        if resize_qty <= 0:
            resize_pct = _f(payload.get("resize_pct") or 0.0, 0.0)
            if resize_pct > 0:
                resize_qty = current_qty * _f(resize_pct / 100.0, 0.0)

        if resize_qty <= 0 or resize_qty > current_qty:
            self._write_event({
                "sid": sid, "symbol": symbol, "action": "resize",
                "event_type": "RESIZE_QTY_INVALID",
                "severity": "warning",
                "resize_qty": resize_qty,
                "current_qty": current_qty,
            })
            return state

        sf = filters.get(symbol)
        close_side = "SELL" if logical_side == "LONG" else "BUY"
        close_qty = _round_down(resize_qty, sf.step_size)
        close_cid = _make_cid(sid, "resize-exit", self.r)

        params: dict[str, Any] = {
            "symbol": symbol,
            "side": close_side,
            "type": "MARKET",
            "quantity": _format_float(close_qty, sf.step_size),
            "newClientOrderId": close_cid,
            "reduceOnly": "true",
        }
        if self.position_mode == "hedge":
            params.pop("reduceOnly", None)
            params["positionSide"] = logical_side

        result: dict[str, Any] = {}
        with contextlib.suppress(Exception):
            resp = client.place_order(**params)
            result["resize_exit_order_id"] = resp.get("orderId")
            result["resize_exit_avg_price"] = _f(resp.get("avgPrice"), 0.0)
            result["resize_exit_qty"] = close_qty

        new_qty = max(0.0, current_qty - close_qty)
        updated = dict(state)
        updated.update({
            "filled_qty": new_qty,
            "resize_mode": resize_mode,
            "resize_ts_ms": _ms_now(),
            **result,
        })
        self._save_state(sid, updated)
        self._write_event({
            "sid": sid, "symbol": symbol, "action": "resize",
            "event_type": "POSITION_RESIZED",
            "resize_qty": close_qty,
            "new_qty": new_qty,
            "ts_queue_ms": ts_queue_ms,
            "ts_exec_start_ms": ts_exec_start_ms,
            **result,
        })
        return updated
