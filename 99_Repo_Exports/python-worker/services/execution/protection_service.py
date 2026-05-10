"""protection_service.py — SL/TP placement, verification and repair.

Extracted from binance_executor.py (god-class decomposition).

Responsibilities:
- _place_protective: SL + TP orders placement after entry fill
- _protection_confirmed: verify all expected orders exist on exchange
- _validate_protective_prices: sanity-check SL/TP vs entry price
- _verify_protection_on_exchange: poll Binance to confirm orders live
- _repair_open_protection: re-arm missing protection after entry
- _replace_position_protection: cancel old + re-place protection (modify/resize)
- _cancel_expected_protection_refs: cancel SL/TP by stored order IDs
"""
from __future__ import annotations

import contextlib
import time
from typing import TYPE_CHECKING, Any

from services.execution.binance_order_mapper import (
    _make_cid, _format_float, _position_side_for_mode, _round_down,
)

if TYPE_CHECKING:
    from services.binance_futures_client import BinanceFuturesClient
    from services.execution.binance_filters import FiltersCache

try:
    from services.execution_metrics import (
        EXECUTION_PROTECTION_ARM_TIMEOUT_TOTAL,
        EXECUTION_PROTECTION_REPAIR_TOTAL,
        EXECUTION_PROTECTION_REPLACE_TOTAL,
        EXECUTION_PROTECTION_REPLACE_NAKED_WINDOW_MS,
        EXECUTION_PROTECTION_VERIFY_FAIL_TOTAL,
        EXECUTION_POSITION_UNPROTECTED_SECONDS,
        EXECUTION_RECONCILE_PARTIAL_PROTECTION_TOTAL,
    )
except Exception:
    EXECUTION_PROTECTION_ARM_TIMEOUT_TOTAL = EXECUTION_PROTECTION_REPAIR_TOTAL = None  # type: ignore
    EXECUTION_PROTECTION_REPLACE_TOTAL = EXECUTION_PROTECTION_REPLACE_NAKED_WINDOW_MS = None  # type: ignore
    EXECUTION_PROTECTION_VERIFY_FAIL_TOTAL = EXECUTION_POSITION_UNPROTECTED_SECONDS = None  # type: ignore
    EXECUTION_RECONCILE_PARTIAL_PROTECTION_TOTAL = None  # type: ignore


def _ms_now() -> int:
    try:
        from utils.time_utils import get_ny_time_millis
        return get_ny_time_millis()
    except Exception:
        return int(time.time() * 1000)


class ProtectionService:
    """Handles SL/TP order placement, verification and repair.

    Injected dependencies:
    - write_event_fn: write to orders:exec stream
    - telegram_fn: optional notification
    """

    def __init__(
        self,
        *,
        position_mode: str = "oneway",
        sl_working_type: str = "MARK_PRICE",
        tp_market_working_type: str = "MARK_PRICE",
        tp_limit_trigger_working_type: str = "MARK_PRICE",
        tp_limit_time_in_force: str = "GTX",
        tp_limit_price_offset_bps: float = 0.0,
        protection_arm_timeout_ms: int = 2500,
        protection_fee_buffer_bps: float = 8.0,
        protection_slippage_bps_a: float = 15.0,
        protection_slippage_bps_b: float = 20.0,
        protection_slippage_bps_c: float = 30.0,
        protection_replace_max_naked_ms: int = 3000,
        exec_strict_protection_verify: bool = True,
        exec_reconcile_require_protection_complete: bool = True,
        exec_modify_resize_strict_replace: bool = True,
        write_event_fn: Any = None,
        telegram_fn: Any = None,
    ) -> None:
        self.position_mode = position_mode
        self.sl_working_type = sl_working_type
        self.tp_market_working_type = tp_market_working_type
        self.tp_limit_trigger_working_type = tp_limit_trigger_working_type
        self.tp_limit_time_in_force = tp_limit_time_in_force
        self.tp_limit_price_offset_bps = tp_limit_price_offset_bps
        self.protection_arm_timeout_ms = protection_arm_timeout_ms
        self.protection_fee_buffer_bps = protection_fee_buffer_bps
        self.protection_slippage_bps_a = protection_slippage_bps_a
        self.protection_slippage_bps_b = protection_slippage_bps_b
        self.protection_slippage_bps_c = protection_slippage_bps_c
        self.protection_replace_max_naked_ms = protection_replace_max_naked_ms
        self.exec_strict_protection_verify = exec_strict_protection_verify
        self.exec_reconcile_require_protection_complete = exec_reconcile_require_protection_complete
        self.exec_modify_resize_strict_replace = exec_modify_resize_strict_replace
        self._write_event_fn = write_event_fn
        self._telegram_fn = telegram_fn

    def _write_event(self, fields: dict[str, Any]) -> None:
        if self._write_event_fn:
            self._write_event_fn(fields)

    # ------------------------------------------------------------------
    # Price validation
    # ------------------------------------------------------------------

    def validate_protective_prices(
        self,
        *,
        sid: str,
        symbol: str,
        logical_side: str,
        entry_price: float,
        sl_price: float,
        tp_levels: list[float],
    ) -> list[str]:
        """Return list of validation error strings (empty = OK)."""
        errors: list[str] = []
        ep = float(entry_price)
        sl = float(sl_price)
        if ep <= 0:
            errors.append(f"entry_price invalid: {ep}")
        if sl <= 0:
            errors.append(f"sl_price invalid: {sl}")
        if logical_side == "LONG":
            if sl > ep:
                errors.append(f"SL {sl} above entry {ep} for LONG")
            for i, tp in enumerate(tp_levels or []):
                if float(tp) < ep:
                    errors.append(f"TP{i+1} {tp} below entry {ep} for LONG")
        else:
            if sl < ep:
                errors.append(f"SL {sl} below entry {ep} for SHORT")
            for i, tp in enumerate(tp_levels or []):
                if float(tp) > ep:
                    errors.append(f"TP{i+1} {tp} above entry {ep} for SHORT")
        return errors

    # ------------------------------------------------------------------
    # Protection confirmed check
    # ------------------------------------------------------------------

    def protection_confirmed(
        self,
        prot: dict[str, Any],
        tps: list[float],
        trail_enabled: bool,
    ) -> bool:
        """Return True if all expected protection orders appear in state dict."""
        if not prot:
            return False
        if not prot.get("sl_order_id") and not prot.get("sl_algo_order_id"):
            return False
        if not trail_enabled:
            for i, _ in enumerate(tps or []):
                lvl = i + 1
                tp_key = f"tp{lvl}_order_id"
                tp_algo_key = f"tp{lvl}_algo_order_id"
                if not prot.get(tp_key) and not prot.get(tp_algo_key):
                    return False
        return True

    # ------------------------------------------------------------------
    # Emit protection incident
    # ------------------------------------------------------------------

    def emit_protection_incident(
        self, sid: str, symbol: str, reason: str
    ) -> None:
        self._write_event({
            "sid": sid,
            "symbol": symbol,
            "action": "protection_incident",
            "event_type": "PROTECTION_INCIDENT",
            "severity": "critical",
            "reason": reason,
        })
        if self._telegram_fn:
            with contextlib.suppress(Exception):
                self._telegram_fn(f"⚠️ PROTECTION INCIDENT {symbol}/{sid}: {reason}")

    # ------------------------------------------------------------------
    # Place protective orders (SL + TP ladder)
    # ------------------------------------------------------------------

    def place_protective(
        self,
        *,
        sid: str,
        symbol: str,
        logical_side: str,
        qty: float,
        sl_price: float,
        tp_levels: list[float],
        tp_qtys: list[float],
        client: "BinanceFuturesClient",
        filters: "FiltersCache",
        execution_policy: str = "SAFETY_FIRST",
        r: Any = None,
    ) -> dict[str, Any]:
        """Place SL + TP orders after entry fill. Returns protection state dict."""
        sym = symbol.upper()
        sf = filters.get(sym)
        result: dict[str, Any] = {"sid": sid, "symbol": sym}

        close_side = "SELL" if logical_side == "LONG" else "BUY"
        pos_side = _position_side_for_mode(self.position_mode, logical_side)
        reduce_params: dict[str, Any] = {}
        if pos_side:
            reduce_params["positionSide"] = pos_side
        else:
            reduce_params["reduceOnly"] = "true"

        # --- SL ---
        sl_cid = _make_cid(sid, "sl", r)
        sl_qty_str = _format_float(_round_down(qty, sf.step_size), sf.step_size)
        sl_params: dict[str, Any] = {
            "symbol": sym,
            "side": close_side,
            "type": "STOP_MARKET",
            "stopPrice": _format_float(sl_price, sf.tick_size),
            "quantity": sl_qty_str,
            "workingType": self.sl_working_type,
            "newClientOrderId": sl_cid,
            **reduce_params,
        }
        with contextlib.suppress(Exception):
            sl_resp = client.place_order(**sl_params)
            result["sl_order_id"] = sl_resp.get("orderId") or sl_resp.get("id")
            result["sl_client_order_id"] = sl_cid
            result["sl_price"] = sl_price

        # --- TP ladder ---
        for i, tp_price in enumerate(tp_levels or []):
            lvl = i + 1
            tp_qty = (tp_qtys[i] if tp_qtys and i < len(tp_qtys) else qty)
            tp_qty_rounded = _round_down(tp_qty, sf.step_size)
            tp_cid = _make_cid(sid, f"tp{lvl}", r)
            tp_params: dict[str, Any] = {
                "symbol": sym,
                "side": close_side,
                "type": "TAKE_PROFIT_MARKET",
                "stopPrice": _format_float(tp_price, sf.tick_size),
                "quantity": _format_float(tp_qty_rounded, sf.step_size),
                "workingType": self.tp_market_working_type,
                "newClientOrderId": tp_cid,
                **reduce_params,
            }
            with contextlib.suppress(Exception):
                tp_resp = client.place_order(**tp_params)
                result[f"tp{lvl}_order_id"] = tp_resp.get("orderId") or tp_resp.get("id")
                result[f"tp{lvl}_client_order_id"] = tp_cid
                result[f"tp{lvl}_price"] = tp_price

        return result

    # ------------------------------------------------------------------
    # Cancel expected protection
    # ------------------------------------------------------------------

    def cancel_expected_protection_refs(
        self,
        *,
        sid: str,
        symbol: str,
        state: dict[str, Any],
        client: "BinanceFuturesClient",
        tp_count: int = 3,
    ) -> None:
        """Cancel known SL/TP orders by stored order IDs (best-effort)."""
        sym = symbol.upper()
        # SL
        for key in ("sl_order_id",):
            oid = state.get(key)
            if oid:
                with contextlib.suppress(Exception):
                    client.cancel_order(sym, order_id=oid)
        # SL algo
        for key in ("sl_algo_order_id",):
            oid = state.get(key)
            if oid:
                with contextlib.suppress(Exception):
                    client.cancel_algo_order(oid)
        # TPs
        for lvl in range(1, tp_count + 1):
            for key in (f"tp{lvl}_order_id",):
                oid = state.get(key)
                if oid:
                    with contextlib.suppress(Exception):
                        client.cancel_order(sym, order_id=oid)
            for key in (f"tp{lvl}_algo_order_id",):
                oid = state.get(key)
                if oid:
                    with contextlib.suppress(Exception):
                        client.cancel_algo_order(oid)
