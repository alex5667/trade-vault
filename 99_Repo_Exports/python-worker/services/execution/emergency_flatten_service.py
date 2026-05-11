"""emergency_flatten_service.py — Emergency position flatten for Binance executor.

Extracted from binance_executor.py (god-class decomposition).

Responsibilities:
- Emergency flatten: cancel all orders + close position at market
- Dust position detection and cleanup
- Force-flatten-exact: reconciles qty from exchange live data
- Verify symbol flat: poll until position is confirmed closed
- Cancel all symbol orders (plain + algo) best-effort
"""
from __future__ import annotations

import contextlib
import math
import time
from typing import TYPE_CHECKING, Any

from services.execution.binance_order_mapper import _f, _format_float

if TYPE_CHECKING:
    from services.binance_futures_client import BinanceFuturesClient
    from services.execution.binance_filters import FiltersCache

try:
    from services.execution_metrics import (
        EXECUTION_DUST_CLEANUP_TOTAL,
        EXECUTION_DUST_RESIDUAL_QTY,
        EXECUTION_EMERGENCY_FLATTEN_TOTAL,  # type: ignore
        EXECUTION_FORCE_FLAT_VERIFY_TOTAL,
    )
except Exception:
    EXECUTION_DUST_CLEANUP_TOTAL = EXECUTION_DUST_RESIDUAL_QTY = None  # type: ignore
    EXECUTION_EMERGENCY_FLATTEN_TOTAL = EXECUTION_FORCE_FLAT_VERIFY_TOTAL = None  # type: ignore


def _ms_now() -> int:
    try:
        from utils.time_utils import get_ny_time_millis
        return get_ny_time_millis()
    except Exception:
        return int(time.time() * 1000)


class EmergencyFlattenService:
    """Handles emergency position flatten and dust cleanup.

    All operations are best-effort and fail-open — a failed flatten still
    writes an event to orders:exec for operator visibility.
    """

    def __init__(
        self,
        *,
        position_mode: str = "oneway",
        dust_notional_usdt: float = 3.0,
        dust_margin_usdt: float = 1.0,
        dust_close_retries: int = 3,
        dust_verify_timeout_ms: int = 3000,
        dust_verify_poll_ms: int = 250,
        sl_working_type: str = "MARK_PRICE",
        write_event_fn: Any = None,
    ) -> None:
        self.position_mode = position_mode
        self.dust_notional_usdt = dust_notional_usdt
        self.dust_margin_usdt = dust_margin_usdt
        self.dust_close_retries = dust_close_retries
        self.dust_verify_timeout_ms = dust_verify_timeout_ms
        self.dust_verify_poll_ms = dust_verify_poll_ms
        self.sl_working_type = sl_working_type
        self._write_event_fn = write_event_fn

    def _write_event(self, fields: dict[str, Any]) -> None:
        if self._write_event_fn:
            self._write_event_fn(fields)

    # ------------------------------------------------------------------
    # Position info helpers
    # ------------------------------------------------------------------

    def get_position_info(
        self, symbol: str, *, client: "BinanceFuturesClient"
    ) -> dict[str, Any] | None:
        """Return position risk dict for symbol, or None if flat / error."""
        try:
            risks = client.get_position_risk() or []
            for pos in risks:
                if str((pos or {}).get("symbol") or "").upper() != symbol.upper():
                    continue
                amt = _f((pos or {}).get("positionAmt"), 0.0)
                if not math.isclose(float(amt), 0.0, abs_tol=1e-12):
                    return dict(pos)
            return None
        except Exception:
            return None

    def get_position_qty(self, symbol: str, *, client: "BinanceFuturesClient") -> float:
        """Return absolute position quantity, 0.0 if flat."""
        info = self.get_position_info(symbol, client=client)
        if info is None:
            return 0.0
        return abs(_f(info.get("positionAmt"), 0.0))

    def get_live_symbol_exposure(
        self, symbol: str, *, client: "BinanceFuturesClient"
    ) -> dict[str, Any]:
        """Return full position exposure metadata for symbol."""
        result: dict[str, Any] = {
            "symbol": symbol.upper(),
            "has_position": False,
            "position_amt": 0.0,
            "unrealized_pnl": 0.0,
            "mark_price": 0.0,
            "margin_used": 0.0,
        }
        try:
            risks = client.get_position_risk() or []
            for pos in risks:
                if str((pos or {}).get("symbol") or "").upper() != symbol.upper():
                    continue
                amt = _f(pos.get("positionAmt"), 0.0)
                if math.isclose(float(amt), 0.0, abs_tol=1e-12):
                    break
                result.update({
                    "has_position": True,
                    "position_amt": amt,
                    "unrealized_pnl": _f(pos.get("unRealizedProfit"), 0.0),
                    "mark_price": _f(pos.get("markPrice"), 0.0),
                    "margin_used": _f(pos.get("isolatedMargin") or pos.get("initialMargin"), 0.0),
                })
                break
        except Exception:
            pass
        return result

    def is_dust_position(self, snapshot: dict[str, Any]) -> bool:
        """Return True if position is too small to close normally (dust)."""
        amt = abs(_f(snapshot.get("positionAmt") or snapshot.get("position_amt"), 0.0))
        mark = _f(snapshot.get("markPrice") or snapshot.get("mark_price"), 0.0)
        margin = _f(snapshot.get("isolatedMargin") or snapshot.get("margin_used"), 0.0)
        notional = amt * mark
        if notional > 0 and notional < self.dust_notional_usdt:
            return True
        if margin > 0 and margin < self.dust_margin_usdt:
            return True
        return False

    # ------------------------------------------------------------------
    # Cancel helpers
    # ------------------------------------------------------------------

    def cancel_all_symbol_orders(
        self, symbol: str, *, client: "BinanceFuturesClient", sid: str = ""
    ) -> None:
        """Cancel all open plain and algo orders for symbol (best-effort)."""
        with contextlib.suppress(Exception):
            client.cancel_all_open_orders(symbol)  # type: ignore
        with contextlib.suppress(Exception):
            algos = client.get_open_algo_orders(symbol) or []
            for algo in algos:
                algo_id = algo.get("orderId") or algo.get("id")
                if algo_id:
                    with contextlib.suppress(Exception):
                        client.cancel_algo_order(algo_id)

    def cancel_algo_order(
        self, algo_order_id: Any, *, client: "BinanceFuturesClient"
    ) -> None:
        with contextlib.suppress(Exception):
            client.cancel_algo_order(algo_order_id)

    def cancel_plain_order(
        self,
        symbol: str,
        *,
        client: "BinanceFuturesClient",
        order_id: Any = None,
        client_order_id: str | None = None,
    ) -> None:
        with contextlib.suppress(Exception):
            if order_id:
                client.cancel_order(symbol, order_id=order_id)  # type: ignore
            elif client_order_id:
                client.cancel_order(symbol, orig_client_order_id=client_order_id)  # type: ignore

    # ------------------------------------------------------------------
    # Verify flat
    # ------------------------------------------------------------------

    def verify_symbol_flat(
        self,
        symbol: str,
        *,
        client: "BinanceFuturesClient",
        timeout_ms: int | None = None,
        poll_ms: int | None = None,
    ) -> bool:
        """Poll until position is confirmed flat or timeout expires."""
        _timeout = timeout_ms or self.dust_verify_timeout_ms
        _poll = poll_ms or self.dust_verify_poll_ms
        deadline = time.monotonic() + _timeout / 1000.0
        while time.monotonic() < deadline:
            qty = self.get_position_qty(symbol, client=client)
            if qty == 0.0 or math.isclose(qty, 0.0, abs_tol=1e-12):
                return True
            time.sleep(_poll / 1000.0)
        return False

    # ------------------------------------------------------------------
    # Emergency flatten
    # ------------------------------------------------------------------

    def emergency_flatten(
        self,
        *,
        sid: str,
        symbol: str,
        logical_side: str,
        qty: float,
        client: "BinanceFuturesClient",
        filters: "FiltersCache",
        reason: str = "emergency",
        r: Any = None,
    ) -> dict[str, Any]:
        """Cancel all protection orders then close position at MARKET (fail-safe).

        Emits execution event. Returns dict with flatten result.
        Always tries cancel-all first, then MARKET reduce-only close.
        """
        sym = symbol.upper()
        result: dict[str, Any] = {
            "sid": sid, "symbol": sym, "reason": reason,
            "cancel_ok": False, "flatten_ok": False,
        }
        with contextlib.suppress(Exception):
            if EXECUTION_EMERGENCY_FLATTEN_TOTAL is not None:
                EXECUTION_EMERGENCY_FLATTEN_TOTAL.labels(symbol=sym, reason=reason).inc()

        # 1. Cancel all orders
        try:
            self.cancel_all_symbol_orders(sym, client=client, sid=sid)
            result["cancel_ok"] = True
        except Exception as exc:
            result["cancel_error"] = str(exc)

        # 2. Determine close side
        close_side = "SELL" if logical_side == "LONG" else "BUY"

        # 3. Quantize qty
        try:
            sf = filters.get(sym)
            step = sf.step_size
            close_qty = math.floor(abs(qty) / step) * step if step > 0 else abs(qty)
            close_qty_str = _format_float(close_qty, step)
        except Exception:
            close_qty = abs(qty)
            close_qty_str = str(close_qty)

        # 4. Market close
        params: dict[str, Any] = {
            "symbol": sym,
            "side": close_side,
            "type": "MARKET",
            "quantity": close_qty_str,
            "reduceOnly": "true",
        }
        if self.position_mode == "hedge":
            params["positionSide"] = logical_side
            params.pop("reduceOnly", None)

        try:
            resp = client.place_order(**params)  # type: ignore
            result["flatten_ok"] = True
            result["flatten_order_id"] = resp.get("orderId") or resp.get("id")
        except Exception as exc:
            result["flatten_error"] = str(exc)

        self._write_event({
            "sid": sid,
            "symbol": sym,
            "action": "emergency_flatten",
            "event_type": "EMERGENCY_FLATTEN",
            "severity": "critical",
            "reason": reason,
            "cancel_ok": int(result["cancel_ok"]),
            "flatten_ok": int(result["flatten_ok"]),
            "close_qty": close_qty_str,
            "close_side": close_side,
        })
        return result

    def force_flatten_exact(
        self,
        *,
        sid: str,
        symbol: str,
        logical_side: str,
        client: "BinanceFuturesClient",
        filters: "FiltersCache",
        reason: str = "force_flatten",
    ) -> dict[str, Any]:
        """Fetch live position qty from exchange, cancel all orders, then close exact qty.

        More precise than emergency_flatten when caller does not have reliable qty.
        """
        sym = symbol.upper()
        exposure = self.get_live_symbol_exposure(sym, client=client)
        if not exposure.get("has_position"):
            return {"sid": sid, "symbol": sym, "action": "force_flatten_skipped", "reason": "no_position"}
        live_qty = abs(_f(exposure.get("position_amt"), 0.0))
        return self.emergency_flatten(
            sid=sid,
            symbol=sym,
            logical_side=logical_side,
            qty=live_qty,
            client=client,
            filters=filters,
            reason=reason,
        )
