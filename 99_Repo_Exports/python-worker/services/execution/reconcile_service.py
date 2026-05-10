"""reconcile_service.py — User-stream reconciliation for Binance executor.

Extracted from binance_executor.py (god-class decomposition).

Responsibilities:
- Lookup user-stream events by clientOrderId
- Submit plain/algo orders with reconcile fallback on ambiguous responses
- Reconcile entry by clientOrderId after 503/ambiguous error
- Reconcile protection by SID after protection placement error
- Attempt reconcile after any exception
"""
from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any

from services.execution.binance_order_mapper import _f

if TYPE_CHECKING:
    from services.binance_futures_client import BinanceFuturesClient


def _ms_now() -> int:
    try:
        from utils.time_utils import get_ny_time_millis
        return get_ny_time_millis()
    except Exception:
        return int(time.time() * 1000)


class ReconcileService:
    """Handles ambiguous order submission reconciliation via user-stream cache.

    When Binance returns 503/Unknown or a connection timeout, the order may
    or may not have been processed. We look up the clientOrderId in the
    user-stream cache (written by binance_user_stream_worker) to determine
    the true state without double-submitting.
    """

    def __init__(
        self,
        *,
        r: Any,
        user_stream_cache_prefix: str = "orders:user_stream:",
        user_stream_stream: str = "orders:user_stream",
        reconcile_enable: bool = True,
        exec_reconcile_on_503_unknown: bool = True,
        exec_reconcile_prefer_user_stream: bool = True,
        write_event_fn: Any = None,
        mark_pending_reconcile_fn: Any = None,
    ) -> None:
        self.r = r
        self.user_stream_cache_prefix = user_stream_cache_prefix.rstrip(":") + ":"
        self.user_stream_stream = user_stream_stream
        self.reconcile_enable = reconcile_enable
        self.exec_reconcile_on_503_unknown = exec_reconcile_on_503_unknown
        self.exec_reconcile_prefer_user_stream = exec_reconcile_prefer_user_stream
        self._write_event_fn = write_event_fn
        self._mark_pending_reconcile_fn = mark_pending_reconcile_fn

    def _write_event(self, fields: dict[str, Any]) -> None:
        if self._write_event_fn:
            self._write_event_fn(fields)

    def _mark_pending_reconcile(self, sid: str, *, symbol: str, action: str, reason: str) -> None:
        if self._mark_pending_reconcile_fn:
            self._mark_pending_reconcile_fn(sid, symbol=symbol, action=action, reason=reason)

    # ------------------------------------------------------------------
    # User-stream lookup
    # ------------------------------------------------------------------

    def cache_key(self, ref_kind: str, ref_value: str) -> str:
        return f"{self.user_stream_cache_prefix}{ref_kind}:{ref_value}"

    def lookup_user_stream_event(
        self,
        *,
        plain_client_id: str | None = None,
        algo_client_id: str | None = None,
    ) -> dict[str, Any]:
        """Look up a user-stream event by clientOrderId from Redis cache."""
        keys = []
        if plain_client_id:
            keys.append(self.cache_key("order", plain_client_id))
        if algo_client_id:
            keys.append(self.cache_key("algo", algo_client_id))
        for key in keys:
            try:
                raw = self.r.get(key)
                if raw:
                    doc = json.loads(raw)
                    if isinstance(doc, dict) and doc:
                        return doc
            except Exception:
                pass
        return {}

    def normalize_user_stream_plain_order(self, event_doc: dict[str, Any]) -> dict[str, Any]:
        """Normalize ORDER_TRADE_UPDATE event to our internal order ref format."""
        order = event_doc.get("order") or event_doc.get("o") or event_doc
        return {
            "order_id": str(order.get("i") or order.get("orderId") or ""),
            "client_order_id": str(order.get("c") or order.get("clientOrderId") or ""),
            "status": str(order.get("X") or order.get("status") or ""),
            "avg_price": _f(order.get("ap") or order.get("avgPrice") or order.get("L"), 0.0),
            "filled_qty": _f(order.get("z") or order.get("executedQty"), 0.0),
            "side": str(order.get("S") or order.get("side") or ""),
            "type": str(order.get("o") or order.get("type") or ""),
            "symbol": str(order.get("s") or order.get("symbol") or ""),
        }

    def normalize_user_stream_algo_order(self, event_doc: dict[str, Any]) -> dict[str, Any]:
        """Normalize ALGO_ORDER_UPDATE event to our internal order ref format."""
        order = event_doc.get("order") or event_doc.get("o") or event_doc
        return {
            "algo_order_id": str(order.get("agId") or order.get("orderId") or ""),
            "client_algo_order_id": str(order.get("algoClientOrderId") or order.get("clientAlgoOrderId") or ""),
            "status": str(order.get("s") or order.get("status") or ""),
            "side": str(order.get("S") or order.get("side") or ""),
            "symbol": str(order.get("sym") or order.get("symbol") or ""),
        }

    # ------------------------------------------------------------------
    # Submit with reconcile
    # ------------------------------------------------------------------

    def submit_plain_order_with_reconcile(
        self,
        *,
        sid: str,
        symbol: str,
        action: str,
        params: dict[str, Any],
        client: "BinanceFuturesClient",
    ) -> dict[str, Any]:
        """Place a plain order; on ambiguous error, check user-stream before raising.

        Returns exchange response dict on success.
        Raises BinanceAPIError or RuntimeError on fatal failure.
        Raises ambiguous_pending=True RuntimeError if still unconfirmed.
        """
        try:
            from services.binance_futures_client import BinanceAPIError
        except Exception:
            from binance_futures_client import BinanceAPIError  # type: ignore[no-redef]

        try:
            return client.place_order(**params)
        except Exception as exc:
            is_ambiguous = False
            if isinstance(exc, BinanceAPIError):
                p = exc.payload if isinstance(exc.payload, dict) else {}
                is_ambiguous = p.get("ambiguous") is True or (exc.status == 503 and "unknown" in (p.get("msg") or "").lower())

            if not (is_ambiguous and self.exec_reconcile_on_503_unknown):
                raise

            # Check user-stream for order confirmation
            cid = params.get("newClientOrderId")
            if cid and self.reconcile_enable:
                ev = self.lookup_user_stream_event(plain_client_id=cid)
                if ev:
                    norm = self.normalize_user_stream_plain_order(ev)
                    if norm.get("order_id"):
                        self._write_event({
                            "sid": sid, "symbol": symbol, "action": action,
                            "event_type": "RECONCILE_CONFIRMED_VIA_USER_STREAM",
                            "severity": "info",
                            "reconcile_client_order_id": cid,
                            "reconcile_order_id": norm.get("order_id"),
                        })
                        return {
                            "orderId": norm["order_id"],
                            "clientOrderId": cid,
                            "status": norm.get("status", "NEW"),
                            "_reconciled": True,
                        }

            self._mark_pending_reconcile(sid, symbol=symbol, action=action, reason="503_ambiguous")
            raise

    def submit_algo_order_with_reconcile(
        self,
        *,
        sid: str,
        symbol: str,
        action: str,
        params: dict[str, Any],
        client: "BinanceFuturesClient",
    ) -> dict[str, Any]:
        """Place an algo order; on ambiguous error, check user-stream before raising."""
        try:
            from services.binance_futures_client import BinanceAPIError
        except Exception:
            from binance_futures_client import BinanceAPIError  # type: ignore[no-redef]

        try:
            return client.place_algo_order(**params)
        except Exception as exc:
            is_ambiguous = False
            if isinstance(exc, BinanceAPIError):
                p = exc.payload if isinstance(exc.payload, dict) else {}
                is_ambiguous = p.get("ambiguous") is True or (exc.status == 503 and "unknown" in (p.get("msg") or "").lower())

            if not (is_ambiguous and self.exec_reconcile_on_503_unknown):
                raise

            cid = params.get("algoClientOrderId") or params.get("newClientOrderId")
            if cid and self.reconcile_enable:
                ev = self.lookup_user_stream_event(algo_client_id=cid)
                if ev:
                    norm = self.normalize_user_stream_algo_order(ev)
                    if norm.get("algo_order_id"):
                        self._write_event({
                            "sid": sid, "symbol": symbol, "action": action,
                            "event_type": "RECONCILE_ALGO_CONFIRMED_VIA_USER_STREAM",
                            "severity": "info",
                            "reconcile_client_algo_order_id": cid,
                            "reconcile_algo_order_id": norm.get("algo_order_id"),
                        })
                        return {
                            "orderId": norm["algo_order_id"],
                            "clientAlgoOrderId": cid,
                            "status": norm.get("status", "NEW"),
                            "_reconciled": True,
                        }

            self._mark_pending_reconcile(sid, symbol=symbol, action=action, reason="algo_503_ambiguous")
            raise

    # ------------------------------------------------------------------
    # Reconcile after exception
    # ------------------------------------------------------------------

    def attempt_reconcile_after_exception(
        self,
        *,
        sid: str,
        symbol: str,
        action: str,
        exc: Exception,
        entry_client_order_id: str | None = None,
    ) -> dict[str, Any] | None:
        """Try to determine true order state after an exception.

        Returns reconciled order dict if confirmed, None otherwise.
        """
        if not self.reconcile_enable or not entry_client_order_id:
            return None
        try:
            ev = self.lookup_user_stream_event(plain_client_id=entry_client_order_id)
            if ev:
                norm = self.normalize_user_stream_plain_order(ev)
                if norm.get("order_id"):
                    self._write_event({
                        "sid": sid, "symbol": symbol, "action": action,
                        "event_type": "RECONCILE_POST_EXCEPTION",
                        "severity": "warning",
                        "original_error": str(exc)[:200],
                        "reconciled_order_id": norm.get("order_id"),
                    })
                    return norm
        except Exception:
            pass
        return None
