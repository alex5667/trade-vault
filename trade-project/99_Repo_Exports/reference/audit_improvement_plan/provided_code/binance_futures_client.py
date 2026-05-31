from __future__ import annotations

"""Binance USDT-M Futures REST client (minimal, stdlib-only).

Why this exists
--------------
Your stack already emits trade signals and queues order commands in Redis.
For execution + post-trade accounting you need a deterministic, testable
connector to Binance.

This module intentionally:
  - uses only Python stdlib (urllib + hmac) to avoid dependency drift
  - implements signed requests (HMAC SHA256) with recvWindow + timestamp
  - is fail-open at call sites: raise explicit exceptions, do not swallow
  - does NOT log secrets

Two client classes are provided:
  - BinanceFuturesREST  — read-only, used by binance_account_reporter (P0)
  - BinanceFuturesClient — full execution client (P1 executor)

Both support account/risk read endpoints; BinanceFuturesClient additionally
has trading endpoints: post_order, delete_order, get_order, get_exchange_info,
get_mark_price, post_leverage, post_margin_type, sync_time.

Endpoints used:
  - GET  /fapi/v2/account
  - GET  /fapi/v2/positionRisk
  - GET  /fapi/v1/openOrders
  - GET  /fapi/v1/exchangeInfo       (symbol filters for quantisation)
  - GET  /fapi/v1/premiumIndex       (mark price for trailing arming)
  - POST /fapi/v1/order              (entry / SL / TP / trailing)
  - GET  /fapi/v1/order
  - DELETE /fapi/v1/order
  - POST /fapi/v1/leverage
  - POST /fapi/v1/marginType
"""

import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, Optional


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class BinanceHTTPError(RuntimeError):
    """Raised by BinanceFuturesREST (read-only client)."""
    def __init__(self, *, status: int, body: str, url: str):
        super().__init__(f"Binance HTTP {status}: {body[:400]}")
        self.status = int(status)
        self.body = str(body)
        self.url = str(url)


class BinanceAPIError(RuntimeError):
    """Raised by BinanceFuturesClient (execution client).

    Carries the full parsed response payload so callers can inspect
    Binance error codes:
      -1021  timestamp outside recvWindow  (transient, retry after sync_time)
      -1003  too many requests             (transient)
      -2019  margin insufficient           (fatal)
    """
    def __init__(self, status: int, payload: Any, message: str = ""):
        super().__init__(message or f"Binance API error status={status} payload={payload}")
        self.status = int(status)
        self.payload = payload  # parsed dict or {"_raw": ...} fallback


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------

def _now_ms() -> int:
    return int(time.time() * 1000)


def _hmac_sha256_hex(secret: str, msg: str) -> str:
    return hmac.new(secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).hexdigest()


def _safe_json_loads(raw: bytes) -> Any:
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return {"_raw": raw[:4096].decode("utf-8", errors="replace")}


def _encode_params_stable(params: Dict[str, Any]) -> str:
    """Stable query-string encoding: sorted keys, no None values."""
    items = []
    for k in sorted(params.keys()):
        v = params[k]
        if v is None:
            continue
        if isinstance(v, bool):
            v = "true" if v else "false"
        items.append((k, str(v)))
    return urllib.parse.urlencode(items)


# ---------------------------------------------------------------------------
# BinanceFuturesREST — read-only client (used by binance_account_reporter P0)
# ---------------------------------------------------------------------------

@dataclass
class BinanceFuturesREST:
    """Minimal READ-ONLY REST client for Binance USDT-M Futures.

    Used by binance_account_reporter to fetch account state hourly.
    Raises BinanceHTTPError on non-2xx responses.
    """

    api_key: str
    api_secret: str
    base_url: str = "https://fapi.binance.com"
    timeout_s: float = 8.0
    recv_window_ms: int = 5000

    def _request(
        self,
        *,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        signed: bool = False,
    ) -> Any:
        method_u = (method or "GET").upper()
        p: Dict[str, Any] = dict(params or {})
        if signed:
            p.setdefault("timestamp", _now_ms())
            p.setdefault("recvWindow", int(self.recv_window_ms))

            # Binance expects signature over query-string (same key order).
            # Keep it deterministic by sorting keys.
            qs = urllib.parse.urlencode(sorted(p.items()), doseq=True)
            sig = _hmac_sha256_hex(self.api_secret, qs)
            p["signature"] = sig

        qs2 = urllib.parse.urlencode(sorted(p.items()), doseq=True)
        url = self.base_url.rstrip("/") + path
        if qs2:
            url = url + "?" + qs2

        req = urllib.request.Request(url=url, method=method_u)
        req.add_header("X-MBX-APIKEY", self.api_key)

        try:
            with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                if not raw:
                    return {}
                try:
                    return json.loads(raw)
                except Exception:
                    return {"raw": raw}
        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8", errors="replace")  # type: ignore[call-arg]
            except Exception:
                body = str(e)
            raise BinanceHTTPError(status=int(getattr(e, "code", 0) or 0), body=body, url=url) from e

    # --- Read APIs ---

    def get_account(self) -> Dict[str, Any]:
        return self._request(method="GET", path="/fapi/v2/account", signed=True)

    def get_position_risk(self) -> Any:
        return self._request(method="GET", path="/fapi/v2/positionRisk", signed=True)

    def get_open_orders(self, *, symbol: Optional[str] = None) -> Any:
        params: Dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        return self._request(method="GET", path="/fapi/v1/openOrders", params=params, signed=True)

    @staticmethod
    def from_env(prefix: str = "BINANCE_") -> "BinanceFuturesREST":
        """Construct from ENV vars.

        Default prefix ``BINANCE_`` reads BINANCE_API_KEY / BINANCE_API_SECRET.
        Pass ``prefix="BINANCE_DEMO_"`` to read BINANCE_DEMO_API_KEY /
        BINANCE_DEMO_API_SECRET / BINANCE_DEMO_FUTURES_BASE_URL — used for
        the testnet / demo account without conflicting with real keys.
        """
        p = prefix.rstrip("_") + "_"  # normalise
        key = (os.getenv(f"{p}API_KEY") or "").strip()
        sec = (os.getenv(f"{p}API_SECRET") or "").strip()
        if not key or not sec:
            raise RuntimeError(
                f"{p}API_KEY / {p}API_SECRET are required"
            )
        base = (
            os.getenv(f"{p}FUTURES_BASE_URL")
            or os.getenv("BINANCE_FUTURES_BASE_URL")
            or "https://fapi.binance.com"
        ).strip()
        timeout_s = float(os.getenv("BINANCE_HTTP_TIMEOUT_S", "8.0"))
        return BinanceFuturesREST(
            api_key=key,
            api_secret=sec,
            base_url=base,
            timeout_s=timeout_s,
        )


# ---------------------------------------------------------------------------
# BinanceFuturesClient — full execution client (used by binance_executor P1)
# ---------------------------------------------------------------------------

@dataclass
class BinanceFuturesClient:
    """Full REST client for Binance USDT-M Futures execution (P1 executor).

    Extends account/risk reads (same as BinanceFuturesREST) with:
      - Trading: post_order, get_order, delete_order
      - Symbol info: get_exchange_info (LOT_SIZE / PRICE_FILTER for quantisation)
      - Mark price: get_mark_price (trailing stop arming)
      - Config: post_leverage, post_margin_type
      - Time sync: sync_time() for -1021 clock drift resolution

    Raises BinanceAPIError on non-2xx; callers inspect e.payload['code'].
    """

    api_key: str
    api_secret: str
    base_url: str = "https://fapi.binance.com"
    timeout_s: float = 8.0
    recv_window: int = 5000
    timestamp_offset_ms: int = 0  # adjusted by sync_time()

    @staticmethod
    def from_env(prefix: str = "BINANCE_") -> "BinanceFuturesClient":
        """Construct from ENV vars.

        Default prefix ``BINANCE_`` reads BINANCE_API_KEY / BINANCE_API_SECRET.
        Pass ``prefix="BINANCE_DEMO_"`` to read BINANCE_DEMO_API_KEY /
        BINANCE_DEMO_API_SECRET / BINANCE_DEMO_FUTURES_BASE_URL — used for
        the testnet / demo account (binance-executor, virtual trade routing).
        """
        p = prefix.rstrip("_") + "_"  # normalise: BINANCE_ or BINANCE_DEMO_
        key = (os.getenv(f"{p}API_KEY") or "").strip()
        sec = (os.getenv(f"{p}API_SECRET") or "").strip()
        if not key or not sec:
            raise RuntimeError(
                f"{p}API_KEY / {p}API_SECRET are required"
            )

        base = (
            os.getenv(f"{p}FUTURES_BASE_URL")
            or os.getenv("BINANCE_FUTURES_BASE_URL")
            or "https://fapi.binance.com"
        ).strip()
        timeout_s = float(os.getenv("BINANCE_HTTP_TIMEOUT_S", "8.0"))
        recv_window = int(os.getenv("BINANCE_RECV_WINDOW", "5000"))
        c = BinanceFuturesClient(
            api_key=key, api_secret=sec,
            base_url=base, timeout_s=timeout_s, recv_window=recv_window,
        )

        if (os.getenv("BINANCE_TIME_SYNC") or "").strip().lower() in {"1", "true", "yes", "on"}:
            c.sync_time()
        return c

    # --- core HTTP ---

    def _request(
        self, method: str, path: str,
        *, params: Optional[Dict[str, Any]] = None, signed: bool = False,
    ) -> Any:
        method = method.upper()
        params = dict(params or {})
        headers: Dict[str, str] = {
            "X-MBX-APIKEY": self.api_key,
            "User-Agent": "scanner_infra/binance_client_v2",
        }

        qs = ""
        body: Optional[bytes] = None

        if signed:
            # Apply server-time offset to minimise -1021 timestamp errors.
            params.setdefault("timestamp", _now_ms() + int(self.timestamp_offset_ms))
            params.setdefault("recvWindow", int(self.recv_window))
            qs = _encode_params_stable(params)
            sig = _hmac_sha256_hex(self.api_secret, qs)
            qs = qs + "&signature=" + sig
        else:
            qs = _encode_params_stable(params) if params else ""

        url = self.base_url.rstrip("/") + path
        if method in {"GET", "DELETE"}:
            if qs:
                url = url + ("?" if "?" not in url else "&") + qs
        else:
            # POST: send body as application/x-www-form-urlencoded (Binance accepts both)
            body = qs.encode("utf-8") if qs else b""

        req = urllib.request.Request(url, data=body, method=method)
        for k, v in headers.items():
            req.add_header(k, v)
        if method in {"POST", "PUT"}:
            req.add_header("Content-Type", "application/x-www-form-urlencoded")

        try:
            with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                raw = resp.read()
                if 200 <= resp.status < 300:
                    return _safe_json_loads(raw)
                raise BinanceAPIError(resp.status, _safe_json_loads(raw))
        except urllib.error.HTTPError as e:
            raw = e.read() if hasattr(e, "read") else b""
            payload = _safe_json_loads(raw) if raw else {"_error": str(e)}
            raise BinanceAPIError(getattr(e, "code", 0) or 0, payload)

    # --- public endpoints (no signature) ---

    def ping(self) -> Any:
        return self._request("GET", "/fapi/v1/ping")

    def get_server_time(self) -> int:
        j = self._request("GET", "/fapi/v1/time")
        return int(j.get("serverTime"))

    def sync_time(self) -> None:
        """Compute local→server offset to reduce -1021 signature rejections.

        Call once at startup or after a -1021 error to re-align clocks.
        """
        try:
            t0 = _now_ms()
            st = self.get_server_time()
            t1 = _now_ms()
            # serverTime corresponds to the mid-point of the local request round-trip.
            mid = (t0 + t1) // 2
            self.timestamp_offset_ms = int(st - mid)
        except Exception:
            # fail-open: leave offset at 0
            self.timestamp_offset_ms = 0

    def get_exchange_info(self) -> Any:
        """Full exchange info with symbol filters (LOT_SIZE, PRICE_FILTER, MIN_NOTIONAL).

        Results are cached by the FiltersCache in binance_executor to avoid
        repeated calls on every order.
        """
        return self._request("GET", "/fapi/v1/exchangeInfo")

    def get_premium_index(self, symbol: str) -> Any:
        """Premium index payload including markPrice (public, cheap).

        Used by trailing arming thread to poll mark price vs TP1.
        """
        return self._request("GET", "/fapi/v1/premiumIndex", params={"symbol": str(symbol).upper()})

    def get_mark_price(self, symbol: str) -> float:
        """Return current mark price for symbol; 0.0 on any error (fail-open)."""
        try:
            j = self.get_premium_index(symbol)
            return float(j.get("markPrice"))
        except Exception:
            return 0.0

    # --- signed account endpoints ---

    def get_account(self) -> Any:
        return self._request("GET", "/fapi/v2/account", signed=True)

    def get_position_risk(self) -> Any:
        return self._request("GET", "/fapi/v2/positionRisk", signed=True)

    def get_open_orders(self, symbol: Optional[str] = None) -> Any:
        """List open orders. Pass symbol to narrow the result."""
        params: Dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        return self._request("GET", "/fapi/v1/openOrders", params=params, signed=True)

    # --- signed trading endpoints ---

    def cancel_all_orders(self, symbol: str) -> Any:
        """Cancel all active regular and conditional orders for the symbol."""
        res_regular = self._request("DELETE", "/fapi/v1/allOpenOrders", params={"symbol": symbol}, signed=True)
        try:
            # New Binance Testnet requirement to clear Algo Orders explicitly
            self._request("DELETE", "/fapi/v1/algoOpenOrders", params={"symbol": symbol}, signed=True)
        except Exception:
            pass
        return res_regular

    def post_leverage(self, symbol: str, leverage: int) -> Any:
        """Set account leverage for a symbol. Idempotent — safe to call on each open."""
        return self._request(
            "POST", "/fapi/v1/leverage",
            params={"symbol": symbol, "leverage": int(leverage)}, signed=True,
        )

    def post_margin_type(self, symbol: str, margin_type: str) -> Any:
        """Set margin type (ISOLATED/CROSSED).

        Binance returns an error if the type is already set — callers should
        swallow that error (idempotent call pattern).
        """
        return self._request(
            "POST", "/fapi/v1/marginType",
            params={"symbol": symbol, "marginType": str(margin_type).upper()}, signed=True,
        )

    def post_order(self, params: Dict[str, Any]) -> Any:
        """Place an order.

        Required param keys: symbol, side, type, quantity (or closePosition=True for SL).
        Optional keys vary by order type (price, stopPrice, callbackRate,
        timeInForce, positionSide, reduceOnly, newClientOrderId, ...).
        """
        # Route conditional orders to the new Algo Order API
        algo_types = {"STOP_MARKET", "TAKE_PROFIT_MARKET", "STOP", "TAKE_PROFIT", "TRAILING_STOP_MARKET"}
        order_type = str(params.get("type", "")).upper()
        
        if order_type in algo_types:
            # Prepare algoOrder payload
            algo_params = dict(params)
            algo_params["algoType"] = "CONDITIONAL"
            
            # Binance Algo API expects 'triggerPrice' instead of 'stopPrice' for Stop/TP orders
            if "stopPrice" in algo_params and order_type in {"STOP_MARKET", "TAKE_PROFIT_MARKET", "STOP", "TAKE_PROFIT"}:
                algo_params["triggerPrice"] = algo_params.pop("stopPrice")
                
            return self._request("POST", "/fapi/v1/algoOrder", params=algo_params, signed=True)
            
        return self._request("POST", "/fapi/v1/order", params=params, signed=True)

    def get_order(
        self, symbol: str, *,
        order_id: Optional[int] = None,
        client_order_id: Optional[str] = None,
        is_algo: bool = False
    ) -> Any:
        """Query order status by orderId or origClientOrderId."""
        p: Dict[str, Any] = {"symbol": symbol}
        if order_id is not None:
            p["orderId"] = int(order_id)
        if client_order_id is not None:
            p["origClientOrderId"] = client_order_id
        
        path = "/fapi/v1/openAlgoOrders" if is_algo else "/fapi/v1/order"
        try:
            return self._request("GET", path, params=p, signed=True)
        except BinanceAPIError as e:
            # If not found and we didn't explicitly ask for algo, try algo (order might be conditional)
            if not is_algo and e.status in (400, 404):
                try:
                    return self._request("GET", "/fapi/v1/openAlgoOrders", params=p, signed=True)
                except Exception:
                    pass
            raise

    def delete_order(
        self, symbol: str, *,
        order_id: Optional[int] = None,
        client_order_id: Optional[str] = None,
        is_algo: bool = False
    ) -> Any:
        """Cancel order by orderId or origClientOrderId."""
        p: Dict[str, Any] = {"symbol": symbol}
        if order_id is not None:
            p["orderId"] = int(order_id)
        if client_order_id is not None:
            p["origClientOrderId"] = client_order_id
            
        path = "/fapi/v1/algoOrder" if is_algo else "/fapi/v1/order"
        try:
            return self._request("DELETE", path, params=p, signed=True)
        except BinanceAPIError as e:
            if not is_algo and e.status in (400, 404):
                try:
                    # Binance requires `algoId` for `DELETE /fapi/v1/algoOrder`. 
                    # If we only have `orderId` or `origClientOrderId` we pass it. The API will accept either one.
                    return self._request("DELETE", "/fapi/v1/algoOrder", params=p, signed=True)
                except Exception:
                    pass
            raise
