from __future__ import annotations

from utils.time_utils import get_ny_time_millis

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
  - GET  /fapi/v1/ticker/price       (contract/last price for trigger monitoring)
  - POST /fapi/v1/order              (entry / SL / TP / trailing)
  - GET  /fapi/v1/order
  - DELETE /fapi/v1/order
  - POST /fapi/v1/leverage
  - POST /fapi/v1/marginType
"""

import hashlib
import hmac
import json
import logging as _logging
import os
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any
import contextlib

try:
    from services.execution_metrics import (
        BINANCE_429_TOTAL,
        BINANCE_503_FAILURE_TOTAL,
        BINANCE_503_UNKNOWN_TOTAL,
        BINANCE_1008_TOTAL,
        BINANCE_API_ERRORS_TOTAL,
    )
except Exception:  # pragma: no cover
    try:
        from execution_metrics import (
            BINANCE_429_TOTAL,
            BINANCE_503_FAILURE_TOTAL,
            BINANCE_503_UNKNOWN_TOTAL,
            BINANCE_1008_TOTAL,
            BINANCE_API_ERRORS_TOTAL,
        )
    except Exception:  # pragma: no cover
        BINANCE_429_TOTAL = BINANCE_503_FAILURE_TOTAL = BINANCE_503_UNKNOWN_TOTAL = None  # type: ignore
        BINANCE_1008_TOTAL = BINANCE_API_ERRORS_TOTAL = None  # type: ignore


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

# Binance error codes — used for targeted handling without string matching.
# -4411: Account has not signed the TradFi-Perps agreement on Binance.
#        This is a fatal, non-retryable account-level restriction.
#        Fix: sign the agreement at Binance Futures UI for the affected symbol.
TRADFI_PERPS_NOT_SIGNED = -4411


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
      -4411  TradFi-Perps agreement not signed (fatal, account-level)
    """
    def __init__(self, status: int, payload: Any, message: str = ""):
        super().__init__(message or f"Binance API error status={status} payload={payload}")
        self.status = int(status)
        self.payload = payload  # parsed dict or {"_raw": ...} fallback


def is_tradfi_perps_error(exc: Exception) -> bool:
    """Return True when exc is a Binance -4411 TradFi-Perps agreement error.

    These errors require manual operator action (signing the agreement on Binance
    Futures UI) and must NOT be retried — they will always fail until the account
    agreement is signed.
    """
    if not isinstance(exc, BinanceAPIError):
        return False
    payload = exc.payload if isinstance(exc.payload, dict) else {}
    try:
        return int(payload.get("code") or 0) == TRADFI_PERPS_NOT_SIGNED
    except (ValueError, TypeError):
        return False


@dataclass(frozen=True)
class PlainOrderRef:
    """Reference to a plain /fapi/v1/order order.

    Stored separately from algo references because Binance uses a different
    identifier family for conditional orders after the Algo Service migration.
    """

    order_id: int | None
    client_order_id: str | None
    type: str
    side: str
    position_side: str | None = None


@dataclass(frozen=True)
class AlgoOrderRef:
    """Reference to a conditional /fapi/v1/algoOrder order."""

    algo_id: int | None
    client_algo_id: str | None
    type: str
    working_type: str
    trigger_price: float | None = None
    close_position: bool = False
    reduce_only: bool = False


# Canonical names used by the execution contract / plan docs.
BinancePlainOrderRef = PlainOrderRef
BinanceAlgoOrderRef = AlgoOrderRef


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------

def _now_ms() -> int:
    return get_ny_time_millis()


def _hmac_sha256_hex(secret: str, msg: str) -> str:
    return hmac.new(secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).hexdigest()


def _safe_json_loads(raw: bytes) -> Any:
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return {"_raw": raw[:4096].decode("utf-8", errors="replace")}


def _encode_params_stable(params: dict[str, Any]) -> str:
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
# Pre-flight order contract validators (P0 — execution safety)
# ---------------------------------------------------------------------------

def _truthy(v: Any) -> bool:
    """Return True for env-style truthy values (1/true/yes/on)."""
    return str(v).strip().lower() in {"1", "true", "yes", "on"}


def _float_or_none(v: Any) -> float | None:
    """Parse v as float; return None on missing / unparseable input."""
    if v in (None, ""):
        return None
    try:
        return float(v)
    except Exception:
        return None


def _require_positive(name: str, value: Any) -> float:
    """Raise ValueError if value is not a positive float."""
    fv = _float_or_none(value)
    if fv is None or fv <= 0:
        raise ValueError(f"{name}_must_be_positive")
    return fv


def _validate_working_type(value: Any) -> str:
    """Normalise and validate workingType; raise ValueError on unknown value."""
    wt = (value or "MARK_PRICE").strip().upper()
    if wt not in {"MARK_PRICE", "CONTRACT_PRICE"}:
        raise ValueError("invalid_workingType")
    return wt


def _validate_plain_order_contract(params: dict[str, Any], *, position_mode: str) -> None:
    """Validate plain order params before sending to /fapi/v1/order.

    Checks:
    - order type present
    - closePosition not allowed on plain orders
    - hedge mode requires explicit positionSide
    - hedge mode forbids reduceOnly
    - quantity > 0 required
    - LIMIT requires price and timeInForce
    """
    p = dict(params or {})
    pm = (position_mode or "oneway").strip().lower()
    ot = (p.get("type") or "").strip().upper()
    if not ot:
        raise ValueError("missing_order_type")
    if _truthy(p.get("closePosition")):
        raise ValueError("plain_order_closePosition_not_supported")
    if pm == "hedge" and (p.get("positionSide") or "").strip().upper() not in {"LONG", "SHORT"}:
        raise ValueError("positionSide_required_in_hedge")
    if pm == "hedge" and _truthy(p.get("reduceOnly")):
        raise ValueError("reduceOnly_forbidden_in_hedge_plain_order")
    qty = _float_or_none(p.get("quantity"))
    if qty is None or qty <= 0:
        raise ValueError("quantity_required")
    if ot == "LIMIT":
        _require_positive("price", p.get("price"))
        tif = (p.get("timeInForce") or "").strip().upper()
        if not tif:
            raise ValueError("limit_requires_timeInForce")


def _validate_algo_order_contract(params: dict[str, Any], *, position_mode: str) -> dict[str, Any]:
    """Validate and normalise algo order params before sending to /fapi/v1/algoOrder.

    Checks:
    - order type present
    - hedge mode requires explicit positionSide
    - closePosition and quantity are mutually exclusive
    - closePosition and reduceOnly are mutually exclusive
    - quantity > 0 required when closePosition is false
    - triggerPrice required for STOP/TP order types
    - TRAILING_STOP_MARKET: callbackRate in [0.1, 10.0], activatePrice > 0 if present
    Returns mutated copy with normalised workingType.
    """
    p = dict(params or {})
    pm = (position_mode or "oneway").strip().lower()
    ot = str(p.get("type") or p.get("algoType") or "").strip().upper()
    if not ot:
        raise ValueError("missing_order_type")
    if pm == "hedge" and (p.get("positionSide") or "").strip().upper() not in {"LONG", "SHORT"}:
        raise ValueError("positionSide_required_in_hedge")
    reduce_only = _truthy(p.get("reduceOnly"))
    close_position = _truthy(p.get("closePosition"))
    qty = _float_or_none(p.get("quantity"))
    # Normalise workingType (must be valid before returning)
    p["workingType"] = _validate_working_type(p.get("workingType") or "MARK_PRICE")
    if close_position and qty not in (None, 0.0):
        raise ValueError("algo_closePosition_incompatible_with_quantity")
    if close_position and reduce_only:
        raise ValueError("algo_closePosition_incompatible_with_reduceOnly")
    if not close_position and (qty is None or qty <= 0):
        raise ValueError("quantity_required")
    trig = _float_or_none(p.get("triggerPrice") if p.get("triggerPrice") not in (None, "") else p.get("stopPrice"))
    if ot in {"STOP_MARKET", "TAKE_PROFIT_MARKET", "STOP", "TAKE_PROFIT"} and (trig is None or trig <= 0):
        raise ValueError("triggerPrice_required")
    if ot == "TRAILING_STOP_MARKET":
        cb = _float_or_none(p.get("callbackRate"))
        if cb is None or cb < 0.1 or cb > 10.0:
            raise ValueError("callbackRate_out_of_range")
        ap = _float_or_none(p.get("activatePrice"))
        if ap is not None and ap <= 0:
            raise ValueError("activatePrice_must_be_positive")
    return p


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
        params: dict[str, Any] | None = None,
        signed: bool = False,
    ) -> Any:
        method_u = (method or "GET").upper()
        p: dict[str, Any] = dict(params or {})
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

    def get_account(self) -> dict[str, Any]:
        return self._request(method="GET", path="/fapi/v2/account", signed=True)

    def get_position_risk(self) -> Any:
        return self._request(method="GET", path="/fapi/v2/positionRisk", signed=True)

    def get_open_orders(self, *, symbol: str | None = None) -> Any:
        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        return self._request(method="GET", path="/fapi/v1/openOrders", params=params, signed=True)

    @staticmethod
    def from_env(prefix: str = "BINANCE_") -> BinanceFuturesREST:
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
# BinanceFuturesPublicREST — keyless public REST client (P0 derivatives context)
# ---------------------------------------------------------------------------

@dataclass
class BinanceFuturesPublicREST:
    """Minimal public REST client for Binance USDⓈ-M Futures.

    No API key is required. This client exists specifically for low-frequency
    context collectors (funding / premium index / open interest) so they do not
    depend on trading credentials.
    """

    base_url: str = "https://fapi.binance.com"
    timeout_s: float = 8.0
    _max_retries: int = 3
    _retry_base_delay_s: float = 1.0

    def _request(self, *, path: str, params: dict[str, Any] | None = None) -> Any:
        import logging as _logging
        _log = _logging.getLogger(__name__)
        qs = urllib.parse.urlencode(sorted((params or {}).items()), doseq=True)
        url = self.base_url.rstrip("/") + path
        if qs:
            url = url + "?" + qs
        req = urllib.request.Request(url=url, method="GET")
        req.add_header("User-Agent", "scanner_infra/binance_public_v1")
        last_err: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                    raw = resp.read()
                return _safe_json_loads(raw)
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                last_err = exc
                delay = self._retry_base_delay_s * (2 ** (attempt - 1))
                if attempt < self._max_retries:
                    _log.debug(
                        "BinanceFuturesPublicREST: transient network error on %s (attempt %d/%d): %s — retry in %.1fs",
                        path, attempt, self._max_retries, exc, delay,
                    )
                else:
                    _log.warning(
                        "BinanceFuturesPublicREST: transient network error on %s (attempt %d/%d): %s — retry in %.1fs",
                        path, attempt, self._max_retries, exc, delay,
                    )
                if attempt < self._max_retries:
                    time.sleep(delay)
        raise urllib.error.URLError(last_err)  # re-raise after all retries exhausted

    def get_premium_index(self, symbol: str) -> Any:
        return self._request(path="/fapi/v1/premiumIndex", params={"symbol": symbol.upper()})

    def get_open_interest(self, symbol: str) -> Any:
        return self._request(path="/fapi/v1/openInterest", params={"symbol": symbol.upper()})

    def get_funding_info(self, symbol: str) -> Any:
        # `/premiumIndex` already carries mark/index/lastFundingRate on Binance.
        return self.get_premium_index(symbol)


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
    _max_retries_429: int = 3
    _retry_base_delay_s: float = 5.0  # 429 backoff: 5s→10s→20s (was 1s→2s→4s)
    _mark_price_cache_ttl_s: float = 2.0  # deduplicate rapid mark-price polls

    def __post_init__(self) -> None:
        # Per-symbol mark price cache: {symbol: (price, expiry_mono)}
        self._mark_price_cache: dict[str, tuple] = {}

    @staticmethod
    def from_env(prefix: str = "BINANCE_") -> BinanceFuturesClient:
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
        recv_window = int(os.getenv("BINANCE_RECV_WINDOW_MS", os.getenv("BINANCE_RECV_WINDOW", "5000")))
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
        *, params: dict[str, Any] | None = None, signed: bool = False,
    ) -> Any:
        _log = _logging.getLogger(__name__)
        method = method.upper()
        orig_params = dict(params or {})
        headers: dict[str, str] = {
            "X-MBX-APIKEY": self.api_key,
            "User-Agent": "scanner_infra/binance_client_v2",
        }

        last_exc: Exception | None = None
        _synced_1021 = False  # single auto-resync guard for -1021
        for attempt in range(1, self._max_retries_429 + 1):
            # Re-build signed params each attempt so timestamp stays fresh.
            params = dict(orig_params)
            qs = ""
            body: bytes | None = None

            if signed:
                # Apply server-time offset to minimise -1021 timestamp errors.
                params["timestamp"] = _now_ms() + int(self.timestamp_offset_ms)
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
                status_code = int(getattr(e, "code", 0) or 0)
                raw = e.read() if hasattr(e, "read") else b""
                payload = _safe_json_loads(raw) if raw else {"_error": str(e)}
                self._observe_api_error(path=path, status=status_code, payload=payload)

                # Retry on HTTP 429 (rate limit) with exponential backoff.
                if status_code == 429 and attempt < self._max_retries_429:
                    # Respect Retry-After header if present; otherwise exponential backoff.
                    retry_after = None
                    try:
                        ra_hdr = e.headers.get("Retry-After") if hasattr(e, "headers") else None
                        if ra_hdr is not None:
                            retry_after = float(ra_hdr)
                    except Exception:
                        pass
                    delay = retry_after if retry_after and retry_after > 0 else self._retry_base_delay_s * (2 ** (attempt - 1))
                    _log.warning(
                        "BinanceFuturesClient: 429 rate-limited on %s %s (attempt %d/%d) — retry in %.1fs",
                        method, path, attempt, self._max_retries_429, delay,
                    )
                    last_exc = BinanceAPIError(status_code, payload)
                    time.sleep(delay)
                    continue

                # Auto-resync on -1021 (timestamp outside recvWindow):
                # re-align local clock offset via sync_time() and retry once.
                api_code = int(payload.get("code") or 0) if isinstance(payload, dict) else 0
                if api_code == -1021 and signed and not _synced_1021:
                    _synced_1021 = True
                    _log.warning(
                        "BinanceFuturesClient: -1021 timestamp drift on %s %s — running sync_time() and retrying",
                        method, path,
                    )
                    with contextlib.suppress(Exception):
                        self.sync_time()
                    continue

                raise BinanceAPIError(status_code, payload)
            except (urllib.error.URLError, TimeoutError, OSError) as e:
                # Classify the error: DNS / connection-refused errors are NOT
                # ambiguous — the request never left the client.  True transport
                # timeouts (connection established, response never arrived) ARE
                # ambiguous because the order may have been accepted server-side.
                root = e
                if isinstance(e, urllib.error.URLError) and e.reason:
                    root = e.reason
                is_dns = isinstance(root, socket.gaierror)
                is_conn_refused = isinstance(root, (ConnectionRefusedError, ConnectionResetError))
                ambiguous = not (is_dns or is_conn_refused)
                code = "dns_resolve_failed" if is_dns else (
                    "connection_refused" if is_conn_refused else "transport_timeout"
                )
                payload = {
                    "code": code,
                    "msg": str(e),
                    "ambiguous": ambiguous,
                }
                self._observe_transport_timeout(path=path)
                label = (
                    "Binance transport timeout / ambiguous request state"
                    if ambiguous
                    else f"Binance network error ({code})"
                )
                raise BinanceAPIError(0, payload, label)

        # All 429 retries exhausted — raise the last captured error.
        if last_exc is not None:
            raise last_exc
        raise BinanceAPIError(0, {"_error": "request_exhausted"})

    def _observe_api_error(self, *, path: str, status: int, payload: Any) -> None:
        endpoint = (path or "unknown")
        doc = payload if isinstance(payload, dict) else {}
        code = str(doc.get("code") or status or "unknown")
        msg = str(doc.get("msg") or doc.get("_error") or "").lower()
        try:
            if BINANCE_API_ERRORS_TOTAL is not None:
                BINANCE_API_ERRORS_TOTAL.labels(endpoint=endpoint, code=code).inc()
            if int(status) == 429 and BINANCE_429_TOTAL is not None:
                BINANCE_429_TOTAL.labels(endpoint=endpoint).inc()
            if str(code) == "-1008" and BINANCE_1008_TOTAL is not None:
                BINANCE_1008_TOTAL.labels(endpoint=endpoint).inc()
            if int(status) == 503:
                if "unknown" in msg:
                    if BINANCE_503_UNKNOWN_TOTAL is not None:
                        BINANCE_503_UNKNOWN_TOTAL.labels(endpoint=endpoint).inc()
                else:
                    if BINANCE_503_FAILURE_TOTAL is not None:
                        BINANCE_503_FAILURE_TOTAL.labels(endpoint=endpoint).inc()
        except Exception:
            pass

    def _observe_transport_timeout(self, *, path: str) -> None:
        try:
            if BINANCE_API_ERRORS_TOTAL is not None:
                BINANCE_API_ERRORS_TOTAL.labels(endpoint=(path or "unknown"), code="transport_timeout").inc()
        except Exception:
            pass

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
        return self._request("GET", "/fapi/v1/premiumIndex", params={"symbol": symbol.upper()})

    def get_mark_price(self, symbol: str) -> float:
        """Return current mark price for symbol; 0.0 on non-fatal error (fail-open).

        Re-raises BinanceAPIError with status 429 so caller loops can apply
        their own backoff instead of spinning on 0.0.  All other errors are
        swallowed (fail-open) to avoid blocking the executor hot-path.

        Results are cached for ``_mark_price_cache_ttl_s`` seconds to
        deduplicate concurrent requests for the same symbol.
        """
        sym = symbol.upper()
        now = time.monotonic()
        cached = self._mark_price_cache.get(sym)
        if cached is not None:
            price, expiry = cached
            if now < expiry:
                return price
        try:
            j = self.get_premium_index(sym)
            price = float(j.get("markPrice"))
            self._mark_price_cache[sym] = (price, now + self._mark_price_cache_ttl_s)
            return price
        except BinanceAPIError as e:
            if e.status == 429:
                raise  # propagate 429 to caller for backpressure
            return 0.0
        except Exception:
            return 0.0

    def get_ticker_price(self, symbol: str) -> float:
        """Return last / contract price for symbol; 0.0 on any error (fail-open)."""
        try:
            j = self._request("GET", "/fapi/v1/ticker/price", params={"symbol": symbol.upper()})
            return float(j.get("price"))
        except Exception:
            return 0.0

    def get_working_price(self, symbol: str, working_type: str) -> float:
        """Resolve the effective trigger price source for watchdog / trigger checks."""
        wt = (working_type or "MARK_PRICE").strip().upper()
        if wt == "CONTRACT_PRICE":
            return self.get_ticker_price(symbol)
        return self.get_mark_price(symbol)

    # --- user data stream management ---

    def start_user_stream(self) -> str:
        """Start or refresh a listenKey for the USDⓈ-M user stream."""
        j = self._request("POST", "/fapi/v1/listenKey")
        return (j.get("listenKey") or "")

    def keepalive_user_stream(self, listen_key: str) -> Any:
        return self._request("PUT", "/fapi/v1/listenKey", params={"listenKey": str(listen_key)})

    def close_user_stream(self, listen_key: str) -> Any:
        return self._request("DELETE", "/fapi/v1/listenKey", params={"listenKey": str(listen_key)})

    def is_ambiguous_execution_error(self, exc: Exception) -> bool:
        """Return True when the request outcome could be unknown and must be reconciled.

        Binance explicitly documents HTTP 503 "Unknown error" as execution-status
        unknown. We extend the same treatment to transport timeouts.
        """
        if not isinstance(exc, BinanceAPIError):
            return False
        payload = exc.payload if isinstance(exc.payload, dict) else {}
        msg = str(payload.get("msg") or payload.get("_error") or exc).lower()
        if exc.status == 503 and "unknown" in msg:
            return True
        if payload.get("ambiguous") is True:
            return True
        return False


    # --- signed account endpoints ---

    def get_account(self) -> Any:
        return self._request("GET", "/fapi/v2/account", signed=True)

    def get_position_risk(self) -> Any:
        return self._request("GET", "/fapi/v2/positionRisk", signed=True)

    def get_symbol_position_risk(
        self,
        symbol: str,
        *,
        position_side: str | None = None,
    ) -> dict[str, Any]:
        """Return the matching positionRisk row for a symbol.

        Executor close/flatten paths should size exits from live exchange qty,
        not from local state snapshots. This helper centralises the matching
        logic so all callers use the same source of truth.
        """
        target_symbol = (symbol or "").upper().strip()
        target_side = (position_side or "").upper().strip()
        for row in self.get_position_risk() or []:
            if (row.get("symbol") or "").upper().strip() != target_symbol:
                continue
            if target_side:
                row_side = (row.get("positionSide") or "").upper().strip()
                if row_side and row_side != target_side:
                    continue
            return dict(row)
        return {}

    def get_open_orders(self, symbol: str | None = None) -> Any:
        """List open plain orders. Pass symbol to narrow the result."""
        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        return self._request("GET", "/fapi/v1/openOrders", params=params, signed=True)

    def get_open_algo_orders(self, symbol: str | None = None) -> Any:
        """List open conditional orders routed via Algo Service.

        Binance exposes algo orders through a dedicated endpoint and a dedicated
        identifier family (`algoId` / `clientAlgoId`), so callers must not mix
        the result with plain order refs.
        """
        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        return self._request("GET", "/fapi/v1/openAlgoOrders", params=params, signed=True)

    # --- signed trading endpoints ---

    def cancel_all_algo_orders(self, symbol: str) -> dict[str, Any]:
        """Best-effort cancel of all open Algo Service orders for a symbol.

        First tries the bulk DELETE endpoint, then iterates the open algo order
        list to cancel each individually as a fallback. Both are attempted so
        transient partial failures don't leave orphaned conditional orders.
        """
        canceled = 0
        last_error = ""
        try:
            self._request("DELETE", "/fapi/v1/algoOpenOrders", params={"symbol": symbol}, signed=True)
        except Exception as exc:
            last_error = str(exc)
        try:
            for row in self.get_open_algo_orders(symbol) or []:
                algo_id = row.get("algoId")
                client_algo_id = row.get("clientAlgoId")
                try:
                    self.cancel_algo_order(
                        symbol,
                        algo_id=int(algo_id) if algo_id not in (None, "") else None,
                        client_algo_id=str(client_algo_id) if client_algo_id not in (None, "") else None,
                    )
                    canceled += 1
                except Exception as exc:
                    last_error = str(exc)
        except Exception as exc:
            if not last_error:
                last_error = str(exc)
        return {"symbol": symbol, "algo_canceled": canceled, "error": last_error}

    def cancel_all_orders(self, symbol: str) -> Any:
        """Cancel all active regular and conditional orders for the symbol."""
        res_regular = self._request("DELETE", "/fapi/v1/allOpenOrders", params={"symbol": symbol}, signed=True)
        try:
            # Use the helper so individual algo fallback cancels also run
            self.cancel_all_algo_orders(symbol)
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

    def position_mode(self) -> str:
        """Return the account's position mode from ENV (oneway or hedge)."""
        return (os.getenv("BINANCE_POSITION_MODE") or "oneway").strip().lower()

    def post_plain_order(self, params: dict[str, Any]) -> Any:
        """Submit a non-conditional order via /fapi/v1/order.

        Runs pre-flight contract validation before sending to Binance.
        """
        plain_params = dict(params or {})
        _validate_plain_order_contract(plain_params, position_mode=self.position_mode())
        return self._request("POST", "/fapi/v1/order", params=plain_params, signed=True)

    def post_algo_order(self, params: dict[str, Any]) -> Any:
        """Submit a conditional order via /fapi/v1/algoOrder.

        The Binance Algo API uses `triggerPrice` and `clientAlgoId`. For
        backward compatibility with older executor payloads we accept
        `stopPrice` and `newClientOrderId` and normalise them here.
        Runs pre-flight contract validation before sending to Binance.
        """
        algo_params = dict(params)
        if "stopPrice" in algo_params and "triggerPrice" not in algo_params:
            algo_params["triggerPrice"] = algo_params.pop("stopPrice")
        if "newClientOrderId" in algo_params and "clientAlgoId" not in algo_params:
            algo_params["clientAlgoId"] = algo_params.pop("newClientOrderId")
        algo_params.setdefault("workingType", algo_params.get("workingType") or "MARK_PRICE")
        algo_params["algoType"] = "CONDITIONAL"
        # Validate and normalise (validator also re-checks workingType)
        algo_params = _validate_algo_order_contract(algo_params, position_mode=self.position_mode())
        return self._request("POST", "/fapi/v1/algoOrder", params=algo_params, signed=True)

    def post_order(self, params: dict[str, Any]) -> Any:
        """Backward-compatible routing wrapper.

        New code should explicitly call post_plain_order()/post_algo_order().
        The wrapper remains to avoid breaking old call-sites while we migrate
        the executor and tests.
        """
        algo_types = {"STOP_MARKET", "TAKE_PROFIT_MARKET", "STOP", "TAKE_PROFIT", "TRAILING_STOP_MARKET"}
        order_type = (params.get("type", "")).upper()
        if order_type in algo_types:
            return self.post_algo_order(params)
        return self.post_plain_order(params)

    def query_plain_order(
        self, symbol: str, *,
        order_id: int | None = None,
        client_order_id: str | None = None,
    ) -> Any:
        p: dict[str, Any] = {"symbol": symbol}
        if order_id is not None:
            p["orderId"] = int(order_id)
        if client_order_id is not None:
            p["origClientOrderId"] = client_order_id
        return self._request("GET", "/fapi/v1/order", params=p, signed=True)

    def query_algo_order(
        self, symbol: str, *,
        algo_id: int | None = None,
        client_algo_id: str | None = None,
    ) -> Any:
        p: dict[str, Any] = {"symbol": symbol}
        if algo_id is not None:
            p["algoId"] = int(algo_id)
        if client_algo_id is not None:
            p["clientAlgoId"] = client_algo_id
        return self._request("GET", "/fapi/v1/algoOrder", params=p, signed=True)

    def get_order(
        self, symbol: str, *,
        order_id: int | None = None,
        client_order_id: str | None = None,
        is_algo: bool = False
    ) -> Any:
        """Backward-compatible query wrapper."""
        if is_algo:
            return self.query_algo_order(symbol, algo_id=order_id, client_algo_id=client_order_id)
        return self.query_plain_order(symbol, order_id=order_id, client_order_id=client_order_id)

    def cancel_plain_order(
        self, symbol: str, *,
        order_id: int | None = None,
        client_order_id: str | None = None,
    ) -> Any:
        p: dict[str, Any] = {"symbol": symbol}
        if order_id is not None:
            p["orderId"] = int(order_id)
        if client_order_id is not None:
            p["origClientOrderId"] = client_order_id
        return self._request("DELETE", "/fapi/v1/order", params=p, signed=True)

    def cancel_algo_order(
        self, symbol: str, *,
        algo_id: int | None = None,
        client_algo_id: str | None = None,
    ) -> Any:
        p: dict[str, Any] = {"symbol": symbol}
        if algo_id is not None:
            p["algoId"] = int(algo_id)
        if client_algo_id is not None:
            p["clientAlgoId"] = client_algo_id
        return self._request("DELETE", "/fapi/v1/algoOrder", params=p, signed=True)

    def replace_algo_order(
        self,
        symbol: str,
        *,
        cancel_algo_id: int | None = None,
        cancel_client_algo_id: str | None = None,
        new_params: dict[str, Any],
    ) -> dict[str, Any]:
        """Best-effort cancel+replace helper for untriggered algo orders.

        Binance Futures does not expose a native modify endpoint for algo orders
        across all order types, so the safe contract is explicit cancel→submit.
        Callers should only use this for orders that have not triggered yet.
        """
        cancel_res = self.cancel_algo_order(
            symbol, algo_id=cancel_algo_id, client_algo_id=cancel_client_algo_id
        )
        create_res = self.post_algo_order(dict(new_params or {}))
        return {"cancel": cancel_res, "create": create_res}

    def delete_order(
        self, symbol: str, *,
        order_id: int | None = None,
        client_order_id: str | None = None,
        is_algo: bool = False
    ) -> Any:
        """Backward-compatible cancel wrapper."""
        if is_algo:
            return self.cancel_algo_order(symbol, algo_id=order_id, client_algo_id=client_order_id)
        return self.cancel_plain_order(symbol, order_id=order_id, client_order_id=client_order_id)

    def reconcile_entry_by_client_id(self, symbol: str, client_order_id: str) -> Any:
        """Reconcile plain entry order by clientOrderId with REST fallbacks."""
        try:
            return self.query_plain_order(symbol, client_order_id=client_order_id)
        except Exception:
            pass
        try:
            for row in self.get_open_orders(symbol) or []:
                if str(row.get('clientOrderId') or row.get('origClientOrderId') or '') == str(client_order_id):
                    return row
        except Exception:
            pass
        raise RuntimeError(f'plain order not found for clientOrderId={client_order_id}')

    def _legacy__reconcile_protection_by_sid__dedupe_0(self, symbol: str, refs: dict[str, Any]) -> dict[str, Any]:
        """[Legacy] Resolve SL/TP/TRAIL algo refs using query endpoint first, open list second."""
        out: dict[str, Any] = {}
        sl_cid = (refs.get('sl_client_algo_id') or '').strip()
        if sl_cid:
            with contextlib.suppress(Exception):
                out['sl'] = self.query_algo_order(symbol, client_algo_id=sl_cid)
        for idx, cid in enumerate(list(refs.get('tp_client_algo_ids') or []), start=1):
            try:
                out[f'tp{idx}'] = self.query_algo_order(symbol, client_algo_id=str(cid))
            except Exception:
                continue
        trail_cid = str(refs.get('trail_client_algo_id') or refs.get('trail_client_id') or '').strip()
        if trail_cid:
            with contextlib.suppress(Exception):
                out['trail'] = self.query_algo_order(symbol, client_algo_id=trail_cid)
        if out:
            return out
        open_orders = self.get_open_algo_orders(symbol) or []
        for row in open_orders:
            cid = (row.get('clientAlgoId') or '')
            if cid and cid == sl_cid:
                out['sl'] = row
            elif trail_cid and cid == trail_cid:
                out['trail'] = row
        return out

    def _legacy__reconcile_protection_by_sid__sha1scan__dedupe_1(self, symbol: str, sid: str) -> dict[str, Any]:
        """[Legacy] sha1-token-based linear scan variant (superseded by inspect_protection_set)."""
        import hashlib as _hl
        token = _hl.sha1(str(sid).encode("utf-8")).hexdigest()[:8]
        matched: dict[str, Any] = {"sid": str(sid), "symbol": symbol.upper()}
        try:
            open_orders = self.get_open_algo_orders(symbol) or []
        except Exception:
            open_orders = []
        for order in open_orders:
            cid = (order.get("clientAlgoId") or "")
            if not cid or f"-{token}-" not in cid:
                continue
            if cid.endswith("-sl"):
                matched["sl"] = order
            elif cid.endswith("-trail"):
                matched["trail"] = order
            elif "-tp" in cid:
                matched.setdefault("tp", []).append(order)
        return matched

    # P4 canonical source-of-truth for protection reconcile via inspect_protection_set.
    def reconcile_protection_by_sid(self, symbol: str, sid: str) -> dict[str, Any]:
        """Scan open algo orders to reconstruct protection refs for a given sid.

        P12/P4 canonical: delegates to inspect_protection_set for a strict, verifiable
        contract of live protection orders keyed by deterministic clientAlgoId.
        Returns the richer view suitable for reconcile-first and strict verification.
        """
        return self.inspect_protection_set(
            symbol=symbol,
            sid=sid,
            expected_sl=True,
            expected_tps=[],
            trail_expected=False,
        )

    # ------------------------------------------------------------------
    # P12: deterministic clientAlgoId helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _sid_token(sid: str) -> str:
        """SHA1[:8] of SID for embedding in clientAlgoId."""
        return hashlib.sha1(str(sid).encode("utf-8")).hexdigest()[:8]

    @classmethod
    def _build_client_algo_id(cls, sid: str, tag: str) -> str:
        """Build a deterministic clientAlgoId ≤36 chars: <base>-<sha1[:8]>-<tag>.

        Ensures exact matching: executor and client agree on the exact ID of
        each protective order without storing a separate mapping.
        """
        token = cls._sid_token(sid)
        base = str(sid).replace(" ", "").replace(":", "-")
        base = base[: max(6, 36 - (len(tag) + len(token) + 2))]
        cid = f"{base}-{token}-{tag}"
        return cid[:36]

    # P4 canonical source-of-truth for protection inspection/reconcile.
    def inspect_protection_set(
        self,
        symbol: str,
        sid: str,
        expected_sl: bool = True,
        expected_tps: list[float] | None = None,
        trail_expected: bool = False,
        # P4 extended params for price-mismatch detection
        expect_sl: bool | None = None,
        expected_tp_count: int = 0,
        expected_sl_price: float | None = None,
        expected_tp_prices: list[float] | None = None,
    ) -> dict[str, Any]:
        """Strict view of live protective orders on-exchange, keyed by clientAlgoId.

        P4: extended with price-mismatch detection. Checks whether on-exchange
        trigger prices match expected values (within 1e-9 relative tolerance).
        Entries with wrong prices are added to the ``mismatched`` list even if
        the clientAlgoId is found, so stale SL/TP orders are correctly flagged.

        Returns:
            {
                "sid": str,
                "symbol": str,
                "by_client_algo_id": {cid: order_dict, ...},
                "sl": order_dict or None,
                "tp_by_index": {1: order_dict, 2: order_dict, ...},
                "trail": order_dict or None,
                "missing": ["sl", "tp1", ...],
                "mismatched": ["sl", "tp2", ...],  # P4: stale trigger prices
                "is_complete": bool,
                "expect_sl": bool,
                "expected_tp_count": int,
            }
        """
        # P4: resolve expect_sl — new param aliases the old expected_sl
        _expect_sl: bool = expect_sl if expect_sl is not None else expected_sl
        tps: list[float] = expected_tp_prices or expected_tps or []
        # P4: expected_tp_count overrides len(tps) when tps list is empty
        _tp_count = expected_tp_count if expected_tp_count > 0 else len(tps)

        token = self._sid_token(sid)
        result: dict[str, Any] = {
            "sid": str(sid),
            "symbol": symbol.upper(),
            "by_client_algo_id": {},
            "sl": None,
            "tp_by_index": {},
            "trail": None,
            "missing": [],
            "mismatched": [],  # P4: stale-price mismatch list
            "is_complete": False,
            "expect_sl": _expect_sl,
            "expected_tp_count": _tp_count,
        }
        try:
            open_orders = self.get_open_algo_orders(symbol) or []
        except Exception:
            open_orders = []

        # P4: inner helper — uses _float_or_none (no _f() dependency)
        def _price_matches(row: dict[str, Any], expected: float | None) -> bool:
            if expected in (None, 0, "", "0"):
                return True
            actual = _float_or_none(row.get("triggerPrice") or row.get("stopPrice") or row.get("activatePrice")) or 0.0
            if actual <= 0:
                return False
            tol = max(1e-9, abs(float(expected)) * 1e-9)
            return abs(actual - float(expected)) <= tol

        # Index all orders belonging to this SID by clientAlgoId
        for order in open_orders:
            cid = (order.get("clientAlgoId") or "")
            if not cid or f"-{token}-" not in cid:
                continue
            result["by_client_algo_id"][cid] = order
            if cid.endswith("-sl"):
                result["sl"] = order
            elif cid.endswith("-trail"):
                result["trail"] = order
            elif "-tp" in cid:
                # Extract TP index: ...-tp1, ...-tp2, etc.
                try:
                    idx = int(cid.rsplit("-tp", 1)[1])
                    result["tp_by_index"][idx] = order
                except (ValueError, IndexError):
                    result["tp_by_index"].setdefault(0, order)

        # Determine missing components
        missing: list[str] = []
        mismatched: list[str] = []  # P4: stale prices

        if _expect_sl:
            if result["sl"] is None:
                missing.append("sl")
            elif not _price_matches(result["sl"], expected_sl_price):
                # P4: SL found but trigger price is stale
                mismatched.append("sl")

        # TP completeness check (use tps list if available, else count)
        _tp_price_list = tps if tps else [None] * _tp_count
        for i, _tp_expected_price in enumerate(_tp_price_list, 1):
            if i not in result["tp_by_index"]:
                missing.append(f"tp{i}")
            elif not _price_matches(result["tp_by_index"][i], _tp_expected_price):
                # P4: TP found but trigger price is stale
                mismatched.append(f"tp{i}")

        if trail_expected and result["trail"] is None:
            missing.append("trail")

        result["missing"] = missing
        result["mismatched"] = mismatched  # P4
        result["is_complete"] = len(missing) == 0 and len(mismatched) == 0
        return result

    def _legacy__replace_untriggered_algo_order__dedupe_1(
        self,
        symbol: str,
        *,
        new_params: dict[str, Any],
        algo_id: int | None = None,
        client_algo_id: str | None = None,
    ) -> dict[str, Any]:
        """Cancel an untriggered algo order and submit a replacement.

        Binance Futures does not expose a native modify endpoint for all algo
        order types, so the safe contract is explicit cancel→submit.  Only
        orders in an untriggered state (NEW / PENDING / WORKING / OPEN /
        CREATED) can be replaced; triggered or terminal orders raise RuntimeError.
        """
        existing = self.query_algo_order(symbol, algo_id=algo_id, client_algo_id=client_algo_id)
        status = str(existing.get("status") or existing.get("X") or existing.get("state") or "NEW").upper()
        if status not in {"NEW", "PENDING", "WORKING", "OPEN", "CREATED"}:
            raise RuntimeError(f"algo_order_not_replaceable:{status}")
        self.cancel_algo_order(symbol, algo_id=algo_id, client_algo_id=client_algo_id)
        return self.post_algo_order(new_params)
