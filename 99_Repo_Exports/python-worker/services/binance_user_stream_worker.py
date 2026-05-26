from __future__ import annotations

from utils.time_utils import get_ny_time_millis

"""Binance USDⓈ-M User Data Stream worker.

Purpose
-------
This worker turns Binance WebSocket user-stream events into normalized Redis
artifacts that the executor can use for reconcile-first flows:

- a Redis stream `orders:user_stream` for audit / replay
- point-in-time cache keys for `clientOrderId` and `clientAlgoId`

The worker is intentionally conservative:
- listenKey lifecycle is handled explicitly (POST / PUT / DELETE)
- event ordering is based on Binance event time `E`
- stale / out-of-order updates are ignored in the point cache
- networking is optional at import time to keep local tests dependency-light
"""

import json
import os
import time
from dataclasses import dataclass
from typing import Any

from core.redis_keys import RedisStreams as RS
import contextlib

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

try:
    import websocket  # type: ignore
except Exception:  # pragma: no cover
    websocket = None  # type: ignore

try:
    from services.binance_futures_client import BinanceFuturesClient
except Exception:  # pragma: no cover
    from binance_futures_client import BinanceFuturesClient

try:
    from services.execution_metrics import (
        LISTENKEY_REFRESH_TOTAL,
        USER_STREAM_CONNECTED,
        USER_STREAM_LAST_EVENT_AGE_MS,
        USER_STREAM_RECONNECT_TOTAL,
    )
except Exception:  # pragma: no cover
    try:
        from execution_metrics import (
            LISTENKEY_REFRESH_TOTAL,
            USER_STREAM_CONNECTED,
            USER_STREAM_LAST_EVENT_AGE_MS,
            USER_STREAM_RECONNECT_TOTAL,
        )
    except Exception:  # pragma: no cover
        LISTENKEY_REFRESH_TOTAL = USER_STREAM_CONNECTED = USER_STREAM_LAST_EVENT_AGE_MS = USER_STREAM_RECONNECT_TOTAL = None  # type: ignore


def _ms_now() -> int:
    return get_ny_time_millis()


def _mono_ms() -> int:
    return int(time.monotonic() * 1000)


@dataclass(frozen=True)
class NormalizedUserStreamEvent:
    event_type: str
    event_time_ms: int
    symbol: str
    side: str
    status: str
    execution_type: str
    order_id: int | None
    client_order_id: str | None
    algo_id: int | None
    client_algo_id: str | None
    raw: dict[str, Any]

    def to_redis_fields(self) -> dict[str, str]:
        return {
            "event_type": str(self.event_type),
            "event_time_ms": str(self.event_time_ms),
            "symbol": str(self.symbol),
            "side": str(self.side),
            "status": str(self.status),
            "execution_type": str(self.execution_type),
            "order_id": "" if self.order_id is None else str(self.order_id),
            "client_order_id": self.client_order_id or "",
            "algo_id": "" if self.algo_id is None else str(self.algo_id),
            "client_algo_id": self.client_algo_id or "",
            "raw_json": json.dumps(self.raw, ensure_ascii=False, default=str),
        }


class BinanceUserStreamWorker:
    def __init__(self, *, redis_client: Any | None = None, client: BinanceFuturesClient | None = None) -> None:
        if redis_client is None and redis is None:
            raise RuntimeError("redis-py is required")
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.stream_key = os.getenv("USER_STREAM_STREAM", "orders:user_stream")
        self.cache_prefix = (os.getenv("USER_STREAM_CACHE_PREFIX") or "orders:user_stream:").rstrip(":") + ":"
        self.status_key = os.getenv("USER_STREAM_STATUS_KEY", "orders:user_stream:status")
        self.cache_ttl_sec = int(os.getenv("USER_STREAM_CACHE_TTL_SEC", "86400"))
        self.keepalive_interval_sec = int(os.getenv("USER_STREAM_KEEPALIVE_SEC", "1800"))
        self.ws_base_url = (os.getenv("BINANCE_FSTREAM_BASE_URL") or "wss://fstream.binance.com").rstrip("/")
        # Redis and HTTP client: injected InMemoryRedis/mock for tests, or real prod connections
        self.r = redis_client if redis_client is not None else redis.from_url(self.redis_url, decode_responses=True)  # type: ignore
        self.client = client if client is not None else BinanceFuturesClient.from_env(prefix=(os.getenv("BINANCE_USER_STREAM_PREFIX") or "BINANCE_"))
        self.listen_key: str | None = None
        self._last_event_time_ms: int = 0
        try:
            if USER_STREAM_CONNECTED is not None:
                USER_STREAM_CONNECTED.set(0)
        except Exception:
            pass

    def _cache_key(self, kind: str, ref: str) -> str:
        return f"{self.cache_prefix}{kind}:{ref}"

    def _state_key(self, sid: str) -> str:
        prefix = (os.getenv("ORDERS_STATE_KEY_PREFIX") or "orders:state:").rstrip(":") + ":"
        return f"{prefix}{sid}"

    def _load_state_doc(self, sid: str) -> dict[str, Any]:
        try:
            raw = self.r.get(self._state_key(sid))
            if not raw:
                return {}
            doc = json.loads(raw)
            return doc if isinstance(doc, dict) else {}
        except Exception:
            return {}

    def _status_doc(self) -> dict[str, Any]:
        """Read the current status doc from Redis (returns {} on any error)."""
        try:
            raw = self.r.get(self.status_key)
            doc = json.loads(raw) if raw else {}
            return doc if isinstance(doc, dict) else {}
        except Exception:
            return {}

    def _write_status(self, **patch: Any) -> None:
        """Merge *patch* fields into the existing status doc and persist.

        Read-merge-write keeps all previously written fields (e.g. listen_key
        set during start_listen_key is not lost when keepalive_listen_key fires).
        The `updated_at_ms` field is always refreshed.
        """
        doc = self._status_doc()
        doc.update({k: v for k, v in patch.items() if v is not None})
        doc['updated_at_ms'] = _ms_now()
        with contextlib.suppress(Exception):
            self.r.set(self.status_key, json.dumps(doc, ensure_ascii=False), ex=self.cache_ttl_sec)

    def _normalise(self, payload: dict[str, Any]) -> NormalizedUserStreamEvent | None:
        e = (payload.get("e") or "").upper()
        event_time_ms = int(payload.get("E") or 0)
        if e == "ORDER_TRADE_UPDATE":
            order = payload.get("o") or {}
            return NormalizedUserStreamEvent(
                event_type=e,
                event_time_ms=event_time_ms,
                symbol=(order.get("s") or ""),
                side=(order.get("S") or ""),
                status=(order.get("X") or ""),
                execution_type=(order.get("x") or ""),
                order_id=int(order.get("i")) if order.get("i") not in (None, "") else None,  # type: ignore
                client_order_id=(order.get("c") or "") or None,
                algo_id=None,
                client_algo_id=None,
                raw=payload,
            )
        if e == "ALGO_UPDATE":
            algo = payload.get("ao") or payload.get("a") or payload.get("o") or {}
            return NormalizedUserStreamEvent(
                event_type=e,
                event_time_ms=event_time_ms,
                symbol=str(algo.get("s") or payload.get("s") or ""),
                side=(algo.get("S") or ""),
                status=str(algo.get("X") or algo.get("x") or ""),
                execution_type=str(algo.get("x") or payload.get("x") or ""),
                order_id=None,
                client_order_id=None,
                algo_id=int(algo.get("algoId")) if algo.get("algoId") not in (None, "") else None,  # type: ignore
                client_algo_id=(algo.get("clientAlgoId") or "") or None,
                raw=payload,
            )
        return None

    def _apply_event(self, event: NormalizedUserStreamEvent) -> bool:
        if int(event.event_time_ms) < int(self._last_event_time_ms):
            return False
        self._last_event_time_ms = int(event.event_time_ms)
        try:
            if USER_STREAM_LAST_EVENT_AGE_MS is not None:
                USER_STREAM_LAST_EVENT_AGE_MS.set(max(0, _ms_now() - int(event.event_time_ms)))
        except Exception:
            pass
        fields = event.to_redis_fields()
        fields["ingest_ts_ms"] = str(_ms_now())
        fields["ingest_mono_ms"] = str(_mono_ms())
        try:
            self.r.xadd(self.stream_key, fields, maxlen=50000, approximate=True)

            exec_stream = os.getenv("EXEC_STREAM", RS.ORDERS_EXEC)

            if event.client_order_id:
                self.r.set(self._cache_key("order", event.client_order_id), json.dumps({"event": fields, "order": event.raw.get("o") or {}}, ensure_ascii=False), ex=self.cache_ttl_sec)
                try:
                    sid = self.r.get(f"orders:cid_to_sid:{event.client_order_id}")
                    if sid:
                        order_data = event.raw.get("o") or {}
                        state_doc = self._load_state_doc(str(sid))
                        exec_fields = {
                            "event_type": "EXCHANGE_FILL" if str(event.event_type).upper() == "ORDER_TRADE_UPDATE" else "EXCHANGE_ORDER_UPDATE",
                            "sid": sid,
                            "symbol": str(event.symbol),
                            "side": str(event.side),
                            "action": "reconcile",
                            "status": str(event.status),
                            "filled_qty": (order_data.get("z") or "0"),
                            "avg_price": (order_data.get("ap") or "0"),
                            "price": (order_data.get("ap") or "0"),
                            "kind": str(
                                state_doc.get("kind")
                                or state_doc.get("scenario")
                                or state_doc.get("signal_kind")
                                or "default"
                            ),
                            "client_order_id": str(event.client_order_id),
                            "binance_order_id": str(event.order_id) if event.order_id else "",
                            "ts_event_ms": str(event.event_time_ms),
                            "ts_ms": str(_ms_now()),
                            "mono_ms": str(_mono_ms())
                        }
                        self.r.xadd(exec_stream, exec_fields, maxlen=50000, approximate=True)
                except Exception:
                    pass

            if event.client_algo_id:
                self.r.set(self._cache_key("algo", event.client_algo_id), json.dumps({"event": fields, "algo": event.raw.get("ao") or event.raw.get("a") or event.raw.get("o") or {}}, ensure_ascii=False), ex=self.cache_ttl_sec)
                try:
                    sid = self.r.get(f"orders:cid_to_sid:{event.client_algo_id}")
                    if sid:
                        exec_fields = {
                            "event_type": "EXCHANGE_ALGO_UPDATE",
                            "sid": sid,
                            "symbol": str(event.symbol),
                            "action": "reconcile",
                            "status": str(event.status),
                            "client_algo_id": str(event.client_algo_id),
                            "binance_order_id": str(event.algo_id) if event.algo_id else "",
                            "ts_event_ms": str(event.event_time_ms),
                            "ts_ms": str(_ms_now()),
                            "mono_ms": str(_mono_ms())
                        }
                        self.r.xadd(exec_stream, exec_fields, maxlen=50000, approximate=True)
                except Exception:
                    pass
            # Update richer status contract required by ExecutionBootstrapSupervisor
            self._write_status(
                status='stream_live',
                connected=True,
                listen_key=self.listen_key or '',
                last_event_ms=int(event.event_time_ms),
                last_ingest_ms=_ms_now(),
            )
            return True
        except Exception:
            return False

    def handle_message(self, raw_message: str) -> bool:
        payload = json.loads(raw_message)
        event = self._normalise(payload)
        if event is None:
            return False
        return self._apply_event(event)

    def start_listen_key(self) -> str:
        try:
            self.listen_key = self.client.start_user_stream()
            if not self.listen_key:
                raise RuntimeError("empty listenKey")
            # Write rich status immediately so supervisor can see listen_key is live
            self._write_status(
                status='listen_key_started',
                connected=False,
                listen_key=self.listen_key,
                listen_key_started_ms=_ms_now(),
                last_keepalive_ms=_ms_now(),
            )
            try:
                if LISTENKEY_REFRESH_TOTAL is not None:
                    LISTENKEY_REFRESH_TOTAL.labels(op="start", result="ok").inc()
                if USER_STREAM_CONNECTED is not None:
                    USER_STREAM_CONNECTED.set(1)
            except Exception:
                pass
            return self.listen_key
        except Exception:
            try:
                if LISTENKEY_REFRESH_TOTAL is not None:
                    LISTENKEY_REFRESH_TOTAL.labels(op="start", result="error").inc()
                if USER_STREAM_CONNECTED is not None:
                    USER_STREAM_CONNECTED.set(0)
            except Exception:
                pass
            raise

    def keepalive_listen_key(self) -> None:
        if not self.listen_key:
            return
        try:
            self.client.keepalive_user_stream(self.listen_key)
            # Refresh keepalive timestamp so supervisor sees continued liveness
            self._write_status(
                status='listen_key_keepalive',
                listen_key=self.listen_key,
                last_keepalive_ms=_ms_now(),
            )
            if LISTENKEY_REFRESH_TOTAL is not None:
                LISTENKEY_REFRESH_TOTAL.labels(op="keepalive", result="ok").inc()
        except Exception:
            try:
                if LISTENKEY_REFRESH_TOTAL is not None:
                    LISTENKEY_REFRESH_TOTAL.labels(op="keepalive", result="error").inc()
            except Exception:
                pass
            raise

    def close_listen_key(self) -> None:
        if not self.listen_key:
            return
        try:
            self.client.close_user_stream(self.listen_key)
            if LISTENKEY_REFRESH_TOTAL is not None:
                LISTENKEY_REFRESH_TOTAL.labels(op="close", result="ok").inc()
        except Exception:
            try:
                if LISTENKEY_REFRESH_TOTAL is not None:
                    LISTENKEY_REFRESH_TOTAL.labels(op="close", result="error").inc()
            except Exception:
                pass
            raise
        finally:
            # Mark stream as closed in status doc for supervisor gate
            self._write_status(status='closed', connected=False, listen_key='')
            self.listen_key = None
            try:
                if USER_STREAM_CONNECTED is not None:
                    USER_STREAM_CONNECTED.set(0)
            except Exception:
                pass

    def run_forever(self) -> None:
        if websocket is None:
            raise RuntimeError("websocket-client package is required for BinanceUserStreamWorker")

        # Backoff parameters for reconnect loops
        _backoff_min = float(os.getenv("USER_STREAM_BACKOFF_MIN_SEC", "2"))
        _backoff_max = float(os.getenv("USER_STREAM_BACKOFF_MAX_SEC", "60"))
        _ws_timeout = int(os.getenv("USER_STREAM_WS_TIMEOUT_SEC", "10"))
        # Transient WS exceptions that should trigger reconnect, not crash
        _transient_exc: tuple[type[BaseException], ...] = (OSError, ConnectionError)
        _timeout_exc: tuple[type[BaseException], ...] = ()
        try:
            # Import websocket exceptions if available
            _transient_exc = (
                OSError,
                ConnectionError,
                websocket.WebSocketConnectionClosedException,
                websocket.WebSocketException,
            )
            _timeout_exc = (websocket.WebSocketTimeoutException,)
        except AttributeError:
            pass

        listen_key = self.start_listen_key()
        ws_url = f"{self.ws_base_url}/ws/{listen_key}"
        reconnect_backoff = _backoff_min

        while True:
            ws = None
            try:
                ws = websocket.create_connection(ws_url, timeout=_ws_timeout)
                reconnect_backoff = _backoff_min  # reset on successful connect
                last_keepalive = time.time()
                # Announce ws_connected so supervisor can allow bootstrap grace window
                self._write_status(
                    status='ws_connected',
                    connected=True,
                    listen_key=listen_key,
                    ws_connected_ms=_ms_now(),
                )
                while True:
                    if time.time() - last_keepalive >= self.keepalive_interval_sec:
                        self.keepalive_listen_key()
                        last_keepalive = time.time()
                    try:
                        message = ws.recv()
                    except _timeout_exc:
                        # Expected when no messages arrive within 10s. Keep stream locally fresh.
                        self._write_status(last_keepalive_ms=_ms_now())
                        continue
                    except _transient_exc as exc:
                        # Disconnect — break inner loop to reconnect, do not crash
                        self._write_status(
                            status='reconnecting',
                            connected=False,
                            listen_key=listen_key,
                            ws_disconnected_ms=_ms_now(),
                        )
                        try:
                            if USER_STREAM_RECONNECT_TOTAL is not None:
                                reason = "timeout" if "timeout" in str(exc).lower() else "disconnect"
                                USER_STREAM_RECONNECT_TOTAL.labels(reason=reason).inc()
                        except Exception:
                            pass
                        break
                    if not message:
                        continue
                    self.handle_message(message)

            except _transient_exc:
                # Connection failed entirely — back off before retrying
                self._write_status(
                    status='reconnecting',
                    connected=False,
                    listen_key=listen_key,
                    ws_disconnected_ms=_ms_now(),
                )
            finally:
                try:
                    if ws is not None:
                        ws.close()
                except Exception:
                    pass
                try:
                    if USER_STREAM_CONNECTED is not None:
                        USER_STREAM_CONNECTED.set(0)
                except Exception:
                    pass

            # Exponential backoff before reconnect
            time.sleep(reconnect_backoff)
            reconnect_backoff = min(reconnect_backoff * 2, _backoff_max)

            # Binance disconnects after 24h; get a fresh or renewed listenKey
            try:
                listen_key = self.start_listen_key()
                ws_url = f"{self.ws_base_url}/ws/{listen_key}"
            except Exception:
                # If listenKey renewal fails, wait and retry outer loop
                self._write_status(status='listen_key_error', connected=False)
                time.sleep(reconnect_backoff)
                reconnect_backoff = min(reconnect_backoff * 2, _backoff_max)


def main() -> None:
    worker = BinanceUserStreamWorker()
    worker.run_forever()


if __name__ == "__main__":  # pragma: no cover
    main()
