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
from typing import Any, Dict, Optional

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
    order_id: Optional[int]
    client_order_id: Optional[str]
    algo_id: Optional[int]
    client_algo_id: Optional[str]
    raw: Dict[str, Any]

    def to_redis_fields(self) -> Dict[str, str]:
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
    def __init__(self) -> None:
        if redis is None:
            raise RuntimeError("redis-py is required")
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.stream_key = os.getenv("USER_STREAM_STREAM", "orders:user_stream")
        self.cache_prefix = (os.getenv("USER_STREAM_CACHE_PREFIX") or "orders:user_stream:").rstrip(":") + ":"
        self.cache_ttl_sec = int(os.getenv("USER_STREAM_CACHE_TTL_SEC", "86400"))
        self.keepalive_interval_sec = int(os.getenv("USER_STREAM_KEEPALIVE_SEC", "1800"))
        self.ws_base_url = (os.getenv("BINANCE_FSTREAM_BASE_URL") or "wss://fstream.binance.com").rstrip("/")
        self.r = redis.from_url(self.redis_url, decode_responses=True)
        self.client = BinanceFuturesClient.from_env(prefix=(os.getenv("BINANCE_USER_STREAM_PREFIX") or "BINANCE_"))
        self.listen_key: Optional[str] = None
        self._last_event_time_ms: int = 0

    def _cache_key(self, kind: str, ref: str) -> str:
        return f"{self.cache_prefix}{kind}:{ref}"

    def _normalise(self, payload: Dict[str, Any]) -> Optional[NormalizedUserStreamEvent]:
        e = str(payload.get("e") or "").upper()
        event_time_ms = int(payload.get("E") or 0)
        if e == "ORDER_TRADE_UPDATE":
            order = payload.get("o") or {}
            return NormalizedUserStreamEvent(
                event_type=e,
                event_time_ms=event_time_ms,
                symbol=str(order.get("s") or ""),
                side=str(order.get("S") or ""),
                status=str(order.get("X") or ""),
                execution_type=str(order.get("x") or ""),
                order_id=int(order.get("i")) if order.get("i") not in (None, "") else None,
                client_order_id=str(order.get("c") or "") or None,
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
                side=str(algo.get("S") or ""),
                status=str(algo.get("X") or algo.get("x") or ""),
                execution_type=str(algo.get("x") or payload.get("x") or ""),
                order_id=None,
                client_order_id=None,
                algo_id=int(algo.get("algoId")) if algo.get("algoId") not in (None, "") else None,
                client_algo_id=str(algo.get("clientAlgoId") or "") or None,
                raw=payload,
            )
        return None

    def _apply_event(self, event: NormalizedUserStreamEvent) -> bool:
        if int(event.event_time_ms) < int(self._last_event_time_ms):
            return False
        self._last_event_time_ms = int(event.event_time_ms)
        fields = event.to_redis_fields()
        fields["ingest_ts_ms"] = str(_ms_now())
        fields["ingest_mono_ms"] = str(_mono_ms())
        try:
            self.r.xadd(self.stream_key, fields)
            if event.client_order_id:
                self.r.set(self._cache_key("order", event.client_order_id), json.dumps({"event": fields, "order": event.raw.get("o") or {}}, ensure_ascii=False), ex=self.cache_ttl_sec)
            if event.client_algo_id:
                self.r.set(self._cache_key("algo", event.client_algo_id), json.dumps({"event": fields, "algo": event.raw.get("ao") or event.raw.get("a") or event.raw.get("o") or {}}, ensure_ascii=False), ex=self.cache_ttl_sec)
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
        self.listen_key = self.client.start_user_stream()
        if not self.listen_key:
            raise RuntimeError("empty listenKey")
        return self.listen_key

    def keepalive_listen_key(self) -> None:
        if not self.listen_key:
            return
        self.client.keepalive_user_stream(self.listen_key)

    def close_listen_key(self) -> None:
        if not self.listen_key:
            return
        try:
            self.client.close_user_stream(self.listen_key)
        finally:
            self.listen_key = None

    def run_forever(self) -> None:
        if websocket is None:
            raise RuntimeError("websocket-client package is required for BinanceUserStreamWorker")
        listen_key = self.start_listen_key()
        ws_url = f"{self.ws_base_url}/ws/{listen_key}"
        last_keepalive = time.time()
        while True:
            ws = websocket.create_connection(ws_url, timeout=30)
            try:
                while True:
                    if time.time() - last_keepalive >= self.keepalive_interval_sec:
                        self.keepalive_listen_key()
                        last_keepalive = time.time()
                    message = ws.recv()
                    if not message:
                        continue
                    self.handle_message(message)
            finally:
                try:
                    ws.close()
                except Exception:
                    pass
                # Binance disconnects after 24h, reconnect with a fresh or renewed listenKey.
                listen_key = self.start_listen_key()
                ws_url = f"{self.ws_base_url}/ws/{listen_key}"


def main() -> None:
    worker = BinanceUserStreamWorker()
    worker.run_forever()


if __name__ == "__main__":  # pragma: no cover
    main()
