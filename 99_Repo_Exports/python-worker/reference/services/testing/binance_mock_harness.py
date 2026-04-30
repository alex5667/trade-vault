from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""Deterministic Binance mock harness for executor/user-stream integration tests.

This module intentionally uses only the stdlib so it can run inside the same
minimal test environment as the production bundle.
"""

from collections import defaultdict, deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock, Thread
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple
import json
import time
import urllib.parse


def _ms_now() -> int:
    return get_ny_time_millis()


# ---------------------------------------------------------------------------
# InMemoryRedis — thread-safe in-memory Redis stub for tests
# ---------------------------------------------------------------------------

class InMemoryPipeline:
    """Minimal pipeline stub that buffers ops and executes them in order."""

    def __init__(self, redis: "InMemoryRedis") -> None:
        self.redis = redis
        self.ops: List[Tuple[str, tuple, dict]] = []

    def set(self, *args, **kwargs):
        self.ops.append(("set", args, kwargs))
        return self

    def sadd(self, *args, **kwargs):
        self.ops.append(("sadd", args, kwargs))
        return self

    def xadd(self, *args, **kwargs):
        self.ops.append(("xadd", args, kwargs))
        return self

    def execute(self):
        out = []
        for name, args, kwargs in self.ops:
            out.append(getattr(self.redis, name)(*args, **kwargs))
        self.ops.clear()
        return out


class InMemoryRedis:
    """Thread-safe in-memory Redis stub covering list/set/kv/stream primitives.

    Covers every Redis primitive used by BinanceExecutor and
    BinanceUserStreamWorker:
      kv     — get / set / delete
      lists  — lpush / rpush / lrange / lrem / brpoplpush
      sets   — sadd / sismember
      streams — xadd / xrange
    """

    def __init__(self) -> None:
        self.kv: Dict[str, str] = {}
        self.lists: Dict[str, List[str]] = defaultdict(list)
        self.streams: Dict[str, List[Tuple[str, Dict[str, str]]]] = defaultdict(list)
        self.sets: Dict[str, set] = defaultdict(set)
        self._lock = Lock()
        self._stream_seq = 0

    def pipeline(self):
        return InMemoryPipeline(self)

    def get(self, key: str):
        with self._lock:
            return self.kv.get(key)

    def set(self, key: str, value: str, ex: Optional[int] = None):
        with self._lock:
            self.kv[key] = value
            return True

    def delete(self, key: str):
        with self._lock:
            existed = key in self.kv
            self.kv.pop(key, None)
            self.lists.pop(key, None)
            self.streams.pop(key, None)
            self.sets.pop(key, None)
            return 1 if existed else 0

    def sadd(self, key: str, *members: str):
        with self._lock:
            before = len(self.sets[key])
            self.sets[key].update(str(m) for m in members)
            return len(self.sets[key]) - before

    def sismember(self, key: str, member: str):
        with self._lock:
            return str(member) in self.sets.get(key, set())

    def lpush(self, key: str, *values: str):
        with self._lock:
            for v in values:
                self.lists[key].insert(0, str(v))
            return len(self.lists[key])

    def rpush(self, key: str, *values: str):
        with self._lock:
            for v in values:
                self.lists[key].append(str(v))
            return len(self.lists[key])

    def lrange(self, key: str, start: int, stop: int):
        with self._lock:
            items = list(self.lists.get(key, []))
        if stop == -1:
            stop = len(items) - 1
        return items[start: stop + 1]

    def lrem(self, key: str, count: int, value: str):
        with self._lock:
            items = self.lists.get(key, [])
            removed = 0
            new_items = []
            for item in items:
                if item == value and (count <= 0 or removed < count):
                    removed += 1
                    continue
                new_items.append(item)
            self.lists[key] = new_items
            return removed

    def brpoplpush(self, source: str, destination: str, timeout: int = 0):
        """Pop from right of source, push to left of destination.

        Blocks up to `timeout` seconds; returns None on idle timeout.
        timeout=0 means: return immediately if empty.
        """
        deadline = time.time() + max(0, timeout)
        while True:
            with self._lock:
                if self.lists.get(source):
                    value = self.lists[source].pop()
                    self.lists[destination].insert(0, value)
                    return value
            if timeout <= 0 or time.time() >= deadline:
                return None
            time.sleep(0.01)

    def xadd(self, key: str, fields: Dict[str, Any], maxlen: Optional[int] = None, approximate: bool = True):
        with self._lock:
            self._stream_seq += 1
            msg_id = f"{_ms_now()}-{self._stream_seq}"
            payload = {str(k): str(v) for k, v in dict(fields).items()}
            self.streams[key].append((msg_id, payload))
            if maxlen and len(self.streams[key]) > int(maxlen):
                self.streams[key] = self.streams[key][-int(maxlen):]
            return msg_id

    def xrange(self, key: str):
        with self._lock:
            return list(self.streams.get(key, []))


# ---------------------------------------------------------------------------
# Scripted order state models
# ---------------------------------------------------------------------------

@dataclass
class ScriptedOrder:
    """Tracks a plain order inside the mock server with an optional query script."""

    order_id: int
    client_order_id: str
    symbol: str
    side: str
    order_type: str
    quantity: float
    reduce_only: bool = False
    status: str = "NEW"
    avg_price: float = 0.0
    executed_qty: float = 0.0
    # Script: list of query-response snapshots consumed in sequence
    script: List[Dict[str, Any]] = field(default_factory=list)
    query_index: int = 0
    # Delta tracking to avoid double-applying position changes
    applied_qty: float = 0.0


@dataclass
class ScriptedAlgoOrder:
    """Tracks an algo (STOP_MARKET / TAKE_PROFIT_MARKET) order inside the mock."""

    algo_id: int
    client_algo_id: str
    symbol: str
    side: str
    order_type: str
    quantity: float
    trigger_price: float
    working_type: str = "MARK_PRICE"
    price: float = 0.0
    reduce_only: bool = False
    close_position: bool = False
    status: str = "NEW"


# ---------------------------------------------------------------------------
# MockBinanceState — REST dispatch + user-stream event queue
# ---------------------------------------------------------------------------

class MockBinanceState:
    """Central state for the deterministic Binance mock server.

    Supports:
      - GET/POST/DELETE /fapi/v1/order (plain orders)
      - GET/POST/DELETE /fapi/v1/algoOrder (algo orders)
      - GET/POST /fapi/v1/listenKey
      - GET /fapi/v1/exchangeInfo, /fapi/v2/account, /fapi/v2/positionRisk, etc.
      - scripted POST responses (error injection, partial-fill sequences)
      - user-stream event queue (consumed by test assertions)
    """

    def __init__(self) -> None:
        self.available_balance = 5000.0
        self.mark_prices: Dict[str, float] = {"BTCUSDT": 100.0}
        self.contract_prices: Dict[str, float] = {"BTCUSDT": 100.0}
        self.positions: Dict[str, float] = defaultdict(float)
        self.leverage: Dict[str, int] = defaultdict(lambda: 10)
        self.margin_type: Dict[str, str] = defaultdict(lambda: "ISOLATED")
        self.listen_key = "mock-listen-key"
        self.next_order_id = 1000
        self.next_algo_id = 5000
        # Full request log for assertion in tests
        self.request_log: List[Dict[str, Any]] = []
        # User-stream events emitted by order fills / algo triggers
        self.user_stream_events: Deque[str] = deque()
        self.plain_orders: Dict[int, ScriptedOrder] = {}
        self.plain_order_by_client: Dict[str, int] = {}
        self.algo_orders: Dict[int, ScriptedAlgoOrder] = {}
        self.algo_order_by_client: Dict[str, int] = {}
        # Per-clientOrderId scripted behaviour (error injection, fill sequences)
        self.order_scripts: Dict[str, Dict[str, Any]] = {}
        # Per-clientAlgoId error injection
        self.algo_errors: Dict[str, Dict[str, Any]] = {}
        # Per-(method, path) HTTP fault injection queue (entries consumed one-by-one)
        self.http_faults: Dict[Tuple[str, str], Deque[Dict[str, Any]]] = defaultdict(deque)
        # Live user-stream sinks: callables that receive raw JSON strings synchronously
        self.user_stream_sinks: List[Callable[[str], None]] = []
        self._lock = Lock()

    # --- Test control helpers ---

    def set_mark_price(self, symbol: str, price: float) -> None:
        self.mark_prices[str(symbol).upper()] = float(price)

    def set_contract_price(self, symbol: str, price: float) -> None:
        self.contract_prices[str(symbol).upper()] = float(price)

    def set_plain_order_script(
        self
        client_order_id: str
        *
        query_sequence: Optional[List[Dict[str, Any]]] = None
        post_error: Optional[Dict[str, Any]] = None
        create_on_post_error: bool = True
    ) -> None:
        """Script the REST behaviour for client_order_id.

        query_sequence: list of snapshots returned in order on successive GET calls.
        post_error:     if set, POST returns this {status, payload} instead of 200.
        create_on_post_error: if False, the order is NOT persisted despite POST error
                              (simulates a truly unknown submission).
        """
        self.order_scripts[str(client_order_id)] = {
            "query_sequence": list(query_sequence or [])
            "post_error": dict(post_error or {}) if post_error else None
            "create_on_post_error": bool(create_on_post_error)
        }

    def set_algo_error(self, client_algo_id: str, *, status: int, payload: Dict[str, Any]) -> None:
        """Make POST /fapi/v1/algoOrder return an error for client_algo_id."""
        self.algo_errors[str(client_algo_id)] = {"status": int(status), "payload": dict(payload)}

    def pop_user_stream_messages(self) -> List[str]:
        """Drain and return all queued user-stream event payloads (raw JSON strings)."""
        out = []
        while self.user_stream_events:
            out.append(self.user_stream_events.popleft())
        return out

    # --- Live user-stream bridge helpers ---

    def register_user_stream_sink(self, sink: Callable[[str], None]) -> None:
        """Register a callable that receives every user-stream event as raw JSON.

        Called synchronously from _publish_user_stream_payload so tests can
        bridge events directly into a BinanceUserStreamWorker.handle_message().
        """
        self.user_stream_sinks.append(sink)

    def set_http_fault(
        self
        method: str
        path: str
        *
        status: int
        payload: Dict[str, Any]
        repeat: int = 1
    ) -> None:
        """Queue `repeat` HTTP fault responses for (method, path).

        The next `repeat` matching requests will receive (status, payload)
        instead of the normal mock response.  Faults are consumed one-by-one
        in FIFO order and removed when exhausted.
        """
        key = (str(method or 'GET').upper(), str(path or '').split('?', 1)[0])
        for _ in range(max(1, int(repeat))):
            self.http_faults[key].append({"status": int(status), "payload": dict(payload)})

    def _publish_user_stream_payload(self, payload: Dict[str, Any]) -> None:
        """Append event to queue and synchronously notify all registered sinks."""
        raw = json.dumps(payload, ensure_ascii=False)
        self.user_stream_events.append(raw)
        for sink in list(self.user_stream_sinks):
            try:
                sink(raw)
            except Exception:
                continue

    # --- Internal event emitters ---

    def _emit_order_update(
        self, order: ScriptedOrder, execution_type: str, *, event_time_ms: Optional[int] = None
    ) -> None:
        payload = {
            "e": "ORDER_TRADE_UPDATE"
            "E": int(event_time_ms or _ms_now())
            "o": {
                "s": order.symbol
                "S": order.side
                "X": order.status
                "x": execution_type
                "i": order.order_id
                "c": order.client_order_id
                "q": f"{order.quantity}"
                "z": f"{order.executed_qty}"
                "ap": f"{order.avg_price}"
            }
        }
        self._publish_user_stream_payload(payload)

    def _emit_algo_update(
        self, algo: ScriptedAlgoOrder, execution_type: str, *, event_time_ms: Optional[int] = None
    ) -> None:
        payload = {
            "e": "ALGO_UPDATE"
            "E": int(event_time_ms or _ms_now())
            "ao": {
                "s": algo.symbol
                "S": algo.side
                "X": algo.status
                "x": execution_type
                "algoId": algo.algo_id
                "clientAlgoId": algo.client_algo_id
            }
        }
        self._publish_user_stream_payload(payload)

    def inject_plain_order_update(
        self
        *
        client_order_id: Optional[str] = None
        order_id: Optional[int] = None
        status: str
        executed_qty: Optional[float] = None
        avg_price: Optional[float] = None
        execution_type: str = "TRADE"
        event_time_ms: Optional[int] = None
    ) -> Dict[str, Any]:
        """Manually inject a plain order update event into the user-stream.

        Looks up an existing ScriptedOrder by order_id or client_order_id
        applies state mutation, and publishes the event to all registered sinks.
        Returns a REST-like snapshot dict for assertion convenience.
        """
        order: Optional[ScriptedOrder] = None
        if order_id is not None:
            order = self.plain_orders.get(int(order_id))
        elif client_order_id:
            oid = self.plain_order_by_client.get(str(client_order_id))
            order = self.plain_orders.get(int(oid or -1))
        if order is None:
            raise KeyError(f"unknown plain order: client_order_id={client_order_id} order_id={order_id}")
        if executed_qty is not None:
            order.executed_qty = float(executed_qty)
        if avg_price is not None:
            order.avg_price = float(avg_price)
        order.status = str(status or order.status).upper()
        delta = max(0.0, float(order.executed_qty) - float(order.applied_qty))
        if delta > 0:
            self._apply_position_delta(order.symbol, order.side, delta, order.reduce_only)
            order.applied_qty += delta
        self._emit_order_update(order, execution_type=execution_type, event_time_ms=event_time_ms)
        return {
            "symbol": order.symbol
            "orderId": order.order_id
            "clientOrderId": order.client_order_id
            "status": order.status
            "executedQty": f"{order.executed_qty}"
            "avgPrice": f"{order.avg_price}"
        }

    def inject_algo_update(
        self
        *
        client_algo_id: Optional[str] = None
        algo_id: Optional[int] = None
        status: str
        execution_type: str = "TRIGGERED"
        event_time_ms: Optional[int] = None
    ) -> Dict[str, Any]:
        """Manually inject an algo order update event into the user-stream.

        Returns a REST-like snapshot dict for assertion convenience.
        """
        algo: Optional[ScriptedAlgoOrder] = None
        if algo_id is not None:
            algo = self.algo_orders.get(int(algo_id))
        elif client_algo_id:
            aid = self.algo_order_by_client.get(str(client_algo_id))
            algo = self.algo_orders.get(int(aid or -1))
        if algo is None:
            raise KeyError(f"unknown algo order: client_algo_id={client_algo_id} algo_id={algo_id}")
        algo.status = str(status or algo.status).upper()
        self._emit_algo_update(algo, execution_type=execution_type, event_time_ms=event_time_ms)
        return {
            "symbol": algo.symbol
            "algoId": algo.algo_id
            "clientAlgoId": algo.client_algo_id
            "status": algo.status
        }

    def _record(self, method: str, path: str, params: Dict[str, Any]) -> None:
        self.request_log.append({"method": method, "path": path, "params": dict(params)})

    def _json_response(self, handler: BaseHTTPRequestHandler, code: int, payload: Any) -> None:
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        handler.send_response(code)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(raw)))
        handler.end_headers()
        handler.wfile.write(raw)

    # --- Exchange model helpers ---

    def _exchange_info(self) -> Dict[str, Any]:
        """Minimal exchangeInfo for BTCUSDT — sufficient for FiltersCache.get()."""
        return {
            "symbols": [
                {
                    "symbol": "BTCUSDT"
                    "pricePrecision": 2
                    "quantityPrecision": 3
                    "filters": [
                        {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"}
                        {"filterType": "PRICE_FILTER", "tickSize": "0.10"}
                        {"filterType": "MIN_NOTIONAL", "notional": "5"}
                    ]
                }
            ]
        }

    def _position_risk(self, symbol: str) -> List[Dict[str, Any]]:
        amt = float(self.positions.get(symbol, 0.0))
        return [{
            "symbol": symbol
            "positionAmt": f"{amt}"
            "isolatedMargin": "50.0" if amt else "0.0"
            "initialMargin": "50.0" if amt else "0.0"
            "leverage": str(self.leverage[symbol])
            "positionSide": "BOTH"
        }]

    def _apply_position_delta(self, symbol: str, side: str, delta_qty: float, reduce_only: bool) -> None:
        current = float(self.positions.get(symbol, 0.0))
        if side == "BUY":
            next_qty = current + delta_qty
        else:
            next_qty = current - delta_qty
        if reduce_only:
            if current > 0:
                next_qty = max(0.0, next_qty)
            elif current < 0:
                next_qty = min(0.0, next_qty)
        self.positions[symbol] = next_qty

    def _new_order(self, params: Dict[str, Any]) -> ScriptedOrder:
        self.next_order_id += 1
        order_id = self.next_order_id
        client_id = str(params.get("newClientOrderId") or f"cid-{order_id}")
        symbol = str(params.get("symbol") or "").upper()
        side = str(params.get("side") or "BUY").upper()
        order_type = str(params.get("type") or "MARKET").upper()
        quantity = float(params.get("quantity") or 0.0)
        reduce_only = str(params.get("reduceOnly") or "false").lower() == "true"
        script_cfg = self.order_scripts.get(client_id, {})
        query_sequence = [dict(x) for x in script_cfg.get("query_sequence") or []]
        order = ScriptedOrder(
            order_id=order_id
            client_order_id=client_id
            symbol=symbol
            side=side
            order_type=order_type
            quantity=quantity
            reduce_only=reduce_only
            script=query_sequence
        )
        # For MARKET orders without a script, auto-fill immediately
        if order_type == "MARKET" and not query_sequence:
            fill_price = float(self.contract_prices.get(symbol, self.mark_prices.get(symbol, 100.0)))
            order.status = "FILLED"
            order.executed_qty = quantity
            order.avg_price = fill_price
            order.applied_qty = quantity
            self._apply_position_delta(symbol, side, quantity, reduce_only)
            self._emit_order_update(order, execution_type="TRADE")
        self.plain_orders[order_id] = order
        self.plain_order_by_client[client_id] = order_id
        return order

    def _new_algo_order(self, params: Dict[str, Any]) -> ScriptedAlgoOrder:
        self.next_algo_id += 1
        algo_id = self.next_algo_id
        client_id = str(params.get("clientAlgoId") or f"algo-{algo_id}")
        symbol = str(params.get("symbol") or "").upper()
        algo = ScriptedAlgoOrder(
            algo_id=algo_id
            client_algo_id=client_id
            symbol=symbol
            side=str(params.get("side") or "SELL").upper()
            order_type=str(params.get("type") or "STOP_MARKET").upper()
            quantity=float(params.get("quantity") or 0.0)
            trigger_price=float(params.get("triggerPrice") or 0.0)
            working_type=str(params.get("workingType") or "MARK_PRICE").upper()
            price=float(params.get("price") or 0.0)
            reduce_only=str(params.get("reduceOnly") or "false").lower() == "true"
            close_position=str(params.get("closePosition") or "false").lower() == "true"
        )
        self.algo_orders[algo_id] = algo
        self.algo_order_by_client[client_id] = algo_id
        self._emit_algo_update(algo, execution_type="NEW")
        return algo

    def _query_plain_order(self, order: ScriptedOrder) -> Dict[str, Any]:
        """Advance the scripted query sequence and return the current snapshot."""
        seq = order.script
        if seq:
            idx = min(order.query_index, len(seq) - 1)
            snapshot = seq[idx]
            order.query_index += 1
            order.status = str(snapshot.get("status") or order.status).upper()
            if "executedQty" in snapshot:
                order.executed_qty = float(snapshot.get("executedQty") or 0.0)
            if "avgPrice" in snapshot:
                order.avg_price = float(snapshot.get("avgPrice") or 0.0)
            # Apply incremental position delta and emit user-stream update
            delta = max(0.0, order.executed_qty - order.applied_qty)
            if delta > 0:
                self._apply_position_delta(order.symbol, order.side, delta, order.reduce_only)
                order.applied_qty += delta
                self._emit_order_update(order, execution_type="TRADE")
            elif snapshot.get("emit", False):
                self._emit_order_update(order, execution_type=str(snapshot.get("executionType") or "TRADE"))
        return {
            "symbol": order.symbol
            "orderId": order.order_id
            "clientOrderId": order.client_order_id
            "status": order.status
            "executedQty": f"{order.executed_qty}"
            "avgPrice": f"{order.avg_price}"
            "side": order.side
            "type": order.order_type
        }

    # --- Main dispatch ---

    def handle(self, handler: BaseHTTPRequestHandler, method: str, path: str, params: Dict[str, Any]) -> None:
        """Route an HTTP request to the appropriate mock handler."""
        path = path.split("?", 1)[0]
        with self._lock:
            self._record(method, path, params)
            # Consume HTTP fault injection (FIFO): checked before any real handler
            fault_key = (str(method or "GET").upper(), path)
            fault_queue = self.http_faults.get(fault_key)
            if fault_queue:
                fault = fault_queue.popleft()
                return self._json_response(handler, int(fault["status"]), fault["payload"])

            # Health / time endpoints
            if path == "/fapi/v1/ping":
                return self._json_response(handler, 200, {})
            if path == "/fapi/v1/time":
                return self._json_response(handler, 200, {"serverTime": _ms_now()})
            if path == "/fapi/v1/exchangeInfo":
                return self._json_response(handler, 200, self._exchange_info())

            # Market data
            if path == "/fapi/v1/premiumIndex":
                symbol = str(params.get("symbol") or "BTCUSDT").upper()
                return self._json_response(handler, 200, {"symbol": symbol, "markPrice": f"{self.mark_prices.get(symbol, 100.0)}"})
            if path == "/fapi/v1/ticker/price":
                symbol = str(params.get("symbol") or "BTCUSDT").upper()
                return self._json_response(handler, 200, {"symbol": symbol, "price": f"{self.contract_prices.get(symbol, 100.0)}"})

            # Account
            if path == "/fapi/v2/account":
                return self._json_response(handler, 200, {"availableBalance": f"{self.available_balance}"})
            if path == "/fapi/v2/positionRisk":
                symbol = str(params.get("symbol") or "BTCUSDT").upper()
                return self._json_response(handler, 200, self._position_risk(symbol))

            # Open orders
            if path == "/fapi/v1/openOrders":
                symbol = str(params.get("symbol") or "BTCUSDT").upper()
                items = [
                    {
                        "symbol": o.symbol
                        "orderId": o.order_id
                        "clientOrderId": o.client_order_id
                        "status": o.status
                        "side": o.side
                    }
                    for o in self.plain_orders.values()
                    if o.symbol == symbol and o.status not in {"CANCELED", "FILLED", "REJECTED", "EXPIRED"}
                ]
                return self._json_response(handler, 200, items)
            if path == "/fapi/v1/openAlgoOrders":
                symbol = str(params.get("symbol") or "BTCUSDT").upper()
                items = [
                    {
                        "symbol": a.symbol
                        "algoId": a.algo_id
                        "clientAlgoId": a.client_algo_id
                        "status": a.status
                        "side": a.side
                        "positionSide": "BOTH"
                    }
                    for a in self.algo_orders.values()
                    if a.symbol == symbol and a.status not in {"CANCELED", "FILLED", "REJECTED", "EXPIRED"}
                ]
                return self._json_response(handler, 200, items)

            # Leverage / margin
            if path == "/fapi/v1/leverage" and method == "POST":
                symbol = str(params.get("symbol") or "BTCUSDT").upper()
                self.leverage[symbol] = int(float(params.get("leverage") or 10))
                return self._json_response(handler, 200, {"symbol": symbol, "leverage": self.leverage[symbol]})
            if path == "/fapi/v1/marginType" and method == "POST":
                symbol = str(params.get("symbol") or "BTCUSDT").upper()
                self.margin_type[symbol] = str(params.get("marginType") or "ISOLATED").upper()
                return self._json_response(handler, 200, {"symbol": symbol, "marginType": self.margin_type[symbol]})

            # ListenKey lifecycle
            if path == "/fapi/v1/listenKey":
                if method == "POST":
                    return self._json_response(handler, 200, {"listenKey": self.listen_key})
                return self._json_response(handler, 200, {})

            # Plain order CRUD
            if path == "/fapi/v1/order" and method == "POST":
                order = self._new_order(params)
                script_cfg = self.order_scripts.get(order.client_order_id, {})
                post_error = script_cfg.get("post_error")
                if post_error:
                    if not script_cfg.get("create_on_post_error", True):
                        self.plain_orders.pop(order.order_id, None)
                        self.plain_order_by_client.pop(order.client_order_id, None)
                    return self._json_response(handler, int(post_error["status"]), post_error["payload"])
                return self._json_response(handler, 200, {
                    "symbol": order.symbol
                    "orderId": order.order_id
                    "clientOrderId": order.client_order_id
                    "status": order.status
                    "executedQty": f"{order.executed_qty}"
                    "avgPrice": f"{order.avg_price}"
                })

            if path == "/fapi/v1/algoOrder" and method == "POST":
                client_algo_id = str(params.get("clientAlgoId") or "")
                if client_algo_id in self.algo_errors:
                    err = self.algo_errors[client_algo_id]
                    return self._json_response(handler, err["status"], err["payload"])
                algo = self._new_algo_order(params)
                return self._json_response(handler, 200, {
                    "symbol": algo.symbol
                    "algoId": algo.algo_id
                    "clientAlgoId": algo.client_algo_id
                    "status": algo.status
                })

            if path == "/fapi/v1/order" and method == "GET":
                order = None
                if params.get("orderId"):
                    order = self.plain_orders.get(int(params["orderId"]))
                elif params.get("origClientOrderId"):
                    order_id = self.plain_order_by_client.get(str(params["origClientOrderId"]))
                    order = self.plain_orders.get(order_id or -1)
                if order is None:
                    return self._json_response(handler, 404, {"code": -2013, "msg": "Order does not exist"})
                return self._json_response(handler, 200, self._query_plain_order(order))

            if path == "/fapi/v1/algoOrder" and method == "GET":
                algo = None
                if params.get("algoId"):
                    algo = self.algo_orders.get(int(params["algoId"]))
                elif params.get("clientAlgoId"):
                    algo_id = self.algo_order_by_client.get(str(params["clientAlgoId"]))
                    algo = self.algo_orders.get(algo_id or -1)
                if algo is None:
                    return self._json_response(handler, 404, {"code": -2013, "msg": "Algo order does not exist"})
                return self._json_response(handler, 200, {
                    "symbol": algo.symbol
                    "algoId": algo.algo_id
                    "clientAlgoId": algo.client_algo_id
                    "status": algo.status
                })

            # Plain order DELETE
            if path == "/fapi/v1/order" and method == "DELETE":
                order = None
                if params.get("orderId"):
                    order = self.plain_orders.get(int(params["orderId"]))
                elif params.get("origClientOrderId"):
                    order_id = self.plain_order_by_client.get(str(params["origClientOrderId"]))
                    order = self.plain_orders.get(order_id or -1)
                if order is None:
                    return self._json_response(handler, 404, {"code": -2011, "msg": "Unknown order"})
                order.status = "CANCELED"
                self._emit_order_update(order, execution_type="CANCELED")
                return self._json_response(handler, 200, {"orderId": order.order_id, "status": order.status})

            # Algo order DELETE
            if path == "/fapi/v1/algoOrder" and method == "DELETE":
                algo = None
                if params.get("algoId"):
                    algo = self.algo_orders.get(int(params["algoId"]))
                elif params.get("clientAlgoId"):
                    algo_id = self.algo_order_by_client.get(str(params["clientAlgoId"]))
                    algo = self.algo_orders.get(algo_id or -1)
                if algo is None:
                    return self._json_response(handler, 404, {"code": -2011, "msg": "Unknown algo order"})
                algo.status = "CANCELED"
                self._emit_algo_update(algo, execution_type="CANCELED")
                return self._json_response(handler, 200, {"algoId": algo.algo_id, "status": algo.status})

            # Batch-cancel plain orders for symbol
            if path == "/fapi/v1/allOpenOrders" and method == "DELETE":
                symbol = str(params.get("symbol") or "BTCUSDT").upper()
                for order in self.plain_orders.values():
                    if order.symbol == symbol and order.status not in {"CANCELED", "FILLED", "REJECTED", "EXPIRED"}:
                        order.status = "CANCELED"
                        self._emit_order_update(order, execution_type="CANCELED")
                return self._json_response(handler, 200, {"code": 200, "msg": "done"})

            # Batch-cancel algo orders for symbol
            if path == "/fapi/v1/algoOpenOrders" and method == "DELETE":
                symbol = str(params.get("symbol") or "BTCUSDT").upper()
                for algo in self.algo_orders.values():
                    if algo.symbol == symbol and algo.status not in {"CANCELED", "FILLED", "REJECTED", "EXPIRED"}:
                        algo.status = "CANCELED"
                        self._emit_algo_update(algo, execution_type="CANCELED")
                return self._json_response(handler, 200, {"code": 200, "msg": "done"})

            return self._json_response(handler, 404, {"code": -404, "msg": f"Unhandled {method} {path}"})


# ---------------------------------------------------------------------------
# HTTP server wiring
# ---------------------------------------------------------------------------

class _Handler(BaseHTTPRequestHandler):
    """HTTP request handler that delegates to the shared MockBinanceState."""

    state: MockBinanceState

    def log_message(self, *args, **kwargs):  # pragma: no cover
        # Suppress default HTTP server logging during tests
        return

    def _params(self) -> Dict[str, Any]:
        """Parse query params from URL and (for POST/PUT/DELETE) from body."""
        parsed = urllib.parse.urlsplit(self.path)
        params: Dict[str, Any] = {}
        for k, vs in urllib.parse.parse_qs(parsed.query, keep_blank_values=True).items():
            params[k] = vs[-1]
        if self.command in {"POST", "PUT", "DELETE"}:
            length = int(self.headers.get("Content-Length") or 0)
            if length > 0:
                body = self.rfile.read(length).decode("utf-8")
                for k, vs in urllib.parse.parse_qs(body, keep_blank_values=True).items():
                    params[k] = vs[-1]
        return params

    def do_GET(self):
        self.state.handle(self, "GET", self.path, self._params())

    def do_POST(self):
        self.state.handle(self, "POST", self.path, self._params())

    def do_PUT(self):
        self.state.handle(self, "PUT", self.path, self._params())

    def do_DELETE(self):
        self.state.handle(self, "DELETE", self.path, self._params())


class DeterministicBinanceMockServer:
    """A lightweight HTTP mock Binance server that binds to a random free port.

    Usage::

        with running_binance_mock() as mock:
            client = BinanceFuturesClient(base_url=mock.base_url, ...)
            # ... drive executor ...
            assert len(mock.state.request_log) > 0
    """

    def __init__(self) -> None:
        self.state = MockBinanceState()
        # Build a handler class that carries the state as a class attribute so
        # ThreadingHTTPServer can instantiate it without custom __init__.
        handler = type("DeterministicBinanceHandler", (_Handler,), {})
        handler.state = self.state  # type: ignore[attr-defined]
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self._thread = Thread(target=self._server.serve_forever, daemon=True)
        self.base_url = f"http://127.0.0.1:{self._server.server_address[1]}"

    def start(self) -> "DeterministicBinanceMockServer":
        self._thread.start()
        return self

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=2)


@contextmanager
def running_binance_mock():
    """Context manager that starts a DeterministicBinanceMockServer and stops it on exit.

    Yields the running server so tests can inspect ``server.state.request_log``
    and inject scripted behaviours via ``server.state.set_plain_order_script()``.
    """
    server = DeterministicBinanceMockServer().start()
    try:
        yield server
    finally:
        server.close()
