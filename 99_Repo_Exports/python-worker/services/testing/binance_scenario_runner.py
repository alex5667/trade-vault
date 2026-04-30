from __future__ import annotations

"""Scenario runner on top of the deterministic Binance mock harness.

The goal is to exercise executor + user-stream behaviour with scripted timelines
instead of hand-written per-test glue. The runner intentionally keeps the API
small and deterministic so replay/load-smoke tests can share the same helpers.

P6.3: Adds:
  - BinanceScenarioRunner — thin orchestration wrapper
  - run_timeline([...]) — data-driven step runner for scripted scenarios
  - attach_live_user_stream_bridge() — synchronously feeds WS events into worker
  - enqueue_open_burst() — rapid signal pack helper for load-smoke tests
  - inject_order_update / inject_algo_update — manual event injection wrappers
  - snapshot() — point-in-time state capture for assertion convenience
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import json
import time

from services.binance_executor import BinanceExecutor
from services.binance_futures_client import BinanceFuturesClient
from services.binance_user_stream_worker import BinanceUserStreamWorker
from services.testing.binance_mock_harness import InMemoryRedis


@dataclass
class ScenarioStepResult:
    """Result of a single timeline step for structured reporting."""
    at: str
    op: str
    result: Dict[str, Any]
    snapshot: Dict[str, Any]


class BinanceScenarioRunner:
    """Reusable orchestration layer over the deterministic Binance mock harness.

    Wraps a running DeterministicBinanceMockServer, an InMemoryRedis, a
    BinanceExecutor and a BinanceUserStreamWorker.  All components share the
    same in-memory Redis and hit the same mock HTTP server.

    Lifecycle:
      - ``restart_executor()`` rebuilds the BinanceExecutor (simulates worker restart)
      - ``restart_worker()`` rebuilds the BinanceUserStreamWorker (simulates reconnect)
      - ``attach_live_user_stream_bridge()`` routes every harness-published WS event
        synchronously into worker.handle_message() — no real WebSocket needed.

    Example (scripted timeline)::

        with running_binance_mock() as mock:
            runner = BinanceScenarioRunner(mock_server=mock)
            runner.run_timeline([
                {"at": "t0", "op": "queue_open", "payload": {...}}
                {"at": "t0", "op": "run_executor_once"}
                {"at": "t1", "op": "drain_user_stream"}
                ...
            ], sid="sid-1")
    """

    def __init__(
        self
        *
        mock_server: Any
        redis_client: Optional[InMemoryRedis] = None
        api_key: str = "k"
        api_secret: str = "s"
    ) -> None:
        self.mock = mock_server
        self.base_url = str(mock_server.base_url)
        self.redis = redis_client if redis_client is not None else InMemoryRedis()
        self.api_key = api_key
        self.api_secret = api_secret
        self.executor = self._new_executor()
        self.worker = self._new_worker()
        self._live_user_stream_bridge_enabled = False

    # --- Internal factory helpers ---

    def _new_client(self) -> BinanceFuturesClient:
        return BinanceFuturesClient(
            api_key=self.api_key
            api_secret=self.api_secret
            base_url=self.base_url
            timeout_s=1.0
            recv_window=3000
        )

    def _new_executor(self) -> BinanceExecutor:
        """Instantiate a fresh BinanceExecutor bound to the shared mock+redis."""
        return BinanceExecutor(
            redis_client=self.redis
            prod_client=self._new_client()
            telegram_client=None
        )

    def _new_worker(self) -> BinanceUserStreamWorker:
        """Instantiate a fresh BinanceUserStreamWorker bound to the shared mock+redis."""
        return BinanceUserStreamWorker(
            redis_client=self.redis
            client=self._new_client()
        )

    # --- Live user-stream bridge ---

    def attach_live_user_stream_bridge(self) -> None:
        """Register a sink that feeds harness-emitted WS events into the worker.

        After calling this, any order fill or algo trigger event published by the
        mock harness (via _publish_user_stream_payload) is immediately delivered to
        self.worker.handle_message() synchronously — no real WebSocket server needed.

        Idempotent: safe to call multiple times.
        """
        if self._live_user_stream_bridge_enabled:
            return

        def _sink(raw: str) -> None:
            try:
                self.worker.handle_message(raw)
            except Exception:
                return

        self.mock.state.register_user_stream_sink(_sink)
        self._live_user_stream_bridge_enabled = True

    # --- Lifecycle controls ---

    def restart_executor(self) -> BinanceExecutor:
        """Replace self.executor with a fresh instance (simulates worker restart).

        The in-memory Redis persists so reconcile / rehydrate paths can be tested.
        """
        self.executor = self._new_executor()
        return self.executor

    def restart_worker(self) -> BinanceUserStreamWorker:
        """Replace self.worker with a fresh instance (simulates WS reconnect).

        Note: the live user-stream bridge sink registered before restart continues
        pointing at the old worker.  Call restart_worker() *before* calling
        attach_live_user_stream_bridge() if you need the bridge on the new instance.
        """
        self.worker = self._new_worker()
        return self.worker

    # --- Queue helpers ---

    def queue_raw(self, payload: Dict[str, Any], *, queue_key: str = "orders:queue:binance") -> None:
        """Push a raw order payload JSON into the executor queue."""
        self.redis.lpush(queue_key, json.dumps(payload))

    def enqueue_open_burst(
        self
        *
        sid_prefix: str
        count: int
        symbol: str = "BTCUSDT"
        side: str = "BUY"
        qty: float = 1.0
        order_type: str = "MARKET"
        sl: Optional[float] = None
        tp_levels: Optional[List[float]] = None
    ) -> None:
        """Push `count` open-position signals into the executor queue.

        Used for burst / load-smoke scenarios.  Each signal gets a unique sid
        derived from ``sid_prefix-{idx}``.
        """
        for idx in range(int(count)):
            payload: Dict[str, Any] = {
                "action": "open"
                "sid": f"{sid_prefix}-{idx}"
                "symbol": symbol
                "side": side
                "qty": qty
                "type": order_type
            }
            if sl is not None:
                payload["sl"] = sl
            if tp_levels is not None:
                payload["tp_levels"] = list(tp_levels)
            self.queue_raw(payload)

    # --- Executor helpers ---

    def run_executor_once(self, *, timeout: int = 0) -> bool:
        """Process one item from the executor queue. Returns True if processed."""
        return bool(self.executor.run_once(timeout=timeout))

    def run_executor_until_idle(self, *, timeout: int = 0, max_items: int = 256) -> int:
        """Process items until the queue is empty or max_items is reached.

        Returns the number of items processed.
        """
        processed = 0
        while processed < int(max_items) and self.run_executor_once(timeout=timeout):
            processed += 1
        return processed

    # --- User-stream drain ---

    def drain_user_stream(self, *, limit: Optional[int] = None) -> int:
        """Pop all pending harness WS events and deliver them to the worker.

        Returns the number of events handled.  Use `limit` to stop early.
        """
        handled = 0
        while True:
            messages = self.mock.state.pop_user_stream_messages()
            if not messages:
                break
            for raw in messages:
                self.worker.handle_message(raw)
                handled += 1
                if limit is not None and handled >= int(limit):
                    return handled
        return handled

    # --- Market state controls ---

    def set_mark_price(self, symbol: str, price: float) -> None:
        """Update the mock mark price for `symbol`."""
        self.mock.state.set_mark_price(symbol, price)

    def set_contract_price(self, symbol: str, price: float) -> None:
        """Update the mock contract price for `symbol`."""
        self.mock.state.set_contract_price(symbol, price)

    # --- HTTP fault injection ---

    def set_http_fault(
        self
        method: str
        path: str
        *
        status: int
        payload: Dict[str, Any]
        repeat: int = 1
    ) -> None:
        """Inject `repeat` HTTP fault responses for (method, path)."""
        self.mock.state.set_http_fault(method, path, status=status, payload=payload, repeat=repeat)

    # --- Manual event injection ---

    def inject_order_update(self, **kwargs) -> Dict[str, Any]:
        """Inject a plain order WS event; delegates to harness state."""
        return self.mock.state.inject_plain_order_update(**kwargs)

    def inject_algo_update(self, **kwargs) -> Dict[str, Any]:
        """Inject an algo order WS event; delegates to harness state."""
        return self.mock.state.inject_algo_update(**kwargs)

    # --- Utility ---

    def sleep_ms(self, ms: int) -> None:
        """Sleep for `ms` milliseconds (for deterministic timeline pacing)."""
        time.sleep(max(0, int(ms)) / 1000.0)

    # --- Stream inspection helpers ---

    def exec_events(self, key: str = "orders:exec") -> List[Dict[str, str]]:
        """Return all entries from the executor event stream."""
        return [fields for _id, fields in self.redis.xrange(key)]

    def user_stream_events(self, key: str = "orders:user_stream") -> List[Dict[str, str]]:
        """Return all entries from the user-stream event stream."""
        return [fields for _id, fields in self.redis.xrange(key)]

    def snapshot(self, *, sid: Optional[str] = None, symbol: str = "BTCUSDT") -> Dict[str, Any]:
        """Return a point-in-time snapshot of observable state.

        Includes:
          - ``state``: parsed orders:state:{sid} document (if sid given)
          - ``position_qty``: mock harness position for symbol
          - ``exec_events``: count of entries in orders:exec
          - ``user_stream_events``: count of entries in orders:user_stream
          - ``request_count``: total HTTP requests logged by the harness
        """
        state_doc: Dict[str, Any] = {}
        if sid:
            raw = self.redis.get(f"orders:state:{sid}")
            if raw:
                try:
                    state_doc = json.loads(raw)
                except Exception:
                    state_doc = {"_raw": raw}
        return {
            "sid": sid or ""
            "state": state_doc
            "position_qty": float(self.mock.state.positions.get(str(symbol).upper(), 0.0))
            "exec_events": len(self.redis.xrange("orders:exec"))
            "user_stream_events": len(self.redis.xrange("orders:user_stream"))
            "request_count": len(self.mock.state.request_log)
        }

    # --- Timeline runner ---

    def run_timeline(
        self
        steps: List[Dict[str, Any]]
        *
        sid: Optional[str] = None
        symbol: str = "BTCUSDT"
    ) -> List[Dict[str, Any]]:
        """Execute a list of scripted steps in order, returning step-by-step report.

        Each step is a dict with at least ``op`` (operation name) and optionally
        ``at`` (label) plus operation-specific parameters.

        Supported operations:
          queue_open / queue_raw   — queue_raw(payload)
          run_executor_once        — run_executor_once(timeout=step.timeout)
          run_executor_until_idle  — run_executor_until_idle(timeout, max_items)
          drain_user_stream        — drain_user_stream(limit=step.limit)
          restart_executor         — restart the executor instance
          restart_worker           — restart the user-stream worker instance
          set_mark_price           — set_mark_price(symbol, price)
          set_contract_price       — set_contract_price(symbol, price)
          sleep_ms                 — sleep_ms(ms)
          inject_order_update      — inject_order_update(**step_kwargs)
          inject_algo_update       — inject_algo_update(**step_kwargs)
          http_fault               — set_http_fault(method, path, status, payload, repeat)
          attach_live_user_stream_bridge — attach_live_user_stream_bridge()
        """
        report: List[Dict[str, Any]] = []
        for idx, step in enumerate(list(steps)):
            op = str(step.get("op") or "").strip()
            at = str(step.get("at") or f"step-{idx}")
            result: Dict[str, Any]

            if op in {"queue_open", "queue_raw"}:
                payload = dict(step.get("payload") or {})
                self.queue_raw(payload)
                result = {"queued": True, "sid": payload.get("sid")}

            elif op == "run_executor_once":
                result = {"processed": self.run_executor_once(timeout=int(step.get("timeout", 0)))}

            elif op == "run_executor_until_idle":
                result = {
                    "processed_count": self.run_executor_until_idle(
                        timeout=int(step.get("timeout", 0))
                        max_items=int(step.get("max_items", 256))
                    )
                }

            elif op == "drain_user_stream":
                result = {"handled": self.drain_user_stream(limit=step.get("limit"))}

            elif op == "restart_executor":
                self.restart_executor()
                result = {"restarted": "executor"}

            elif op == "restart_worker":
                self.restart_worker()
                result = {"restarted": "worker"}

            elif op == "set_mark_price":
                self.set_mark_price(str(step.get("symbol") or symbol), float(step["price"]))
                result = {"mark_price": float(step["price"])}

            elif op == "set_contract_price":
                self.set_contract_price(str(step.get("symbol") or symbol), float(step["price"]))
                result = {"contract_price": float(step["price"])}

            elif op == "sleep_ms":
                ms = int(step.get("ms") or 0)
                self.sleep_ms(ms)
                result = {"slept_ms": ms}

            elif op == "inject_order_update":
                result = self.inject_order_update(
                    **{k: v for k, v in step.items() if k not in {"op", "at"}}
                )

            elif op == "inject_algo_update":
                result = self.inject_algo_update(
                    **{k: v for k, v in step.items() if k not in {"op", "at"}}
                )

            elif op == "http_fault":
                self.set_http_fault(
                    str(step.get("method") or "GET")
                    str(step.get("path") or "")
                    status=int(step.get("status") or 500)
                    payload=dict(step.get("payload") or {"code": -1000, "msg": "fault"})
                    repeat=int(step.get("repeat") or 1)
                )
                result = {"fault_queued": True}

            elif op == "attach_live_user_stream_bridge":
                self.attach_live_user_stream_bridge()
                result = {"live_user_stream_bridge": True}

            else:
                raise ValueError(f"unknown scenario op: {op!r}")

            report.append({
                "at": at
                "op": op
                "result": result
                "snapshot": self.snapshot(sid=sid, symbol=symbol)
            })
        return report
