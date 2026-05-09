"""P6.1/P6.2 Integration tests: deterministic Binance mock harness.

Covers:
  - queue → executor → mock REST → exec stream (integration)
  - 503/Unknown → reconcile via query, no duplicate submit (replay)
  - user-stream worker ingests ORDER_TRADE_UPDATE + ALGO_UPDATE from mock
  - restart after partial fill + duplicate queue delivery (idempotent replay)
  - maker TP watchdog falls back to market close when fill delayed
  - burst of 8 open signals processed without DLQ leakage (load-smoke)
"""

import json
import time

from services.binance_executor import BinanceExecutor, _make_cid
from services.binance_futures_client import BinanceFuturesClient
from services.binance_user_stream_worker import BinanceUserStreamWorker
from services.testing.binance_mock_harness import InMemoryRedis, running_binance_mock

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _configure_env(monkeypatch, *, base_url: str, maker: bool = False, fill_timeout_s: str = "0.25") -> None:
    """Set all env vars required by BinanceExecutor to point at the mock server."""
    values = {
        "BINANCE_CLIENT_MODE": "real",
        "BINANCE_FUTURES_BASE_URL": base_url,
        "BINANCE_POSITION_MODE": "oneway",
        "BINANCE_SYMBOL_ALLOWLIST": "BTCUSDT",
        "BINANCE_INIT_SYMBOL_SETTINGS": "0",
        "BINANCE_FILL_TIMEOUT_S": fill_timeout_s,
        "BINANCE_FILL_POLL_S": "0.05",
        "BINANCE_RECV_WINDOW_MS": "3000",
        "PROTECTION_ARM_TIMEOUT_MS": "2500",
        "TP_LIMIT_WATCHDOG_ENABLE": "1",
        "TP_LIMIT_WATCHDOG_TIMEOUT_MS": "120",
        "TP_TRIGGER_MONITOR_TIMEOUT_S": "1.0",
        "BINANCE_TRAIL_ARM_POLL_S": "0.05",
        "BINANCE_TRAIL_NOTIFY": "0",
        "EXEC_RECONCILE_ENABLE": "1",
        "EXEC_RECONCILE_ON_503_UNKNOWN": "1",
        "EXEC_RECONCILE_PREFER_USER_STREAM": "1",
        "EXEC_REHYDRATE_ON_STATE_MISS": "1",
        "EXEC_JOURNAL_SQL_ENABLE": "0",
        "EXECUTION_JOURNAL_DSN": "",
        "EXECUTION_QUARANTINE_LEDGER_DSN": "",
        "REDIS_URL": "redis://mock/0",
        "EXEC_STREAM": "orders:exec",
        "ORDERS_QUEUE_BINANCE": "orders:queue:binance",
        "ORDERS_QUEUE_BINANCE_PROCESSING": "orders:queue:binance:processing",
        "ORDERS_QUEUE_BINANCE_DLQ": "orders:queue:binance:dlq",
        "USER_STREAM_STREAM": "orders:user_stream",
        "USER_STREAM_CACHE_PREFIX": "orders:user_stream:",
        # Maker TP policy activated only when maker=True
        "EXEC_POLICY_DEFAULT": "MAKER_FIRST" if maker else "SAFETY_FIRST",
        "EXEC_POLICY_MAKER_ALLOWED_SYMBOLS": "BTCUSDT" if maker else "DO_NOT_USE",
        "EXEC_FORCE_SAFETY_FIRST": "0" if maker else "1",
    }
    for k, v in values.items():
        monkeypatch.setenv(k, v)


def _new_client(base_url: str) -> BinanceFuturesClient:
    """Construct a BinanceFuturesClient pointed at the mock server."""
    return BinanceFuturesClient(api_key="k", api_secret="s", base_url=base_url, timeout_s=1.0, recv_window=3000)


def _new_executor(redis_client: InMemoryRedis, base_url: str) -> BinanceExecutor:
    """Build a BinanceExecutor with injected mock redis and prod_client."""
    return BinanceExecutor(redis_client=redis_client, prod_client=_new_client(base_url), telegram_client=None)


def _new_worker(redis_client: InMemoryRedis, base_url: str) -> BinanceUserStreamWorker:
    """Build a BinanceUserStreamWorker with injected mock redis and client."""
    return BinanceUserStreamWorker(redis_client=redis_client, client=_new_client(base_url))


def _queue_open(redis_client: InMemoryRedis, payload: dict) -> None:
    """Push an open signal into the executor queue (left-push → BRPOPLPUSH pops from right)."""
    redis_client.lpush("orders:queue:binance", json.dumps(payload))


def _stream_events(redis_client: InMemoryRedis, key: str = "orders:exec"):
    """Return all fields dicts from a Redis stream key."""
    return [fields for _id, fields in redis_client.xrange(key)]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_queue_executor_to_mock_places_entry_and_protection(monkeypatch):
    """Integration: queue → executor → mock REST places entry + SL + TP algo orders."""
    with running_binance_mock() as mock:
        _configure_env(monkeypatch, base_url=mock.base_url, maker=False)
        redis_client = InMemoryRedis()
        executor = _new_executor(redis_client, mock.base_url)
        sid = "sid-open-1"
        _queue_open(redis_client, {
            "action": "open",
            "sid": sid,
            "symbol": "BTCUSDT",
            "side": "BUY",
            "qty": 1,
            "type": "MARKET",
            "sl": 95,
            "tp_levels": [110],
        })

        assert executor.run_once(timeout=0) is True

        posts = [x for x in mock.state.request_log if x["method"] == "POST"]
        # At least one plain order (entry)
        assert any(x["path"] == "/fapi/v1/order" for x in posts)
        # Exactly two algo orders: SL + TP
        algo_posts = [x for x in posts if x["path"] == "/fapi/v1/algoOrder"]
        assert len(algo_posts) == 2
        assert {x["params"]["type"] for x in algo_posts} == {"STOP_MARKET", "TAKE_PROFIT_MARKET"}

        # exec stream should contain an open event
        events = _stream_events(redis_client)
        assert any(ev.get("action") == "open" and ev.get("sid") == sid for ev in events)

        # orders:state:{sid} should be persisted with protection types
        state_doc = json.loads(redis_client.get(f"orders:state:{sid}")),
        assert state_doc["sl_order_type"] == "STOP_MARKET"
        assert state_doc["tp1_order_type"] == "TAKE_PROFIT_MARKET"


def test_503_unknown_reconciles_via_query_without_duplicate_submit(monkeypatch):
    """Replay: 503 Unknown on POST → executor reconciles via GET, no duplicate submit, no DLQ."""
    with running_binance_mock() as mock:
        _configure_env(monkeypatch, base_url=mock.base_url),
        redis_client = InMemoryRedis(),
        executor = _new_executor(redis_client, mock.base_url),
        sid = "sid-503-1",
        cid = _make_cid(sid, "entry"),

        # Script the entry clientOrderId to return 503 on POST but be queryable via GET
        mock.state.set_plain_order_script(
            cid,
            query_sequence=[{"status": "FILLED", "executedQty": "1", "avgPrice": "100.0"}],
            post_error={
                "status": 503,
                "payload": {"code": 0, "msg": "Unknown error, please check your request or try again later."},
            },
            create_on_post_error=True,  # order IS created server-side despite HTTP 503
        )
        _queue_open(redis_client, {
            "action": "open",
            "sid": sid,
            "symbol": "BTCUSDT",
            "side": "BUY",
            "qty": 1,
            "type": "MARKET",
            "sl": 95,
            "tp_levels": [110],
        })

        executor.run_once(timeout=0)

        # Only one POST attempt — reconcile must not re-submit
        post_orders = [x for x in mock.state.request_log if x["path"] == "/fapi/v1/order" and x["method"] == "POST"]
        get_orders = [x for x in mock.state.request_log if x["path"] == "/fapi/v1/order" and x["method"] == "GET"]
        assert len(post_orders) == 1
        assert len(get_orders) >= 1

        # Must NOT be DLQ'd
        assert redis_client.lrange("orders:queue:binance:dlq", 0, -1) == []

        # Exec stream should show PROTECTED state transition
        events = _stream_events(redis_client)
        assert any(
            ev.get("event_type") == "state_transition" and ev.get("fsm_state") == "PROTECTED"
            for ev in events
        )


def test_user_stream_worker_ingests_order_and_algo_updates_from_mock(monkeypatch):
    """Integration: user-stream worker handles ORDER_TRADE_UPDATE + ALGO_UPDATE emitted by mock."""
    with running_binance_mock() as mock:
        _configure_env(monkeypatch, base_url=mock.base_url)
        redis_client = InMemoryRedis()
        executor = _new_executor(redis_client, mock.base_url)
        worker = _new_worker(redis_client, mock.base_url)
        sid = "sid-stream-1"

        _queue_open(redis_client, {
            "action": "open",
            "sid": sid,
            "symbol": "BTCUSDT",
            "side": "BUY",
            "qty": 1,
            "type": "MARKET",
            "sl": 95,
            "tp_levels": [110],
        })
        executor.run_once(timeout=0)

        # Drain user-stream events produced by the mock during execution
        msgs = mock.state.pop_user_stream_messages()
        assert msgs, "mock should emit user-stream updates on fill and algo NEW"
        for raw in msgs:
            assert worker.handle_message(raw) is True

        user_events = _stream_events(redis_client, "orders:user_stream")
        event_types = {ev["event_type"] for ev in user_events}
        assert "ORDER_TRADE_UPDATE" in event_types
        assert "ALGO_UPDATE" in event_types

        # Point-in-time cache keys must be written by the worker
        assert redis_client.get(f"orders:user_stream:order:{_make_cid(sid, 'entry')}")
        assert redis_client.get(f"orders:user_stream:algo:{_make_cid(sid, 'sl')}")


def test_restart_after_partial_fill_duplicate_delivery_is_idempotent(monkeypatch):
    """Replay: duplicate queue delivery after partial fill → only one POST, duplicate prevented."""
    with running_binance_mock() as mock:
        _configure_env(monkeypatch, base_url=mock.base_url, fill_timeout_s="0.12")
        redis_client = InMemoryRedis()
        sid = "sid-partial-1"
        cid = _make_cid(sid, "entry")

        # Script two PARTIALLY_FILLED query responses — executor will timeout waiting
        mock.state.set_plain_order_script(
            cid,
            query_sequence=[
                {"status": "PARTIALLY_FILLED", "executedQty": "0.5", "avgPrice": "100.0"},
                {"status": "PARTIALLY_FILLED", "executedQty": "0.5", "avgPrice": "100.0"}]
        )
        payload = {
            "action": "open",
            "sid": sid,
            "symbol": "BTCUSDT",
            "side": "BUY",
            "qty": 1,
            "type": "MARKET",
            "sl": 95,
            "tp_levels": [110],
        }
        _queue_open(redis_client, payload)

        # First executor processes and leaves redis state with partially_filled
        executor1 = _new_executor(redis_client, mock.base_url)
        executor1.run_once(timeout=0)

        # Second executor (simulating restart) receives the same payload again
        executor2 = _new_executor(redis_client, mock.base_url)
        raw = json.dumps(payload)
        executor2.process_one(raw)  # direct call to avoid needing message in queue

        # Only one REST POST should have been made (duplicate guard kicked in on second pass)
        post_orders = [x for x in mock.state.request_log if x["path"] == "/fapi/v1/order" and x["method"] == "POST"]
        assert len(post_orders) == 1

        events = _stream_events(redis_client)
        assert any(ev.get("event_type") == "duplicate_prevented" and ev.get("sid") == sid for ev in events)

        state_doc = json.loads(redis_client.get(f"orders:state:{sid}"))
        assert state_doc["status"] == "partially_filled"


def test_maker_tp_watchdog_falls_back_to_market_close(monkeypatch):
    """Maker TP: watchdog triggers and submits a reduce-only MARKET close when fill delayed.

    Note: this test relies on a 0.35 s sleep for the watchdog thread. Marked as
    potentially flaky on very slow CI systems (increase TP_LIMIT_WATCHDOG_TIMEOUT_MS
    and sleep duration if needed).
    """
    with running_binance_mock() as mock:
        _configure_env(monkeypatch, base_url=mock.base_url, maker=True)
        mock.state.set_mark_price("BTCUSDT", 100.0)
        mock.state.set_contract_price("BTCUSDT", 99.0)
        redis_client = InMemoryRedis()
        executor = _new_executor(redis_client, mock.base_url)
        sid = "sid-maker-watchdog-1"

        _queue_open(redis_client, {
            "action": "open",
            "sid": sid,
            "symbol": "BTCUSDT",
            "side": "BUY",
            "qty": 1,
            "type": "MARKET",
            "sl": 95,
            "tp_levels": [101],
        })
        executor.run_once(timeout=0)

        # Simulate mark price crossing TP1 — watchdog should detect and fall back
        mock.state.set_mark_price("BTCUSDT", 101.5)
        time.sleep(0.35)  # wait for watchdog thread polling interval

        events = _stream_events(redis_client)
        tp_states = [ev.get("tp_state") for ev in events if ev.get("event_type") == "tp_watchdog"]
        assert "TP1_TRIGGERED" in tp_states
        assert "TP1_WATCHDOG_MARKET_FALLBACK" in tp_states

        market_posts = [
            x for x in mock.state.request_log
            if x["path"] == "/fapi/v1/order"
            and x["method"] == "POST"
            and x["params"].get("reduceOnly") == "true"
        ]
        assert market_posts, "watchdog should submit a reduce-only MARKET close"
        # Position must be flat after watchdog close
        assert abs(mock.state.positions.get("BTCUSDT", 0.0)) <= 1e-9


def test_burst_open_signals_smoke(monkeypatch):
    """Load-smoke: 8 concurrent open signals processed without DLQ leakage."""
    with running_binance_mock() as mock:
        _configure_env(monkeypatch, base_url=mock.base_url)
        redis_client = InMemoryRedis()
        executor = _new_executor(redis_client, mock.base_url)

        for i in range(8):
            _queue_open(redis_client, {
                "action": "open",
                "sid": f"sid-burst-{i}",
                "symbol": "BTCUSDT",
                "side": "BUY",
                "qty": 1,
                "type": "MARKET",
                "sl": 95,
                "tp_levels": [110],
            })

        processed = 0
        while executor.run_once(timeout=0):
            processed += 1

        assert processed == 8
        assert redis_client.lrange("orders:queue:binance:dlq", 0, -1) == [], "no DLQ entries expected"

        open_events = [ev for ev in _stream_events(redis_client) if ev.get("action") == "open"]
        assert len(open_events) >= 8
