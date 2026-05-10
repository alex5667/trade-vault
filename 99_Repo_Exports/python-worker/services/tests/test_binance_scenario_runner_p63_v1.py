"""P6.3 scenario runner tests — scripted timeline + smoke/load pack.

Covers:
  test_scripted_timeline_partial_restart_trigger_delayed_fill
    t0: submit open signal
    t1: drain user-stream → partial fill lands
    t2: restart executor → detect duplicate → skip re-open
    t3: trigger TP (mark rises above TP1 price)
    t4: sleep for watchdog timeout → TP watchdog fires market fallback

  test_degraded_rest_with_live_user_stream_bridge_reconciles_without_query
    Degraded REST: POST /fapi/v1/order returns 503 unknown.
    Healthy WS: attach_live_user_stream_bridge so the fill event arrives
                synchronously via handle_message() → reconcile uses user-stream
                cache instead of issuing a REST query.

  test_burst_with_worker_reconnect_smoke_pack
    Burst: 12 open signals
    Mid-pack: restart worker (simulates WS reconnect)
    Assertion: all 12 processed without DLQ, both halves' stream events delivered.

All tests use InMemoryRedis + DeterministicBinanceMockServer (no real network
connections, no Postgres, no Telegram).
"""

import json

from services.binance_executor import _make_cid
from services.testing.binance_mock_harness import running_binance_mock
from services.testing.binance_scenario_runner import BinanceScenarioRunner

from core.redis_keys import RedisStreams as RS

# ---------------------------------------------------------------------------
# Shared env-setup helper
# ---------------------------------------------------------------------------

def _configure_env(
    monkeypatch,
    *,
    base_url: str,
    maker: bool = False,
    fill_timeout_s: str = "0.25",
) -> None:
    """Set all required ENV vars so BinanceExecutor + BinanceUserStreamWorker
    can be instantiated without a real Binance account."""
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
        # TP watchdog: short timeout so the fallback fires in tests
        "TP_LIMIT_WATCHDOG_ENABLE": "1",
        "TP_LIMIT_WATCHDOG_TIMEOUT_MS": "120",
        "TP_TRIGGER_MONITOR_TIMEOUT_S": "1.0",
        "BINANCE_TRAIL_ARM_POLL_S": "0.05",
        "BINANCE_TRAIL_NOTIFY": "0",
        # Reconcile: prefer user-stream cache on 503-unknown
        "EXEC_RECONCILE_ENABLE": "1",
        "EXEC_RECONCILE_ON_503_UNKNOWN": "1",
        "EXEC_RECONCILE_PREFER_USER_STREAM": "1",
        "EXEC_REHYDRATE_ON_STATE_MISS": "1",
        # SQL sinks disabled (no Postgres in test env)
        "EXEC_JOURNAL_SQL_ENABLE": "0",
        "EXECUTION_JOURNAL_DSN": "",
        "EXECUTION_QUARANTINE_LEDGER_DSN": "",
        # Redis / stream keys
        "REDIS_URL": "redis://mock/0",
        "EXEC_STREAM": RS.ORDERS_EXEC,
        "ORDERS_QUEUE_BINANCE": RS.ORDERS_QUEUE_BINANCE,
        "ORDERS_QUEUE_BINANCE_PROCESSING": RS.ORDERS_QUEUE_BINANCE_PROCESSING,
        "ORDERS_QUEUE_BINANCE_DLQ": RS.ORDERS_QUEUE_BINANCE_DLQ,
        "USER_STREAM_STREAM": "orders:user_stream",
        "USER_STREAM_CACHE_PREFIX": "orders:user_stream:",
        # Execution policy
        "EXEC_POLICY_DEFAULT": "MAKER_FIRST" if maker else "SAFETY_FIRST",
        "EXEC_POLICY_MAKER_ALLOWED_SYMBOLS": "BTCUSDT" if maker else "DO_NOT_USE",
        "EXEC_FORCE_SAFETY_FIRST": "0" if maker else "1",
    }
    for k, v in values.items():
        monkeypatch.setenv(k, v)


# ---------------------------------------------------------------------------
# Test 1 — scripted timeline: partial fill → restart → TP trigger → watchdog
# ---------------------------------------------------------------------------

def test_scripted_timeline_partial_restart_trigger_delayed_fill(monkeypatch):
    """Integration scenario: partial fill, executor restart, TP trigger, watchdog fallback.

    Timeline:
      t0 — queue open signal, run_executor_once → entry submitted (partial script)
      t1 — drain_user_stream → PARTIAL_FILLED event lands in worker + Redis
      t2 — restart_executor → re-queue same signal → detect duplicate → skip
      t3 — set_mark_price above TP1 → TP monitor detects trigger
      t4 — sleep > watchdog timeout → watchdog issues market close fallback

    Assertions:
      - t1 state.status == "partially_filled"
      - exec stream contains duplicate_prevented event for the sid
      - exec stream contains tp_watchdog events: TP1_TRIGGERED + TP1_WATCHDOG_MARKET_FALLBACK
      - final position ≤ 1e-9 (flat, closed by fallback)
    """
    with running_binance_mock() as mock:
        _configure_env(monkeypatch, base_url=mock.base_url, maker=True, fill_timeout_s="0.12")
        mock.state.set_mark_price("BTCUSDT", 100.0)
        mock.state.set_contract_price("BTCUSDT", 99.0)
        runner = BinanceScenarioRunner(mock_server=mock)

        sid = "sid-timeline-1"
        payload = {
            "action": "open",
            "sid": sid,
            "symbol": "BTCUSDT",
            "side": "BUY",
            "qty": 1,
            "type": "MARKET",
            "sl": 95,
            "tp_levels": [101],
        }

        # Script the entry order to return PARTIAL_FILLED on first query
        mock.state.set_plain_order_script(
            _make_cid(sid, "entry"),
            query_sequence=[
                {"status": "PARTIALLY_FILLED", "executedQty": "0.4", "avgPrice": "100.0"}
            ],
        )

        report = runner.run_timeline([
            {"at": "t0", "op": "queue_open", "payload": payload},
            {"at": "t0", "op": "run_executor_once"},
            {"at": "t1", "op": "drain_user_stream"},
            {"at": "t2", "op": "restart_executor"},
            # Re-queue: executor should detect duplicate and skip
            {"at": "t2", "op": "queue_raw", "payload": payload},
            {"at": "t2", "op": "run_executor_once"},
            {"at": "t2", "op": "drain_user_stream"},
            # Raise mark: TP1 at 101 should now be triggered
            {"at": "t3", "op": "set_mark_price", "symbol": "BTCUSDT", "price": 101.5},
            # Sleep > watchdog timeout (120 ms + buffer for TP limit not filled)
            {"at": "t4", "op": "sleep_ms", "ms": 350},
            {"at": "t4", "op": "drain_user_stream"},
        ], sid=sid, symbol="BTCUSDT")

        # t1 snapshot should show partially_filled state
        partial_snapshot = next(item for item in report if item["at"] == "t1")
        assert partial_snapshot["snapshot"]["state"]["status"] == "partially_filled"

        exec_events = runner.exec_events()

        # Duplicate signal should have been detected and logged
        assert any(
            ev.get("event_type") == "duplicate_prevented" and ev.get("sid") == sid
            for ev in exec_events
        )

        # TP watchdog + market fallback should have fired
        tp_states = [ev.get("tp_state") for ev in exec_events if ev.get("event_type") == "tp_watchdog"]
        assert "TP1_TRIGGERED" in tp_states
        assert "TP1_WATCHDOG_MARKET_FALLBACK" in tp_states

        # Position should be flat after emergency close
        assert abs(mock.state.positions["BTCUSDT"]) <= 1e-9


# ---------------------------------------------------------------------------
# Test 2 — degraded REST + healthy WS: reconcile w/o extra query
# ---------------------------------------------------------------------------

def test_degraded_rest_with_live_user_stream_bridge_reconciles_without_query(monkeypatch):
    """Degraded REST submit (503 unknown) + healthy user-stream reconcile.

    POST /fapi/v1/order returns 503 unknown; the order IS created on Binance side
    (create_on_post_error=True).  The live user-stream bridge delivers the FILLED
    event synchronously into handle_message() before the executor checks the
    user-stream cache.

    Assertions:
      - processed=True (executor did not DLQ the signal)
      - exactly one POST to /fapi/v1/order (no retries beyond the initial ambiguous one)
      - DLQ is empty
      - orders:state:{sid}.fsm_state ∈ {PROTECTED, TP_POLICY_ARMED, TRAIL_ARMED}
      - exec stream contains PENDING_RECONCILE event (reconcile path taken)
    """
    with running_binance_mock() as mock:
        _configure_env(monkeypatch, base_url=mock.base_url, maker=False)
        runner = BinanceScenarioRunner(mock_server=mock)

        sid = "sid-ws-reconcile-1"
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

        entry_cid = _make_cid(sid, "entry")
        # POST returns 503 unknown but order IS created (simulates Binance fire-and-error)
        mock.state.set_plain_order_script(
            entry_cid,
            post_error={
                "status": 503,
                "payload": {
                    "code": 0,
                    "msg": "Unknown error, please check your request or try again later.",
                },
            },
            create_on_post_error=True,
        )

        # P6.3: bridge routes the harness MARKET fill event synchronously into
        # handle_message() — executor sees user-stream cache hit and uses it for reconcile
        report = runner.run_timeline([
            {"at": "t0", "op": "attach_live_user_stream_bridge"},
            {"at": "t0", "op": "queue_open", "payload": payload},
            {"at": "t0", "op": "run_executor_once"},
        ], sid=sid)

        assert report[-1]["result"]["processed"] is True

        # Only one POST should have been issued
        reqs = mock.state.request_log
        post_order_reqs = [x for x in reqs if x["path"] == "/fapi/v1/order" and x["method"] == "POST"]
        assert len(post_order_reqs) == 1

        # DLQ must be empty
        assert runner.redis.lrange(RS.ORDERS_QUEUE_BINANCE_DLQ, 0, -1) == []

        # State key must exist and be in a protected FSM state
        state_raw = runner.redis.get(f"orders:state:{sid}")
        assert state_raw is not None
        state_doc = json.loads(state_raw)
        assert state_doc["fsm_state"] in {"PROTECTED", "TP_POLICY_ARMED", "TRAIL_ARMED"}

        # Reconcile path must have been logged
        exec_events = runner.exec_events()
        assert any(ev.get("fsm_state") == "PENDING_RECONCILE" for ev in exec_events)


# ---------------------------------------------------------------------------
# Test 3 — burst + worker reconnect smoke pack
# ---------------------------------------------------------------------------

def test_burst_with_worker_reconnect_smoke_pack(monkeypatch):
    """Smoke: 12 open signals, worker reconnect mid-pack.

    First 6 processed, then worker reconnected, then remaining 6 processed.
    Both halves drain user-stream successfully.

    Assertions:
      - first == 6, second == 6
      - both drain calls return > 0 events (MARKET fills + algo events)
      - DLQ empty
      - at least 12 open exec events in orders:exec
      - at least 12 user-stream events in orders:user_stream
    """
    with running_binance_mock() as mock:
        _configure_env(monkeypatch, base_url=mock.base_url, maker=False)
        runner = BinanceScenarioRunner(mock_server=mock)

        runner.enqueue_open_burst(
            sid_prefix="sid-burst-reconnect",
            count=12,
            symbol="BTCUSDT",
            side="BUY",
            qty=1,
            order_type="MARKET",
            sl=95,
            tp_levels=[110],
        )

        # Process first 6
        first = runner.run_executor_until_idle(max_items=6)
        first_events = runner.drain_user_stream()

        # Simulate WS reconnect mid-pack
        runner.restart_worker()

        # Process remaining 6
        second = runner.run_executor_until_idle(max_items=12)
        second_events = runner.drain_user_stream()

        assert first == 6
        assert second == 6
        assert first_events > 0, "first batch should have WS events from fills"
        assert second_events > 0, "second batch (after reconnect) should have WS events"

        assert runner.redis.lrange(RS.ORDERS_QUEUE_BINANCE_DLQ, 0, -1) == [], "DLQ must be empty"

        exec_open = [ev for ev in runner.exec_events() if ev.get("action") == "open"]
        assert len(exec_open) >= 12, f"expected ≥12 open events, got {len(exec_open)}"

        user_events = runner.user_stream_events()
        assert len(user_events) >= 12, f"expected ≥12 user-stream events, got {len(user_events)}"
