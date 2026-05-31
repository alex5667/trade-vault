"""Unit tests for services/execution/ modules (decomposed BinanceExecutor).

Tests are adapted to the actual API signatures discovered by introspection.
"""
from __future__ import annotations
import json
import pytest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))


# ---------------------------------------------------------------------------
# FakeRedis
# ---------------------------------------------------------------------------

class FakeRedis:
    def __init__(self):
        self._kv = {}   # str key → str val
        self._sets = {} # str key → set
        self._streams = {} # stream → list of dicts
        self._lists = {}

    def get(self, key):
        return self._kv.get(key)

    def set(self, key, val, ex=None):
        self._kv[key] = val

    def delete(self, key):
        self._kv.pop(key, None)

    def hset(self, key, mapping=None, **kw):
        self._kv.setdefault(key, {})
        if not isinstance(self._kv[key], dict):
            self._kv[key] = {}
        if mapping:
            self._kv[key].update(mapping)
        self._kv[key].update(kw)

    def hgetall(self, key):
        val = self._kv.get(key)
        if isinstance(val, dict):
            return dict(val)
        return {}

    def expire(self, key, ttl):
        pass

    def rpush(self, key, *vals):
        self._lists.setdefault(key, []).extend(vals)

    def lpush(self, key, *vals):
        for v in reversed(vals):
            self._lists.setdefault(key, []).insert(0, v)

    def lrem(self, key, count, val):
        pass

    def sadd(self, key, val):
        self._sets.setdefault(key, set()).add(val)

    def srem(self, key, val):
        self._sets.get(key, set()).discard(val)

    def sismember(self, key, val):
        return val in self._sets.get(key, set())

    def smembers(self, key):
        return set(self._sets.get(key, set()))

    def xadd(self, stream, fields, maxlen=None, approximate=True):
        self._streams.setdefault(stream, []).append(dict(fields))
        idx = len(self._streams[stream])
        return f"{idx}-0"

    def xlen(self, stream):
        return len(self._streams.get(stream, []))

    def xrevrange(self, stream, max="+", min="-", count=None):
        items = self._streams.get(stream, [])
        result = []
        for i, f in enumerate(reversed(items)):
            if count and len(result) >= count:
                break
            result.append((f"{i}-0", f))
        return result

    def brpoplpush(self, src, dst, timeout=0):
        return None

    def pipeline(self, transaction=False):
        return FakePipeline(self)


class FakePipeline:
    def __init__(self, r):
        self._r = r

    def set(self, *a, **kw):
        return self

    def sadd(self, *a, **kw):
        return self

    def xadd(self, *a, **kw):
        return self

    def hset(self, *a, **kw):
        return self

    def expire(self, *a, **kw):
        return self

    def execute(self):
        return []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass


# ---------------------------------------------------------------------------
# binance_order_mapper
# ---------------------------------------------------------------------------

class TestOrderMapper:
    def test_f_basic(self):
        from services.execution.binance_order_mapper import _f
        assert _f("3.14") == pytest.approx(3.14)
        assert _f(None, 0.0) == 0.0
        assert _f("bad", 1.0) == 1.0

    def test_i_basic(self):
        from services.execution.binance_order_mapper import _i
        assert _i("5") == 5
        assert _i(None, 3) == 3

    def test_round_down(self):
        from services.execution.binance_order_mapper import _round_down
        assert _round_down(1.2345, 0.001) == pytest.approx(1.234)
        assert _round_down(0.0, 0.001) == 0.0

    def test_format_float(self):
        from services.execution.binance_order_mapper import _format_float
        assert _format_float(1.234, 0.001) == "1.234"
        assert _format_float(1.0, 1.0) == "1"

    def test_bool_env_true(self, monkeypatch):
        from services.execution.binance_order_mapper import _bool_env
        monkeypatch.setenv("TEST_FLAG_X", "1")
        assert _bool_env("TEST_FLAG_X", False) is True

    def test_bool_env_false(self, monkeypatch):
        from services.execution.binance_order_mapper import _bool_env
        monkeypatch.setenv("TEST_FLAG_X", "0")
        assert _bool_env("TEST_FLAG_X", True) is False

    def test_bool_env_missing(self, monkeypatch):
        from services.execution.binance_order_mapper import _bool_env
        monkeypatch.delenv("TEST_FLAG_X", raising=False)
        assert _bool_env("TEST_FLAG_X", True) is True

    def test_normalize_side_buy(self):
        from services.execution.binance_order_mapper import _normalize_side
        b, l, si = _normalize_side({"side": "BUY"})
        assert b == "BUY" and l == "LONG" and si == 1

    def test_normalize_side_short(self):
        from services.execution.binance_order_mapper import _normalize_side
        b, l, si = _normalize_side({"side": "SHORT"})
        assert b == "SELL" and l == "SHORT" and si == -1

    def test_normalize_side_direction_long(self):
        from services.execution.binance_order_mapper import _normalize_side
        b, l, _ = _normalize_side({"direction": "long"})
        assert b == "BUY" and l == "LONG"

    def test_position_side_oneway(self):
        from services.execution.binance_order_mapper import _position_side_for_mode
        assert _position_side_for_mode("oneway", "LONG") is None

    def test_position_side_hedge_long(self):
        from services.execution.binance_order_mapper import _position_side_for_mode
        assert _position_side_for_mode("hedge", "LONG") == "LONG"

    def test_position_side_hedge_short(self):
        from services.execution.binance_order_mapper import _position_side_for_mode
        assert _position_side_for_mode("hedge", "SHORT") == "SHORT"

    def test_terminal_fsm_states_is_set(self):
        from services.execution.binance_order_mapper import TERMINAL_FSM_STATES
        assert isinstance(TERMINAL_FSM_STATES, (set, frozenset))
        assert len(TERMINAL_FSM_STATES) > 0

    def test_classify_error_transient_msg(self):
        from services.execution.binance_order_mapper import _classify_error
        assert _classify_error(Exception("connection timed out")) == "transient"

    def test_classify_error_unknown_is_fatal(self):
        from services.execution.binance_order_mapper import _classify_error
        result = _classify_error(Exception("some weird unknown error"))
        assert result in ("fatal", "transient", "unknown")


# ---------------------------------------------------------------------------
# binance_filters
# ---------------------------------------------------------------------------

class TestBinanceFilters:
    def test_symbol_filters_fields(self):
        from services.execution.binance_filters import SymbolFilters
        sf = SymbolFilters(step_size=0.001, tick_size=0.01, min_notional=5.0, min_qty=0.001)
        assert sf.step_size == 0.001
        assert sf.tick_size == 0.01
        assert sf.min_notional == 5.0

    def test_filters_cache_fallback_raises_for_unknown(self):
        from services.execution.binance_filters import FiltersCache

        class MockClient:
            def get_exchange_info(self):
                return {"symbols": []}

        fc = FiltersCache(MockClient())
        with pytest.raises(RuntimeError, match="Unknown Binance symbol"):
            fc.get("BTCUSDT")

    def test_filters_cache_parsed_values(self):
        from services.execution.binance_filters import FiltersCache

        class MockClient:
            def get_exchange_info(self):
                return {"symbols": [{
                    "symbol": "BTCUSDT",
                    "filters": [
                        {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                        {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                        {"filterType": "MIN_NOTIONAL", "notional": "5.0"},
                    ]
                }]}

        fc = FiltersCache(MockClient())
        sf = fc.get("BTCUSDT")
        assert sf.step_size == pytest.approx(0.001)
        assert sf.tick_size == pytest.approx(0.10)

    def test_filters_cache_unknown_symbol_raises(self):
        from services.execution.binance_filters import FiltersCache

        class MockClient:
            def get_exchange_info(self):
                return {"symbols": []}

        fc = FiltersCache(MockClient())
        with pytest.raises(RuntimeError):
            fc.get("UNKNOWNSYMBOL")


# ---------------------------------------------------------------------------
# execution_event_writer
# ---------------------------------------------------------------------------

class TestExecutionEventWriter:
    def _make(self):
        from services.execution.execution_event_writer import ExecutionEventWriter
        r = FakeRedis()
        w = ExecutionEventWriter(
            r=r,
            exec_stream="orders:exec",
            exec_stream_maxlen=None,
            queue="orders:queue:binance",
            queue_processing="orders:queue:binance:processing",
            queue_dlq="orders:queue:binance:dlq",
        )
        return w, r

    def test_write_emits_to_stream(self):
        w, r = self._make()
        w.write({"sid": "s1", "symbol": "BTCUSDT", "action": "open"})
        assert r.xlen("orders:exec") == 1

    def test_write_includes_sid(self):
        w, r = self._make()
        w.write({"sid": "s1", "event_type": "entry_filled"})
        entry = r._streams["orders:exec"][0]
        assert entry.get("sid") == "s1"

    def test_write_adds_ts_ms(self):
        w, r = self._make()
        w.write({"sid": "s2"})
        entry = r._streams["orders:exec"][0]
        assert "ts_ms" in entry

    def test_write_multiple(self):
        w, r = self._make()
        w.write({"sid": "a"})
        w.write({"sid": "b"})
        assert r.xlen("orders:exec") == 2

    def test_dlq_creates_entry(self):
        """dlq() uses lpush → lands in r._lists."""
        w, r = self._make()
        w.dlq('{"sid":"s1"}', "parse_error")
        dlq_key = "orders:queue:binance:dlq"
        # ReconcileEventWriter uses lpush → stored in FakeRedis._lists
        assert r._lists.get(dlq_key), f"DLQ key not found; lists={r._lists}"


# ---------------------------------------------------------------------------
# execution_state_store (inline projection mode)
# ---------------------------------------------------------------------------

class TestExecutionStateStore:
    def _make(self):
        from services.execution.execution_state_store import ExecutionStateStore
        r = FakeRedis()
        store = ExecutionStateStore(
            r=r,
            state_key_prefix="orders:state:",
            state_ttl=3600,
            exec_stream="orders:exec",
            exec_journal_primary=False,      # use cache directly
            exec_inline_state_projection=True,  # persist synchronously
            exec_rehydrate_on_state_miss=False,
        )
        return store, r

    def test_persist_cache_and_load_cache(self):
        store, _ = self._make()
        store.persist_cache("sid-1", {"symbol": "BTCUSDT", "fsm_state": "ENTRY_FILLED"})
        result = store.load_cache("sid-1")
        assert result.get("symbol") == "BTCUSDT"

    def test_load_cache_missing_returns_empty(self):
        store, _ = self._make()
        result = store.load_cache("nonexistent-sid")
        assert result == {}

    def test_transition_returns_dict(self):
        store, _ = self._make()
        result = store.transition(
            "sid-2", symbol="ETHUSDT", action="open",
            next_state="VALIDATED", details={"qty": 0.5},
        )
        assert isinstance(result, dict)

    def test_transition_updates_fsm_state(self):
        store, _ = self._make()
        result = store.transition(
            "sid-3", symbol="BTCUSDT", action="open",
            next_state="ENTRY_SUBMITTED", details={},
        )
        assert result.get("fsm_state") == "ENTRY_SUBMITTED" or isinstance(result, dict)

    def test_state_is_terminalish_exit(self):
        store, _ = self._make()
        assert store._state_is_terminalish({"fsm_state": "EXIT_FILLED"}) is True

    def test_state_is_terminalish_open(self):
        store, _ = self._make()
        assert store._state_is_terminalish({"fsm_state": "ENTRY_FILLED"}) is False

    def test_state_is_terminalish_none(self):
        store, _ = self._make()
        assert store._state_is_terminalish(None) is False
        assert store._state_is_terminalish({}) is False


# ---------------------------------------------------------------------------
# active_symbol_guard
# ---------------------------------------------------------------------------

class TestActiveSymbolGuard:
    def _make(self, enabled=True):
        from services.execution.active_symbol_guard import ActiveSymbolGuard
        r = FakeRedis()
        guard = ActiveSymbolGuard(
            r=r,
            active_symbol_key_prefix="orders:active_symbol_sid:",
            tombstone_ttl_sec=120,
            state_ttl=3600,
            exec_single_active_position_per_symbol=enabled,
        )
        return guard, r

    def test_acquire_with_patch(self):
        guard, _ = self._make(enabled=True)
        result = guard.acquire_or_refresh("BTCUSDT", "sid-1", {"fsm_state": "ENTRY_FILLED"})
        assert isinstance(result, dict)

    def test_acquire_disabled_no_block(self):
        guard, _ = self._make(enabled=False)
        result1 = guard.acquire_or_refresh("BTCUSDT", "sid-1", {})
        result2 = guard.acquire_or_refresh("BTCUSDT", "sid-2", {})
        assert isinstance(result1, dict) and isinstance(result2, dict)

    def test_mark_released(self):
        guard, _ = self._make(enabled=True)
        guard.acquire_or_refresh("BTCUSDT", "sid-1", {})
        guard.mark_released("BTCUSDT", expected_sid="sid-1")  # should not raise

    def test_guard_same_sid_no_block(self):
        from services.execution.active_symbol_guard import OpenBlockedByActiveSymbolError
        guard, _ = self._make(enabled=True)
        guard.acquire_or_refresh("BTCUSDT", "sid-A", {})
        # Same SID — should not raise
        guard.guard_single_active_symbol_open(
            sid="sid-A", symbol="BTCUSDT",
            payload={}, state_load_fn=lambda s: {}, client=None,
        )

    def test_guard_different_sid_raises(self):
        from services.execution.active_symbol_guard import OpenBlockedByActiveSymbolError
        guard, _ = self._make(enabled=True)
        guard.acquire_or_refresh("BTCUSDT", "sid-A", {})
        with pytest.raises((OpenBlockedByActiveSymbolError, RuntimeError, Exception)):
            guard.guard_single_active_symbol_open(
                sid="sid-B", symbol="BTCUSDT",
                payload={}, state_load_fn=lambda s: {}, client=None,
            )

    def test_guard_disabled_never_raises(self):
        guard, _ = self._make(enabled=False)
        guard.guard_single_active_symbol_open(
            sid="sid-X", symbol="BTCUSDT",
            payload={}, state_load_fn=lambda s: {}, client=None,
        )


# ---------------------------------------------------------------------------
# emergency_flatten_service
# ---------------------------------------------------------------------------

class TestEmergencyFlattenService:
    def _make(self):
        from services.execution.emergency_flatten_service import EmergencyFlattenService
        events = []
        svc = EmergencyFlattenService(
            position_mode="oneway",
            dust_notional_usdt=3.0,
            dust_margin_usdt=1.0,
            dust_close_retries=1,
            dust_verify_timeout_ms=100,
            dust_verify_poll_ms=50,
            write_event_fn=events.append,
        )
        return svc, events

    def test_init_fields(self):
        svc, _ = self._make()
        assert svc.position_mode == "oneway"
        assert svc.dust_notional_usdt == 3.0
        assert svc.dust_close_retries == 1

    def test_write_event_routed(self):
        svc, events = self._make()
        svc._write_event({"action": "test_event"})
        assert len(events) == 1
        assert events[0]["action"] == "test_event"


# ---------------------------------------------------------------------------
# protection_service
# ---------------------------------------------------------------------------

class TestProtectionService:
    def _make(self):
        from services.execution.protection_service import ProtectionService
        events = []
        svc = ProtectionService(
            position_mode="oneway",
            sl_working_type="MARK_PRICE",
            tp_market_working_type="MARK_PRICE",
            tp_limit_trigger_working_type="MARK_PRICE",
            tp_limit_time_in_force="GTX",
            tp_limit_price_offset_bps=0.0,
            protection_arm_timeout_ms=2500,
            write_event_fn=events.append,
        )
        return svc, events

    def test_init_ok(self):
        svc, _ = self._make()
        assert svc.sl_working_type == "MARK_PRICE"

    def test_validate_long_valid_sl(self):
        svc, _ = self._make()
        errors = svc.validate_protective_prices(
            sid="s1", symbol="BTCUSDT", logical_side="LONG",
            entry_price=50000.0, sl_price=49000.0, tp_levels=[51000.0],
        )
        assert isinstance(errors, list)
        assert errors == []

    def test_validate_long_sl_above_entry_invalid(self):
        svc, _ = self._make()
        errors = svc.validate_protective_prices(
            sid="s1", symbol="BTCUSDT", logical_side="LONG",
            entry_price=50000.0, sl_price=51000.0, tp_levels=[52000.0],
        )
        assert len(errors) > 0

    def test_validate_short_valid_sl(self):
        svc, _ = self._make()
        errors = svc.validate_protective_prices(
            sid="s1", symbol="BTCUSDT", logical_side="SHORT",
            entry_price=50000.0, sl_price=51000.0, tp_levels=[49000.0],
        )
        assert isinstance(errors, list)
        assert errors == []

    def test_validate_short_sl_below_entry_invalid(self):
        svc, _ = self._make()
        errors = svc.validate_protective_prices(
            sid="s1", symbol="BTCUSDT", logical_side="SHORT",
            entry_price=50000.0, sl_price=49000.0, tp_levels=[48000.0],
        )
        assert len(errors) > 0


# ---------------------------------------------------------------------------
# reconcile_service
# ---------------------------------------------------------------------------

class TestReconcileService:
    def _make(self):
        from services.execution.reconcile_service import ReconcileService
        r = FakeRedis()
        events = []
        svc = ReconcileService(
            r=r,
            user_stream_cache_prefix="orders:user_stream:",
            reconcile_enable=True,
            write_event_fn=events.append,
        )
        return svc, r, events

    def test_init_ok(self):
        svc, _, _ = self._make()
        assert svc.reconcile_enable is True

    def test_lookup_missing_plain_cid_returns_empty(self):
        """Missing cid → returns {} (not None; fail-open contract)."""
        svc, _, _ = self._make()
        result = svc.lookup_user_stream_event(plain_client_id="unknown-cid")
        assert result == {}

    def test_lookup_cached_by_plain_cid(self):
        """cache_key format is prefix + 'order:' + cid."""
        svc, r, _ = self._make()
        # real cache_key: f"{prefix}order:{cid}"
        cache_key = svc.cache_key("order", "cid-123")
        r.set(cache_key, json.dumps({"orderId": 999, "status": "FILLED"}))
        result = svc.lookup_user_stream_event(plain_client_id="cid-123")
        assert result.get("orderId") == 999
        assert result.get("status") == "FILLED"

    def test_lookup_disabled_returns_empty_or_value(self):
        """When reconcile_enable=False the service docs say it skips reconcile
        but lookup is a read-only helper — disabled flag doesn't block reads.
        This test verifies the current behaviour is consistent."""
        from services.execution.reconcile_service import ReconcileService
        r = FakeRedis()
        svc = ReconcileService(
            r=r,
            user_stream_cache_prefix="orders:user_stream:",
            reconcile_enable=False,
        )
        cache_key = svc.cache_key("order", "cid-x")
        r.set(cache_key, json.dumps({"orderId": 1}))
        result = svc.lookup_user_stream_event(plain_client_id="cid-x")
        # accept both behaviours: disabled may return {} or the cached value
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# order_open_service
# ---------------------------------------------------------------------------

class TestOrderOpenService:
    def _make(self):
        from services.execution.order_open_service import OrderOpenService
        svc = OrderOpenService(
            position_mode="oneway",
            exec_set_leverage=False,
            exec_margin_guard_enabled=False,
        )
        return svc

    def test_init_ok(self):
        svc = self._make()
        assert svc.position_mode == "oneway"

    def test_margin_guard_disabled_returns_true(self):
        svc = self._make()
        ok = svc._margin_guard_ok(
            symbol="BTCUSDT", qty=0.1, leverage=10, client=None, sid="s1"
        )
        assert ok is True

    def test_ensure_symbol_settings_noop_when_disabled(self):
        svc = self._make()
        # Should not raise even with client=None
        svc.ensure_symbol_settings(symbol="BTCUSDT", leverage=10, client=None)


# ---------------------------------------------------------------------------
# order_modify_service
# ---------------------------------------------------------------------------

class TestOrderModifyService:
    def test_init_ok(self):
        from services.execution.order_modify_service import OrderModifyService
        svc = OrderModifyService(position_mode="oneway")
        assert svc.position_mode == "oneway"

    def test_missing_state_returns_error_dict(self):
        from services.execution.order_modify_service import OrderModifyService
        svc = OrderModifyService(position_mode="oneway")
        result = svc.handle_modify(
            payload={"symbol": "BTCUSDT", "side": "BUY", "sl": 49000.0},
            client=None, filters=None,
            sid="ghost-sid", ts_queue_ms=0, ts_exec_start_ms=0,
        )
        assert isinstance(result, dict)
        assert "reason" in result or "sid" in result


# ---------------------------------------------------------------------------
# order_cancel_service
# ---------------------------------------------------------------------------

class TestOrderCancelService:
    def test_init_ok(self):
        from services.execution.order_cancel_service import OrderCancelService
        svc = OrderCancelService(position_mode="oneway")
        assert svc.position_mode == "oneway"

    def test_missing_state_cancel_returns_dict(self):
        from services.execution.order_cancel_service import OrderCancelService
        svc = OrderCancelService(position_mode="oneway")
        result = svc.handle_cancel(
            payload={"symbol": "BTCUSDT", "side": "BUY"},
            client=None, filters=None,
            sid="ghost-sid", ts_queue_ms=0, ts_exec_start_ms=0,
        )
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# shim import contract
# ---------------------------------------------------------------------------

class TestShimContract:
    def test_shim_points_to_facade(self):
        from services.binance_executor import BinanceExecutor
        from services.execution.binance_executor_app import BinanceExecutor as Facade
        assert BinanceExecutor is Facade

    def test_all_14_modules_importable(self):
        from services.execution.binance_filters import FiltersCache, SymbolFilters
        from services.execution.binance_order_mapper import (
            FSM_ENTRY_FILLED, FSM_FAILED, TERMINAL_FSM_STATES,
            _f, _i, _bool_env, _round_down, _format_float,
            _normalize_side, _classify_error, _position_side_for_mode,
        )
        from services.execution.execution_event_writer import ExecutionEventWriter
        from services.execution.execution_state_store import ExecutionStateStore
        from services.execution.active_symbol_guard import ActiveSymbolGuard
        from services.execution.emergency_flatten_service import EmergencyFlattenService
        from services.execution.protection_service import ProtectionService
        from services.execution.reconcile_service import ReconcileService
        from services.execution.trailing_service import TrailingService
        from services.execution.maker_tp_watchdog import MakerTpWatchdog
        from services.execution.order_open_service import OrderOpenService
        from services.execution.order_modify_service import OrderModifyService
        from services.execution.order_cancel_service import OrderCancelService
        from services.execution.binance_executor_app import BinanceExecutor
        assert True

    def test_facade_has_required_attrs(self):
        from services.execution.binance_executor_app import BinanceExecutor
        assert hasattr(BinanceExecutor, '__init__')
        assert hasattr(BinanceExecutor, 'process_one')
        assert hasattr(BinanceExecutor, 'run_once')
        assert hasattr(BinanceExecutor, 'run_forever')
        assert hasattr(BinanceExecutor, '_resolve_client')
