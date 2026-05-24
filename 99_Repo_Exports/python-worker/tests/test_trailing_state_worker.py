"""Tests for TrailingStateWorker public event handlers, rate-limit, shadow vs live
emit, and tp_event_listener wiring to dispatch_event.

These complement tests/test_trailing_state_machine.py which covers compute_new_sl,
idempotency primitives, and on_tick routing basics.
"""

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock

import fakeredis
import pytest


@pytest.fixture
def fake_redis():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def worker(fake_redis, monkeypatch):
    monkeypatch.setenv("TRAILING_STATE_ENABLED", "1")
    monkeypatch.setenv("TRAILING_STATE_SHADOW", "1")
    monkeypatch.setenv("TRAILING_MIN_MOVE_TICKS", "5")
    monkeypatch.setenv("TRAILING_MIN_UPDATE_INTERVAL_MS", "3000")
    monkeypatch.setenv("TRAILING_MAX_UPDATES_PER_POSITION", "30")
    monkeypatch.setenv("TRAILING_PRICE_STALE_MS", "3000")
    from services.trailing_state_worker import TrailingStateWorker
    return TrailingStateWorker(redis_client=fake_redis)


# ──────────────────────────────────────────────────────────────────────────────
# TestTpHitCreatesState
# ──────────────────────────────────────────────────────────────────────────────

class TestTpHitCreatesState:
    def test_tp_hit_creates_state_long(self, worker, fake_redis):
        from services.trailing_state_worker import TrailingStateEnum

        state = worker.on_tp_hit({
            "sid": "s1",
            "symbol": "BTCUSDT",
            "side": "LONG",
            "entry_price": 100000,
            "current_sl": 99800,
            "atr_value": 120,
            "atr_mult": 1.2,
            "position_id": "p1",
            "price": 100100,
        })

        assert state is not None
        assert state.state == TrailingStateEnum.TRAILING_ACTIVE.value
        # trail_distance = atr_value * atr_mult = 120 * 1.2 = 144
        assert state.trail_distance == pytest.approx(144.0)
        assert state.high_watermark == pytest.approx(100100.0)
        assert "s1" in worker._symbol_index.get("BTCUSDT", set())
        # Redis hash exists
        assert fake_redis.exists("trailing:state:s1") == 1
        data = fake_redis.hgetall("trailing:state:s1")
        assert data["sid"] == "s1"
        assert data["side"] == "LONG"
        assert data["state"] == TrailingStateEnum.TRAILING_ACTIVE.value

    def test_tp_hit_creates_state_short(self, worker, fake_redis):
        from services.trailing_state_worker import TrailingStateEnum

        state = worker.on_tp_hit({
            "sid": "s_short",
            "symbol": "ETHUSDT",
            "side": "SHORT",
            "entry_price": 2000,
            "current_sl": 2020,
            "atr_value": 10,
            "atr_mult": 1.5,
            "position_id": "p_short",
            "price": 1990,
        })

        assert state is not None
        assert state.state == TrailingStateEnum.TRAILING_ACTIVE.value
        assert state.low_watermark == pytest.approx(1990.0)
        assert "s_short" in worker._symbol_index.get("ETHUSDT", set())

    def test_tp_hit_skipped_when_disabled(self, worker, fake_redis):
        worker.enabled = False
        result = worker.on_tp_hit({
            "sid": "s2",
            "symbol": "BTCUSDT",
            "side": "LONG",
            "entry_price": 100000,
            "current_sl": 99800,
            "atr_value": 120,
            "atr_mult": 1.2,
            "position_id": "p2",
            "price": 100100,
        })

        assert result is None
        assert fake_redis.exists("trailing:state:s2") == 0


# ──────────────────────────────────────────────────────────────────────────────
# TestPositionClosedExitsState
# ──────────────────────────────────────────────────────────────────────────────

class TestPositionClosedExitsState:
    def test_position_closed_transitions_to_exited(self, worker, fake_redis):
        from services.trailing_state_worker import TrailingStateEnum

        worker.on_tp_hit({
            "sid": "s1",
            "symbol": "BTCUSDT",
            "side": "LONG",
            "entry_price": 100000,
            "current_sl": 99800,
            "atr_value": 120,
            "atr_mult": 1.2,
            "position_id": "p1",
            "price": 100100,
        })
        assert "s1" in worker._symbol_index.get("BTCUSDT", set())

        ok = worker.on_position_closed({
            "sid": "s1",
            "event_type": "POSITION_CLOSED",
            "symbol": "BTCUSDT",
        })
        assert ok is True

        data = fake_redis.hgetall("trailing:state:s1")
        assert data["state"] == TrailingStateEnum.EXITED.value
        assert "s1" not in worker._symbol_index.get("BTCUSDT", set())

    def test_position_closed_no_state_returns_false(self, worker):
        ok = worker.on_position_closed({
            "sid": "nonexistent",
            "event_type": "POSITION_CLOSED",
            "symbol": "BTCUSDT",
        })
        assert ok is False


# ──────────────────────────────────────────────────────────────────────────────
# TestMinUpdateIntervalBlocksSpam
# ──────────────────────────────────────────────────────────────────────────────

class TestMinUpdateIntervalBlocksSpam:
    def test_rapid_ticks_rate_limited(self, worker, fake_redis):
        # Create state with a price seed
        worker.on_tp_hit({
            "sid": "s_rate",
            "symbol": "BTCUSDT",
            "side": "LONG",
            "entry_price": 100000,
            "current_sl": 99800,
            "atr_value": 100,
            "atr_mult": 1.0,            # trail_distance = 100
            "tick_size": 0.1,
            "position_id": "p_rate",
            "price": 100000,
        })
        # Make sure interval is what we expect
        assert worker.min_update_interval_ms == 3000

        now_ms = int(time.time() * 1000)

        # First tick: large price jump → SL should move
        moved1 = worker.on_tick("BTCUSDT", 101000.0, now_ms)
        assert "s_rate" in moved1, f"expected first tick to move SL, got {moved1}"

        # State must reflect last_cmd_ts_ms now set
        from services.trailing_state_worker import TrailingState
        data = fake_redis.hgetall("trailing:state:s_rate")
        state = TrailingState.from_dict(data)
        assert state.last_cmd_ts_ms > 0

        # Second tick: immediate (same now_ms), price moves further up — should be rate-limited
        moved2 = worker.on_tick("BTCUSDT", 102000.0, now_ms)
        assert moved2 == [], f"expected rate-limit to block, got {moved2}"

    def test_tick_allowed_after_interval(self, worker, fake_redis):
        worker.on_tp_hit({
            "sid": "s_after",
            "symbol": "BTCUSDT",
            "side": "LONG",
            "entry_price": 100000,
            "current_sl": 99800,
            "atr_value": 100,
            "atr_mult": 1.0,
            "tick_size": 0.1,
            "position_id": "p_after",
            "price": 100000,
        })

        now_ms = int(time.time() * 1000)

        moved1 = worker.on_tick("BTCUSDT", 101000.0, now_ms)
        assert "s_after" in moved1

        # Simulate the passing of time by editing the stored last_cmd_ts_ms
        # to (now_ms - 4000) so the interval elapsed
        from services.trailing_state_worker import TrailingState
        data = fake_redis.hgetall("trailing:state:s_after")
        state = TrailingState.from_dict(data)
        state.last_cmd_ts_ms = now_ms - 4000  # 4s ago > 3000ms interval
        # persist
        fake_redis.hset("trailing:state:s_after", mapping=state.to_dict())

        # Now a further uptick should be permitted
        moved2 = worker.on_tick("BTCUSDT", 102000.0, now_ms)
        assert "s_after" in moved2, f"expected SL move after interval, got {moved2}"


# ──────────────────────────────────────────────────────────────────────────────
# TestShadowVsLiveCommand
# ──────────────────────────────────────────────────────────────────────────────

class TestShadowVsLiveCommand:
    def test_shadow_mode_no_command_emitted(self, worker, fake_redis):
        assert worker.shadow is True
        worker.on_tp_hit({
            "sid": "s_shadow",
            "symbol": "BTCUSDT",
            "side": "LONG",
            "entry_price": 100000,
            "current_sl": 99800,
            "atr_value": 100,
            "atr_mult": 1.0,
            "tick_size": 0.1,
            "position_id": "p_shadow",
            "price": 100000,
        })

        # baseline xlen on commands stream
        assert fake_redis.xlen("events:trailing:commands") == 0

        moved = worker.on_tick(
            "BTCUSDT", 101000.0, int(time.time() * 1000),
        )
        assert "s_shadow" in moved

        # Shadow → no command in events:trailing:commands
        assert fake_redis.xlen("events:trailing:commands") == 0
        # But audit stream MUST have entries (TRAILING_ACTIVE transition + SL_MOVE)
        assert fake_redis.xlen("events:trailing:state") >= 1

    def test_live_mode_emits_command(self, worker, fake_redis):
        worker.shadow = False
        worker.on_tp_hit({
            "sid": "s_live",
            "symbol": "BTCUSDT",
            "side": "LONG",
            "entry_price": 100000,
            "current_sl": 99800,
            "atr_value": 100,
            "atr_mult": 1.0,
            "tick_size": 0.1,
            "position_id": "p_live",
            "price": 100000,
        })

        moved = worker.on_tick(
            "BTCUSDT", 101000.0, int(time.time() * 1000),
        )
        assert "s_live" in moved

        assert fake_redis.xlen("events:trailing:commands") >= 1
        entries = fake_redis.xrange("events:trailing:commands")
        # last entry — verify payload
        _id, payload = entries[-1]
        assert payload["sid"] == "s_live"
        assert payload["symbol"] == "BTCUSDT"
        assert "new_sl" in payload
        assert payload["reason_code"] == "watermark_advance"
        assert payload["shadow"] == "0"

    def test_emit_command_duplicate_setnx_blocks(self, worker, fake_redis):
        from services.trailing_state_worker import TrailingState, TrailingStateEnum

        state = TrailingState(
            sid="s_dup",
            position_id="p_dup",
            symbol="BTCUSDT",
            side="LONG",
            state=TrailingStateEnum.TRAILING_ACTIVE.value,
            entry_price=100.0,
            current_sl=99.0,
            tick_size=0.1,
        )

        ok1 = worker._emit_command(state, 100.5, "reason1")
        ok2 = worker._emit_command(state, 100.5, "reason1")

        assert ok1 is True
        assert ok2 is False
        assert fake_redis.xlen("events:trailing:commands") == 1


# ──────────────────────────────────────────────────────────────────────────────
# TestListenerDispatchesToTsw
# ──────────────────────────────────────────────────────────────────────────────

class TestListenerDispatchesToTsw:
    def test_listener_calls_dispatch_event_when_tsw_set(self, fake_redis):
        from services.tp_event_listener import TPEventListener

        listener = TPEventListener.__new__(TPEventListener)
        listener.r = fake_redis
        listener.stats = {
            "messages_read": 0,
            "messages_processed": 0,
            "messages_acked": 0,
            "errors": 0,
            "last_message_ts": 0,
        }
        listener.orchestrator = MagicMock()
        listener.orchestrator.handle_event.return_value = type(
            "R", (), {"success": True, "skipped": False, "error": None}
        )()
        listener.events_stream = "events:trades"
        listener.consumer_group = "g"
        listener.consumer_name = "c"
        listener._tsw = MagicMock()
        listener._push_listener_dlq = MagicMock(return_value=True)
        listener._xack = MagicMock()
        listener._check_poison_cap = MagicMock(return_value=False)

        listener._process_one_message(
            "msg-1",
            {"data": json.dumps({
                "event_type": "TP1_HIT",
                "sid": "s1",
                "symbol": "BTCUSDT",
                "side": "LONG",
            })},
        )

        listener._tsw.dispatch_event.assert_called_once()
        call_event = listener._tsw.dispatch_event.call_args[0][0]
        assert call_event["event_type"] == "TP1_HIT"
        assert call_event["sid"] == "s1"
        assert call_event["symbol"] == "BTCUSDT"


class TestDispatchBeforeXack:
    """Audit §4.3: dispatch_event must run BEFORE XACK so worker failures don't
    swallow events. In LIVE mode a dispatch error must push to DLQ before ACK."""

    def _build_listener(self, fake_redis):
        from services.tp_event_listener import TPEventListener
        listener = TPEventListener.__new__(TPEventListener)
        listener.r = fake_redis
        listener.stats = {"messages_read":0, "messages_processed":0, "messages_acked":0, "errors":0, "last_message_ts":0}
        listener.orchestrator = MagicMock()
        listener.orchestrator.handle_event.return_value = type("R", (), {"success":True, "skipped":False, "error":None})()
        listener.events_stream = "events:trades"
        listener.consumer_group = "g"
        listener.consumer_name = "c"
        listener._push_listener_dlq = MagicMock(return_value=True)
        listener._xack = MagicMock()
        listener._check_poison_cap = MagicMock(return_value=False)
        return listener

    def test_dispatch_called_before_xack(self, fake_redis):
        """Order of operations: dispatch_event() must precede _xack()."""
        listener = self._build_listener(fake_redis)
        call_order: list[str] = []
        listener._tsw = MagicMock()
        listener._tsw.dispatch_event = MagicMock(side_effect=lambda e: call_order.append("dispatch"))
        listener._xack = MagicMock(side_effect=lambda mid: call_order.append("xack"))

        listener._process_one_message(
            "msg-1",
            {"data": json.dumps({"event_type":"TP1_HIT", "sid":"s1", "symbol":"BTCUSDT", "side":"LONG"})},
        )

        assert call_order == ["dispatch", "xack"], f"expected dispatch→xack, got {call_order}"

    def test_dispatch_exception_in_shadow_still_acks(self, fake_redis):
        """Shadow mode (shadow=True): dispatch exception → log only, still ACK."""
        listener = self._build_listener(fake_redis)
        listener._tsw = MagicMock()
        listener._tsw.shadow = True
        listener._tsw.dispatch_event = MagicMock(side_effect=RuntimeError("boom"))

        listener._process_one_message(
            "msg-2",
            {"data": json.dumps({"event_type":"TP1_HIT", "sid":"s2", "symbol":"BTCUSDT", "side":"LONG"})},
        )

        listener._xack.assert_called_once()
        listener._push_listener_dlq.assert_not_called()

    def test_dispatch_exception_in_live_pushes_dlq(self, fake_redis):
        """Live mode (shadow=False): dispatch exception → DLQ, then ACK."""
        listener = self._build_listener(fake_redis)
        listener._tsw = MagicMock()
        listener._tsw.shadow = False
        listener._tsw.dispatch_event = MagicMock(side_effect=RuntimeError("boom"))

        listener._process_one_message(
            "msg-3",
            {"data": json.dumps({"event_type":"TP1_HIT", "sid":"s3", "symbol":"BTCUSDT", "side":"LONG"})},
        )

        listener._push_listener_dlq.assert_called_once()
        # DLQ reason should mention tsw_dispatch_error
        reason = listener._push_listener_dlq.call_args[0][2]
        assert "tsw_dispatch_error" in reason
        listener._xack.assert_called_once()  # DLQ ok → ACK to break loop

    def test_dispatch_exception_in_live_dlq_fail_no_ack(self, fake_redis):
        """Live mode + DLQ write fails → no ACK (message stays in PEL)."""
        listener = self._build_listener(fake_redis)
        listener._tsw = MagicMock()
        listener._tsw.shadow = False
        listener._tsw.dispatch_event = MagicMock(side_effect=RuntimeError("boom"))
        listener._push_listener_dlq = MagicMock(return_value=False)
        listener._xack = MagicMock()

        listener._process_one_message(
            "msg-4",
            {"data": json.dumps({"event_type":"TP1_HIT", "sid":"s4", "symbol":"BTCUSDT", "side":"LONG"})},
        )

        listener._push_listener_dlq.assert_called_once()
        listener._xack.assert_not_called()


class TestSymbolIndexRebuild:
    """Audit §8: _symbol_index must be rebuilt from Redis on init so worker
    restart resumes tick routing for active positions."""

    def test_rebuild_picks_up_active_states(self, fake_redis, monkeypatch):
        """States with state=trailing_active in Redis → indexed; others skipped."""
        monkeypatch.setenv("TRAILING_STATE_ENABLED", "1")
        from services.trailing_state_worker import TrailingStateWorker, TrailingStateEnum

        # Seed Redis with mix of active and non-active states
        fake_redis.hset("trailing:state:s_active1", mapping={
            "sid":"s_active1", "symbol":"BTCUSDT", "side":"LONG",
            "state":TrailingStateEnum.TRAILING_ACTIVE.value,
            "entry_price":"100000", "current_sl":"99800",
            "atr_value":"120", "atr_mult":"1.2",
        })
        fake_redis.hset("trailing:state:s_active2", mapping={
            "sid":"s_active2", "symbol":"ETHUSDT", "side":"SHORT",
            "state":TrailingStateEnum.TRAILING_ACTIVE.value,
            "entry_price":"2000", "current_sl":"2020",
            "atr_value":"5", "atr_mult":"1.0",
        })
        fake_redis.hset("trailing:state:s_exited", mapping={
            "sid":"s_exited", "symbol":"BTCUSDT", "side":"LONG",
            "state":TrailingStateEnum.EXITED.value,
            "entry_price":"100000", "current_sl":"99800",
        })

        worker = TrailingStateWorker(redis_client=fake_redis)
        # _symbol_index populated from scan
        assert "BTCUSDT" in worker._symbol_index
        assert "ETHUSDT" in worker._symbol_index
        assert "s_active1" in worker._symbol_index["BTCUSDT"]
        assert "s_active2" in worker._symbol_index["ETHUSDT"]
        # Exited state must NOT be indexed
        assert "s_exited" not in worker._symbol_index.get("BTCUSDT", set())

    def test_rebuild_empty_redis_no_index(self, fake_redis, monkeypatch):
        monkeypatch.setenv("TRAILING_STATE_ENABLED", "1")
        from services.trailing_state_worker import TrailingStateWorker
        worker = TrailingStateWorker(redis_client=fake_redis)
        assert worker._symbol_index == {}

    def test_rebuild_returns_active_count(self, fake_redis, monkeypatch):
        monkeypatch.setenv("TRAILING_STATE_ENABLED", "1")
        from services.trailing_state_worker import TrailingStateWorker, TrailingStateEnum
        fake_redis.hset("trailing:state:s1", mapping={
            "sid":"s1", "symbol":"BTCUSDT", "side":"LONG",
            "state":TrailingStateEnum.TRAILING_ACTIVE.value,
            "entry_price":"100", "current_sl":"99",
        })
        worker = TrailingStateWorker(redis_client=fake_redis)
        # Call directly to verify the return value
        worker._symbol_index = {}  # reset
        n = worker._rebuild_index_from_redis()
        assert n == 1
