"""Integration tests for the HWM (high-watermark) live trailing canary path.

Covers the full chain:
  TPEventListener._process_one_message
    → TrailingStateWorker.dispatch_event(TP1_HIT)
      → on_tp_hit → state created → trailing:state:{sid} written
    → TrailingStateWorker.on_tick → SL_MOVE audit + (live) XADD command
  TrailingCommandConsumer
    → _parse_command → OrderTrailingDispatcher.send_trailing_modify
    → XACK on success, DLQ on failure

Uses fakeredis (no docker, deterministic, CI-friendly).
"""

from __future__ import annotations

import json
import time

import fakeredis
import pytest
from unittest.mock import MagicMock


# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def fake_redis():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def mock_gateway():
    """Mock OrderTrailingDispatcher.send_trailing_modify() recorder.

    `send_trailing_modify` returns bool (truthy → success).
    """
    gw = MagicMock()
    gw.send_trailing_modify = MagicMock(return_value=True)
    return gw


@pytest.fixture
def worker(fake_redis, monkeypatch):
    """TrailingStateWorker in LIVE mode (SHADOW=0)."""
    monkeypatch.setenv("TRAILING_STATE_ENABLED", "1")
    monkeypatch.setenv("TRAILING_STATE_SHADOW", "0")  # LIVE for these tests
    monkeypatch.setenv("TRAILING_MIN_MOVE_TICKS", "1")
    monkeypatch.setenv("TRAILING_MIN_UPDATE_INTERVAL_MS", "0")
    monkeypatch.setenv("TRAILING_MAX_UPDATES_PER_POSITION", "30")
    monkeypatch.setenv("TRAILING_PRICE_STALE_MS", "999999999")
    from services.trailing_state_worker import TrailingStateWorker
    w = TrailingStateWorker(redis_client=fake_redis)
    # Defensive: explicit shadow=False in case autocal default flipped it
    w.shadow = False
    return w


@pytest.fixture
def consumer(fake_redis, mock_gateway, monkeypatch):
    """TrailingCommandConsumer with mocked gateway dispatcher."""
    monkeypatch.setenv("TCC_ENABLED", "1")
    monkeypatch.setenv("TCC_FOLLOW_AUTOCAL", "0")
    from services.trailing_command_consumer import TrailingCommandConsumer
    c = TrailingCommandConsumer(redis_client=fake_redis)
    # Inject mock gateway after construction (OrderTrailingDispatcher real
    # init may run inside __init__).
    c.dispatcher = mock_gateway
    c.force_enabled = True
    c.autocal_active = True
    return c


# ── Helpers ──────────────────────────────────────────────────────────────────


def _tp_hit_event(
    sid: str = "s1",
    symbol: str = "BTCUSDT",
    side: str = "LONG",
    entry_price: float = 100000.0,
    current_sl: float = 99800.0,
    atr_value: float = 120.0,
    atr_mult: float = 1.2,
    position_id: str = "p1",
    price: float = 100100.0,
    tick_size: float = 0.1,
):
    return {
        "event_type": "TP1_HIT",
        "sid": sid,
        "symbol": symbol,
        "side": side,
        "entry_price": entry_price,
        "current_sl": current_sl,
        "atr_value": atr_value,
        "atr_mult": atr_mult,
        "position_id": position_id,
        "price": price,
        "tick_size": tick_size,
        "profile": "default",
    }


# ── Tests: full chain ────────────────────────────────────────────────────────


class TestHwmLiveCanaryFullChain:
    """Worker emits HWM SL_MOVE → Consumer dispatches to gateway."""

    def test_tp_hit_then_tick_emits_command_to_gateway(
        self, worker, consumer, fake_redis, mock_gateway
    ):
        from services.trailing_state_worker import TrailingStateEnum

        # 1. TP1_HIT creates TRAILING_ACTIVE state
        state = worker.on_tp_hit(_tp_hit_event())
        assert state is not None
        assert state.state == TrailingStateEnum.TRAILING_ACTIVE.value

        # 2. Tick at higher price should move the SL
        result = worker.on_tick("BTCUSDT", 100500.0, int(time.time() * 1000))
        assert "s1" in result, f"expected sid 's1' to be moved, got {result}"

        # SL must have moved above original 99800
        loaded = worker._load_state("s1")
        assert loaded is not None
        assert loaded.current_sl > 99800.0, f"current_sl={loaded.current_sl}"

        # 3. Verify exactly one command was XADDed
        assert fake_redis.xlen("events:trailing:commands") == 1, (
            "expected 1 command on events:trailing:commands"
        )

        # 4. Parse and dispatch via the consumer (sidestep XREADGROUP plumbing)
        msgs = fake_redis.xrange("events:trailing:commands")
        assert len(msgs) == 1
        _msg_id, fields = msgs[0]

        cmd = consumer._parse_command(fields)
        assert cmd is not None, f"parse_command returned None for {fields}"
        success, err = consumer._dispatch(cmd)
        assert success, f"dispatch failed: {err}"

        # 5. Verify gateway was called with correct kwargs
        mock_gateway.send_trailing_modify.assert_called_once()
        call_kwargs = mock_gateway.send_trailing_modify.call_args.kwargs
        assert call_kwargs["sid"] == "s1"
        assert call_kwargs["symbol"] == "BTCUSDT"
        assert call_kwargs["side"] == "LONG"
        assert call_kwargs["position_id"] == "p1"
        # new_sl must match the candidate within tick rounding
        assert call_kwargs["new_sl"] == pytest.approx(loaded.current_sl, rel=1e-6)

    def test_gateway_failure_pushes_dlq(
        self, worker, consumer, fake_redis, mock_gateway
    ):
        """Gateway returning False → command goes to DLQ."""
        # Force gateway to report failure
        mock_gateway.send_trailing_modify = MagicMock(return_value=False)
        consumer.dispatcher = mock_gateway

        # Bootstrap consumer group so _process_one_message can XACK
        consumer._ensure_group()

        # Run worker pipeline to produce one command
        worker.on_tp_hit(_tp_hit_event())
        worker.on_tick("BTCUSDT", 100500.0, int(time.time() * 1000))
        assert fake_redis.xlen("events:trailing:commands") == 1

        # Read into the consumer group so msg_id lives in PEL → can be XACKed
        resp = fake_redis.xreadgroup(
            consumer.group,
            consumer.consumer,
            streams={consumer.stream: ">"},
            count=10,
            block=10,
        )
        assert resp, "expected one batch from xreadgroup"
        _stream, batch = resp[0]
        assert len(batch) == 1
        msg_id, fields = batch[0]

        # Process: gateway returns False → DLQ + ACK
        consumer._process_one_message(msg_id, fields)

        # DLQ must contain exactly 1 message
        dlq_msgs = fake_redis.xrange(consumer.dlq_stream)
        assert len(dlq_msgs) == 1, f"expected 1 DLQ msg, got {len(dlq_msgs)}"
        _dlq_id, dlq_fields = dlq_msgs[0]
        reason = dlq_fields.get("reason", "")
        assert "dispatch_failed" in reason, f"unexpected DLQ reason: {reason}"

    def test_duplicate_command_not_dispatched_twice(
        self, worker, consumer, fake_redis, mock_gateway
    ):
        """SETNX dedup blocks a second emit at the same rounded SL value."""
        worker.on_tp_hit(_tp_hit_event())
        worker.on_tick("BTCUSDT", 100500.0, int(time.time() * 1000))
        assert fake_redis.xlen("events:trailing:commands") == 1

        # Attempt manual second emit with the SAME new_sl → SETNX must block
        state = worker._load_state("s1")
        assert state is not None
        emitted_again = worker._emit_command(state, state.current_sl, "watermark_advance")
        assert emitted_again is False, "duplicate emit should be blocked by SETNX"

        # Stream length must still be 1
        assert fake_redis.xlen("events:trailing:commands") == 1

        # Now dispatch the single message to the gateway → exactly one call
        msgs = fake_redis.xrange("events:trailing:commands")
        _msg_id, fields = msgs[0]
        cmd = consumer._parse_command(fields)
        assert cmd is not None
        consumer._dispatch(cmd)
        assert mock_gateway.send_trailing_modify.call_count == 1

    def test_shadow_mode_no_gateway_call(
        self, worker, consumer, fake_redis, mock_gateway
    ):
        """When worker.shadow=True, no command is XADDed → gateway never called."""
        worker.shadow = True  # override LIVE → SHADOW

        worker.on_tp_hit(_tp_hit_event())
        result = worker.on_tick("BTCUSDT", 100500.0, int(time.time() * 1000))

        # SL still moves internally (audit only)
        assert "s1" in result

        # But no command emitted → no XADD on the commands stream
        assert fake_redis.xlen("events:trailing:commands") == 0

        # And gateway was never invoked
        mock_gateway.send_trailing_modify.assert_not_called()


# ── Tests: autocal integration ───────────────────────────────────────────────


class TestHwmLiveCanaryAutocalIntegration:
    """Autocal shadow=false → consumer activates; shadow=true → suspends."""

    def test_autocal_promote_activates_consumer_processing(
        self, fake_redis, mock_gateway, monkeypatch
    ):
        monkeypatch.setenv("TCC_ENABLED", "0")
        monkeypatch.setenv("TCC_FOLLOW_AUTOCAL", "1")
        from services.trailing_command_consumer import TrailingCommandConsumer

        c = TrailingCommandConsumer(redis_client=fake_redis)
        c.dispatcher = mock_gateway
        # Initial state: no autocal key → inactive
        assert c.force_enabled is False
        assert c.follow_autocal is True
        c.autocal_active = False  # explicit baseline
        assert c.is_active is False

        # Promote: write autocal:trailing_state:state with shadow=false
        fake_redis.set("autocal:trailing_state:state", json.dumps({"shadow": False}))
        c._refresh_autocal_state()

        assert c.autocal_active is True
        assert c.is_active is True

        # Telegram notification should be on the notify stream
        msgs = fake_redis.xrange("notify:telegram")
        assert len(msgs) >= 1, "expected Telegram notification on promote"
        _, fields = msgs[-1]
        assert fields.get("subtype") == "trailing_cmd_consumer"
        assert "ACTIVATED" in fields.get("text", "")

    def test_autocal_rollback_suspends_consumer(
        self, fake_redis, mock_gateway, monkeypatch
    ):
        monkeypatch.setenv("TCC_ENABLED", "0")
        monkeypatch.setenv("TCC_FOLLOW_AUTOCAL", "1")
        from services.trailing_command_consumer import TrailingCommandConsumer

        c = TrailingCommandConsumer(redis_client=fake_redis)
        c.dispatcher = mock_gateway
        # Start active (simulate prior promote)
        c.autocal_active = True
        assert c.is_active is True

        # Roll back: shadow=true
        fake_redis.set("autocal:trailing_state:state", json.dumps({"shadow": True}))
        c._refresh_autocal_state()

        assert c.autocal_active is False
        assert c.is_active is False

        msgs = fake_redis.xrange("notify:telegram")
        assert len(msgs) >= 1
        _, fields = msgs[-1]
        assert fields.get("subtype") == "trailing_cmd_consumer"
        assert "SUSPENDED" in fields.get("text", "")

    def test_autocal_no_state_keeps_inactive(
        self, fake_redis, mock_gateway, monkeypatch
    ):
        monkeypatch.setenv("TCC_ENABLED", "0")
        monkeypatch.setenv("TCC_FOLLOW_AUTOCAL", "1")
        from services.trailing_command_consumer import TrailingCommandConsumer

        c = TrailingCommandConsumer(redis_client=fake_redis)
        c.dispatcher = mock_gateway
        # No autocal key written
        assert fake_redis.get("autocal:trailing_state:state") is None

        c._refresh_autocal_state()
        assert c.autocal_active is False
        assert c.is_active is False


# ── Tests: worker restart / resume ───────────────────────────────────────────


class TestHwmLiveCanaryWorkerRestart:
    """Worker restart rebuilds _symbol_index from Redis."""

    def test_worker_restart_resumes_active_states(self, fake_redis, monkeypatch):
        """Pre-seeded TRAILING_ACTIVE state must be picked up on new worker init."""
        from services.trailing_state_worker import (
            TrailingState,
            TrailingStateEnum,
            TrailingStateWorker,
        )

        monkeypatch.setenv("TRAILING_STATE_ENABLED", "1")
        monkeypatch.setenv("TRAILING_STATE_SHADOW", "0")
        monkeypatch.setenv("TRAILING_MIN_MOVE_TICKS", "1")
        monkeypatch.setenv("TRAILING_MIN_UPDATE_INTERVAL_MS", "0")
        monkeypatch.setenv("TRAILING_PRICE_STALE_MS", "999999999")

        now_ms = int(time.time() * 1000)
        state = TrailingState(
            sid="s_active",
            position_id="p_active",
            symbol="BTCUSDT",
            side="LONG",
            state=TrailingStateEnum.TRAILING_ACTIVE.value,
            entry_price=100000.0,
            current_sl=99800.0,
            atr_value=120.0,
            atr_mult=1.2,
            trail_distance=144.0,
            tick_size=0.1,
            min_move_ticks=1,
            min_update_interval_ms=0,
            max_updates=30,
            high_watermark=100100.0,
            profile="default",
            created_ts_ms=now_ms,
            updated_ts_ms=now_ms,
        )
        fake_redis.hset(f"trailing:state:s_active", mapping=state.to_dict())  # type: ignore[arg-type]

        # New worker reads pre-existing state
        w = TrailingStateWorker(redis_client=fake_redis)
        w.shadow = False  # ensure LIVE for cmd emission

        assert "BTCUSDT" in w._symbol_index, f"_symbol_index={w._symbol_index}"
        assert "s_active" in w._symbol_index["BTCUSDT"]

        # Tick at a higher price triggers a move for the resumed state
        moved = w.on_tick("BTCUSDT", 100600.0, int(time.time() * 1000))
        assert "s_active" in moved
        # And a command was emitted to events:trailing:commands
        assert fake_redis.xlen("events:trailing:commands") == 1

    def test_worker_restart_skips_exited_states(self, fake_redis, monkeypatch):
        """EXITED states must NOT be re-indexed for tick routing."""
        from services.trailing_state_worker import (
            TrailingState,
            TrailingStateEnum,
            TrailingStateWorker,
        )

        monkeypatch.setenv("TRAILING_STATE_ENABLED", "1")
        monkeypatch.setenv("TRAILING_STATE_SHADOW", "0")
        monkeypatch.setenv("TRAILING_PRICE_STALE_MS", "999999999")

        now_ms = int(time.time() * 1000)
        exited = TrailingState(
            sid="s_exited",
            position_id="p_exited",
            symbol="BTCUSDT",
            side="LONG",
            state=TrailingStateEnum.EXITED.value,
            entry_price=100000.0,
            current_sl=99800.0,
            atr_value=120.0,
            atr_mult=1.2,
            tick_size=0.1,
            created_ts_ms=now_ms,
            updated_ts_ms=now_ms,
        )
        fake_redis.hset(f"trailing:state:s_exited", mapping=exited.to_dict())  # type: ignore[arg-type]

        w = TrailingStateWorker(redis_client=fake_redis)

        # _symbol_index must NOT contain the exited state
        assert "BTCUSDT" not in w._symbol_index or "s_exited" not in w._symbol_index.get("BTCUSDT", set())

        # on_tick returns empty list (nothing to process)
        moved = w.on_tick("BTCUSDT", 100600.0, int(time.time() * 1000))
        assert moved == []
