"""Tests for TrailingStateWorker FSM.

TestComputeNewSl       — 8 unit tests for pure compute_new_sl function
TestTrailingStateIdempotency — 2 tests for SETNX dedup in _emit_command
TestTickRouting        — 3 tests for on_tick routing, shadow, stale
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import fakeredis
import pytest

from services.trailing_state_worker import (
    TrailingState,
    TrailingStateEnum,
    TrailingStateWorker,
    compute_new_sl,
    round_to_tick,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_long_state(**kwargs) -> TrailingState:
    defaults = dict(
        sid="sid-001",
        symbol="BTCUSDT",
        side="LONG",
        state=TrailingStateEnum.TRAILING_ACTIVE.value,
        entry_price=30_000.0,
        current_sl=29_500.0,
        last_sent_sl=29_500.0,
        atr_value=200.0,
        atr_mult=1.5,
        trail_distance=300.0,   # 200 * 1.5
        tick_size=0.1,
        min_move_ticks=5,
        min_update_interval_ms=3000,
        max_updates=30,
        updates_sent=0,
        high_watermark=30_000.0,
    )
    defaults.update(kwargs)
    return TrailingState(**defaults)


def _make_short_state(**kwargs) -> TrailingState:
    defaults = dict(
        sid="sid-002",
        symbol="ETHUSDT",
        side="SHORT",
        state=TrailingStateEnum.TRAILING_ACTIVE.value,
        entry_price=2_000.0,
        current_sl=2_100.0,
        last_sent_sl=2_100.0,
        atr_value=20.0,
        atr_mult=1.5,
        trail_distance=30.0,    # 20 * 1.5
        tick_size=0.01,
        min_move_ticks=5,
        min_update_interval_ms=3000,
        max_updates=30,
        updates_sent=0,
        low_watermark=2_000.0,
    )
    defaults.update(kwargs)
    return TrailingState(**defaults)


def _make_worker(fake: fakeredis.FakeRedis | None = None, **env_overrides) -> TrailingStateWorker:
    if fake is None:
        fake = fakeredis.FakeRedis(decode_responses=True)
    with patch.dict("os.environ", {
        "TRAILING_STATE_ENABLED": "1",
        "TRAILING_STATE_SHADOW": "1",
        **{k: str(v) for k, v in env_overrides.items()},
    }):
        worker = TrailingStateWorker(redis_client=fake)
    return worker


# ── TestComputeNewSl ──────────────────────────────────────────────────────────

class TestComputeNewSl:
    def test_long_hwm_rises_sl_moves_up(self):
        """LONG: price rises above hwm → candidate SL is higher than current."""
        state = _make_long_state(
            high_watermark=30_000.0,
            current_sl=29_500.0,
            last_sent_sl=29_500.0,
            trail_distance=300.0,
        )
        # Price rises to 31_000 → new hwm=31_000 → candidate = 31_000 - 300 = 30_700
        result = compute_new_sl(state, 31_000.0)
        assert result is not None
        assert result > state.current_sl
        assert abs(result - 30_700.0) < 1.0

    def test_long_price_below_hwm_no_move(self):
        """LONG: price below hwm → no SL move."""
        state = _make_long_state(
            high_watermark=31_000.0,
            current_sl=30_500.0,
            last_sent_sl=30_500.0,
            trail_distance=300.0,
        )
        # price = 30_800 < hwm=31_000 → candidate = 31_000-300=30_700 < current_sl=30_500? NO
        # Actually candidate=30_700 > current_sl=30_500, but the hwm doesn't rise so no new move
        # candidate is 30_700, current_sl=30_500 → 30_700 > 30_500, so we DO get a move
        # Let's set current_sl high enough to block
        state.current_sl = 30_800.0
        state.last_sent_sl = 30_800.0
        # candidate = 31_000 - 300 = 30_700 < current_sl=30_800 → None (never retreat)
        result = compute_new_sl(state, 30_900.0)
        assert result is None

    def test_long_sl_never_retreats(self):
        """LONG: candidate < current_sl → must return None."""
        state = _make_long_state(
            high_watermark=30_500.0,
            current_sl=30_300.0,
            last_sent_sl=30_300.0,
            trail_distance=300.0,
        )
        # new hwm = max(30_500, 30_400) = 30_500 → candidate = 30_500 - 300 = 30_200 < current_sl=30_300
        result = compute_new_sl(state, 30_400.0)
        assert result is None

    def test_short_lwm_falls_sl_moves_down(self):
        """SHORT: price falls below lwm → SL moves lower."""
        state = _make_short_state(
            low_watermark=2_000.0,
            current_sl=2_100.0,
            last_sent_sl=2_100.0,
            trail_distance=30.0,
        )
        # Price drops to 1_900 → new lwm=1_900 → candidate = 1_900 + 30 = 1_930 < current_sl=2_100
        result = compute_new_sl(state, 1_900.0)
        assert result is not None
        assert result < state.current_sl
        assert abs(result - 1_930.0) < 1.0

    def test_short_price_above_lwm_no_move(self):
        """SHORT: price above lwm → lwm doesn't fall → candidate >= current_sl → None."""
        state = _make_short_state(
            low_watermark=1_900.0,
            current_sl=1_950.0,
            last_sent_sl=1_950.0,
            trail_distance=30.0,
        )
        # price = 2_000 > lwm=1_900 → lwm stays 1_900 → candidate = 1_900 + 30 = 1_930 < current_sl=1_950
        # → that IS a valid move... set current_sl lower
        state.current_sl = 1_920.0
        state.last_sent_sl = 1_920.0
        # candidate = 1_930 >= current_sl=1_920 + min_move_ticks*tick_size=5*0.01=0.05 not blocked
        # candidate 1_930 > 1_920 → retreat (never rises) → None
        result = compute_new_sl(state, 2_000.0)
        assert result is None

    def test_short_sl_never_rises(self):
        """SHORT: candidate > current_sl → return None (SL must not rise for SHORT)."""
        state = _make_short_state(
            low_watermark=1_970.0,
            current_sl=1_980.0,
            last_sent_sl=1_980.0,
            trail_distance=30.0,
        )
        # lwm stays 1_970 (price=1_990 > lwm) → candidate = 1_970 + 30 = 2_000 > current_sl=1_980
        result = compute_new_sl(state, 1_990.0)
        assert result is None

    def test_min_move_ticks_prevents_noise(self):
        """Move smaller than min_move_ticks * tick_size → None."""
        state = _make_long_state(
            high_watermark=30_000.0,
            current_sl=29_500.0,
            last_sent_sl=29_500.0,
            trail_distance=300.0,
            tick_size=1.0,
            min_move_ticks=10,  # min move = 10 ticks = 10.0 price units
        )
        # price = 30_005 → new hwm = 30_005 → candidate = 30_005 - 300 = 29_705
        # abs(29_705 - 29_500) = 205 >> 10 → would pass ... let's make hwm close
        state.high_watermark = 29_804.0
        state.current_sl = 29_500.0
        state.last_sent_sl = 29_500.0
        # price = 29_808 → new hwm = 29_808 → candidate = 29_808 - 300 = 29_508
        # round_to_tick(29_508, 1.0) = 29_508
        # abs(29_508 - 29_500) = 8 < 10 → None
        result = compute_new_sl(state, 29_808.0)
        assert result is None

    def test_max_updates_blocks_further_moves(self):
        """updates_sent >= max_updates → None, no more moves."""
        state = _make_long_state(
            high_watermark=30_000.0,
            current_sl=29_500.0,
            last_sent_sl=29_500.0,
            trail_distance=300.0,
            updates_sent=30,
            max_updates=30,
        )
        result = compute_new_sl(state, 35_000.0)
        assert result is None


# ── TestTrailingStateIdempotency ──────────────────────────────────────────────

class TestTrailingStateIdempotency:
    def test_duplicate_command_setnx_blocks(self):
        """Same trail:cmd:{sid}:{pos}:{sl} key already in Redis → _emit_command returns False."""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake, TRAILING_STATE_SHADOW="0")
        worker.enabled = True
        worker.shadow = False

        state = _make_long_state(
            sid="sid-dedup",
            position_id="pos-001",
            current_sl=29_500.0,
        )

        # Pre-set the dedup key as if a command was already sent
        dedup_key = worker._dedup_key(state, 30_000.0)
        fake.set(dedup_key, "1", ex=300)

        result = worker._emit_command(state, 30_000.0, "watermark_advance")
        assert result is False

    def test_new_sl_different_key_allowed(self):
        """Different SL value → different key → command emitted (returns True)."""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake, TRAILING_STATE_SHADOW="0")
        worker.enabled = True
        worker.shadow = False

        state = _make_long_state(
            sid="sid-dedup2",
            position_id="pos-001",
            current_sl=29_500.0,
        )

        # Pre-set dedup for 30_000
        dedup_key = worker._dedup_key(state, 30_000.0)
        fake.set(dedup_key, "1", ex=300)

        # Different SL (30_100) → different key → allowed
        result = worker._emit_command(state, 30_100.0, "watermark_advance")
        assert result is True
        # Verify command was XADDed
        cmds = fake.xrange("events:trailing:commands", count=10)
        assert len(cmds) > 0


# ── TestTickRouting ───────────────────────────────────────────────────────────

class TestTickRouting:
    def _setup_active_state(
        self,
        fake: fakeredis.FakeRedis,
        worker: TrailingStateWorker,
        sid: str = "sid-tick-001",
        symbol: str = "BTCUSDT",
        side: str = "LONG",
    ) -> TrailingState:
        state = _make_long_state(
            sid=sid,
            symbol=symbol,
            side=side,
            high_watermark=30_000.0,
            current_sl=29_500.0,
            last_sent_sl=29_500.0,
            trail_distance=300.0,
            tick_size=0.1,
            min_move_ticks=5,
            min_update_interval_ms=0,  # no rate limit for tests
        )
        worker._save_state(state)
        worker._index_add(symbol, sid)
        return state

    def test_on_tick_long_moves_sl(self):
        """TRAILING_ACTIVE LONG state: price rises → compute_new_sl returns value → state updated."""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake, TRAILING_STATE_SHADOW="1")
        worker.enabled = True

        state = self._setup_active_state(fake, worker)
        now_ms = int(time.time() * 1000)

        # Price rises to 31_000: new hwm=31_000, candidate = 31_000-300=30_700
        moved = worker.on_tick("BTCUSDT", 31_000.0, now_ms)
        assert "sid-tick-001" in moved

        # Verify saved state
        updated = worker._load_state("sid-tick-001")
        assert updated is not None
        assert updated.current_sl > 29_500.0
        assert updated.updates_sent == 1

    def test_on_tick_shadow_no_command(self):
        """TRAILING_STATE_SHADOW=1 → compute runs but no XADD to commands stream."""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake, TRAILING_STATE_SHADOW="1")
        worker.enabled = True
        worker.shadow = True

        self._setup_active_state(fake, worker)
        now_ms = int(time.time() * 1000)

        worker.on_tick("BTCUSDT", 31_000.0, now_ms)

        # commands stream should be empty (shadow mode)
        cmds = fake.xrange("events:trailing:commands", count=10)
        assert len(cmds) == 0

    def test_on_tick_stale_price_skipped(self):
        """Price timestamp older than TRAILING_PRICE_STALE_MS → skip, no state update."""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake, TRAILING_STATE_SHADOW="1", TRAILING_PRICE_STALE_MS="3000")
        worker.enabled = True

        self._setup_active_state(fake, worker)

        # Stale timestamp: 10 seconds ago
        stale_ts_ms = int(time.time() * 1000) - 10_000

        moved = worker.on_tick("BTCUSDT", 31_000.0, stale_ts_ms)
        assert moved == []

        # State should not have been updated
        state = worker._load_state("sid-tick-001")
        assert state is not None
        assert state.updates_sent == 0
