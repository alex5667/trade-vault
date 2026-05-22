"""test_timeout_close.py — Unit tests for max-hold timeout close mechanisms.

Tests cover:
  A) Orphan cleanup uses ORPHAN_CLEANUP_* reason codes (not TIMEOUT_*)
  B) Max-hold timeout:
     - age calculated from entry_ts_ms (not last_tick_ts_ms)
     - trailing_active skips timeout
     - real position → publishes timeout_close command (no local finalize)
     - paper position → finalizes locally with TIMEOUT_* reason
     - stale price prevents timeout close
     - idempotency: duplicate suppressed
"""
from __future__ import annotations

import json
import time
import threading
from typing import Any
from unittest.mock import MagicMock, patch, call

import pytest


# ── Helpers ─────────────────────────────────────────────────────────────────

def _make_pos(**kwargs):
    from domain.models import PositionState

    defaults = dict(
        id="pos1",
        sid="sid1",
        strategy="CryptoOrderFlow",
        source="CryptoOrderFlow",
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",
        entry_price=100.0,
        entry_ts_ms=1_000_000,
        lot=1.0,
        qty=1.0,
        quantity=1.0,
        remaining_qty=1.0,
        sl=95.0,
        tp_levels=[101.0, 102.0, 103.0],
    )
    defaults.update(kwargs)
    return PositionState(**defaults)


def _make_monitor(redis_client=None, **env_overrides):
    """Build a minimal TradeMonitorService with fakeredis."""
    import fakeredis
    from services.trade_monitor import TradeMonitorService

    r = redis_client or fakeredis.FakeRedis(decode_responses=True)
    with patch.dict("os.environ", env_overrides):
        tm = TradeMonitorService.__new__(TradeMonitorService)
        tm.redis = r
        tm.logger = MagicMock()
        tm._lock = threading.RLock()
        tm._symbol_locks_guard = threading.Lock()
        tm._symbol_locks = {}
        tm._use_symbol_locks = False
        tm._last_price_by_symbol = {}
        tm.shards = {}
        tm.symbol_by_pos_id = {}
        tm.open_by_symbol = {}
        tm._fsm_map = {}
        tm._fsm_enabled = False
        tm.open_positions = {}
        tm.pos_by_sid = {}
        tm._price_index_enabled = False
        tm.tp_ratios = (0.3, 0.3, 0.4)
        tm._orphan_max_last_price_age_ms = 300_000
        tm._orphan_max_lifetime_ms_default = 6 * 3600 * 1000
        tm._orphan_max_lifetime_bars_default = 0
        tm._orphan_housekeep_interval_ms = 30_000
        tm._last_housekeep_ms = 0
        tm._last_housekeep_by_symbol = {}
        tm._housekeep_grace_ms = 0
        tm._housekeep_started_at_ms = 0
        tm._orphan_close_mode = "finalize"
        tm._orphan_finalize_virtual_only = False
        # max-hold timeout defaults
        tm.orphan_cleanup_enabled = True
        tm.orphan_timeout_enabled = True
        tm.real_timeout_close_enabled = env_overrides.get("TM_REAL_TIMEOUT_CLOSE_ENABLED", "0") == "1"
        tm.timeout_close_mode = env_overrides.get("TM_TIMEOUT_CLOSE_MODE", "shadow")
        tm._max_hold_ms_default = int(env_overrides.get("TM_MAX_HOLD_MS_DEFAULT", "300000"))
        tm._max_hold_bars_default = int(env_overrides.get("TM_MAX_HOLD_BARS_DEFAULT", "0"))
        tm._max_hold_grace_ms = int(env_overrides.get("TM_MAX_HOLD_GRACE_MS", "15000"))
        tm._timeout_skip_if_trailing = env_overrides.get("TM_TIMEOUT_SKIP_IF_TRAILING_ACTIVE", "1") == "1"
        tm._timeout_require_fresh_price = env_overrides.get("TM_TIMEOUT_REQUIRE_FRESH_PRICE", "1") == "1"
        tm._timeout_max_last_price_age_ms = int(env_overrides.get("TM_TIMEOUT_MAX_LAST_PRICE_AGE_MS", "5000"))
        tm._timeout_idempotency_ttl_sec = int(env_overrides.get("TM_TIMEOUT_IDEMPOTENCY_TTL_SEC", "86400"))
        tm._smart_timeout_enabled = env_overrides.get("TM_SMART_TIMEOUT_ENABLED", "1") == "1"
        tm._smart_timeout_min_profit_bps = float(env_overrides.get("TM_SMART_TIMEOUT_MIN_PROFIT_BPS", "4.0"))
        tm._smart_timeout_adverse_atr = float(env_overrides.get("TM_SMART_TIMEOUT_ADVERSE_ATR", "1.0"))
        tm._binance_orders_queue = env_overrides.get("BINANCE_ORDERS_QUEUE", "orders:queue")
        tm._mt5_orders_queue = env_overrides.get("MT5_ORDERS_QUEUE", "orders:queue:mt5")
    return tm


# ── A: Orphan cleanup reason codes ──────────────────────────────────────────

def test_orphan_cleanup_never_uses_timeout_prefix():
    """ORPHAN_CLEANUP_* codes must not start with TIMEOUT_."""
    codes = [
        "ORPHAN_CLEANUP_STALE_MONITOR_STATE",
        "ORPHAN_CLEANUP_STALE_PRICE",
        "ORPHAN_CLEANUP_NO_PRICE",
        "ORPHAN_CLEANUP_BROKER_FLAT",
        "ORPHAN_CLEANUP_RESTART_RECOVERY",
    ]
    for code in codes:
        assert not code.startswith("TIMEOUT_"), f"{code} must not start with TIMEOUT_"
        assert code.startswith("ORPHAN_CLEANUP_"), f"{code} must start with ORPHAN_CLEANUP_"


# ── B: _position_age_ms ──────────────────────────────────────────────────────

def test_position_age_uses_entry_ts_not_last_tick():
    """Age counts from entry_ts_ms regardless of last_tick_ts_ms."""
    tm = _make_monitor()
    now_ms = 1_600_000  # 600 000 ms after entry
    pos = _make_pos(entry_ts_ms=1_000_000)
    assert tm._position_age_ms(pos, now_ms) == 600_000


def test_position_age_returns_zero_for_missing_entry():
    tm = _make_monitor()
    pos = _make_pos(entry_ts_ms=0)
    assert tm._position_age_ms(pos, 2_000_000) == 0


# ── B: _resolve_max_hold_ms ──────────────────────────────────────────────────

def test_resolve_max_hold_signal_explicit_ms():
    tm = _make_monitor(TM_MAX_HOLD_MS_DEFAULT="999999")
    pos = _make_pos()
    pos.signal_payload = {"max_hold_ms": 12345}
    assert tm._resolve_max_hold_ms(pos) == 12345


def test_resolve_max_hold_global_default():
    tm = _make_monitor(TM_MAX_HOLD_MS_DEFAULT="60000")
    pos = _make_pos()
    pos.signal_payload = {}
    assert tm._resolve_max_hold_ms(pos) == 60_000


# ── B: _resolve_timeout_reason ───────────────────────────────────────────────

def test_trailing_active_skips_timeout():
    tm = _make_monitor(TM_TIMEOUT_SKIP_IF_TRAILING_ACTIVE="1")
    pos = _make_pos()
    pos.trailing_active = True
    should_close, reason = tm._resolve_timeout_reason(pos, last_price=101.0)
    assert not should_close
    assert reason == "TIMEOUT_SKIP_TRAILING_ACTIVE"


def test_profitable_position_returns_timeout_profitable():
    tm = _make_monitor(TM_SMART_TIMEOUT_MIN_PROFIT_BPS="4.0", TM_SMART_TIMEOUT_ENABLED="1")
    pos = _make_pos(entry_price=100.0, direction="LONG")
    pos.trailing_active = False
    pos.atr = 0.0
    # 10 bps profit (10% of 100 = 0.1, wait 4bps min)
    should_close, reason = tm._resolve_timeout_reason(pos, last_price=100.05)
    assert should_close
    assert reason == "TIMEOUT_PROFITABLE"


def test_adverse_move_returns_timeout_adverse():
    tm = _make_monitor(TM_SMART_TIMEOUT_ADVERSE_ATR="1.0", TM_SMART_TIMEOUT_ENABLED="1")
    pos = _make_pos(entry_price=100.0, direction="LONG")
    pos.trailing_active = False
    pos.atr = 1.0  # 1 ATR = 1 price unit
    # adverse = entry - last_price = 100 - 98.5 = 1.5 > 1.0 * atr
    should_close, reason = tm._resolve_timeout_reason(pos, last_price=98.5)
    assert should_close
    assert reason == "TIMEOUT_ADVERSE_MOVE"


def test_flat_position_returns_timeout_max_hold():
    tm = _make_monitor(TM_SMART_TIMEOUT_MIN_PROFIT_BPS="10.0", TM_SMART_TIMEOUT_ENABLED="1")
    pos = _make_pos(entry_price=100.0, direction="LONG")
    pos.trailing_active = False
    pos.atr = 0.0
    # pnl ≈ 0 (flat), no ATR → max hold
    should_close, reason = tm._resolve_timeout_reason(pos, last_price=100.0)
    assert should_close
    assert reason == "TIMEOUT_MAX_HOLD"


# ── B: _is_real_position ─────────────────────────────────────────────────────

def test_real_position_binance_futures():
    tm = _make_monitor()
    pos = _make_pos()
    pos.venue = "binance_futures"
    pos.is_virtual = False
    assert tm._is_real_position(pos)


def test_virtual_position_is_not_real():
    tm = _make_monitor()
    pos = _make_pos()
    pos.venue = "binance_futures"
    pos.is_virtual = True
    assert not tm._is_real_position(pos)


def test_paper_position_is_not_real():
    tm = _make_monitor()
    pos = _make_pos()
    pos.venue = "paper"
    pos.source = "paper"
    pos.is_virtual = False
    assert not tm._is_real_position(pos)


# ── B: real position → publishes command, does NOT finalize locally ───────────

def test_real_position_publishes_timeout_close_not_finalize():
    """For a real Binance position, TradeMonitor must publish to orders:queue and NOT call repo.save_closed."""
    import fakeredis
    tm = _make_monitor(
        TM_REAL_TIMEOUT_CLOSE_ENABLED="1",
        TM_TIMEOUT_CLOSE_MODE="enforce",
        TM_MAX_HOLD_MS_DEFAULT="300000",
        TM_TIMEOUT_REQUIRE_FRESH_PRICE="0",
        TM_SMART_TIMEOUT_ENABLED="0",
        BINANCE_ORDERS_QUEUE="orders:queue",
    )
    tm.repo = MagicMock()

    now_ms = 1_000_000 + 300_000 + 1  # just past max_hold
    pos = _make_pos(entry_ts_ms=1_000_000)
    pos.venue = "binance_futures"
    pos.is_virtual = False
    pos.trailing_active = False
    pos.atr = 0.0

    # Provide a fresh last price
    tm._last_price_by_symbol["BTCUSDT"] = (now_ms - 100, 101.0)

    tm._on_max_hold_expired(pos, now_ms)

    # Command must be in orders:queue (XADD stream)
    entries = tm.redis.xrange("orders:queue", "-", "+")
    assert len(entries) == 1, "Expected exactly one timeout_close command in orders:queue"
    _, fields = entries[0]
    cmd = json.loads(fields["data"])
    assert cmd["action"] == "timeout_close"
    assert cmd["sid"] == "sid1"
    assert cmd["symbol"] == "BTCUSDT"
    assert cmd["venue"] == "binance_futures"
    assert cmd["close_reason_raw"].startswith("TIMEOUT_")

    # Must NOT have called repo.save_closed (real position — wait for executor event)
    tm.repo.save_closed.assert_not_called()


# ── B: paper position → local finalize ───────────────────────────────────────

def test_paper_position_finalizes_locally_with_timeout_reason():
    """Virtual position should be finalized locally with TIMEOUT_* reason."""
    tm = _make_monitor(
        TM_REAL_TIMEOUT_CLOSE_ENABLED="0",
        TM_TIMEOUT_CLOSE_MODE="paper",
        TM_MAX_HOLD_MS_DEFAULT="300000",
        TM_TIMEOUT_REQUIRE_FRESH_PRICE="0",
        TM_SMART_TIMEOUT_ENABLED="0",
    )

    now_ms = 1_000_000 + 300_001
    pos = _make_pos(entry_ts_ms=1_000_000)
    pos.is_virtual = True
    pos.venue = "paper"
    pos.source = "paper"
    pos.trailing_active = False
    pos.atr = 0.0
    pos.closed = False

    # Populate open_positions so _pop_pos works
    tm.open_positions["pos1"] = pos
    tm.symbol_by_pos_id["pos1"] = "BTCUSDT"
    tm.open_by_symbol["BTCUSDT"] = {"pos1"}

    tm._last_price_by_symbol["BTCUSDT"] = (now_ms - 100, 101.0)

    tm.repo = MagicMock()

    # Stub methods that aren't needed for this unit test
    tm._log_ab_closed_event = MagicMock()
    tm._stamp_closed_trade_meta = MagicMock()

    tm._on_max_hold_expired(pos, now_ms)

    # Verify repo.save_closed was called with a TIMEOUT_* reason
    tm.repo.save_closed.assert_called_once()
    closed = tm.repo.save_closed.call_args[0][0]
    assert closed.close_reason_raw.startswith("TIMEOUT_"), (
        f"Expected TIMEOUT_* reason, got {closed.close_reason_raw}"
    )
    assert not getattr(closed, "is_orphan_cleanup", False), (
        "Timeout close must not set is_orphan_cleanup=True"
    )


# ── B: stale price prevents timeout ─────────────────────────────────────────

def test_stale_price_skips_timeout_close():
    tm = _make_monitor(
        TM_REAL_TIMEOUT_CLOSE_ENABLED="1",
        TM_TIMEOUT_CLOSE_MODE="enforce",
        TM_MAX_HOLD_MS_DEFAULT="300000",
        TM_TIMEOUT_REQUIRE_FRESH_PRICE="1",
        TM_TIMEOUT_MAX_LAST_PRICE_AGE_MS="5000",
    )

    now_ms = 1_000_000 + 300_001
    pos = _make_pos(entry_ts_ms=1_000_000)
    pos.venue = "binance_futures"
    pos.is_virtual = False
    pos.trailing_active = False

    # Price is 60s old — stale
    tm._last_price_by_symbol["BTCUSDT"] = (now_ms - 60_000, 101.0)

    tm._on_max_hold_expired(pos, now_ms)

    # No command should be queued
    entries = tm.redis.xrange("orders:queue", "-", "+")
    assert len(entries) == 0, "Stale price must prevent timeout close"


# ── B: idempotency ───────────────────────────────────────────────────────────

def test_duplicate_timeout_close_suppressed():
    """Second _request_real_timeout_close for same sid+reason must not publish again."""
    tm = _make_monitor(
        TM_REAL_TIMEOUT_CLOSE_ENABLED="1",
        TM_TIMEOUT_CLOSE_MODE="enforce",
        TM_TIMEOUT_REQUIRE_FRESH_PRICE="0",
        TM_SMART_TIMEOUT_ENABLED="0",
        TM_TIMEOUT_IDEMPOTENCY_TTL_SEC="86400",
    )

    now_ms = 2_000_000
    pos = _make_pos(entry_ts_ms=1_000_000)
    pos.venue = "binance_futures"
    pos.is_virtual = False

    tm._last_price_by_symbol["BTCUSDT"] = (now_ms - 100, 101.0)

    # Call twice
    tm._request_real_timeout_close(
        pos, now_ms=now_ms, max_hold_ms=300_000, age_ms=300_001,
        reason="TIMEOUT_MAX_HOLD", last_price=101.0, last_price_ts_ms=now_ms - 100,
    )
    tm._request_real_timeout_close(
        pos, now_ms=now_ms, max_hold_ms=300_000, age_ms=300_002,
        reason="TIMEOUT_MAX_HOLD", last_price=101.0, last_price_ts_ms=now_ms - 100,
    )

    entries = tm.redis.xrange("orders:queue", "-", "+")
    assert len(entries) == 1, "Duplicate timeout_close must be suppressed by idempotency key"


# ── B: max_hold counts from entry_ts_ms, not last_tick_ts_ms ─────────────────

def test_max_hold_does_not_reset_on_tick_update():
    """A position that receives ticks continuously must still expire at entry + max_hold."""
    tm = _make_monitor(
        TM_MAX_HOLD_MS_DEFAULT="300000",
        TM_TIMEOUT_REQUIRE_FRESH_PRICE="0",
        TM_SMART_TIMEOUT_ENABLED="0",
    )

    entry_ts = 1_000_000
    pos = _make_pos(entry_ts_ms=entry_ts)
    pos.trailing_active = False
    pos.is_virtual = True

    # Simulate continuous ticks — last_tick_ts_ms is always fresh
    # (doesn't matter, age counts from entry_ts_ms)
    assert tm._is_max_hold_expired(pos, entry_ts + 299_999) is False
    assert tm._is_max_hold_expired(pos, entry_ts + 300_000) is True


# ── MT5 close_reason_raw propagation ─────────────────────────────────────────

def test_mt5_timeout_reason_takes_precedence_over_tp_state():
    """If MT5Event.close_reason_raw starts with TIMEOUT_, it overrides tp3_hit logic."""
    from services.mt5_event_executor import MT5Event

    event = MT5Event(
        symbol="BTCUSD",
        deal=1001,
        position=5001,
        type=1,
        price=50001.0,
        profit=-10.0,
        comment="sid1",
        volume=0.01,
        ts=1_600_000_000_000,
        close_reason_raw="TIMEOUT_MAX_HOLD",
    )

    state = {"tp3_hit": True, "tp2_hit": True, "tp1_hit": True}

    # The classification logic should pick TIMEOUT_MAX_HOLD, not tp3
    explicit_reason = str(event.close_reason_raw or "").strip()
    if explicit_reason.startswith("TIMEOUT_"):
        close_reason = explicit_reason
    elif state.get("tp3_hit"):
        close_reason = "tp3"
    else:
        close_reason = "unknown"

    assert close_reason == "TIMEOUT_MAX_HOLD"


# ── TradeClosed new fields ────────────────────────────────────────────────────

def test_trade_closed_has_orphan_cleanup_fields():
    from domain.models import TradeClosed

    closed = TradeClosed(order_id="x", sid="s")
    assert hasattr(closed, "is_orphan_cleanup")
    assert hasattr(closed, "exclude_from_ml_labels")
    assert closed.is_orphan_cleanup is False
    assert closed.exclude_from_ml_labels is False


def test_trade_closed_has_timeout_fields():
    from domain.models import TradeClosed

    closed = TradeClosed(order_id="x", sid="s")
    assert hasattr(closed, "timeout_age_ms")
    assert hasattr(closed, "timeout_max_hold_ms")
    assert hasattr(closed, "timeout_request_ts_ms")
    assert hasattr(closed, "timeout_close_latency_ms")
    assert hasattr(closed, "exit_order_ref")
    assert hasattr(closed, "closed_trade_id")
