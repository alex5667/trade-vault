"""
tests/test_orphan_timeout_fixes.py

Прямые тесты методов orphan-timeout в TradeMonitorService:
  - _is_orphan_expired       — TTL считается от entry_ts_ms (не last_tick_ts_ms)
  - _resolve_orphan_ttl_ms   — приоритет TTL: signal > bars > global
  - Smart Timeout block      — hold если breakeven и нет риска
  - close_reason             — ORPHAN_TIMEOUT / STALE_PRICE / NO_PRICE
"""
import os
import types
import threading
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_svc(**overrides):
    """Minimal TradeMonitorService with just orphan-related attrs."""
    from services.trade_monitor import TradeMonitorService

    svc = TradeMonitorService.__new__(TradeMonitorService)
    svc._lock = threading.RLock()
    svc.orphan_timeout_enabled = True
    svc._orphan_max_lifetime_ms_default = overrides.get("max_lifetime_ms", 3_600_000)  # 1h
    svc._orphan_max_lifetime_bars_default = overrides.get("max_lifetime_bars", 0)
    svc._orphan_max_last_price_age_ms = overrides.get("max_price_age_ms", 300_000)   # 5m
    svc._orphan_close_mode = overrides.get("close_mode", "finalize")
    svc._orphan_finalize_virtual_only = overrides.get("finalize_virtual_only", False)
    svc._housekeep_grace_ms = 0          # disable grace period in tests
    svc._housekeep_started_at_ms = 1     # mark as started
    svc._last_price_by_symbol = overrides.get("last_prices", {})
    svc.shards = {}
    svc.open_positions = {}
    svc.open_by_symbol = {}
    svc.symbol_by_pos_id = {}
    svc._last_housekeep_ms = 0
    svc._last_housekeep_by_symbol = {}
    svc._orphan_housekeep_interval_ms = 0  # no throttle in tests
    svc._price_index_enabled = False
    svc._sl_index = {}
    svc._tp_index = {}
    svc.tp_ratios = (0.3, 0.35, 0.35)
    # Prometheus stubs
    _counter = MagicMock()
    _counter.labels = MagicMock(return_value=MagicMock(inc=MagicMock()))
    svc.tm_orphans_force_closed = _counter
    svc.tm_orphan_cleanup_duration_ms = MagicMock(set=MagicMock())
    log = types.SimpleNamespace(
        info=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        error=lambda *a, **k: None,
        debug=lambda *a, **k: None,
    )
    svc.logger = log
    return svc


def _pos(entry_ts_ms=1_000_000, tf="1m", trailing_active=False,
         signal_payload=None, closed=False, **kwargs):
    p = MagicMock()
    p.entry_ts_ms = entry_ts_ms
    p.last_tick_ts_ms = entry_ts_ms + 999_999_999  # far future — must NOT affect TTL
    p.last_update_ts_ms = entry_ts_ms + 999_999_999
    p.tf = tf
    p.trailing_active = trailing_active
    p.signal_payload = signal_payload or {}
    p.closed = closed
    p.entry_price = kwargs.get("entry_price", 100.0)
    p.direction = kwargs.get("direction", "LONG")
    p.atr = kwargs.get("atr", 0.0)
    p.symbol = kwargs.get("symbol", "BTCUSDT")
    p.is_virtual = kwargs.get("is_virtual", True)
    return p


# ---------------------------------------------------------------------------
# 1. _is_orphan_expired: TTL от entry_ts_ms
# ---------------------------------------------------------------------------

class TestIsOrphanExpiredUsesEntryTs:

    def test_not_expired_when_within_ttl(self):
        svc = _make_svc(max_lifetime_ms=3_600_000)
        pos = _pos(entry_ts_ms=1_000_000)
        now = 1_000_000 + 3_599_999  # 1ms до истечения
        assert svc._is_orphan_expired(pos, now) is False

    def test_expired_exactly_at_ttl(self):
        svc = _make_svc(max_lifetime_ms=3_600_000)
        pos = _pos(entry_ts_ms=1_000_000)
        now = 1_000_000 + 3_600_000  # ровно 1 час
        assert svc._is_orphan_expired(pos, now) is True

    def test_last_tick_ts_does_not_reset_timer(self):
        """Ключевой тест: тики не сбрасывают таймер."""
        svc = _make_svc(max_lifetime_ms=3_600_000)
        # last_tick_ts = entry + 3h (имитация непрерывных тиков)
        pos = _pos(entry_ts_ms=1_000_000)
        pos.last_tick_ts_ms = 1_000_000 + 3 * 3_600_000  # +3h тиков

        # now = entry + 1h → должен истечь по entry, несмотря на тики
        now = 1_000_000 + 3_600_000
        assert svc._is_orphan_expired(pos, now) is True

    def test_trailing_active_blocks_expiry(self):
        svc = _make_svc(max_lifetime_ms=3_600_000)
        pos = _pos(entry_ts_ms=1_000_000, trailing_active=True)
        now = 1_000_000 + 10 * 3_600_000  # +10h
        assert svc._is_orphan_expired(pos, now) is False

    def test_disabled_returns_false(self):
        svc = _make_svc(max_lifetime_ms=3_600_000)
        svc.orphan_timeout_enabled = False
        pos = _pos(entry_ts_ms=1_000_000)
        now = 1_000_000 + 10 * 3_600_000
        assert svc._is_orphan_expired(pos, now) is False

    def test_zero_entry_ts_not_expired(self):
        svc = _make_svc(max_lifetime_ms=3_600_000)
        pos = _pos(entry_ts_ms=0)
        now = 999_999_999
        assert svc._is_orphan_expired(pos, now) is False

    def test_closed_pos_not_expired(self):
        svc = _make_svc(max_lifetime_ms=3_600_000)
        pos = _pos(entry_ts_ms=1_000_000, closed=True)
        now = 1_000_000 + 10 * 3_600_000
        assert svc._is_orphan_expired(pos, now) is False

    def test_zero_ttl_not_expired(self):
        svc = _make_svc(max_lifetime_ms=0)
        pos = _pos(entry_ts_ms=1_000_000)
        now = 999_999_999
        assert svc._is_orphan_expired(pos, now) is False


# ---------------------------------------------------------------------------
# 2. _resolve_orphan_ttl_ms: приоритет TTL
# ---------------------------------------------------------------------------

class TestResolveTTL:

    def test_signal_orphan_ttl_ms_takes_priority(self):
        svc = _make_svc(max_lifetime_ms=3_600_000)
        pos = _pos(signal_payload={"orphan_ttl_ms": 7_200_000})
        assert svc._resolve_orphan_ttl_ms(pos) == 7_200_000

    def test_bars_ttl_used_when_no_explicit_ms(self):
        svc = _make_svc(max_lifetime_ms=3_600_000)
        pos = _pos(tf="5m", signal_payload={"max_lifetime_bars_after_entry": 12})
        # 12 bars × 5min = 3600s = 3_600_000ms
        assert svc._resolve_orphan_ttl_ms(pos) == 12 * 5 * 60_000

    def test_global_bars_used_when_enabled(self):
        svc = _make_svc(max_lifetime_ms=3_600_000, max_lifetime_bars=24)
        pos = _pos(tf="1m", signal_payload={})
        # 24 bars × 1min = 1_440_000ms
        assert svc._resolve_orphan_ttl_ms(pos) == 24 * 60_000

    def test_fallback_to_global_max_lifetime_ms(self):
        svc = _make_svc(max_lifetime_ms=3_600_000, max_lifetime_bars=0)
        pos = _pos(signal_payload={})
        assert svc._resolve_orphan_ttl_ms(pos) == 3_600_000

    def test_env_tm_orphan_max_lifetime_ms_respected(self, monkeypatch):
        monkeypatch.setenv("TM_ORPHAN_MAX_LIFETIME_MS", "1800000")
        svc = _make_svc()
        # Rebuild the attr from env (как при реальном init)
        svc._orphan_max_lifetime_ms_default = int(os.getenv("TM_ORPHAN_MAX_LIFETIME_MS", str(6 * 3600_000)))
        pos = _pos(signal_payload={})
        assert svc._resolve_orphan_ttl_ms(pos) == 1_800_000

    def test_signal_priority_over_bars(self):
        svc = _make_svc(max_lifetime_ms=3_600_000)
        pos = _pos(tf="1m", signal_payload={
            "orphan_ttl_ms": 120_000,
            "max_lifetime_bars_after_entry": 100,
        })
        assert svc._resolve_orphan_ttl_ms(pos) == 120_000


# ---------------------------------------------------------------------------
# 3. Smart Timeout — hold/close logic
# ---------------------------------------------------------------------------

class TestSmartTimeoutInHousekeep:
    """
    Тестируем smart timeout блок через _housekeep_expired_positions.
    Создаём минимальный svc с одной expired позицией и проверяем:
    - держит ли позицию при breakeven
    - закрывает при профите
    - закрывает при высоком drawdown
    """

    def _make_expired_svc(self, entry_price, last_price, atr=0.0, direction="LONG",
                          smart_enabled="1", min_pnl="10.0", mae_atr="1.0",
                          finalize_cb=None):
        """Build svc with one position that's already past TTL."""
        entry_ts = 1_000_000
        now_ms   = entry_ts + 3_600_001  # 1h + 1ms (past TTL)
        sym = "BTCUSDT"

        svc = _make_svc(
            max_lifetime_ms=3_600_000,
            last_prices={sym: (now_ms - 1000, last_price)},  # fresh price
            close_mode="finalize",
        )

        pos = _pos(
            entry_ts_ms=entry_ts,
            symbol=sym,
            entry_price=entry_price,
            direction=direction,
            atr=atr,
        )
        pos.id = "pos-1"
        pos.sid = "sid-1"
        pos.remaining_qty = 0.0
        pos.realized_pnl_gross = 0.0
        pos.trailing_active = False

        svc.shards[sym] = {"pos-1": pos}
        svc.open_positions["pos-1"] = pos
        svc.open_by_symbol[sym] = {"pos-1"}
        svc.symbol_by_pos_id["pos-1"] = sym

        return svc, pos, now_ms, sym

    def _run_housekeep(self, svc, now_ms, sym, monkeypatch,
                       smart_enabled="1", min_pnl="10.0", mae_atr="1.0"):
        """
        Patch env and run housekeep, capture which positions get closed.
        Returns list of (pos, reason) that were finalized.
        """
        closed_calls = []

        def fake_finalize(p, spec, exit_price, exit_ts_ms, close_reason_raw, tp_ratios):
            from domain.models import TradeClosed
            tc = MagicMock(spec=TradeClosed)
            tc.close_reason = close_reason_raw
            tc.close_reason_raw = close_reason_raw
            tc.close_reason_detail = close_reason_raw
            tc.symbol = p.symbol
            tc.pnl_net = 0.0
            tc.trailing_profile = ""
            closed_calls.append((p, close_reason_raw))
            return tc

        monkeypatch.setenv("TM_SMART_TIMEOUT_ENABLED", smart_enabled)
        monkeypatch.setenv("TM_SMART_TIMEOUT_PNL_BPS", min_pnl)
        monkeypatch.setenv("TM_SMART_TIMEOUT_MAE_ATR", mae_atr)

        MOD = "services.trade_monitor._monolith"
        with patch(f"{MOD}.finalize_trade", side_effect=fake_finalize), \
             patch(f"{MOD}.asdict", return_value={}), \
             patch.object(svc, "_get_spec", return_value=MagicMock(
                 pnl_money=lambda *a, **k: 0.0)), \
             patch.object(svc, "_io_save_closed", return_value=None), \
             patch.object(svc, "_log_ab_closed_event", return_value=None), \
             patch.object(svc, "_stamp_closed_trade_meta", return_value=None), \
             patch.object(svc, "_calc_commission_adjusted_exit_price",
                          return_value=99.9), \
             patch.object(svc, "_pop_pos", return_value=None), \
             patch.object(svc, "_get_symbol_lock",
                          return_value=threading.RLock()), \
             patch.object(svc, "_run_io_tasks", return_value=None), \
             patch.object(svc, "_update_stats", return_value=None), \
             patch.object(svc, "_fsm_transition", return_value=None), \
             patch(f"{MOD}.analytics_db") as _mock_db, \
             patch(f"{MOD}.get_ny_time_millis", return_value=now_ms):
            _mock_db.save_trade_closed = MagicMock()
            svc.redis = MagicMock()
            svc._io_save_closed = MagicMock()
            svc._housekeep_expired_positions(now_ms, current_symbol=sym)

        return closed_calls

    def test_profitable_position_is_closed(self, monkeypatch):
        """pnl_bps >= 10.0 → close allowed."""
        # entry=100, last=100.15 → pnl = +15 bps > 10 → close
        svc, pos, now_ms, sym = self._make_expired_svc(
            entry_price=100.0, last_price=100.15, atr=1.0)
        closed = self._run_housekeep(svc, now_ms, sym, monkeypatch, min_pnl="10.0")
        assert len(closed) == 1, "Profitable position should be closed"

    def test_breakeven_position_is_held(self, monkeypatch):
        """pnl_bps < 10 AND drawdown < 1 ATR → hold (no close)."""
        # entry=100, last=100.05 → pnl = +5 bps < 10, adverse=0 < 1 ATR
        svc, pos, now_ms, sym = self._make_expired_svc(
            entry_price=100.0, last_price=100.05, atr=1.0)
        closed = self._run_housekeep(svc, now_ms, sym, monkeypatch, min_pnl="10.0")
        assert len(closed) == 0, "Breakeven position should be held"

    def test_risky_position_is_closed(self, monkeypatch):
        """drawdown > 1 ATR → close even if not profitable."""
        # entry=100, last=98.5 → adverse=1.5 > 1.0 ATR → close
        svc, pos, now_ms, sym = self._make_expired_svc(
            entry_price=100.0, last_price=98.5, atr=1.0)
        closed = self._run_housekeep(svc, now_ms, sym, monkeypatch, min_pnl="10.0")
        assert len(closed) == 1, "Risky (deep loss) position should be closed"

    def test_no_atr_closes_unconditionally(self, monkeypatch):
        """atr == 0 → smart timeout не применяется → закрывает в любом случае."""
        svc, pos, now_ms, sym = self._make_expired_svc(
            entry_price=100.0, last_price=100.05, atr=0.0)
        closed = self._run_housekeep(svc, now_ms, sym, monkeypatch, min_pnl="10.0")
        assert len(closed) == 1, "No ATR → smart timeout skipped → close"

    def test_smart_timeout_disabled_closes_unconditionally(self, monkeypatch):
        """TM_SMART_TIMEOUT_ENABLED=0 → всегда закрывает."""
        svc, pos, now_ms, sym = self._make_expired_svc(
            entry_price=100.0, last_price=100.05, atr=1.0)
        closed = self._run_housekeep(svc, now_ms, sym, monkeypatch,
                                     smart_enabled="0", min_pnl="10.0")
        assert len(closed) == 1, "Disabled smart timeout → always close"

    def test_short_position_profitable(self, monkeypatch):
        """SHORT: entry=100, last=99.85 → pnl = +15 bps → close."""
        svc, pos, now_ms, sym = self._make_expired_svc(
            entry_price=100.0, last_price=99.85, atr=1.0, direction="SHORT")
        closed = self._run_housekeep(svc, now_ms, sym, monkeypatch, min_pnl="10.0")
        assert len(closed) == 1

    def test_short_position_held_at_breakeven(self, monkeypatch):
        """SHORT: entry=100, last=100.05 → pnl = -5 bps, adverse=0.05 < 1 ATR → hold."""
        svc, pos, now_ms, sym = self._make_expired_svc(
            entry_price=100.0, last_price=100.05, atr=1.0, direction="SHORT")
        closed = self._run_housekeep(svc, now_ms, sym, monkeypatch, min_pnl="10.0")
        assert len(closed) == 0


# ---------------------------------------------------------------------------
# 4. Close reason: ORPHAN_TIMEOUT / STALE_PRICE / NO_PRICE
# ---------------------------------------------------------------------------

class TestCloseReasons:

    def _run_and_get_reason(self, svc, now_ms, sym, monkeypatch):
        closed_calls = []

        def fake_finalize(p, spec, exit_price, exit_ts_ms, close_reason_raw, tp_ratios):
            from domain.models import TradeClosed
            tc = MagicMock()
            tc.close_reason = close_reason_raw
            tc.close_reason_raw = close_reason_raw
            tc.close_reason_detail = close_reason_raw
            tc.symbol = p.symbol
            tc.pnl_net = 0.0
            tc.trailing_profile = ""
            closed_calls.append(close_reason_raw)
            return tc

        monkeypatch.setenv("TM_SMART_TIMEOUT_ENABLED", "0")  # отключаем для чистоты

        MOD = "services.trade_monitor._monolith"
        with patch(f"{MOD}.finalize_trade", side_effect=fake_finalize), \
             patch(f"{MOD}.asdict", return_value={}), \
             patch.object(svc, "_get_spec", return_value=MagicMock(
                 pnl_money=lambda *a, **k: 0.0)), \
             patch.object(svc, "_io_save_closed", return_value=None), \
             patch.object(svc, "_log_ab_closed_event", return_value=None), \
             patch.object(svc, "_stamp_closed_trade_meta", return_value=None), \
             patch.object(svc, "_calc_commission_adjusted_exit_price",
                          return_value=99.9), \
             patch.object(svc, "_pop_pos", return_value=None), \
             patch.object(svc, "_get_symbol_lock",
                          return_value=threading.RLock()), \
             patch.object(svc, "_run_io_tasks", return_value=None), \
             patch.object(svc, "_update_stats", return_value=None), \
             patch.object(svc, "_fsm_transition", return_value=None), \
             patch(f"{MOD}.analytics_db") as _mock_db, \
             patch(f"{MOD}.get_ny_time_millis", return_value=now_ms):
            _mock_db.save_trade_closed = MagicMock()
            svc.redis = MagicMock()
            svc._io_save_closed = MagicMock()
            svc._housekeep_expired_positions(now_ms, current_symbol=sym)

        return closed_calls

    def _setup_pos(self, svc, sym, entry_ts, now_ms):
        pos = _pos(entry_ts_ms=entry_ts, symbol=sym)
        pos.id = "pos-x"
        pos.sid = "sid-x"
        pos.remaining_qty = 0.0
        pos.realized_pnl_gross = 0.0
        pos.trailing_active = False
        svc.shards[sym] = {"pos-x": pos}
        svc.open_positions["pos-x"] = pos
        svc.open_by_symbol[sym] = {"pos-x"}
        svc.symbol_by_pos_id["pos-x"] = sym
        return pos

    def test_reason_orphan_timeout_with_fresh_price(self, monkeypatch):
        sym = "BTCUSDT"
        entry_ts = 1_000_000
        now_ms = entry_ts + 3_600_001
        # Fresh price (within 5m age)
        svc = _make_svc(
            max_lifetime_ms=3_600_000,
            last_prices={sym: (now_ms - 60_000, 100.0)},  # 1min old
            close_mode="finalize",
        )
        self._setup_pos(svc, sym, entry_ts, now_ms)
        reasons = self._run_and_get_reason(svc, now_ms, sym, monkeypatch)
        assert reasons == ["ORPHAN_TIMEOUT"], f"Expected ORPHAN_TIMEOUT, got {reasons}"

    def test_reason_stale_price(self, monkeypatch):
        sym = "BTCUSDT"
        entry_ts = 1_000_000
        now_ms = entry_ts + 3_600_001
        # Stale price (> 5m old)
        svc = _make_svc(
            max_lifetime_ms=3_600_000,
            max_price_age_ms=300_000,
            last_prices={sym: (now_ms - 400_000, 100.0)},  # 400s old > 300s threshold
            close_mode="finalize",
        )
        self._setup_pos(svc, sym, entry_ts, now_ms)
        reasons = self._run_and_get_reason(svc, now_ms, sym, monkeypatch)
        assert reasons == ["ORPHAN_TIMEOUT_STALE_PRICE"], f"Expected STALE_PRICE, got {reasons}"

    def test_reason_no_price(self, monkeypatch):
        sym = "BTCUSDT"
        entry_ts = 1_000_000
        now_ms = entry_ts + 3_600_001
        # No price at all
        svc = _make_svc(
            max_lifetime_ms=3_600_000,
            last_prices={},
            close_mode="finalize",
        )
        self._setup_pos(svc, sym, entry_ts, now_ms)
        reasons = self._run_and_get_reason(svc, now_ms, sym, monkeypatch)
        assert reasons == ["ORPHAN_TIMEOUT_NO_PRICE"], f"Expected NO_PRICE, got {reasons}"

    def test_no_orphan_forced_close_reason(self, monkeypatch):
        """Убедиться что ORPHAN_FORCED_CLOSE больше не используется."""
        sym = "BTCUSDT"
        entry_ts = 1_000_000
        now_ms = entry_ts + 3_600_001
        svc = _make_svc(
            max_lifetime_ms=3_600_000,
            last_prices={sym: (now_ms - 1000, 100.0)},
            close_mode="finalize",
        )
        self._setup_pos(svc, sym, entry_ts, now_ms)
        reasons = self._run_and_get_reason(svc, now_ms, sym, monkeypatch)
        assert "ORPHAN_FORCED_CLOSE" not in reasons, "ORPHAN_FORCED_CLOSE should be gone"
