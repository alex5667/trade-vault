from __future__ import annotations
from tests.trade_monitor_test_utils import create_mock_trade_monitor

from types import SimpleNamespace
from unittest.mock import patch

from services.trade_monitor import TradeMonitorService


class _SpecStub:
    trailing_profile_default = "rocket_v1"

def _mk_monitor() -> TradeMonitorService:
    """
    Minimal TradeMonitorService instance for causality testing.
    """
    mon = create_mock_trade_monitor()
    # Mocking SymbolSpec dependency
    mon._get_spec = lambda symbol: _SpecStub()
    mon.last_warnings = []
    mon.last_infos = []

    def log_warning(msg):
        mon.last_warnings.append(str(msg))
    def log_info(msg):
        mon.last_infos.append(str(msg))

    mon.logger = SimpleNamespace(
        debug=lambda *a, **k: None,
        info=log_info,
        warning=log_warning,
        exception=lambda *a, **k: None,
    )
    mon.default_lot = 1.0
    mon.stop_atr_mult = 1.0
    mon.rr_levels = [1.0, 2.0, 3.0]
    mon._crypto_suffixes = ("USDT", "USDC", "BUSD")
    mon._crypto_exclude_prefixes = ()
    mon._margin_fx_symbols = frozenset({""})
    mon._max_tick_ts_ms = 0
    return mon

@patch("services.trade_monitor._monolith.get_ny_time_millis")
def test_trade_monitor_causality_grace_period(mock_now):
    mon = _mk_monitor()

    # 1. Market Time is fixed at T
    T = 1700000000000
    mon._max_tick_ts_ms = T

    # 2. Wall Clock is T + 300ms (market lags slightly)
    mock_now.return_value = T + 300

    # --- Case 1: Within Grace Period (100ms) ---
    raw = {
        "sid": "sig-1", "symbol": "BTCUSDT", "tf": "1m",
        "direction": "LONG", "entry": 50000.0, "ts": T + 50
    }
    sig = mon._normalize_signal(raw)
    assert sig.entry_ts_ms == T + 50  # GRACE: timestamp must be preserved
    assert len(mon.last_warnings) == 0
    assert len(mon.last_infos) == 0

    # --- Case 2: Beyond Grace, but within Wall Clock ---
    # Total market lag is 300ms (below 5s warning threshold)
    raw_2 = {
        "sid": "sig-2", "symbol": "BTCUSDT", "tf": "1m",
        "direction": "LONG", "entry": 50000.0, "ts": T + 150
    }
    sig_2 = mon._normalize_signal(raw_2)
    assert sig_2.entry_ts_ms == T + 150 # Preserved, but logs info about jitter
    assert any("Ingestion jitter" in m for m in mon.last_infos)

    # --- Case 3: Genuine Future (Clock Skew) ---
    # Signal is T + 2000ms, while wall clock is only T + 300ms
    # Tolerance is 1000ms, so 2000ms > 300ms + 1000ms -> CLAMP
    raw_3 = {
        "sid": "sig-3", "symbol": "BTCUSDT", "tf": "1m",
        "direction": "LONG", "entry": 50000.0, "ts": T + 2000
    }
    sig_3 = mon._normalize_signal(raw_3)
    assert sig_3.entry_ts_ms == T + 300 # CLAMPED to wall clock
    assert any("Clock skew detected" in m for m in mon.last_warnings)

@patch("services.trade_monitor._monolith.get_ny_time_millis")
def test_trade_monitor_market_lag_warning_explicit(mock_now):
    mon = _mk_monitor()

    # Market lags significantly (10 seconds)
    T = 1700000000000
    mon._max_tick_ts_ms = T
    mock_now.return_value = T + 10000 # Wall clock = T + 10s

    # Signal is at T + 6000ms (6s ahead of market, but behind wall clock)
    # 6000ms > 5000ms (threshold) -> WARNING
    raw = {
        "sid": "sig-lag", "symbol": "BTCUSDT", "tf": "1m",
        "direction": "LONG", "entry": 50000.0, "ts": T + 6000
    }
    sig = mon._normalize_signal(raw)
    assert sig.entry_ts_ms == T + 6000 # Preserved
    assert any("Market data lag detected" in m for m in mon.last_warnings)
    assert not any("Clock skew detected" in m for m in mon.last_warnings)
