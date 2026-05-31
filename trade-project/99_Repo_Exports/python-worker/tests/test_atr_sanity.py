from utils.time_utils import get_ny_time_millis

# -*- coding: utf-8 -*-
"""
Tests for ATR sanity module (robust sanity checks with median + last-good fallback).
"""
import pytest

from core.atr_sanity import ATRSanity


def test_atr_sanity_stale_uses_last_good(monkeypatch):
    monkeypatch.setenv("ATR_MAX_AGE_MS", "1000")
    monkeypatch.setenv("ATR_LAST_GOOD_TTL_MS", "60000")
    s = ATRSanity(window=40)

    now = get_ny_time_millis()
    r1 = s.update(atr=10.0, px=1000.0, age_ms=10, now_ms=now, symbol="TEST")
    assert r1.bad == 0
    assert r1.used_last_good == 0
    assert r1.atr_used == 10.0

    # stale → should fallback to last_good (age_ms > 120000 for tf=1m default)
    r2 = s.update(atr=9.0, px=1000.0, age_ms=200000, now_ms=now + 2000, symbol="TEST", tf="1m")
    assert r2.bad == 1
    assert r2.used_last_good == 1
    assert r2.atr_used == 10.0


def test_atr_sanity_jump_rel(monkeypatch):
    monkeypatch.setenv("ATR_JUMP_MAX_REL", "0.2")
    s = ATRSanity(window=20)
    now = get_ny_time_millis()
    # seed stable values
    for i in range(15):
        r = s.update(atr=10.0, px=1000.0, age_ms=10, now_ms=now + i, symbol="TEST")
        assert r.bad == 0
    # big jump in atr_bps should be marked bad
    rj = s.update(atr=30.0, px=1000.0, age_ms=10, now_ms=now + 999, symbol="TEST")
    assert rj.bad == 1


def test_atr_sanity_basic_ok():
    """Test that sane ATR values pass."""
    sanity = ATRSanity(window=120)
    now_ms = get_ny_time_millis()

    # Good ATR: 50 bps, fresh, within bounds
    result = sanity.update(atr=0.5, px=100.0, age_ms=1000, now_ms=now_ms, symbol="TEST")

    assert result.bad == 0
    assert result.atr_used == 0.5
    assert result.used_last_good == 0
    assert result.atr_bps == pytest.approx(50.0, rel=0.01)
    assert result.jump_event == 0
    assert result.jump_count_window == 0


def test_atr_sanity_stale():
    """Test that stale ATR is marked bad."""
    sanity = ATRSanity(window=120)
    now_ms = get_ny_time_millis()

    # Stale ATR (age > max_age_ms)
    result = sanity.update(atr=0.5, px=100.0, age_ms=1000000, now_ms=now_ms, symbol="TEST")

    assert result.bad == 1
    assert "stale" in result.reason


def test_atr_sanity_bps_out_of_bounds():
    """Test that ATR with out-of-bounds bps is marked bad."""
    sanity = ATRSanity(window=120)
    now_ms = get_ny_time_millis()

    # ATR too low (0.1 bps < min 2)
    result = sanity.update(atr=0.001, px=100.0, age_ms=1000, now_ms=now_ms, symbol="TEST")
    assert result.bad == 1
    assert "atr_bps_oob" in result.reason

    # ATR too high (1000 bps > max 800)
    result = sanity.update(atr=10.0, px=100.0, age_ms=1000, now_ms=now_ms, symbol="TEST")
    assert result.bad == 1
    assert "atr_bps_oob" in result.reason


def test_atr_sanity_jump_detection():
    """Test that large jumps relative to median are detected."""
    sanity = ATRSanity(window=120)
    now_ms = get_ny_time_millis()

    # Build up median with consistent values (~50 bps)
    for i in range(30):
        sanity.update(atr=0.5, px=100.0, age_ms=1000, now_ms=now_ms + i * 1000, symbol="TEST")

    # Now send a huge jump (5x median)
    result = sanity.update(atr=2.5, px=100.0, age_ms=1000, now_ms=now_ms + 30000, symbol="TEST")

    assert result.bad == 1
    assert "jump_rel" in result.reason
    assert result.jump_event == 1


def test_atr_sanity_last_good_fallback():
    """Test that last good ATR is used when current is bad."""
    sanity = ATRSanity(window=120)
    now_ms = get_ny_time_millis()
    symbol = "TEST"

    # First: good ATR
    result1 = sanity.update(atr=0.5, px=100.0, age_ms=1000, now_ms=now_ms, symbol=symbol)
    assert result1.bad == 0

    # Second: bad ATR (stale)
    result2 = sanity.update(atr=0.6, px=100.0, age_ms=1000000, now_ms=now_ms + 1000, symbol=symbol)

    assert result2.bad == 1
    assert result2.used_last_good == 1
    assert result2.atr_used == 0.5  # Uses last good


def test_atr_sanity_last_good_ttl_expired():
    """Test that last good is not used if TTL expired."""
    sanity = ATRSanity(window=120)
    now_ms = get_ny_time_millis()
    symbol = "TEST"

    # First: good ATR
    sanity.update(atr=0.5, px=100.0, age_ms=1000, now_ms=now_ms, symbol=symbol)

    # Wait beyond TTL (default 1800000 ms = 30 min)
    now_ms_expired = now_ms + 2000000

    # Bad ATR after TTL expired
    result = sanity.update(atr=0.6, px=100.0, age_ms=1000000, now_ms=now_ms_expired, symbol=symbol)

    assert result.bad == 1
    assert result.used_last_good == 0  # TTL expired, can't use last good
    assert result.atr_used == 0.6  # Uses current (bad) value


def test_atr_sanity_zero_atr():
    """Test that zero ATR is marked bad."""
    sanity = ATRSanity(window=120)
    now_ms = get_ny_time_millis()

    result = sanity.update(atr=0.0, px=100.0, age_ms=1000, now_ms=now_ms, symbol="TEST")

    assert result.bad == 1
    assert "atr<=0" in result.reason


def test_atr_sanity_window_median():
    """Test that median is computed correctly from window."""
    sanity = ATRSanity(window=20)
    now_ms = get_ny_time_millis()

    # Feed enough values to build median (need at least max(10, window//4) = 10 values)
    # Feed values: 40, 50, 50, 50, 50, 50, 50, 50, 50, 60 bps (median ~50)
    values = [0.4] + [0.5] * 8 + [0.6]
    for v in values:
        sanity.update(atr=v, px=100.0, age_ms=1000, now_ms=now_ms, symbol="TEST")
        now_ms += 1000

    # Median should be 50 bps (0.5 ATR)
    # Send a value that's 3x median (should trigger jump)
    result = sanity.update(atr=1.5, px=100.0, age_ms=1000, now_ms=now_ms, symbol="TEST")

    # With jump_max_rel=1.2, 1.5 vs 0.5 = 2.0 relative change > 1.2
    assert result.bad == 1
    assert "jump_rel" in result.reason
    assert result.jump_event == 1


def test_atr_sanity_jump_event_tracking(monkeypatch):
    """Test that jump events are tracked and counted in window."""
    monkeypatch.setenv("ATR_JUMP_MAX_REL", "0.5")
    monkeypatch.setenv("ATR_JUMP_WINDOW_SEC", "1")  # 1 second window for testing
    sanity = ATRSanity(window=20)
    now_ms = get_ny_time_millis()
    symbol = "BTCUSDT"

    # Seed stable values
    for i in range(15):
        r = sanity.update(atr=10.0, px=1000.0, age_ms=10, now_ms=now_ms + i * 100, symbol=symbol)
        assert r.bad == 0
        assert r.jump_event == 0
        assert r.jump_count_window == 0

    # First jump
    r1 = sanity.update(atr=30.0, px=1000.0, age_ms=10, now_ms=now_ms + 2000, symbol=symbol)
    assert r1.bad == 1
    assert r1.jump_event == 1
    assert r1.jump_count_window == 1

    # Second jump within window
    r2 = sanity.update(atr=25.0, px=1000.0, age_ms=10, now_ms=now_ms + 2100, symbol=symbol)
    assert r2.jump_event == 1
    assert r2.jump_count_window == 2

    # Third jump after window expired (1 second = 1000ms)
    r3 = sanity.update(atr=35.0, px=1000.0, age_ms=10, now_ms=now_ms + 3200, symbol=symbol)
    assert r3.jump_event == 1
    assert r3.jump_count_window == 1  # Only the new jump, old ones expired


def test_atr_sanity_per_symbol_tracking(monkeypatch):
    """Test that jump tracking is per-symbol."""
    monkeypatch.setenv("ATR_JUMP_MAX_REL", "0.5")
    monkeypatch.setenv("ATR_JUMP_WINDOW_SEC", "10")
    sanity = ATRSanity(window=20)
    now_ms = get_ny_time_millis()

    # Seed stable values for symbol1
    for i in range(15):
        sanity.update(atr=10.0, px=1000.0, age_ms=10, now_ms=now_ms + i * 100, symbol="SYM1")

    # Seed stable values for symbol2
    for i in range(15):
        sanity.update(atr=20.0, px=1000.0, age_ms=10, now_ms=now_ms + i * 100, symbol="SYM2")

    # Jump for symbol1
    r1 = sanity.update(atr=30.0, px=1000.0, age_ms=10, now_ms=now_ms + 2000, symbol="SYM1")
    assert r1.jump_event == 1
    assert r1.jump_count_window == 1

    # Jump for symbol2 (should be independent)
    r2 = sanity.update(atr=50.0, px=1000.0, age_ms=10, now_ms=now_ms + 2000, symbol="SYM2")
    assert r2.jump_event == 1
    assert r2.jump_count_window == 1  # Independent count for SYM2


def test_atr_sanity_regime_adaptation():
    """Test that ATR sanity eventually adapts to a new high-volatility regime."""
    # Window=20 means we need ~10-15 samples for median to start shifting significantly
    sanity = ATRSanity(window=20)
    now_ms = get_ny_time_millis()

    # 1. Establish low volatility baseline (ATR=10)
    for i in range(20):
        sanity.update(atr=10.0, px=1000.0, age_ms=10, now_ms=now_ms + i*1000, symbol="TEST")

    # 2. Sudden regime shift (ATR=50, 5x jump)
    # This should be flagged as BAD initially
    r = sanity.update(atr=50.0, px=1000.0, age_ms=10, now_ms=now_ms + 21000, symbol="TEST")
    assert r.bad == 1
    assert "jump_rel" in r.reason

    # 3. Sustained high volatility
    # Feed 20 more samples of high ATR.
    # The median should eventually eventually catch up.
    bad_count = 0
    ok_count = 0

    # We expect adaptation within the window size (20 samples)
    # Fixed: use 60s steps to cross TF buckets and trigger step-change acceptance (jump_accept)
    for i in range(30):
        r = sanity.update(atr=50.0, px=1000.0, age_ms=10, now_ms=now_ms + 22000 + i*60000, symbol="TEST", tf="1m")
        if r.bad == 1:
            bad_count += 1
        else:
            ok_count += 1

    # It should have adapted by now
    # The first few will be bad, but eventually it must become good
    assert ok_count > 0, "Sanity failed to adapt to new regime"

    # Verify we are now stable in new regime
    final = sanity.update(atr=50.0, px=1000.0, age_ms=10, now_ms=now_ms + 90000, symbol="TEST")
    assert final.bad == 0

