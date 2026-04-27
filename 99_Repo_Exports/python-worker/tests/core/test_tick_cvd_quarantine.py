"""Tests for CVD quarantine functionality in TickCVDState."""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import os
import time
from unittest.mock import patch

from unittest.mock import patch, MagicMock

import pytest

from core.tick_cvd import TickCVDState
from core.of_confirm_engine import OFConfirmEngine


def test_cvd_quarantine_disabled():
    """Test that quarantine is disabled by default."""
    with patch.dict(os.environ, {"CVD_QUARANTINE_ENABLE": "0"}):
        state = TickCVDState("BTCUSDT")
        assert not state.quarantine_active()
        assert state.quarantine_reason() == ""


def test_cvd_quarantine_jump_detection():
    """Test jump detection triggers quarantine."""
    with patch.dict(os.environ, {
        "CVD_QUARANTINE_ENABLE": "1",
        "CVD_JUMP_ABS_QTY": "100",
        "CVD_JUMP_REL_K": "2.0",
        "CVD_JUMP_WINDOW_MS": "10000",
        "CVD_JUMP_K_EVENTS": "2",
        "CVD_QUARANTINE_TTL_MS": "5000",
    }):
        state = TickCVDState("BTCUSDT", ema_period_delta=10)
        now_ms = get_ny_time_millis()
        
        # Normal ticks
        for i in range(10):
            state.update({"ts": now_ms + i * 100, "qty": 10, "side": "BUY"})
        
        assert not state.quarantine_active(now_ms + 1000)
        
        # Large jump (should trigger)
        state.update({"ts": now_ms + 2000, "qty": 1000, "side": "BUY"})
        state.update({"ts": now_ms + 2100, "qty": 1000, "side": "BUY"})
        
        # Should be quarantined after 2 jumps
        assert state.quarantine_active(now_ms + 3000)
        assert "delta_jump" in state.quarantine_reason()


def test_cvd_quarantine_indicators():
    """Test that quarantine flags appear in indicators."""
    with patch.dict(os.environ, {
        "CVD_QUARANTINE_ENABLE": "1",
        "CVD_JUMP_ABS_QTY": "100",
        "CVD_JUMP_REL_K": "2.0",
        "CVD_JUMP_WINDOW_MS": "10000",
        "CVD_JUMP_K_EVENTS": "1",  # Trigger on 1 event for test
        "CVD_QUARANTINE_TTL_MS": "5000",
    }):
        state = TickCVDState("BTCUSDT", ema_period_delta=10)
        now_ms = get_ny_time_millis()
        
        # Trigger quarantine
        state.update({"ts": now_ms - 100, "qty": 10, "side": "BUY"})
        state.update({"ts": now_ms, "qty": 1000, "side": "BUY"})
        
        indicators = state.indicators_light()
        assert "cvd_quarantine_active" in indicators
        assert "cvd_quarantine_until_ms" in indicators
        assert "cvd_quarantine_reason" in indicators
        assert "cvd_jump_events_total" in indicators
        
        assert indicators["cvd_quarantine_active"] == 1
        assert indicators["cvd_jump_events_total"] >= 1


def test_cvd_quarantine_expires():
    """Test that quarantine expires after TTL."""
    with patch.dict(os.environ, {
        "CVD_QUARANTINE_ENABLE": "1",
        "CVD_JUMP_ABS_QTY": "100",
        "CVD_JUMP_REL_K": "2.0",
        "CVD_JUMP_WINDOW_MS": "10000",
        "CVD_JUMP_K_EVENTS": "1",
        "CVD_QUARANTINE_TTL_MS": "1000",  # 1s TTL
    }):
        state = TickCVDState("BTCUSDT", ema_period_delta=10)
        now_ms = get_ny_time_millis()
        
        # Trigger quarantine
        state.update({"ts": now_ms - 100, "qty": 10, "side": "BUY"})
        state.update({"ts": now_ms, "qty": 1000, "side": "BUY"})
        assert state.quarantine_active(now_ms + 100)
        
        # Should expire after TTL
        assert not state.quarantine_active(now_ms + 2000)

def test_cvd_quarantine_div_match_fallback():
    """Test OFConfirmEngine sets div_match via delta_tick fallback when cvd_quarantine_active=1."""
    engine = OFConfirmEngine(version=3)
    
    # 1) When NO quarantine, fallback should NOT apply
    indicators_primary = {
        "cvd_quarantine_active": 0,
        "sweep_dir_bias": "LONG",
        "delta_tick": 5.0, # Positive delta
        "now_ts_ms": get_ny_time_millis()
    }
    
    runtime = MagicMock()
    runtime.last_div = None # No divergence
    
    engine.build(
        symbol="BTCUSDT", tf="1s", direction="LONG", tick_ts_ms=0, price=50000.0,
        delta_z=0.0, runtime=runtime, cfg={}, indicators=indicators_primary
    )
    
    assert indicators_primary["div_match"] == 0
    assert indicators_primary.get("div_match_source") == "none"
    
    # 2) When CVD is quarantined, we fallback to volume delta (delta_tick)
    indicators_fallback = {
        "cvd_quarantine_active": 1,
        "sweep_dir_bias": "LONG",
        "delta_tick": 5.0, # Match long direction
        "now_ts_ms": get_ny_time_millis()
    }
    
    # Needs sweep_recent condition passing
    with patch("core.of_confirm_engine.compute_sweep_recent", return_value=True):
        engine.build(
            symbol="BTCUSDT", tf="1s", direction="LONG", tick_ts_ms=0, price=50000.0,
            delta_z=0.0, runtime=runtime, cfg={}, indicators=indicators_fallback
        )
        
    assert indicators_fallback["div_match"] == 0
    assert indicators_fallback["div_match_fallback"] == 1
    assert indicators_fallback.get("div_match_source") == "delta_tick_fallback"

    # 3) Negative fallback check (wrong direction)
    indicators_fallback_neg = {
        "cvd_quarantine_active": 1,
        "sweep_dir_bias": "SHORT",
        "delta_tick": 5.0, # Mismatch short direction (should be < 0)
        "now_ts_ms": get_ny_time_millis()
    }
    
    with patch("core.of_confirm_engine.compute_sweep_recent", return_value=True):
        engine.build(
            symbol="BTCUSDT", tf="1s", direction="SHORT", tick_ts_ms=0, price=50000.0,
            delta_z=0.0, runtime=runtime, cfg={}, indicators=indicators_fallback_neg
        )
        
    assert indicators_fallback_neg["div_match"] == 0
    assert indicators_fallback_neg["div_match_fallback"] == 0
    assert indicators_fallback_neg.get("div_match_source") == "none"

