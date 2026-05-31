import os
import time
import pytest
from unittest.mock import patch, MagicMock
from collections import deque

from core.manip_calibrator import ManipCalibrator
from orderflow_services.manip_gate_calibrator_v1 import _read_inputs_stream
from services.orderflow.manip_patterns import ManipulationTracker
from handlers.crypto_orderflow.components.gates import GateDecisionV1
from services.orderflow.signal_pipeline import SignalPipeline

class TestManipCalibratorFixes:
    def test_ts_ms_zero_handled_correctly(self):
        """Bug 5 & 6 Fix: explicit 0 for ts_ms/now_ms should be respected, not treated as missing."""
        cal = ManipCalibrator(window_ms=1000)
        
        # Insert at t=0
        cal.observe("BTCUSDT", 0.5, 0.5, ts_ms=0)
        assert len(cal.bins["BTCUSDT"].buf) == 1
        assert cal.bins["BTCUSDT"].buf[0].ts_ms == 0
        
        # Evict at t=0 (cutoff = -1000, so t=0 stays)
        cal.evict_all(now_ms=0)
        assert len(cal.bins["BTCUSDT"].buf) == 1
        
        # Evict at t=1500 (cutoff = +500, so t=0 should be evicted)
        cal.evict_all(now_ms=1500)
        assert len(cal.bins["BTCUSDT"].buf) == 0

    def test_evict_all_with_none_uses_time(self):
        cal = ManipCalibrator(window_ms=1000)
        cal.observe("BTCUSDT", 0.5, 0.5, ts_ms=int(time.time() * 1000) - 2000)
        assert len(cal.bins["BTCUSDT"].buf) == 1
        cal.evict_all(now_ms=None)
        assert len(cal.bins["BTCUSDT"].buf) == 0

class TestManipGateCalibratorV1Fixes:
    def test_read_inputs_stream_observes_zeros(self):
        """Bug 7 Fix: Zeros should be observed to prevent upward percentile bias."""
        calibrator = MagicMock()
        r = MagicMock()
        
        # 1 message with positive scores, 1 with zeros
        messages = [
            ("1-0", {"symbol": "BTCUSDT", "indicators": '{"layering_score": 0.5, "quote_stuffing_score": 0.5}'}),
            ("2-0", {"symbol": "BTCUSDT", "indicators": '{"layering_score": 0.0, "quote_stuffing_score": 0.0}'})
        ]
        r.xread.return_value = [("stream", messages)]
        
        _read_inputs_stream(r, "0-0", calibrator)
        
        # Calibrator should be called twice!
        assert calibrator.observe.call_count == 2
        calibrator.observe.assert_any_call("BTCUSDT", 0.5, 0.5)
        calibrator.observe.assert_any_call("BTCUSDT", 0.0, 0.0)

class TestManipPatternsFixes:
    def test_layering_score_resets_on_new_build(self):
        """Bug 8 Fix: layering_score should reset to 0.0 on transition to build phase."""
        tracker = ManipulationTracker()
        
        # Simulate idle phase with some decayed score
        tracker._lay_state = "idle"
        tracker.layering_score = 0.5
        
        # Setup EMA to trigger build
        tracker._lay_bid_ema_usd = 100.0
        
        # Trigger build: low trade rate, bid ratio = 500/100 = 5.0 (>= 3.0 mult)
        # min peak = 500k by default. We'll set ENV to lower it for test
        with patch.dict("os.environ", {"LAYERING_MIN_PEAK_USD": "100"}):
            tracker.update_from_book(
                ts_ms=1000,
                bid_depth_usd=500.0,
                ask_depth_usd=100.0,
                book_update_rate_z=0.0,
                cancel_rate_z=0.0,
                trade_msg_rate_hz=0.0,
                mid_px=50000.0
            )
            
        assert tracker._lay_state == "build"
        assert tracker.layering_score == 0.0  # Must be reset!

class TestSignalPipelineManipModeFix:
    @patch("core.redis_client.get_redis", return_value=MagicMock())
    def test_manip_mode_auto_respects_gate_profile(self, mock_get_redis):
        """Bug 1 Fix: MANIP_MODE=auto should fall back to GATE_PROFILE, not act as 'auto' profile."""
        with patch.dict("os.environ", {
            "MANIP_MODE": "auto",
            "GATE_PROFILE": "strict",
            "EDGE_SLIPPAGE_CAL_ENABLED": "0"
        }):
            pipeline = SignalPipeline(publisher=MagicMock(), atr_cache=MagicMock())
            assert pipeline._cached_manip_profile == "strict"
            
        with patch.dict("os.environ", {
            "MANIP_MODE": "tighten",
            "GATE_PROFILE": "default",
            "EDGE_SLIPPAGE_CAL_ENABLED": "0"
        }):
            pipeline = SignalPipeline(publisher=MagicMock(), atr_cache=MagicMock())
            assert pipeline._cached_manip_profile == "tighten"

        with patch.dict("os.environ", {
            "MANIP_MODE": "auto",
            "GATE_PROFILE": "hard",
            "EDGE_SLIPPAGE_CAL_ENABLED": "0"
        }):
            pipeline = SignalPipeline(publisher=MagicMock(), atr_cache=MagicMock())
            assert pipeline._cached_manip_profile == "hard"
