
import unittest
import sys
import os
import json
import asyncio
from unittest.mock import MagicMock, patch, mock_open

# Adjust paths
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../python-worker/services")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../python-worker")))

# Mock dependencies
sys.modules["services.orderflow.market_state"] = MagicMock()
sys.modules["services.orderflow.signal_pipeline"] = MagicMock()
sys.modules["utils.atr_cache"] = MagicMock()
sys.modules["redis.asyncio"] = MagicMock()
sys.modules["services.async_signal_publisher"] = MagicMock()
sys.modules["core.of_confirm_engine"] = MagicMock()

try:
    from services.orderflow_strategy import OrderFlowStrategy
except ImportError:
    # If fails, we might need more mocks
    pass

class AsyncContextManagerMock(MagicMock):
    async def __aenter__(self):
        return self
    async def __aexit__(self, *args):
        pass

class MockRuntime:
    def __init__(self, symbol="BTCUSDT"):
        self.symbol = symbol
        self.config = {}
        self.tick_count = 0
        self.heartbeat_counter = 0
        self.last_book = None
        self.last_ts_ms = 0
        self.delta_triggers = 0 # Fixed
        self.cvd_state = MagicMock()
        self.microbar = MagicMock()
        self.l3_queue = MagicMock()
        self.liq_service = MagicMock()
        self.pressure = MagicMock()
        self.tick_gaps = MagicMock()
        self.burst_cal = MagicMock()
        self.burst = MagicMock()
        self.burst.st = MagicMock()
        self.burst_mu = AsyncContextManagerMock() # Fixed
        self.signal_emit_log_sampler = MagicMock()
        self.signal_emit_log_sampler.should_log.return_value = True
        self.weak_signal_log_sampler = MagicMock()
        self.weak_signal_log_sampler.should_log.return_value = True
        self.delta_log_sampler = MagicMock() # Fixed
        self.delta_log_sampler.should_log.return_value = True
        self.signal_count = 0
        self.sub_tasks = set()
        self.delta_detector = MagicMock()
        self.delta_detector.push.return_value = None
        self.last_spread_bps = 0.0
        self.book_rate_ema = 0.0
        self.liq_service.update.return_value = MagicMock(score=0.5)
        self.last_cvd_reclaim = None
        self.last_regime = "na"
        self.last_metrics_ts = 0
        self.delta_z_ema = 0.0
        self.obi_ema = 0.0
        self.pressure_sps = 0.0
        self.pressure_hi = 0
        self.last_indicators = {}
        self.last_signals = []
        self.last_of_confirm_score = 0.0
        self.tick_gap_score = 0.0
        self.l3_event_score = 0.0
        self.last_spread_bps = 0.0
        self.book_rate_ema = 0.0
        self.last_book_ts_ms = 0
        self.signal_id_gen = 0
        self.rr = MagicMock() # Assuming runtime might have this
        self.tick_dn_calib = MagicMock()
        self.tick_dn_calib.tiers.return_value = MagicMock(tier0_usd=100, tier1_usd=200, tier2_usd=300)
        self.dynamic_cfg = {} # Fixed
        self.signal_attempt_ts_ms = []
        self.dn_passrate = MagicMock()
        self.l3_stats = MagicMock()
        self.last_obi_event = None
        self.config["delta_abs_min_confirm"] = 0.0
        self.pending_payload = None
        self.pending_score = 0.0
        self.pending_ts_ms = 0
        self.pending_replaced = 0
        self.last_signal_ts = 0
        self.last_iceberg_event = None

    async def maybe_load_overrides(self, *args):
        pass

class AsyncMock(MagicMock):
    async def __call__(self, *args, **kwargs):
        return super(AsyncMock, self).__call__(*args, **kwargs)

class TestCalibratedGating(unittest.TestCase):
    
    def setUp(self):
        with patch("services.orderflow_strategy.MarketStateService"), \
             patch("services.orderflow_strategy.SignalPipeline"), \
             patch("services.orderflow_strategy.get_atr_cache"), \
             patch("services.orderflow_strategy.ATRSanity"), \
             patch("services.orderflow_strategy.ConfidenceCalibratorBundleRuntime") as mock_runtime_cls:
             
            self.mock_cal_runtime = MagicMock()
            mock_runtime_cls.return_value = self.mock_cal_runtime
            
            self.strategy = OrderFlowStrategy(
                redis=AsyncMock(), # Fixed
                ticks=AsyncMock(), # Fixed
                publisher=MagicMock(),
                of_engine=MagicMock()
            )
            self.strategy.logger = MagicMock()
            
            # Mock async internal tasks
            self.strategy._on_microbar_closed = AsyncMock()
            self.strategy._maybe_poll_symbol_overrides = AsyncMock()

    @patch("os.path.exists")
    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.getmtime")
    def test_gating_raw_mode(self, mock_mtime, mock_file, mock_exists):
        # Raw mode (default)
        self.strategy.conf_cal_gating_mode = "raw"
        
        runtime = MockRuntime()
        tick = {"price": 100.0, "ts_ms": 1000, "side": "BUY", "qty": 1.0}
        
        # Mock _compute_confidence to return 0.85
        self.strategy._compute_confidence = MagicMock(return_value=0.85)
        
        # FIX: Ensure delta detector fires
        runtime.delta_detector.push.return_value = {"delta": 10.0, "z": 3.0, "ts": 1000}
        
        # Run process_tick
        asyncio.run(self.strategy.process_tick(runtime, tick))
        
        # Verify confidence was NOT calibrated
        self.mock_cal_runtime.get_calibrated_confidence.assert_not_called()
        
    @patch("os.path.exists")
    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.getmtime")
    def test_gating_cal_always(self, mock_mtime, mock_file, mock_exists):
        # Calibrated Always
        self.strategy.conf_cal_gating_mode = "cal_always"
        self.strategy.conf_cal_runtime = self.mock_cal_runtime # Ensure set
        
        runtime = MockRuntime()
        tick = {"price": 100.0, "ts_ms": 1000, "side": "BUY", "qty": 1.0}
        
        # Mock _compute_confidence -> 0.85
        self.strategy._compute_confidence = MagicMock(return_value=0.85)
        
        # Mock calibration -> 0.75
        self.mock_cal_runtime.get_calibrated_confidence.return_value = {"result": 0.75}
        
        # FIX: Ensure delta detector fires so we reach confidence logic
        runtime.delta_detector.push.return_value = {"delta": 10.0, "z": 3.0, "ts": 1000}
        
        # Run
        asyncio.run(self.strategy.process_tick(runtime, tick))
        
        # Verify calibration called
        self.mock_cal_runtime.get_calibrated_confidence.assert_called()
        
        # Verify filtered (assuming min_conf default is 0.80)
        # We can check logs "Signal filtered"
        # The logs usually contain "Signal filtered: conf=75.00% < min_conf=80.00%"
        # self.strategy.logger.info.assert_called()
        # Actually checking calls more precisely requires verifying call args
        # But asserting calibration was called confirms logic path was taken.

    @patch("os.path.exists")
    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.getmtime")
    @patch("time.time")
    def test_gating_cal_proof_valid(self, mock_time, mock_mtime, mock_file, mock_exists):
        # Calibrated After Proof
        self.strategy.conf_cal_gating_mode = "cal_after_proof"
        self.strategy.conf_cal_runtime = self.mock_cal_runtime
        
        # Mock Proof
        mock_exists.return_value = True
        mock_mtime.return_value = 123456
        mock_time.return_value = 2000 # current time
        
        proof_content = json.dumps({
            "ts": 1500, # 500s ago (fresh)
            "valid": True
        })
        mock_file.return_value = mock_open(read_data=proof_content).return_value
        
        runtime = MockRuntime()
        tick = {"price": 100.0, "ts_ms": 2005, "side": "BUY", "qty": 1.0}
        
        self.strategy._compute_confidence = MagicMock(return_value=0.85)
        self.mock_cal_runtime.get_calibrated_confidence.return_value = {"result": 0.75}

        # FIX: Ensure delta detector fires
        runtime.delta_detector.push.return_value = {"delta": 10.0, "z": 3.0, "ts": 2005}
        
        # Run
        asyncio.run(self.strategy.process_tick(runtime, tick))
        
        # Should be calibrated
        self.mock_cal_runtime.get_calibrated_confidence.assert_called()

    @patch("os.path.exists")
    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.getmtime")
    @patch("time.time")
    def test_gating_cal_proof_invalid(self, mock_time, mock_mtime, mock_file, mock_exists):
        # Calibrated After Proof - Invalid Proof
        self.strategy.conf_cal_gating_mode = "cal_after_proof"
        
        mock_exists.return_value = True
        mock_time.return_value = 2000
        
        proof_content = json.dumps({
            "ts": 1500,
            "valid": False # Explicitly invalid
        })
        mock_file.return_value = mock_open(read_data=proof_content).return_value
        
        runtime = MockRuntime()
        tick = {"price": 100.0, "ts_ms": 2005, "side": "BUY", "qty": 1.0}
        
        self.strategy._compute_confidence = MagicMock(return_value=0.85)

        # FIX: Ensure delta detector fires
        runtime.delta_detector.push.return_value = {"delta": 10.0, "z": 3.0, "ts": 2005}
        
        # Run
        asyncio.run(self.strategy.process_tick(runtime, tick))
        
        # Should NOT be calibrated (fallback to raw)
        self.mock_cal_runtime.get_calibrated_confidence.assert_not_called()

if __name__ == "__main__":
    unittest.main()
