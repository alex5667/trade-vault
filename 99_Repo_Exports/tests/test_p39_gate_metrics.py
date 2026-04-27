import asyncio
import json
import unittest
from unittest.mock import MagicMock, AsyncMock
from services.orderflow.components.tick_processor import TickProcessor

class TestGateMetrics(unittest.IsolatedAsyncioTestCase):
    async def test_emit_gate_metrics(self):
        # Setup mocks
        redis = AsyncMock()
        runtime = MagicMock()
        runtime.config = {
            "of_gate_metrics_sample": 1.0,  # 100% sampling
            "of_gate_metrics_stream": "metrics:of_gate_test",
            "of_gate_metrics_maxlen": 1000
        }
        runtime.symbol = "BTCUSDT"
        
        # Instantiate TickProcessor
        # We only need enough args to init, most can be None
        tp = TickProcessor(redis, None, None, None, None, None, None)
        tp.of_gate_metrics_enable = True
        tp.of_gate_metrics_sample = 1.0
        
        # Test Data
        ofc = MagicMock()
        ofc.ok = 1
        ofc.scenario = "trend_up"
        ofc.sid = "test_sid"
        
        indicators = {
            "of_confirm_score": 0.95,
            "delta_z": 2.5,
            "pressure_per_min": 100.0,
            "spread_bp": 1.5,
            "dn_tier": 2,
            "of_build_us": 42
        }
        
        ev = {
            "meta_model_feature_total": 10,
            "meta_model_feature_missing": 2,
            "meta_enforce_cov_bucket": "high",
            "meta_mode": "ENFORCE",
            "meta_enable": 1
        }
        
        tick_ts = 1678888888000
        
        # Execution
        tp._emit_gate_metrics(runtime, ofc, indicators, ev, tick_ts)
        
        # Allow async task to run
        await asyncio.sleep(0.01)
        
        # Verification
        redis.xadd.assert_called_once()
        call_args = redis.xadd.call_args
        api_stream = call_args[0][0]
        kwargs = call_args[1]
        
        self.assertEqual(api_stream, "metrics:of_gate_test")
        fields = kwargs["fields"]
        self.assertEqual(fields["symbol"], "BTCUSDT")
        self.assertEqual(fields["ts_ms"], str(tick_ts))
        
        payload = json.loads(fields["payload"])
        
        # Check standard fields
        self.assertEqual(payload["symbol"], "BTCUSDT")
        self.assertEqual(payload["ok"], 1)
        self.assertEqual(payload["of_build_us"], 42)
        
        # Check meta fields
        self.assertEqual(payload["meta_model_feature_total"], 10)
        self.assertEqual(payload["meta_model_feature_missing"], 2)
        self.assertAlmostEqual(payload["meta_feature_coverage"], 0.8) # 1 - 2/10
        self.assertEqual(payload["meta_enforce_cov_bucket"], "high")
        self.assertEqual(payload["meta_mode"], "ENFORCE")
        
    async def test_sampling_skip(self):
        # Setup mocks
        redis = AsyncMock()
        runtime = MagicMock()
        # Rate 0.0 -> Should skip
        runtime.config = {"of_gate_metrics_sample": 0.0}
        
        tp = TickProcessor(redis, None, None, None, None, None, None)
        tp.of_gate_metrics_enable = True
        
        # Execution
        tp._emit_gate_metrics(runtime, MagicMock(), {}, {}, 123456789)
        
        await asyncio.sleep(0.01)
        
        # Verification
        redis.xadd.assert_not_called()

if __name__ == "__main__":
    unittest.main()
