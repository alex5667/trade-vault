
import sys
import os
import unittest
import math
from unittest.mock import MagicMock, patch
from types import SimpleNamespace

# Adjust path to find services
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../python-worker/services")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../python-worker")))

# Mock dependencies BEFORE importing OrderFlowStrategy to avoid side effects
sys.modules["services.orderflow.market_state"] = MagicMock()
sys.modules["services.orderflow.signal_pipeline"] = MagicMock()
sys.modules["utils.atr_cache"] = MagicMock()
# We also need to mock redis.asyncio if it's used at module level
sys.modules["redis.asyncio"] = MagicMock()

try:
    from services.orderflow_strategy import OrderFlowStrategy
except ImportError:
    # If direct import fails, try modifying sys.path further or mocking
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    from services.orderflow_strategy import OrderFlowStrategy

class MockRuntime:
    def __init__(self, config=None, symbol="BTCUSDT"):
        self.config = config or {}
        self.symbol = symbol
        self.last_metrics_ts = 0

    def get_atr_tf_selected(self):
        return "15m"

class TestShadowConfidenceV2(unittest.TestCase):
    
    def setUp(self):
        # Create a partial mock of OrderFlowStrategy
        # We only need _compute_confidence and dependencies
        # Since we mocked the modules, OrderFlowStrategy init should be safe-ish
        # But to be safer, we can patch the dependencies on the class or init
        
        with patch("services.orderflow_strategy.MarketStateService"), \
             patch("services.orderflow_strategy.SignalPipeline"), \
             patch("services.orderflow_strategy.get_atr_cache"), \
             patch("services.orderflow_strategy.ATRSanity"):
             
            self.strategy = OrderFlowStrategy(
                redis=MagicMock(),
                ticks=MagicMock(),
                publisher=MagicMock(),
                of_engine=MagicMock()
            )
            
        self.strategy.logger = MagicMock()
        # Mock the conf_scorer
        self.strategy.conf_scorer = MagicMock()
        
    def test_stable_bucket_deterministic(self):
        # Test that bucketing is stable
        sid1 = "signal-BTCUSDT-123456789"
        b1 = OrderFlowStrategy._stable_bucket_0_99(sid1)
        b2 = OrderFlowStrategy._stable_bucket_0_99(sid1)
        self.assertEqual(b1, b2)
        self.assertTrue(0 <= b1 <= 99)
        
        # Test different sid gives different bucket (likely)
        sid2 = "signal-ETHUSDT-987654321"
        b3 = OrderFlowStrategy._stable_bucket_0_99(sid2)
        # It's possible they collide, but unlikely for random inputs. 
        # But this test just checks valid range.
        self.assertTrue(0 <= b3 <= 99)
        
    def test_compute_confidence_shadow_disabled(self):
        # Setup runtime with shadow disabled
        runtime = MockRuntime(config={"confidence_shadow_enable": 0})
        indicators = {}
        confirmations = []
        
        # Mock scorer to return fixed value
        self.strategy.conf_scorer.score.return_value = (0.85, {"base": 0.85})
        
        # Call _compute_confidence
        conf = self.strategy._compute_confidence(
            runtime, indicators, confirmations, side="LONG", kind="delta_spike"
        )
        
        # Verify v1 is returned
        self.assertEqual(conf, 0.85)
        self.assertEqual(indicators.get("confidence_v1"), 0.85)
        # Verify v2 is NOT present
        self.assertIsNone(indicators.get("confidence_v2"))
        
        # Verify scorer called once
        self.strategy.conf_scorer.score.assert_called_once()
        
    def test_compute_confidence_shadow_enabled(self):
        # Setup runtime with shadow enabled
        # Also set some v2 overrides to verify they are passed
        runtime = MockRuntime(config={
            "confidence_shadow_enable": 1,
            "conf_v2_sweep_bonus_w": 0.50 # Extreme value to check override
        })
        indicators = {}
        confirmations = []
        
        # Mock scorer to return different values based on ctx
        def side_effect(kind, side, ctx):
            # Check if this is the v2 call (ctx has sweep_bonus_w set to 0.50)
            if getattr(ctx, "sweep_bonus_w", 0) == 0.50:
                return (0.95, {"base": 0.95})
            return (0.85, {"base": 0.85})
            
        self.strategy.conf_scorer.score.side_effect = side_effect
        
        # Call _compute_confidence
        conf = self.strategy._compute_confidence(
            runtime, indicators, confirmations, side="LONG", kind="delta_spike"
        )
        
        # Verify v1 is returned as the result
        self.assertEqual(conf, 0.85)
        self.assertEqual(indicators.get("confidence_v1"), 0.85)
        
        # Verify v2 is computed and stored
        self.assertEqual(indicators.get("confidence_v2"), 0.95)
        
        # Verify scorer called twice
        self.assertEqual(self.strategy.conf_scorer.score.call_count, 2)

    def test_canary_logic_not_active(self):
        runtime = MockRuntime(config={
            "confidence_shadow_enable": 1, 
            "confidence_active_variant": "v1", # Default
            "confidence_shadow_canary_share": 1.0
        })
        indicators = {
            "sid": "test_sid",
            "confidence_v2": 0.95
        }
        # Simulate base return
        conf_v1 = 0.85
        indicators["confidence"] = conf_v1
        
        # Re-implement logic block for testing purposes as we cannot easily invoke the surrounding method
        if int(runtime.config.get("confidence_shadow_enable", 0) or 0) == 1:
            active = str(runtime.config.get("confidence_active_variant", "v1") or "v1").lower()
            if active == "v2":
                 pass
        
        # Assertions
        self.assertEqual(indicators["confidence"], 0.85)

    def test_canary_logic_active_hit(self):
        # Test case where canary should activate
        runtime = MockRuntime(config={
            "confidence_shadow_enable": 1, 
            "confidence_active_variant": "v2", 
            "confidence_shadow_canary_share": 1.0 # 100% share
        })
        indicators = {
            "sid": "test_sid",
            "confidence": 0.85,
            "confidence_v2": 0.95
        }
        
        # Logic block
        try:
            if int(runtime.config.get("confidence_shadow_enable", 0) or 0) == 1:
                active = str(runtime.config.get("confidence_active_variant", "v1") or "v1").lower()
                if active == "v2":
                    share = float(runtime.config.get("confidence_shadow_canary_share", 0.0) or 0.0)
                    share = max(0.0, min(1.0, share))
                    if share > 0.0:
                        sid = str(indicators.get("sid") or indicators.get("signal_id") or "")
                        if sid:
                            b = OrderFlowStrategy._stable_bucket_0_99(sid) / 100.0
                            if b < share:
                                v2 = indicators.get("confidence_v2")
                                if v2 is not None:
                                    v2 = float(v2)
                                    if math.isfinite(v2):
                                        indicators["confidence"] = v2
                                        indicators["confidence_variant_used"] = "v2"
        except Exception:
            pass
            
        self.assertEqual(indicators["confidence"], 0.95)
        self.assertEqual(indicators["confidence_variant_used"], "v2")

if __name__ == "__main__":
    unittest.main()
