import unittest
import json
import os
import tempfile
import sys
import math

# Add python-worker to path to import runtime
# Assuming running from scanner_infra root
sys.path.append(os.path.join(os.getcwd(), 'python-worker'))

try:
    from orderflow_services.confidence_calibrator_bundle_runtime import ConfidenceCalibratorBundleRuntime
except ImportError:
    # Try alternate path if running from inside unittest discovery
# [AUTOGRAVITY CLEANUP]     sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../python-worker')))
    from orderflow_services.confidence_calibrator_bundle_runtime import ConfidenceCalibratorBundleRuntime

class TestConfidenceCalibratorBundleRuntimeHier(unittest.TestCase):
    
    def setUp(self):
        self.tmp_fd, self.tmp_path = tempfile.mkstemp(suffix=".json")
        os.close(self.tmp_fd)
        
        # Create a V3 Bundle
        self.bundle = {
            "schema_version": 3,
            "version": "test_v3",
            "generated_at": 1234567890,
            "meta": {
                "bucket_by": "hierarchical", # or "session_regime" but V3 logic applies
                "method_global": "platt"
            },
            "buckets": {
                "global": {
                    "method": "identity",
                    "params": {}
                },
                # Specific
                "BTCUSDT|ASIA|trend_up": {
                    "method": "platt_logit",
                    "params": {"a": 2.0, "b": 0.0} # steep sigmoid
                },
                # Fallback: Session Any
                "BTCUSDT|ASIA|any": {
                    "method": "platt_logit",
                    "params": {"a": 1.0, "b": 0.0} # standard sigmoid
                },
                # Fallback: Symbol Any
                "BTCUSDT|any|any": {
                    "method": "platt_logit", 
                    "params": {"a": 0.5, "b": 0.0} # flatter sigmoid
                },
                # Global Regimes
                "GLOBAL|any|trend_down": {
                    "method": "platt_logit",
                    "params": {"a": 0.1, "b": 0.0} # very flat
                }
            }
        }
        
        with open(self.tmp_path, "w") as f:
            json.dump(self.bundle, f)
            
        self.runtime = ConfidenceCalibratorBundleRuntime(self.tmp_path)
        self.runtime._load_bundle() # Force load

    def tearDown(self):
        if os.path.exists(self.tmp_path):
            os.remove(self.tmp_path)

    def test_schema_version(self):
        res = self.runtime.get_calibrated_confidence(0.5, {})
        self.assertEqual(res["schema_version"], 3)
        self.assertEqual(res["bucket_key"], "global")
        self.assertEqual(res["result"], 0.5) # Identity

    def test_exact_match(self):
        # BTCUSDT|ASIA|trend_up -> a=2.0
        # logit(0.5) = 0. 2*0 + 0 = 0. sigmoid(0) = 0.5. 
        # let's try 0.731 (logit ~ 1.0)
        # raw=0.731 -> logit=1.0. cal_logit = 2*1+0 = 2.0. sigmoid(2.0) = 0.88
        
        ctx = {"symbol": "BTCUSDT", "session": "ASIA", "regime": "trend_up"}
        raw = 0.731058 # sigmoid(1)
        res = self.runtime.get_calibrated_confidence(raw, ctx)
        
        self.assertEqual(res["bucket_key"], "BTCUSDT|ASIA|trend_up")
        self.assertAlmostEqual(res["result"], 0.880797, places=4)

    def test_fallback_session_any(self):
        # BTCUSDT|ASIA|neutral -> should match BTCUSDT|ASIA|any (a=1.0)
        ctx = {"symbol": "BTCUSDT", "session": "ASIA", "regime": "neutral"}
        raw = 0.731058 # mid high
        res = self.runtime.get_calibrated_confidence(raw, ctx)
        
        self.assertEqual(res["bucket_key"], "BTCUSDT|ASIA|any")
        # a=1.0 -> cal_logit = 1.0. sigmoid(1.0) = raw
        self.assertAlmostEqual(res["result"], raw, places=4)

    def test_fallback_symbol_any(self):
        # BTCUSDT|LONDON|neutral -> should match BTCUSDT|any|any (a=0.5)
        ctx = {"symbol": "BTCUSDT", "session": "LONDON", "regime": "neutral"}
        raw = 0.880797 # sigmoid(2) = raw. logit=2.
        # cal_logit = 0.5 * 2 = 1.0. sigmoid(1.0) = 0.731...
        
        res = self.runtime.get_calibrated_confidence(raw, ctx)
        
        self.assertEqual(res["bucket_key"], "BTCUSDT|any|any")
        self.assertAlmostEqual(res["result"], 0.731058, places=4)

    def test_fallback_global_regime(self):
        # ETHUSDT|LONDON|trend_down -> matches GLOBAL|any|trend_down (a=0.1)
        # ETHUSDT not in buckets, so falls back to global variants
        ctx = {"symbol": "ETHUSDT", "session": "LONDON", "regime": "trend_down"}
        raw = 0.99 # logit ~ 4.6
        # cal_logit = 0.1 * 4.6 = 0.46. sigmoid(0.46) ~ 0.613
        
        res = self.runtime.get_calibrated_confidence(raw, ctx)
        
        self.assertEqual(res["bucket_key"], "GLOBAL|any|trend_down")
        self.assertTrue(res["result"] < raw) 

    def test_fallback_global_catchall(self):
        # ETHUSDT|LONDON|neutral -> matches global (identity)
        ctx = {"symbol": "ETHUSDT", "session": "LONDON", "regime": "neutral"}
        raw = 0.6
        res = self.runtime.get_calibrated_confidence(raw, ctx)
        self.assertEqual(res["bucket_key"], "global")
        self.assertEqual(res["result"], 0.6)

if __name__ == '__main__':
    unittest.main()
