
import json
import os
import time
import unittest
import tempfile
import shutil
from unittest.mock import MagicMock, patch

from orderflow_services.confidence_calibrator_bundle_runtime import ConfidenceCalibratorBundleRuntime

class TestConfidenceCalibratorBundleRuntime(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.bundle_path = os.path.join(self.test_dir, "test_bundle.json")
        
    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def _write_bundle(self, data):
        with open(self.bundle_path, "w") as f:
            json.dump(data, f)
        # Ensure mtime changes
        os.utime(self.bundle_path, None)

    def test_load_and_reload(self):
        data1 = {
            "schema_version": 2,
            "meta": {"bucket_by": "none"},
            "buckets": {
                "global": {"method": "identity"},
            }
        }
        self._write_bundle(data1)
        
        loader = ConfidenceCalibratorBundleRuntime(self.bundle_path, poll_interval_ms=100)
        loader.maybe_reload(1000)
        
        self.assertTrue(loader.config_loaded)
        res = loader.get_calibrated_confidence(0.5, {})
        self.assertEqual(res["result"], 0.5)
        self.assertEqual(res["method"], "identity")

        # Update bundle
        time.sleep(0.2) # Wait for mtime diff
        data2 = {
            "schema_version": 2,
            "meta": {"bucket_by": "none"},
            "buckets": {
                "global": {"method": "platt", "params": {"a": 10.0, "b": 0.0}}
            }
        }
        self._write_bundle(data2)
        
        # Should NOT reload yet (poll interval)
        loader.maybe_reload(1010) 
        # mtime check is done inside, but guarded by poll_interval.
        # last_check_ms was 1000. poll_interval 100. 1010 - 1000 = 10 < 100. No reload.
        
        # Advance time
        loader.maybe_reload(2000)
        
        res = loader.get_calibrated_confidence(0.5, {})
        # Platt: 1 / (1 + exp(-(10*0.5 + 0))) = 1 / (1 + exp(-5)) ~= 0.9933
        self.assertAlmostEqual(res["result"], 0.9933, places=4)
        self.assertEqual(res["method"], "platt")

    def test_bucketing_regime(self):
        data = {
            "schema_version": 2,
            "meta": {"bucket_by": "regime"},
            "buckets": {
                "global": {"method": "identity"},
                "trend_up": {"method": "input"}, # alias for identity
                "trend_down": {"method": "platt", "params": {"a": 0.0, "b": 100.0}} # sigmoid(100) -> 1.0
            }
        }
        self._write_bundle(data)
        loader = ConfidenceCalibratorBundleRuntime(self.bundle_path, poll_interval_ms=0)
        loader.maybe_reload(1000)
        
        # Case 1: trend_up -> properties
        res = loader.get_calibrated_confidence(0.5, {"regime": "trend_up"})
        self.assertEqual(res["bucket_key"], "trend_up")
        self.assertEqual(res["result"], 0.5)
        
        # Case 2: trend_down -> platt high
        res = loader.get_calibrated_confidence(0.5, {"regime": "trend_down"})
        self.assertEqual(res["bucket_key"], "trend_down")
        self.assertAlmostEqual(res["result"], 1.0, places=4)
        
        # Case 3: missing bucket -> fallback global
        res = loader.get_calibrated_confidence(0.3, {"regime": "range"})
        self.assertEqual(res["bucket_key"], "global")
        self.assertEqual(res["result"], 0.3)

    def test_methods(self):
        # Test beta, temperature, isotonic
        data = {
            "schema_version": 2,
            "meta": {"bucket_by": "none"},
            "buckets": {
                "global": {"method": "identity"}, # Only one bucket can exist if bucket_by=none but let's cheat and switch at runtime or test logic
            }
        }
        # Actually I can just inject the bundle dict directly for unit testing logic if I want
        # but let's stick to file for integration feel.
        
        # Let's test temperature
        data["buckets"]["global"] = {"method": "temperature_scaling", "params": {"temperature": 2.0}}
        self._write_bundle(data)
        loader = ConfidenceCalibratorBundleRuntime(self.bundle_path, poll_interval_ms=0)
        loader.maybe_reload(1000)
        
        # raw=0.731 (sigmoid(1)) -> logit=1. 
        # Scaled logit = 1/2 = 0.5. sigmoid(0.5) = 0.6224
        import math
        raw = 1.0 / (1.0 + math.exp(-1.0)) # ~0.73105
        res = loader.get_calibrated_confidence(raw, {})
        expected = 1.0 / (1.0 + math.exp(-0.5))
        self.assertAlmostEqual(res["result"], expected, places=4)
        
        # Test beta
        # a=2, b=1, c=0
        # logit = 2*ln(p) - 1*ln(1-p)
        data["buckets"]["global"] = {"method": "beta", "params": {"a": 2.0, "b": 1.0, "c": 0.0}}
        self._write_bundle(data)
        loader.last_check_ms = 0 # force reload
        loader.maybe_reload(2000)
        
        p = 0.8
        ln_p = math.log(p)
        ln_1_p = math.log(1-p)
        logit_beta = 2.0 * ln_p - 1.0 * ln_1_p 
        cal_beta = 1.0 / (1.0 + math.exp(-logit_beta))
        
        res = loader.get_calibrated_confidence(p, {})
        self.assertAlmostEqual(res["result"], cal_beta, places=4)

        # Test isotonic
        data["buckets"]["global"] = {"method": "isotonic", "params": {"boundaries": [0.0, 0.5, 1.0], "values": [0.0, 0.2, 0.8]}}
        self._write_bundle(data)
        loader.last_check_ms = 0
        loader.maybe_reload(3000)
        
        # 0.25 -> interp between 0.0 and 0.5 (values 0.0 and 0.2) -> 0.1
        res = loader.get_calibrated_confidence(0.25, {})
        self.assertAlmostEqual(res["result"], 0.1, places=4)
        
        # 0.75 -> interp between 0.5 and 1.0 (values 0.2 and 0.8) -> 0.2 + 0.5*(0.6) = 0.5
        res = loader.get_calibrated_confidence(0.75, {})
        self.assertAlmostEqual(res["result"], 0.5, places=4)


if __name__ == '__main__':
    unittest.main()
