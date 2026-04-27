from utils.time_utils import get_ny_time_millis
# -*- coding: utf-8 -*-
import unittest
import time
import json
import os
from unittest.mock import MagicMock, patch
from orderflow_services import conf_score_guardrails_promote_v1 as promote

class TestPromote(unittest.TestCase):
    def setUp(self):
        self.tmp_state = "/tmp/test_conf_promote_health.json"

    def tearDown(self):
        if os.path.exists(self.tmp_state):
            os.remove(self.tmp_state)

    def write_state(self, data):
        with open(self.tmp_state, "w") as f:
            json.dump(data, f)

    def test_health_gates_clean(self):
        now = get_ny_time_millis()
        state = {
            "ts_ms": now,
            "degrade": 0,
            "metrics": {
                "ece_cal": 0.005,
                "brier_cal": 0.005,
                "n": 500
            }
        }
        self.write_state(state)
        
        ok, reason, _ = promote.check_health_gates(
            self.tmp_state, 
            max_age_sec=60, 
            ece_margin=0.01, 
            brier_margin=0.01, 
            min_n=300
        )
        self.assertTrue(ok, f"Should pass: {reason}")
        self.assertEqual(reason, "ok")

    def test_health_gates_stale(self):
        now = get_ny_time_millis()
        state = {
            "ts_ms": now - 70000, # 70s old
            "degrade": 0,
            "metrics": {"n": 500}
        }
        self.write_state(state)
        
        ok, reason, _ = promote.check_health_gates(self.tmp_state, max_age_sec=60, ece_margin=0.01, brier_margin=0.01, min_n=300)
        self.assertFalse(ok)
        self.assertIn("stale_state", reason)

    def test_health_gates_degraded(self):
        now = get_ny_time_millis()
        state = {
            "ts_ms": now,
            "degrade": 1, 
            "metrics": {"n": 500}
        }
        self.write_state(state)
        
        ok, reason, _ = promote.check_health_gates(self.tmp_state, max_age_sec=60, ece_margin=0.01, brier_margin=0.01, min_n=300)
        self.assertFalse(ok)
        self.assertEqual(reason, "degraded_state")

    def test_health_gates_ece_fail(self):
        now = get_ny_time_millis()
        state = {
            "ts_ms": now,
            "degrade": 0, 
            "metrics": {
                "ece_cal": 0.02, # > 0.01
                "n": 500
            }
        }
        self.write_state(state)
        
        ok, reason, _ = promote.check_health_gates(self.tmp_state, max_age_sec=60, ece_margin=0.01, brier_margin=0.01, min_n=300)
        self.assertFalse(ok)
        self.assertIn("ece_high", reason)

    def test_health_gates_min_n_fail(self):
        now = get_ny_time_millis()
        state = {
            "ts_ms": now,
            "degrade": 0, 
            "metrics": {
                "ece_cal": 0.001, 
                "n": 100 # < 300
            }
        }
        self.write_state(state)
        
        ok, reason, _ = promote.check_health_gates(self.tmp_state, max_age_sec=60, ece_margin=0.01, brier_margin=0.01, min_n=300)
        self.assertFalse(ok)
        self.assertIn("insufficient_n", reason)

    # Basic Redis logic test
    @patch("redis.Redis")
    def test_redis_apply(self, mock_redis_cls):
        mock_r = MagicMock()
        mock_redis_cls.from_url.return_value = mock_r
        
        bundle = {
            "ts_ms": 1234567890,
            "decisions": {
                "BTCUSDT": {"freeze": 1, "scale": 0.8}
            }
        }
        
        # Mock get to return empty or existing
        mock_r.get.return_value = None
        
        count = promote.apply_bundle_to_live(mock_r, bundle, "prefix:", dry_run=False)
        self.assertEqual(count, 1)
        
        # Verify set call
        mock_r.set.assert_called_once()
        args = mock_r.set.call_args
        key = args[0][0]
        val = json.loads(args[0][1])
        
        self.assertEqual(key, "prefix:BTCUSDT")
        self.assertEqual(val["confidence_score_freeze"], 1)
        self.assertEqual(val["confidence_score_scale"], 0.8)
        self.assertEqual(val["conf_score_guard_source"], "promote")

if __name__ == "__main__":
    unittest.main()
