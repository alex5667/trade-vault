import os
import sys
import unittest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from tools.recommend_tick_quality_policy import compute_recommendation


class TestRecommendTickQualityPolicy(unittest.TestCase):
    def test_low_sample_recommends_ignore(self):
        smoke = {
            "ticks": {
                "n": 100,
                "by_side_conf": {"unknown": 20},
                "by_ts_source": {"payload": 100},
                "abs_event_stream_skew": {"p99_ms": 1000.0},
            }
        }
        out = compute_recommendation(smoke)
        env = out["recommendations"]["env"]
        self.assertEqual(env["CRYPTO_OF_UNKNOWN_SIDE_POLICY"], "ignore_delta")

    def test_quarantine_recommendation(self):
        smoke = {
            "ticks": {
                "n": 5000,
                "by_side_conf": {"unknown": 400},
                "by_ts_source": {"payload": 4800, "now": 200},
                "abs_event_stream_skew": {"p99_ms": 2500.0},
            }
        }
        out = compute_recommendation(smoke)
        env = out["recommendations"]["env"]
        self.assertEqual(env["CRYPTO_OF_UNKNOWN_SIDE_POLICY"], "quarantine")
        self.assertIn("TICK_SIDE_QUARANTINE_SAMPLE", env)
        # max_ts_skew is 2x p99, clamped
        self.assertEqual(env["CRYPTO_OF_MAX_TS_SKEW_MS"], 5000)

        yaml_rules = out.get("prometheus_rules_yaml") or ""
        self.assertIn("ticks_ts_source_total", yaml_rules)
        self.assertIn("ticks_unknown_side_policy_total", yaml_rules)


