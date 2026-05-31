import unittest

from tools.check_ml_confirm_stream_health import compute_health


class TestMLConfirmHealth(unittest.TestCase):
    def test_ok_stream(self):
        samples = [
            {"ts_ms": 1000, "status": "ok", "p_edge": 0.5, "conf": 0.8, "lat_ms": 10, "missing_n": 0},
            {"ts_ms": 900, "status": "ok", "p_edge": 0.4, "conf": 0.7, "lat_ms": 12, "missing_n": 0},
        ]
        ok, report = compute_health(samples, now_ms=1050, max_stale_ms=120000)
        self.assertTrue(ok)
        self.assertEqual(report["n"], 2)
        self.assertEqual(report["stale_ms"], 50)

    def test_stale_stream(self):
        samples = [
            {"ts_ms": 1000, "status": "ok", "p_edge": 0.5, "conf": 0.8, "lat_ms": 10, "missing_n": 0},
        ]
        ok, report = compute_health(samples, now_ms=200000, max_stale_ms=120000)
        self.assertFalse(ok)
        self.assertIn("stale_stream_ms", report["reason"])

    def test_high_error_rate(self):
        samples = [
            {"ts_ms": 1000, "status": "fail", "p_edge": 0.5, "conf": 0.8, "lat_ms": 10, "missing_n": 0},
            {"ts_ms": 900, "status": "error", "p_edge": 0.4, "conf": 0.7, "lat_ms": 12, "missing_n": 0},
            {"ts_ms": 800, "status": "ok", "p_edge": 0.4, "conf": 0.7, "lat_ms": 12, "missing_n": 0},
        ]
        ok, report = compute_health(samples, now_ms=1050, max_stale_ms=120000)
        self.assertFalse(ok)
        self.assertIn("err_rate", report["reason"])

    def test_zero_p_edge_rate(self):
        samples = [{"ts_ms": 1000, "status": "ok", "p_edge": 0.0, "conf": 0.8, "lat_ms": 10, "missing_n": 0}] * 100
        ok, report = compute_health(samples, now_ms=1050, max_stale_ms=120000)
        self.assertFalse(ok)
        self.assertIn("p_edge_zero_rate", report["reason"])

if __name__ == "__main__":
    unittest.main()
