from utils.time_utils import get_ny_time_millis
import json
import os
import sys
import tempfile
import time
import unittest


# Make tests runnable from repo root without requiring external PYTHONPATH.
_THIS_DIR = os.path.dirname(__file__)
_ROOT = os.path.abspath(os.path.join(_THIS_DIR, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


class TestMetaAbV2ReportExporterV1(unittest.TestCase):
    def test_parse_report_obj(self):
        from tools.meta_ab_v2_report_exporter_v1 import parse_report_obj

        now_ms = get_ny_time_millis()
        obj = {
            "ts_ms": now_ms - 10_000,
            "winner": "challenger",
            "counts": {"n_total": 1234, "n_eligible": 1000},
            "delta": {"exp_r_per_candidate": 0.003, "tail_rate_per_candidate": 0.0},
            "ramp": {"share_current": 0.1, "share_next": 0.15, "action": "increase_share"},
        }
        rep = parse_report_obj(obj, file_mtime_ms=None, stale_after_h=30.0)

        self.assertTrue(rep.parsed_ok)
        self.assertTrue(rep.run_ok)
        self.assertEqual(rep.winner, "challenger")
        self.assertEqual(rep.action, "increase_share")
        self.assertEqual(rep.n_total, 1234)
        self.assertEqual(rep.n_eligible, 1000)
        self.assertAlmostEqual(rep.share_next or 0.0, 0.15, places=8)
        self.assertIsNotNone(rep.report_age_sec)

    def test_read_report_missing(self):
        from tools.meta_ab_v2_report_exporter_v1 import read_report

        rep, err = read_report("/tmp/this_file_should_not_exist_123456789.json")
        self.assertFalse(rep.parsed_ok)
        self.assertIsNotNone(err)

    def test_read_report_success(self):
        from tools.meta_ab_v2_report_exporter_v1 import read_report

        now_ms = get_ny_time_millis()
        obj = {"ts_ms": now_ms, "winner": "tie", "counts": {"n_total": 1, "n_eligible": 1}, "delta": {}}

        with tempfile.TemporaryDirectory() as td:
            p = os.path.join(td, "r.json")
            with open(p, "w", encoding="utf-8") as f:
                json.dump(obj, f)
            rep, err = read_report(p)
            self.assertTrue(rep.parsed_ok)
            self.assertIsNone(err)


if __name__ == "__main__":
    unittest.main()
