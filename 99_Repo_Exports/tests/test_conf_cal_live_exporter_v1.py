import json
import os
import tempfile
import time
import unittest
from unittest.mock import MagicMock, patch

from orderflow_services.conf_cal_live_status_exporter_v1 import Exporter, _write_json_atomic


class TestConfCalLiveStatusExporter(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.TemporaryDirectory()
        self.status_path = os.path.join(self.test_dir.name, "status.json")
        self.state_path = os.path.join(self.test_dir.name, "state.json")

    def tearDown(self):
        self.test_dir.cleanup()

    def test_exporter_step_and_apply(self):
        # 1. Create dummy status
        status = {
            "ok": True,
            "ts_ms": int(time.time() * 1000),
            "degrade": False,
            "rows": 100,
            "rows_cal": 90,
            "bad_streak": 2,
            "bad_streak_max": 5,
            "raw": {"ece": 0.05, "brier": 0.20},
            "cal": {"ece": 0.04, "brier": 0.19},
            "rollback": {"performed": False},
        }
        _write_json_atomic(self.status_path, status)

        exporter = Exporter(self.status_path, self.state_path)
        exporter.step()

        # Check state (should be 0 initially)
        self.assertEqual(exporter.state.rollback_total, 0)

    def test_rollback_detection(self):
        # 1. Initial state
        exporter = Exporter(self.status_path, self.state_path)
        self.assertEqual(exporter.state.rollback_total, 0)

        # 2. Trigger rollback event
        ts_ms = int(time.time() * 1000)
        status = {
            "ok": True,
            "ts_ms": ts_ms,
            "rollback": {"performed": True},
            "rollback_total": 0, # Exporter should increment its own if loop hasn't yet
        }
        _write_json_atomic(self.status_path, status)
        exporter.step()

        self.assertEqual(exporter.state.rollback_total, 1)
        self.assertEqual(exporter.state.last_rb_event_ts_ms, ts_ms)

        # 3. Repeat same ts (should not double count)
        exporter.step()
        self.assertEqual(exporter.state.rollback_total, 1)

        # 4. New event
        ts_ms_2 = ts_ms + 1000
        status["ts_ms"] = ts_ms_2
        _write_json_atomic(self.status_path, status)
        exporter.step()
        self.assertEqual(exporter.state.rollback_total, 2)

    def test_sync_forward_rollback_total(self):
        # If live loop already has a higher rollback_total, sync to it
        exporter = Exporter(self.status_path, self.state_path)
        status = {
            "ok": True,
            "ts_ms": int(time.time() * 1000),
            "rollback_total": 5,
            "rollback": {"performed": False},
        }
        _write_json_atomic(self.status_path, status)
        exporter.step()
        self.assertEqual(exporter.state.rollback_total, 5)


if __name__ == "__main__":
    unittest.main()
