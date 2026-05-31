import json
import os
import unittest
from types import SimpleNamespace

from orderflow_services.confidence_calibrator import get_cached_calibrator


class TestConfidenceCalibratorCache(unittest.TestCase):
    def setUp(self):
        self.test_path = "/tmp/test_calibrator.json"
        if os.path.exists(self.test_path):
            os.remove(self.test_path)
        self.runtime = SimpleNamespace()

    def tearDown(self):
        if os.path.exists(self.test_path):
            os.remove(self.test_path)

    def test_cache_loading(self):
        # 1. Initial: file missing -> return None
        cal = get_cached_calibrator(self.runtime, self.test_path)
        self.assertIsNone(cal)

        # 2. Create file
        config = {
            "type": "temp_logit",
            "t": 1.5,
            "schema_version": 1
        }
        with open(self.test_path, "w") as f:
            json.dump(config, f)

        # 3. Load -> should get calibrator (bypass throttle)
        cal = get_cached_calibrator(self.runtime, self.test_path, check_every_ms=0)
        self.assertIsNotNone(cal)
        self.assertEqual(cal.type, "temp_logit")
        self.assertEqual(cal.t, 1.5)

        # 4. Immediate second call -> should be cached (no disk check)
        # Even if we change the file, it shouldn't see it because of check_every_ms=5000
        with open(self.test_path, "w") as f:
            json.dump({"type": "temp_logit", "t": 2.0, "schema_version": 1}, f)

        cal2 = get_cached_calibrator(self.runtime, self.test_path, check_every_ms=5000)
        self.assertEqual(cal2.t, 1.5) # Still 1.5

        # 5. Force reload by setting check_every_ms=0
        cal3 = get_cached_calibrator(self.runtime, self.test_path, check_every_ms=0)
        self.assertEqual(cal3.t, 2.0) # Now 2.0

    def test_cache_fail_open(self):
        # Create valid file
        with open(self.test_path, "w") as f:
            json.dump({"type": "temp_logit", "t": 1.5, "schema_version": 1}, f)

        cal = get_cached_calibrator(self.runtime, self.test_path)
        self.assertIsNotNone(cal)

        # Delete file
        os.remove(self.test_path)

        # Should return None (fail-open) after throttle expires
        cal2 = get_cached_calibrator(self.runtime, self.test_path, check_every_ms=0)
        self.assertIsNone(cal2)

if __name__ == "__main__":
    unittest.main()
