import json
import os
import unittest
from tempfile import NamedTemporaryFile

from core.meta_freeze_file import MetaFreezeFile


class TestMetaFreezeFile(unittest.TestCase):
    def setUp(self):
        self.tmp = NamedTemporaryFile(delete=False, suffix=".json")
        self.tmp_path = self.tmp.name
        self.tmp.close()

    def tearDown(self):
        if os.path.exists(self.tmp_path):
            os.remove(self.tmp_path)

    def test_fail_open_nonexistent(self):
        # Test behavior when file doesn't exist
        guard = MetaFreezeFile("/nonexistent/path.json")
        state = guard.get_guard_state()
        self.assertEqual(state["freeze"], 0)
        self.assertEqual(state["ab_share_cap"], 1.0)
        self.assertEqual(state["comment"], "fallback_default")

    def test_load_valid_file(self):
        with open(self.tmp_path, "w") as f:
            json.dump({
                "freeze": 1,
                "ab_share_cap": 0.5,
                "enforce_share_cap": 0.2,
                "comment": "test_guard"
            }, f)

        guard = MetaFreezeFile(self.tmp_path, ttl_sec=0) # No TTL for testing
        state = guard.get_guard_state()
        self.assertEqual(state["freeze"], 1)
        self.assertEqual(state["ab_share_cap"], 0.5)
        self.assertEqual(state["enforce_share_cap"], 0.2)
        self.assertEqual(state["comment"], "test_guard")

    def test_ttl_cache(self):
        # 1. Write initial state
        with open(self.tmp_path, "w") as f:
            json.dump({"freeze": 1}, f)

        guard = MetaFreezeFile(self.tmp_path, ttl_sec=60) # Long TTL
        state1 = guard.get_guard_state()
        self.assertEqual(state1["freeze"], 1)

        # 2. Modify file
        with open(self.tmp_path, "w") as f:
            json.dump({"freeze": 0}, f)

        # 3. Should still return cached value
        state2 = guard.get_guard_state()
        self.assertEqual(state2["freeze"], 1)

        # 4. Force TTL bypass (by manually resetting shared stats if we were testing internal state,
        # but here we just test that it DOES cache)
        # To truly test expiration we'd need to mock time or wait, but simple cache check is enough.

    def test_corrupted_json_fail_open(self):
        # 1. Start with good state
        with open(self.tmp_path, "w") as f:
            json.dump({"freeze": 1}, f)

        guard = MetaFreezeFile(self.tmp_path, ttl_sec=0)
        state1 = guard.get_guard_state()
        self.assertEqual(state1["freeze"], 1)

        # 2. Corrupt file
        with open(self.tmp_path, "w") as f:
            f.write("{invalid_json:")

        # 3. Should return previous cache (fail-open)
        state2 = guard.get_guard_state()
        self.assertEqual(state2["freeze"], 1)

if __name__ == "__main__":
    unittest.main()
