import os
import unittest
from unittest.mock import MagicMock, patch

from tools import auto_apply_job_entrypoint_hardguard_v1 as sut


class TestAutoApplyJobEntrypointHardGuard(unittest.TestCase):
    def setUp(self):
        self.env_patcher = patch.dict(os.environ, {
            "REDIS_URL": "redis://localhost:6379/9",
            "AUTO_APPLY_CMD": "echo ok",
            "AUTO_APPLY_BLOCK_PREFIX": "test_block",
        })
        self.env_patcher.start()
        self.redis_mock = MagicMock()
        self.redis_patcher = patch("tools.auto_apply_job_entrypoint_hardguard_v1.redis.Redis.from_url", return_value=self.redis_mock)
        self.redis_patcher.start()

    def tearDown(self):
        self.redis_patcher.stop()
        self.env_patcher.stop()

    def test_missing_redis_url(self):
        with patch.dict(os.environ):
            os.environ.pop("REDIS_URL", None)
            with self.assertRaises(RuntimeError):
                sut._redis()

    def test_scan_block_keys(self):
        # Setup mock to return some keys
        self.redis_mock.scan.side_effect = [
            (10, [b"test_block:1"]),
            (0, [b"test_block:2"])
        ]
        keys = sut._scan_block_keys(self.redis_mock, "test_block")
        self.assertEqual(keys, [b"test_block:1", b"test_block:2"])

    def test_read_block_state_hash_blocked_true(self):
        self.redis_mock.type.return_value = b"hash"
        self.redis_mock.hgetall.return_value = {b"blocked": b"1", b"reason": b"foo"}
        blocked, reason, _ = sut._read_block_state(self.redis_mock, b"k")
        self.assertTrue(blocked)
        self.assertEqual(reason, "foo")

    def test_read_block_state_hash_blocked_false(self):
        self.redis_mock.type.return_value = b"hash"
        self.redis_mock.hgetall.return_value = {b"blocked": b"0", b"reason": b"foo"}
        blocked, reason, _ = sut._read_block_state(self.redis_mock, b"k")
        self.assertFalse(blocked)

    def test_read_block_state_string_blocked(self):
        self.redis_mock.type.return_value = b"string"
        self.redis_mock.get.return_value = b"1"
        blocked, _, _ = sut._read_block_state(self.redis_mock, b"k")
        self.assertTrue(blocked)

    def test_read_block_state_string_not_blocked(self):
        self.redis_mock.type.return_value = b"string"
        self.redis_mock.get.return_value = b"0"
        blocked, _, _ = sut._read_block_state(self.redis_mock, b"k")
        self.assertFalse(blocked)

    def test_main_skipped_frozen(self):
        # Simulate found block key
        self.redis_mock.scan.return_value = (0, [b"test_block:1"])
        # key matches hash with blocked=1
        self.redis_mock.type.return_value = b"hash"
        self.redis_mock.hgetall.return_value = {b"blocked": b"1", b"reason": b"test"}

        rc = sut.main()
        self.assertEqual(rc, 0) # Default skip exit code is 0

        # Verify no subprocess run
        # We need to spy on _run_cmd or subprocess.run, but here checking audit stream
        self.redis_mock.xadd.assert_called()
        # Check that we have at least one call with "decision" in fields
        found_decision = False
        for call in self.redis_mock.xadd.call_args_list:
            args, _ = call
            if len(args) >= 2 and "decision" in str(args[1]):
                 found_decision = True
                 break
        self.assertTrue(found_decision, "Audit record with decision not found in xadd calls")
        # ideally we check the content

    def test_main_run_ok(self):
        # No block keys
        self.redis_mock.scan.return_value = (0, [])

        with patch("subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0
            mock_run.return_value.stdout = "ok"
            rc = sut.main()
            self.assertEqual(rc, 0)
            mock_run.assert_called_once()

if __name__ == "__main__":
    unittest.main()
