import os
import unittest
from unittest.mock import MagicMock, patch

# Add python-worker to path to import tools
# [AUTOGRAVITY CLEANUP] sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tools import auto_apply_job_entrypoint_hardguard_v1 as guard


class TestAutoApplyGuardV1(unittest.TestCase):
    def test_split_csv(self):
        self.assertEqual(guard._split_csv(""), [])
        self.assertEqual(guard._split_csv("a,b,c"), ["a", "b", "c"])
        self.assertEqual(guard._split_csv(" a , b, c "), ["a", "b", "c"])

    def test_truthy(self):
        self.assertTrue(guard._truthy("1"))
        self.assertTrue(guard._truthy("true"))
        self.assertTrue(guard._truthy("yes"))
        self.assertFalse(guard._truthy("0"))
        self.assertFalse(guard._truthy("false"))
        self.assertFalse(guard._truthy(""))
        self.assertFalse(guard._truthy(None))

    def test_read_block_state_hash(self):
        r = MagicMock()

        # Case 1: Hash with explicit blocked=1
        r.type.return_value = b"hash"
        r.hgetall.return_value = {b"blocked": b"1", b"reason": b"manual"}
        blocked, reason, _ = guard._read_block_state(r, b"key", existence_blocks=True)
        self.assertTrue(blocked)
        self.assertEqual(reason, "manual")

        # Case 2: Hash with explicit blocked=0
        r.hgetall.return_value = {b"blocked": b"0"}
        blocked, reason, _ = guard._read_block_state(r, b"key", existence_blocks=True)
        self.assertFalse(blocked)

        # Case 3: Hash with no blocked field, existence_blocks=True
        r.hgetall.return_value = {b"some": b"data"}
        blocked, reason, _ = guard._read_block_state(r, b"key", existence_blocks=True)
        self.assertTrue(blocked)
        self.assertIn("blocked_key_present", reason)

        # Case 4: Hash with no blocked field, existence_blocks=False
        r.hgetall.return_value = {b"some": b"data"}
        blocked, reason, _ = guard._read_block_state(r, b"key", existence_blocks=False)
        self.assertFalse(blocked)

    def test_read_block_state_string(self):
        r = MagicMock()
        r.type.return_value = b"string"

        # Blocked
        r.get.return_value = b"1"
        blocked, _, _ = guard._read_block_state(r, b"key", existence_blocks=True)
        self.assertTrue(blocked)

        # Not blocked
        r.get.return_value = b"0"
        blocked, _, _ = guard._read_block_state(r, b"key", existence_blocks=True)
        self.assertFalse(blocked)

    @patch.dict(os.environ, {
        "REDIS_URL": "redis://localhost:6379/0",
        "AUTO_APPLY_CMD": "echo hello",
        "AUTO_APPLY_BLOCK_EXISTENCE_BLOCKS": "0",  # Test default off
        "AUTO_APPLY_BLOCK_IGNORE_KEY_SUFFIXES": ":ignored",
        "AUTO_APPLY_BLOCK_IGNORE_KEYS_REGEX": ".*:ignore_me$",
        "AUTO_APPLY_BLOCK_IGNORE_REASONS_REGEX": "skip_this_reason",
        "AUTO_APPLY_GUARD_METRICS_ENABLE": "1"
    })
    @patch('tools.auto_apply_job_entrypoint_hardguard_v1._redis')
    @patch('tools.auto_apply_job_entrypoint_hardguard_v1._run_cmd')
    def test_main_guard_logic(self, mock_run, mock_redis_ctor):
        r = MagicMock()
        mock_redis_ctor.return_value = r

        # Setup scenarios
        # 1. Ignored by suffix
        # 2. Ignored by regex
        # 3. Blocked by hash (explicit) <- should trigger block
        # 4. Ignored by existence-only check (implied by env var=0)

        # We mock _scan_block_keys to return a list of keys
        # We'll mock _read_block_state calls or just rely on the implementation if simple enough
        # Actually safer to mock the helper if we want to test main logic flow?
        # But we want integration of components. Let's rely on r calls.

        # Key1: prefix:ignored -> Suffix match
        # Key2: prefix:ignore_me -> Regex match
        # Key3: prefix:valid -> Explicit block

        mock_scan = [b"prefix:ignored", b"prefix:ignore_me", b"prefix:valid"]
        r.scan.side_effect = [(0, mock_scan)]

        r.type.return_value = b"hash"

        def hgetall_side_effect(key):
            if key == b"prefix:valid":
                return {b"blocked": b"1", b"reason": b"real_block"}
            return {b"blocked": b"1"} # Others would block if not ignored

        r.hgetall.side_effect = hgetall_side_effect

        mock_run.return_value = (0, "out", "err", 100)

        # Run main
        ret_code = guard.main()

        # Should exit 0 because SKIPPED_FROZEN defaults to 0
        self.assertEqual(ret_code, 0)
        # Should exit 0 because SKIPPED_FROZEN defaults to 0, but check decision logic
        # Wait, if blocked it exits 0 (default skip code) or custom.
        # Let's verify what happened via redis calls.

        # Verify emit metrics
        # Expecting SKIPPED_FROZEN because Key3 is valid block

        # Check if xadd called with decision=SKIPPED_FROZEN
        found_decision = False
        for call in r.xadd.call_args_list:
            args, kwargs = call
            if args[0] == "ops:auto_apply_runs" and args[1]['decision'] == "SKIPPED_FROZEN":
                 found_decision = True
                 self.assertEqual(args[1]['block_key'], "prefix:valid")
                 break
        self.assertTrue(found_decision, "Should have decided to SKIP based on prefix:valid")

        # Now test ignore reason
        # Set config to ignore "real_block" reason
        with patch.dict(os.environ, {"AUTO_APPLY_BLOCK_IGNORE_REASONS_REGEX": "real_block"}):
             # Reset mocks
             r.reset_mock()
             r.scan.side_effect = [(0, [b"prefix:valid"])]
             r.type.return_value = b"hash"
             r.hgetall.return_value = {b"blocked": b"1", b"reason": b"real_block"}

             # Run
             guard.main()

             # Should RUN because reason matches ignore regex
             # Check for RUN/OK
             found_run = False
             for call in r.xadd.call_args_list:
                args, kwargs = call
                if args[0] == "ops:auto_apply_runs" and args[1]['decision'] == "OK":
                     found_run = True
                     break
             self.assertTrue(found_run, "Should have RUN because reason was ignored")

if __name__ == '__main__':
    unittest.main()
