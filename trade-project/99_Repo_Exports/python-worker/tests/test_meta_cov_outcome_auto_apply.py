import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# Add tools directory to path
# [AUTOGRAVITY CLEANUP] sys.path.append(os.path.join(os.path.dirname(__file__), "../tools"))
from tools.meta_cov_outcome_auto_apply_v1 import main, summarize
import contextlib


class TestMetaCovOutcomeAutoApply(unittest.TestCase):
    def test_summarize(self):
        self.assertEqual(summarize([1.0, 2.0]), {"n": 2, "meanR": 1.5, "tail_rate": 0.0})
        self.assertEqual(summarize([-1.5, -0.5]), {"n": 2, "meanR": -1.0, "tail_rate": 0.5})
        self.assertEqual(summarize([]), {"n": 0, "meanR": 0.0, "tail_rate": 0.0})

    @patch("meta_cov_outcome_auto_apply_v1._redis")
    @patch("meta_cov_outcome_auto_apply_v1.read_closed_trades")
    def test_logic_downgrade(self, mock_read, mock_redis):
        # Setup mock redis
        r = MagicMock()
        mock_redis.return_value = r
        r.hgetall.return_value = {
            "meta_cov_rollout_last_change_ms": "0",
            "meta_enforce_share_cov_a": "1.0"
        }

        # Setup mock trades
        # Force "a" bucket to be bad:
        # Enforce: 100 trades, mean -0.5, tail 0.4
        # Control: 100 trades, mean 0.1, tail 0.1
        trades = []
        # Enforce bad
        for _ in range(100):
            trades.append({
                "meta_enforce_cov_bucket": "a",
                "meta_enforce_applied": 1,
                "r_mult": -1.5 # tail
            })
        # Control good
        for _ in range(100):
            trades.append({
                "meta_enforce_cov_bucket": "a",
                "meta_enforce_applied": 0,
                "r_mult": 0.5
            })

        mock_read.return_value = trades

        # Run main with dry run
        with patch.dict(os.environ, {
            "META_COV_OUTCOME_LOOKBACK_HOURS": "1",
            "META_COV_OUTCOME_MIN_N_ENFORCE": "10",
            "META_COV_OUTCOME_MIN_N_CONTROL": "10",
            "META_COV_OUTCOME_TAIL_THRESH": "0.3",
            "META_COV_OUTCOME_DOWN_STEP": "0.1"
        }):
            # Capture stdout
            from io import StringIO
            captured_output = StringIO()
            sys.stdout = captured_output
            with contextlib.suppress(SystemExit):
                main()
            sys.stdout = sys.__stdout__

            output = captured_output.getvalue()
            # We expect a decision to downgrade bucket 'a'
            self.assertIn('"ok": 1', output)
            self.assertIn('"bucket": "a"', output)
            self.assertIn('"new_share": 0.0', output) # severe panic tail > 0.45 -> 0.0

    @patch("meta_cov_outcome_auto_apply_v1._redis")
    @patch("meta_cov_outcome_auto_apply_v1.read_closed_trades")
    def test_logic_no_downgrade(self, mock_read, mock_redis):
        r = MagicMock()
        mock_redis.return_value = r
        r.hgetall.return_value = {}

        # Both good
        trades = []
        for _ in range(50):
            trades.append({"meta_enforce_cov_bucket": "b", "meta_enforce_applied": 1, "r_mult": 0.5})
            trades.append({"meta_enforce_cov_bucket": "b", "meta_enforce_applied": 0, "r_mult": 0.5})

        mock_read.return_value = trades

        with patch.dict(os.environ, {
            "META_COV_OUTCOME_MIN_N_ENFORCE": "10",
        }):
            from io import StringIO
            captured_output = StringIO()
            sys.stdout = captured_output
            with contextlib.suppress(SystemExit):
                main()
            sys.stdout = sys.__stdout__

            output = captured_output.getvalue()
            self.assertIn('"skipped": 1', output)
            self.assertIn('"reason": "no_downgrade"', output)

if __name__ == '__main__':
    unittest.main()
