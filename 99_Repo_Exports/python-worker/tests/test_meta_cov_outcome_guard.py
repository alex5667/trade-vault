
import os
import unittest
from unittest.mock import MagicMock, patch

# Add the python-worker directory to the python path
# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

# Use a relative import or ensure we pick the right tools package
try:
    from tools import meta_cov_outcome_guard_v1
except ImportError:
    # If tools is shadowed, try importing directly from the file path source
    import importlib.util
    spec = importlib.util.spec_from_file_location("meta_cov_outcome_guard_v1",
        os.path.abspath(os.path.join(os.path.dirname(__file__), '../tools/meta_cov_outcome_guard_v1.py')))
    meta_cov_outcome_guard_v1 = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(meta_cov_outcome_guard_v1)

class TestMetaCovOutcomeGuard(unittest.TestCase):
    def setUp(self):
        self.mock_redis = MagicMock()

    def test_simulate_share_logic(self):
        # Setup data
        rows = [
            {"meta_enforce_key": "k1", "r_mult": "1.0", "meta_veto": "0"}, # Hash(nosalt:k1) approx 0.5 ish? No, strict hash.
            {"meta_enforce_key": "k2", "r_mult": "-1.0", "meta_veto": "1"},
        ]

        # We need to control the hash to test logic deterministically
        with patch('tools.meta_cov_outcome_guard_v1._sha_to_unit_interval') as mock_hash:
            # k1 -> 0.1 (low), k2 -> 0.9 (high)
            mock_hash.side_effect = lambda salt, key: 0.1 if key == "k1" else 0.9

            # Share 0.5:
            # k1 (0.1 < 0.5) -> Apply. Veto=0 -> Exec. Outcome=1.0
            # k2 (0.9 > 0.5) -> No Apply. Outcome=-1.0 (original)
            # Result: 1 exec, 1 original.
            # Wait, logic is:
            # if apply:
            #   if veto: blocked (outcome 0)
            #   else: outcome r_mult
            # else: outcome r_mult

            # Case 1: Share 0.5
            # k1: apply=True, veto=0 -> outcome 1.0. Used=1.
            # k2: apply=False -> outcome -1.0. Used=1.
            res = meta_cov_outcome_guard_v1._simulate_share(rows, 0.5, "nosalt")
            self.assertEqual(res['used'], 2)
            self.assertEqual(res['blocked'], 0)
            self.assertEqual(res['opp']['n'], 2.0)
            # Mean = (1.0 - 1.0) / 2 = 0.0
            self.assertAlmostEqual(res['opp']['meanR'], 0.0)

            # Case 2: Share 1.0
            # k1: apply=True, veto=0 -> outcome 1.0
            # k2: apply=True, veto=1 -> blocked -> outcome 0.0
            res = meta_cov_outcome_guard_v1._simulate_share(rows, 1.0, "nosalt")
            self.assertEqual(res['used'], 2)
            self.assertEqual(res['blocked'], 1)
            # opp is [1.0, 0.0] -> mean 0.5
            self.assertAlmostEqual(res['opp']['meanR'], 0.5)

    def test_summary_stats(self):
        xs = [1.0, -1.0, 0.5, -2.0]
        stats = meta_cov_outcome_guard_v1._summary_stats(xs)
        self.assertEqual(stats['n'], 4.0)
        self.assertEqual(stats['meanR'], -0.375) # (1-1+0.5-2)/4 = -1.5/4 = -0.375
        self.assertEqual(stats['tail_rate_le_neg1R'], 0.5) # -1.0 and -2.0

if __name__ == '__main__':
    unittest.main()
