
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

    def test_parse_entry_key_extracted_when_bucket_already_at_toplevel(self):
        """Regression: P41 adds meta_enforce_cov_bucket as native TradeClosed field.
        It arrives at top-level in stream data. Before this fix, _parse_entry skipped
        signal_payload extraction entirely when bucket was found, so meta_enforce_key
        was never extracted. Result: used=0 for range/other buckets.
        """
        import json
        fields = {
            b"event": b"POSITION_CLOSED",
            b"r_mult": b"0.5",
            b"meta_enforce_cov_bucket": b"range",   # native P41 field — always present
            b"meta_enforce_applied": b"1",
            b"exit_ts_ms": b"1748000000001",
            b"signal_payload": json.dumps({
                "indicators": {
                    "of_confirm": {
                        "evidence": {
                            "meta_enforce_bucket": "range",
                            "meta_enforce_cov_bucket": "range",
                            "meta_enforce_key": "crypto-of:BTCUSDT:1748000000001|LONG|iceberg",
                            "meta_enforce_salt": "enf_v1",
                            "meta_veto": 1,
                        }
                    }
                }
            }).encode(),
        }
        result = meta_cov_outcome_guard_v1._parse_entry(fields)
        self.assertEqual(result.get("meta_enforce_cov_bucket"), "range")
        self.assertEqual(
            result.get("meta_enforce_key"),
            "crypto-of:BTCUSDT:1748000000001|LONG|iceberg",
            "meta_enforce_key must be extracted from signal_payload even when bucket is at top level",
        )
        self.assertEqual(result.get("meta_enforce_salt"), "enf_v1")
        self.assertEqual(result.get("meta_veto"), 1)

    def test_parse_entry_key_missing_when_no_signal_payload(self):
        """When signal_payload is absent, meta_enforce_key stays empty (expected)."""
        fields = {
            b"event": b"POSITION_CLOSED",
            b"r_mult": b"0.3",
            b"meta_enforce_cov_bucket": b"trend",
            b"exit_ts_ms": b"1748000000002",
        }
        result = meta_cov_outcome_guard_v1._parse_entry(fields)
        self.assertEqual(result.get("meta_enforce_cov_bucket"), "trend")
        self.assertEqual(result.get("meta_enforce_key", ""), "")

    def test_parse_entry_bucket_extracted_from_signal_payload_when_absent_at_toplevel(self):
        """Old path still works: if bucket not at top level, extract both bucket and key from signal_payload."""
        import json
        fields = {
            b"event": b"POSITION_CLOSED",
            b"r_mult": b"1.2",
            b"exit_ts_ms": b"1748000000003",
            # No meta_enforce_cov_bucket at top level (pre-P41 record)
            b"signal_payload": json.dumps({
                "indicators": {
                    "of_confirm": {
                        "evidence": {
                            "meta_enforce_cov_bucket": "trend",
                            "meta_enforce_key": "crypto-of:ETHUSDT:1748000000003|SHORT|delta_spike",
                            "meta_enforce_salt": "enf_v1",
                            "meta_veto": 0,
                        }
                    }
                }
            }).encode(),
        }
        result = meta_cov_outcome_guard_v1._parse_entry(fields)
        self.assertEqual(result.get("meta_enforce_cov_bucket"), "trend")
        self.assertEqual(result.get("meta_enforce_key"), "crypto-of:ETHUSDT:1748000000003|SHORT|delta_spike")

    def test_simulate_share_used_zero_when_key_missing(self):
        """Rows without meta_enforce_key are skipped (used=0)."""
        rows = [
            {"meta_enforce_cov_bucket": "range", "r_mult": "0.5", "meta_veto": "0"},  # no key
            {"meta_enforce_cov_bucket": "range", "r_mult": "-0.5", "meta_veto": "1"},  # no key
        ]
        res = meta_cov_outcome_guard_v1._simulate_share(rows, 1.0, "nosalt")
        self.assertEqual(res["used"], 0)
        self.assertEqual(res["exec_rate"], 0.0)

    def test_simulate_share_used_correct_when_key_present(self):
        """Rows with meta_enforce_key are counted and simulated correctly."""
        rows = [
            {"meta_enforce_key": "k1", "meta_enforce_cov_bucket": "range", "r_mult": "0.8", "meta_veto": "0"},
            {"meta_enforce_key": "k2", "meta_enforce_cov_bucket": "range", "r_mult": "-0.4", "meta_veto": "0"},
        ]
        res = meta_cov_outcome_guard_v1._simulate_share(rows, 1.0, "nosalt")
        self.assertEqual(res["used"], 2)
        self.assertGreater(res["exec_rate"], 0.0)


if __name__ == '__main__':
    unittest.main()
