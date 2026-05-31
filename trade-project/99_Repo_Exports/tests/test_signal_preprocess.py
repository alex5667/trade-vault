import os
import sys
import pathlib
import unittest

# Ensure repo root is on sys.path so `services.*` imports work in plain unittest runs
REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.signal_preprocess import preprocess_signal_for_publish


class TestSignalPreprocess(unittest.TestCase):
    def test_flags_are_deduped_and_lowercased(self) -> None:
        os.environ["DQ_BOOK_STALE_FLAG_MS"] = "1000"
        os.environ["DQ_SPREAD_WIDE_FLAG_BPS"] = "10"

        sig = {
            "symbol": "btcusdt",
            "tick_ts": 1700000000000,
            "direction": "LONG",
            "micro": {"book_stale_ms": 1500, "spread_bps": 12.5},
            "indicators": {"tick_oood": 1, "tick_ts_missing": 0, "tick_gap_ms": 6000},
            "data_quality_flags": ["Wide_Spread", "wide_spread", "  "],
        }

        preprocess_signal_for_publish(sig, symbol="BTCUSDT", source="unit", logger=None)

        flags = sig.get("data_quality_flags") or []
        self.assertIn("stale_l2", flags)
        self.assertIn("wide_spread", flags)
        self.assertIn("tick_oood", flags)
        self.assertIn("tick_gap", flags)
        self.assertIn("missing_trade_id", flags)
        # dedup
        self.assertEqual(flags.count("wide_spread"), 1)

    def test_time_fields_are_epoch_ms(self) -> None:
        sig = {"symbol": "ethusdt", "ts": "1700000000123"}
        preprocess_signal_for_publish(sig, symbol="ETHUSDT", source="unit", logger=None)
        self.assertIsInstance(sig["ts_ms"], int)
        self.assertGreater(sig["ts_ms"], 0)
        self.assertIsInstance(sig["tick_ts"], int)
        self.assertGreater(sig["tick_ts"], 0)


if __name__ == "__main__":
    unittest.main()

