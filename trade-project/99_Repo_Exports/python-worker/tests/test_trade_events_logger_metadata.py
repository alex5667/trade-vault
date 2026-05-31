
import unittest

from services.trade_events_logger import _merge_close_metadata


class TestTradeEventsLoggerMetadata(unittest.TestCase):
    def test_merge_close_metadata(self):
        # minimal: close_reason + custom metadata dict must co-exist
        md = {"ab_arm": "B", "ab_group": "thin", "pnl_r": 1.25}

        merged = _merge_close_metadata(
             close_reason="tp1",
             base=md,
             ab_arm="C", # Should override if logic allows, or base overrides?
             # Implementation: base is copied, then args added.
             # So args override base keys.
        )
        # If I pass ab_arm="C" as arg, it should overwrite "B" from base?
        # Let's check impl:
        # md = deepcopy(base)
        # if ab_arm: md["ab_arm"] = ab_arm
        # Yes, args override base.

        self.assertEqual(merged["ab_arm"], "C")
        self.assertEqual(merged["ab_group"], "thin")
        self.assertEqual(merged["pnl_r"], 1.25)
        self.assertEqual(merged["close_reason"], "tp1")

if __name__ == "__main__":
    unittest.main()
