import unittest

try:
    from services.orderflow.decision_ctx_fields import ensure_decision_ctx_fields
except Exception:
    # fallback for alternative import path during local runs
    from python_worker.services.orderflow.decision_ctx_fields import ensure_decision_ctx_fields  # type: ignore


class DummyRuntime:
    def __init__(self):
        self.symbol = "BTCUSDT"
        # minimal book snapshot compatible with .get() and .bids/.asks
        self.last_book = {
            "best_bid_px": 100.0,
            "best_ask_px": 101.0,
            "bids": [(100.0, 2.0), (99.5, 1.0)],
            "asks": [(101.0, 2.0), (101.5, 1.0)],
            "depth_5_bid_vol": 3.0,
            "depth_5_ask_vol": 3.0,
        }
        self.lob_depth_slope_bid = 1.2
        self.lob_depth_slope_ask = 1.1
        self.last_ofi_event = {"ofi_norm": 0.33}


class DecisionCtxFieldsTests(unittest.TestCase):
    def test_basic_mid_spread(self):
        ctx = {"tick_ts": 1700000000000, "best_bid": 100.0, "best_ask": 101.0}
        ensure_decision_ctx_fields(ctx, indicators={}, runtime=None, now_ms=1700000000000)
        self.assertEqual(ctx["decision_ts_ms"], 1700000000000)
        self.assertAlmostEqual(ctx["decision_mid"], 100.5, places=8)
        self.assertTrue("decision_spread_bps" in ctx)
        self.assertTrue(ctx["tca_ready"])

    def test_crossed_book_sets_flag_and_not_ready(self):
        ctx = {"tick_ts": 1700000000000, "best_bid": 101.0, "best_ask": 100.0}
        ensure_decision_ctx_fields(ctx, indicators={}, runtime=None, now_ms=1700000000000)
        self.assertIn("crossed_bbo", ctx.get("book_sanity_flags", []))
        self.assertFalse(ctx["tca_ready"])

    def test_runtime_fallback(self):
        rt = DummyRuntime()
        ctx = {"tick_ts": 1700000000000}
        ensure_decision_ctx_fields(ctx, indicators={}, runtime=rt, now_ms=1700000000000)
        self.assertAlmostEqual(ctx["decision_bid"], 100.0, places=8)
        self.assertAlmostEqual(ctx["decision_ask"], 101.0, places=8)
        self.assertAlmostEqual(ctx["decision_mid"], 100.5, places=8)
        self.assertAlmostEqual(ctx["decision_book_slope_bid"], 1.2, places=8)
        self.assertAlmostEqual(ctx["decision_book_slope_ask"], 1.1, places=8)
        self.assertAlmostEqual(ctx["decision_ofi_norm"], 0.33, places=8)
        # DWS proxy should exist when top5 is present
        self.assertTrue("decision_dws_bps" in ctx)

    def test_preserve_existing_fields(self):
        ctx = {"tick_ts": 1700000000000, "decision_mid": 123.0, "decision_bid": 122.0, "decision_ask": 124.0}
        ensure_decision_ctx_fields(ctx, indicators={}, runtime=None, now_ms=1700000000000)
        self.assertEqual(ctx["decision_mid"], 123.0)
        self.assertEqual(ctx["decision_bid"], 122.0)
        self.assertEqual(ctx["decision_ask"], 124.0)


if __name__ == "__main__":
    unittest.main()
