
import unittest

from core.microbar import MicroBarAggregator


class TestPortableFootprint(unittest.TestCase):
    def test_bucket_snapping(self):
        # Symbol with tick_size 0.5
        agg = MicroBarAggregator(symbol="TEST", tick_size=0.5)
        agg.fp_enabled = True
        agg.fp_bucket_bp = 10.0 # 10 bps

        # At price 1000, 10 bps is 1.0. 1.0 is a multiple of 0.5.
        agg._start_new_bar(1000000, 1000.0)
        self.assertEqual(agg.cur.fp_bucket_px, 1.0)

        # At price 100, 10 bps is 0.1. Tick size is 0.5. Should snap to 0.5.
        agg._start_new_bar(2000000, 100.0)
        self.assertEqual(agg.cur.fp_bucket_px, 0.5)

        # At price 5000, 10 bps is 5.0. Multiple of 0.5.
        agg._start_new_bar(3000000, 5000.0)
        self.assertEqual(agg.cur.fp_bucket_px, 5.0)

        # At price 1000.25, 10 bps is 1.00025. Should snap to 1.0.
        agg._start_new_bar(4000000, 1000.25)
        self.assertEqual(agg.cur.fp_bucket_px, 1.0)

    def test_efficiency_normalization(self):
        from core.footprint_lite import FootprintLite

        # High priced symbol (BTC)
        fp_btc = FootprintLite(bucket_px=10.0)
        fp_btc.update(price=50000.0, qty=1.0, signed_qty=1.0)
        # Move 100 points ($50000 -> $50100) = 20 bps
        # Delta 1 BTC ($50000 core value)
        # move_bp = 20.0
        # quote_delta = 50000.0
        # eff_quote = 20 / 50000 = 0.0004
        snap_btc = fp_btc.finalize(
            bar_open=50000.0,
            bar_close=50100.0,
            bar_high=50100.0,
            bar_low=50000.0,
            bar_delta_sum=1.0,
            bar_vol=1.0
        )

        # Low priced symbol (XRP)
        fp_xrp = FootprintLite(bucket_px=0.001)
        fp_xrp.update(price=0.500, qty=100000.0, signed_qty=100000.0)
        # Move 0.001 points ($0.500 -> $0.501) = 20 bps
        # Delta 100,000 XRP ($50000 core value)
        # move_bp = 20.0
        # quote_delta = 50000.0
        # eff_quote = 20 / 50000 = 0.0004
        snap_xrp = fp_xrp.finalize(
            bar_open=0.500,
            bar_close=0.501,
            bar_high=0.501,
            bar_low=0.500,
            bar_delta_sum=100000.0,
            bar_vol=100000.0
        )

        self.assertAlmostEqual(snap_btc.extra["fp_eff_quote"], snap_xrp.extra["fp_eff_quote"], places=6)
        # 10000 * 100 / 50050 / 50050 = 0.000399201...
        self.assertAlmostEqual(snap_btc.extra["fp_eff_quote"], 0.000399201, places=6)

if __name__ == "__main__":
    unittest.main()
