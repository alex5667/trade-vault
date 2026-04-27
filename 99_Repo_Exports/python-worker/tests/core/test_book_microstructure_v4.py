import unittest
import math
from core.book_microstructure_v4 import compute_microstructure_v4

class TestBookMicrostructureV4(unittest.TestCase):
    def setUp(self):
        self.levels = 5

    def _make_snap(self, bids, asks):
        # bids/asks: list of [px, qty]
        return {
            "bids": bids,
            "asks": asks
        }

    def test_empty_lists(self):
        snap = self._make_snap([], [])
        res = compute_microstructure_v4(snap, None, self.levels)
        self.assertEqual(res["mp_mid_bps"], 0.0)
        self.assertEqual(res["depth_bid_5"], 0.0)

    def test_microprice_calc(self):
        # simple case: 
        # bid: 100 @ 10, ask: 101 @ 30
        # mid = 100.5
        # mp = (10*101 + 30*100) / (10+30) = (1010 + 3000)/40 = 4010/40 = 100.25
        # diff = 100.25 - 100.5 = -0.25
        # bps = (-0.25 / 100.5) * 10000 = -24.8756...
        
        bids = [[100.0, 10.0]]
        asks = [[101.0, 30.0]]
        snap = self._make_snap(bids, asks)
        res = compute_microstructure_v4(snap, None, self.levels)
        
        mid = 100.5
        mp = (10.0 * 101.0 + 30.0 * 100.0) / 40.0
        expected_bps = ((mp - mid) / mid) * 10000.0
        
        self.assertAlmostEqual(res["mp_mid_bps"], expected_bps, places=4)

    def test_slope_and_convexity(self):
        # bids L1..L5
        # qty: 1, 1, 1, 1, 1 -> slope should be log(5/1)/4
        # cum: 1, 2, 3, 4, 5
        bids = [[100-i, 1.0] for i in range(5)]
        asks = [[101+i, 1.0] for i in range(5)]
        snap = self._make_snap(bids, asks)
        
        res = compute_microstructure_v4(snap, None, self.levels)
        
        # Slope check
        cum1 = 1.0
        cum5 = 5.0
        expected_slope = math.log(5.0/1.0) / 4.0
        self.assertAlmostEqual(res["book_slope_bid"], expected_slope, places=4)
        
        # Convexity check
        # v1=1, v3=3, v5=5
        # s13 = log(3/1)/2 = 0.5493
        # s35 = log(5/3)/2 = 0.2554
        # conv = s35 - s13 = -0.2938 (concave growth, i.e. linear volume is "concave" in log space?)
        expected_conv = (math.log(5.0/3.0)/2.0) - (math.log(3.0/1.0)/2.0)
        self.assertAlmostEqual(res["book_convex_bid"], expected_conv, places=4)

    def test_mp_shift(self):
        # prev: mp=100.25 (same as test_microprice_calc)
        # curr: mp=100.25 + delta
        # Let's say prev had equal volume: 100@20, 101@20 -> mp=100.5
        # current has 100@10, 101@30 -> mp=100.25
        # Shift = 100.25 - 100.5 = -0.25
        # Bps = -0.25/100.5 * 10000
        
        bids_p = [[100.0, 20.0]]
        asks_p = [[101.0, 20.0]]
        snap_p = self._make_snap(bids_p, asks_p)
        
        bids_c = [[100.0, 10.0]]
        asks_c = [[101.0, 30.0]]
        snap_c = self._make_snap(bids_c, asks_c)
        
        res = compute_microstructure_v4(snap_c, snap_p, self.levels)
        # Shift is mp_curr - mp_prev
        # mp_curr = 100.25
        # mp_prev = 100.5
        mid = 100.5
        expected_shift_bps = ((100.25 - 100.5) / mid) * 10000.0 
        self.assertAlmostEqual(res["mp_shift_bps"], expected_shift_bps, places=4)

    def test_obi_dw_calculation(self):
        # 2 levels
        # L1: bids=[100, 10], asks=[101, 30] -> imb = (10-30)/40 = -0.5, w=1
        # L2: bids=[99, 50], asks=[102, 50]  -> imb = 0, w=0.5
        
        # num = 1*(-0.5) + 0.5*(0) = -0.5
        # den = 1 + 0.5 = 1.5
        # res = -0.5 / 1.5 = -0.3333
        
        bids = [[100, 10], [99, 50]]
        asks = [[101, 30], [102, 50]]
        snap = self._make_snap(bids, asks)
        
        res = compute_microstructure_v4(snap, None, 2) # force 2 levels logic if loop relies on min(len, levels)
        
        # The code uses loop min(len, levels).
        # We passed 5 as default in test, but provided 2 levels.
        # It handles partial fill? No, it breaks if k > len
        
        self.assertAlmostEqual(res["obi_dw"], -1/3.0, places=4)

if __name__ == '__main__':
    unittest.main()
