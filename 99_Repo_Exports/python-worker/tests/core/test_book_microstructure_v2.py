import unittest
from core.book_microstructure_v2 import compute_queue_imbalance_topn, compute_ofi_multilevel_topn

class TestBookMicrostructureV2(unittest.TestCase):
    def test_qimb(self):
        # Top 3 levels
        # Bids: 100@10, 200@9, 300@8
        # Asks: 50@11, 100@12, 150@13
        snap = {
            "bids": [[10, 100], [9, 200], [8, 300]],
            "asks": [[11, 50], [12, 100], [13, 150]]
        }
        qimb = compute_queue_imbalance_topn(snap, levels=3)
        
        # L1: (100 - 50) / (100 + 50) = 50 / 150 = 0.333
        self.assertAlmostEqual(qimb["qimb_l1"], 0.333333, places=3)
        
        # L2: (200 - 100) / (200 + 100) = 100 / 300 = 0.333
        self.assertAlmostEqual(qimb["qimb_l2"], 0.333333, places=3)
        
        # L3: (300 - 150) / (300 + 150) = 150 / 450 = 0.333
        self.assertAlmostEqual(qimb["qimb_l3"], 0.333333, places=3)
        
        # WMean: (0.333*1 + 0.333*0.5 + 0.333*0.333) / (1 + 0.5 + 0.333) = 0.333
        self.assertAlmostEqual(qimb["qimb_wmean"], 0.333333, places=3)

    def test_ofi(self):
        # Prev: B=[10, 100], A=[11, 100]
        # Curr: B=[10, 150], A=[11, 80]
        # Bid flow: p=p_prev => q - q_prev = 150 - 100 = +50
        # Ask flow: p=p_prev => q - q_prev = 80 - 100 = -20
        # OFI = flow_b - flow_a = 50 - (-20) = 70
        
        prev = {"bids": [[10, 100]], "asks": [[11, 100]]}
        curr = {"bids": [[10, 150]], "asks": [[11, 80]]}
        
        ofi = compute_ofi_multilevel_topn(prev, curr, levels=1)
        self.assertAlmostEqual(ofi["ofi_ml"], 70.0)
        self.assertAlmostEqual(ofi["ofi_ml_wsum"], 70.0) # weight 1.0 for L1

    def test_missing_snap(self):
        # Should return empty
        self.assertEqual(compute_queue_imbalance_topn(None), {})
        self.assertEqual(compute_ofi_multilevel_topn(None, {}), {})
        self.assertEqual(compute_ofi_multilevel_topn({}, None), {})

    def test_empty_levels(self):
        # Snap with empty bids/asks
        snap = {"bids": [], "asks": []}
        qimb = compute_queue_imbalance_topn(snap)
        # Should return empty or handle gracefully
        # Current impl checks if not bids and not asks -> return out (empty)
        self.assertEqual(qimb, {})
        
        # OFI with empty
        prev = {"bids": [], "asks": []}
        ofi = compute_ofi_multilevel_topn(prev, snap)
        self.assertEqual(ofi["ofi_ml"], 0.0)


if __name__ == "__main__":
    unittest.main()
