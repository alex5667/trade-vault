
import unittest
from signals.risk_levels import compute_levels

class TestRiskLevelsRRMode(unittest.TestCase):
    
    def test_rr_stability_under_slq(self):
        """
        Verify that when SLQ increases STOP_ATR_MULT, the TP levels move further away
        to maintain the constant RR ratio.
        """
        entry = 1000.0
        atr = 10.0
        side = "LONG"
        
        # Scenario 1: Base SL
        cfg_base = {
            "STOP_MODE": "ATR",
            "STOP_ATR_MULT": 0.8,
            "TP_MODE": "RR",
            "TP_RR": "1.0,2.0,3.0",
            "slq_used": 1
        }
        res1 = compute_levels(entry, atr, side, cfg_base, symbol="TEST1")
        
        stop_dist1 = res1["stop_dist"]
        self.assertAlmostEqual(stop_dist1, 8.0) # 0.8 * 10
        
        tp1_dist1 = abs(res1["tp_levels"][0] - entry)
        rr1_actual = tp1_dist1 / stop_dist1
        self.assertAlmostEqual(rr1_actual, 1.0)
        
        
        # Scenario 2: SLQ increases Stop Multiplier to 1.5
        cfg_slq = {
            "STOP_MODE": "ATR",
            "STOP_ATR_MULT": 1.5,
            "TP_MODE": "RR",
            "TP_RR": "1.0,2.0,3.0",
            "slq_used": 1,
            "slq_original_mult": 0.8
        }
        res2 = compute_levels(entry, atr, side, cfg_slq, symbol="TEST2")
        
        stop_dist2 = res2["stop_dist"]
        self.assertAlmostEqual(stop_dist2, 15.0) # 1.5 * 10
        
        # TP should also move further
        tp1_dist2 = abs(res2["tp_levels"][0] - entry)
        self.assertGreater(tp1_dist2, tp1_dist1)
        
        # But RR ratio should remain exactly 1.0
        rr2_actual = tp1_dist2 / stop_dist2
        self.assertAlmostEqual(rr2_actual, 1.0)
        
        # Verify TP2 RR
        tp2_dist2 = abs(res2["tp_levels"][1] - entry)
        rr2_tp2 = tp2_dist2 / stop_dist2
        self.assertAlmostEqual(rr2_tp2, 2.0)

    def test_rr_ignores_atr_mults(self):
        """
        If TP_MODE=RR and is_widened, any legacy TP_ATR_MULTS should be strictly ignored.
        """
        entry = 1000.0
        atr = 10.0
        side = "LONG"
        
        cfg = {
            "STOP_MODE": "ATR",
            "STOP_ATR_MULT": 1.0,
            "TP_MODE": "RR",
            "TP_RR": "2.0",    # Expect TPs at +20
            "TP_ATR_MULTS": "0.1, 0.2", # Should be ignored (would be +1, +2)
            "slq_used": 1,
            "slq_original_mult": 0.5
        }
        
        res = compute_levels(entry, atr, side, cfg, symbol="TEST3")
        
        # Expected stop_dist = 10.0
        self.assertAlmostEqual(res["stop_dist"], 10.0)
        
        # Expected TP1 = entry + 20 (RR=2)
        tp1 = res["tp_levels"][0]
        self.assertAlmostEqual(tp1, 1020.0)
        
        # Not 1001.0 (ATR mult 0.1)
        self.assertNotAlmostEqual(tp1, 1001.0)

    def test_case_insensitivity(self):
        """
        Verify that lower-case keys also work.
        """
        entry = 1000.0
        atr = 10.0
        side = "SHORT"
        
        cfg = {
            "stop_mode": "atr",
            "stop_atr_mult": 1.0,
            "tp_mode": "rr",
            "tp_rr": "1.5",
            "slq_used": 1
        }
        
        res = compute_levels(entry, atr, side, cfg, symbol="SHORT_TEST")
        
        # Stop dist: 10
        self.assertAlmostEqual(res["stop_dist"], 10.0)
        
        # TP dist: 15 (1.5R)
        # Entry 1000 -> Short TP at 985
        tp1 = res["tp_levels"][0]
        self.assertAlmostEqual(tp1, 985.0)

    def test_conditional_rr_fallback_to_atr(self):
        """
        If TP_MODE=RR but SL is at default (not widened),
        it should fallback to ATR logic (respecting ATR mults).
        """
        entry = 100.0
        atr = 10.0
        side = "LONG"
        
        cfg = {
            "STOP_MODE": "ATR",
            "STOP_ATR_MULT": 0.8,
            "TP_MODE": "RR",
            "TP_RR": "1.0, 2.0",
            "TP_ATR_MULTS": "0.6, 1.2", # Should be USED because SL is default
            "slq_original_mult": 0.8,
            "slq_used": 0
        }
        
        res = compute_levels(entry, atr, side, cfg, symbol="TEST_FALLBACK")
        
        # Expected TP1 = 100 + (0.6 * 10) = 106.0
        # If it incorrectly used RR, it would be 100 + (1.0 * 8.0) = 108.0
        self.assertAlmostEqual(res["tp_levels"][0], 106.0)
        self.assertEqual(res["mode"]["tp"], "ATR")

    def test_conditional_rr_activation_on_widening(self):
        """
        If TP_MODE=RR and SL is widened, it should use RR logic.
        """
        entry = 100.0
        atr = 10.0
        side = "LONG"
        
        cfg = {
            "STOP_MODE": "ATR",
            "STOP_ATR_MULT": 1.2, # Widened from 0.8
            "TP_MODE": "RR",
            "TP_RR": "1.0, 2.0",
            "TP_ATR_MULTS": "0.6, 1.2", # Should be IGNORED
            "slq_original_mult": 0.8,
            "slq_used": 1
        }
        
        res = compute_levels(entry, atr, side, cfg, symbol="TEST_RR_ACTIVE")
        
        # stop_dist = 1.2 * 10 = 12.0
        # Expected TP1 = 100 + (1.0 * 12.0) = 112.0
        self.assertAlmostEqual(res["tp_levels"][0], 112.0)
        self.assertEqual(res["mode"]["tp"], "RR")

    def test_rocket_v1_preserved_on_default(self):
        """
        If TP_MODE=RR but SL is default, rocket_v1 TP1 should work.
        """
        entry = 100.0
        atr = 10.0
        side = "LONG"
        
        cfg = {
            "STOP_MODE": "ATR",
            "STOP_ATR_MULT": 0.8,
            "TP_MODE": "RR",
            "trail_profile": "rocket_v1",
            "ROCKET_TP1_ATR_MULT": 2.5, # Explicitly high for testing
            "TP_ATR_MULTS": "1.0, 2.0", # For TP2, TP3
            "slq_original_mult": 0.8,
        }
        
        res = compute_levels(entry, atr, side, cfg, symbol="TEST_ROCKET_DEFAULT")
        
        # Expected TP1 = entry + 2.5 * atr = 125.0
        self.assertAlmostEqual(res["tp_levels"][0], 125.0)

if __name__ == '__main__':
    unittest.main()
