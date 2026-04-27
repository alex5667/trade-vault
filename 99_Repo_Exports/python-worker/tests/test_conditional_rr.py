import unittest
from signals.risk_levels import compute_levels

class TestConditionalTrueRR(unittest.TestCase):

    def test_a_default_stop_legacy_atr_logic(self):
        """
        Test A: TP_MODE=RR, стоп дефолтный -> legacy ATR/rocket НЕ игнорируется
        Ожидание: TP1 = 0.78 * ATR (rocket), а не stop_dist * RR1.
        """
        entry = 1000.0
        atr = 10.0
        side = "LONG"
        cfg = {
            "STOP_MODE": "ATR",
            "STOP_ATR_MULT": 0.8,
            "STOP_ATR_MULT_BASE": 0.8,
            "TP_MODE": "RR",
            "TP_RR": "1.0, 2.0, 3.0",
            "trail_profile": "rocket_v1",
            "ROCKET_TP1_ATR_MULT": 0.78
        }
        
        res = compute_levels(entry, atr, side, cfg, symbol="TEST_A")
        
        # Rocket TP1 = entry + 0.78 * atr = 1007.8
        self.assertAlmostEqual(res["tp_levels"][0], 1007.8)
        self.assertEqual(res["tp_mode_used"], "ATR_LEGACY")

    def test_b_significant_expansion_threshold(self):
        """
        Test B: TP_MODE=RR, стоп расширен существенно (ratio=1.5 >= 1.10) 
        Ожидание: Strict RR (с учетом компромисса для rocket_v1)
        """
        entry = 1000.0
        atr = 10.0
        side = "LONG"
        cfg = {
            "STOP_MODE": "ATR",
            "STOP_ATR_MULT_BASE": 0.8,
            "STOP_ATR_MULT": 1.2, # ratio = 1.2 / 0.8 = 1.5
            "TP_MODE": "RR",
            "TP_RR": "1.0, 2.0, 3.0",
            "trail_profile": "none" # no compromise
        }
        
        res = compute_levels(entry, atr, side, cfg, symbol="TEST_B")
        
        # stop_dist = 1.2 * 10 = 12.0
        # Strict RR TP1 = entry + 1.0 * 12.0 = 1012.0
        self.assertAlmostEqual(res["tp_levels"][0], 1012.0)
        self.assertEqual(res["tp_mode_used"], "RR_STRICT")

    def test_c_minor_expansion_stays_legacy(self):
        """
        Test C: TP_MODE=RR, стоп расширен мало (ratio=1.05 < 1.10)
        Ожидание: ATR_LEGACY
        """
        entry = 1000.0
        atr = 10.0
        side = "LONG"
        cfg = {
            "STOP_MODE": "ATR",
            "STOP_ATR_MULT_BASE": 0.8,
            "STOP_ATR_MULT": 0.84, # ratio = 0.84 / 0.8 = 1.05
            "TP_MODE": "RR",
            "TP_RR": "1.0, 2.0, 3.0"
        }
        
        res = compute_levels(entry, atr, side, cfg, symbol="TEST_C")
        self.assertEqual(res["tp_mode_used"], "ATR_LEGACY")

    def test_d_narrowing_stays_legacy(self):
        """
        Test D: TP_MODE=RR, стоп сужен -> ATR_LEGACY
        """
        entry = 1000.0
        atr = 10.0
        side = "LONG"
        cfg = {
            "STOP_MODE": "ATR",
            "STOP_ATR_MULT_BASE": 0.8,
            "STOP_ATR_MULT": 0.6, # ratio = 0.75
            "TP_MODE": "RR"
        }
        
        res = compute_levels(entry, atr, side, cfg, symbol="TEST_D")
        self.assertEqual(res["tp_mode_used"], "ATR_LEGACY")

    def test_e_rocket_compromise_in_strict_rr(self):
        """
        Test E: Strict RR active, but trail_profile=rocket_v1
        Ожидание: TP1 = ATR-based, TP2+ = RR-based
        """
        entry = 1000.0
        atr = 10.0
        side = "LONG"
        cfg = {
            "STOP_MODE": "ATR",
            "STOP_ATR_MULT_BASE": 0.8,
            "STOP_ATR_MULT": 1.2, # ratio=1.5
            "TP_MODE": "RR",
            "TP_RR": "1.0, 2.0, 3.0",
            "trail_profile": "rocket_v1",
            "ROCKET_TP1_ATR_MULT": 0.78
        }
        
        res = compute_levels(entry, atr, side, cfg, symbol="TEST_E")
        
        # TP1 (ATR-based) = 1000 + 0.78 * 10 = 1007.8
        self.assertAlmostEqual(res["tp_levels"][0], 1007.8)
        
        # TP2 (RR-based) = 1000 + 2.0 * stop_dist(12.0) = 1024.0
        self.assertAlmostEqual(res["tp_levels"][1], 1024.0)
        self.assertEqual(res["tp_mode_used"], "RR_STRICT")

    def test_f_slq_flag_overrides_threshold(self):
        """
        Test F: slq_used=1 activates Strict RR even if expansion is minor
        """
        entry = 1000.0
        atr = 10.0
        cfg = {
            "STOP_MODE": "ATR",
            "STOP_ATR_MULT": 0.81, # tiny expansion
            "slq_used": 1,
            "TP_MODE": "RR",
            "TP_RR": "1.0"
        }
        res = compute_levels(entry, atr, "LONG", cfg)
        self.assertEqual(res["tp_mode_used"], "RR_STRICT")

if __name__ == "__main__":
    unittest.main()
