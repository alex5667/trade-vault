import unittest

from core.burst_gate_v1 import eval_burst_gate


class TestBurstGateV1(unittest.TestCase):
    def test_burst_gate_defaults(self):
        indicators = {}
        cfg = {"burst_gate_enable": 1}
        pen, veto, reason, snap = eval_burst_gate(indicators, cfg)

        self.assertEqual(pen, 0.0)
        self.assertEqual(veto, 0)
        self.assertEqual(reason, "ok")
        self.assertIn("burst_ctr", snap)

    def test_burst_penalty_ctr(self):
        # High CTR -> penalty
        indicators = {
            "taker_rate_ema": 10.0,
            "cancel_rate_ema": 50.0 # CTR = 5
        }
        cfg = {
            "burst_gate_enable": 1,
            "burst_gate_mode": "penalty",
            "burst_ctr_thr": 2.0,
            "burst_pen_w": 0.1,
            "burst_pen_max": 0.5
        }

        pen, veto, reason, snap = eval_burst_gate(indicators, cfg)

        # excess = 5.0 - 2.0 = 3.0
        # pen = 3.0 * 0.1 = 0.3
        self.assertAlmostEqual(pen, 0.3)
        self.assertEqual(veto, 0)
        self.assertIn("ctr=", reason)
        self.assertAlmostEqual(snap["burst_ctr"], 5.0)

    def test_burst_veto(self):
        # Very high CTR -> veto
        indicators = {
            "taker_rate_ema": 10.0,
            "cancel_rate_ema": 100.0 # CTR = 10
        }
        cfg = {
            "burst_gate_enable": 1,
            "burst_gate_mode": "veto",
            "burst_ctr_thr": 2.0,
            "burst_veto_mult": 2.0 # veto at 4.0
        }

        pen, veto, reason, snap = eval_burst_gate(indicators, cfg)

        self.assertEqual(veto, 1)
        self.assertIn("VETO", reason)

    def test_hawkes_excess(self):
        indicators = {
            "hawkes_trade_lam": 20.0,
            "hawkes_cancel_lam": 5.0
        }
        cfg = {
            "burst_gate_enable": 1,
            "hawkes_mu_t": 10.0,
            "hawkes_mu_c": 10.0,
            "burst_gate_mode": "penalty"
        }

        # Excess T = 20/10 = 2.0 -> >1.0 -> penalty
        # Excess C = 5/10 = 0.5 -> <1.0 -> no penalty

        pen, veto, reason, snap = eval_burst_gate(indicators, cfg)

        self.assertIn("burst_exc", snap)
        # combined excess max(2.0, 0.5, 0.0) = 2.0
        self.assertAlmostEqual(snap["burst_exc"], 2.0)

if __name__ == "__main__":
    unittest.main()
