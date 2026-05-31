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
        # CTR=5, thr=2.0, veto_mult=default 1.6 → veto threshold = 3.2 → would_veto=1
        indicators = {
            "taker_rate_ema": 10.0,
            "cancel_rate_ema": 50.0,  # CTR = 5
        }
        cfg = {
            "burst_gate_enable": 1,
            "burst_gate_mode": "penalty",
            "burst_ctr_thr": 2.0,
            "burst_pen_w": 0.1,
            "burst_pen_max": 0.5,
        }

        pen, veto, reason, snap = eval_burst_gate(indicators, cfg)

        # pen = (5.0 - 2.0) * 0.1 = 0.3
        self.assertAlmostEqual(pen, 0.3)
        self.assertEqual(veto, 0)  # no hard veto in penalty mode
        self.assertIn("ctr=", reason)
        self.assertAlmostEqual(float(snap["burst_ctr"]), 5.0)
        # would_veto=1 because CTR=5 > thr*veto_mult=3.2
        self.assertEqual(snap["burst_would_veto"], 1)
        self.assertEqual(snap["burst_would_veto_reason"], "veto_ctr")

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
        self.assertIn("veto_ctr", reason)

    def test_shadow_mode_no_hard_veto(self):
        # In shadow mode veto conditions are met but hard veto is suppressed
        indicators = {
            "taker_rate_ema": 10.0,
            "cancel_rate_ema": 100.0,  # CTR = 10 → exceeds thr*veto_mult = 2.0*1.6 = 3.2
        }
        cfg = {
            "burst_gate_enable": 1,
            "burst_gate_mode": "shadow",
            "burst_ctr_thr": 2.0,
            "burst_veto_mult": 1.6,
        }

        pen, veto, reason, snap = eval_burst_gate(indicators, cfg)

        self.assertEqual(veto, 0)                          # no hard block in shadow
        self.assertEqual(snap["burst_would_veto"], 1)      # but conditions were met
        self.assertEqual(snap["burst_would_veto_reason"], "veto_ctr")

    def test_penalty_no_would_veto(self):
        # Low CTR: no penalty, no would_veto
        indicators = {
            "taker_rate_ema": 10.0,
            "cancel_rate_ema": 5.0,  # CTR = 0.5, well below threshold
        }
        cfg = {"burst_gate_enable": 1, "burst_gate_mode": "penalty"}

        pen, veto, reason, snap = eval_burst_gate(indicators, cfg)

        self.assertEqual(pen, 0.0)
        self.assertEqual(veto, 0)
        self.assertEqual(snap["burst_would_veto"], 0)
        self.assertEqual(snap["burst_would_veto_reason"], "")

    def test_enforce_hard_veto(self):
        # In enforce mode the same conditions produce a hard veto
        indicators = {
            "taker_rate_ema": 10.0,
            "cancel_rate_ema": 100.0,  # CTR = 10
        }
        cfg = {
            "burst_gate_enable": 1,
            "burst_gate_mode": "enforce",
            "burst_ctr_thr": 2.0,
            "burst_veto_mult": 1.6,
        }

        pen, veto, reason, snap = eval_burst_gate(indicators, cfg)

        self.assertEqual(veto, 1)
        self.assertEqual(snap["burst_would_veto"], 1)
        self.assertIn("veto_ctr", reason)

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

    def test_snap_contains_burst_mode(self):
        # burst_mode must be in snap so Prometheus label can be set by the caller
        for mode in ("penalty", "shadow", "enforce"):
            indicators = {}
            cfg = {"burst_gate_enable": 1, "burst_gate_mode": mode}
            _, _, _, snap = eval_burst_gate(indicators, cfg)
            self.assertEqual(snap["burst_mode"], mode)

if __name__ == "__main__":
    unittest.main()
