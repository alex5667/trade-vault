from __future__ import annotations

import unittest

from core.eff_quote_calibrator import EffQuoteCalibrator


class TestEffCalibrator(unittest.TestCase):
    def test_calibrator_ready_and_thresholds(self):
        c = EffQuoteCalibrator(min_samples=10)
        for i in range(1, 21):
            # eff_quote increasing, quote_delta increasing
            c.update(regime="na", eff_quote=float(i) * 0.001, quote_delta=float(i) * 10.0)

        self.assertTrue(c.ready("na"))
        th = c.thresholds(regime="na", default_eff_th=0.5, default_min_qd=0.0)

        self.assertGreaterEqual(th.n, 10)
        self.assertGreater(th.eff_quote_th, 0.0)
        self.assertTrue(th.src.startswith("calib"))

        # Check P2 values approximately (p20 of 1..20 is around 4-5)
        self.assertLess(th.eff_quote_th, 0.01)
        self.assertGreater(th.eff_quote_th, 0.001)

    def test_p2_deterministic(self):
        from core.quantile_p2 import P2Quantile
        p2 = P2Quantile(p=0.5)
        data = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
        for x in data:
            p2.update(x)

        val1 = p2.value()
        # Non-random
        p2_2 = P2Quantile(p=0.5)
        for x in data:
            p2_2.update(x)
        self.assertEqual(val1, p2_2.value())

if __name__ == "__main__":
    unittest.main()
