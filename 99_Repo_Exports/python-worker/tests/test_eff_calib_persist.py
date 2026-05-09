from __future__ import annotations

import unittest

from core.eff_quote_calibrator import EffQuoteCalibrator


class TestEffCalibPersist(unittest.TestCase):
    def test_dump_load_regime_roundtrip(self):
        c = EffQuoteCalibrator(min_samples=10)
        for i in range(1, 30):
            c.update(regime="na", eff_quote=0.001 * i, quote_delta=10.0 * i)

        st = c.dump_regime_state(symbol="BTCUSDT", regime="na", updated_ts_ms=123456)
        c2 = EffQuoteCalibrator(min_samples=10)
        c2.load_regime_state(st)

        th1 = c.thresholds(regime="na", default_eff_th=0.5, default_min_qd=0.0)
        th2 = c2.thresholds(regime="na", default_eff_th=0.5, default_min_qd=0.0)

        self.assertEqual(th2.n, th1.n)
        self.assertAlmostEqual(th2.eff_quote_th, th1.eff_quote_th, places=9)
        self.assertAlmostEqual(th2.min_quote_delta, th1.min_quote_delta, places=9)

if __name__ == "__main__":
    unittest.main()
