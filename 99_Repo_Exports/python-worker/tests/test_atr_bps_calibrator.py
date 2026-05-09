import unittest

from core.atr_bps_calibrator import ATRBpsCalibrator


class TestATRBpsCalibrator(unittest.TestCase):
    def test_bootstrap(self):
        calib = ATRBpsCalibrator(min_samples=100)
        # Not enough samples -> returns bootstrap defaults
        th = calib.thresholds(
            regime="trend",
            default_floor_t0=3.0,
            default_floor_t1=5.0,
            default_floor_t2=8.0
        )
        self.assertEqual(th.floor_t0, 3.0)
        self.assertEqual(th.floor_t1, 5.0)
        self.assertEqual(th.floor_t2, 8.0)
        self.assertEqual(th.n, 0)
        self.assertEqual(th.src, "static")

    def test_update_logic(self):
        calib = ATRBpsCalibrator(min_samples=10)
        # Feed 15 samples of value 10.0
        for _ in range(15):
            calib.update(regime="trend", atr_bps=10.0)

        th = calib.thresholds(
            regime="trend",
            default_floor_t0=3.0,
            default_floor_t1=5.0,
            default_floor_t2=8.0
        )
        self.assertEqual(th.n, 15)
        self.assertEqual(th.src, "calib_q10q20q30")
        # Since all inputs are 10.0, quantiles should be ~10.0
        self.assertAlmostEqual(th.floor_t0, 10.0, delta=0.5)
        self.assertAlmostEqual(th.floor_t1, 10.0, delta=0.5)
        self.assertAlmostEqual(th.floor_t2, 10.0, delta=0.5)

    def test_monotonicity(self):
        calib = ATRBpsCalibrator(min_samples=5)
        # Feed scattered values
        vals = [5.0, 10.0, 15.0, 20.0, 25.0, 30.0]
        for v in vals:
            calib.update(regime="trend", atr_bps=v)

        th = calib.thresholds(
            regime="trend",
            default_floor_t0=0,
            default_floor_t1=0,
            default_floor_t2=0
        )
        # t0 <= t1 <= t2
        self.assertTrue(th.floor_t0 <= th.floor_t1)
        self.assertTrue(th.floor_t1 <= th.floor_t2)

    def test_persistence(self):
        c1 = ATRBpsCalibrator(min_samples=10)
        c1.update(regime="range", atr_bps=50.0)
        ts = 123456789

        state = c1.dump_regime_state(symbol="BTC", regime="range", updated_ts_ms=ts)
        self.assertEqual(state["symbol"], "BTC")
        self.assertEqual(state["regime"], "range")
        self.assertEqual(state["n"], 1)

        c2 = ATRBpsCalibrator(min_samples=10)
        c2.load_regime_state(state)
        th = c2.thresholds(
            regime="range",
            default_floor_t0=0,
            default_floor_t1=0,
            default_floor_t2=0
        )
        self.assertEqual(th.n, 1)

if __name__ == '__main__':
    unittest.main()
