import unittest

from core.rate_meter import RollingRateMeter


class TestRollingRateMeter(unittest.TestCase):
    def test_rate_calculation(self):
        # 60s window
        meter = RollingRateMeter(window_ms=60000)

        # Add 10 events spaced by 1s starting from t=1000
        for i in range(10):
            meter.add(1000 + i*1000)

        # Check rate at t=11000 (all 10 in window)
        # 10 events in 60s window implies rate of 10 events/min?
        # WAIT: the formula is count / (window_size_in_min).
        # count=10, window=1min -> rate=10. Correct.
        self.assertAlmostEqual(meter.rate_per_min(11000), 10.0)
        self.assertEqual(meter.count(11000), 10)

    def test_pruning(self):
        meter = RollingRateMeter(window_ms=1000)
        meter.add(1000)
        meter.add(1500)
        meter.add(2500)

        # At 2500:
        # 1000 is outsie [1500, 2500] -> pruned
        # 1500 is inside -> kept
        # 2500 is inside -> kept
        self.assertEqual(meter.count(2500), 2)

        # Rate: 2 events in 1s window = 120 events/min
        self.assertAlmostEqual(meter.rate_per_min(2500), 120.0)

    def test_empty_window(self):
        meter = RollingRateMeter(window_ms=60000)
        self.assertEqual(meter.rate_per_min(1000), 0.0)

if __name__ == '__main__':
    unittest.main()
