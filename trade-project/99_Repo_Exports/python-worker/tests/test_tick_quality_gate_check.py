import unittest

from tools.tick_quality_gate_check import (
    Histogram,
    histogram_quantile,
    histogram_window_delta,
)


class TestTickQualityGateCheck(unittest.TestCase):
    def test_histogram_window_delta_and_quantile(self):
        # cumulative buckets
        h1 = Histogram(buckets={1.0: 10, 2.0: 20, float('inf'): 20}, count=20)
        h2 = Histogram(buckets={1.0: 30, 2.0: 80, float('inf'): 100}, count=100)
        hd = histogram_window_delta(h1, h2)
        self.assertEqual(hd.count, 80)
        # delta buckets should be non-negative and cumulative
        self.assertEqual(hd.buckets[1.0], 20)
        self.assertEqual(hd.buckets[2.0], 60)
        self.assertEqual(hd.buckets[float('inf')], 80)
        # 50% of 80 is 40 -> should fall into 2.0 bucket (cum=60)
        self.assertEqual(histogram_quantile(0.5, hd), 2.0)


if __name__ == '__main__':
    unittest.main()
