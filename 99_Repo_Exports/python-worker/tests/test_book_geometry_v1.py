import sys
from pathlib import Path
import unittest

# Make imports work regardless of how tests are executed.
# Repo layout: <repo>/python-worker/services/orderflow/...
PYWORKER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PYWORKER))

from services.orderflow.book_geometry import (
    calc_book_slope,
    calc_depth_weighted_spread,
    calc_cost_to_cross,
)


class TestBookGeometry(unittest.TestCase):
    def test_slope_and_dws(self):
        bids = [(99.9, 2.0), (99.8, 2.0), (99.5, 2.0)]
        asks = [(100.1, 2.0), (100.2, 2.0), (100.5, 2.0)]
        mid = 100.0
        sb, sa = calc_book_slope(bids, asks, mid)
        self.assertGreater(sb, 0.0)
        self.assertGreater(sa, 0.0)
        dws = calc_depth_weighted_spread(bids, asks, mid, xbps=50)
        self.assertGreater(dws, 0.0)

    def test_cost_to_cross(self):
        bids = [(99.99, 1.0), (99.0, 10.0)]
        mid = 100.0
        n1 = calc_cost_to_cross(bids, mid, xbps=1.0)
        self.assertGreater(n1, 0.0)
        n01 = calc_cost_to_cross(bids, mid, xbps=0.1)
        self.assertLessEqual(n01, n1)

    def test_fail_open_bad_inputs(self):
        """Ensure fail-open on bad/empty inputs."""
        sb, sa = calc_book_slope([], [], 0.0)
        self.assertEqual(sb, 0.0)
        self.assertEqual(sa, 0.0)
        dws = calc_depth_weighted_spread(None, None, 100.0)
        self.assertEqual(dws, 0.0)
        n = calc_cost_to_cross([], 100.0, xbps=5.0)
        self.assertEqual(n, 0.0)

    def test_slope_symmetric(self):
        """Symmetric book => bid slope ≈ ask slope."""
        bids = [(99.9, 5.0), (99.8, 5.0)]
        asks = [(100.1, 5.0), (100.2, 5.0)]
        mid = 100.0
        sb, sa = calc_book_slope(bids, asks, mid)
        self.assertAlmostEqual(sb, sa, delta=sb * 0.01)

    def test_dws_wider_than_spread_bps(self):
        """DWS should be >= raw BBO spread when inside band."""
        bids = [(99.0, 10.0)]
        asks = [(101.0, 10.0)]
        mid = 100.0
        raw_spread_bps = (101.0 - 99.0) / mid * 10_000.0  # 200.0 bps
        dws = calc_depth_weighted_spread(bids, asks, mid, xbps=200.0)
        self.assertAlmostEqual(dws, raw_spread_bps, delta=1.0)


if __name__ == '__main__':
    unittest.main()
