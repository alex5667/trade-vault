import unittest
import sys
import os


# Ensure repo root is on sys.path for imports when running via `python -m unittest`
# tests/ -> orderflow/ -> services/ -> python-worker/
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


from services.orderflow.book_geometry import (
    calc_book_slope
    calc_depth_weighted_spread
    calc_cost_to_cross
)


class TestBookGeometryV1(unittest.TestCase):
    def test_slope_positive(self) -> None:
        """Both sides should produce positive slopes for normal book data."""
        mid = 100.0
        bids = [(99.9, 10.0), (99.8, 5.0), (99.5, 2.0)]
        asks = [(100.1, 10.0), (100.2, 5.0), (100.5, 2.0)]
        sb, sa = calc_book_slope(bids, asks, mid=mid)
        self.assertGreater(sb, 0.0)
        self.assertGreater(sa, 0.0)

    def test_slope_zero_for_empty_book(self) -> None:
        """Empty book should return 0.0 for both sides."""
        sb, sa = calc_book_slope([], [], mid=100.0)
        self.assertEqual(sb, 0.0)
        self.assertEqual(sa, 0.0)

    def test_slope_zero_for_invalid_mid(self) -> None:
        """Zero mid price should return 0.0 slopes (fail-open)."""
        sb, sa = calc_book_slope([(99.9, 1.0)], [(100.1, 1.0)], mid=0.0)
        self.assertEqual(sb, 0.0)
        self.assertEqual(sa, 0.0)

    def test_dws_reasonable(self) -> None:
        """DWS should be a small positive number for a tight book."""
        mid = 100.0
        bids = [(99.99, 1.0), (99.98, 2.0), (99.95, 10.0)]
        asks = [(100.01, 1.0), (100.02, 2.0), (100.05, 10.0)]
        dws = calc_depth_weighted_spread(bids, asks, mid=mid, xbps=5.0)
        # Should be close to a few bps, not negative
        self.assertGreaterEqual(dws, 0.0)
        self.assertLess(dws, 50.0)

    def test_dws_zero_for_empty_book(self) -> None:
        """Empty book → no VWAP → return 0.0."""
        dws = calc_depth_weighted_spread([], [], mid=100.0, xbps=5.0)
        self.assertEqual(dws, 0.0)

    def test_dws_bounded_to_10000(self) -> None:
        """Extremely bad books should be capped at 10000 bps."""
        bids = [(50.0, 1.0)]  # bid at 50% below mid
        asks = [(150.0, 1.0)]  # ask at 50% above mid
        dws = calc_depth_weighted_spread(bids, asks, mid=100.0, xbps=10_000.0)
        self.assertLessEqual(dws, 10_000.0)

    def test_notional_within_band(self) -> None:
        """Notional within 1bp should include only the near level."""
        mid = 100.0
        # 99.99 is within 1bp of 100.0 (1bp = 0.01), 99.80 is outside
        bids = [(99.99, 1.0), (99.80, 1.0)]
        n1 = calc_cost_to_cross(bids, mid=mid, xbps=1.0)
        # Only the 99.99 level should count: 99.99 * 1.0
        self.assertAlmostEqual(n1, 99.99 * 1.0, places=6)

    def test_notional_within_5bp_includes_both(self) -> None:
        """Notional within 5bp should include both near levels."""
        mid = 100.0
        bids = [(99.99, 1.0), (99.96, 2.0)]  # Both within 5bp
        n5 = calc_cost_to_cross(bids, mid=mid, xbps=5.0)
        expected = 99.99 * 1.0 + 99.96 * 2.0
        self.assertAlmostEqual(n5, expected, places=4)

    def test_notional_zero_for_empty(self) -> None:
        """Empty levels should return 0.0."""
        n = calc_cost_to_cross([], mid=100.0, xbps=5.0)
        self.assertEqual(n, 0.0)

    def test_notional_zero_for_zero_mid(self) -> None:
        """Zero mid should return 0.0 (fail-open)."""
        n = calc_cost_to_cross([(99.9, 1.0)], mid=0.0, xbps=1.0)
        self.assertEqual(n, 0.0)


if __name__ == "__main__":
    unittest.main()
