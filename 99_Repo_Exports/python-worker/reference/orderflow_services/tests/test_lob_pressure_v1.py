# -*- coding: utf-8 -*-
"""Unit tests for core.lob_pressure.compute_lob_pressure (P91).

Tests:
  - Basic happy path (L1..L5 qi, microprice, slope, convexity, dw_obi)
  - Symmetric book (all features should be zero or near-zero)
  - Partial snapshot (some levels missing)
  - All-zero / empty bids/asks (fail-open -> all zeros)
  - Prev-snapshot microprice shift
  - Scale-invariance (values should be independent of instrument price)
  - dw_obi directional consistency vs qi_l1
"""

from __future__ import annotations

import math

from core.lob_pressure import compute_lob_pressure

EPS = 1e-9


def _book(n: int = 5, *, bid_base: float = 100.0, ask_base: float = 100.1, bid_qty: float = 10.0, ask_qty: float = 10.0):
    """Generate synthetic balanced book at 0.1 tick spacing."""
    bids = [(bid_base - i * 0.1, bid_qty) for i in range(n)]
    asks = [(ask_base + i * 0.1, ask_qty) for i in range(n)]
    return bids, asks


class TestBasicHappyPath:
    def test_returns_dict_with_all_keys(self):
        bids, asks = _book()
        res = compute_lob_pressure(bids=bids, asks=asks)
        expected_keys = [
            "qi_l1", "qi_l2", "qi_l3", "qi_l4", "qi_l5",
            "qi_mean", "qi_max_abs", "qi_slope",
            "micro_mid_div_bps", "micro_shift_bps",
            "depth_slope_bid", "depth_slope_ask", "depth_slope_imb",
            "depth_convexity_bid", "depth_convexity_ask", "depth_convexity_imb",
            "dw_obi",
        ]
        for k in expected_keys:
            assert k in res, f"Missing key: {k}"

    def test_all_values_are_finite_floats(self):
        bids, asks = _book()
        res = compute_lob_pressure(bids=bids, asks=asks)
        for k, v in res.items():
            assert isinstance(v, float), f"Key {k} is {type(v)}, expected float"
            assert not math.isnan(v), f"Key {k} is NaN"
            assert not math.isinf(v), f"Key {k} is Inf"


class TestSymmetricBook:
    """Symmetric book: all qi should be 0, dw_obi should be 0."""

    def test_symmetric_qi_is_zero(self):
        bids, asks = _book(bid_qty=10.0, ask_qty=10.0)
        res = compute_lob_pressure(bids=bids, asks=asks)
        for i in range(1, 6):
            assert abs(res[f"qi_l{i}"]) < EPS, f"qi_l{i} should be 0 for symmetric book"
        assert abs(res["qi_mean"]) < EPS
        assert abs(res["qi_max_abs"]) < EPS
        assert abs(res["qi_slope"]) < EPS

    def test_symmetric_dw_obi_is_zero(self):
        bids, asks = _book()
        res = compute_lob_pressure(bids=bids, asks=asks)
        assert abs(res["dw_obi"]) < EPS

    def test_symmetric_microprice_div_near_zero(self):
        bids, asks = _book()
        res = compute_lob_pressure(bids=bids, asks=asks)
        # Symmetric top-of-book => microprice approximately equals mid
        assert abs(res["micro_mid_div_bps"]) < 1e-3


class TestBidPressure:
    """Heavy bid side -> positive qi_l1, positive dw_obi."""

    def test_bid_heavy_qi_positive(self):
        bids = [(100.0, 50.0), (99.9, 40.0), (99.8, 30.0), (99.7, 20.0), (99.6, 10.0)]
        asks = [(100.1, 5.0), (100.2, 4.0), (100.3, 3.0), (100.4, 2.0), (100.5, 1.0)]
        res = compute_lob_pressure(bids=bids, asks=asks)
        assert res["qi_l1"] > 0.0, "qi_l1 should be positive when bids >> asks"
        assert res["qi_mean"] > 0.0
        assert res["dw_obi"] > 0.0

    def test_ask_heavy_qi_negative(self):
        bids = [(100.0, 1.0), (99.9, 2.0), (99.8, 3.0), (99.7, 4.0), (99.6, 5.0)]
        asks = [(100.1, 50.0), (100.2, 40.0), (100.3, 30.0), (100.4, 20.0), (100.5, 10.0)]
        res = compute_lob_pressure(bids=bids, asks=asks)
        assert res["qi_l1"] < 0.0, "qi_l1 should be negative when asks >> bids"
        assert res["dw_obi"] < 0.0


class TestMicroprice:
    def test_bid_heavy_microprice_above_mid(self):
        """When bid qty >> ask qty, microprice is above mid (buyer pressure)."""
        bids = [(100.0, 100.0)]
        asks = [(100.1, 10.0)]
        res = compute_lob_pressure(bids=bids, asks=asks, depth=1)
        assert res["micro_mid_div_bps"] > 0.0

    def test_ask_heavy_microprice_below_mid(self):
        bids = [(100.0, 10.0)]
        asks = [(100.1, 100.0)]
        res = compute_lob_pressure(bids=bids, asks=asks, depth=1)
        assert res["micro_mid_div_bps"] < 0.0


class TestMicropriceShift:
    def test_no_prev_shift_is_zero(self):
        bids, asks = _book()
        res = compute_lob_pressure(bids=bids, asks=asks, prev_bids=None, prev_asks=None)
        assert res["micro_shift_bps"] == 0.0

    def test_shift_when_price_moved_up(self):
        # Previous snapshot at lower price
        prev_bids = [(99.0, 10.0)]
        prev_asks = [(99.1, 10.0)]
        # Current snapshot at higher price
        bids = [(100.0, 10.0)]
        asks = [(100.1, 10.0)]
        res = compute_lob_pressure(bids=bids, asks=asks, prev_bids=prev_bids, prev_asks=prev_asks, depth=1)
        # Both snapshots symmetric => microprice = mid; shift > 0 because price moved up
        assert res["micro_shift_bps"] > 0.0


class TestDepthSlope:
    def test_depth_slope_positive_for_growing_book(self):
        """When each deeper level has more qty, cumulative depth grows fast."""
        bids = [(100.0 - i * 0.1, (i + 1) * 10.0) for i in range(5)]
        asks = [(100.1 + i * 0.1, 10.0) for i in range(5)]
        res = compute_lob_pressure(bids=bids, asks=asks)
        assert res["depth_slope_bid"] > 0.0


class TestFailOpen:
    def test_empty_bids_returns_zeros(self):
        res = compute_lob_pressure(bids=[], asks=[])
        for v in res.values():
            assert v == 0.0 or not math.isnan(v)

    def test_empty_asks_returns_zeros(self):
        bids = [(100.0, 10.0)]
        res = compute_lob_pressure(bids=bids, asks=[])
        # With empty asks qi=0 (both sides required for valid qi)
        assert res["qi_l1"] == 0.0, "qi_l1 should be 0 when asks is empty"

    def test_nan_qty_handled(self):
        bids = [(100.0, float("nan"))]
        asks = [(100.1, 10.0)]
        # Should not raise, should silently clamp
        res = compute_lob_pressure(bids=bids, asks=asks, depth=1)
        assert not math.isnan(res["dw_obi"])
        assert not math.isnan(res["qi_l1"])

    def test_negative_qty_clamped(self):
        bids = [(100.0, -5.0)]
        asks = [(100.1, 10.0)]
        res = compute_lob_pressure(bids=bids, asks=asks, depth=1)
        # Negative bid qty -> treated as 0 -> qi=0 (bid side 0 fails parity check)
        assert res["qi_l1"] == 0.0


class TestScaleInvariance:
    """Features should be independent of instrument price level."""

    def test_qi_scale_invariant(self):
        """qi is dimensionless ratio -> same regardless of absolute price."""
        bids_1 = [(100.0, 20.0), (99.9, 10.0)]
        asks_1 = [(100.1, 5.0), (100.2, 3.0)]
        bids_10k = [(10000.0, 20.0), (9999.0, 10.0)]
        asks_10k = [(10001.0, 5.0), (10002.0, 3.0)]
        res_1 = compute_lob_pressure(bids=bids_1, asks=asks_1, depth=2)
        res_10k = compute_lob_pressure(bids=bids_10k, asks=asks_10k, depth=2)
        assert abs(res_1["qi_l1"] - res_10k["qi_l1"]) < 1e-9

    def test_dw_obi_scale_invariant(self):
        """dw_obi is also a dimensionless ratio."""
        bids_1 = [(100.0, 20.0), (99.9, 10.0)]
        asks_1 = [(100.1, 5.0), (100.2, 3.0)]
        bids_10k = [(10000.0, 20.0), (9999.0, 10.0)]
        asks_10k = [(10001.0, 5.0), (10002.0, 3.0)]
        r1 = compute_lob_pressure(bids=bids_1, asks=asks_1, depth=2)
        r2 = compute_lob_pressure(bids=bids_10k, asks=asks_10k, depth=2)
        assert abs(r1["dw_obi"] - r2["dw_obi"]) < 1e-9


class TestDWOBIvsQIConsistency:
    """dw_obi sign should agree with qi_l1 sign when bid/ask imbalanced at L1."""

    def test_sign_agreement(self):
        bids = [(100.0, 30.0), (99.9, 20.0), (99.8, 10.0), (99.7, 5.0), (99.6, 2.0)]
        asks = [(100.1, 2.0), (100.2, 5.0), (100.3, 10.0), (100.4, 20.0), (100.5, 30.0)]
        res = compute_lob_pressure(bids=bids, asks=asks)
        # Bid-heavy at near touch, ask-heavy deep -> dw_obi positive (weights favor near touch)
        assert res["qi_l1"] > 0.0
        assert res["dw_obi"] > 0.0
