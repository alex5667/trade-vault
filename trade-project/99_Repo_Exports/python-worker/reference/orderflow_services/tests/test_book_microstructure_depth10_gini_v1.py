from __future__ import annotations

"""Unit tests for core.book_microstructure_v4.compute_microstructure_v4 (A1).

Coverage (A1):
  - depth_total_10 / depth_imbalance_10 (top-10 depth aggregates)
  - gini_depth_10 (depth distribution inequality proxy)
  - micro_price (absolute) and micro_price_diff_bps (vs mid, bps)
  - Guards: bounded outputs, fail-open on partial snapshots and bad quantities
"""


# Allow running tests from repo root without PYTHONPATH tweaks.
# [AUTOGRAVITY CLEANUP] sys.path.insert(0, str(Path(__file__).resolve().parents[3]))  # .../tick_flow_full

import math

from core.book_microstructure_v4 import compute_microstructure_v4

EPS = 1e-9


def _book(n: int, *, bid_base: float = 100.0, ask_base: float = 100.1, bid_qty: float = 10.0, ask_qty: float = 10.0):
    """Generate a synthetic book snapshot compatible with compute_microstructure_v4."""
    bids = [(bid_base - i * 0.1, float(bid_qty)) for i in range(n)]
    asks = [(ask_base + i * 0.1, float(ask_qty)) for i in range(n)]
    return {"bids": bids, "asks": asks}


def test_depth10_balanced_and_microprice_zero_diff():
    snap = _book(10, bid_qty=10.0, ask_qty=10.0)
    out = compute_microstructure_v4(snap, prev_snap=None)

    # Presence
    for k in ("depth_total_10", "depth_imbalance_10", "gini_depth_10", "micro_price", "micro_price_diff_bps"):
        assert k in out, f"missing key: {k}"

    # Depth aggregates (10 levels each side)
    assert abs(out["depth_total_10"] - 200.0) < 1e-6
    assert abs(out["depth_imbalance_10"]) < 1e-9

    # Balanced book => perfectly equal depth distribution
    assert abs(out["gini_depth_10"]) < 1e-9

    # L1 qty balanced => micro_price == mid
    # best_bid=100.0, best_ask=100.1 => mid=100.05
    assert abs(out["micro_price"] - 100.05) < 1e-9
    assert abs(out["micro_price_diff_bps"]) < 1e-9
    assert abs(out["mp_mid_bps"]) < 1e-9  # legacy alias stays consistent
    assert abs(out["micro_price_diff_bps"] - out["mp_mid_bps"]) < 1e-9


def test_depth10_imbalance_microprice_sign_and_gini_positive():
    snap = _book(10, bid_qty=20.0, ask_qty=10.0)
    out = compute_microstructure_v4(snap, prev_snap=None)

    # Depth imbalance positive (more bid depth)
    assert out["depth_total_10"] > 0.0
    assert out["depth_imbalance_10"] > 0.0
    assert out["depth_imbalance_10"] <= 1.0 + EPS

    # With bid_qty > ask_qty microprice should shift toward ask (mp > mid) => positive diff bps
    assert out["micro_price_diff_bps"] > 0.0
    assert abs(out["micro_price_diff_bps"] - out["mp_mid_bps"]) < 1e-9

    # Distribution is not uniform (bid vs ask levels differ) => gini > 0 and bounded
    assert out["gini_depth_10"] > 0.0
    assert 0.0 - EPS <= out["gini_depth_10"] <= 1.0 + EPS


def test_partial_snapshot_less_than_10_levels_is_safe():
    snap = _book(3, bid_qty=5.0, ask_qty=5.0)
    out = compute_microstructure_v4(snap, prev_snap=None)

    # Only 3 levels each side -> totals reflect available depth
    assert abs(out["depth_total_10"] - 30.0) < 1e-6
    assert abs(out["depth_imbalance_10"]) < 1e-9
    assert abs(out["gini_depth_10"]) < 1e-9


def test_negative_qty_is_clamped_and_bounded():
    snap = {"bids": [(100.0, -10.0), (99.9, 5.0)], "asks": [(100.1, -7.0), (100.2, 3.0)]}
    out = compute_microstructure_v4(snap, prev_snap=None)

    # Negative qty should not make totals negative; features must be finite and bounded.
    assert out["depth_total_10"] >= 0.0
    assert -1.0 - EPS <= out["depth_imbalance_10"] <= 1.0 + EPS
    assert 0.0 - EPS <= out["gini_depth_10"] <= 1.0 + EPS
    assert math.isfinite(float(out["micro_price"]))
    assert math.isfinite(float(out["micro_price_diff_bps"]))
