"""Tests for new CVD features in sweep_detector_v2:
  - compute_cvd_median_abs_delta_usd
  - compute_cvd_divergence_from_price
"""
from __future__ import annotations

import math
import sys

import pytest

sys.path.insert(0, "/home/alex/front/trade/scanner_infra/python-worker")

from services.sweep_detector_v2 import (
    compute_cvd_divergence_from_price,
    compute_cvd_median_abs_delta_usd,
)


def _tick(ts_ms: int, price: float, qty: float, side: str) -> tuple:
    sign = 1.0 if side == "buy" else -1.0
    return (ts_ms, price, qty, sign * qty)


# ── compute_cvd_median_abs_delta_usd ─────────────────────────────────────────


def test_cvd_mad_empty():
    assert compute_cvd_median_abs_delta_usd([]) == 0.0


def test_cvd_mad_single_tick():
    # |signed_qty * price| = 2.0 * 100.0 = 200.0
    ticks = [_tick(0, 100.0, 2.0, "buy")]
    assert compute_cvd_median_abs_delta_usd(ticks) == pytest.approx(200.0)


def test_cvd_mad_odd_count():
    # deltas: 100, 200, 300  → sorted → median = 200
    ticks = [
        _tick(0, 100.0, 1.0, "buy"),   # 100
        _tick(1, 100.0, 2.0, "sell"),  # 200
        _tick(2, 100.0, 3.0, "buy"),   # 300
    ]
    assert compute_cvd_median_abs_delta_usd(ticks) == pytest.approx(200.0)


def test_cvd_mad_even_count():
    # deltas: 100, 200, 300, 400 → median = (200+300)/2 = 250
    ticks = [
        _tick(0, 100.0, 1.0, "buy"),
        _tick(1, 100.0, 2.0, "buy"),
        _tick(2, 100.0, 3.0, "sell"),
        _tick(3, 100.0, 4.0, "sell"),
    ]
    assert compute_cvd_median_abs_delta_usd(ticks) == pytest.approx(250.0)


def test_cvd_mad_sign_ignored():
    # |signed_qty| same whether buy or sell — median depends only on abs
    ticks_buy = [_tick(i, 1.0, float(i + 1), "buy") for i in range(5)]
    ticks_sell = [_tick(i, 1.0, float(i + 1), "sell") for i in range(5)]
    assert compute_cvd_median_abs_delta_usd(ticks_buy) == compute_cvd_median_abs_delta_usd(ticks_sell)


# ── compute_cvd_divergence_from_price ────────────────────────────────────────


def _make_ticks(n: int, price_start: float, price_end: float, net_side: str) -> list:
    """Build n ticks with linear price drift and uniform CVD direction."""
    ticks = []
    for i in range(n):
        frac = i / max(n - 1, 1)
        price = price_start + frac * (price_end - price_start)
        ticks.append(_tick(i * 100, price, 1.0, net_side))
    return ticks


def test_divergence_too_few_ticks():
    ticks = [_tick(i, 100.0, 1.0, "buy") for i in range(9)]
    assert compute_cvd_divergence_from_price(ticks) == 0.0


def test_divergence_zero_flow():
    # Half buy half sell → net CVD = 0 → total_abs_flow check
    ticks = []
    for i in range(10):
        side = "buy" if i % 2 == 0 else "sell"
        ticks.append(_tick(i, 100.0, 0.0, side))  # qty=0 → zero flow
    assert compute_cvd_divergence_from_price(ticks) == 0.0


def test_divergence_bullish():
    # CVD positive (buying) + price falling → bullish divergence → result > 0
    ticks = _make_ticks(20, price_start=100.0, price_end=99.0, net_side="buy")
    result = compute_cvd_divergence_from_price(ticks)
    assert result > 0.0, f"expected bullish divergence > 0, got {result}"


def test_divergence_bearish():
    # CVD negative (selling) + price rising → bearish divergence → result < 0
    ticks = _make_ticks(20, price_start=100.0, price_end=101.0, net_side="sell")
    result = compute_cvd_divergence_from_price(ticks)
    assert result < 0.0, f"expected bearish divergence < 0, got {result}"


def test_divergence_aligned_no_divergence():
    # CVD positive + price rising → aligned → result near 0
    ticks = _make_ticks(20, price_start=100.0, price_end=101.0, net_side="buy")
    result = compute_cvd_divergence_from_price(ticks)
    # Both pointing up → no divergence (result slightly < 0 due to normalisation)
    assert result < 0.3, f"aligned move should show low divergence, got {result}"


def test_divergence_bounded():
    # Result must always be in [-1, 1]
    for side in ("buy", "sell"):
        for p_end in (90.0, 100.0, 110.0):
            ticks = _make_ticks(20, 100.0, p_end, side)
            r = compute_cvd_divergence_from_price(ticks)
            assert -1.0 <= r <= 1.0, f"out of bounds: {r}"
