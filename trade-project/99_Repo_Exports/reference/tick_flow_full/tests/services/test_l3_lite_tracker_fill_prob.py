# tick_flow_full/tests/services/test_l3_lite_tracker_fill_prob.py
# -*- coding: utf-8 -*-
"""
Tests for fill_prob enrichment in L3LiteTracker and L3LiteSnapshot.
Validates that fill_prob_bid / fill_prob_ask are computed and propagated correctly.
"""
import pytest
from tick_flow_full.services.l3_lite_tracker import L3LiteTracker, L3LiteSnapshot


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _warm_up(tracker: L3LiteTracker, n: int = 10, price_usd: float = 100.0) -> None:
    """Warm up tracker with synthetic trades and a series of book updates."""
    ts = 1_000
    for i in range(n):
        # Alternate taker-buy / taker-sell to generate non-zero rates
        tracker.on_trade(ts=ts, qty=float(price_usd), side=1)   # taker-buy
        tracker.on_trade(ts=ts, qty=float(price_usd * 0.8), side=-1)  # taker-sell
        ts += 10

    for i in range(n):
        tracker.on_book(ts=ts, depth_bid_5=float(price_usd * 5), depth_ask_5=float(price_usd * 5))
        ts += 100


# ──────────────────────────────────────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────────────────────────────────────

def test_l3_fill_prob_fields_in_snapshot():
    """L3LiteSnapshot must have fill_prob_bid and fill_prob_ask fields."""
    snap = L3LiteSnapshot()
    assert hasattr(snap, "fill_prob_bid")
    assert hasattr(snap, "fill_prob_ask")
    # Both default to 0.0
    assert snap.fill_prob_bid == 0.0
    assert snap.fill_prob_ask == 0.0


def test_l3_fill_prob_basic():
    """After warm-up, fill_prob_bid and fill_prob_ask must be in [0, 1]."""
    tracker = L3LiteTracker(alpha=0.2, min_dt_ms=1)
    _warm_up(tracker, n=12, price_usd=1000.0)

    assert 0.0 <= tracker.snap.fill_prob_bid <= 1.0, (
        f"fill_prob_bid out of range: {tracker.snap.fill_prob_bid}"
    )
    assert 0.0 <= tracker.snap.fill_prob_ask <= 1.0, (
        f"fill_prob_ask out of range: {tracker.snap.fill_prob_ask}"
    )


def test_l3_fill_prob_high_cancel_penalises():
    """Very high cancel-to-trade ratio should push fill_prob well below 1."""
    tracker = L3LiteTracker(alpha=0.9, min_dt_ms=1)
    ts = 1_000
    # Tiny taker-buy/sell → huge depth decrease → large implied cancel
    for i in range(20):
        tracker.on_trade(ts=ts, qty=0.001, side=1)
        tracker.on_trade(ts=ts, qty=0.001, side=-1)
        ts += 10
    for i in range(10):
        # Depth shrinks a lot between updates (large cancel implied)
        tracker.on_book(ts=ts, depth_bid_5=100_000.0, depth_ask_5=100_000.0)
        ts += 100
        tracker.on_book(ts=ts, depth_bid_5=1.0, depth_ask_5=1.0)
        ts += 100

    # cancel_to_trade should be very high by now; fill_prob should reflect that
    snap = tracker.snap
    assert 0.0 <= snap.fill_prob_bid <= 1.0
    assert 0.0 <= snap.fill_prob_ask <= 1.0
    # At least one side should be penalised to < 0.9 (usually much lower)
    assert (snap.fill_prob_bid < 0.9 or snap.fill_prob_ask < 0.9), (
        f"Expected penalised fill_prob, got bid={snap.fill_prob_bid:.3f} ask={snap.fill_prob_ask:.3f}"
    )


def test_l3_attach_to_context_includes_fill_prob():
    """attach_to_context must set fill_prob_bid and fill_prob_ask on the context object."""
    tracker = L3LiteTracker(alpha=0.2, min_dt_ms=1)
    _warm_up(tracker, n=10, price_usd=500.0)

    class _Ctx:
        pass

    ctx = _Ctx()
    tracker.attach_to_context(ctx)

    assert hasattr(ctx, "fill_prob_bid"), "fill_prob_bid missing from context after attach"
    assert hasattr(ctx, "fill_prob_ask"), "fill_prob_ask missing from context after attach"
    assert isinstance(ctx.fill_prob_bid, float)
    assert isinstance(ctx.fill_prob_ask, float)
    assert 0.0 <= ctx.fill_prob_bid <= 1.0
    assert 0.0 <= ctx.fill_prob_ask <= 1.0


def test_l3_disabled_tracker_returns_zero_fill_prob():
    """Disabled tracker must leave fill_prob at 0 (no computation)."""
    tracker = L3LiteTracker(enabled=False, alpha=0.2, min_dt_ms=1)
    tracker.on_trade(ts=1000, qty=10.0, side=1)
    tracker.on_book(ts=2000, depth_bid_5=1000.0, depth_ask_5=1000.0)
    assert tracker.snap.fill_prob_bid == 0.0
    assert tracker.snap.fill_prob_ask == 0.0
