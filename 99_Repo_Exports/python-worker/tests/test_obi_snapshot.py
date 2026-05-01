from __future__ import annotations
"""Tests for OBI snapshot and p_cluster normalization."""

from core.crypto_orderflow_detectors import OBIDetector


def _make_book(ts_ms: int, bid_vol: float, ask_vol: float, depth: int = 3) -> dict:
    """Helper: create a book with uniform levels."""
    per_bid = bid_vol / depth
    per_ask = ask_vol / depth
    bids = [[100 - i, per_bid] for i in range(depth)]
    asks = [[101 + i, per_ask] for i in range(depth)]
    return {"ts_ms": ts_ms, "bids": bids, "asks": asks}


def test_snapshot_always_available_after_push():
    """snapshot() returns raw OBI even when push() returns None (no stable event)."""
    det = OBIDetector(depth=3, threshold=0.4, hold_secs=2.0)
    book = _make_book(1000, bid_vol=18, ask_vol=3)  # OBI = (18-3)/21 ≈ 0.71

    ev = det.push(book)
    assert ev is None  # first push, not stable yet

    snap = det.snapshot()
    assert snap is not None
    assert abs(snap["obi"] - (18 - 3) / (18 + 3)) < 1e-6
    assert snap["direction"] == "long"
    assert snap["above_threshold"] is True
    assert snap["ts_ms"] == 1000
    assert "obi_z" in snap


def test_snapshot_updates_on_every_push():
    """snapshot() tracks latest OBI even with rapid direction flips."""
    det = OBIDetector(depth=3, threshold=0.4, hold_secs=2.0)

    # Push bid-heavy book
    book1 = _make_book(1000, bid_vol=18, ask_vol=3)
    det.push(book1)
    snap1 = det.snapshot()
    assert snap1["obi"] > 0
    assert snap1["direction"] == "long"

    # Flip to ask-heavy book (direction change resets timer)
    book2 = _make_book(1500, bid_vol=3, ask_vol=18)
    det.push(book2)
    snap2 = det.snapshot()
    assert snap2["obi"] < 0
    assert snap2["direction"] == "short"
    assert snap2["ts_ms"] == 1500


def test_snapshot_below_threshold():
    """snapshot tracks OBI even below threshold."""
    det = OBIDetector(depth=3, threshold=0.4, hold_secs=2.0)
    book = _make_book(1000, bid_vol=11, ask_vol=10)  # OBI = 1/21 ≈ 0.047

    ev = det.push(book)
    assert ev is None

    snap = det.snapshot()
    assert snap is not None
    assert snap["above_threshold"] is False
    assert abs(snap["obi"]) < 0.4  # below threshold


def test_snapshot_preserves_backward_compat():
    """Original event-based flow still works (push returns dict after hold_secs)."""
    det = OBIDetector(depth=3, threshold=0.2, hold_secs=1.0, z_alpha=0.5)
    book0 = _make_book(1000, bid_vol=18, ask_vol=3)
    book1 = _make_book(2000, bid_vol=18, ask_vol=3)

    ev0 = det.push(book0)
    assert ev0 is None

    ev1 = det.push(book1)
    assert ev1 is not None
    assert ev1["stable_secs"] >= 1.0

    # snapshot should also be available and match
    snap = det.snapshot()
    assert snap is not None
    assert abs(snap["obi"] - ev1["obi"]) < 1e-6


def test_p_cluster_normalization_long():
    """p_cluster for LONG signal: positive OBI → high p_cluster."""
    from services.orderflow.signal_pipeline import SignalPipeline
    pipe = SignalPipeline()

    indicators = {"obi": 0.7, "direction": "LONG"}
    mix = pipe._build_mix_dict(delta=100.0, delta_z=3.0, indicators=indicators, confirmations=[])
    assert 0.0 <= mix["p_cluster"] <= 1.0
    assert mix["p_cluster"] == 0.7  # positive OBI confirms LONG


def test_p_cluster_normalization_short():
    """p_cluster for SHORT signal: negative OBI → high p_cluster."""
    from services.orderflow.signal_pipeline import SignalPipeline
    pipe = SignalPipeline()

    indicators = {"obi": -0.5, "direction": "SHORT"}
    mix = pipe._build_mix_dict(delta=100.0, delta_z=3.0, indicators=indicators, confirmations=[])
    assert 0.0 <= mix["p_cluster"] <= 1.0
    assert mix["p_cluster"] == 0.5  # negative OBI confirms SHORT


def test_p_cluster_opposing_direction():
    """p_cluster should be 0 when OBI opposes signal direction."""
    from services.orderflow.signal_pipeline import SignalPipeline
    pipe = SignalPipeline()

    indicators = {"obi": -0.3, "direction": "LONG"}  # negative OBI opposes LONG
    mix = pipe._build_mix_dict(delta=100.0, delta_z=3.0, indicators=indicators, confirmations=[])
    assert mix["p_cluster"] == 0.0  # clamped to 0
