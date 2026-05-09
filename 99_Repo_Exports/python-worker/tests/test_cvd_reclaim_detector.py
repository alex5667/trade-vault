"""Unit tests for CVD Reclaim detector.

Tests cover:
- LONG bias with positive CVD support
- SHORT bias with negative CVD support
- Insufficient data (too few bars)
- Baseline calculation robustness
"""

from core.cvd_reclaim_detector import CVDReclaimDetector, CVDReclaimTracker


def test_cvd_reclaim_long_ok():
    """Test LONG reclaim with strong positive CVD support."""
    tr = CVDReclaimTracker(maxlen=1000)
    ts = 1_000_000

    # baseline noise ~1.0
    for _ in range(200):
        tr.push(ts_ms=ts, delta=1.0)
        ts += 1000

    det = CVDReclaimDetector(ratio_min=1.0, lookback_n=120, exclude_first_bar=True)

    sweep_ts = ts
    # exclude_first_bar=True -> sweep bar ignored
    tr.push(ts_ms=ts, delta=50.0)   # sweep bar (ignored)
    ts += 1000

    # reclaim window bars
    for _ in range(6):
        tr.push(ts_ms=ts, delta=10.0)
        ts += 1000
    reclaim_ts = ts - 1000

    res = det.evaluate(tracker=tr, bias="LONG", ts_from=sweep_ts, ts_to=reclaim_ts)
    assert res.ok is True, "Expected CVD reclaim confirmation for LONG"
    assert res.ratio > 1.0, f"Expected ratio > 1.0, got {res.ratio}"
    assert res.n >= 2, f"Expected at least 2 bars, got {res.n}"
    assert res.cvd_delta > 0, "Expected positive CVD delta for LONG"


def test_cvd_reclaim_short_ok():
    """Test SHORT reclaim with strong negative CVD support."""
    tr = CVDReclaimTracker(maxlen=1000)
    ts = 2_000_000
    for _ in range(200):
        tr.push(ts_ms=ts, delta=1.0)
        ts += 1000

    det = CVDReclaimDetector(ratio_min=1.0, lookback_n=120, exclude_first_bar=True)

    sweep_ts = ts
    tr.push(ts_ms=ts, delta=50.0)  # ignored
    ts += 1000

    for _ in range(6):
        tr.push(ts_ms=ts, delta=-10.0)
        ts += 1000
    reclaim_ts = ts - 1000

    res = det.evaluate(tracker=tr, bias="SHORT", ts_from=sweep_ts, ts_to=reclaim_ts)
    assert res.ok is True, "Expected CVD reclaim confirmation for SHORT"
    assert res.ratio > 1.0, f"Expected ratio > 1.0, got {res.ratio}"
    assert res.cvd_delta < 0, "Expected negative CVD delta for SHORT"


def test_cvd_reclaim_too_few_points():
    """Test that detector rejects windows with insufficient data."""
    tr = CVDReclaimTracker(maxlen=1000)
    det = CVDReclaimDetector(ratio_min=1.0, lookback_n=120, exclude_first_bar=True)
    res = det.evaluate(tracker=tr, bias="LONG", ts_from=1000, ts_to=2000)
    assert res.ok is False, "Expected rejection with empty tracker"
    assert res.n == 0, f"Expected 0 bars, got {res.n}"


def test_cvd_reclaim_weak_signal():
    """Test that weak CVD signal is correctly rejected."""
    tr = CVDReclaimTracker(maxlen=1000)
    ts = 3_000_000

    # baseline ~5.0
    for _ in range(200):
        tr.push(ts_ms=ts, delta=5.0)
        ts += 1000

    det = CVDReclaimDetector(ratio_min=2.0, lookback_n=120, exclude_first_bar=True)

    sweep_ts = ts
    tr.push(ts_ms=ts, delta=10.0)  # ignored
    ts += 1000

    # weak reclaim window (only 3 bars with small delta)
    for _ in range(3):
        tr.push(ts_ms=ts, delta=2.0)
        ts += 1000
    reclaim_ts = ts - 1000

    res = det.evaluate(tracker=tr, bias="LONG", ts_from=sweep_ts, ts_to=reclaim_ts)
    # ratio = (2.0 * 3) / (5.0 * sqrt(3)) ≈ 0.69 < 2.0
    assert res.ok is False, "Expected rejection for weak CVD signal"
    assert res.ratio < 2.0, f"Expected ratio < 2.0, got {res.ratio}"


def test_cvd_reclaim_baseline_robustness():
    """Test that median baseline is robust to outliers."""
    tr = CVDReclaimTracker(maxlen=1000)
    ts = 4_000_000

    # mostly small deltas with occasional outliers
    for i in range(200):
        delta = 1.0 if i % 10 != 0 else 100.0  # 10% outliers
        tr.push(ts_ms=ts, delta=delta)
        ts += 1000

    baseline = tr.median_abs_delta(120)
    # median should be ~1.0, not affected by 100.0 outliers
    assert 0.5 < baseline < 2.0, f"Expected robust baseline ~1.0, got {baseline}"


def test_cvd_reclaim_monotonic_time():
    """Test that non-monotonic timestamps are rejected."""
    tr = CVDReclaimTracker(maxlen=100)

    tr.push(ts_ms=1000, delta=1.0)
    tr.push(ts_ms=2000, delta=2.0)
    tr.push(ts_ms=1500, delta=3.0)  # bad time, should be ignored
    tr.push(ts_ms=3000, delta=4.0)

    # should only have 3 points (1000, 2000, 3000)
    assert len(tr.buf) == 3, f"Expected 3 points, got {len(tr.buf)}"
    assert tr.last_ts == 3000, f"Expected last_ts=3000, got {tr.last_ts}"
