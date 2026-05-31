from core.obi_stability_tracker import OBIStabilityTracker


def test_obi_stable_long_high_score():
    tr = OBIStabilityTracker(window_ms=3000, threshold=0.4, deadband=0.05, grace_ms=250)
    ts = 0
    score = secs = 0.0
    for _ in range(12):
        score, secs = tr.update(ts_ms=ts, obi=0.6)
        ts += 300
    assert score > 0.85
    assert secs >= 2.0
