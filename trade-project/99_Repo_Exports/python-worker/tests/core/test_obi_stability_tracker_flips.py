from core.obi_stability_tracker import OBIStabilityTracker


def test_obi_flips_low_score():
    tr = OBIStabilityTracker(window_ms=3000, threshold=0.4, deadband=0.05, grace_ms=250)
    ts = 0
    score = secs = 0.0
    for i in range(20):
        obi = 0.6 if (i % 2) == 0 else -0.6
        score, secs = tr.update(ts_ms=ts, obi=obi)
        ts += 150
    assert score < 0.75
    assert secs < 1.0
