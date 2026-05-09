from __future__ import annotations

from core.burst_gate import BurstCandidate, BurstCandidateSelector


def test_burst_selects_best_and_flushes():
    b = BurstCandidateSelector(window_ms=1000, max_age_ms=5000)
    # start burst
    b.consider(ts_ms=1000, cand=BurstCandidate(ts_ms=1000, score=0.5, payload={"x": 1}))
    # better candidate inside window
    b.consider(ts_ms=1500, cand=BurstCandidate(ts_ms=1500, score=0.9, payload={"x": 2}))
    # before deadline => no emit
    assert b.maybe_flush(now_ts_ms=1999) is None
    # at/after deadline => emit best
    out = b.maybe_flush(now_ts_ms=2000)
    assert out is not None
    assert out["x"] == 2
    assert out["burst_emitted_at"] == 2000
