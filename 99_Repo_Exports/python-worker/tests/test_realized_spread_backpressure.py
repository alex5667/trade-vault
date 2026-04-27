from __future__ import annotations

from microstructure.realized_spread_tracker import PendingTrade, RealizedSpreadTracker


class _M:
    def __init__(self) -> None:
        self.incs = {}
        self.gauges = {}

    def inc(self, k: str, v: int = 1) -> None:
        self.incs[k] = self.incs.get(k, 0) + int(v)

    def gauge(self, k: str, v: float) -> None:
        self.gauges[k] = float(v)


def test_pending_utilization_definition_len_minus_head_over_max():
    tr = RealizedSpreadTracker(max_pending=10)
    assert tr.pending_utilization() == 0.0
    for i in range(5):
        tr.append_pending(PendingTrade(ts_ms=1000 + i, side=1, price=100.0, qty=1.0))
    assert tr.pending_active() == 5
    assert abs(tr.pending_utilization() - 0.5) < 1e-9
    tr.pending_head = 2
    assert tr.pending_active() == 3
    assert abs(tr.pending_utilization() - 0.3) < 1e-9


def test_backpressure_pauses_and_drops_when_projected_util_exceeds_0_9():
    m = _M()
    tr = RealizedSpreadTracker(
        max_pending=10,
        pending_pause_high=0.90,
        pending_resume_low=0.80,
        metrics=m,
    )
    # Fill up to utilization == 0.9 (active == 9) -> still accepted
    for i in range(9):
        ok = tr.append_pending(PendingTrade(ts_ms=1000 + i, side=1, price=100.0, qty=1.0))
        assert ok is True
    assert tr.pending_active() == 9
    assert abs(tr.pending_utilization() - 0.9) < 1e-9

    # Next append would project to 1.0 (> 0.9) -> must be dropped, paused
    ok2 = tr.append_pending(PendingTrade(ts_ms=2000, side=1, price=100.0, qty=1.0))
    assert ok2 is False
    assert tr.pending_active() == 9
    assert m.incs.get("realized_spread.pending_drop.backpressure", 0) >= 1
    assert m.incs.get("realized_spread.pending_pause", 0) >= 1

    # Reduce active below resume_low and ensure append resumes
    tr.mark_pending_consumed(2)  # active -> 7, util -> 0.7
    ok3 = tr.append_pending(PendingTrade(ts_ms=2001, side=1, price=100.0, qty=1.0))
    assert ok3 is True
    assert tr.pending_active() == 8
    assert m.incs.get("realized_spread.pending_resume", 0) >= 1
