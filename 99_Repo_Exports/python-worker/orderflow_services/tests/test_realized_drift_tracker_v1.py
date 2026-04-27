"""Tests for the realized drift tracker (world-practice adverse selection v1)."""
from services.orderflow.world_practice.realized_drift_tracker_v1 import RealizedDriftTrackerV1


def test_realized_drift_tracker_basic_long_favorable():
    tr = RealizedDriftTrackerV1(horizon_ms=1000, alpha=0.5, min_n=1, mean_th_bps=0.1, bad_share_th=0.5, z_th=0.1)
    tr.on_signal(ts_ms=0, direction="LONG", px0=100.0, bucket="NORMAL")
    processed = tr.update(ts_ms=999, px_now=101.0)
    assert processed == {}
    processed = tr.update(ts_ms=1000, px_now=101.0)
    assert processed.get("NORMAL") == 1
    snap = tr.snapshot("NORMAL")
    assert snap["adverse_rd_n"] >= 1
    assert snap["adverse_rd_mean_bps"] > 0
    assert snap["adverse_rd_veto"] == 0.0


def test_realized_drift_tracker_short_favorable():
    tr = RealizedDriftTrackerV1(horizon_ms=1, alpha=0.5, min_n=1, mean_th_bps=0.1, bad_share_th=0.5, z_th=0.1)
    tr.on_signal(ts_ms=0, direction="SHORT", px0=100.0, bucket="WIDE")
    processed = tr.update(ts_ms=1, px_now=99.0)
    assert processed.get("WIDE") == 1
    snap = tr.snapshot("WIDE")
    assert snap["adverse_rd_mean_bps"] > 0


def test_realized_drift_tracker_veto_triggers():
    tr = RealizedDriftTrackerV1(horizon_ms=1, alpha=1.0, min_n=3, mean_th_bps=0.1, bad_share_th=0.6, z_th=0.1)
    for i in range(3):
        tr.on_signal(ts_ms=i, direction="LONG", px0=100.0, bucket="STRESSED")
        tr.update(ts_ms=i + 1, px_now=99.0)  # adverse
    snap = tr.snapshot("STRESSED")
    assert snap["adverse_rd_n"] >= 3
    assert snap["adverse_rd_mean_bps"] < 0
    assert snap["adverse_rd_bad_share"] >= 0.6
    assert snap["adverse_rd_veto"] == 1.0


def test_realized_drift_tracker_no_processing_before_horizon():
    """Evaluations should not be processed before the horizon expires."""
    tr = RealizedDriftTrackerV1(horizon_ms=5000, alpha=0.5, min_n=1, mean_th_bps=0.1, bad_share_th=0.5, z_th=0.1)
    tr.on_signal(ts_ms=0, direction="LONG", px0=100.0, bucket="NORMAL")
    processed = tr.update(ts_ms=4999, px_now=110.0)
    assert processed == {}
    snap = tr.snapshot("NORMAL")
    assert snap["adverse_rd_n"] == 0.0


def test_realized_drift_tracker_invalid_direction_ignored():
    """Unknown direction should be silently ignored."""
    tr = RealizedDriftTrackerV1(horizon_ms=1, alpha=0.5, min_n=1, mean_th_bps=0.1, bad_share_th=0.5, z_th=0.1)
    tr.on_signal(ts_ms=0, direction="SIDEWAYS", px0=100.0, bucket="NORMAL")
    processed = tr.update(ts_ms=2, px_now=101.0)
    assert processed == {}


def test_realized_drift_tracker_max_pending_backpressure():
    """When pending queue overflows, oldest entries are dropped."""
    tr = RealizedDriftTrackerV1(horizon_ms=10000, alpha=0.5, min_n=1, mean_th_bps=0.1, bad_share_th=0.5, z_th=0.1, max_pending=5)
    for i in range(10):
        tr.on_signal(ts_ms=i, direction="LONG", px0=100.0, bucket="NORMAL")
    assert len(tr._pending) <= 5


def test_realized_drift_tracker_default_snapshot_before_any_eval():
    """Before any evaluation, snapshot should return safe defaults."""
    tr = RealizedDriftTrackerV1()
    snap = tr.snapshot("NORMAL")
    assert snap["adverse_rd_veto"] == 0.0
    assert snap["adverse_rd_n"] == 0.0
    assert snap["adverse_rd_sigma_bps"] > 0.0  # sigma_floor
