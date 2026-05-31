"""Tests for the realized drift tracker (world-practice adverse selection v1) - tick_flow_full."""
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
