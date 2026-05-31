"""Plan 3 / Step 3 — drift_auto_demote service unit tests."""
from __future__ import annotations

from orderflow_services.drift_auto_demote_v1 import (
    DriftMonitor,
    ExpectancyMonitor,
    compute_brier,
    compute_expectancy_top_pct,
    demote_model_mode,
    freeze_bucket,
    split_by_bucket,
    write_drift_state,
)


class _FakeRedis:
    def __init__(self):
        self.hashes: dict[str, dict] = {}
        self.expires: dict[str, int] = {}

    def hset(self, key, mapping):
        self.hashes.setdefault(key, {}).update(mapping)
        return len(mapping)

    def expire(self, key, ttl):
        self.expires[key] = ttl
        return True


# ─── pure helpers ────────────────────────────────────────────────────────────


def test_compute_brier_tp_high_p():
    assert compute_brier(0.9, label=1) == (0.9 - 1) ** 2


def test_compute_brier_sl_high_p():
    assert compute_brier(0.9, label=-1) == (0.9 - 0) ** 2


def test_compute_brier_timeout_zero_target():
    """label=0 (timeout) treated as loss."""
    assert compute_brier(0.6, label=0) == (0.6 - 0) ** 2


def test_compute_brier_returns_none_when_missing():
    assert compute_brier(None, label=1) is None
    assert compute_brier(0.5, label=None) is None


def test_split_by_bucket_groups_correctly():
    rows = [
        {"kind": "iceberg", "symbol": "BTCUSDT"},
        {"kind": "iceberg", "symbol": "BTCUSDT"},
        {"kind": "delta_spike", "symbol": "BTCUSDT"},
        {"kind": "iceberg", "symbol": "ETHUSDT"},
    ]
    out = split_by_bucket(rows)
    assert len(out[("iceberg", "BTCUSDT")]) == 2
    assert len(out[("delta_spike", "BTCUSDT")]) == 1
    assert len(out[("iceberg", "ETHUSDT")]) == 1


def test_split_by_bucket_handles_missing_fields():
    rows = [{"realized_r": 0.0}]
    out = split_by_bucket(rows)
    assert ("_default", "_unknown") in out


# ─── DriftMonitor behavior ───────────────────────────────────────────────────


def _row(ts_ms, realized_r=None, calib_prob=None, label=None, kind="iceberg", symbol="BTC"):
    return {
        "decision_time_ms": ts_ms,
        "realized_r": realized_r,
        "calib_prob": calib_prob,
        "label": label,
        "kind": kind,
        "symbol": symbol,
    }


def test_monitor_no_signal_on_stable_edge():
    m = DriftMonitor(min_n=20, edge_delta=0.05, edge_threshold=2.5,
                     brier_delta=0.005, brier_threshold=2.5)
    rows = [_row(ts_ms=i, realized_r=0.0) for i in range(1, 200)]
    out = m.process_bucket("iceberg", "BTC", rows)
    assert out["edge"]["signal"] is False


def test_monitor_signals_edge_drop_after_losing_streak():
    m = DriftMonitor(min_n=20, edge_delta=0.02, edge_threshold=2.0,
                     brier_delta=0.005, brier_threshold=2.5)
    stable = [_row(ts_ms=i, realized_r=0.0) for i in range(1, 100)]
    losing = [_row(ts_ms=i, realized_r=-1.0) for i in range(100, 400)]
    out = m.process_bucket("iceberg", "BTC", stable + losing)
    assert out["edge"]["signal"] is True


def test_monitor_advances_cursor_so_repeat_scans_dont_double_count():
    m = DriftMonitor(min_n=5, edge_delta=0.0, edge_threshold=0.001,
                     brier_delta=0.005, brier_threshold=2.5)
    rows1 = [_row(ts_ms=i, realized_r=0.0) for i in range(1, 11)]
    m.process_bucket("iceberg", "BTC", rows1)
    n_after_first = m._detectors[("iceberg", "BTC", "edge")].n()

    # Re-scan SAME rows — must NOT re-feed (timestamps not strictly > cursor)
    m.process_bucket("iceberg", "BTC", rows1)
    n_after_second = m._detectors[("iceberg", "BTC", "edge")].n()
    assert n_after_second == n_after_first


def test_monitor_brier_only_when_calib_prob_present():
    m = DriftMonitor(min_n=10, edge_delta=0.02, edge_threshold=2.0,
                     brier_delta=0.005, brier_threshold=2.5)
    rows = [_row(ts_ms=i, realized_r=0.0) for i in range(1, 50)]  # no calib_prob
    out = m.process_bucket("iceberg", "BTC", rows)
    assert "brier" not in out
    assert "edge" in out


def test_monitor_brier_present_when_calib_prob_set():
    m = DriftMonitor(min_n=10, edge_delta=0.02, edge_threshold=2.0,
                     brier_delta=0.005, brier_threshold=2.5)
    rows = [_row(ts_ms=i, realized_r=0.0, calib_prob=0.5, label=1) for i in range(1, 50)]
    out = m.process_bucket("iceberg", "BTC", rows)
    assert "brier" in out


# ─── Redis side effects ──────────────────────────────────────────────────────


def test_write_drift_state_hash_and_ttl():
    rc = _FakeRedis()
    write_drift_state(rc, kind="iceberg", symbol="BTC", metric="edge",
                      severity="critical", score=3.5, n=200, action="demote", now_ms=42)
    key = "drift:state:iceberg:BTC:edge"
    assert rc.hashes[key]["severity"] == "critical"
    assert rc.hashes[key]["score"] == "3.5000"
    assert rc.hashes[key]["action"] == "demote"
    assert rc.expires[key] == 7 * 24 * 3600


def test_demote_model_mode_writes_shadow():
    rc = _FakeRedis()
    ok = demote_model_mode(rc, kind="iceberg", reason="drift:edge")
    assert ok is True
    key = "cfg:ml_confirm:iceberg"
    assert rc.hashes[key]["mode"] == "SHADOW"
    assert rc.hashes[key]["auto_demoted"] == "1"
    assert rc.hashes[key]["auto_demote_reason"] == "drift:edge"


def test_freeze_bucket_writes_freeze_flag():
    rc = _FakeRedis()
    ok = freeze_bucket(rc, kind="iceberg", symbol="BTC", reason="drift:brier")
    assert ok is True
    key = "drift:freeze:iceberg:BTC"
    assert rc.hashes[key]["frozen"] == "1"
    assert rc.hashes[key]["reason"] == "drift:brier"
    assert rc.expires[key] == 24 * 3600


# ─── Plan 2 Gap 6: top-pct expectancy trigger ────────────────────────────────


def test_compute_expectancy_top_pct_basic():
    # 20 rows: top 5% = 1 row → highest calib_prob wins.
    rows = [
        {"calib_prob": 0.1 * i, "realized_r": -1.0 if i < 19 else 5.0}
        for i in range(1, 21)
    ]
    avg, n = compute_expectancy_top_pct(rows, top_pct=0.05, min_n=10)
    assert n == 20
    # Top 5% of 20 = 1 entry → realized_r=5.0
    assert avg == 5.0


def test_compute_expectancy_top_pct_takes_top_slice():
    # 100 rows ranked by calib_prob: top 5% (=5 rows) all return -2.0 → neg expectancy
    rows = [
        {"calib_prob": 0.01 * i,
         "realized_r": -2.0 if i >= 96 else 1.0}
        for i in range(1, 101)
    ]
    avg, n = compute_expectancy_top_pct(rows, top_pct=0.05, min_n=50)
    assert n == 100
    assert avg == -2.0


def test_compute_expectancy_top_pct_below_min_n_returns_none():
    rows = [{"calib_prob": 0.5, "realized_r": 1.0} for _ in range(10)]
    avg, n = compute_expectancy_top_pct(rows, top_pct=0.05, min_n=50)
    assert avg is None
    assert n == 10


def test_compute_expectancy_top_pct_drops_rows_missing_fields():
    rows = [
        {"calib_prob": 0.9, "realized_r": 1.0},
        {"calib_prob": None, "realized_r": 1.0},
        {"calib_prob": 0.8, "realized_r": None},
        {"calib_prob": 0.7, "realized_r": -2.0},
    ]
    avg, n = compute_expectancy_top_pct(rows, top_pct=1.0, min_n=2)
    # Only rows with both fields counted: 2 eligible → avg = (1.0 + -2.0)/2 = -0.5
    assert n == 2
    assert avg == -0.5


def test_compute_expectancy_top_pct_bad_top_pct():
    rows = [{"calib_prob": 0.5, "realized_r": 1.0}] * 100
    assert compute_expectancy_top_pct(rows, top_pct=0.0, min_n=1) == (None, 0)
    assert compute_expectancy_top_pct(rows, top_pct=1.5, min_n=1) == (None, 0)


def test_expectancy_monitor_does_not_fire_below_sustain():
    em = ExpectancyMonitor(top_pct=1.0, min_n=2, threshold=0.0, sustain_scans=3)
    rows = [{"calib_prob": 0.5, "realized_r": -1.0}] * 10
    out1 = em.evaluate("kind", "sym", rows)
    out2 = em.evaluate("kind", "sym", rows)
    assert out1["fired"] is False
    assert out2["fired"] is False
    assert out2["streak"] == 2


def test_expectancy_monitor_fires_after_sustain():
    em = ExpectancyMonitor(top_pct=1.0, min_n=2, threshold=0.0, sustain_scans=3)
    rows = [{"calib_prob": 0.5, "realized_r": -1.0}] * 10
    for _ in range(3):
        out = em.evaluate("kind", "sym", rows)
    assert out["fired"] is True
    assert out["streak"] == 3


def test_expectancy_monitor_positive_resets_streak():
    em = ExpectancyMonitor(top_pct=1.0, min_n=2, threshold=0.0, sustain_scans=3)
    neg = [{"calib_prob": 0.5, "realized_r": -1.0}] * 10
    pos = [{"calib_prob": 0.5, "realized_r": 1.0}] * 10
    em.evaluate("kind", "sym", neg)
    em.evaluate("kind", "sym", neg)
    out = em.evaluate("kind", "sym", pos)
    assert out["streak"] == 0
    assert out["fired"] is False


def test_expectancy_monitor_isolates_buckets():
    em = ExpectancyMonitor(top_pct=1.0, min_n=2, threshold=0.0, sustain_scans=2)
    neg = [{"calib_prob": 0.5, "realized_r": -1.0}] * 10
    pos = [{"calib_prob": 0.5, "realized_r": 1.0}] * 10
    # bucket A: negative streak
    em.evaluate("iceberg", "BTC", neg)
    em.evaluate("iceberg", "BTC", neg)
    # bucket B: should not be affected
    out_b = em.evaluate("iceberg", "ETH", pos)
    assert out_b["streak"] == 0
    assert out_b["fired"] is False


def test_expectancy_monitor_min_n_unmet_resets_streak():
    em = ExpectancyMonitor(top_pct=1.0, min_n=50, threshold=0.0, sustain_scans=2)
    # Build streak with enough data
    big_neg = [{"calib_prob": 0.5, "realized_r": -1.0}] * 100
    em.evaluate("kind", "sym", big_neg)
    em.evaluate("kind", "sym", big_neg)
    # Then sparse data — should reset (no decision possible)
    small = [{"calib_prob": 0.5, "realized_r": -1.0}] * 10
    out = em.evaluate("kind", "sym", small)
    assert out["expectancy"] is None
    assert out["streak"] == 0
    assert out["fired"] is False
