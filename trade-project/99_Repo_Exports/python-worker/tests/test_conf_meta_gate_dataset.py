"""Plan 1 Phase 1 — dataset builder tests.

We exercise the pure helpers (feature coercion, session/regime mapping,
compatibility filter) and a small fake-cursor end-to-end to assert the
build_dataset() join + filter pipeline.
"""
from __future__ import annotations

from dataclasses import asdict

from calibration.conf_meta_gate_dataset import (
    CompatibilityFilter,
    MetaGateTrainRow,
    _gated_out_row_from_db,
    _passed_row_from_db,
    apply_compatibility_filter,
    build_dataset,
    coerce_features,
    fetch_feature_snapshots,
    regime_to_code,
    session_to_onehot,
)


# ── pure helpers ────────────────────────────────────────────────────────────


def test_coerce_features_drops_non_numeric() -> None:
    src = {"a": 1.0, "b": "2.5", "c": None, "d": "hello", "e": float("nan")}
    out = coerce_features(src)
    assert out == {"a": 1.0, "b": 2.5}


def test_coerce_features_accepts_json_string() -> None:
    out = coerce_features('{"x": 3, "y": -1.5}')
    assert out == {"x": 3.0, "y": -1.5}


def test_coerce_features_handles_garbage_input() -> None:
    assert coerce_features(None) == {}
    assert coerce_features("not json") == {}
    assert coerce_features([1, 2, 3]) == {}


def test_regime_to_code_known_values() -> None:
    assert regime_to_code("trending_bull") == 1.0
    assert regime_to_code("range") == 3.0
    assert regime_to_code("unknown") == 0.0
    assert regime_to_code("") == 0.0
    assert regime_to_code(None) == 0.0
    assert regime_to_code("weird-state") == 0.0


def test_session_onehot_categories() -> None:
    a, e, u, w = session_to_onehot("asia")
    assert (a, e, u, w) == (1.0, 0.0, 0.0, 0.0)
    a, e, u, w = session_to_onehot("weekend")
    assert (a, e, u, w) == (0.0, 0.0, 0.0, 1.0)
    a, e, u, w = session_to_onehot("us")
    assert (a, e, u, w) == (0.0, 0.0, 1.0, 0.0)
    a, e, u, w = session_to_onehot("european")
    assert (a, e, u, w) == (0.0, 1.0, 0.0, 0.0)
    a, e, u, w = session_to_onehot("???")
    assert (a, e, u, w) == (0.0, 0.0, 0.0, 0.0)


# ── compatibility filter ────────────────────────────────────────────────────


def _row(horizon: int = 600_000, tp: float = 30.0, sl: float = 20.0, cohort: str = "passed",
         sid: str = "s") -> MetaGateTrainRow:
    return MetaGateTrainRow(
        sid=sid, ts_ms=1, symbol="BTCUSDT", kind="x", side="LONG", cohort=cohort,
        legacy_confidence=0.5, legacy_min_confidence=0.7, legacy_would_allow=0,
        p_edge_raw=0.0, p_edge_cal=0.0, rule_score=0.0, have_need_ratio=0.0,
        spread_bps=0.0, expected_slippage_bps=0.0, fee_bps=0.0, exec_cost_bps=0.0,
        expected_edge_bps=0.0, exec_risk_norm=0.0,
        dq_score=1.0, dq_flag_count=0.0, signal_age_ms=0.0,
        regime_code=0.0, session_asia=0, session_europe=0, session_us=0, weekend_flag=0,
        prior_winrate=0.0, prior_ev_r=0.0, prior_sample_count_log=0.0,
        horizon_ms=horizon, tp_bps=tp, sl_bps=sl,
        y_win=0, y_util_pos=0, r_mult=0.0, ret_bps=0.0,
    )


def test_filter_keeps_in_range_rows() -> None:
    flt = CompatibilityFilter(
        horizon_ms_allowed=frozenset({600_000, 1_800_000}),
        tp_bps_min=5.0, tp_bps_max=80.0,
        sl_bps_min=5.0, sl_bps_max=60.0,
    )
    rows = [_row(600_000, 30, 20), _row(1_800_000, 60, 40)]
    kept = apply_compatibility_filter(rows, flt)
    assert len(kept) == 2


def test_filter_drops_out_of_range_horizon() -> None:
    flt = CompatibilityFilter(horizon_ms_allowed=frozenset({600_000}))
    rows = [_row(600_000), _row(900_000)]
    kept = apply_compatibility_filter(rows, flt)
    assert [r.horizon_ms for r in kept] == [600_000]


def test_filter_drops_zero_tp_or_sl() -> None:
    flt = CompatibilityFilter()
    rows = [_row(tp=0.0), _row(sl=0.0)]
    assert apply_compatibility_filter(rows, flt) == []


def test_filter_empty_horizon_allowlist_means_any() -> None:
    flt = CompatibilityFilter(
        horizon_ms_allowed=frozenset(),
        tp_bps_min=1.0, tp_bps_max=100.0,
        sl_bps_min=1.0, sl_bps_max=100.0,
    )
    rows = [_row(123_456), _row(987_654)]
    assert len(apply_compatibility_filter(rows, flt)) == 2


# ── per-record builders ─────────────────────────────────────────────────────


def test_passed_row_from_db_minimum() -> None:
    rec = {
        "sid": "p-1",
        "decision_time_ms": 1_700_000_000_000,
        "ingest_time_ms": 1_700_000_000_500,
        "symbol": "BTCUSDT",
        "side": 1,
        "kind": "edge_stack_v1",
        "regime": "trending_bull",
        "atr_bps": 12.0,
        "ttl_ms": 600_000,
        "tp_r": 1.0,
        "sl_r": 1.0,
        "r_unit_px": 50.0,    # ⇒ sl_bps = (50 / 50000) * 10_000 = 10 bp
        "entry_px": 50_000.0,
        "calib_prob": 0.6,
        "raw_score": 0.55,
        "label": 1,
        "realized_r": 1.5,
        "realized_bps": 18.0,
        "features": {"rule_score": 0.7, "spread_bps": 1.0},
    }
    row = _passed_row_from_db(rec)
    assert row is not None
    assert row.cohort == "passed"
    assert row.tp_bps == 10.0
    assert row.sl_bps == 10.0
    assert row.y_win == 1
    assert row.y_util_pos == 1  # realized_bps 18 > 4 bp threshold
    assert row.rule_score == 0.7
    assert row.regime_code == 1.0


def test_passed_row_from_db_rejects_missing_sid() -> None:
    rec = {"sid": "", "decision_time_ms": 1}
    assert _passed_row_from_db(rec) is None


def test_gated_out_row_from_db_uses_cost_fields() -> None:
    rec = {
        "sid": "g-1",
        "ts_ms": 1_700_000_000_000,
        "ts_close_ms": 1_700_000_001_000,
        "symbol": "ETHUSDT",
        "direction": -1,
        "kind": "iceberg",
        "entry_px": 3000.0,
        "tp_bps": 25.0,
        "sl_bps": 15.0,
        "horizon_ms": 1_800_000,
        "confidence": 0.6,
        "min_conf": 0.7,
        "label": -1,
        "r_mult": -1.0,
        "ret_bps": -15.0,
        "cost_bps": 3.0,
        "cost_fees_bps": 1.0,
        "cost_spread_bps": 1.5,
        "cost_slippage_bps": 0.5,
        "y_edge_cost_aware": 0,
    }
    row = _gated_out_row_from_db(rec, snapshot_features={})
    assert row is not None
    assert row.cohort == "gated_out"
    assert row.side == "SHORT"
    assert row.spread_bps == 1.5
    assert row.fee_bps == 1.0
    assert row.exec_cost_bps == 3.0
    assert row.y_win == 0
    assert row.y_util_pos == 0


def test_gated_out_row_attaches_snapshot_features() -> None:
    rec = {
        "sid": "g-2",
        "ts_ms": 1_700_000_000_000,
        "symbol": "BTCUSDT",
        "direction": 1,
        "entry_px": 50_000.0,
        "tp_bps": 25.0,
        "sl_bps": 15.0,
        "horizon_ms": 600_000,
        "confidence": 0.5,
        "min_conf": 0.7,
        "label": 0,
    }
    snaps = {"g-2": {"p_edge_raw": 0.45, "rule_score": 0.6}}
    row = _gated_out_row_from_db(rec, snapshot_features=snaps)
    assert row is not None
    assert row.p_edge_raw == 0.45
    assert row.rule_score == 0.6


# ── fake-cursor end-to-end ──────────────────────────────────────────────────


class _FakeCursor:
    def __init__(self, payloads: dict[str, list[tuple]]):
        self._payloads = payloads
        self._last_key: str | None = None
        self.description = []
        self._last_rows: list[tuple] = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *exc) -> None:
        return None

    def execute(self, sql: str, params=()) -> None:
        if "FROM signal_outcome" in sql:
            key = "passed"
        elif "FROM signal_gated_out_outcomes" in sql:
            key = "gated_out"
        elif "FROM signal_feature_snapshots" in sql:
            key = "snapshots"
        else:
            key = "unknown"
        rows = self._payloads.get(key, [])
        cols = self._payloads.get(f"{key}_cols", [])
        self.description = [(c,) for c in cols]
        self._last_rows = rows

    def fetchall(self) -> list[tuple]:
        return self._last_rows


class _FakeConn:
    def __init__(self, payloads: dict) -> None:
        self._payloads = payloads

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._payloads)


def test_build_dataset_joins_and_filters() -> None:
    passed_cols = [
        "sid", "decision_time_ms", "ingest_time_ms", "symbol", "side",
        "kind", "regime", "atr_bps", "ttl_ms", "tp_r", "sl_r", "r_unit_px",
        "entry_px", "calib_prob", "raw_score", "label", "realized_r",
        "realized_bps", "features",
    ]
    passed_rows = [
        ("p-1", 1_700_000_000_000, 1_700_000_000_500, "BTCUSDT", 1, "x",
         "trending_bull", 12.0, 600_000, 1.0, 1.0, 50.0, 50_000.0,
         0.6, 0.55, 1, 1.5, 18.0, {"spread_bps": 1.0}),
    ]
    gated_cols = [
        "sid", "ts_ms", "ts_close_ms", "symbol", "direction", "kind",
        "entry_px", "tp_bps", "sl_bps", "horizon_ms", "confidence", "min_conf",
        "label", "r_mult", "ret_bps", "cost_bps", "cost_fees_bps",
        "cost_spread_bps", "cost_slippage_bps", "y_edge_cost_aware",
    ]
    gated_rows = [
        ("g-1", 1_700_000_000_000, 1_700_000_001_000, "ETHUSDT", -1, "iceberg",
         3000.0, 25.0, 15.0, 600_000, 0.5, 0.7, -1, -1.0, -15.0, 3.0, 1.0, 1.5, 0.5, 0),
        # Out-of-horizon row that should be filtered out:
        ("g-2", 1_700_000_000_000, 1_700_000_001_000, "ETHUSDT", -1, "iceberg",
         3000.0, 25.0, 15.0, 99, 0.5, 0.7, 0, 0.0, 0.0, 3.0, 1.0, 1.5, 0.5, 0),
    ]
    snaps_rows: list[tuple] = []

    conn = _FakeConn({
        "passed": passed_rows, "passed_cols": passed_cols,
        "gated_out": gated_rows, "gated_out_cols": gated_cols,
        "snapshots": snaps_rows, "snapshots_cols": ["sid", "features"],
    })
    flt = CompatibilityFilter(
        horizon_ms_allowed=frozenset({600_000}),
        tp_bps_min=5.0, tp_bps_max=80.0,
        sl_bps_min=5.0, sl_bps_max=60.0,
    )
    rows = build_dataset(conn, since_ms=0, until_ms=10**13, flt=flt)
    # passed: 1 row (tp_bps=10, sl_bps=10, horizon=600k) passes the filter.
    # gated_out: only g-1 has horizon=600k. g-2 has horizon=99 (out of allowlist).
    sids = sorted(r.sid for r in rows)
    assert sids == ["g-1", "p-1"]
    cohorts = sorted(r.cohort for r in rows)
    assert cohorts == ["gated_out", "passed"]


def test_fetch_feature_snapshots_empty_sid_list_short_circuits() -> None:
    conn = _FakeConn({})
    assert fetch_feature_snapshots(conn, []) == {}


def test_row_is_serialisable() -> None:
    row = _row()
    payload = asdict(row)
    assert isinstance(payload, dict)
    assert payload["sid"] == "s"
