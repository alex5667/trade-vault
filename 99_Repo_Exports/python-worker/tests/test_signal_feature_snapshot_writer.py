"""Plan 3 / Step 1 — signal_feature_snapshot_writer row builder tests."""
from __future__ import annotations

import json

from services.signal_feature_snapshot_writer import (
    build_snapshot_row,
    extract_indicators,
    parse_signal,
)


def _baseline_signal(**overrides):
    sig = {
        "signal_id": "sig-abc",
        "symbol": "BTCUSDT",
        "direction": "LONG",
        "ts_ms": 1_700_000_000_000,
        "event_time_ms": 1_700_000_000_010,
        "ingest_time_ms": 1_700_000_000_050,
        "price": 50_000.0,
        "source": "crypto-of",
        "trace_id": "trace-1",
        "kind": "iceberg",
        "indicators": {
            "spread_bps": 2.0,
            "regime": "momentum",
            "p_edge": 0.61,
            "of_score": 0.78,
        },
    }
    sig.update(overrides)
    return sig


def test_parse_signal_payload_blob():
    sig = _baseline_signal()
    fields = {"payload": json.dumps(sig)}
    parsed = parse_signal(fields)
    assert parsed is not None
    assert parsed["signal_id"] == "sig-abc"


def test_parse_signal_flat_fallback():
    fields = {"symbol": "ETHUSDT", "signal_id": "x"}
    parsed = parse_signal(fields)
    assert parsed == fields


def test_parse_signal_returns_none_on_empty():
    assert parse_signal({}) is None


def test_extract_indicators_dict():
    assert extract_indicators({"indicators": {"a": 1}}) == {"a": 1}


def test_extract_indicators_json_str():
    assert extract_indicators({"indicators": '{"a": 2}'}) == {"a": 2}


def test_extract_indicators_missing():
    assert extract_indicators({}) == {}


def test_build_row_full_signal():
    sig = _baseline_signal()
    row = build_snapshot_row(sig, now_ms=1_700_000_000_500, schema_name="of_v1", schema_version="v1")
    assert row is not None
    decision_ms, sid, symbol, kind, side, source, trace_id, *_ = row
    assert sid == "sig-abc"
    assert symbol == "BTCUSDT"
    assert kind == "iceberg"
    assert side == 1
    assert source == "crypto-of"
    assert trace_id == "trace-1"
    # decision_time_ms uses sig.ts_ms when present
    assert decision_ms == 1_700_000_000_000


def test_build_row_short_side():
    sig = _baseline_signal(direction="SHORT")
    row = build_snapshot_row(sig, now_ms=0, schema_name="x", schema_version="y")
    assert row is not None
    # side is index 4
    assert row[4] == -1


def test_build_row_skips_when_no_sid():
    sig = _baseline_signal()
    del sig["signal_id"]
    row = build_snapshot_row(sig, now_ms=0, schema_name="x", schema_version="y")
    assert row is None


def test_build_row_skips_when_no_symbol():
    sig = _baseline_signal(symbol="")
    row = build_snapshot_row(sig, now_ms=0, schema_name="x", schema_version="y")
    assert row is None


def test_build_row_entry_px_long_above_mid():
    """LONG: entry_px should be > mid (paying half-spread + slip)."""
    sig = _baseline_signal()
    row = build_snapshot_row(sig, now_ms=0, schema_name="x", schema_version="y", slip_prior_bps=1.5)
    assert row is not None
    # row index 14 = entry_px_expected, 15 = mid_px_submit
    entry_px = row[14]
    mid_px = row[15]
    assert entry_px is not None and mid_px is not None
    assert entry_px > mid_px


def test_build_row_entry_px_short_below_mid():
    sig = _baseline_signal(direction="SHORT")
    row = build_snapshot_row(sig, now_ms=0, schema_name="x", schema_version="y", slip_prior_bps=1.5)
    assert row is not None
    entry_px = row[14]
    mid_px = row[15]
    assert entry_px is not None and mid_px is not None
    assert entry_px < mid_px


def test_build_row_dq_flags_no_mid_px():
    sig = _baseline_signal(price=0)
    sig["entry"] = None
    sig["entry_price"] = None
    sig["indicators"] = dict(sig["indicators"])  # copy
    sig["indicators"].pop("price", None)
    sig["indicators"].pop("entry_price", None)
    row = build_snapshot_row(sig, now_ms=0, schema_name="x", schema_version="y")
    assert row is not None
    dq_flags = json.loads(row[19])
    assert "no_mid_px" in dq_flags


def test_build_row_dq_flags_no_spread():
    sig = _baseline_signal()
    sig["indicators"] = dict(sig["indicators"])
    sig["indicators"]["spread_bps"] = 0.0
    row = build_snapshot_row(sig, now_ms=0, schema_name="x", schema_version="y")
    assert row is not None
    dq_flags = json.loads(row[19])
    assert "no_spread" in dq_flags


def test_build_row_schema_hash_stable_across_calls():
    sig1 = _baseline_signal()
    sig2 = _baseline_signal()
    row1 = build_snapshot_row(sig1, now_ms=0, schema_name="x", schema_version="y")
    row2 = build_snapshot_row(sig2, now_ms=0, schema_name="x", schema_version="y")
    assert row1 is not None and row2 is not None
    # row index 9 = schema_hash
    assert row1[9] == row2[9]


def test_build_row_schema_hash_changes_when_indicator_added():
    sig1 = _baseline_signal()
    sig2 = _baseline_signal()
    sig2["indicators"] = dict(sig2["indicators"])
    sig2["indicators"]["new_feature"] = 99.0
    row1 = build_snapshot_row(sig1, now_ms=0, schema_name="x", schema_version="y")
    row2 = build_snapshot_row(sig2, now_ms=0, schema_name="x", schema_version="y")
    assert row1 is not None and row2 is not None
    assert row1[9] != row2[9]


def test_build_row_features_json_is_valid_json():
    sig = _baseline_signal()
    row = build_snapshot_row(sig, now_ms=0, schema_name="x", schema_version="y")
    assert row is not None
    parsed = json.loads(row[18])
    assert parsed["spread_bps"] == 2.0
    assert parsed["regime"] == "momentum"


def test_build_row_meta_carries_feature_counts():
    sig = _baseline_signal()
    row = build_snapshot_row(sig, now_ms=0, schema_name="x", schema_version="y")
    assert row is not None
    meta = json.loads(row[20])
    assert meta["feature_cols_n"] >= 1
    assert "raw_score" in meta
    assert "calib_prob" in meta


def test_build_row_uses_now_ms_when_signal_ts_missing():
    sig = _baseline_signal()
    del sig["ts_ms"]
    sig.pop("timestamp_ms", None)
    row = build_snapshot_row(sig, now_ms=42, schema_name="x", schema_version="y")
    assert row is not None
    assert row[0] == 42  # decision_time_ms
