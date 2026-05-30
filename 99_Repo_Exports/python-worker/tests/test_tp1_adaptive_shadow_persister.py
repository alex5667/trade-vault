"""Tests for services/tp1_adaptive_shadow_persister.parse_entry — flat envelope."""

from __future__ import annotations

import pytest

from services.tp1_adaptive_shadow_persister import parse_entry


def _full_fields() -> dict[str, str]:
    return {
        "ts_ms": "1700000000000",
        "sid": "of:BTCUSDT:42",
        "symbol": "BTCUSDT",
        "kind": "of",
        "side": "LONG",
        "regime": "range",
        "entry_price": "10000.0",
        "sl_price": "9900.0",
        "baseline_tp1_price": "10115.0",
        "baseline_tp1_rr": "1.15",
        "adaptive_tp1_price": "10080.0",
        "adaptive_tp1_rr": "0.80",
        "p_hit_baseline": "0.40",
        "p_hit_adaptive": "0.90",
        "ev_baseline_r": "-0.5",
        "ev_adaptive_r": "0.1",
        "ev_delta_r": "0.6",
        "cost_r": "0.08",
        "spread_bps": "2.0",
        "slippage_bps": "1.0",
        "fee_bps": "4.0",
        "samples": "350",
        "reason_code": "tp1_adaptive_shadow",
        "mode": "shadow",
    }


def test_parse_full_envelope() -> None:
    row = parse_entry("1700000000000-0", _full_fields())
    assert row is not None
    assert row["ts_ms"] == 1700000000000
    assert row["sid"] == "of:BTCUSDT:42"
    assert row["symbol"] == "BTCUSDT"
    assert row["entry_price"] == pytest.approx(10000.0)
    assert row["baseline_tp1_rr"] == pytest.approx(1.15)
    assert row["adaptive_tp1_rr"] == pytest.approx(0.80)
    assert row["p_hit_baseline"] == pytest.approx(0.40)
    assert row["ev_delta_r"] == pytest.approx(0.6)
    assert row["samples"] == 350
    assert row["reason_code"] == "tp1_adaptive_shadow"
    assert row["mode"] == "shadow"


def test_parse_handles_empty_optionals() -> None:
    f = _full_fields()
    # adaptive_tp1_* are NULL when policy did not pick a candidate.
    f["adaptive_tp1_price"] = ""
    f["adaptive_tp1_rr"] = ""
    f["p_hit_adaptive"] = ""
    row = parse_entry("1700000000000-0", f)
    assert row is not None
    assert row["adaptive_tp1_price"] is None
    assert row["adaptive_tp1_rr"] is None
    assert row["p_hit_adaptive"] is None


def test_parse_falls_back_ts_to_stream_id() -> None:
    f = _full_fields()
    f.pop("ts_ms")
    row = parse_entry("1234567890-0", f)
    assert row is not None
    assert row["ts_ms"] == 1234567890


def test_parse_rejects_missing_sid() -> None:
    f = _full_fields()
    f.pop("sid")
    assert parse_entry("1234-0", f) is None


def test_parse_rejects_missing_required_numeric() -> None:
    f = _full_fields()
    f["entry_price"] = ""
    assert parse_entry("1234-0", f) is None


def test_parse_rejects_missing_reason_or_mode() -> None:
    f = _full_fields()
    f.pop("reason_code")
    assert parse_entry("1234-0", f) is None
    f = _full_fields()
    f.pop("mode")
    assert parse_entry("1234-0", f) is None


def test_parse_handles_corrupted_numeric_gracefully() -> None:
    f = _full_fields()
    f["p_hit_baseline"] = "nan"
    f["cost_r"] = "inf"
    row = parse_entry("1234-0", f)
    assert row is not None
    # NaN/inf coerced to None
    assert row["p_hit_baseline"] is None
    assert row["cost_r"] is None
