"""Tests for services/signal_snapshot_persister.py parsing layer."""
from __future__ import annotations

import gzip
import json

import pytest

from services.signal_snapshot_persister import parse_entry, _safe_float


class TestSafeFloat:
    def test_finite(self):
        assert _safe_float(3.14) == 3.14
        assert _safe_float("1.5") == 1.5
        assert _safe_float(0) == 0.0

    def test_none_empty(self):
        assert _safe_float(None) is None
        assert _safe_float("") is None

    def test_invalid(self):
        assert _safe_float("abc") is None

    def test_nan_inf(self):
        assert _safe_float(float("nan")) is None
        assert _safe_float(float("inf")) is None
        assert _safe_float(float("-inf")) is None


# ── parse_entry ───────────────────────────────────────────────────────────────


def _make_signal(sid: str = "of:BTCUSDT:1779000000000:L",
                 symbol: str = "BTCUSDT",
                 ts_ms: int = 1779000000000,
                 indicators: dict | None = None,
                 confidence: float = 0.85,
                 direction: str = "LONG") -> dict:
    ind = indicators or {
        "regime": "trending_bull",
        "confidence_v1": confidence,
        "confidence_breakdown": {
            "base": 0.5, "mult": 1.0, "pen_total": 0.0,
            "ml_shadow_conf01": 0.14, "scorer_mode": "canary_shadow",
        },
        "atr_bps_exec": 12.5,
    }
    return {
        "sid": sid, "symbol": symbol, "ts_ms": ts_ms,
        "direction": direction, "side": direction,
        "confidence": confidence, "indicators": ind,
    }


class TestParseEntry:
    def test_minimal_valid_entry(self):
        sig = _make_signal()
        row = parse_entry("1779000000000-0", {"payload": json.dumps(sig)})
        assert row is not None
        assert row["sid"] == "of:BTCUSDT:1779000000000:L"
        assert row["symbol"] == "BTCUSDT"
        assert row["ts_ms"] == 1779000000000
        assert row["direction"] == "LONG"
        assert row["kind"] == "of"
        assert row["regime"] == "trending_bull"
        assert row["confidence"] == 0.85
        assert row["ml_shadow_conf01"] == 0.14
        assert row["scorer_mode"] == "canary_shadow"
        assert "atr_bps_exec" in row["indicators"]
        assert row["payload_gz"] is not None
        assert row["payload_size_bytes"] > 0

    def test_no_payload_returns_none(self):
        assert parse_entry("1-0", {}) is None
        assert parse_entry("1-0", {"payload": ""}) is None

    def test_invalid_json_returns_none(self):
        assert parse_entry("1-0", {"payload": "not json {"}) is None

    def test_no_sid_returns_none(self):
        sig = _make_signal()
        del sig["sid"]
        assert parse_entry("1-0", {"payload": json.dumps(sig)}) is None

    def test_ts_falls_back_to_msg_id(self):
        sig = _make_signal()
        del sig["ts_ms"]
        sig.pop("tick_ts", None)
        sig.pop("ts_emit_ms", None)
        row = parse_entry("1700000000000-0", {"payload": json.dumps(sig)})
        assert row is not None
        assert row["ts_ms"] == 1700000000000

    def test_kind_extraction_from_sid(self):
        for sid, expected in [
            ("of:BTC:123:L", "of"),
            ("iceberg:ETH:456:S", "iceberg"),
            ("delta_spike:SOL:789", "delta_spike"),
            ("BTC:123:L", None),  # no kind prefix
        ]:
            sig = _make_signal(sid=sid)
            row = parse_entry("1-0", {"payload": json.dumps(sig)})
            assert row is not None
            assert row["kind"] == expected, f"sid={sid}"

    def test_regime_none_string_treated_as_null(self):
        sig = _make_signal(indicators={"regime": "None"})
        row = parse_entry("1-0", {"payload": json.dumps(sig)})
        assert row is not None
        assert row["regime"] is None

    def test_regime_na_kept(self):
        sig = _make_signal(indicators={"regime": "na"})
        row = parse_entry("1-0", {"payload": json.dumps(sig)})
        assert row["regime"] == "na"

    def test_envelope_wrapped_in_data_field(self):
        """Some publishers wrap inner JSON in {"data": "<json-string>"} field."""
        inner = _make_signal()
        wrapper = {"data": json.dumps(inner)}
        row = parse_entry("1-0", {"payload": json.dumps(wrapper)})
        assert row is not None
        assert row["sid"] == "of:BTCUSDT:1779000000000:L"

    def test_payload_gz_roundtrip(self):
        sig = _make_signal()
        raw = json.dumps(sig)
        row = parse_entry("1-0", {"payload": raw})
        assert row["payload_gz"] is not None
        decompressed = gzip.decompress(row["payload_gz"]).decode()
        assert decompressed == raw
        # Compression should reduce size
        assert len(row["payload_gz"]) < row["payload_size_bytes"]

    def test_indicators_dict_preserved(self):
        sig = _make_signal(indicators={
            "atr_bps_exec": 5.2, "delta_z": -1.3, "obi": 0.42,
        })
        row = parse_entry("1-0", {"payload": json.dumps(sig)})
        assert row["indicators"]["atr_bps_exec"] == 5.2
        assert row["indicators"]["delta_z"] == -1.3
        assert row["indicators"]["obi"] == 0.42

    def test_scorer_mode_passes_through(self):
        sig = _make_signal()
        sig["indicators"]["confidence_breakdown"]["scorer_mode"] = "ml_canary_enforce"
        row = parse_entry("1-0", {"payload": json.dumps(sig)})
        assert row["scorer_mode"] == "ml_canary_enforce"

    def test_missing_confidence_breakdown_yields_none_ml(self):
        sig = _make_signal(indicators={"regime": "trending_bull"})
        row = parse_entry("1-0", {"payload": json.dumps(sig)})
        assert row["ml_shadow_conf01"] is None
        assert row["scorer_mode"] is None

    def test_direction_alias_side(self):
        sig = _make_signal()
        del sig["direction"]
        sig["side"] = "SHORT"
        row = parse_entry("1-0", {"payload": json.dumps(sig)})
        assert row["direction"] == "SHORT"

    def test_symbol_uppercased(self):
        sig = _make_signal(symbol="ethusdt")
        row = parse_entry("1-0", {"payload": json.dumps(sig)})
        assert row["symbol"] == "ETHUSDT"

    def test_msg_id_passed_through(self):
        sig = _make_signal()
        row = parse_entry("1700000123456-0", {"payload": json.dumps(sig)})
        assert row["msg_id"] == "1700000123456-0"
