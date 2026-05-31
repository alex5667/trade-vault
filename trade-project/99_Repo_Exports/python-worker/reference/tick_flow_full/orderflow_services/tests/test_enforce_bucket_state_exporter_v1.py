"""
Tests for enforce_bucket_state_exporter_v1.py (P77)

Tests the pure-logic helpers:
  _parse_list, _as_float, _as_int, _as_str, _load_json, Exporter tick logic.
No Prometheus background server is started — gauges are only referenced, not scraped.
"""

import json
import os

# Resolve PYTHONPATH: python-worker root is the test root
# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from orderflow_services.enforce_bucket_state_exporter_v1 import (
    Exporter,
    _as_float,
    _as_int,
    _as_str,
    _load_json,
    _parse_list,
)

# ---- unit tests for pure helpers ----

def test_parse_list_basic():
    result = _parse_list("NORMAL,HIGH_VOL,LOW_LIQ")
    assert result == ["NORMAL", "HIGH_VOL", "LOW_LIQ"]


def test_parse_list_dedup():
    result = _parse_list("NORMAL,HIGH_VOL,NORMAL")
    assert result == ["NORMAL", "HIGH_VOL"]


def test_parse_list_semicolon():
    result = _parse_list("NORMAL;HIGH_VOL")
    assert result == ["NORMAL", "HIGH_VOL"]


def test_parse_list_empty():
    assert _parse_list("") == []
    assert _parse_list("   ") == []


def test_parse_list_uppercase():
    result = _parse_list("normal,low_liq")
    assert result == ["NORMAL", "LOW_LIQ"]


def test_as_float_normal():
    assert _as_float("3.14") == 3.14


def test_as_float_none():
    assert _as_float(None, 1.0) == 1.0


def test_as_float_bool():
    # bool should use default
    assert _as_float(True, 0.0) == 0.0


def test_as_int_normal():
    assert _as_int("42") == 42


def test_as_int_float_string():
    assert _as_int("9.9") == 9


def test_as_int_none():
    assert _as_int(None, 7) == 7


def test_as_str_bytes():
    assert _as_str(b"hello") == "hello"


def test_as_str_none():
    assert _as_str(None, "default") == "default"


def test_load_json_valid(tmp_path):
    p = tmp_path / "test.json"
    p.write_text('{"ts_ms": 123, "apply": false}', encoding="utf-8")
    obj = _load_json(str(p))
    assert isinstance(obj, dict)
    assert obj["ts_ms"] == 123


def test_load_json_missing():
    assert _load_json("/nonexistent/path/test.json") is None


def test_load_json_invalid(tmp_path):
    p = tmp_path / "bad.json"
    p.write_text("not json", encoding="utf-8")
    assert _load_json(str(p)) is None


# ---- integration-style tests with mocked redis ----

def _make_exporter_no_redis():
    """Create Exporter with redis disabled (no REDIS_URL env)."""
    os.environ.pop("REDIS_URL", None)
    os.environ.pop("CRYPTO_NOTIFY_REDIS_URL", None)
    ex = Exporter()
    return ex


def test_exporter_set_allow_flags(monkeypatch):
    """_set_allow_flags must update gauge correctly for each bucket."""
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.delenv("CRYPTO_NOTIFY_REDIS_URL", raising=False)
    ex = _make_exporter_no_redis()
    ex.buckets = ["NORMAL", "HIGH_VOL", "LOW_LIQ", "HIGH_VOL_LOW_LIQ"]

    # Track gauge calls
    seen: dict = {}
    original_labels = None

    import orderflow_services.enforce_bucket_state_exporter_v1 as mod

    class FakeGauge:
        def __init__(self):
            self._val = None

        def set(self, v):
            self._val = v

    fake_gauges: dict = {}

    def fake_labels(**kw):
        key = (kw.get("component"), kw.get("sym"), kw.get("bucket"))
        if key not in fake_gauges:
            fake_gauges[key] = FakeGauge()
        return fake_gauges[key]

    monkeypatch.setattr(mod.of_enforce_bucket_flag, "labels", fake_labels)

    ex._set_allow_flags(component="slippage", sym="global", allow="HIGH_VOL,LOW_LIQ"),

    assert fake_gauges[("slippage", "global", "HIGH_VOL")]._val == 1.0
    assert fake_gauges[("slippage", "global", "LOW_LIQ")]._val == 1.0
    assert fake_gauges[("slippage", "global", "NORMAL")]._val == 0.0
    assert fake_gauges[("slippage", "global", "HIGH_VOL_LOW_LIQ")]._val == 0.0


def test_exporter_reads_status_file(tmp_path, monkeypatch):
    """_export_promoter must read from status file when present."""
    monkeypatch.delenv("REDIS_URL", raising=False),
    monkeypatch.delenv("CRYPTO_NOTIFY_REDIS_URL", raising=False),
    monkeypatch.setenv("ENFORCE_PROMOTER_STATUS_PATH", str(tmp_path / "status.json")),

    report = {
        "ts_ms": 1000000000,
        "apply": False,
        "decisions": {
            "slippage": {"ok": True, "added": "HIGH_VOL", "reasons": []},
            "taker": {"ok": False, "added": "", "reasons": []},
        },
        "bucket_health": {
            "HIGH_VOL": {"db_n": 500, "resid_p95": 1.5, "resid_p99": 3.0, "gate_n": 1000, "ok_soft_rate": 0.20}
        }
    }
    (tmp_path / "status.json").write_text(json.dumps(report), encoding="utf-8")

    ex = Exporter()
    result = ex._read_promoter_report()
    assert result is not None
    assert result["ts_ms"] == 1000000000
    assert result["decisions"]["slippage"]["ok"] is True


def test_exporter_empty_symbols_skips_coeffs(monkeypatch):
    """_export_coeffs should be a no-op when no symbols configured."""
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.setenv("ENFORCE_STATE_EXPORTER_SYMBOLS", "")
    ex = Exporter()
    assert ex.symbols == []
    # Should not raise
    ex._export_coeffs()


def test_exporter_edge_neg_share_gauge_set_from_status(tmp_path, monkeypatch):
    """P89: of_enforce_promoter_bucket_edge_neg_share must be populated from bucket_health in status file."""
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.delenv("CRYPTO_NOTIFY_REDIS_URL", raising=False)
    monkeypatch.setenv("ENFORCE_PROMOTER_STATUS_PATH", str(tmp_path / "status.json"))

    report = {
        "ts_ms": 1700000000000,
        "apply": False,
        "decisions": {
            "slippage": {"ok": False, "added": "", "reasons": []},
            "taker": {"ok": False, "added": "", "reasons": []},
        },
        # P89: edge_neg_share now included in bucket_health
        "bucket_health": {
            "HIGH_VOL": {"db_n": 300, "resid_p95": 2.1, "resid_p99": 4.5, "edge_neg_share": 0.42, "gate_n": 600, "ok_soft_rate": 0.10},
            "HIGH_VOL_LOW_LIQ": {"db_n": 100, "resid_p95": 1.0, "resid_p99": 2.0, "edge_neg_share": 0.12, "gate_n": 200, "ok_soft_rate": 0.15},
        }
    }
    (tmp_path / "status.json").write_text(json.dumps(report), encoding="utf-8")

    import orderflow_services.enforce_bucket_state_exporter_v1 as mod

    seen: dict = {}

    class FakeGauge:
        def __init__(self):
            self._val = None
        def set(self, v):
            self._val = v

    fake_gs = {}

    def fake_labels(**kw):
        key = kw.get("bucket", "")
        if key not in fake_gs:
            fake_gs[key] = FakeGauge()
        return fake_gs[key]

    monkeypatch.setattr(mod.of_enforce_promoter_bucket_edge_neg_share, "labels", fake_labels)

    ex = Exporter()
    ex._export_promoter()

    assert "HIGH_VOL" in fake_gs
    assert abs(fake_gs["HIGH_VOL"]._val - 0.42) < 1e-6
    assert "HIGH_VOL_LOW_LIQ" in fake_gs
    assert abs(fake_gs["HIGH_VOL_LOW_LIQ"]._val - 0.12) < 1e-6
