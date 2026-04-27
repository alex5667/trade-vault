"""Tests for OFConfirm golden replay tools"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools.export_of_confirm_inputs_ndjson import _decode_payload, _load_state, _save_state, _atomic_write
from tools.of_confirm_replay_from_inputs import (
    _safe_loads,
    _safe_loads_maybe,
    _extract_inputs,
    _mk_runtime,
    _key,
    _ofc_to_dict,
    _evidence,
    _to_int,
    _to_float,
    _norm_direction,
    _norm_tick_ts_ms,
    _norm_tf,
    _norm_price,
    _norm_delta_z,
    replay_one,
)
from tools.of_confirm_diff_report import _load_ndjson, _group_key
from core.of_confirm_engine import OFConfirmEngine


def test_decode_payload():
    """Test payload decoding from Redis stream fields"""
    fields = {"payload": '{"symbol": "BTCUSDT", "ts_ms": 1000}'}
    result = _decode_payload(fields, "payload")
    assert result == '{"symbol": "BTCUSDT", "ts_ms": 1000}'
    
    fields_bytes = {"payload": b'{"symbol": "ETHUSDT"}'}
    result = _decode_payload(fields_bytes, "payload")
    assert result == '{"symbol": "ETHUSDT"}'


def test_state_persistence():
    """Test state file save/load"""
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
        state_path = f.name
    
    try:
        state = {"last_id": "123-0", "updated_ts_ms": 1000, "wrote": 42}
        _save_state(state_path, state)
        loaded = _load_state(state_path)
        assert loaded == state
    finally:
        Path(state_path).unlink(missing_ok=True)


def test_safe_loads():
    """Test safe JSON loading"""
    assert _safe_loads('{"a": 1}') == {"a": 1}
    assert _safe_loads("invalid") is None
    assert _safe_loads("") is None


def test_extract_inputs():
    """Test input extraction from various payload shapes"""
    # Flat dict
    assert _extract_inputs({"symbol": "BTCUSDT"}) == {"symbol": "BTCUSDT"}
    
    # Nested payload
    assert _extract_inputs({"payload": {"symbol": "ETHUSDT"}}) == {"symbol": "ETHUSDT"}
    
    # Nested data
    assert _extract_inputs({"data": {"symbol": "SOLUSDT"}}) == {"symbol": "SOLUSDT"}


def test_mk_runtime():
    """Test runtime stub construction"""
    inp = {"symbol": "BTCUSDT", "micro_tf": "1s"}
    rt = _mk_runtime(inp)
    assert rt.symbol == "BTCUSDT"
    assert rt.config.get("micro_tf") == "1s"
    
    inp2 = {"symbol": "ETHUSDT", "runtime_config": {"micro_tf": "5s"}}
    rt2 = _mk_runtime(inp2)
    assert rt2.symbol == "ETHUSDT"
    assert rt2.config.get("micro_tf") == "5s"


def test_key():
    """Test key generation for deduplication"""
    inp = {"symbol": "BTCUSDT", "tick_ts_ms": 1000, "direction": "LONG", "tf": "1s"}
    k = _key(inp)
    assert k == "BTCUSDT|1000|LONG|1s"
    
    inp2 = {"symbol": "ethusdt", "ts_ms": 2000, "direction": "SHORT", "micro_tf": "5s"}
    k2 = _key(inp2)
    assert k2 == "ETHUSDT|2000|SHORT|5s"


def test_ofc_to_dict():
    """Test OFConfirm to dict conversion"""
    # None case
    assert _ofc_to_dict(None) == {}
    
    # Dict case
    assert _ofc_to_dict({"ok": 1, "score": 0.5}) == {"ok": 1, "score": 0.5}
    
    # Object with to_dict
    obj = SimpleNamespace(ok=1, score=0.5, to_dict=lambda: {"ok": 1, "score": 0.5})
    assert _ofc_to_dict(obj) == {"ok": 1, "score": 0.5}


def test_evidence():
    """Test evidence extraction"""
    obj = SimpleNamespace(evidence={"scenario_v4": "reversal", "ok_soft": 1})
    ev = _evidence(obj)
    assert ev == {"scenario_v4": "reversal", "ok_soft": 1}
    
    assert _evidence(None) == {}


def test_replay_one_smoke():
    """Smoke test for replay_one with minimal valid input"""
    engine = OFConfirmEngine()
    
    inp = {
        "symbol": "BTCUSDT",
        "tick_ts_ms": 1700000000000,
        "direction": "LONG",
        "tf": "1s",
        "price": 50000.0,
        "delta_z": 2.5,
        "indicators": {
            "book_health_ok": 1,
            "spread_bps": 2.0,
        },
        "cfg": {},
    }
    
    out, dbg = replay_one(engine, inp)
    
    assert "k" in out
    assert out["symbol"] == "BTCUSDT"
    assert out["tick_ts_ms"] == 1700000000000
    assert "ok" in out
    assert "latency_us" in out
    assert "inputs" in dbg
    assert "ofc" in dbg


def test_load_ndjson():
    """Test NDJSON loading"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".ndjson", delete=False) as f:
        f.write(json.dumps({"k": "BTCUSDT|1000|LONG|1s", "ok": 1}) + "\n")
        f.write(json.dumps({"k": "ETHUSDT|2000|SHORT|1s", "ok": 0}) + "\n")
        fpath = f.name
    
    try:
        data = _load_ndjson(fpath)
        assert len(data) == 2
        assert "BTCUSDT|1000|LONG|1s" in data
        assert data["BTCUSDT|1000|LONG|1s"]["ok"] == 1
    finally:
        Path(fpath).unlink(missing_ok=True)


def test_group_key():
    """Test group key generation for diff report"""
    row = {"symbol": "BTCUSDT", "scenario_v4": "reversal"}
    assert _group_key(row) == "BTCUSDT|reversal"
    
    row2 = {"symbol": "ETHUSDT", "scenario_v4": ""}
    assert _group_key(row2) == "ETHUSDT|"


def test_safe_loads_maybe():
    """Test safe loads with various input types"""
    # None
    assert _safe_loads_maybe(None) is None
    
    # Dict
    assert _safe_loads_maybe({"a": 1}) == {"a": 1}
    
    # String JSON
    assert _safe_loads_maybe('{"b": 2}') == {"b": 2}
    
    # Bytes
    assert _safe_loads_maybe(b'{"c": 3}') == {"c": 3}
    
    # Invalid
    assert _safe_loads_maybe("invalid") is None
    assert _safe_loads_maybe("") is None
    assert _safe_loads_maybe(123) is None


def test_extract_inputs_advanced():
    """Test input extraction with various Redis/XADD shapes"""
    # Wrapper with JSON string payload
    raw1 = {"payload": '{"symbol": "BTCUSDT", "price": 50000}', "ts_ms": 1000}
    inp1 = _extract_inputs(raw1)
    assert inp1["symbol"] == "BTCUSDT"
    assert inp1["price"] == 50000
    assert inp1["ts_ms"] == 1000  # meta fills missing
    
    # Wrapper with dict payload
    raw2 = {"payload": {"symbol": "ETHUSDT", "price": 3000}, "direction": "LONG"}
    inp2 = _extract_inputs(raw2)
    assert inp2["symbol"] == "ETHUSDT"
    assert inp2["price"] == 3000
    assert inp2["direction"] == "LONG"
    
    # Data wrapper
    raw3 = {"data": {"symbol": "SOLUSDT"}, "tf": "1s"}
    inp3 = _extract_inputs(raw3)
    assert inp3["symbol"] == "SOLUSDT"
    assert inp3["tf"] == "1s"
    
    # Bytes payload
    raw4 = {"payload": b'{"symbol": "ADAUSDT"}'}
    inp4 = _extract_inputs(raw4)
    assert inp4["symbol"] == "ADAUSDT"


def test_key_with_signal_id():
    """Test key generation with signal_id for collision avoidance"""
    inp1 = {"symbol": "BTCUSDT", "tick_ts_ms": 1000, "direction": "LONG", "tf": "1s", "sid": "sig123"}
    k1 = _key(inp1)
    assert "sig123" in k1
    assert k1 == "BTCUSDT|1000|LONG|1s|sig123"
    
    inp2 = {"symbol": "BTCUSDT", "tick_ts_ms": 1000, "direction": "LONG", "tf": "1s", "signal_id": "sig456"}
    k2 = _key(inp2)
    assert "sig456" in k2
    
    inp3 = {"symbol": "BTCUSDT", "tick_ts_ms": 1000, "direction": "LONG", "tf": "1s"}
    k3 = _key(inp3)
    assert k3 == "BTCUSDT|1000|LONG|1s"  # no sid


def test_to_int():
    """Test integer conversion"""
    assert _to_int(42) == 42
    assert _to_int(42.7) == 42
    assert _to_int("42") == 42
    assert _to_int(None) == 0
    assert _to_int(True) == 1
    assert _to_int(False) == 0
    assert _to_int("invalid", 99) == 99


def test_to_float():
    """Test float conversion"""
    assert _to_float(42.5) == 42.5
    assert _to_float(42) == 42.0
    assert _to_float("42.5") == 42.5
    assert _to_float(None) == 0.0
    assert _to_float(True) == 1.0
    assert _to_float("invalid", 99.0) == 99.0


def test_norm_direction():
    """Test direction normalization"""
    assert _norm_direction({"direction": "LONG"}) == "LONG"
    assert _norm_direction({"dir": "SHORT"}) == "SHORT"
    assert _norm_direction({"side": "LONG"}) == "LONG"
    assert _norm_direction({"direction": "LONG", "dir": "SHORT"}) == "LONG"  # direction wins
    assert _norm_direction({}) == ""


def test_norm_tick_ts_ms():
    """Test timestamp normalization"""
    assert _norm_tick_ts_ms({"tick_ts_ms": 1000}) == 1000
    assert _norm_tick_ts_ms({"ts_ms": 2000}) == 2000
    assert _norm_tick_ts_ms({"tick_ts": 3000}) == 3000
    assert _norm_tick_ts_ms({"timestamp_ms": 4000}) == 4000
    assert _norm_tick_ts_ms({"tick_ts_ms": "5000"}) == 5000
    assert _norm_tick_ts_ms({}) == 0


def test_norm_tf():
    """Test timeframe normalization"""
    runtime = SimpleNamespace(config={"micro_tf": "5s"})
    
    assert _norm_tf({"tf": "1s"}, runtime) == "1s"
    assert _norm_tf({"timeframe": "3s"}, runtime) == "3s"
    assert _norm_tf({"micro_tf": "2s"}, runtime) == "2s"
    assert _norm_tf({}, runtime) == "5s"  # from runtime
    assert _norm_tf({}, SimpleNamespace(config={})) == "1s"  # default


def test_norm_price():
    """Test price normalization with strict order"""
    assert _norm_price({"price": 50000.0}) == 50000.0
    assert _norm_price({"last_price": 51000.0}) == 51000.0
    assert _norm_price({"mid_price": 50500.0}) == 50500.0
    assert _norm_price({"mid": 50250.0}) == 50250.0
    assert _norm_price({"px": 50100.0}) == 50100.0
    # price wins if multiple present
    assert _norm_price({"price": 50000.0, "last_price": 51000.0}) == 50000.0
    assert _norm_price({}) == 0.0


def test_norm_delta_z():
    """Test delta_z normalization"""
    assert _norm_delta_z({"delta_z": 2.5}) == 2.5
    assert _norm_delta_z({"delta_z_used": 3.0}) == 3.0
    assert _norm_delta_z({"deltaZ": 2.7}) == 2.7
    assert _norm_delta_z({"delta_zscore": 2.8}) == 2.8
    assert _norm_delta_z({"delta_z_spike": 2.9}) == 2.9
    assert _norm_delta_z({}) == 0.0


def test_replay_one_determinism():
    """Test that replay_one produces deterministic results with deepcopy"""
    from core.of_confirm_engine import OFConfirmEngine
    
    engine = OFConfirmEngine()
    
    # Input with mutable structures
    indicators = {"book_health_ok": 1, "spread_bps": 2.0}
    absorption = {"level": 100, "delta": 50}
    
    inp = {
        "symbol": "BTCUSDT",
        "tick_ts_ms": 1700000000000,
        "direction": "LONG",
        "tf": "1s",
        "price": 50000.0,
        "delta_z": 2.5,
        "indicators": indicators,
        "absorption": absorption,
        "cfg2": {"test": True},
    }
    
    # First replay
    out1, dbg1 = replay_one(engine, inp)
    
    # Verify indicators/absorption weren't mutated
    assert indicators == {"book_health_ok": 1, "spread_bps": 2.0}
    assert absorption == {"level": 100, "delta": 50}
    
    # Second replay should produce same result
    out2, dbg2 = replay_one(engine, inp)
    
    # Key fields should match
    assert out1["k"] == out2["k"]
    assert out1["symbol"] == out2["symbol"]
    assert out1["tick_ts_ms"] == out2["tick_ts_ms"]
    assert out1["ok"] == out2["ok"]
    
    # Normalized fields in debug should match
    assert dbg1["normalized"]["symbol"] == dbg2["normalized"]["symbol"]
    assert dbg1["normalized"]["tf"] == dbg2["normalized"]["tf"]
    assert dbg1["normalized"]["direction"] == dbg2["normalized"]["direction"]


def test_mk_runtime_determinism():
    """Test runtime construction preserves structure deterministically"""
    inp1 = {
        "symbol": "BTCUSDT",
        "runtime": {"extra_field": "value", "config": {"micro_tf": "1s"}},
        "runtime_config": {"test": True},
    }
    rt1 = _mk_runtime(inp1)
    assert rt1.symbol == "BTCUSDT"
    assert rt1.config.get("test") is True
    assert rt1.config.get("micro_tf") == "1s"
    assert hasattr(rt1, "extra_field")
    
    # Without runtime dict
    inp2 = {"symbol": "ETHUSDT", "micro_tf": "5s"}
    rt2 = _mk_runtime(inp2)
    assert rt2.symbol == "ETHUSDT"
    assert rt2.config.get("micro_tf") == "5s"


def test_cfg2_priority():
    """Test that cfg2 takes priority over cfg"""
    from core.of_confirm_engine import OFConfirmEngine
    
    engine = OFConfirmEngine()
    
    inp = {
        "symbol": "BTCUSDT",
        "tick_ts_ms": 1700000000000,
        "direction": "LONG",
        "tf": "1s",
        "price": 50000.0,
        "delta_z": 2.5,
        "cfg": {"old": True},
        "cfg2": {"new": True},
    }
    
    out, dbg = replay_one(engine, inp)
    
    # Verify cfg2 was used (check in debug if available)
    assert "inputs_used" in dbg
    # cfg2 should be in inputs_used, not cfg
    if "cfg" in dbg["inputs_used"]:
        assert dbg["inputs_used"]["cfg"].get("new") is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

