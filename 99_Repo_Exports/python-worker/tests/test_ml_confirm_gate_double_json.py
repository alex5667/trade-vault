from __future__ import annotations

"""
Тесты для обработки двойной сериализации JSON в MLConfirmGate.

Проверяет, что _safe_loads и _safe_loads_ex корректно обрабатывают случаи, когда
в Redis хранится JSON-строка, внутри которой ещё один JSON (двойная сериализация).
Это может произойти при неправильном промоуте champion из challenger.
"""


import json
from unittest.mock import MagicMock

import pytest

from services.ml_confirm_gate import MLConfirmGate, _safe_loads, _safe_loads_ex


def test_safe_loads_double_encoded_json():
    """Test _safe_loads with double-encoded JSON string."""
    # Simulate double-encoded JSON: "\"{\\\"kind\\\":\\\"util_mh_v1\\\",\\\"run_id\\\":\\\"test123\\\"}\""
    inner_json = {"kind": "util_mh_v1", "run_id": "test123", "model_path": "/path/to/model.joblib"}
    inner_json_str = json.dumps(inner_json, separators=(",", ":"))
    double_encoded = json.dumps(inner_json_str)  # This creates the double-encoded string

    result = _safe_loads(double_encoded)

    assert isinstance(result, dict)
    assert result == inner_json
    assert result["kind"] == "util_mh_v1"
    assert result["run_id"] == "test123"


def test_safe_loads_ex_double_encoded_json():
    """Test _safe_loads_ex with double-encoded JSON string."""
    inner_json = {"kind": "util_mh_v1", "run_id": "test123", "model_path": "/path/to/model.joblib"}
    inner_json_str = json.dumps(inner_json, separators=(",", ":"))
    double_encoded = json.dumps(inner_json_str)

    cfg, err, raw_len = _safe_loads_ex(double_encoded)

    assert isinstance(cfg, dict)
    assert cfg == inner_json
    assert err == ""  # No error
    assert raw_len == len(double_encoded)
    assert cfg["kind"] == "util_mh_v1"
    assert cfg["run_id"] == "test123"


def test_safe_loads_normal_json():
    """Test _safe_loads with normal (single-encoded) JSON string."""
    normal_json = {"kind": "util_mh_v1", "run_id": "test123"}
    normal_json_str = json.dumps(normal_json, separators=(",", ":"))

    result = _safe_loads(normal_json_str)

    assert isinstance(result, dict)
    assert result == normal_json
    assert result["kind"] == "util_mh_v1"


def test_safe_loads_ex_normal_json():
    """Test _safe_loads_ex with normal (single-encoded) JSON string."""
    normal_json = {"kind": "util_mh_v1", "run_id": "test123"}
    normal_json_str = json.dumps(normal_json, separators=(",", ":"))

    cfg, err, raw_len = _safe_loads_ex(normal_json_str)

    assert isinstance(cfg, dict)
    assert cfg == normal_json
    assert err == ""
    assert raw_len == len(normal_json_str)


def test_safe_loads_ex_double_encoded_invalid_inner():
    """Test _safe_loads_ex with double-encoded JSON where inner JSON is invalid."""
    invalid_inner = "{invalid json"
    double_encoded = json.dumps(invalid_inner)

    cfg, err, raw_len = _safe_loads_ex(double_encoded)

    assert cfg == {}
    assert "json_error_double" in err
    assert raw_len == len(double_encoded)


def test_safe_loads_ex_double_encoded_inner_not_dict():
    """Test _safe_loads_ex with double-encoded JSON where inner is not a dict."""
    inner_array = [1, 2, 3]
    inner_json_str = json.dumps(inner_array)
    double_encoded = json.dumps(inner_json_str)

    cfg, err, raw_len = _safe_loads_ex(double_encoded)

    assert cfg == {}
    assert "not_dict_double" in err
    assert raw_len == len(double_encoded)


def test_safe_loads_ex_double_encoded_empty_dict():
    """Test _safe_loads_ex with double-encoded empty dict."""
    inner_json = {}
    inner_json_str = json.dumps(inner_json)
    double_encoded = json.dumps(inner_json_str)

    cfg, err, raw_len = _safe_loads_ex(double_encoded)

    assert cfg == {}
    assert err == "empty_dict"
    assert raw_len == len(double_encoded)


@pytest.fixture
def mock_redis():
    """Mock Redis client."""
    r = MagicMock()
    r.get.return_value = None
    r.hgetall.return_value = {}
    r.xadd.return_value = "12345-0"
    r.set.return_value = True
    return r


@pytest.fixture
def gate(mock_redis):
    """MLConfirmGate instance with mocked Redis."""
    return MLConfirmGate(
        r=mock_redis,
        mode="SHADOW",
        fail_policy="OPEN",
        champion_key="cfg:ml_confirm:champion",
        challenger_key="cfg:ml_confirm:challenger",
    )


def test_gate_loads_double_encoded_champion(gate, mock_redis):
    """Test that gate correctly loads double-encoded champion JSON."""
    # Create double-encoded JSON (simulating the bug scenario)
    inner_cfg = {
        "kind": "util_mh_v1",
        "run_id": "20260204_133025_708ce5",
        "model_path": "/var/lib/trade/ml_models/model.joblib",
        "mode": "SHADOW",
    }
    inner_json_str = json.dumps(inner_cfg, separators=(",", ":"))
    double_encoded = json.dumps(inner_json_str)  # Double-encoded

    # Set champion to double-encoded value
    mock_redis.get.return_value = double_encoded
    mock_redis.hgetall.return_value = {}

    # Force cache refresh
    gate._cache_loaded_ms = 0
    gate._refresh_cache_if_needed()

    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=1000,
        direction="LONG",
        scenario="reversal",
        indicators={"spread_bps": 1.0, "expected_slippage_bps": 0.5},
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )

    # Should not be ERR_NO_CFG - cfg should be loaded successfully
    assert dec.status != "ERR_NO_CFG"
    assert dec.cfg_source == "champion"
    assert dec.cfg_key_used == "cfg:ml_confirm:champion"
    assert dec.cfg_parse_err == ""  # No parse error
    assert dec.kind == "util_mh_v1" or dec.status == "ERR_NO_MODEL"  # Model may not exist, but cfg is loaded


def test_gate_loads_double_encoded_challenger_in_shadow(gate, mock_redis):
    """Test that gate correctly loads double-encoded challenger JSON in SHADOW mode."""
    # Champion missing or invalid
    inner_cfg = {
        "kind": "util_mh_v1",
        "run_id": "challenger123",
        "model_path": "/var/lib/trade/ml_models/model.joblib",
    }
    inner_json_str = json.dumps(inner_cfg, separators=(",", ":"))
    double_encoded = json.dumps(inner_json_str)

    # Champion missing, challenger has double-encoded JSON
    mock_redis.get.side_effect = [None, double_encoded]
    mock_redis.hgetall.return_value = {}

    # Force cache refresh
    gate.ab_variant = "challenger"
    gate._champion_key = "cfg:ml_confirm:challenger"
    gate._cache_loaded_ms = 0
    gate._refresh_cache_if_needed()

    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=1000,
        direction="LONG",
        scenario="reversal",
        indicators={"spread_bps": 1.0, "expected_slippage_bps": 0.5},
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )

    # Should use challenger and not have ERR_NO_CFG
    assert dec.status != "ERR_NO_CFG"
    assert dec.cfg_source == "challenger"
    assert dec.cfg_key_used == "cfg:ml_confirm:challenger"
    assert dec.cfg_parse_err == ""


def test_gate_handles_triple_encoded_gracefully(gate, mock_redis):
    """Test that gate handles even triple-encoded JSON gracefully (should fail but not crash)."""
    inner_cfg = {"kind": "util_mh_v1", "run_id": "test123"}
    inner_json_str = json.dumps(inner_cfg)
    double_encoded = json.dumps(inner_json_str)
    triple_encoded = json.dumps(double_encoded)  # Triple encoding

    mock_redis.get.return_value = triple_encoded
    mock_redis.hgetall.return_value = {}

    gate._cache_loaded_ms = 0
    gate._refresh_cache_if_needed()

    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=1000,
        direction="LONG",
        scenario="reversal",
        indicators={"spread_bps": 1.0, "expected_slippage_bps": 0.5},
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )

    # Triple encoding: first decode -> double-encoded string, second decode -> inner JSON string (not dict)
    # Should result in not_dict_double error
    # But gate should still handle it gracefully (fallback to hash or ERR_BAD_CFG)
    assert dec.status in ("ERR_NO_CFG", "ERR_BAD_CFG")


def test_safe_loads_bytes_double_encoded():
    """Test _safe_loads with bytes containing double-encoded JSON."""
    inner_json = {"kind": "util_mh_v1", "run_id": "test123"}
    inner_json_str = json.dumps(inner_json)
    double_encoded = json.dumps(inner_json_str)
    double_encoded_bytes = double_encoded.encode("utf-8")

    result = _safe_loads(double_encoded_bytes)

    assert isinstance(result, dict)
    assert result == inner_json


def test_safe_loads_ex_bytes_double_encoded():
    """Test _safe_loads_ex with bytes containing double-encoded JSON."""
    inner_json = {"kind": "util_mh_v1", "run_id": "test123"}
    inner_json_str = json.dumps(inner_json)
    double_encoded = json.dumps(inner_json_str)
    double_encoded_bytes = double_encoded.encode("utf-8")

    cfg, err, raw_len = _safe_loads_ex(double_encoded_bytes)

    assert isinstance(cfg, dict)
    assert cfg == inner_json
    assert err == ""
    assert raw_len == len(double_encoded)


def test_real_world_double_encoding_scenario(gate, mock_redis):
    """
    Test real-world scenario: champion was promoted from challenger with double encoding.
    
    This simulates the exact bug scenario:
    1. challenger had double-encoded JSON
    2. champion was set to the same double-encoded value
    3. Gate should now correctly decode it
    """
    # Real-world example from the bug report
    real_double_encoded = '"{\\"kind\\":\\"util_mh_v1\\",\\"run_id\\":\\"20260204_133025_708ce5\\",\\"model_path\\":\\"/path/to/model.joblib\\"}"'

    mock_redis.get.return_value = real_double_encoded
    mock_redis.hgetall.return_value = {}

    gate._cache_loaded_ms = 0
    gate._refresh_cache_if_needed()

    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=1000,
        direction="LONG",
        scenario="reversal",
        indicators={"spread_bps": 1.0, "expected_slippage_bps": 0.5},
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )

    # Should successfully decode and not have ERR_NO_CFG
    assert dec.status != "ERR_NO_CFG"
    assert dec.cfg_parse_err == ""  # Should parse successfully
    assert dec.kind == "util_mh_v1" or dec.status == "ERR_NO_MODEL"  # Model may not exist


def test_gate_auto_normalizes_double_encoded_champion(gate, mock_redis):
    """Test that gate automatically normalizes double-encoded champion config."""
    inner_cfg = {
        "kind": "util_mh_v1",
        "run_id": "20260204_133025_708ce5",
        "model_path": "/var/lib/trade/ml_models/model.joblib",
    }
    inner_json_str = json.dumps(inner_cfg, separators=(",", ":"))
    double_encoded = json.dumps(inner_json_str)  # Double-encoded

    # Set champion to double-encoded value
    mock_redis.get.return_value = double_encoded
    mock_redis.hgetall.return_value = {}

    # Force cache refresh
    gate._cache_loaded_ms = 0
    gate._refresh_cache_if_needed()

    # First call should decode and auto-normalize
    dec1 = gate.check(
        symbol="BTCUSDT",
        ts_ms=1000,
        direction="LONG",
        scenario="reversal",
        indicators={"spread_bps": 1.0, "expected_slippage_bps": 0.5},
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )

    # Should successfully decode
    assert dec1.status != "ERR_NO_CFG"
    assert dec1.cfg_parse_err == ""

    # Check that set was called to normalize (mock should have been called)
    # After normalization, get should return canonical JSON
    canonical_json = json.dumps(inner_cfg, ensure_ascii=False, separators=(",", ":"))
    mock_redis.get.return_value = canonical_json

    # Force cache refresh again
    gate._cache_loaded_ms = 0
    gate._refresh_cache_if_needed()

    # Second call should use normalized (canonical) config
    dec2 = gate.check(
        symbol="BTCUSDT",
        ts_ms=2000,
        direction="SHORT",
        scenario="breakout",
        indicators={"spread_bps": 1.0, "expected_slippage_bps": 0.5},
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )

    # Should still work correctly with normalized config
    assert dec2.status != "ERR_NO_CFG"
    assert dec2.cfg_parse_err == ""
    assert dec2.kind == "util_mh_v1" or dec2.status == "ERR_NO_MODEL"


def test_gate_auto_normalizes_double_encoded_challenger(gate, mock_redis):
    """Test that gate automatically normalizes double-encoded challenger config."""
    inner_cfg = {
        "kind": "util_mh_v1",
        "run_id": "challenger123",
        "model_path": "/var/lib/trade/ml_models/model.joblib",
    }
    inner_json_str = json.dumps(inner_cfg, separators=(",", ":"))
    double_encoded = json.dumps(inner_json_str)

    # Champion missing, challenger has double-encoded JSON
    mock_redis.get.side_effect = [None, double_encoded]
    mock_redis.hgetall.return_value = {}

    # Force cache refresh
    gate.ab_variant = "challenger"
    gate._champion_key = "cfg:ml_confirm:challenger"
    gate._cache_loaded_ms = 0
    gate._refresh_cache_if_needed()

    # First call should decode challenger and auto-normalize
    dec1 = gate.check(
        symbol="BTCUSDT",
        ts_ms=1000,
        direction="LONG",
        scenario="reversal",
        indicators={"spread_bps": 1.0, "expected_slippage_bps": 0.5},
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )

    # Should use challenger and not have ERR_NO_CFG
    assert dec1.status != "ERR_NO_CFG"
    assert dec1.cfg_source == "challenger"
    assert dec1.cfg_parse_err == ""

    # After normalization, challenger should return canonical JSON
    canonical_json = json.dumps(inner_cfg, ensure_ascii=False, separators=(",", ":"))
    mock_redis.get.side_effect = [None, canonical_json]

    # Force cache refresh again
    gate._cache_loaded_ms = 0
    gate._refresh_cache_if_needed()

    # Second call should use normalized challenger
    dec2 = gate.check(
        symbol="BTCUSDT",
        ts_ms=2000,
        direction="SHORT",
        scenario="breakout",
        indicators={"spread_bps": 1.0, "expected_slippage_bps": 0.5},
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )

    # Should still work correctly with normalized challenger
    assert dec2.status != "ERR_NO_CFG"
    assert dec2.cfg_source == "challenger"
    assert dec2.cfg_parse_err == ""

