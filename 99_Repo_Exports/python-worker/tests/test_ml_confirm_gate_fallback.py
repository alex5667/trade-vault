"""
Тесты для fallback механизма в MLConfirmGate (hash cfg:ml_confirm fallback).
"""

from __future__ import annotations

import json
import os
from unittest.mock import MagicMock, patch

import pytest

from services.ml_confirm_gate import MLConfirmGate


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


def test_fallback_to_hash_when_champion_missing(gate, mock_redis):
    """Test that gate falls back to hash cfg:ml_confirm when champion is missing."""
    # Champion and challenger are missing
    mock_redis.get.return_value = None
    
    # Hash cfg exists
    hash_cfg = {
        "mode": "SHADOW",
        "fail_policy": "OPEN",
        "enforce_share": "0.1",
        "kind": "util_mh_v1",
    }
    mock_redis.hgetall.return_value = hash_cfg
    
    # Force cache refresh
    gate._cache_loaded_ms = 0
    gate._refresh_cache_if_needed()
    
    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=1000,
        direction="LONG",
        scenario="range_meanrev",
        indicators={"spread_bps": 1.0, "expected_slippage_bps": 0.5},
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )
    
    # Should not be ERR_NO_CFG
    assert dec.status != "ERR_NO_CFG"
    assert gate._cfg_source == "hash_fallback"
    
    # Should have attempted to write through to champion
    mock_redis.set.assert_any_call("cfg:ml_confirm:champion", json.dumps(hash_cfg, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def test_champion_takes_precedence_over_hash(gate, mock_redis):
    """Test that champion JSON takes precedence over hash fallback."""
    # Champion exists
    champion_cfg = {
        "mode": "SHADOW",
        "fail_policy": "OPEN",
        "enforce_share": 0.2,
        "kind": "util_mh_v1",
    }
    mock_redis.get.return_value = json.dumps(champion_cfg)
    
    # Hash also exists (should be ignored)
    hash_cfg = {
        "mode": "ENFORCE",
        "fail_policy": "CLOSED",
        "enforce_share": "0.1",
    }
    mock_redis.hgetall.return_value = hash_cfg
    
    # Force cache refresh
    gate._cache_loaded_ms = 0
    
    gate._refresh_cache_if_needed()
    
    # Should use champion, not hash
    assert gate._cfg_source == "champion"
    assert gate._cfg.get("enforce_share") == 0.2


def test_challenger_in_shadow_mode(gate, mock_redis):
    """Test that challenger is used in SHADOW mode when champion is missing."""
    # Champion missing
    mock_redis.get.side_effect = [None, json.dumps({
        "mode": "SHADOW",
        "fail_policy": "OPEN",
        "enforce_share": 0.15,
        "kind": "util_mh_v1",
    })]
    
    # Force cache refresh
    gate.ab_variant = "challenger"
    gate._champion_key = "cfg:ml_confirm:challenger"
    gate._cache_loaded_ms = 0
    
    gate._refresh_cache_if_needed()
    
    # Should use challenger
    assert gate._cfg_source == "challenger"
    assert gate._cfg.get("enforce_share") == 0.15


def test_coerce_hash_cfg_adds_defaults(gate):
    """Test that _coerce_hash_cfg adds required defaults."""
    hash_data = {
        "kind": "util_mh_v1",
        "model_path": "/path/to/model",
    }
    
    cfg = gate._coerce_hash_cfg(hash_data)
    
    # Should have defaults
    assert cfg["mode"] == "SHADOW"
    assert cfg["fail_policy"] == "OPEN"
    assert cfg["enforce_share"] == 0.05
    
    # Should preserve original
    assert cfg["kind"] == "util_mh_v1"
    assert cfg["model_path"] == "/path/to/model"


def test_cfg_source_in_metrics(gate, mock_redis):
    """Test that cfg_source is included in metrics."""
    # Use hash fallback
    mock_redis.get.return_value = None
    mock_redis.hgetall.return_value = {
        "mode": "SHADOW",
        "fail_policy": "OPEN",
        "enforce_share": "0.1",
    }
    
    # Force cache refresh
    gate._cache_loaded_ms = 0
    gate._refresh_cache_if_needed()
    
    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=1000,
        direction="LONG",
        scenario="range_meanrev",
        indicators={"spread_bps": 1.0, "expected_slippage_bps": 0.5},
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )
    
    # Check that xadd was called with cfg_source
    assert mock_redis.xadd.called
    call_args = mock_redis.xadd.call_args
    metrics = call_args[0][1]
    assert "cfg_source" in metrics
    assert metrics["cfg_source"] == "hash_fallback"


def test_no_cfg_no_hash_results_in_err_no_cfg(gate, mock_redis):
    """Test that ERR_NO_CFG is returned when both champion and hash are missing."""
    # Both champion and hash are missing
    mock_redis.get.return_value = None
    mock_redis.hgetall.return_value = {}
    
    # Force cache refresh
    gate._cache_loaded_ms = 0
    gate._refresh_cache_if_needed()
    
    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=1000,
        direction="LONG",
        scenario="range_meanrev",
        indicators={"spread_bps": 1.0, "expected_slippage_bps": 0.5},
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )
    
    # Should be ERR_NO_CFG
    assert dec.status == "ERR_NO_CFG"
    assert gate._cfg_source == "none"


def test_empty_champion_string_falls_back_to_hash(gate, mock_redis):
    """Test that empty champion string falls back to hash."""
    # Champion exists but is empty string
    mock_redis.get.return_value = ""
    mock_redis.hgetall.return_value = {
        "mode": "SHADOW",
        "fail_policy": "OPEN",
        "enforce_share": "0.1",
        "kind": "util_mh_v1",
    }
    
    gate._cache_loaded_ms = 0
    
    gate._refresh_cache_if_needed()
    
    # Should use hash fallback
    assert gate._cfg_source == "hash_fallback"
    assert gate._cfg.get("mode") == "SHADOW"
    assert gate._cfg.get("enforce_share") == "0.1"  # String preserved from hash (parsing happens downstream)
    
    # Should bootstrap champion
    mock_redis.set.assert_any_call("cfg:ml_confirm:champion", json.dumps(mock_redis.hgetall.return_value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def test_invalid_json_champion_falls_back_to_hash(gate, mock_redis):
    """Test that invalid JSON in champion falls back to hash."""
    # Champion exists but has invalid JSON
    mock_redis.get.return_value = "{invalid json}"
    mock_redis.hgetall.return_value = {
        "mode": "SHADOW",
        "fail_policy": "OPEN",
        "enforce_share": "0.1",
        "kind": "util_mh_v1",
    }
    
    gate._cache_loaded_ms = 0
    
    gate._refresh_cache_if_needed()
    
    # Should use hash fallback
    assert gate._cfg_source == "hash_fallback"
    assert gate._cfg.get("mode") == "SHADOW"
    
    # Should bootstrap champion
    assert mock_redis.set.called


def test_whitespace_only_champion_falls_back_to_hash(gate, mock_redis):
    """Test that whitespace-only champion falls back to hash."""
    # Champion exists but is only whitespace
    mock_redis.get.return_value = "   \n\t  "
    mock_redis.hgetall.return_value = {
        "mode": "SHADOW",
        "fail_policy": "OPEN",
        "enforce_share": "0.1",
        "kind": "util_mh_v1",
    }
    
    gate._cache_loaded_ms = 0
    
    gate._refresh_cache_if_needed()
    
    # Should use hash fallback
    assert gate._cfg_source == "hash_fallback"
    assert gate._cfg.get("mode") == "SHADOW"


def test_empty_dict_champion_falls_back_to_hash(gate, mock_redis):
    """Test that empty dict in champion JSON falls back to hash."""
    # Champion exists but JSON is empty dict
    mock_redis.get.return_value = "{}"
    mock_redis.hgetall.return_value = {
        "mode": "SHADOW",
        "fail_policy": "OPEN",
        "enforce_share": "0.1",
        "kind": "util_mh_v1",
    }
    
    gate._cache_loaded_ms = 0
    
    gate._refresh_cache_if_needed()
    
    # Should use hash fallback
    assert gate._cfg_source == "hash_fallback"
    assert gate._cfg.get("mode") == "SHADOW"


def test_bootstrap_creates_valid_json(gate, mock_redis):
    """Test that bootstrap creates valid JSON that can be read back."""
    # Champion missing, hash exists
    mock_redis.get.return_value = None
    hash_cfg = {
        "mode": "SHADOW",
        "fail_policy": "OPEN",
        "enforce_share": "0.1",
        "kind": "util_mh_v1",
        "model_path": "/path/to/model.joblib",
    }
    mock_redis.hgetall.return_value = hash_cfg
    
    gate._cache_loaded_ms = 0
    
    gate._refresh_cache_if_needed()
    
    # Should bootstrap champion
    bootstrapped_json = json.dumps(hash_cfg, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    mock_redis.set.assert_any_call("cfg:ml_confirm:champion", bootstrapped_json)
    
    # Verify the bootstrapped JSON is valid
    parsed = json.loads(bootstrapped_json)
    assert isinstance(parsed, dict)
    assert parsed.get("mode") == "SHADOW"
    assert parsed.get("fail_policy") == "OPEN"


def test_hash_fallback_preserves_string_values(gate, mock_redis):
    """Test that hash fallback preserves string values from HGETALL."""
    # Hash contains string values (as HGETALL returns)
    mock_redis.get.return_value = None
    hash_cfg = {
        "mode": "SHADOW",
        "fail_policy": "OPEN",
        "enforce_share": "0.15",  # String, not float
        "kind": "util_mh_v1",
    }
    mock_redis.hgetall.return_value = hash_cfg
    
    gate._cache_loaded_ms = 0
    
    gate._refresh_cache_if_needed()
    
    # Should preserve string values (parsing happens downstream)
    assert gate._cfg.get("enforce_share") == "0.15"  # Still string
    assert gate._cfg.get("mode") == "SHADOW"
    assert gate._cfg.get("kind") == "util_mh_v1"


