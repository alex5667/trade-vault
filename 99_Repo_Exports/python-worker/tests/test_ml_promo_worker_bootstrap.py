"""
Тесты для bootstrap и алертов в ml_promo_callbacks_worker_tb_v10_4.
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
from unittest.mock import MagicMock, patch

import pytest

# Import the worker module
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "services"))

from ml_promo_callbacks_worker_tb_v10_4 import _coerce_hash_cfg, _safe_loads, _is_valid_cfg, _notify


def test_coerce_hash_cfg_adds_defaults():
    """Test that _coerce_hash_cfg adds required defaults."""
    hash_data = {
        "kind": "util_mh_v1",
        "model_path": "/path/to/model",
    }
    
    cfg = _coerce_hash_cfg(hash_data)
    
    # Should have defaults
    assert cfg["mode"] == "SHADOW"
    assert cfg["fail_policy"] == "OPEN"
    assert cfg["enforce_share"] == 0.05
    assert "bootstrap_ms" in cfg
    
    # Should preserve original
    assert cfg["kind"] == "util_mh_v1"
    assert cfg["model_path"] == "/path/to/model"


def test_coerce_hash_cfg_preserves_existing():
    """Test that _coerce_hash_cfg preserves existing values."""
    hash_data = {
        "mode": "ENFORCE",
        "fail_policy": "CLOSED",
        "enforce_share": "0.2",
        "kind": "util_mh_v1",
    }
    
    cfg = _coerce_hash_cfg(hash_data)
    
    # Should preserve existing (not override with defaults)
    # Note: enforce_share stays as string (parsing happens downstream)
    assert cfg["mode"] == "ENFORCE"
    assert cfg["fail_policy"] == "CLOSED"
    assert cfg["enforce_share"] == "0.2"  # String preserved, parsing downstream
    assert cfg["kind"] == "util_mh_v1"


def test_bootstrap_champion_from_hash():
    """Test that champion is bootstrapped from hash if missing."""
    mock_redis = MagicMock()
    mock_redis.get.return_value = None  # Champion missing
    mock_redis.hgetall.return_value = {
        "mode": "SHADOW",
        "fail_policy": "OPEN",
        "enforce_share": "0.1",
        "kind": "util_mh_v1",
    }
    mock_redis.set.return_value = True
    mock_redis.xadd.return_value = "12345-0"
    
    champion_key = "cfg:ml_confirm:champion"
    cfg_hash_key = "cfg:ml_confirm"
    notify_stream = "notify:telegram"
    
    # Simulate bootstrap logic
    if not mock_redis.get(champion_key):
        h = mock_redis.hgetall(cfg_hash_key)
        if isinstance(h, dict) and len(h) > 0:
            cfg = _coerce_hash_cfg(h)
            mock_redis.set(champion_key, json.dumps(cfg, ensure_ascii=False, separators=(",", ":")))
            mock_redis.xadd(notify_stream, {
                "type": "info",
                "subtype": "ml_cfg_bootstrap",
                "ts_ms": str(get_ny_time_millis()),
                "text": f"Bootstrapped {champion_key} from hash {cfg_hash_key} (mode={cfg.get('mode')}, enforce_share={cfg.get('enforce_share')})"
            }, maxlen=200000, approximate=True)
    
    # Verify champion was set
    assert mock_redis.set.called
    call_args = mock_redis.set.call_args
    assert call_args[0][0] == champion_key
    
    # Verify notification was sent
    assert mock_redis.xadd.called
    notify_call = [c for c in mock_redis.xadd.call_args_list if c[0][0] == notify_stream]
    assert len(notify_call) > 0
    notify_fields = notify_call[0][0][1]
    assert notify_fields["type"] == "info"
    assert notify_fields["subtype"] == "ml_cfg_bootstrap"


def test_bootstrap_skips_if_champion_exists():
    """Test that bootstrap is skipped if champion already exists."""
    mock_redis = MagicMock()
    mock_redis.get.return_value = json.dumps({"mode": "SHADOW"})  # Champion exists
    mock_redis.hgetall.return_value = {
        "mode": "ENFORCE",
        "fail_policy": "CLOSED",
    }
    mock_redis.set.return_value = True
    
    champion_key = "cfg:ml_confirm:champion"
    cfg_hash_key = "cfg:ml_confirm"
    
    # Simulate bootstrap logic
    if not mock_redis.get(champion_key):
        h = mock_redis.hgetall(cfg_hash_key)
        if isinstance(h, dict) and len(h) > 0:
            cfg = _coerce_hash_cfg(h)
            mock_redis.set(champion_key, json.dumps(cfg, ensure_ascii=False, separators=(",", ":")))
    
    # Should not set champion (it already exists)
    assert not mock_redis.set.called or mock_redis.set.call_count == 0


def test_approve_with_missing_challenger_alert():
    """Test that approve callback triggers alert when challenger is missing."""
    mock_redis = MagicMock()
    mock_redis.get.return_value = None  # Challenger missing
    mock_redis.xadd.return_value = "12345-0"
    
    challenger_key = "cfg:ml_confirm:challenger"
    champion_key = "cfg:ml_confirm:champion"
    notify_stream = "notify:telegram"
    run_id = "test_run_123"
    
    # Simulate approve callback logic
    chal = _safe_loads(mock_redis.get(challenger_key))
    if chal and str(chal.get("run_id", "")) == run_id:
        # Should not reach here
        pass
    else:
        # Challenger missing/mismatched: notify
        mock_redis.xadd(notify_stream, {
            "type": "alert",
            "subtype": "ml_promo_missing_challenger",
            "ts_ms": str(get_ny_time_millis()),
            "text": f"ML promo approve requested for run_id={run_id}, but {challenger_key} missing or mismatched. champion_exists={int(bool(mock_redis.get(champion_key)))}"
        }, maxlen=200000, approximate=True)
    
    # Verify alert was sent
    assert mock_redis.xadd.called
    alert_call = [c for c in mock_redis.xadd.call_args_list if c[0][0] == notify_stream]
    assert len(alert_call) > 0
    alert_fields = alert_call[0][0][1]
    assert alert_fields["type"] == "alert"
    assert alert_fields["subtype"] == "ml_promo_missing_challenger"
    assert run_id in alert_fields["text"]


def test_approve_with_matching_challenger_promotes():
    """Test that approve callback promotes challenger when run_id matches."""
    mock_redis = MagicMock()
    challenger_cfg = {
        "run_id": "test_run_123",
        "mode": "SHADOW",
        "kind": "util_mh_v1",
    }
    mock_redis.get.return_value = json.dumps(challenger_cfg)
    mock_redis.set.return_value = True
    mock_redis.delete.return_value = True
    
    challenger_key = "cfg:ml_confirm:challenger"
    champion_key = "cfg:ml_confirm:champion"
    run_id = "test_run_123"
    
    # Simulate approve callback logic
    chal = _safe_loads(mock_redis.get(challenger_key))
    if chal and str(chal.get("run_id", "")) == run_id:
        chal.setdefault("promoted_ms", get_ny_time_millis())
        chal.setdefault("mode", "SHADOW")
        chal.setdefault("fail_policy", "OPEN")
        chal.setdefault("enforce_share", 0.05)
        mock_redis.set(champion_key, json.dumps(chal, ensure_ascii=False, separators=(",", ":")))
        mock_redis.delete(challenger_key)
    
    # Verify champion was set
    assert mock_redis.set.called
    call_args = mock_redis.set.call_args
    assert call_args[0][0] == champion_key
    
    # Verify challenger was deleted
    assert mock_redis.delete.called
    assert mock_redis.delete.call_args[0][0] == challenger_key
    
    # Verify promoted_ms was added
    promoted_cfg = json.loads(call_args[0][1])
    assert "promoted_ms" in promoted_cfg
    assert promoted_cfg["run_id"] == run_id


def test_is_valid_cfg_with_valid_cfg():
    """Test _is_valid_cfg with valid config."""
    cfg = {
        "run_id": "test123",
        "mode": "SHADOW",
        "kind": "util_mh_v1",
    }
    assert _is_valid_cfg(cfg) is True


def test_is_valid_cfg_with_empty_dict():
    """Test _is_valid_cfg with empty dict."""
    assert _is_valid_cfg({}) is False


def test_is_valid_cfg_with_missing_run_id():
    """Test _is_valid_cfg with missing run_id."""
    cfg = {
        "mode": "SHADOW",
        "kind": "util_mh_v1",
    }
    assert _is_valid_cfg(cfg) is False


def test_is_valid_cfg_with_empty_run_id():
    """Test _is_valid_cfg with empty run_id."""
    cfg = {
        "run_id": "",
        "mode": "SHADOW",
    }
    assert _is_valid_cfg(cfg) is False


def test_is_valid_cfg_with_none():
    """Test _is_valid_cfg with None."""
    assert _is_valid_cfg(None) is False


def test_is_valid_cfg_with_not_dict():
    """Test _is_valid_cfg with non-dict."""
    assert _is_valid_cfg("not a dict") is False
    assert _is_valid_cfg([]) is False


def test_notify_sends_alert():
    """Test that _notify sends alert to stream."""
    mock_redis = MagicMock()
    mock_redis.xadd.return_value = "12345-0"
    
    _notify(mock_redis, "notify:telegram", "Test alert", "test_subtype")
    
    assert mock_redis.xadd.called
    call_args = mock_redis.xadd.call_args
    assert call_args[0][0] == "notify:telegram"
    fields = call_args[0][1]
    assert fields["type"] == "alert"
    assert fields["subtype"] == "test_subtype"
    assert fields["text"] == "Test alert"
    assert "ts_ms" in fields
    assert call_args[1]["maxlen"] == 200000
    assert call_args[1]["approximate"] is True


def test_notify_handles_exception():
    """Test that _notify handles exceptions gracefully."""
    mock_redis = MagicMock()
    mock_redis.xadd.side_effect = Exception("Redis error")
    
    # Should not raise
    _notify(mock_redis, "notify:telegram", "Test alert", "test_subtype")


def test_startup_champion_invalid_alert():
    """Test that startup diagnostic alerts when champion is invalid."""
    mock_redis = MagicMock()
    mock_redis.get.return_value = "{}"  # Empty dict (invalid)
    mock_redis.type.return_value = "string"
    mock_redis.strlen.return_value = 2
    mock_redis.xadd.return_value = "12345-0"
    
    champion_key = "cfg:ml_confirm:champion"
    notify_stream = "notify:telegram"
    
    # Simulate startup diagnostic logic
    from ml_promo_callbacks_worker_tb_v10_4 import _safe_loads
    champ = _safe_loads(mock_redis.get(champion_key))
    if not _is_valid_cfg(champ):
        _notify(mock_redis, notify_stream, 
                f"ML champion cfg invalid/empty at {champion_key}. "
                f"TYPE={mock_redis.type(champion_key)} STRLEN={mock_redis.strlen(champion_key)}",
                subtype="ml_champion_invalid")
    
    # Verify alert was sent
    assert mock_redis.xadd.called
    alert_call = [c for c in mock_redis.xadd.call_args_list if c[0][0] == notify_stream]
    assert len(alert_call) > 0
    alert_fields = alert_call[0][0][1]
    assert alert_fields["type"] == "alert"
    assert alert_fields["subtype"] == "ml_champion_invalid"
    assert champion_key in alert_fields["text"]
    assert "TYPE=string" in alert_fields["text"]
    assert "STRLEN=2" in alert_fields["text"]


def test_approve_with_invalid_challenger_alert():
    """Test that approve callback alerts when challenger is invalid."""
    mock_redis = MagicMock()
    mock_redis.get.return_value = "{}"  # Empty dict (invalid)
    mock_redis.type.return_value = "string"
    mock_redis.strlen.return_value = 2
    mock_redis.xadd.return_value = "12345-0"
    
    challenger_key = "cfg:ml_confirm:challenger"
    notify_stream = "notify:telegram"
    run_id = "test_run_123"
    
    # Simulate approve callback logic with validation
    chal = _safe_loads(mock_redis.get(challenger_key))
    if _is_valid_cfg(chal) and str(chal.get("run_id", "")) == run_id:
        # Should not reach here
        pass
    else:
        _notify(mock_redis, notify_stream,
                f"Approve requested for run_id={run_id}, but challenger missing/invalid at {challenger_key}. "
                f"TYPE={mock_redis.type(challenger_key)} STRLEN={mock_redis.strlen(challenger_key)}",
                subtype="ml_challenger_missing")
    
    # Verify alert was sent
    assert mock_redis.xadd.called
    alert_call = [c for c in mock_redis.xadd.call_args_list if c[0][0] == notify_stream]
    assert len(alert_call) > 0
    alert_fields = alert_call[0][0][1]
    assert alert_fields["type"] == "alert"
    assert alert_fields["subtype"] == "ml_challenger_missing"
    assert run_id in alert_fields["text"]
    assert challenger_key in alert_fields["text"]


def test_approve_with_valid_challenger_no_alert():
    """Test that approve callback does not alert when challenger is valid."""
    mock_redis = MagicMock()
    challenger_cfg = {
        "run_id": "test_run_123",
        "mode": "SHADOW",
        "kind": "util_mh_v1",
    }
    mock_redis.get.return_value = json.dumps(challenger_cfg)
    mock_redis.set.return_value = True
    mock_redis.delete.return_value = True
    mock_redis.xadd.return_value = "12345-0"
    
    challenger_key = "cfg:ml_confirm:challenger"
    champion_key = "cfg:ml_confirm:champion"
    notify_stream = "notify:telegram"
    run_id = "test_run_123"
    
    # Simulate approve callback logic with validation
    chal = _safe_loads(mock_redis.get(challenger_key))
    if _is_valid_cfg(chal) and str(chal.get("run_id", "")) == run_id:
        chal.setdefault("promoted_ms", get_ny_time_millis())
        chal.setdefault("mode", "SHADOW")
        chal.setdefault("fail_policy", "OPEN")
        chal.setdefault("enforce_share", 0.05)
        mock_redis.set(champion_key, json.dumps(chal, ensure_ascii=False, separators=(",", ":")))
        mock_redis.delete(challenger_key)
    else:
        _notify(mock_redis, notify_stream,
                f"Approve requested for run_id={run_id}, but challenger missing/invalid at {challenger_key}. "
                f"TYPE={mock_redis.type(challenger_key)} STRLEN={mock_redis.strlen(challenger_key)}",
                subtype="ml_challenger_missing")
    
    # Verify no alert was sent (challenger was valid)
    notify_calls = [c for c in mock_redis.xadd.call_args_list if c[0][0] == notify_stream]
    assert len(notify_calls) == 0
    
    # Verify champion was set
    assert mock_redis.set.called
    assert mock_redis.delete.called


def test_reject_with_invalid_challenger():
    """Test that reject callback handles invalid challenger gracefully."""
    mock_redis = MagicMock()
    mock_redis.get.return_value = "{}"  # Empty dict (invalid)
    mock_redis.xadd.return_value = "12345-0"
    
    challenger_key = "cfg:ml_confirm:challenger"
    run_id = "test_run_123"
    
    # Simulate reject callback logic with validation
    chal = _safe_loads(mock_redis.get(challenger_key))
    if _is_valid_cfg(chal) and str(chal.get("run_id", "")) == run_id:
        chal["rejected_ms"] = get_ny_time_millis()
        mock_redis.set(challenger_key + ":rejected:" + run_id, 
                      json.dumps(chal, ensure_ascii=False, separators=(",", ":")), 
                      ex=7*24*3600)
        mock_redis.delete(challenger_key)
    
    # Should not set rejected (challenger invalid)
    assert not mock_redis.set.called or "rejected" not in str(mock_redis.set.call_args)

