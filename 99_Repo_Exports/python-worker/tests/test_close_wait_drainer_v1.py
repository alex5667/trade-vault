import json
import pytest
from unittest.mock import MagicMock, patch
try:
    from tools.close_wait_drainer_v1 import (
        Cfg, process_one, parse_close_wait_payload, build_trades_closed_payload,
        extract_close_fields
    )
except ImportError:
    import sys
    import os
# [AUTOGRAVITY CLEANUP]     sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
    from tools.close_wait_drainer_v1 import (
        Cfg, process_one, parse_close_wait_payload, build_trades_closed_payload,
        extract_close_fields
    )

@pytest.fixture
def cfg():
    return Cfg(
        redis_url="redis://localhost:6379/0",
        close_wait_stream="trades:close_wait",
        close_wait_group="group1",
        close_wait_consumer="c1",
        trades_closed_stream="trades:closed",
        ml_replay_inputs_stream="ml_replay_inputs_v1",
        write_ml_replay_inputs=True,
        decision_key_prefix="decision:",
        dedup_key_prefix="join:closed:",
        dedup_ttl_sec=3600,
        lock_ttl_sec=30,
        max_attempts=5,
        max_wait_age_ms=3600000,
        label_win_r_min=0.0,
        metrics_hash="metrics:test",
        delete_after_ack=False
    )

@pytest.fixture
def mock_redis():
    return MagicMock()

def test_parse_close_wait_payload():
    fields = {
        b"payload": json.dumps({
            "sid": "sid123",
            "close_event": {"symbol": "BTCUSDT", "r_mult": 1.5}
        }).encode()
    }
    sid, close_ev = parse_close_wait_payload(fields)
    assert sid == "sid123"
    assert close_ev["symbol"] == "BTCUSDT"

def test_extract_close_fields():
    ev = {
        "symbol": "BTCUSDT",
        "close_ts_ms": 1700000000000,
        "r_mult": "1.5",
        "meta_enforce_applied": "1"
    }
    extracted = extract_close_fields(ev)
    assert extracted["symbol"] == "BTCUSDT"
    assert extracted["r_mult"] == 1.5
    assert extracted["meta_enforce_applied"] == "1"

def test_build_trades_closed_payload(cfg):
    sid = "sid123"
    close_ev = {"symbol": "BTCUSDT", "close_ts_ms": 1700000100000, "r_mult": 1.5}
    decision = {
        "ml_p_cal": 0.8,
        "decision_ts_ms": 1700000000000,
        "dq_state": "ok",
        "drift_state": "ok"
    }
    payload = build_trades_closed_payload(cfg, sid, close_ev, decision)
    assert payload["sid"] == "sid123"
    assert payload["y"] == 1
    assert payload["decision_age_ms"] == 100000
    assert payload["dq_state"] == "ok"

def test_process_one_success(mock_redis, cfg):
    msg_id = b"123-0"
    fields = {
        b"payload": json.dumps({
            "sid": "sid123",
            "close_event": {"symbol": "BTCUSDT", "close_ts_ms": 1700000100000, "r_mult": 1.5}
        }).encode()
    }
    decision = {"ml_p_cal": 0.8, "decision_ts_ms": 1700000000000, "dq_state": "ok"}
    
    mock_redis.set.return_value = True # lock
    mock_redis.exists.return_value = False # dedup
    mock_redis.get.return_value = json.dumps(decision).encode()
    
    pipeline = mock_redis.pipeline.return_value
    pipeline.execute.return_value = [True, b"new_id"]

    process_one(mock_redis, cfg, msg_id, fields)

    mock_redis.xack.assert_called_once()
    assert mock_redis.xadd.call_count == 0 # It's in pipeline
    pipeline.xadd.assert_called()

def test_process_one_missing_decision(mock_redis, cfg):
    msg_id = b"123-0"
    fields = {
        b"payload": json.dumps({"sid": "sid123", "close_event": {}}).encode()
    }
    
    mock_redis.set.return_value = True # lock
    mock_redis.exists.return_value = False # dedup
    mock_redis.get.return_value = None # missing decision
    mock_redis.incr.return_value = 1 # attempt
    
    process_one(mock_redis, cfg, msg_id, fields)

    mock_redis.xack.assert_not_called()
    mock_redis.hincrby.assert_any_call(cfg.metrics_hash, "missing_decision_total", 1)
