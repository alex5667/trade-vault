import json
from unittest.mock import MagicMock

import pytest

from tools.close_backfill_replay_v1 import (
    Cfg,
    build_trades_closed_payload,
    decision_get,
    extract_close_fields,
    is_position_closed,
    json_loads_safe,
)


def test_extract_close_fields():
    # Test common field extraction
    ev = {
        "event_type": "POSITION_CLOSED",
        "ts_ms": 1740000000000,
        "symbol": "BTCUSDT",
        "tf": "1m",
        "r_mult": 1.5,
        "sid": "sid123"
    }
    extracted = extract_close_fields(ev)
    assert extracted["close_ts_ms"] == 1740000000000
    assert extracted["symbol"] == "BTCUSDT"
    assert extracted["r_mult"] == 1.5

    # Test alternate keys
    ev2 = {
        "type": "POSITION_CLOSED",
        "timestamp": 1740000000, # seconds
        "sym": "ETHUSDT",
        "RMult": "2.0"
    }
    extracted2 = extract_close_fields(ev2)
    assert extracted2["close_ts_ms"] == 1740000000000
    assert extracted2["symbol"] == "ETHUSDT"
    assert extracted2["r_mult"] == 2.0

def test_is_position_closed():
    assert is_position_closed({"event_type": "POSITION_CLOSED"}) is True
    assert is_position_closed({"type": "position_closed"}) is True
    assert is_position_closed({"event_type": "POSITION_OPENED"}) is False
    assert is_position_closed({}) is False

def test_build_trades_closed_payload():
    sid = "sid1"
    close_ev = {
        "ts_ms": 1740000000000,
        "r_mult": 1.0,
        "symbol": "BTCUSDT"
    }
    decision = {
        "ts_ms": 1739999000000,
        "ml_p_cal": 0.8,
        "rule_score": 10,
        "drift_state": "ok"
    }
    label_win_r_min = 0.5

    payload = build_trades_closed_payload(sid, close_ev, decision, label_win_r_min)

    assert payload["sid"] == sid
    assert payload["symbol"] == "BTCUSDT"
    assert payload["close_ts_ms"] == 1740000000000
    assert payload["decision_ts_ms"] == 1739999000000
    assert payload["decision_age_ms"] == 1000000
    assert payload["y"] == 1
    assert payload["ml_p_cal"] == 0.8
    assert payload["brier"] == pytest.approx((0.8 - 1.0)**2)
    assert payload["source"] == "close_backfill_replay"

def test_decision_get():
    r = MagicMock()
    cfg = Cfg(
        redis_url="", trade_events_stream="", trades_closed_stream="",
        close_wait_stream="", decision_key_prefix="decision:",
        join_dedup_prefix="", seen_event_prefix="", seen_ttl_sec=0,
        dedup_ttl_sec=0, label_win_r_min=0, direct_join=True,
        scan_batch=0, max_count=0, metrics_hash="",
        write_ml_replay_inputs=False, ml_replay_inputs_stream="",
    )

    # Test GET
    r.get.return_value = json.dumps({"sid": "sid1", "drift_state": "ok"}).encode()
    dec = decision_get(r, cfg, "sid1")
    assert dec["sid"] == "sid1"

    # Test HGET fallback
    r.get.return_value = None
    r.hget.return_value = json.dumps({"sid": "sid2", "drift_state": "warn"}).encode()
    dec = decision_get(r, cfg, "sid2")
    assert dec["sid"] == "sid2"
    r.hget.assert_called_with("decision:sid2", b"payload")

def test_json_loads_safe():
    assert json_loads_safe(b'{"a": 1}') == {"a": 1}
    assert json_loads_safe('{"a": 1}') == {"a": 1}
    assert json_loads_safe('invalid') is None
    assert json_loads_safe(None) is None
