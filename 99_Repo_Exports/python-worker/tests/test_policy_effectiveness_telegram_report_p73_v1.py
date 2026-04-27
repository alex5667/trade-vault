"""Tests for P73 policy effectiveness Telegram report."""

import pytest
from unittest.mock import MagicMock, patch

from orderflow_services import policy_effectiveness_telegram_report_p73_v1 as p73

def test_build_message():
    cfg = {
        "policy_effectiveness_last_ts_ms": "1700000000000",
        "policy_effectiveness_input_last_ts_ms": "1700000000000",
        "policy_effectiveness_baseline_ok_present": "1",
        "policy_effectiveness_total_n_24h": "500",
        "policy_effectiveness_share_24h_ok": "0.5",
        "policy_effectiveness_share_24h_warn": "0.3",
        "policy_effectiveness_share_24h_block": "0.2",
        "policy_effectiveness_share_24h_unknown": "0.0",
        "policy_effectiveness_expectancy_r_delta_24h_warn": "-0.05",
        "policy_effectiveness_precision_top5p_delta_24h_warn": "-0.01",
        "policy_effectiveness_ece_delta_24h_warn": "0.02",
        "policy_effectiveness_expectancy_r_delta_24h_block": "-0.1",
        "policy_effectiveness_precision_top5p_delta_24h_block": "-0.05",
        "policy_effectiveness_ece_delta_24h_block": "0.05",
    }
    
    with patch("orderflow_services.policy_effectiveness_telegram_report_p73_v1.now_ms", return_value=1700000000000):
        msg, meta = p73.build_message(cfg)
        
    assert "<b>Policy effectiveness (24h)</b>" in msg
    assert "total_n=<code>500</code>" in msg
    assert "baseline_ok=<code>1</code>" in msg
    assert "share: ok=<code>50.00%</code> warn=<code>30.00%</code> block=<code>20.00%</code>" in msg
    assert "Δ vs ok (warn): exp_R=<code>-0.050</code>" in msg
    
    assert meta["total_n"] == 500
    assert meta["baseline_ok"] == 1
    assert meta["shares"]["ok"] == 0.5
    assert meta["deltas"]["warn"]["exp_r"] == -0.05

def test_classify_severity():
    # Fresh report, baseline present
    assert p73._classify_severity(100, 1, 500, 1800, 7200) == "info"
    
    # Stale warning
    assert p73._classify_severity(2000, 1, 500, 1800, 7200) == "warning"
    
    # Stale critical
    assert p73._classify_severity(8000, 1, 500, 1800, 7200) == "critical"
    
    # Baseline missing (critical if n >= 50)
    assert p73._classify_severity(100, 0, 50, 1800, 7200) == "critical"
    
    # Baseline missing but low N (not critical due to N)
    assert p73._classify_severity(100, 0, 40, 1800, 7200) == "info"

def test_should_send():
    r_mock = MagicMock()
    
    # Empty state -> should send
    r_mock.hgetall.return_value = {}
    send, reason = p73.should_send(r_mock, "key", "msg1", "info", 3600, 1)
    assert send is True
    assert reason == "ok"
    
    # Cooldown active, same hash -> no send
    with patch("orderflow_services.policy_effectiveness_telegram_report_p73_v1.now_ms", return_value=1000000):
        r_mock.hgetall.return_value = {
            "last_sent_ts_ms": "900000",
            "last_hash": p73._sha1("msg1"),
            "last_severity": "info"
        }
        send, reason = p73.should_send(r_mock, "key", "msg1", "info", 3600, 1)
        assert send is False
        assert reason == "dedup_cooldown"
        
        # Cooldown active, different hash -> still no send
        send, reason = p73.should_send(r_mock, "key", "msg2", "info", 3600, 1)
        assert send is False
        assert reason == "cooldown"
        
        # Critical bypass override (force 20% interval)
        send, reason = p73.should_send(r_mock, "key", "msg2", "critical", 3600, 1)
        assert send is True
        
    with patch("orderflow_services.policy_effectiveness_telegram_report_p73_v1.now_ms", return_value=1800000):
        # Time passes enough for critical override (800 > 720s)
        r_mock.hgetall.return_value = {
            "last_sent_ts_ms": "1000000",
            "last_hash": p73._sha1("msg1"),
            "last_severity": "info"
        }
        send, reason = p73.should_send(r_mock, "key", "msg2", "critical", 3600, 1)
        assert send is True
        assert reason == "critical_bypass"
