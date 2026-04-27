import pytest
from services.atr_rollout_cert_service import evaluate_metrics

def test_evaluate_metrics_pass():
    thresholds = {
        "min_n_trades": 20,
        "min_avg_pnl_bps": -2.0,
        "max_avg_slippage_bps": 6.0,
        "max_stop_rate": 0.60,
        "min_tp1_rate": 0.25,
        "max_avg_mae_pct": 0.02
    }
    
    stats = {
        "n_trades": 30,
        "avg_pnl_bps": 1.5,
        "avg_slippage_bps": 4.5,
        "stop_rate": 0.40,
        "tp1_rate": 0.35,
        "max_mae_pct": 0.01
    }
    
    status, reason, checks = evaluate_metrics(stats, thresholds)
    
    assert status == "passed"
    assert reason == "ROLL_CERT_PASS"
    assert all(checks.values())

def test_evaluate_metrics_pending():
    thresholds = {"min_n_trades": 50, "min_avg_pnl_bps": -2.0}
    stats = {"n_trades": 10, "avg_pnl_bps": 1.5}
    
    status, reason, checks = evaluate_metrics(stats, thresholds)
    
    assert status == "pending"
    assert reason == "WAIT_TRADES"
    assert checks["min_n_trades"] is False

def test_evaluate_metrics_hard_stop_pnl():
    thresholds = {
        "min_n_trades": 20,
        "min_avg_pnl_bps": -2.0,
        "max_avg_slippage_bps": 6.0,
        "max_stop_rate": 0.60,
        "min_tp1_rate": 0.25,
        "max_avg_mae_pct": 0.02
    }
    
    stats = {
        "n_trades": 15,  # > min_n_trades/2
        "avg_pnl_bps": -8.0, # severely below -2.0 - 5.0
        "avg_slippage_bps": 4.5,
        "stop_rate": 0.40,
        "tp1_rate": 0.35,
        "max_mae_pct": 0.01
    }
    
    status, reason, checks = evaluate_metrics(stats, thresholds)
    
    assert status == "failed"
    assert reason == "ROLL_CERT_NEGATIVE_PNL"

def test_evaluate_metrics_hard_stop_slippage():
    thresholds = {
        "min_n_trades": 20,
        "min_avg_pnl_bps": -2.0,
        "max_avg_slippage_bps": 6.0,
        "max_stop_rate": 0.60,
        "min_tp1_rate": 0.25,
        "max_avg_mae_pct": 0.02
    }
    
    stats = {
        "n_trades": 15,
        "avg_pnl_bps": 1.0,
        "avg_slippage_bps": 10.0, # > 6.0 + 3.0
        "stop_rate": 0.40,
        "tp1_rate": 0.35,
        "max_mae_pct": 0.01
    }
    
    status, reason, checks = evaluate_metrics(stats, thresholds)
    
    assert status == "failed"
    assert reason == "ROLL_CERT_SLIPPAGE_SPIKE"
