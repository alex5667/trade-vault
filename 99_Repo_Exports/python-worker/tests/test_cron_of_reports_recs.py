"""
Tests for propose_cfg_recs function in cron_of_reports.py
"""
import os
import json
import pytest
import tempfile
from pathlib import Path
from collections import Counter
from tools.cron_of_reports import propose_cfg_recs, ReplayStats, analyze_outcome


def test_recs_disabled():
    """Test that recommendations are disabled when RECS_ENABLE=0"""
    os.environ["RECS_ENABLE"] = "0"
    os.environ.pop("CFG_TARGET_SYMBOLS", None)
    os.environ.pop("CANARY_SYMBOLS", None)
    
    stats = ReplayStats(
        n=100,
        ok_rate=0.1,
        no_data=0,
        by_scenario={},
        exec_risk_norm_p50=0.5,
        exec_risk_norm_p90=0.95,
        vol_shock_cap_hit_rate=0.5,
        saw_chop_hard_miss_rate=0.5,
        ok_soft_rate=0.0,
        top_missing_legs=[],
    )
    
    recs = propose_cfg_recs(stats, mode="monitor", outcome=None)
    assert len(recs) == 0


def test_recs_exec_risk_high():
    """Test recommendations when exec_risk_norm_p90 is high"""
    os.environ["RECS_ENABLE"] = "1"
    os.environ["CFG_TARGET_SYMBOLS"] = "BTCUSDT,ETHUSDT"
    os.environ["EXEC_RISK_NORM_P90_WARN"] = "0.85"
    os.environ["RECS_STEP_W_EXEC"] = "0.02"
    os.environ["RECS_STEP_EXEC_REF_BPS"] = "1.0"
    os.environ["CFG_HASH_PREFIX"] = "config:orderflow:"
    
    stats = ReplayStats(
        n=100,
        ok_rate=0.5,
        ok_soft_rate=0.0,
        no_data=0,
        by_scenario={},
        exec_risk_norm_p50=0.5,
        exec_risk_norm_p90=0.90,  # Above threshold
        vol_shock_cap_hit_rate=0.1,
        saw_chop_hard_miss_rate=0.1,
        top_missing_legs=[],
    )
    
    recs = propose_cfg_recs(stats, mode="monitor", outcome=None)
    
    # Should have recommendations for both symbols
    assert len(recs) >= 2
    
    # Check that we have recommendations for both symbols
    symbols = {r["symbol"] for r in recs if r.get("scope") == "per_symbol"}
    assert "BTCUSDT" in symbols
    assert "ETHUSDT" in symbols
    
    # Check format of recommendations
    for r in recs:
        if r.get("scope") == "per_symbol" and "w_exec_risk" in r.get("key", ""):
            assert "cmd" in r
            assert "redis-cli HSET" in r["cmd"]
            assert "config:orderflow:" in r["cmd"]
            assert "w_exec_risk" in r["cmd"]
            assert "exec_risk_ref_bps" in r["cmd"]
            assert "why" in r
            assert "exec_risk_norm p90" in r["why"]


def test_recs_ok_rate_low():
    """Test recommendations when ok_rate is too low"""
    os.environ["RECS_ENABLE"] = "1"
    os.environ["CFG_TARGET_SYMBOLS"] = "BTCUSDT"
    os.environ["PASS_RATE_MIN"] = "0.25"
    os.environ["RECS_STEP_SCORE_MIN"] = "0.02"
    os.environ["CFG_HASH_PREFIX"] = "config:orderflow:"
    
    stats = ReplayStats(
        n=100,
        ok_rate=0.15,
        ok_soft_rate=0.0,
        no_data=0,  # Below threshold
        by_scenario={},
        exec_risk_norm_p50=0.5,
        exec_risk_norm_p90=0.7,
        vol_shock_cap_hit_rate=0.1,
        saw_chop_hard_miss_rate=0.1,
        top_missing_legs=[],
    )
    
    recs = propose_cfg_recs(stats, mode="monitor", outcome=None)
    
    # Should have at least one recommendation
    assert len(recs) >= 1
    
    # Check for score_min recommendation
    score_recs = [r for r in recs if r.get("key") == "of_score_min"]
    assert len(score_recs) >= 1
    
    for r in score_recs:
        assert r["symbol"] == "BTCUSDT"
        assert "of_score_min" in r["cmd"]
        assert "ok_rate" in r["why"]


def test_recs_vol_shock_cap_hit():
    """Test recommendations when vol_shock cap-hit rate is high"""
    os.environ["RECS_ENABLE"] = "1"
    os.environ["CFG_TARGET_SYMBOLS"] = "BTCUSDT"
    os.environ["VOL_SHOCK_CAP_HIT_WARN"] = "0.20"
    os.environ["RECS_VOL_SHOCK_FAIL_CLOSED_ON_CAP"] = "1"
    os.environ["CFG_HASH_PREFIX"] = "config:orderflow:"
    
    stats = ReplayStats(
        n=100,
        ok_rate=0.5,
        ok_soft_rate=0.0,
        no_data=0,
        by_scenario={},
        exec_risk_norm_p50=0.5,
        exec_risk_norm_p90=0.7,
        vol_shock_cap_hit_rate=0.25,  # Above threshold
        saw_chop_hard_miss_rate=0.1,
        top_missing_legs=[],
    )
    
    recs = propose_cfg_recs(stats, mode="monitor", outcome=None)
    
    # Should have vol_shock recommendation
    vol_recs = [r for r in recs if r.get("key") == "vol_shock_fail_closed"]
    assert len(vol_recs) >= 1
    
    for r in vol_recs:
        assert r["symbol"] == "BTCUSDT"
        assert "vol_shock_fail_closed" in r["cmd"]
        assert "vol_shock cap-hit rate" in r["why"]


def test_recs_saw_chop_hard_miss():
    """Test recommendations when saw_chop hard-miss rate is high"""
    os.environ["RECS_ENABLE"] = "1"
    os.environ["CFG_TARGET_SYMBOLS"] = "BTCUSDT"
    os.environ["SAW_CHOP_HARD_MISS_WARN"] = "0.30"
    os.environ["RECS_SAW_CHOP_FAIL_CLOSED_ON_MISS"] = "1"
    os.environ["CFG_HASH_PREFIX"] = "config:orderflow:"
    
    stats = ReplayStats(
        n=100,
        ok_rate=0.5,
        ok_soft_rate=0.0,
        no_data=0,
        by_scenario={},
        exec_risk_norm_p50=0.5,
        exec_risk_norm_p90=0.7,
        vol_shock_cap_hit_rate=0.1,
        saw_chop_hard_miss_rate=0.35,  # Above threshold
        top_missing_legs=[],
    )
    
    recs = propose_cfg_recs(stats, mode="monitor", outcome=None)
    
    # Should have saw_chop recommendation
    saw_recs = [r for r in recs if r.get("key") == "saw_chop_fail_closed"]
    assert len(saw_recs) >= 1
    
    for r in saw_recs:
        assert r["symbol"] == "BTCUSDT"
        assert "saw_chop_fail_closed" in r["cmd"]
        assert "saw_chop hard-miss rate" in r["why"]


def test_recs_diagnostics_missing_legs():
    """Test diagnostic recommendations for missing legs"""
    os.environ["RECS_ENABLE"] = "1"
    os.environ["CFG_TARGET_SYMBOLS"] = "BTCUSDT"
    os.environ["CFG_HASH_PREFIX"] = "config:orderflow:"
    
    stats = ReplayStats(
        n=100,
        ok_rate=0.5,
        no_data=0,
        by_scenario={},
        exec_risk_norm_p50=0.5,
        exec_risk_norm_p90=0.7,
        vol_shock_cap_hit_rate=0.1,
        saw_chop_hard_miss_rate=0.1,
        ok_soft_rate=0.0,
        top_missing_legs=[("fp_edge", 10), ("ofi", 8), ("iceberg", 5)],
    )
    
    recs = propose_cfg_recs(stats, mode="monitor", outcome=None)
    
    # Should have diagnostics recommendation
    diag_recs = [r for r in recs if r.get("scope") == "info"]
    assert len(diag_recs) >= 1
    
    for r in diag_recs:
        assert r["key"] == "diagnostics"
        assert "top_missing_legs" in r["value"]
        assert len(r["value"]["top_missing_legs"]) > 0


def test_recs_multiple_conditions():
    """Test recommendations when multiple conditions are met"""
    os.environ["RECS_ENABLE"] = "1"
    os.environ["CFG_TARGET_SYMBOLS"] = "BTCUSDT,ETHUSDT"
    os.environ["EXEC_RISK_NORM_P90_WARN"] = "0.85"
    os.environ["PASS_RATE_MIN"] = "0.25"
    os.environ["VOL_SHOCK_CAP_HIT_WARN"] = "0.20"
    os.environ["SAW_CHOP_HARD_MISS_WARN"] = "0.30"
    os.environ["RECS_VOL_SHOCK_FAIL_CLOSED_ON_CAP"] = "1"
    os.environ["RECS_SAW_CHOP_FAIL_CLOSED_ON_MISS"] = "1"
    os.environ["CFG_HASH_PREFIX"] = "config:orderflow:"
    
    stats = ReplayStats(
        n=100,
        ok_rate=0.15,
        no_data=0,  # Low
        by_scenario={},
        exec_risk_norm_p50=0.5,
        exec_risk_norm_p90=0.90,  # High
        vol_shock_cap_hit_rate=0.25,  # High
        saw_chop_hard_miss_rate=0.35,  # High
        ok_soft_rate=0.0,
        top_missing_legs=[("fp_edge", 10)],
    )
    
    recs = propose_cfg_recs(stats, mode="monitor", outcome=None)
    
    # Should have multiple types of recommendations
    assert len(recs) >= 4  # At least exec, ok_rate, vol_shock, saw_chop
    
    # Check that we have recommendations for different issues
    keys = {r.get("key") for r in recs if r.get("scope") == "per_symbol"}
    assert "w_exec_risk, exec_risk_ref_bps" in keys or any("w_exec_risk" in k for k in keys)
    assert "of_score_min" in keys
    assert "vol_shock_fail_closed" in keys
    assert "saw_chop_fail_closed" in keys


def test_recs_fallback_to_canary_symbols():
    """Test that recommendations fall back to CANARY_SYMBOLS if CFG_TARGET_SYMBOLS is not set"""
    os.environ["RECS_ENABLE"] = "1"
    os.environ.pop("CFG_TARGET_SYMBOLS", None)
    os.environ["CANARY_SYMBOLS"] = "XRPUSDT,ADAUSDT"
    os.environ["EXEC_RISK_NORM_P90_WARN"] = "0.85"
    os.environ["CFG_HASH_PREFIX"] = "config:orderflow:"
    
    stats = ReplayStats(
        n=100,
        ok_rate=0.5,
        ok_soft_rate=0.0,
        no_data=0,
        by_scenario={},
        exec_risk_norm_p50=0.5,
        exec_risk_norm_p90=0.90,  # Above threshold
        vol_shock_cap_hit_rate=0.1,
        saw_chop_hard_miss_rate=0.1,
        top_missing_legs=[],
    )
    
    recs = propose_cfg_recs(stats, mode="monitor", outcome=None)
    
    # Should have recommendations for canary symbols
    symbols = {r["symbol"] for r in recs if r.get("scope") == "per_symbol"}
    assert "XRPUSDT" in symbols
    assert "ADAUSDT" in symbols


def test_recs_no_symbols():
    """Test that no recommendations are generated when no symbols are configured"""
    os.environ["RECS_ENABLE"] = "1"
    os.environ.pop("CFG_TARGET_SYMBOLS", None)
    os.environ.pop("CANARY_SYMBOLS", None)
    os.environ["EXEC_RISK_NORM_P90_WARN"] = "0.85"
    
    stats = ReplayStats(
        n=100,
        ok_rate=0.5,
        ok_soft_rate=0.0,
        no_data=0,
        by_scenario={},
        exec_risk_norm_p50=0.5,
        exec_risk_norm_p90=0.90,  # Above threshold
        vol_shock_cap_hit_rate=0.1,
        saw_chop_hard_miss_rate=0.1,
        top_missing_legs=[],
    )
    
    recs = propose_cfg_recs(stats, mode="monitor", outcome=None)
    
    # Should only have diagnostics if any, but no per_symbol recommendations
    per_symbol_recs = [r for r in recs if r.get("scope") == "per_symbol"]
    assert len(per_symbol_recs) == 0


def test_recs_vol_shock_disabled():
    """Test that vol_shock recommendations are disabled when RECS_VOL_SHOCK_FAIL_CLOSED_ON_CAP=0"""
    os.environ["RECS_ENABLE"] = "1"
    os.environ["CFG_TARGET_SYMBOLS"] = "BTCUSDT"
    os.environ["VOL_SHOCK_CAP_HIT_WARN"] = "0.20"
    os.environ["RECS_VOL_SHOCK_FAIL_CLOSED_ON_CAP"] = "0"  # Disabled
    os.environ["CFG_HASH_PREFIX"] = "config:orderflow:"
    
    stats = ReplayStats(
        n=100,
        ok_rate=0.5,
        ok_soft_rate=0.0,
        no_data=0,
        by_scenario={},
        exec_risk_norm_p50=0.5,
        exec_risk_norm_p90=0.7,
        vol_shock_cap_hit_rate=0.25,  # Above threshold
        saw_chop_hard_miss_rate=0.1,
        top_missing_legs=[],
    )
    
    recs = propose_cfg_recs(stats, mode="monitor", outcome=None)
    
    # Should NOT have vol_shock recommendation
    vol_recs = [r for r in recs if r.get("key") == "vol_shock_fail_closed"]
    assert len(vol_recs) == 0


def test_recs_saw_chop_disabled():
    """Test that saw_chop recommendations are disabled when RECS_SAW_CHOP_FAIL_CLOSED_ON_MISS=0"""
    os.environ["RECS_ENABLE"] = "1"
    os.environ["CFG_TARGET_SYMBOLS"] = "BTCUSDT"
    os.environ["SAW_CHOP_HARD_MISS_WARN"] = "0.30"
    os.environ["RECS_SAW_CHOP_FAIL_CLOSED_ON_MISS"] = "0"  # Disabled
    os.environ["CFG_HASH_PREFIX"] = "config:orderflow:"
    
    stats = ReplayStats(
        n=100,
        ok_rate=0.5,
        ok_soft_rate=0.0,
        no_data=0,
        by_scenario={},
        exec_risk_norm_p50=0.5,
        exec_risk_norm_p90=0.7,
        vol_shock_cap_hit_rate=0.1,
        saw_chop_hard_miss_rate=0.35,  # Above threshold
        top_missing_legs=[],
    )
    
    recs = propose_cfg_recs(stats, mode="monitor", outcome=None)
    
    # Should NOT have saw_chop recommendation
    saw_recs = [r for r in recs if r.get("key") == "saw_chop_fail_closed"]
    assert len(saw_recs) == 0


def test_analyze_outcome_basic():
    """Test analyze_outcome with basic data"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson', delete=False) as f:
        # Write test data
        test_data = [
            {"event_type": "POSITION_CLOSED", "r_mult": 1.5, "scenario_v4": "continuation", "of_confirm_ok": 1},
            {"event_type": "POSITION_CLOSED", "r_mult": -0.5, "scenario_v4": "reversal", "of_confirm_ok": 1},
            {"event_type": "POSITION_CLOSED", "r_mult": -1.5, "scenario_v4": "continuation", "of_confirm_ok": 0},
            {"event_type": "POSITION_CLOSED", "r_mult": 2.5, "scenario_v4": "reversal", "of_confirm_ok": 1},
            {"event_type": "POSITION_CLOSED", "r_mult": 0.3, "scenario_v4": "continuation", "of_confirm_ok": 1},
        ]
        for row in test_data:
            f.write(json.dumps(row) + "\n")
        f.flush()
        
        outcome = analyze_outcome(f.name)
        
        assert outcome["n"] == 5
        assert outcome["winrate"] == 0.6  # 3 wins out of 5
        assert abs(outcome["meanR"] - 0.46) < 0.01  # (1.5 - 0.5 - 1.5 + 2.5 + 0.3) / 5
        assert outcome["tail_loss_rate"] == 0.2  # 1 out of 5 (r_mult <= -1.0)
        assert outcome["bigwin_rate"] == 0.2  # 1 out of 5 (r_mult >= 2.0)
        assert "by_scenario" in outcome
        assert "by_of_confirm_ok" in outcome
        
        # Check scenario grouping
        assert "continuation" in outcome["by_scenario"]
        assert "reversal" in outcome["by_scenario"]
        
        # Clean up
        os.unlink(f.name)


def test_analyze_outcome_empty():
    """Test analyze_outcome with empty file"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson', delete=False) as f:
        f.write("")
        f.flush()
        
        outcome = analyze_outcome(f.name)
        
        assert outcome["n"] == 0
        assert outcome["winrate"] == 0.0
        assert outcome["meanR"] == 0.0
        
        os.unlink(f.name)


def test_analyze_outcome_filters_non_closed():
    """Test that analyze_outcome filters out non-POSITION_CLOSED events"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson', delete=False) as f:
        test_data = [
            {"event_type": "POSITION_OPENED", "r_mult": 1.0},
            {"event_type": "POSITION_CLOSED", "r_mult": 1.5},
            {"type": "CLOSE", "r_mult": 0.5},
            {"event_type": "OTHER", "r_mult": 2.0},
        ]
        for row in test_data:
            f.write(json.dumps(row) + "\n")
        f.flush()
        
        outcome = analyze_outcome(f.name)
        
        # Should only count POSITION_CLOSED and CLOSE
        assert outcome["n"] == 2
        
        os.unlink(f.name)


def test_recs_with_outcome_tail_loss():
    """Test recommendations when outcome shows high tail-loss"""
    os.environ["RECS_ENABLE"] = "1"
    os.environ["RECS_USE_OUTCOME"] = "1"
    os.environ["CFG_TARGET_SYMBOLS"] = "BTCUSDT"
    os.environ["OUTCOME_MIN_N"] = "10"
    os.environ["OUTCOME_TAIL_LOSS_MAX"] = "0.18"
    os.environ["CFG_HASH_PREFIX"] = "config:orderflow:"
    
    stats = ReplayStats(
        n=100,
        ok_rate=0.5,
        ok_soft_rate=0.0,
        no_data=0,
        by_scenario={},
        exec_risk_norm_p50=0.5,
        exec_risk_norm_p90=0.7,  # Below threshold
        vol_shock_cap_hit_rate=0.1,  # Below threshold
        saw_chop_hard_miss_rate=0.1,  # Below threshold
        top_missing_legs=[],
    )
    
    outcome = {
        "n": 100,
        "tail_loss_rate": 0.25,  # Above threshold
        "meanR": 0.15,
        "bigwin_rate": 0.10,
    }
    
    recs = propose_cfg_recs(stats, mode="regress", outcome=outcome)
    
    # Should have recommendations based on outcome tail-loss
    tail_recs = [r for r in recs if "tail-loss" in r.get("why", "").lower()]
    assert len(tail_recs) > 0


def test_recs_with_outcome_low_meanr():
    """Test recommendations when outcome shows low meanR with high ok_rate"""
    os.environ["RECS_ENABLE"] = "1"
    os.environ["RECS_USE_OUTCOME"] = "1"
    os.environ["CFG_TARGET_SYMBOLS"] = "BTCUSDT"
    os.environ["OUTCOME_MIN_N"] = "10"
    os.environ["OUTCOME_MEANR_MIN"] = "0.10"
    os.environ["CFG_HASH_PREFIX"] = "config:orderflow:"
    
    stats = ReplayStats(
        n=100,
        ok_rate=0.6,
        ok_soft_rate=0.0,
        no_data=0,  # High ok_rate
        by_scenario={},
        exec_risk_norm_p50=0.5,
        exec_risk_norm_p90=0.7,
        vol_shock_cap_hit_rate=0.1,
        saw_chop_hard_miss_rate=0.1,
        top_missing_legs=[],
    )
    
    outcome = {
        "n": 100,
        "meanR": 0.05,  # Below threshold
        "tail_loss_rate": 0.10,
        "bigwin_rate": 0.10,
    }
    
    recs = propose_cfg_recs(stats, mode="regress", outcome=outcome)
    
    # Should have recommendations to raise score_min
    meanr_recs = [r for r in recs if "meanR" in r.get("why", "") and "of_score_min" in r.get("key", "")]
    assert len(meanr_recs) > 0


def test_recs_with_outcome_low_bigwin():
    """Test recommendations when outcome shows low bigwin rate"""
    os.environ["RECS_ENABLE"] = "1"
    os.environ["RECS_USE_OUTCOME"] = "1"
    os.environ["CFG_TARGET_SYMBOLS"] = "BTCUSDT"
    os.environ["OUTCOME_MIN_N"] = "10"
    os.environ["OUTCOME_BIGWIN_MIN"] = "0.10"
    os.environ["OUTCOME_TAIL_LOSS_MAX"] = "0.18"
    os.environ["CFG_HASH_PREFIX"] = "config:orderflow:"
    
    stats = ReplayStats(
        n=100,
        ok_rate=0.5,
        ok_soft_rate=0.0,
        no_data=0,
        by_scenario={},
        exec_risk_norm_p50=0.5,
        exec_risk_norm_p90=0.7,
        vol_shock_cap_hit_rate=0.1,
        saw_chop_hard_miss_rate=0.1,
        top_missing_legs=[],
    )
    
    outcome = {
        "n": 100,
        "bigwin_rate": 0.05,  # Below threshold
        "tail_loss_rate": 0.10,  # Below threshold (safe to relax)
        "meanR": 0.15,
    }
    
    recs = propose_cfg_recs(stats, mode="regress", outcome=outcome)
    
    # Should have recommendations to slightly lower score_min
    bigwin_recs = [r for r in recs if "bigwin" in r.get("why", "").lower() and "of_score_min" in r.get("key", "")]
    assert len(bigwin_recs) > 0


def test_recs_with_outcome_disabled():
    """Test that outcome recommendations are disabled when RECS_USE_OUTCOME=0"""
    os.environ["RECS_ENABLE"] = "1"
    os.environ["RECS_USE_OUTCOME"] = "0"
    os.environ["CFG_TARGET_SYMBOLS"] = "BTCUSDT"
    os.environ["OUTCOME_MIN_N"] = "10"
    os.environ["OUTCOME_TAIL_LOSS_MAX"] = "0.18"
    os.environ["CFG_HASH_PREFIX"] = "config:orderflow:"
    
    stats = ReplayStats(
        n=100,
        ok_rate=0.5,
        ok_soft_rate=0.0,
        no_data=0,
        by_scenario={},
        exec_risk_norm_p50=0.5,
        exec_risk_norm_p90=0.7,
        vol_shock_cap_hit_rate=0.1,
        saw_chop_hard_miss_rate=0.1,
        top_missing_legs=[],
    )
    
    outcome = {
        "n": 100,
        "tail_loss_rate": 0.25,  # Above threshold, but should be ignored
        "meanR": 0.15,
        "bigwin_rate": 0.10,
    }
    
    recs = propose_cfg_recs(stats, mode="regress", outcome=outcome)
    
    # Should NOT have outcome-based recommendations
    outcome_recs = [r for r in recs if "outcome" in r.get("why", "").lower()]
    assert len(outcome_recs) == 0


def test_recs_with_outcome_insufficient_n():
    """Test that outcome recommendations are skipped when n < OUTCOME_MIN_N"""
    os.environ["RECS_ENABLE"] = "1"
    os.environ["RECS_USE_OUTCOME"] = "1"
    os.environ["CFG_TARGET_SYMBOLS"] = "BTCUSDT"
    os.environ["OUTCOME_MIN_N"] = "100"
    os.environ["OUTCOME_TAIL_LOSS_MAX"] = "0.18"
    os.environ["CFG_HASH_PREFIX"] = "config:orderflow:"
    
    stats = ReplayStats(
        n=100,
        ok_rate=0.5,
        ok_soft_rate=0.0,
        no_data=0,
        by_scenario={},
        exec_risk_norm_p50=0.5,
        exec_risk_norm_p90=0.7,
        vol_shock_cap_hit_rate=0.1,
        saw_chop_hard_miss_rate=0.1,
        top_missing_legs=[],
    )
    
    outcome = {
        "n": 50,  # Below threshold
        "tail_loss_rate": 0.25,  # Above threshold, but should be ignored due to low n
        "meanR": 0.15,
        "bigwin_rate": 0.10,
    }
    
    recs = propose_cfg_recs(stats, mode="regress", outcome=outcome)
    
    # Should NOT have outcome-based recommendations
    outcome_recs = [r for r in recs if "outcome" in r.get("why", "").lower()]
    assert len(outcome_recs) == 0


@pytest.fixture(autouse=True)
def cleanup_env():
    """Clean up environment variables after each test"""
    yield
    # Clean up test-specific env vars
    for key in [
        "RECS_ENABLE",
        "RECS_USE_OUTCOME",
        "CFG_TARGET_SYMBOLS",
        "CANARY_SYMBOLS",
        "EXEC_RISK_NORM_P90_WARN",
        "PASS_RATE_MIN",
        "VOL_SHOCK_CAP_HIT_WARN",
        "SAW_CHOP_HARD_MISS_WARN",
        "RECS_STEP_W_EXEC",
        "RECS_STEP_EXEC_REF_BPS",
        "RECS_STEP_SCORE_MIN",
        "RECS_VOL_SHOCK_FAIL_CLOSED_ON_CAP",
        "RECS_SAW_CHOP_FAIL_CLOSED_ON_MISS",
        "CFG_HASH_PREFIX",
        "OUTCOME_MIN_N",
        "OUTCOME_TAIL_LOSS_MAX",
        "OUTCOME_MEANR_MIN",
        "OUTCOME_BIGWIN_MIN",
    ]:
        os.environ.pop(key, None)

