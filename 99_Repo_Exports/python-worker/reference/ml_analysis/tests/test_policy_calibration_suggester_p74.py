import importlib.util
from pathlib import Path


def _load_module():
    repo_root = Path(__file__).resolve().parents[2]
    p = repo_root / "tools" / "policy_calibration_suggester_p74.py"
    spec = importlib.util.spec_from_file_location("policy_calibration_suggester_p74", p)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod


def test_baseline_missing_no_actions():
    m = _load_module()
    now_ms = 1700000000000
    cfg2 = {
        "policy_effectiveness_last_ts_ms": str(now_ms)
        "policy_regime_effectiveness_last_ts_ms": str(now_ms)
        "policy_effectiveness_baseline_ok_present": "0"
        "policy_effectiveness_share_24h_ok": "0.0"
        "policy_effectiveness_share_24h_warn": "0.2"
        "policy_effectiveness_share_24h_block": "0.1"
        "policy_effectiveness_share_24h_unknown": "0.0"
        "policy_effectiveness_expectancy_r_delta_24h_warn": "-0.5"
        "policy_regime_effectiveness_worst_warn_expectancy_r_delta": "-0.5"
    }
    s = m.build_suggestion(cfg2, now_ms=now_ms)
    assert s["warn"]["action_code"] == 0
    assert s["block"]["action_code"] == 0
    assert s["ok_baseline_present"] == 0


def test_tighten_warn_when_high_severity_and_share():
    m = _load_module()
    now_ms = 1700000000000
    cfg2 = {
        "policy_effectiveness_last_ts_ms": str(now_ms)
        "policy_regime_effectiveness_last_ts_ms": str(now_ms)
        "policy_effectiveness_baseline_ok_present": "1"
        "policy_effectiveness_share_24h_ok": "0.7"
        "policy_effectiveness_share_24h_warn": "0.2"
        "policy_effectiveness_share_24h_block": "0.1"
        "policy_effectiveness_share_24h_unknown": "0.0"
        "policy_effectiveness_expectancy_r_delta_24h_warn": "-0.3"
        "policy_effectiveness_precision_top5p_delta_24h_warn": "-0.05"
        "policy_effectiveness_ece_delta_24h_warn": "0.05"
        "policy_regime_effectiveness_worst_warn_expectancy_r_delta": "-0.4"
        "policy_regime_effectiveness_worst_warn_precision_top5p_delta": "-0.05"
        "policy_regime_effectiveness_worst_warn_ece_delta": "0.05"
    }
    s = m.build_suggestion(cfg2, now_ms=now_ms)
    assert s["warn"]["severity"] > 1.0
    assert s["warn"]["action_code"] == 1


def test_loosen_warn_when_high_share_low_severity():
    m = _load_module()
    now_ms = 1700000000000
    cfg2 = {
        "policy_effectiveness_last_ts_ms": str(now_ms)
        "policy_regime_effectiveness_last_ts_ms": str(now_ms)
        "policy_effectiveness_baseline_ok_present": "1"
        "policy_effectiveness_share_24h_ok": "0.45"
        "policy_effectiveness_share_24h_warn": "0.50"
        "policy_effectiveness_share_24h_block": "0.05"
        "policy_effectiveness_share_24h_unknown": "0.0"
        "policy_effectiveness_expectancy_r_delta_24h_warn": "-0.02"
        "policy_effectiveness_precision_top5p_delta_24h_warn": "-0.01"
        "policy_effectiveness_ece_delta_24h_warn": "0.01"
        "policy_regime_effectiveness_worst_warn_expectancy_r_delta": "-0.02"
        "policy_regime_effectiveness_worst_warn_precision_top5p_delta": "-0.01"
        "policy_regime_effectiveness_worst_warn_ece_delta": "0.01"
    }
    s = m.build_suggestion(cfg2, now_ms=now_ms)
    assert s["warn"]["severity"] < 0.5
    assert s["warn"]["action_code"] == -1


def test_inputs_stale_flag():
    m = _load_module()
    now_ms = 1700000000000
    old_ms = now_ms - 4 * 3600 * 1000
    cfg2 = {
        "policy_effectiveness_last_ts_ms": str(old_ms)
        "policy_regime_effectiveness_last_ts_ms": str(old_ms)
        "policy_effectiveness_baseline_ok_present": "1"
        "policy_effectiveness_share_24h_ok": "0.7"
        "policy_effectiveness_share_24h_warn": "0.2"
        "policy_effectiveness_share_24h_block": "0.1"
        "policy_effectiveness_share_24h_unknown": "0.0"
    }
    s = m.build_suggestion(cfg2, now_ms=now_ms, stale_max_sec=7200)
    assert s["inputs_stale"] == 1
