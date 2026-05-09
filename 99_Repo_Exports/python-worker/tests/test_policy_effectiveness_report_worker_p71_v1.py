import json
from unittest.mock import MagicMock, patch

from orderflow_services.policy_effectiveness_report_worker_v1 import (
    _build_mode_kpis,
    _to_float,
    _to_int,
    run_once,
)


def test_to_int():
    assert _to_int(" 123 ") == 123
    assert _to_int(45) == 45
    assert _to_int(b"67") == 67
    assert _to_int(None, default=10) == 10
    assert _to_int("abc", default=5) == 5
    assert _to_int(" 12.5 ") == 12

def test_to_float():
    assert _to_float(" 123.45 ") == 123.45
    assert _to_float(None, default=1.1) == 1.1
    assert _to_float("nan") == 0.0
    assert _to_float("abc", default=2.2) == 2.2

def test_build_mode_kpis():
    cfg = {
        "signal_quality_n_24h_policy_ok": "100",
        "signal_quality_expectancy_r_24h_policy_ok": "0.5",
        "signal_quality_precision_top5p_24h_policy_ok": "0.1",
        "signal_quality_ece_24h_policy_ok": "0.05",

        "signal_quality_n_24h_policy_warn": "50",
        "signal_quality_expectancy_r_24h_policy_warn": "0.2",
        "signal_quality_precision_top5p_24h_policy_warn": "0.05",
        "signal_quality_ece_24h_policy_warn": "0.1",
    }

    kpis = _build_mode_kpis(cfg)

    assert "ok" in kpis
    assert kpis["ok"].n == 100
    assert kpis["ok"].expectancy_r == 0.5
    assert kpis["ok"].precision_top5p == 0.1
    assert kpis["ok"].ece == 0.05

    assert "warn" in kpis
    assert kpis["warn"].n == 50
    assert kpis["warn"].expectancy_r == 0.2

    assert "block" in kpis
    assert kpis["block"].n == 0

@patch("orderflow_services.policy_effectiveness_report_worker_v1._redis")
@patch("orderflow_services.policy_effectiveness_report_worker_v1._now_ms", return_value=123456789)
@patch("orderflow_services.policy_effectiveness_report_worker_v1.os.environ.get")
def test_run_once(mock_env_get, mock_now_ms, mock_redis):
    # Setup mocks
    mock_env_get.side_effect = lambda k, d="": {"DYN_CFG_KEY": "settings:dynamic_cfg", "POLICY_EFF_BASELINE_MIN_N": "40"}.get(k, d)

    r_mock = MagicMock()
    mock_redis.return_value = r_mock

    # Mock HGETALL data
    r_mock.hgetall.return_value = {
        b"signal_quality_policy_mode_last_ts_ms": b"987654321",
        b"signal_quality_n_24h_policy_ok": b"100",
        b"signal_quality_expectancy_r_24h_policy_ok": b"1.0",
        b"signal_quality_precision_top5p_24h_policy_ok": b"0.8",
        b"signal_quality_ece_24h_policy_ok": b"0.1",
        b"signal_quality_n_24h_policy_warn": b"50",
        b"signal_quality_expectancy_r_24h_policy_warn": b"0.2",
        b"signal_quality_precision_top5p_24h_policy_warn": b"0.1",
        b"signal_quality_ece_24h_policy_warn": b"0.5",
    }

    # Run
    rc = run_once()
    assert rc == 0

    # Assert dyn_cfg hset
    r_mock.hset.assert_called_once()
    args, kwargs = r_mock.hset.call_args
    assert args[0] == "settings:dynamic_cfg"
    mapping = kwargs["mapping"]
    assert mapping["policy_effectiveness_baseline_ok_present"] == "1"
    assert mapping["policy_effectiveness_total_n_24h"] == "150"

    assert mapping["policy_effectiveness_expectancy_r_delta_24h_ok"] == "0.000000"
    assert mapping["policy_effectiveness_expectancy_r_delta_24h_warn"] == "-0.800000"

    # Assert json/csv sets
    assert r_mock.set.call_count == 2
    calls = r_mock.set.call_args_list
    assert calls[0][0][0] == "reports:policy_effectiveness:p71:last_json"

    report = json.loads(calls[0][0][1])
    assert report["baseline"]["present"] == 1
    assert report["total_n"] == 150
    assert report["input_last_ts_ms"] == 987654321

    assert calls[1][0][0] == "reports:policy_effectiveness:p71:last_csv"
    csv_data = calls[1][0][1]
    assert "ok,100,0.666667,1.000000,0.800000,0.100000,0.000000,0.000000,0.000000" in csv_data
    assert "warn,50,0.333333,0.200000,0.100000,0.500000,-0.800000,-0.700000,0.400000" in csv_data
