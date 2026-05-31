from common.conf_thresholds import compute_conf_thresholds, should_log_edge_veto


def test_compute_conf_thresholds_defaults_only():
    env = {}
    min_conf, min_cf = compute_conf_thresholds(env, "BTCUSDT")
    assert min_conf == 70.0
    assert min_cf == 0.45


def test_compute_conf_thresholds_with_global_defaults():
    env = {"MIN_CONF_DEFAULT": "75", "MIN_CONF_FACTOR_DEFAULT": "0.5"}
    min_conf, min_cf = compute_conf_thresholds(env, "BTCUSDT")
    assert min_conf == 75.0
    assert min_cf == 0.5


def test_compute_conf_thresholds_with_symbol_override():
    env = {
        "MIN_CONF_DEFAULT": "70",
        "MIN_CONF_FACTOR_DEFAULT": "0.45",
        "MIN_CONF_BTCUSDT": "82",
        "MIN_CONF_FACTOR_BTCUSDT": "0.61",
    }
    min_conf, min_cf = compute_conf_thresholds(env, "BTCUSDT")
    assert min_conf == 82.0
    assert min_cf == 0.61


def test_should_log_edge_veto():
    assert should_log_edge_veto({"LOG_EDGE_VETO": "1"}) is True
    assert should_log_edge_veto({"LOG_EDGE_VETO": "true"}) is True
    assert should_log_edge_veto({"LOG_EDGE_VETO": "0"}) is False
    assert should_log_edge_veto({}) is False
