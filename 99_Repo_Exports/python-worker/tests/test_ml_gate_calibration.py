
from unittest.mock import Mock

import pytest
import redis

from services.ml_calibration import PlattLogitCalibrator, clip_prob, logit, sigmoid
from services.ml_confirm_gate import MLConfirmGate


class DummyUtilMH:
    feature_cols = ["f_spread_bps"]
    horizons = [60000]
    unc_k = 0.5

    def predict_util(self, X):
        return {60000: [1.0]} # positive score

    def predict_unc(self, X):
        return {60000: [0.1]}

@pytest.fixture
def gate():
    r = Mock(spec=redis.Redis)
    r.get = Mock(return_value=None)
    g = MLConfirmGate(
        r=r,
        mode="SHADOW",
        fail_policy="OPEN",
        champion_key="k1",
        challenger_key="k2"
    )
    g._cache_loaded_ms = 9999999999999
    return g

def test_gate_loads_calibrator_from_cfg(gate):
    # Prepare a config with calibration params
    # a=2.0 (steeper slope), b=0.5 (shift)
    cal_params = {"type": "platt_logit", "a": 2.0, "b": 0.5, "eps": 1e-6}

    gate._cfg = {
        "kind": "util_mh_v1",
        "run_id": "test_calib",
        "calibrator": cal_params,
        "util_floors": {"global": {"floor": 0.01}},
    }
    gate._model = DummyUtilMH()

    # We need to trigger _refresh_cache_if_needed behavior manually or mock it
    # _refresh_cache_if_needed reads from _cfg if loaded.
    # But it also parses 'calibrator' key.
    # Since we injected _cfg directly, we need to call the parsing logic
    # OR we can rely on _refresh_cache_if_needed logic which calls _load_cfg_and_model.
    # To test strictly, we should let _refresh_cache_if_needed run logic.
    # But _refresh_cache_if_needed fetches from Redis.

    # Let's verify _decide_util_mh logic directly, assuming _calibrator is set.
    gate._calibrator = PlattLogitCalibrator.from_dict(cal_params)
    gate._calib_type = "platt_logit"

    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=1000,
        direction="LONG",
        scenario="trend",
        indicators={"spread_bps": 1.0, "expected_slippage_bps": 1.0},
        rule_score=1.0, rule_have=1, rule_need=1, cancel_spike_veto=0, ok_rule=1
    )

    # Score = 1.0 - 0.5*0.1 = 0.95
    # Scale: base=2.5. 0.95 is in [-5, 5]. scale=2.5.
    # scaled = 0.95 * 2.5 = 2.375
    # p_raw = sigmoid(2.375)
    # p_raw approx 0.9149

    score = 0.95
    scaled = score * 2.5
    p_raw_expected = sigmoid(scaled)

    # Calibrated: sigm(2.0 * logit(p_raw) + 0.5)
    # logit(p_raw) = log(p/(1-p)) = scaled (roughly, if sigmoid inverse)
    # Actually logit(sigmoid(x)) = x
    # So logit(p_raw) = 2.375
    # z_cal = 2.0 * 2.375 + 0.5 = 4.75 + 0.5 = 5.25
    # p_cal = sigmoid(5.25)

    lr = logit(clip_prob(p_raw_expected))
    p_cal_expected = sigmoid(2.0 * lr + 0.5)

    assert dec.p_edge_raw == pytest.approx(p_raw_expected, abs=1e-6)
    assert dec.p_edge_cal == pytest.approx(p_cal_expected, abs=1e-6)
    assert dec.p_edge == dec.p_edge_cal
    assert dec.calib_type == "platt_logit"

def test_gate_calibration_disabled(gate):
    # Same setup but without calibrator
    gate._cfg = {
        "kind": "util_mh_v1",
        "run_id": "test_calib",
        "util_floors": {"global": {"floor": 0.01}},
    }
    gate._model = DummyUtilMH()
    gate._calibrator = None
    gate._calib_type = "none"

    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=1000,
        direction="LONG",
        scenario="trend",
        indicators={"spread_bps": 1.0, "expected_slippage_bps": 1.0},
        rule_score=1.0, rule_have=1, rule_need=1, cancel_spike_veto=0, ok_rule=1
    )

    assert dec.p_edge == dec.p_edge_raw
    assert dec.calib_type == "none"

