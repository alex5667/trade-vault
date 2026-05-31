
import os

from services.ml_confirm import MLConfirmGate


def test_ml_gate_off_allows():
    os.environ["ML_CONFIRM_MODE"] = "OFF"
    os.environ["ML_CONFIRM_FAIL_POLICY"] = "CLOSED"
    os.environ["ML_CONFIRM_MODEL_PATH"] = ""  # missing ok
    g = MLConfirmGate.from_env()
    dec = g.check(
        symbol="BTCUSDT",
        ts_ms=1,
        direction="LONG",
        scenario="reversal",
        indicators={"delta_z": 1.0},
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )
    assert dec.allow is True
    assert dec.mode == "OFF"

def test_ml_gate_enforce_fail_closed_blocks():
    os.environ["ML_CONFIRM_MODE"] = "ENFORCE"
    os.environ["ML_CONFIRM_FAIL_POLICY"] = "CLOSED"
    os.environ["ML_CONFIRM_MODEL_PATH"] = ""  # missing -> error -> fail_closed blocks if ok_rule==1
    g = MLConfirmGate.from_env()
    dec = g.check(
        symbol="BTCUSDT",
        ts_ms=1,
        direction="LONG",
        scenario="reversal",
        indicators={"delta_z": 1.0},
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )
    assert dec.allow is False

def test_ml_gate_enforce_fail_open_allows():
    os.environ["ML_CONFIRM_MODE"] = "ENFORCE"
    os.environ["ML_CONFIRM_FAIL_POLICY"] = "OPEN"
    os.environ["ML_CONFIRM_MODEL_PATH"] = ""
    g = MLConfirmGate.from_env()
    dec = g.check(
        symbol="BTCUSDT",
        ts_ms=1,
        direction="LONG",
        scenario="reversal",
        indicators={"delta_z": 1.0},
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )
    assert dec.allow is True
