import pytest
from unittest.mock import MagicMock, patch
from services.ml_confirm_gate import MLConfirmGate, MLConfirmDecision

@pytest.fixture
def mock_redis():
    mock = MagicMock()
    return mock

def _create_decision(**kwargs):
    dec = MLConfirmDecision()
    for k, v in kwargs.items():
        setattr(dec, k, v)
    return dec

def test_ml_confirm_gate_from_env(mock_redis):
    with patch("services.ml_confirm_gate.redis.Redis.from_url", return_value=mock_redis):
        gate = MLConfirmGate.from_env()
        assert gate is not None
        assert gate.champion_key == "cfg:ml_confirm:champion"

def test_ml_decision_to_dict():
    dec = _create_decision(
        mode="ENFORCE",
        allow=False,
        p_edge=0.45,
        p_min=0.55,
        model_ver="test",
        latency_us=1000,
        missing=["f1"],
        error="",
        reason="block"
    )
    d = dec.to_dict()
    assert d["mode"] == "ENFORCE"
    assert d["allow"] is False

def test_ml_confirm_gate_off_mode(mock_redis):
    gate = MLConfirmGate(r=mock_redis, mode="OFF", fail_policy="OPEN", champion_key="cfg:ml_confirm:champion", challenger_key="cfg:ml_confirm:challenger")
    assert gate.mode == "OFF"
