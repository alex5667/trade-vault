from __future__ import annotations
"""
Тесты для replay capture, метрик и selective prediction в MLConfirmGate.
"""

from utils.time_utils import get_ny_time_millis

import json
import os
import time
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from services.ml_confirm_gate import (
    MLConfirmGate,
    MLConfirmDecision,
    _json_safe,
)


@pytest.fixture
def mock_redis():
    """Mock Redis client."""
    r = MagicMock()
    r.get.return_value = None
    r.xadd.return_value = "12345-0"
    return r


@pytest.fixture
def gate(mock_redis):
    """MLConfirmGate instance with mocked Redis."""
    return MLConfirmGate(
        r=mock_redis,
        mode="SHADOW",
        fail_policy="OPEN",
        champion_key="cfg:ml_confirm:champion",
        challenger_key="cfg:ml_confirm:challenger",
    )


class DummyUtilMH:
    """Mock model compatible with v10.4 util_mh_v1."""
    feature_cols = [
        "f_spread_bps",
        "f_expected_slippage_bps",
        "f_exec_risk_norm",
        "direction_LONG",
        "scenario_v4_range_meanrev",
    ]
    horizons = [60000, 180000]
    unc_k = 0.5

    def predict_util(self, X):
        return {60000: np.array([0.01]), 180000: np.array([0.05])}

    def predict_unc(self, X):
        return {60000: np.array([0.02]), 180000: np.array([0.01])}


def test_json_safe_basic():
    """Test _json_safe with basic types."""
    assert _json_safe(None) is None
    assert _json_safe("test") == "test"
    assert _json_safe(42) == 42
    assert _json_safe(3.14) == 3.14
    assert _json_safe(True) is True
    assert _json_safe(False) is False


def test_json_safe_bytes():
    """Test _json_safe with bytes."""
    assert _json_safe(b"test") == "test"
    assert _json_safe(b"\xff\xfe") == ""  # fallback to str with ignore


def test_json_safe_list():
    """Test _json_safe with lists."""
    result = _json_safe([1, 2.0, "test", None, b"bytes"])
    assert result == [1, 2.0, "test", None, "bytes"]


def test_json_safe_dict():
    """Test _json_safe with dicts."""
    result = _json_safe({"a": 1, "b": 2.0, "c": "test", "d": None})
    assert result == {"a": 1, "b": 2.0, "c": "test", "d": None}


def test_json_safe_numpy():
    """Test _json_safe with numpy scalars."""
    result = _json_safe(np.float32(3.14))
    assert isinstance(result, (float, int))


def test_conf_from_margin():
    """Test _conf_from_margin calculation."""
    # margin = 0 -> conf = 0
    conf = MLConfirmGate._conf_from_margin(0.0)
    assert conf == 0.0

    # margin > 0 -> conf > 0
    conf = MLConfirmGate._conf_from_margin(0.1)
    assert 0.0 < conf < 1.0

    # margin large -> conf close to 1
    conf = MLConfirmGate._conf_from_margin(5.0)
    assert conf > 0.9

    # negative margin -> same as positive (abs)
    conf_pos = MLConfirmGate._conf_from_margin(0.1)
    conf_neg = MLConfirmGate._conf_from_margin(-0.1)
    assert conf_pos == conf_neg


def test_apply_selective_band(gate):
    """Test _apply_selective with abstain_band."""
    gate.mode = "ENFORCE"
    gate._abstain_band = 0.02
    gate._conf_min = 0.0

    dec = MLConfirmDecision(
        mode="ENFORCE",
        allow=False,
        p_edge=0.56,
        p_min=0.55,
        p_margin=0.01,
        conf=0.1,
        status="BLOCK",
    )

    # margin = 0.01 <= band = 0.02 -> should abstain
    gate._apply_selective(dec, ok_rule=1)
    assert dec.abstain is True
    assert dec.allow is True
    assert dec.status == "ABSTAIN_BAND"
    assert "ml_abstain_band" in dec.reason


def test_apply_selective_lowconf(gate):
    """Test _apply_selective with conf_min."""
    gate.mode = "ENFORCE"
    gate._abstain_band = 0.0
    gate._conf_min = 0.2

    dec = MLConfirmDecision(
        mode="ENFORCE",
        allow=False,
        p_edge=0.56,
        p_min=0.55,
        p_margin=0.01,
        conf=0.1,  # < conf_min
        status="BLOCK",
    )

    gate._apply_selective(dec, ok_rule=1)
    assert dec.abstain is True
    assert dec.allow is True
    assert dec.status == "ABSTAIN_LOWCONF"
    assert "ml_abstain_lowconf" in dec.reason


def test_apply_selective_shadow_mode(gate):
    """Test _apply_selective in SHADOW mode (no abstain)."""
    gate.mode = "SHADOW"
    dec = MLConfirmDecision(mode="SHADOW", allow=True, p_edge=0.5, p_min=0.4, p_margin=0.1, conf=0.1)
    gate._apply_selective(dec, ok_rule=1)
    assert dec.status == "SHADOW" or dec.status == ""
    assert dec.abstain is False


def test_emit_metrics_enabled(gate):
    """Test _emit_metrics when enabled."""
    gate._metrics_enable = True
    gate._metrics_sample = 1.0
    gate._metrics_stream = "metrics:ml_confirm"

    dec = MLConfirmDecision(
        mode="ENFORCE",
        allow=True,
        kind="util_mh_v1",
        p_edge=0.6,
        p_min=0.55,
        p_margin=0.05,
        conf=0.3,
        best_h_ms=60000,
        floor=0.55,
        missing=[],
        error="",
        reason="test",
        latency_us=1500,
        abstain=False,
        status="ALLOW",
        model_run_id="test_run",
    )

    gate._emit_metrics(
        dec,
        symbol="BTCUSDT",
        ts_ms=1000000,
        direction="LONG",
        scenario="range_meanrev",
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )

    assert gate.r.xadd.called
    call_args = gate.r.xadd.call_args
    assert call_args[0][0] == "metrics:ml_confirm"
    fields = call_args[0][1]
    assert fields["symbol"] == "BTCUSDT"
    assert fields["allow"] == 1
    assert fields["abstain"] == 0
    assert fields["status"] == "ALLOW"
    assert "latency_us" in fields
    assert "latency_ms" in fields


def test_emit_metrics_disabled(gate):
    """Test _emit_metrics when disabled."""
    gate._metrics_enable = False
    dec = MLConfirmDecision(mode="OFF", allow=True)
    gate._emit_metrics(dec, symbol="BTCUSDT", ts_ms=1000000, direction="LONG", scenario="test",
                       rule_score=0.0, rule_have=0, rule_need=0, cancel_spike_veto=0, ok_rule=0)
    assert not gate.r.xadd.called


def test_capture_replay_input_enabled(gate):
    """Test _capture_replay_input when enabled."""
    gate._replay_capture = True
    gate._replay_sample = 1.0
    gate._replay_stream = "stream:ml_confirm:inputs"
    gate._replay_maxlen = 200000
    gate._cfg = {
        "kind": "util_mh_v1",
        "run_id": "test_run",
        "model_path": "/tmp/test.pkl",
        "util_floors": {"global": {"floor": 0.03}},
    }

    dec = MLConfirmDecision(
        mode="ENFORCE",
        allow=True,
        model_run_id="test_run",
    )

    indicators = {
        "spread_bps": 2.0,
        "expected_slippage_bps": 1.5,
        "exec_risk_norm": 0.3,
    }

    gate._capture_replay_input(
        dec,
        symbol="BTCUSDT",
        ts_ms=1000000,
        direction="LONG",
        scenario="range_meanrev",
        indicators=indicators,
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )

    assert gate.r.xadd.called
    call_args = gate.r.xadd.call_args
    assert call_args[0][0] == "stream:ml_confirm:inputs"
    fields = call_args[0][1]
    assert "payload" in fields
    payload = json.loads(fields["payload"])
    assert payload["symbol"] == "BTCUSDT"
    assert payload["ts_ms"] == 1000000
    assert payload["direction"] == "LONG"
    assert "indicators" in payload
    assert payload["indicators"]["spread_bps"] == 2.0


def test_capture_replay_input_disabled(gate):
    """Test _capture_replay_input when disabled."""
    gate._replay_capture = False
    dec = MLConfirmDecision(mode="OFF", allow=True)
    gate._capture_replay_input(dec, symbol="BTCUSDT", ts_ms=1000000, direction="LONG", scenario="test",
                                indicators={}, rule_score=0.0, rule_have=0, rule_need=0, cancel_spike_veto=0, ok_rule=0)
    assert not gate.r.xadd.called


def test_check_with_metrics_and_replay(gate):
    """Test check() method emits metrics and captures replay when enabled."""
    gate.mode = "ENFORCE"
    gate._metrics_enable = True
    gate._metrics_sample = 1.0
    gate._replay_capture = True
    gate._replay_sample = 1.0

    gate._cfg = {
        "kind": "util_mh_v1",
        "run_id": "test_run",
        "model_path": "/tmp/test.pkl",
        "util_floors": {
            "global": {"floor": 0.03},
            "by_bucket": {},
        },
    }
    gate._model = DummyUtilMH()
    gate._cache_loaded_ms = get_ny_time_millis()

    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=get_ny_time_millis(),
        direction="LONG",
        scenario="range_meanrev",
        indicators={"spread_bps": 2.0, "expected_slippage_bps": 2.0},
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )

    assert dec.latency_us > 0
    assert dec.status in ["ALLOW", "BLOCK", "ABSTAIN_BAND", "ABSTAIN_LOWCONF"]
    # Metrics should be emitted
    assert gate.r.xadd.call_count >= 1


def test_check_latency_tracking(gate):
    """Test that check() tracks latency correctly."""
    gate.mode = "OFF"
    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=get_ny_time_millis(),
        direction="LONG",
        scenario="test",
        indicators={},
        rule_score=0.0,
        rule_have=0,
        rule_need=0,
        cancel_spike_veto=0,
        ok_rule=0,
    )
    assert dec.latency_us >= 0
    assert dec.status == "OFF"


def test_decision_to_dict_includes_new_fields():
    """Test that to_dict() includes new fields."""
    dec = MLConfirmDecision(
        mode="ENFORCE",
        allow=True,
        p_edge=0.6,
        p_min=0.55,
        latency_us=1500,
        abstain=True,
        conf=0.3,
        p_margin=0.05,
        status="ABSTAIN_BAND",
    )
    d = dec.to_dict()
    assert d["latency_us"] == 1500
    assert d["abstain"] == 1
    assert d["conf"] == 0.3
    assert d["p_margin"] == 0.05
    assert d["status"] == "ABSTAIN_BAND"


