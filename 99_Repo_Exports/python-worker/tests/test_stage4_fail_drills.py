from unittest.mock import MagicMock

import redis

from services.ml_confirm import MLConfirmGate


class MockRedis:
    def __init__(self, fail=False):
        self.fail = fail
    def get(self, k):
        if self.fail: raise redis.ConnectionError("Connection host not found")
        return None
    def xadd(self, *args, **kwargs):
        if self.fail: raise redis.ConnectionError("Redis down")

def test_missing_model_file_fail_open():
    """Scenario A: Model file is missing, fail_policy=OPEN -> ALLOW."""
    gate = MLConfirmGate(
        r=MockRedis(),
        mode="ENFORCE",
        fail_policy="OPEN",
        champion_key="cfg:champ",
        challenger_key="cfg:chall"
    )

    # Force model path to non-existent and set kind
    gate._cfg = {"kind": "edge_stack_v1", "model_path": "/non/existent/model.json"}
    gate._model = None
    gate._model_load_error = "file_not_found"

    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=1700000000000,
        direction="LONG",
        scenario="reversal",
        indicators={"sid": "test"},
        rule_score=0.5,
        rule_have=5,
        rule_need=5,
        cancel_spike_veto=0,
        ok_rule=1
    )

    assert dec.allow is True
    assert "file_not_found" in dec.reason

def test_missing_model_file_fail_closed():
    """Scenario A: Model file is missing, fail_policy=CLOSED -> DENY."""
    gate = MLConfirmGate(
        r=MockRedis(),
        mode="ENFORCE",
        fail_policy="CLOSED",
        champion_key="cfg:champ",
        challenger_key="cfg:chall"
    )

    gate._cfg = {"kind": "edge_stack_v1", "model_path": "/non/existent/model.json"}
    gate._model = None
    gate._model_load_error = "file_not_found"

    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=1700000000000,
        direction="LONG",
        scenario="reversal",
        indicators={"sid": "test"},
        rule_score=0.5,
        rule_have=5,
        rule_need=5,
        cancel_spike_veto=0,
        ok_rule=1
    )

    assert dec.allow is False
    assert "file_not_found" in dec.reason

def test_redis_connection_error_handling():
    """Scenario B: Redis is down during check -> should fail-open if OPEN or shadow."""
    gate = MLConfirmGate(
        r=MockRedis(fail=True),
        mode="SHADOW", # SHADOW always allows
        fail_policy="OPEN",
        champion_key="cfg:champ",
        challenger_key="cfg:chall"
    )

    gate._cfg = {"kind": "edge_stack_v1"}
    gate._model = None # ensure it uses fail_allow logic

    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=1700000000000,
        direction="LONG",
        scenario="reversal",
        indicators={"sid": "test"},
        rule_score=0.5,
        rule_have=5,
        rule_need=5,
        cancel_spike_veto=0,
        ok_rule=1
    )

    assert dec.allow is True

def test_bad_json_config():
    """Scenario C: Bad JSON in Redis -> handle gracefully."""
    mr = MagicMock()
    mr.get.return_value = "invalid { json"

    gate = MLConfirmGate(
        r=mr,
        mode="ENFORCE",
        fail_policy="OPEN",
        champion_key="cfg:champ",
        challenger_key="cfg:chall"
    )

    gate._cfg = {"kind": "edge_stack_v1"}
    gate._model = None
    gate._model_load_error = "no_model"

    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=1700000000000,
        direction="LONG",
        scenario="reversal",
        indicators={"sid": "test"},
        rule_score=0.5,
        rule_have=5,
        rule_need=5,
        cancel_spike_veto=0,
        ok_rule=1
    )

    assert dec.allow is True # fail-policy OPEN
