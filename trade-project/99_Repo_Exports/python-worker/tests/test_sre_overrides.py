
import json
from typing import Any

from core.orderflow_overrides_v1 import OrderflowOverridesV1, RolloutV1


def test_overrides_schema_ok():
    raw = json.dumps({
        "v": 1,
        "enabled": 1,
        "updated_ts_ms": 123,
        "abs_lvl_tier_trend": 0,
        "strong_need_reversal": 3,
        "rollout": {
            "mode": "full"
        }
    })
    o, reason = OrderflowOverridesV1.from_json(raw)
    assert o is not None
    assert reason == "ok"
    assert o.abs_lvl_tier_trend == 0
    assert o.strong_need_reversal == 3

def test_overrides_schema_bad_types():
    raw = json.dumps({
        "v": 1,
        "enabled": "yes",  # should be int, but we are lenient in _i helper
        "abs_lvl_tier_trend": "zero" # -> -1 -> invalid
    })
    o, reason = OrderflowOverridesV1.from_json(raw)
    # The validation checks valid ranges (-1 is invalid for tier)
    assert o is None
    assert reason == "abs_lvl_tier_trend"

def test_overrides_fail_open_bad_v():
    raw = json.dumps({"v": 2})
    o, reason = OrderflowOverridesV1.from_json(raw)
    assert o is None
    assert reason == "v"

def test_overrides_application():
    o = OrderflowOverridesV1(
        abs_lvl_tier_trend=2,
        burst_window_min_ms=500
    )
    base_cfg: dict[str, Any] = {"abs_lvl_tier_trend": 0, "other": 123}
    new_cfg = o.apply_to_cfg(base_cfg)

    assert new_cfg["abs_lvl_tier_trend"] == 2
    assert new_cfg["other"] == 123
    assert new_cfg["burst_window_min_ms"] == 500
    # base intact?
    assert base_cfg["abs_lvl_tier_trend"] == 0

def test_rollout_canary_match():
    rr = RolloutV1(mode="canary", canary_symbols=["BTCUSDT"])
    o = OrderflowOverridesV1(rollout=rr, abs_lvl_tier_trend=2)

    # Simulate usage
    symbol = "BTCUSDT"
    target = True
    if o.rollout.mode == "canary":
         if symbol not in o.rollout.canary_symbols:
             target = False

    assert target is True

    symbol = "ETHUSDT"
    target = True
    if o.rollout.mode == "canary":
         if symbol not in o.rollout.canary_symbols:
             target = False
    assert target is False
