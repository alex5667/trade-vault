from __future__ import annotations

import json

import pytest

pytest.importorskip('redis')

from services.orderflow.configuration import OrderFlowConfigLoader


def test_conf_score_weight_tuning_json_override_parses_dict() -> None:
    loader = OrderFlowConfigLoader(redis_client=None)
    cfg = {"delta_abs_min": 1.0}
    overrides = {
        "conf_score_weight_tuning_json": json.dumps({"version": 1, "by_regime": {"trend": {"rsi_agree": 0.01}}}),
    }

    loader._apply_overrides(cfg, overrides)

    assert "conf_score_weight_tuning" in cfg
    assert isinstance(cfg["conf_score_weight_tuning"], dict)
    assert cfg["conf_score_weight_tuning"].get("version") == 1


def test_conf_score_weight_tuning_json_override_ignores_non_dict() -> None:
    loader = OrderFlowConfigLoader(redis_client=None)
    cfg = {"delta_abs_min": 1.0}
    overrides = {
        "conf_score_weight_tuning_json": json.dumps([1, 2, 3]),
    }

    loader._apply_overrides(cfg, overrides)

    # non-dict should not override
    assert "conf_score_weight_tuning" not in cfg


def test_conf_score_weight_tuning_json_override_ignores_invalid_json() -> None:
    loader = OrderFlowConfigLoader(redis_client=None)
    cfg = {"delta_abs_min": 1.0}
    overrides = {
        "conf_score_weight_tuning_json": "{invalid-json",
    }

    loader._apply_overrides(cfg, overrides)

    assert "conf_score_weight_tuning" not in cfg
