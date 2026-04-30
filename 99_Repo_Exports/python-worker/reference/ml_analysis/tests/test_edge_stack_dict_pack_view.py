import pytest


def test_dict_pack_view_enables_scaler_in_feature_row():
    try:
        from services.ml_confirm_gate import MLConfirmGate, _DictPackModelView
        from core.feature_engineering import RobustScalerPack  # noqa: F401
    except Exception as e:
        pytest.skip(f"required modules not importable: {e}", allow_module_level=True)

    class DummyRedis:
        pass

    gate = MLConfirmGate(r=DummyRedis(), mode="OFF", fail_policy="OPEN", champion_key="x", challenger_key="y")

    pack = {
        "feature_cols": ["f_x", "f_spread_bps", "f_expected_slippage_bps", "f_exec_risk_norm"]
        "feature_transforms": {}
        "robust_scaler": {"x": {"center": 10.0, "scale": 2.0}}
    }
    view = _DictPackModelView(pack)

    indicators = {
        "x": 12.0
        "spread_bps": 1.0
        "expected_slippage_bps": 1.0
        "exec_risk_norm": 0.2
    }

    row, missing = gate._build_feature_row(
        model=view
        indicators=indicators
        direction="BUY"
        scenario="trend"
        ts_ms=1700000000000
    )

    assert missing == []
    # First feature is scaled x: should not equal raw 12.0
    assert abs(float(row[0]) - 12.0) > 1e-9


def test_dict_pack_model_view_build_feature_row():
    """Commit 11: bucket: / hour: / dow: one-hots must be correctly encoded by gate _build_feature_row.

    ts_ms=1700000000000 => UTC 2023-11-14 22:13:20 => hour=22, weekday=Tuesday=1 (Mon=0).
    scenario='trend' => bucket='trend'.
    All three one-hots for the specific col value must be 1.0; others 0.0."""
    try:
        from tick_flow_full.services.ml_confirm_gate import MLConfirmGate, _DictPackModelView
    except Exception as e:
        pytest.skip(f"required modules not importable: {e}", allow_module_level=True)

    class DummyRedis:
        pass

    gate = MLConfirmGate(r=DummyRedis(), mode="OFF", fail_policy="OPEN", champion_key="x", challenger_key="y")

    # feature_cols with bucket:/hour:/dow: columns (Commit 11 format)
    pack = {
        "type": "edge_stack_v1"
        "feature_cols": [
            "f_x", "f_spread_bps", "f_expected_slippage_bps", "f_exec_risk_norm"
            "bucket:trend", "hour:22", "dow:1"
        ]
        "weights": {"intercept": 0.0, "coef": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]}
        "meta": {"name": "test"}
    }
    view = _DictPackModelView(pack)

    indicators = {
        "x": 12.0
        "spread_bps": 2.0
        "expected_slippage_bps": 3.0
        "exec_risk_norm": 4.0
    }

    # ts_ms=1700000000000: UTC 2023-11-14 22:13:20 => hour=22, dow=1 (Tue, Mon=0)
    row, missing = gate._build_feature_row(
        model=view
        indicators=indicators
        direction="BUY"
        scenario="trend"
        ts_ms=1700000000000
    )

    assert missing == []
    # last three cols: bucket:trend=1.0, hour:22=1.0, dow:1=1.0
    assert row[-3] == 1.0, f"bucket:trend expected 1.0, got {row[-3]}"
    assert row[-2] == 1.0, f"hour:22 expected 1.0, got {row[-2]}"
    assert row[-1] == 1.0, f"dow:1 expected 1.0, got {row[-1]}"
