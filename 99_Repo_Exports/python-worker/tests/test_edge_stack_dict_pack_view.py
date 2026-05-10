import pytest


def test_dict_pack_view_enables_scaler_in_feature_row():
    try:
        from core.feature_engineering import RobustScalerPack  # noqa: F401
        from services.ml_confirm import MLConfirmGate, _DictPackModelView
    except Exception as e:
        pytest.skip(f"required modules not importable: {e}", allow_module_level=True)

    class DummyRedis:
        pass

    gate = MLConfirmGate(r=DummyRedis(), mode="OFF", fail_policy="OPEN", champion_key="x", challenger_key="y")

    pack = {
        "feature_cols": ["f_x", "f_spread_bps", "f_expected_slippage_bps", "f_exec_risk_norm"],
        "feature_transforms": {},
        "robust_scaler": {"x": {"center": 10.0, "scale": 2.0}},
    }
    view = _DictPackModelView(pack)

    indicators = {
        "x": 12.0,
        "spread_bps": 1.0,
        "expected_slippage_bps": 1.0,
        "exec_risk_norm": 0.2,
    }

    row, missing = gate._build_feature_row(
        model=view,
        indicators=indicators,
        direction="BUY",
        scenario="trend",
        ts_ms=1700000000000,
    )

    assert missing == []
    # First feature is scaled x: should not equal raw 12.0
    assert abs(float(row[0]) - 12.0) > 1e-9

