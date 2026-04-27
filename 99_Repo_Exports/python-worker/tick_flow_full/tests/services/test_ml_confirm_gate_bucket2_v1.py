from __future__ import annotations

import sys
import types


def test_ml_confirm_gate_bucket2_onehot_encoding():
    # The repo code types against redis.Redis, but redis-py may be absent in the
    # lightweight CI environment for unit tests.
    if "redis" not in sys.modules:
        stub = types.ModuleType("redis")
        stub.Redis = object  # minimal attribute for type hints
        sys.modules["redis"] = stub

    # Import from tick_flow_full/services via test sys.path injection.
    from services.ml_confirm_gate import MLConfirmGate

    class _DummyModel:
        # Minimal set of feature columns used in _build_feature_row.
        feature_cols = [
            "direction_BUY",
            "bucket:trend",
            "bucket2:breakout",
            "hour:1",
            "dow:3",  # 1970-01-01 is Thursday (tm_wday=3)
            "f_spread_bps",
        ]

    gate = MLConfirmGate(
        r=None,  # not used by _build_feature_row
        mode="OFF",
        fail_policy="OPEN",
        champion_key="cfg:ml_confirm:champion",
        challenger_key="cfg:ml_confirm:challenger",
    )

    row, missing = gate._build_feature_row(
        model=_DummyModel(),
        indicators={"spread_bps": 5.0, "bucket2": "breakout", "expected_slippage_bps": 0.0},
        direction="BUY",
        scenario="trend",
        ts_ms=3600 * 1000,  # 1970-01-01 01:00:00 UTC
    )

    assert missing == []
    assert row == [1.0, 1.0, 1.0, 1.0, 1.0, 5.0]
