from __future__ import annotations

from core.world_practice_snapshot_v1 import extract_world_practice_indicators


def test_extract_world_practice_indicators_defaults() -> None:
    out = extract_world_practice_indicators({})
    assert out["vol_regime_label"] == "na"
    assert out["vol_fast_bps"] == 0.0
    assert out["res_recovered"] == 0
    assert out["res_recovery_ms"] == 0


def test_extract_world_practice_indicators_sanitizes_non_finite() -> None:
    dc = {
        "vol_fast_bps": float("nan"),
        "vol_slow_bps": float("inf"),
        "vol_ratio": -1.23,  # allowed
        "vol_ratio_z": float("-inf"),
        "vol_regime_label": "shock",
        "res_recovery_ms": "1500",
        "res_speed_per_s": float("nan"),
        "res_recovered": 1,
    }
    out = extract_world_practice_indicators(dc)
    assert out["vol_fast_bps"] == 0.0
    assert out["vol_slow_bps"] == 0.0
    assert out["vol_ratio"] == -1.23
    assert out["vol_ratio_z"] == 0.0
    assert out["vol_regime_label"] == "shock"
    assert out["res_recovery_ms"] == 1500
    assert out["res_speed_per_s"] == 0.0
    assert out["res_recovered"] == 1
