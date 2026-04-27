from __future__ import annotations

# Allow running tests from repo root without PYTHONPATH tweaks.
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))  # .../tick_flow_full

import math

from core.flags_sessions_v1 import (
    compute_a5_flags,
    session_onehot,
    session_open_close_flags,
    update_time_ema,
)
from core.feature_registry import get_edge_stack_feature_spec


def _ts(hour: int, minute: int = 0, second: int = 0) -> int:
    # 2026-01-01 00:00:00 UTC in ms (arbitrary fixed day)
    base = 1767225600  # 2026-01-01 00:00:00
    return int((base + hour * 3600 + minute * 60 + second) * 1000)


def test_session_onehot_boundaries() -> None:
    assert session_onehot(_ts(0))["session_asia"] == 1
    assert session_onehot(_ts(6, 59))["session_asia"] == 1

    assert session_onehot(_ts(7))["session_eu"] == 1
    assert session_onehot(_ts(12, 59))["session_eu"] == 1

    assert session_onehot(_ts(13))["session_us"] == 1
    assert session_onehot(_ts(20, 59))["session_us"] == 1

    assert session_onehot(_ts(21))["session_off"] == 1
    assert session_onehot(_ts(23, 59))["session_off"] == 1


def test_session_open_close_flags_window() -> None:
    # default window=5m: boundary at 07:00 should trigger both open (EU starts) and close (ASIA ends)
    flags = session_open_close_flags(_ts(7, 0, 0), edge_window_ms=300_000)
    assert flags["flag_session_open"] == 1
    assert flags["flag_session_close"] == 1

    flags2 = session_open_close_flags(_ts(7, 10, 0), edge_window_ms=300_000)
    assert flags2["flag_session_open"] == 0
    assert flags2["flag_session_close"] == 0


def test_update_time_ema_dt() -> None:
    # first observation sets EMA
    ema, ts, bad = update_time_ema(prev_ema=0.0, x=10.0, prev_ts_ms=0, ts_ms=1000, tau_ms=60_000)
    assert ema == 10.0
    assert ts == 1000
    assert bad is False

    # out-of-order timestamp -> bad_time
    ema2, ts2, bad2 = update_time_ema(prev_ema=ema, x=20.0, prev_ts_ms=ts, ts_ms=900, tau_ms=60_000)
    assert ema2 == ema
    assert ts2 == ts
    assert bad2 is True

    # forward update
    ema3, ts3, bad3 = update_time_ema(prev_ema=ema, x=20.0, prev_ts_ms=ts, ts_ms=ts + 60_000, tau_ms=60_000)
    assert ts3 == ts + 60_000
    assert bad3 is False
    # alpha for dt=tau is 1-exp(-1) ~= 0.632
    assert abs(ema3 - ((1.0 - (1.0 - math.exp(-1.0))) * 10.0 + (1.0 - math.exp(-1.0)) * 20.0)) < 1e-6


def test_compute_a5_flags_basic() -> None:
    ts_ms = _ts(10)
    indicators = {
        "vol_ratio_z": 2.1,
        "microbar_range_bps": 12.0,
        "microbar_body_bps": 2.0,
    }

    out = compute_a5_flags(
        ts_ms=ts_ms,
        qty=10.0,
        indicators=indicators,
        trade_qty_ema=1.5,
        depth_total10=100.0,
        depth_total10_ema=400.0,
        cfg={
            "a5_high_vol_z_th": 2.0,
            "a5_low_liq_ratio_th": 0.35,
            "a5_large_trade_mult": 6.0,
            "a5_mr_body_ratio_th": 0.35,
            "a5_mr_range_th_bps": 5.0,
            "a5_session_edge_window_ms": 300_000,
        },
    )

    assert out["flag_high_vol"] == 1
    assert out["flag_low_liquidity"] == 1
    assert out["flag_large_trade"] == 1
    assert out["flag_mean_reversion_mode"] == 1
    assert out["flag_macro_event"] == 0


def test_feature_registry_v7_includes_session_onehot() -> None:
    spec = get_edge_stack_feature_spec("v7_of")
    assert "session_asia" in spec.feature_cols
    assert "session_eu" in spec.feature_cols
    assert "session_us" in spec.feature_cols
    assert "session_off" in spec.feature_cols


def test_feature_registry_v10_includes_session_onehot() -> None:
    """v10_of must include session one-hots and have >= 160 f_* numeric cols."""
    spec = get_edge_stack_feature_spec("v10_of")
    for sess in ("session_asia", "session_eu", "session_us", "session_off"):
        assert sess in spec.feature_cols, f"v10_of missing {sess}"
    f_cols = [c for c in spec.feature_cols if c.startswith("f_")]
    assert len(f_cols) >= 160, f"v10_of only has {len(f_cols)} f_* cols, expected >= 160"
    # Spot-check key Group 2 additions
    for key in ("f_vpin_rolling", "f_rsi_price", "f_spread_bps", "f_microbar_range_bps",
                "f_mae_r", "f_btc_corr_5m", "f_book_slope_bid"):
        assert key in spec.feature_cols, f"v10_of missing expected key {key}"


def test_feature_registry_v10_alias_resolves() -> None:
    """'v10' alias must resolve to v10_of with same schema_hash."""
    from core.feature_registry import get_schema_info
    info_full = get_schema_info("v10_of")
    info_alias = get_schema_info("v10")
    assert info_full.ver == "v10_of"
    assert info_alias.ver == "v10_of"
    assert info_full.schema_hash == info_alias.schema_hash


def test_feature_registry_v9_includes_session_onehot() -> None:
    """v9_of must include session one-hots now that OFInputsV2 publishes them to signals:of:inputs."""
    spec = get_edge_stack_feature_spec("v9_of")
    assert "session_asia" in spec.feature_cols
    assert "session_eu" in spec.feature_cols
    assert "session_us" in spec.feature_cols
    assert "session_off" in spec.feature_cols