"""Tests for pit_priors_rolling_v1 cold-start / field normalization."""

from __future__ import annotations

import os
import unittest.mock

import pytest


def test_kind_label_prefers_scenario_then_kind():
    from orderflow_services.pit_priors_rolling_v1 import _kind_label

    assert _kind_label({"scenario": "continuation", "kind": "reclaim"}) == "continuation"
    assert _kind_label({"kind": "reclaim"}) == "reclaim"
    assert _kind_label({}) == "default"


def test_derive_mfe_r_from_bps_and_one_r():
    from orderflow_services.pit_priors_rolling_v1 import _derive_mfe_r, _enrich_trade_fields

    t = {"mfe_bps": "50.0", "one_r_money": "100.0", "entry_px": "50000.0"}
    mfe_r = _derive_mfe_r(t)
    assert mfe_r == pytest.approx(2.5, rel=1e-3)
    _enrich_trade_fields(t)
    assert float(t["mfe_r"]) == pytest.approx(2.5, rel=1e-3)


def test_slippage_p95_in_7d_aggregate():
    from orderflow_services import pit_priors_rolling_v1 as m

    now_ms = 30 * 86_400_000 + 5_000_000_000
    ts_close = now_ms - 7_200_000
    trades = []
    for i in range(1, 21):
        trades.append({
            "symbol": "ETHUSDT",
            "kind": "default",
            "session": "us_main",
            "ts_close": str(ts_close - i * 1_000),
            "result": "WIN",
            "r_multiple": "1.0",
            "slippage_bps_est": str(float(i)),
        })
    with unittest.mock.patch.dict(os.environ, {"PIT_ROLLING_MIN_SAMPLES": "10"}):
        p7, _ = m.compute_rolling_priors(trades, now_ms)
    agg = p7[("ETHUSDT", "default", "all")]
    assert agg["slippage_p95_bps"] >= agg["winrate"] >= 0.0
    assert agg["slippage_p95_bps"] >= 18.0


def test_cold_start_lower_min_for_default_all():
    from orderflow_services import pit_priors_rolling_v1 as m

    now_ms = 30 * 86_400_000 + 5_000_000_000
    ts_close = now_ms - 7_200_000
    trades = [
        {
            "symbol": "BTCUSDT",
            "scenario": "continuation",
            "session": "asian",
            "ts_close": str(ts_close - i * 1_000),
            "result": "WIN" if i % 2 else "LOSS",
            "r_multiple": "1.0" if i % 2 else "-1.0",
            "mfe_bps": "30",
            "mae_bps": "20",
            "one_r_money": "50",
        }
        for i in range(12)
    ]
    with unittest.mock.patch.dict(
        os.environ,
        {"PIT_ROLLING_MIN_SAMPLES": "20", "PIT_ROLLING_COLD_START_MIN_SAMPLES": "10"},
    ):
        p7, _ = m.compute_rolling_priors(trades, now_ms)
    assert ("BTCUSDT", "default", "all") in p7
    assert p7[("BTCUSDT", "default", "all")]["sample_count"] == 12.0
    assert ("BTCUSDT", "continuation", "all") not in p7
