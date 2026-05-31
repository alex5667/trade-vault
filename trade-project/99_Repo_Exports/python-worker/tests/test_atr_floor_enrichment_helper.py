"""Tests for SignalPipeline._enrich_atr_floor_indicators helper.

Validates the helper extracted from the legacy inline block at signal_pipeline.py:3110-3179.
Investigation: 2026-05-22 — ind_atr_th_bps fill 17%→11% root cause was virtual veto
signals emitted before the inline block ran. The helper enables early enrichment
via ATR_FLOOR_ENRICHMENT_EARLY=1 feature flag.
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from core.dyn_cfg_keys import DynCfgKeys as DK
from services.orderflow.signal_pipeline import SignalPipeline


def _make_runtime(
    *,
    regime: str = "trend",
    t0: float = 3.0,
    t1: float = 5.0,
    t2: float = 8.0,
    calib_ready: int = 1,
    bps_src: str = "calibrated",
    bps_n: int = 42,
) -> Any:
    dyn_cfg = {
        DK.ATR_FLOOR_T0_BPS: t0,
        DK.ATR_FLOOR_T1_BPS: t1,
        DK.ATR_FLOOR_T2_BPS: t2,
        DK.ATR_CALIB_READY: calib_ready,
        DK.ATR_BPS_SRC: bps_src,
        DK.ATR_BPS_N: bps_n,
    }
    return SimpleNamespace(
        symbol="BTCUSDT",
        last_regime=regime,
        dynamic_cfg=dyn_cfg,
        config={},
    )


class _FakePipeline:
    """Minimal class with helper + dependencies, avoiding full SignalPipeline init."""

    FEES_BPS_RT = 6.0
    TP_BPS_BUFFER = 2.0

    def _get_rocket_multiplier(self, symbol: str) -> float:
        return 1.0

    _enrich_atr_floor_indicators = SignalPipeline._enrich_atr_floor_indicators


def test_helper_writes_all_atr_floor_keys():
    pipe = _FakePipeline()
    indicators: dict[str, Any] = {}
    runtime = _make_runtime()
    cfg = {"tp_ratio": "0.5,0.5"}

    pipe._enrich_atr_floor_indicators(
        indicators=indicators, runtime=runtime, cfg=cfg, entry=65000.0, atr=50.0,
    )

    required_keys = {
        "atr_bps_exec",
        "atr_floor_t0_bps",
        "atr_floor_t1_bps",
        "atr_floor_t2_bps",
        "atr_floor_tier",
        "atr_floor_picked_bps",
        "atr_floor_th_bps",
        "atr_floor_rg",
        "atr_floor_ready",
        "atr_floor_src",
        "atr_floor_n",
        "atr_bps_th",
        "atr_fees_th_bps",
        "atr_fees_tp1_share",
        "atr_fees_rocket_mult",
        "atr_unified_th_bps",
        "atr_gate_dominant",
    }
    missing = required_keys - set(indicators.keys())
    assert not missing, f"Missing keys: {missing}"


def test_helper_idempotent():
    pipe = _FakePipeline()
    indicators: dict[str, Any] = {}
    runtime = _make_runtime()
    cfg = {"tp_ratio": "0.5,0.5"}

    pipe._enrich_atr_floor_indicators(
        indicators=indicators, runtime=runtime, cfg=cfg, entry=65000.0, atr=50.0,
    )
    snapshot_1 = dict(indicators)

    pipe._enrich_atr_floor_indicators(
        indicators=indicators, runtime=runtime, cfg=cfg, entry=65000.0, atr=50.0,
    )
    snapshot_2 = dict(indicators)

    assert snapshot_1 == snapshot_2, "Helper must be idempotent"


def test_helper_atr_bps_exec_computed_from_atr_and_entry():
    pipe = _FakePipeline()
    indicators: dict[str, Any] = {}
    runtime = _make_runtime()
    cfg = {"tp_ratio": "0.5,0.5"}

    pipe._enrich_atr_floor_indicators(
        indicators=indicators, runtime=runtime, cfg=cfg, entry=65000.0, atr=50.0,
    )
    expected = 10000.0 * (50.0 / 65000.0)
    assert indicators["atr_bps_exec"] == pytest.approx(expected, rel=1e-6)


def test_helper_zero_entry_atr_safe():
    pipe = _FakePipeline()
    indicators: dict[str, Any] = {}
    runtime = _make_runtime()
    cfg = {"tp_ratio": "0.5,0.5"}

    pipe._enrich_atr_floor_indicators(
        indicators=indicators, runtime=runtime, cfg=cfg, entry=0.0, atr=0.0,
    )
    assert indicators["atr_bps_exec"] == 0.0
    assert "atr_floor_th_bps" in indicators


def test_helper_unified_th_is_max_of_floor_and_fees():
    pipe = _FakePipeline()
    indicators: dict[str, Any] = {}
    runtime = _make_runtime(t1=10.0)
    cfg = {"tp_ratio": "0.5,0.5"}

    pipe._enrich_atr_floor_indicators(
        indicators=indicators, runtime=runtime, cfg=cfg, entry=65000.0, atr=50.0,
    )
    expected_unified = max(
        float(indicators["atr_floor_th_bps"]),
        float(indicators["atr_fees_th_bps"]),
    )
    assert indicators["atr_unified_th_bps"] == pytest.approx(expected_unified)


def test_helper_regime_lowercased():
    pipe = _FakePipeline()
    indicators: dict[str, Any] = {}
    runtime = _make_runtime(regime="TREND")
    cfg = {"tp_ratio": "0.5,0.5"}

    pipe._enrich_atr_floor_indicators(
        indicators=indicators, runtime=runtime, cfg=cfg, entry=65000.0, atr=50.0,
    )
    assert indicators["atr_floor_rg"] == "trend"


def test_helper_does_not_overwrite_unrelated_keys():
    pipe = _FakePipeline()
    indicators: dict[str, Any] = {
        "delta_z": 1.5,
        "confidence": 0.85,
        "atr_floor_veto": 1,
    }
    runtime = _make_runtime()
    cfg = {"tp_ratio": "0.5,0.5"}

    pipe._enrich_atr_floor_indicators(
        indicators=indicators, runtime=runtime, cfg=cfg, entry=65000.0, atr=50.0,
    )
    assert indicators["delta_z"] == 1.5
    assert indicators["confidence"] == 0.85
    assert indicators["atr_floor_veto"] == 1


def test_helper_handles_missing_dynamic_cfg_keys():
    pipe = _FakePipeline()
    indicators: dict[str, Any] = {}
    runtime = SimpleNamespace(
        symbol="BTCUSDT",
        last_regime="range",
        dynamic_cfg={},
        config={},
    )
    cfg = {"tp_ratio": "0.5,0.5"}

    pipe._enrich_atr_floor_indicators(
        indicators=indicators, runtime=runtime, cfg=cfg, entry=65000.0, atr=50.0,
    )
    assert "atr_floor_th_bps" in indicators
    assert indicators["atr_floor_t0_bps"] == 0.0
    assert indicators["atr_floor_ready"] == 0


def test_helper_handles_none_last_regime():
    pipe = _FakePipeline()
    indicators: dict[str, Any] = {}
    runtime = _make_runtime()
    runtime.last_regime = None
    cfg = {"tp_ratio": "0.5,0.5"}

    pipe._enrich_atr_floor_indicators(
        indicators=indicators, runtime=runtime, cfg=cfg, entry=65000.0, atr=50.0,
    )
    assert indicators["atr_floor_rg"] == "na"


def test_helper_gate_dominant_na_when_unified_zero():
    pipe = _FakePipeline()
    indicators: dict[str, Any] = {}
    runtime = _make_runtime(t0=0.0, t1=0.0, t2=0.0)
    cfg = {"tp_ratio": "0.5,0.5"}

    pipe._enrich_atr_floor_indicators(
        indicators=indicators, runtime=runtime, cfg=cfg, entry=65000.0, atr=0.0,
    )
    if indicators["atr_unified_th_bps"] == 0.0:
        assert indicators["atr_gate_dominant"] == "na"


def test_helper_exception_isolation():
    pipe = _FakePipeline()
    indicators: dict[str, Any] = {}
    runtime = SimpleNamespace(
        symbol="BTCUSDT",
        last_regime="trend",
        dynamic_cfg=None,
        config={},
    )
    cfg = {"tp_ratio": "0.5,0.5"}

    pipe._enrich_atr_floor_indicators(
        indicators=indicators, runtime=runtime, cfg=cfg, entry=65000.0, atr=50.0,
    )
    assert "atr_unified_th_bps" in indicators
