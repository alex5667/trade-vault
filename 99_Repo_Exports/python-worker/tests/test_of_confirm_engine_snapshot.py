from __future__ import annotations
"""Tests for OFConfirmEngine runtime snapshot (golden replay support)."""


from types import SimpleNamespace

import pytest

from core.of_confirm_engine import OFConfirmEngine, _get_attr_or_key
from core.dyn_cfg_keys import DynCfgKeys as DK


def test_get_attr_or_key_dict():
    """Test _get_attr_or_key with dict access."""
    obj = {"key1": "value1", "key2": 42}
    assert _get_attr_or_key(obj, "key1", None) == "value1"
    assert _get_attr_or_key(obj, "key2", 0) == 42
    assert _get_attr_or_key(obj, "missing", "default") == "default"
    assert _get_attr_or_key(obj, "missing", None) is None


def test_get_attr_or_key_object():
    """Test _get_attr_or_key with object access."""
    obj = SimpleNamespace(key1="value1", key2=42)
    assert _get_attr_or_key(obj, "key1", None) == "value1"
    assert _get_attr_or_key(obj, "key2", 0) == 42
    assert _get_attr_or_key(obj, "missing", "default") == "default"


def test_get_attr_or_key_none():
    """Test _get_attr_or_key with None object."""
    assert _get_attr_or_key(None, "any", "default") == "default"
    assert _get_attr_or_key(None, "any", None) is None


def test_export_runtime_snapshot_basic():
    """Test export_runtime_snapshot with basic runtime."""
    engine = OFConfirmEngine()
    runtime = SimpleNamespace(
        symbol="BTCUSDT",
        ts=1000000,
        dynamic_cfg={"pressure_hi": 1, "atr_floor_t0_bps": 10.0},
        last_regime="bull",
        liq_regime="normal",
        book_churn_hi=0,
        pressure_hi=1,
        cont_ctx_ts_ms=500000,
        last_obi_event={"ts_ms": 999000, "direction": "LONG"},
        last_iceberg_event={"ts_ms": 998000, "side": "bid"},
        last_ofi_event={"ts_ms": 997000, "direction": "LONG"},
        last_sweep=SimpleNamespace(ts_ms=996000, kind="EQH", direction_bias="LONG"),
        last_reclaim=SimpleNamespace(ts_ms=995000, hold_bars=2, direction_bias="LONG", level=100.0, pool_id="p1"),
        last_div=SimpleNamespace(ts_ms=994000, kind="bullish"),
        last_wp=SimpleNamespace(ts_ms=993000, weak_any=True),
        last_fp_edge=SimpleNamespace(ts_ms=992000, bias="LONG", strength=0.8),
        last_bar=SimpleNamespace(
            id=1,
            fp_enabled=True,
            fp_absorption_bias="LONG",
            fp_ladder_low_len=3,
        ),
    )
    snap = engine.export_runtime_snapshot(runtime)
    assert isinstance(snap, dict)
    assert snap.get("schema", snap.get("v")) in (1, 2, 3)
    assert snap["symbol"] == "BTCUSDT"
    assert snap["ts_ms"] == 1000000
    assert snap["last_regime"] == "bull"
    assert snap["liq_regime"] == "normal"
    assert snap["book_churn_hi"] == 0
    assert snap["pressure_hi"] == 1
    assert snap["cont_ctx_ts_ms"] == 500000
    assert isinstance(snap["dynamic_cfg"], dict)
    assert snap["dynamic_cfg"]["pressure_hi"] == 1
    assert snap["last_sweep"] is not None
    assert snap["last_sweep"]["ts_ms"] == 996000
    assert snap["last_sweep"]["kind"] == "EQH"
    assert snap["last_reclaim"] is not None
    assert snap["last_reclaim"]["ts_ms"] == 995000
    assert snap["last_reclaim"]["hold_bars"] == 2


def test_export_runtime_snapshot_minimal():
    """Test export_runtime_snapshot with minimal runtime."""
    engine = OFConfirmEngine()
    runtime = SimpleNamespace(
        symbol="ETHUSDT",
        ts=2000000,
        dynamic_cfg={},
        last_regime="na",
        liq_regime="na",
        book_churn_hi=0,
        pressure_hi=0,
        cont_ctx_ts_ms=0,
        last_obi_event=None,
        last_iceberg_event=None,
        last_ofi_event=None,
        last_sweep=None,
        last_reclaim=None,
        last_div=None,
        last_wp=None,
        last_fp_edge=None,
        last_bar=None,
    )
    snap = engine.export_runtime_snapshot(runtime)
    assert isinstance(snap, dict)
    assert snap.get("schema", snap.get("v")) in (1, 2, 3)
    assert snap["symbol"] == "ETHUSDT"
    assert snap["last_regime"] == "na"
    assert snap["last_sweep"] is None
    assert snap["last_reclaim"] is None


def test_validate_runtime_snapshot_valid():
    """Test validate_runtime_snapshot with valid snapshot."""
    engine = OFConfirmEngine()
    snap = {
        "schema": 3,
        "symbol": "BTCUSDT",
        "ts_ms": 1000000,
        "dynamic_cfg": {},
        "last_regime": "bull",
        "liq_regime": "normal",
        "book_churn_hi": 0,
        "pressure_hi": 1,
        "cont_ctx_ts_ms": 500000,
    }
    missing = engine.validate_runtime_snapshot(snap)
    assert missing == []


def test_validate_runtime_snapshot_missing():
    """Test validate_runtime_snapshot with missing fields."""
    engine = OFConfirmEngine()
    snap = {
        "v": 1,
        "symbol": "BTCUSDT",
        # missing: dynamic_cfg, last_regime, etc.
    }
    missing = engine.validate_runtime_snapshot(snap)
    assert len(missing) > 0
    assert "dynamic_cfg" in missing
    assert "last_regime" in missing


def test_validate_runtime_snapshot_not_dict():
    """Test validate_runtime_snapshot with non-dict input."""
    engine = OFConfirmEngine()
    missing = engine.validate_runtime_snapshot("not a dict")
    assert missing == ["snap_not_dict"]


def test_build_runtime_from_snapshot():
    """Test build_runtime_from_snapshot creates SimpleNamespace."""
    engine = OFConfirmEngine()
    snap = {
        "v": 1,
        "symbol": "BTCUSDT",
        "ts_ms": 1000000,
        "dynamic_cfg": {"pressure_hi": 1},
        "last_regime": "bull",
        "liq_regime": "normal",
        "book_churn_hi": 0,
        "pressure_hi": 1,
        "cont_ctx_ts_ms": 500000,
        "last_sweep": {"ts_ms": 996000, "kind": "EQH", "direction_bias": "LONG"},
        "last_reclaim": {"ts_ms": 995000, "hold_bars": 2, "direction_bias": "LONG"},
    }
    rt = engine.build_runtime_from_snapshot(snap)
    assert isinstance(rt, SimpleNamespace)
    assert rt.symbol == "BTCUSDT"
    assert rt.last_regime == "bull"
    assert rt.pressure_hi == 1
    assert isinstance(rt.dynamic_cfg, dict)
    assert rt.dynamic_cfg[DK.PRESSURE_HI] == 1
    assert isinstance(rt.last_sweep, dict)
    assert rt.last_sweep["ts_ms"] == 996000


def test_build_runtime_from_snapshot_minimal():
    """Test build_runtime_from_snapshot with minimal snapshot."""
    engine = OFConfirmEngine()
    snap = {
        "schema": 3,
        "symbol": "ETHUSDT",
        "dynamic_cfg": {},
    }
    rt = engine.build_runtime_from_snapshot(snap)
    assert isinstance(rt, SimpleNamespace)
    assert rt.symbol == "ETHUSDT"
    assert isinstance(rt.dynamic_cfg, dict)

