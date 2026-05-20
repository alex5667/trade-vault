"""Tests for Task 2.2 BURST_MIN_GAP_SEC floor in _cooldown_ms_for()."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from services.orderflow.utils import _burst_min_gap_floor_ms, _cooldown_ms_for


def _rt(symbol: str, *, regime: str = "na", config: dict | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        symbol=symbol,
        config=config or {"signal_cooldown_sec": 10},
        last_regime=regime,
        last_emit_dir="NONE",
        last_spread_bps=0.0,
        liq_regime="normal",
        pressure_hi=0,
    )


def test_burst_floor_global_env(monkeypatch):
    monkeypatch.setenv("BURST_MIN_GAP_SEC", "45")
    rt = _rt("BTCUSDT")
    assert _burst_min_gap_floor_ms(rt, cur_dir="LONG") == 45_000


def test_burst_floor_per_symbol_overrides_global(monkeypatch):
    monkeypatch.setenv("BURST_MIN_GAP_SEC", "30")
    monkeypatch.setenv("BURST_MIN_GAP_SEC_PEPE", "120")
    rt = _rt("1000PEPEUSDT")
    assert _burst_min_gap_floor_ms(rt, cur_dir="LONG") == 120_000


def test_burst_floor_per_symbol_strip_1000(monkeypatch):
    monkeypatch.setenv("BURST_MIN_GAP_SEC_PEPE", "180")
    rt = _rt("1000PEPEUSDT")
    assert _burst_min_gap_floor_ms(rt, cur_dir="SHORT") == 180_000


def test_burst_floor_symbol_direction_overrides_symbol(monkeypatch):
    monkeypatch.setenv("BURST_MIN_GAP_SEC_PEPE", "120")
    monkeypatch.setenv("BURST_MIN_GAP_SEC_PEPE_LONG", "240")
    rt = _rt("1000PEPEUSDT")
    assert _burst_min_gap_floor_ms(rt, cur_dir="LONG") == 240_000
    # Other direction falls back to per-symbol
    assert _burst_min_gap_floor_ms(rt, cur_dir="SHORT") == 120_000


def test_burst_floor_long_in_downtrend_multiplier(monkeypatch):
    monkeypatch.setenv("BURST_MIN_GAP_SEC_SOL", "60")
    monkeypatch.setenv("BURST_MIN_GAP_DOWNTREND_MUL", "2.5")
    rt = _rt("SOLUSDT", regime="trending_bear")
    assert _burst_min_gap_floor_ms(rt, cur_dir="LONG") == 150_000
    # SHORT in same regime — no multiplier
    assert _burst_min_gap_floor_ms(rt, cur_dir="SHORT") == 60_000


def test_burst_floor_disabled_when_no_env():
    rt = _rt("BTCUSDT")
    assert _burst_min_gap_floor_ms(rt, cur_dir="LONG") == 0


def test_cooldown_lifted_to_burst_floor(monkeypatch):
    monkeypatch.setenv("BURST_MIN_GAP_SEC_PEPE", "90")
    # base config: 10s cooldown
    rt = _rt("1000PEPEUSDT", config={"signal_cooldown_sec": 10, "cooldown_min_ms": 1000, "cooldown_max_ms": 300000})
    cd = _cooldown_ms_for(rt, scenario="continuation", now_ms=0, new_dir="LONG")
    # Floor must lift to 90s (90_000 ms) — above base 10_000.
    assert cd == 90_000


def test_cooldown_keeps_higher_value_than_floor(monkeypatch):
    monkeypatch.setenv("BURST_MIN_GAP_SEC", "10")
    rt = _rt(
        "BTCUSDT",
        config={
            "signal_cooldown_sec": 60,
            "cooldown_min_ms": 1000,
            "cooldown_max_ms": 300000,
        },
    )
    cd = _cooldown_ms_for(rt, scenario="continuation", now_ms=0, new_dir="LONG")
    # Base is 60_000 (>10_000 floor) — should stay 60_000.
    assert cd == 60_000


def test_cooldown_floor_clamped_by_cooldown_max(monkeypatch):
    monkeypatch.setenv("BURST_MIN_GAP_SEC_PEPE", "10000")  # 10000 sec
    rt = _rt(
        "1000PEPEUSDT",
        config={"signal_cooldown_sec": 5, "cooldown_min_ms": 1000, "cooldown_max_ms": 300_000},
    )
    cd = _cooldown_ms_for(rt, scenario="continuation", now_ms=0, new_dir="LONG")
    # Floor (10_000_000 ms) clamped to cooldown_max (300_000)
    assert cd == 300_000


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
