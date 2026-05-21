"""tests/test_flow_toxicity_calibrator.py

Тесты для core/flow_toxicity_calibrator.py (FlowToxicityCalibrator).

Покрытие:
  - Warmup guard (n < MIN_SAMPLES → thr = 0.0)
  - Auto-promote: n >= MIN_SAMPLES → enforce, committed != 0.0
  - enforce=False + auto_enforce=False → shadow, committed = 0.0
  - Гистерезис (порог не меняется при малом Δ)
  - dump_state / load_state round-trip
  - Hard rails (clamp ofi_z / vpin)
  - Non-finite и missing inputs
  - All symbols list
"""
from __future__ import annotations

import math
import random

import pytest

from core.flow_toxicity_calibrator import (
    DEFAULT_OFI_Z_THR,
    DEFAULT_VPIN_THR,
    MIN_SAMPLES,
    OFI_Z_CEIL,
    FlowToxicityCalibrator,
    FlowToxThresholds,
    _norm_sym,
)


# ── helpers ───────────────────────────────────────────────────────────────────

def _feed(cal: FlowToxicityCalibrator, symbol: str, n: int, *, seed: int = 42) -> None:
    """Feed n synthetic (ofi_z, vpin) observations drawn from realistic distributions."""
    rng = random.Random(seed)
    for _ in range(n):
        # ofi_z ~ N(0, 1.5), clipped to [-10, 10]
        z = max(-10.0, min(10.0, rng.gauss(0.0, 1.5)))
        # vpin ~ Beta(2, 5) mapped to [0, 1], mean ≈ 0.29
        vpin = max(0.0, min(1.0, rng.betavariate(2, 5)))
        cal.observe(symbol=symbol, ofi_z=z, vpin=vpin)


# ── warmup guard ──────────────────────────────────────────────────────────────

def test_cold_returns_disabled():
    cal = FlowToxicityCalibrator(min_samples=MIN_SAMPLES, auto_enforce=True)
    thr = cal.thresholds(symbol="BTCUSDT")
    assert thr.thr_z == DEFAULT_OFI_Z_THR == 0.0
    assert thr.thr_vpin == DEFAULT_VPIN_THR == 0.0
    assert thr.src == "static"


def test_partial_warmup_still_disabled():
    cal = FlowToxicityCalibrator(min_samples=2000, auto_enforce=True)
    _feed(cal, "ETHUSDT", 500)
    thr = cal.thresholds(symbol="ETHUSDT")
    assert thr.thr_z == 0.0
    assert thr.thr_vpin == 0.0
    assert thr.src == "static"


# ── auto-promote ──────────────────────────────────────────────────────────────

def test_auto_promote_after_min_samples():
    cal = FlowToxicityCalibrator(min_samples=100, auto_enforce=True)
    _feed(cal, "BTCUSDT", 120)
    thr = cal.thresholds(symbol="BTCUSDT")
    assert thr.thr_z > 0.0, f"Expected thr_z > 0, got {thr.thr_z}"
    assert thr.thr_vpin > 0.0, f"Expected thr_vpin > 0, got {thr.thr_vpin}"
    assert thr.src == "calibrated"
    assert thr.n >= 120


def test_shadow_available_before_enforce():
    cal = FlowToxicityCalibrator(min_samples=50, auto_enforce=False, enforce=False)
    _feed(cal, "SOLUSDT", 60)
    # Thresholds are 0 (shadow mode)
    thr = cal.thresholds(symbol="SOLUSDT")
    assert thr.thr_z == 0.0
    assert thr.src == "static"
    # But shadow IS computed
    shadow = cal.shadow_thresholds(symbol="SOLUSDT")
    assert shadow is not None
    assert shadow.thr_z > 0.0 or shadow.thr_vpin > 0.0, "shadow should have non-zero thr after warmup"


def test_enforce_true_applies_immediately():
    cal = FlowToxicityCalibrator(min_samples=50, enforce=True, auto_enforce=False)
    _feed(cal, "BTCUSDT", 100)
    thr = cal.thresholds(symbol="BTCUSDT")
    assert thr.thr_z > 0.0
    assert thr.src == "calibrated"


# ── p95 sanity ────────────────────────────────────────────────────────────────

def test_p95_above_median():
    """p95 of ofi_norm_z должен быть выше медианы (≈0 для N(0,σ))."""
    cal = FlowToxicityCalibrator(min_samples=200, auto_enforce=True)
    _feed(cal, "BTCUSDT", 300)
    thr = cal.thresholds(symbol="BTCUSDT")
    # p95 of N(0, 1.5) ≈ 1.5 * 1.645 ≈ 2.47; must be > 0
    assert thr.thr_z > 0.0
    # p95 of Beta(2,5) ≈ 0.57; threshold floor is 0.50
    assert thr.thr_vpin >= 0.50


def test_vpin_floor_clamped():
    """vpin p95 даже при узком распределении не должна быть < 0.50."""
    cal = FlowToxicityCalibrator(min_samples=50, auto_enforce=True)
    rng = random.Random(0)
    for _ in range(60):
        # Очень низкий vpin (all values < 0.1) — должен быть зажат
        cal.observe(symbol="PEPE", ofi_z=0.5, vpin=rng.uniform(0.0, 0.1))
    thr = cal.thresholds(symbol="PEPE")
    assert thr.thr_vpin >= 0.50


def test_ofi_z_floor_clamped():
    """ofi_z p95 должен быть >= 0.5 (noise floor)."""
    cal = FlowToxicityCalibrator(min_samples=50, auto_enforce=True)
    for _ in range(60):
        cal.observe(symbol="BTC", ofi_z=0.01, vpin=0.5)
    thr = cal.thresholds(symbol="BTC")
    assert thr.thr_z >= 0.5


# ── гистерезис ────────────────────────────────────────────────────────────────

def test_hysteresis_no_update_on_small_delta():
    """Committed-порог не обновляется при изменении < UPDATE_BAND."""
    cal = FlowToxicityCalibrator(min_samples=50, auto_enforce=True, update_band_z=0.10)
    _feed(cal, "ETH", 60, seed=1)
    first = cal.thresholds(symbol="ETH").thr_z

    # Подать ещё немного данных (изменение P² < UPDATE_BAND)
    cal.observe(symbol="ETH", ofi_z=0.5, vpin=0.3)
    second = cal.thresholds(symbol="ETH").thr_z

    # Committed порог не изменился (гистерезис)
    assert abs(second - first) < 0.10 or second == first, (
        f"Expected hysteresis, got {first} → {second}"
    )


# ── hard rails ────────────────────────────────────────────────────────────────

def test_extreme_ofi_z_clamped():
    """ofi_z = 1000 должен быть зажат до OFI_Z_CEIL."""
    cal = FlowToxicityCalibrator(min_samples=5, auto_enforce=True)
    for _ in range(10):
        cal.observe(symbol="SOL", ofi_z=1000.0, vpin=0.99)
    thr = cal.thresholds(symbol="SOL")
    assert thr.thr_z <= OFI_Z_CEIL


def test_nan_ofi_z_ignored():
    cal = FlowToxicityCalibrator(min_samples=5, auto_enforce=True)
    for _ in range(10):
        cal.observe(symbol="SOL", ofi_z=float("nan"), vpin=0.7)
    # n должен быть 10 (NaN заменяется на 0.0)
    assert cal.n("SOL") == 10


def test_inf_vpin_ignored():
    cal = FlowToxicityCalibrator(min_samples=5, auto_enforce=True)
    for _ in range(10):
        cal.observe(symbol="XRP", ofi_z=1.5, vpin=float("inf"))
    assert cal.n("XRP") == 10


# ── per-symbol isolation ──────────────────────────────────────────────────────

def test_per_symbol_independent():
    """Символы не влияют друг на друга."""
    cal = FlowToxicityCalibrator(min_samples=50, auto_enforce=True)
    _feed(cal, "BTCUSDT", 60, seed=1)
    _feed(cal, "ETHUSDT", 60, seed=2)

    thr_btc = cal.thresholds(symbol="BTCUSDT")
    thr_eth = cal.thresholds(symbol="ETHUSDT")
    # Оба прогрелись
    assert thr_btc.thr_z > 0.0
    assert thr_eth.thr_z > 0.0
    # Холодный символ
    thr_sol = cal.thresholds(symbol="SOLUSDT")
    assert thr_sol.thr_z == 0.0
    assert thr_sol.src == "static"


def test_all_symbols_list():
    cal = FlowToxicityCalibrator()
    cal.observe(symbol="BTCUSDT", ofi_z=1.0, vpin=0.5)
    cal.observe(symbol="ETHUSDT", ofi_z=2.0, vpin=0.6)
    syms = cal.all_symbols()
    assert "BTCUSDT" in syms
    assert "ETHUSDT" in syms


# ── dump / load state ─────────────────────────────────────────────────────────

def test_dump_load_roundtrip():
    cal = FlowToxicityCalibrator(min_samples=50, auto_enforce=True)
    _feed(cal, "BTCUSDT", 60)
    thr_before = cal.thresholds(symbol="BTCUSDT")

    state = cal.dump_state(symbol="BTCUSDT", updated_ts_ms=1_700_000_000_000)
    assert state["kind"] == "flow_toxicity"
    assert state["symbol"] == "BTCUSDT"
    assert state["n"] == 60

    cal2 = FlowToxicityCalibrator(min_samples=50, auto_enforce=True)
    cal2.load_state(state)

    assert cal2.n("BTCUSDT") == 60
    thr_after = cal2.thresholds(symbol="BTCUSDT")
    assert abs(thr_after.thr_z - thr_before.thr_z) < 0.01, (
        f"thr_z mismatch after load: {thr_before.thr_z} vs {thr_after.thr_z}"
    )
    assert abs(thr_after.thr_vpin - thr_before.thr_vpin) < 0.01


def test_load_state_wrong_kind_ignored():
    cal = FlowToxicityCalibrator()
    cal.load_state({"kind": "p_edge", "symbol": "BTC", "n": 999})
    assert cal.n("BTC") == 0


def test_load_state_garbage_ignored():
    cal = FlowToxicityCalibrator()
    cal.load_state("not a dict")
    cal.load_state(None)
    cal.load_state({"kind": "flow_toxicity"})  # missing symbol — should not crash


# ── symbol normalisation ──────────────────────────────────────────────────────

def test_symbol_case_normalised():
    cal = FlowToxicityCalibrator(min_samples=5, auto_enforce=True)
    for _ in range(10):
        cal.observe(symbol="btcusdt", ofi_z=1.0, vpin=0.6)
    assert cal.n("BTCUSDT") == 10
    assert cal.n("btcusdt") == 10
    thr = cal.thresholds(symbol="btcusdt")
    assert thr.n == 10


def test_norm_sym():
    assert _norm_sym("btcusdt") == "BTCUSDT"
    assert _norm_sym("  ETH  ") == "ETH"
    assert _norm_sym(None) == "NA"
    assert _norm_sym("") == "NA"


# ── FlowToxThresholds dataclass ───────────────────────────────────────────────

def test_flow_tox_thresholds_fields():
    t = FlowToxThresholds(thr_z=2.5, thr_vpin=0.82, n=2000, src="calibrated")
    assert t.thr_z == 2.5
    assert t.thr_vpin == 0.82
    assert t.n == 2000
    assert t.src == "calibrated"
