"""Tests for FundingBasisCalibrator (core/funding_basis_calibrator.py) v1 + v2."""
from __future__ import annotations

import random

from core.funding_basis_calibrator import (
    BASIS_BPS_CEIL,
    BASIS_BPS_FLOOR,
    DEFAULT_BASIS_BPS,
    DEFAULT_FUNDING_Z,
    FUNDING_Z_CEIL,
    FUNDING_Z_FLOOR,
    MIN_PANIC_SAMPLES,
    PANIC_BB_BOUNDARY,
    PANIC_COLD_BB_MULT,
    PANIC_COLD_FZ_MULT,
    PANIC_FZ_BOUNDARY,
    FundingBasisCalibrator,
)


# ── helpers ──────────────────────────────────────────────────────────────────

def _warm(cal: FundingBasisCalibrator, regime: str, n: int = 550) -> None:
    rng = random.Random(42)
    for _ in range(n):
        cal.observe(
            regime=regime,
            abs_funding_z=abs(rng.gauss(1.5, 0.7)),
            abs_basis_bps=abs(rng.gauss(5.0, 3.0)),
        )


# ── cold / warmup ─────────────────────────────────────────────────────────────

def test_cold_returns_static_defaults():
    cal = FundingBasisCalibrator(min_samples=500)
    th = cal.thresholds(regime="btcusdt")
    assert th.funding_z == DEFAULT_FUNDING_Z
    assert th.basis_bps == DEFAULT_BASIS_BPS
    assert th.src == "static"
    assert th.n == 0


def test_enforce_defaults_while_cold():
    # enforce=True + n < min_samples: values remain at defaults (quantile not converged).
    cal = FundingBasisCalibrator(min_samples=500, enforce=True)
    cal.observe(regime="x", abs_funding_z=2.0, abs_basis_bps=5.0)
    th = cal.thresholds(regime="x")
    assert th.funding_z == DEFAULT_FUNDING_Z
    assert th.basis_bps == DEFAULT_BASIS_BPS
    assert th.n == 1


def test_shadow_populated_after_warmup():
    cal = FundingBasisCalibrator(min_samples=50)
    _warm(cal, "ethusdt", n=60)
    cal.thresholds(regime="ethusdt")
    shadow = cal.shadow_thresholds(regime="ethusdt")
    assert shadow is not None
    assert shadow.src == "calib_q95"


# ── observation filters ───────────────────────────────────────────────────────

def test_nan_funding_z_ignored():
    cal = FundingBasisCalibrator()
    cal.observe(regime="x", abs_funding_z=float("nan"), abs_basis_bps=5.0)
    # basis_bps was valid — still counts
    assert cal.n("x") == 1


def test_nan_both_no_count():
    cal = FundingBasisCalibrator()
    cal.observe(regime="x", abs_funding_z=float("nan"), abs_basis_bps=float("nan"))
    assert cal.n("x") == 0


def test_below_floor_ignored():
    cal = FundingBasisCalibrator()
    cal.observe(regime="x", abs_funding_z=FUNDING_Z_FLOOR - 0.01, abs_basis_bps=BASIS_BPS_FLOOR - 0.01)
    assert cal.n("x") == 0


def test_above_ceil_ignored():
    cal = FundingBasisCalibrator()
    cal.observe(regime="x", abs_funding_z=FUNDING_Z_CEIL + 0.01, abs_basis_bps=BASIS_BPS_CEIL + 0.01)
    assert cal.n("x") == 0


def test_boundary_values_accepted():
    cal = FundingBasisCalibrator()
    cal.observe(regime="x", abs_funding_z=FUNDING_Z_FLOOR, abs_basis_bps=BASIS_BPS_FLOOR)
    cal.observe(regime="x", abs_funding_z=FUNDING_Z_CEIL, abs_basis_bps=BASIS_BPS_CEIL)
    assert cal.n("x") == 2


# ── enforce + calibration ─────────────────────────────────────────────────────

def test_enforce_low_distribution_lowers_thresholds():
    """Low-funding symbol should calibrate to lower threshold."""
    cal = FundingBasisCalibrator(min_samples=50, enforce=True)
    for _ in range(60):
        cal.observe(regime="lowvol", abs_funding_z=1.2, abs_basis_bps=2.0)
    th = cal.thresholds(regime="lowvol")
    assert th.src == "calib_q95"
    assert th.funding_z < DEFAULT_FUNDING_Z
    assert th.basis_bps < DEFAULT_BASIS_BPS


def test_enforce_high_distribution_raises_thresholds():
    """High-volatility alt should calibrate to higher threshold."""
    cal = FundingBasisCalibrator(min_samples=50, enforce=True)
    for _ in range(60):
        cal.observe(regime="highvol", abs_funding_z=5.0, abs_basis_bps=25.0)
    th = cal.thresholds(regime="highvol")
    assert th.src == "calib_q95"
    assert th.funding_z > DEFAULT_FUNDING_Z
    assert th.basis_bps > DEFAULT_BASIS_BPS


def test_calibrated_values_within_rails():
    cal = FundingBasisCalibrator(min_samples=50, enforce=True)
    _warm(cal, "btcusdt", n=60)
    th = cal.thresholds(regime="btcusdt")
    assert FUNDING_Z_FLOOR <= th.funding_z <= FUNDING_Z_CEIL
    assert BASIS_BPS_FLOOR <= th.basis_bps <= BASIS_BPS_CEIL


# ── hysteresis ───────────────────────────────────────────────────────────────

def test_hysteresis_prevents_small_update():
    cal = FundingBasisCalibrator(min_samples=10, enforce=True,
                                  update_band_fz=1.0, update_band_bb=5.0)
    for _ in range(15):
        cal.observe(regime="r", abs_funding_z=2.0, abs_basis_bps=8.0)
    th1 = cal.thresholds(regime="r")
    committed = th1.funding_z

    # Small additional observation — shouldn't shift committed threshold by > band
    cal.observe(regime="r", abs_funding_z=2.01, abs_basis_bps=8.01)
    th2 = cal.thresholds(regime="r")
    assert abs(th2.funding_z - committed) < 1.0 + 0.01


# ── regime independence ───────────────────────────────────────────────────────

def test_two_regimes_independent():
    cal = FundingBasisCalibrator(min_samples=50, enforce=True)
    for _ in range(60):
        cal.observe(regime="btcusdt", abs_funding_z=1.2, abs_basis_bps=2.5)
    for _ in range(60):
        cal.observe(regime="solusdt", abs_funding_z=5.0, abs_basis_bps=30.0)

    th_btc = cal.thresholds(regime="btcusdt")
    th_sol = cal.thresholds(regime="solusdt")
    assert th_sol.funding_z > th_btc.funding_z
    assert th_sol.basis_bps > th_btc.basis_bps


def test_regime_key_normalised():
    cal = FundingBasisCalibrator(min_samples=5, enforce=True)
    for _ in range(10):
        cal.observe(regime="BTCUSDT", abs_funding_z=2.0, abs_basis_bps=5.0)
    assert cal.n("btcusdt") == 10


# ── persistence ──────────────────────────────────────────────────────────────

def test_dump_load_roundtrip():
    cal = FundingBasisCalibrator(min_samples=50, enforce=True)
    _warm(cal, "ethusdt", n=60)
    th_before = cal.thresholds(regime="ethusdt")

    state = cal.dump_regime_state(symbol="ETHUSDT", regime="ethusdt", updated_ts_ms=1_000)
    cal2 = FundingBasisCalibrator(min_samples=50, enforce=True)
    cal2.load_regime_state(state)
    th_after = cal2.thresholds(regime="ethusdt")

    assert th_after.n == th_before.n
    assert th_after.src == th_before.src
    assert abs(th_after.funding_z - th_before.funding_z) < 0.5
    assert abs(th_after.basis_bps - th_before.basis_bps) < 2.0


def test_dump_kind_field():
    cal = FundingBasisCalibrator()
    state = cal.dump_regime_state(symbol="X", regime="x", updated_ts_ms=0)
    assert state["kind"] == "funding_basis"
    assert state["v"] == 2


def test_load_wrong_kind_noop():
    cal = FundingBasisCalibrator(min_samples=10, enforce=True)
    for _ in range(15):
        cal.observe(regime="r", abs_funding_z=2.0, abs_basis_bps=5.0)
    th_before = cal.thresholds(regime="r")
    cal.load_regime_state({"kind": "vol_z_thr", "regime": "r", "n": 9999})
    assert cal.n("r") == th_before.n


def test_load_corrupt_state_noop():
    cal = FundingBasisCalibrator()
    cal.load_regime_state(None)
    cal.load_regime_state(42)
    cal.load_regime_state({"kind": "funding_basis", "regime": None, "n": "bad"})


# ── shadow ───────────────────────────────────────────────────────────────────

def test_shadow_none_before_first_call():
    cal = FundingBasisCalibrator()
    assert cal.shadow_thresholds(regime="new") is None


def test_shadow_available_after_thresholds_call():
    cal = FundingBasisCalibrator(min_samples=10)
    _warm(cal, "ethusdt", n=15)
    cal.thresholds(regime="ethusdt")
    assert cal.shadow_thresholds(regime="ethusdt") is not None


# ── realistic distribution ────────────────────────────────────────────────────

def test_q95_realistic_crypto_funding():
    """q95 on typical crypto funding values should be within rails."""
    rng = random.Random(99)
    cal = FundingBasisCalibrator(min_samples=200, enforce=True)
    for _ in range(250):
        fz = abs(rng.gauss(0.8, 0.9))
        bb = abs(rng.gauss(4.0, 5.0))
        cal.observe(regime="btcusdt",
                    abs_funding_z=min(fz, FUNDING_Z_CEIL),
                    abs_basis_bps=min(bb, BASIS_BPS_CEIL))
    th = cal.thresholds(regime="btcusdt")
    assert FUNDING_Z_FLOOR <= th.funding_z <= FUNDING_Z_CEIL
    assert BASIS_BPS_FLOOR <= th.basis_bps <= BASIS_BPS_CEIL


# ════════════════════════════════════════════════════════════════════════════
# v2: regime tagging (carry / panic)
# ════════════════════════════════════════════════════════════════════════════

# ── detect_tag ───────────────────────────────────────────────────────────────

def test_detect_tag_carry_below_boundary():
    cal = FundingBasisCalibrator()
    assert cal.detect_tag(abs_fz=1.0, abs_bb=5.0) == "carry"
    assert cal.detect_tag(abs_fz=PANIC_FZ_BOUNDARY - 0.01, abs_bb=PANIC_BB_BOUNDARY - 0.01) == "carry"


def test_detect_tag_panic_fz_at_boundary():
    cal = FundingBasisCalibrator()
    assert cal.detect_tag(abs_fz=PANIC_FZ_BOUNDARY, abs_bb=1.0) == "panic"


def test_detect_tag_panic_bb_at_boundary():
    cal = FundingBasisCalibrator()
    assert cal.detect_tag(abs_fz=1.0, abs_bb=PANIC_BB_BOUNDARY) == "panic"


def test_detect_tag_nan_does_not_panic():
    cal = FundingBasisCalibrator()
    assert cal.detect_tag(abs_fz=float("nan"), abs_bb=5.0) == "carry"


# ── observe returns tag ───────────────────────────────────────────────────────

def test_observe_returns_carry_tag():
    cal = FundingBasisCalibrator()
    tag = cal.observe(regime="x", abs_funding_z=1.5, abs_basis_bps=5.0)
    assert tag == "carry"


def test_observe_returns_panic_tag():
    cal = FundingBasisCalibrator()
    tag = cal.observe(regime="x", abs_funding_z=PANIC_FZ_BOUNDARY, abs_basis_bps=5.0)
    assert tag == "panic"


# ── panic observations go to both combined AND panic estimators ───────────────

def test_panic_obs_increments_both_counts():
    cal = FundingBasisCalibrator()
    cal.observe(regime="r", abs_funding_z=3.0, abs_basis_bps=20.0)  # panic
    assert cal.n("r") == 1          # combined
    assert cal.n_panic("r") == 1    # panic-only


def test_carry_obs_increments_only_combined():
    cal = FundingBasisCalibrator()
    cal.observe(regime="r", abs_funding_z=1.5, abs_basis_bps=5.0)  # carry
    assert cal.n("r") == 1
    assert cal.n_panic("r") == 0


# ── cold panic fallback ───────────────────────────────────────────────────────

def test_cold_panic_returns_elevated_static():
    """Cold panic (n_panic < min) should return threshold > static default."""
    cal = FundingBasisCalibrator(min_samples=10, enforce=True)
    # Warm up combined with carry observations.
    for _ in range(15):
        cal.observe(regime="x", abs_funding_z=1.5, abs_basis_bps=5.0)
    # n_panic=0 → cold panic fallback.
    th = cal.thresholds(regime="x", current_regime_tag="panic")
    assert th.src == "static_panic"
    assert th.funding_z > DEFAULT_FUNDING_Z
    assert th.basis_bps > DEFAULT_BASIS_BPS
    assert th.regime_tag == "panic"


def test_cold_panic_fallback_values():
    """Fallback = default × PANIC_COLD_MULT, clamped to rails."""
    cal = FundingBasisCalibrator(min_samples=5, enforce=True)
    for _ in range(10):
        cal.observe(regime="x", abs_funding_z=1.5, abs_basis_bps=5.0)
    th = cal.thresholds(regime="x", current_regime_tag="panic",
                        default_funding_z=DEFAULT_FUNDING_Z,
                        default_basis_bps=DEFAULT_BASIS_BPS)
    expected_fz = min(DEFAULT_FUNDING_Z * PANIC_COLD_FZ_MULT, FUNDING_Z_CEIL)
    expected_bb = min(DEFAULT_BASIS_BPS * PANIC_COLD_BB_MULT, BASIS_BPS_CEIL)
    assert abs(th.funding_z - expected_fz) < 1e-9
    assert abs(th.basis_bps - expected_bb) < 1e-9


# ── warm panic uses q99 ───────────────────────────────────────────────────────

def _warm_panic(cal: FundingBasisCalibrator, regime: str, n: int = 60) -> None:
    """Feed panic observations: fz=4.0, bb=30.0."""
    for _ in range(n):
        cal.observe(regime=regime, abs_funding_z=4.0, abs_basis_bps=30.0)


def test_warm_panic_src_is_calib_q99():
    cal = FundingBasisCalibrator(min_samples=10, min_panic_samples=MIN_PANIC_SAMPLES, enforce=True)
    # Warm combined first (min_samples=10).
    for _ in range(15):
        cal.observe(regime="x", abs_funding_z=1.5, abs_basis_bps=5.0)
    # Warm panic (min_panic_samples=50).
    _warm_panic(cal, "x", n=MIN_PANIC_SAMPLES + 5)
    th = cal.thresholds(regime="x", current_regime_tag="panic")
    assert th.src == "calib_q99"
    assert th.regime_tag == "panic"


def test_panic_threshold_above_carry_threshold():
    """Invariant: panic threshold ≥ carry threshold."""
    cal = FundingBasisCalibrator(min_samples=10, min_panic_samples=MIN_PANIC_SAMPLES, enforce=True)
    for _ in range(15):
        cal.observe(regime="x", abs_funding_z=1.5, abs_basis_bps=5.0)
    _warm_panic(cal, "x", n=MIN_PANIC_SAMPLES + 5)

    carry = cal.thresholds(regime="x", current_regime_tag="carry")
    panic = cal.thresholds(regime="x", current_regime_tag="panic")
    assert panic.funding_z >= carry.funding_z
    assert panic.basis_bps >= carry.basis_bps


# ── shadow per-tag ────────────────────────────────────────────────────────────

def test_shadow_carry_and_panic_are_independent():
    cal = FundingBasisCalibrator(min_samples=10)
    for _ in range(15):
        cal.observe(regime="x", abs_funding_z=1.5, abs_basis_bps=5.0)
    _warm_panic(cal, "x", n=MIN_PANIC_SAMPLES + 5)
    cal.thresholds(regime="x", current_regime_tag="carry")
    cal.thresholds(regime="x", current_regime_tag="panic")
    sh_carry = cal.shadow_thresholds(regime="x", regime_tag="carry")
    sh_panic = cal.shadow_thresholds(regime="x", regime_tag="panic")
    assert sh_carry is not None
    assert sh_panic is not None
    assert sh_carry.regime_tag == "carry"
    assert sh_panic.regime_tag == "panic"


# ── regime_tag in FundingBasisThresholds ──────────────────────────────────────

def test_thresholds_carry_has_regime_tag_carry():
    cal = FundingBasisCalibrator(min_samples=10, enforce=True)
    _warm(cal, "x", n=15)
    th = cal.thresholds(regime="x", current_regime_tag="carry")
    assert th.regime_tag == "carry"


def test_thresholds_no_tag_defaults_to_carry():
    cal = FundingBasisCalibrator(min_samples=10, enforce=True)
    _warm(cal, "x", n=15)
    th = cal.thresholds(regime="x")
    assert th.regime_tag == "carry"


# ── persistence v2 ────────────────────────────────────────────────────────────

def test_dump_load_v2_panic_roundtrip():
    cal = FundingBasisCalibrator(min_samples=10, min_panic_samples=MIN_PANIC_SAMPLES, enforce=True)
    _warm(cal, "btcusdt", n=15)
    _warm_panic(cal, "btcusdt", n=MIN_PANIC_SAMPLES + 5)
    # Warm panic thresholds to populate committed.
    cal.thresholds(regime="btcusdt", current_regime_tag="panic")

    state = cal.dump_regime_state(symbol="BTCUSDT", regime="btcusdt", updated_ts_ms=1_000)
    assert state["v"] == 2
    assert state["n_panic"] >= MIN_PANIC_SAMPLES

    cal2 = FundingBasisCalibrator(min_samples=10, min_panic_samples=MIN_PANIC_SAMPLES, enforce=True)
    cal2.load_regime_state(state)
    assert cal2.n_panic("btcusdt") == state["n_panic"]


def test_load_v1_state_backward_compat():
    """v1 state (no panic fields) should load as carry-only without error."""
    v1_state = {
        "v": 1,
        "kind": "funding_basis",
        "symbol": "BTCUSDT",
        "regime": "btcusdt",
        "updated_ts_ms": 0,
        "min_samples": 50,
        "enforce": True,
        "n": 60,
        "committed_fz": 2.5,
        "committed_bb": 6.0,
        "fz95": None,
        "bb95": None,
    }
    cal = FundingBasisCalibrator(min_samples=50, enforce=True)
    cal.load_regime_state(v1_state)
    assert cal.n("btcusdt") == 60
    assert cal.n_panic("btcusdt") == 0
    # carry threshold should still use committed value from v1 state.
    th = cal.thresholds(regime="btcusdt", current_regime_tag="carry")
    assert th.n == 60


# ── PANIC_BB_BOUNDARY boundary test ──────────────────────────────────────────

def test_bb_boundary_triggers_panic():
    cal = FundingBasisCalibrator()
    # Just below → carry
    tag = cal.observe(regime="x", abs_funding_z=1.0, abs_basis_bps=PANIC_BB_BOUNDARY - 0.01)
    assert tag == "carry"
    assert cal.n_panic("x") == 0
    # At boundary → panic
    tag = cal.observe(regime="x", abs_funding_z=1.0, abs_basis_bps=PANIC_BB_BOUNDARY)
    assert tag == "panic"
    assert cal.n_panic("x") == 1
