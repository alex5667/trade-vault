"""Unit tests for SpreadStalenessCalibrator."""
from __future__ import annotations

import json

from core.spread_staleness_calibrator import (
    DEFAULT_BOOK_STALE_HARD_MS,
    DEFAULT_BOOK_STALE_SOFT_MS,
    DEFAULT_SPREAD_SHOCK_BPS,
    DEFAULT_SPREAD_SHOCK_BPS_HARD,
    SPREAD_BPS_FLOOR,
    SpreadStalenessCalibrator,
)


# ── helpers ────────────────────────────────────────────────────────────────────

def _feed(calib: SpreadStalenessCalibrator, regime: str, spreads: list[float], ages: list[float] | None = None) -> None:
    ages = ages or [0.0] * len(spreads)
    for sp, ba in zip(spreads, ages):
        calib.observe(regime=regime, spread_bps=sp, book_age_ms=ba)


# ── cold / shadow mode ─────────────────────────────────────────────────────────

def test_cold_returns_static_defaults():
    c = SpreadStalenessCalibrator(min_samples=50)
    th = c.thresholds(regime="btcusdt:ny")
    assert th.src == "static"
    assert th.spread_shock_bps == DEFAULT_SPREAD_SHOCK_BPS
    assert th.spread_shock_bps_hard == DEFAULT_SPREAD_SHOCK_BPS_HARD
    assert th.book_stale_soft_ms == DEFAULT_BOOK_STALE_SOFT_MS
    assert th.book_stale_hard_ms == DEFAULT_BOOK_STALE_HARD_MS
    assert th.n == 0


def test_shadow_mode_returns_static_even_when_warm():
    c = SpreadStalenessCalibrator(min_samples=5, enforce=False)
    _feed(c, "btcusdt:ny", [3.0, 4.0, 5.0, 6.0, 7.0, 8.0])
    th = c.thresholds(regime="btcusdt:ny")
    assert th.src == "static", "enforce=False must always return static"


def test_shadow_thresholds_exposed_after_warmup():
    c = SpreadStalenessCalibrator(min_samples=5, enforce=False)
    _feed(c, "btcusdt:ny", [3.0, 4.0, 5.0, 6.0, 7.0, 8.0])
    c.thresholds(regime="btcusdt:ny")  # triggers shadow computation
    shadow = c.shadow_thresholds(regime="btcusdt:ny")
    assert shadow is not None
    assert shadow.src == "calib_q90q95"
    assert shadow.n >= 5


def test_shadow_thresholds_none_before_first_thresholds_call():
    c = SpreadStalenessCalibrator(min_samples=5)
    _feed(c, "na", [5.0, 6.0, 7.0])
    # shadow is populated only on thresholds() call
    assert c.shadow_thresholds(regime="na") is None


# ── enforce mode ───────────────────────────────────────────────────────────────

def test_enforce_uses_calibrated_after_warmup():
    c = SpreadStalenessCalibrator(min_samples=5, enforce=True)
    _feed(c, "ethusdt:asia", [2.0, 3.0, 5.0, 8.0, 12.0, 10.0, 4.0])
    th = c.thresholds(regime="ethusdt:asia")
    assert th.src == "calib_q90q95"
    assert th.n >= 5
    assert th.spread_shock_bps > 0
    assert th.spread_shock_bps_hard >= th.spread_shock_bps


def test_enforce_still_returns_static_when_cold():
    c = SpreadStalenessCalibrator(min_samples=100, enforce=True)
    _feed(c, "btcusdt:ny", [3.0, 4.0])  # only 2 samples
    th = c.thresholds(regime="btcusdt:ny")
    assert th.src == "static"


# ── monotonicity + rails ───────────────────────────────────────────────────────

def test_hard_always_gte_soft():
    c = SpreadStalenessCalibrator(min_samples=5, enforce=True)
    # uniform spread → q90 ≈ q95 ≈ same value
    _feed(c, "sol:ny", [10.0] * 20, ages=[500.0] * 20)
    th = c.thresholds(regime="sol:ny")
    assert th.spread_shock_bps_hard >= th.spread_shock_bps
    assert th.book_stale_hard_ms >= th.book_stale_soft_ms


def test_calibrated_spread_within_rails():
    c = SpreadStalenessCalibrator(min_samples=5, enforce=True)
    # feed very large spread values — should be clamped to SPREAD_BPS_CEIL
    _feed(c, "illiquid:na", [400.0, 450.0, 480.0, 490.0, 495.0, 499.0])
    th = c.thresholds(regime="illiquid:na")
    assert th.spread_shock_bps <= 500.0
    assert th.spread_shock_bps_hard <= 500.0


def test_calibrated_floor_enforced():
    c = SpreadStalenessCalibrator(min_samples=5, enforce=True)
    # tiny spread → q90 still at least SPREAD_BPS_FLOOR (unlikely to hit in practice)
    _feed(c, "btcusdt:off", [1.1, 1.2, 1.1, 1.2, 1.1, 1.2])
    th = c.thresholds(regime="btcusdt:off")
    assert th.spread_shock_bps >= SPREAD_BPS_FLOOR


# ── filtering of invalid values ────────────────────────────────────────────────

def test_zero_spread_not_counted():
    c = SpreadStalenessCalibrator(min_samples=3, enforce=True)
    c.observe(regime="na", spread_bps=0.0, book_age_ms=0.0)
    c.observe(regime="na", spread_bps=float("nan"), book_age_ms=0.0)
    c.observe(regime="na", spread_bps=float("inf"), book_age_ms=0.0)
    th = c.thresholds(regime="na")
    assert th.n == 0
    assert th.src == "static"


def test_book_age_zero_not_counted_for_book_estimators():
    c = SpreadStalenessCalibrator(min_samples=3, enforce=True)
    # feed valid spread, but book_age = 0 (fresh book — excluded)
    for sp in [5.0, 6.0, 7.0]:
        c.observe(regime="btcusdt:ny", spread_bps=sp, book_age_ms=0.0)
    # spread is counted (n=3), but book estimators are cold
    th = c.thresholds(regime="btcusdt:ny")
    assert th.src == "calib_q90q95"
    # book thresholds fall back to static defaults (no ba samples)
    assert th.book_stale_soft_ms == DEFAULT_BOOK_STALE_SOFT_MS
    assert th.book_stale_hard_ms == DEFAULT_BOOK_STALE_HARD_MS


def test_book_age_above_ceil_filtered():
    c = SpreadStalenessCalibrator(min_samples=3, enforce=True)
    _feed(c, "na", [5.0, 6.0, 7.0], ages=[35_000.0, 35_000.0, 35_000.0])
    th = c.thresholds(regime="na")
    # spread counted, book_age filtered
    assert th.book_stale_soft_ms == DEFAULT_BOOK_STALE_SOFT_MS


# ── regime isolation ───────────────────────────────────────────────────────────

def test_regime_isolation():
    c = SpreadStalenessCalibrator(min_samples=5, enforce=True)
    _feed(c, "btcusdt:ny", [5.0, 6.0, 7.0, 6.0, 5.5, 6.5])
    th_btc = c.thresholds(regime="btcusdt:ny")
    th_eth = c.thresholds(regime="ethusdt:ny")
    assert th_btc.src == "calib_q90q95"
    assert th_eth.src == "static"


def test_regime_key_normalised_to_lower():
    c = SpreadStalenessCalibrator(min_samples=3, enforce=True)
    _feed(c, "BTCUSDT:NY", [5.0, 6.0, 7.0])
    th = c.thresholds(regime="btcusdt:ny")
    assert th.src == "calib_q90q95"


# ── persistence ────────────────────────────────────────────────────────────────

def test_persistence_roundtrip():
    c = SpreadStalenessCalibrator(min_samples=5, enforce=True)
    _feed(c, "btcusdt:ny", [3.0, 5.0, 8.0, 12.0, 6.0, 4.0, 7.0],
          ages=[100.0, 200.0, 300.0, 400.0, 150.0, 180.0, 250.0])
    st = c.dump_regime_state(symbol="BTCUSDT", regime="btcusdt:ny", updated_ts_ms=1_234_567_890)
    raw = json.dumps(st)

    c2 = SpreadStalenessCalibrator(min_samples=5, enforce=True)
    c2.load_regime_state(json.loads(raw))
    th2 = c2.thresholds(regime="btcusdt:ny")
    assert th2.src == "calib_q90q95"
    assert th2.n == st["n"]
    assert th2.spread_shock_bps > 0
    assert th2.book_stale_soft_ms > 0


def test_persistence_bad_state_silently_ignored():
    c = SpreadStalenessCalibrator()
    c.load_regime_state(None)       # type: ignore
    c.load_regime_state("not-dict") # type: ignore
    c.load_regime_state({})
    # no crash; calibrator stays in default state


def test_loads_valid_json():
    d = SpreadStalenessCalibrator.loads('{"v":1,"kind":"spread_staleness"}')
    assert d is not None
    assert d["v"] == 1


def test_loads_invalid_json_returns_none():
    assert SpreadStalenessCalibrator.loads("not-json") is None
    assert SpreadStalenessCalibrator.loads("null") is None


# ── book age calibration when stale samples provided ──────────────────────────

def test_book_age_calibrated_when_stale_samples_present():
    c = SpreadStalenessCalibrator(min_samples=5, enforce=True)
    ages = [200.0, 300.0, 500.0, 800.0, 1000.0, 600.0, 400.0]
    _feed(c, "xrpusdt:ny", [5.0] * len(ages), ages=ages)
    th = c.thresholds(regime="xrpusdt:ny")
    assert th.src == "calib_q90q95"
    # calibrated values should differ from static defaults (high staleness sample)
    assert th.book_stale_hard_ms >= th.book_stale_soft_ms
    # q90/q95 of [200,300,400,500,600,800,1000] ≈ 700–900 ms range
    assert 100.0 < th.book_stale_soft_ms < 1200.0


# ── custom defaults override ───────────────────────────────────────────────────

def test_custom_defaults_used_when_cold():
    c = SpreadStalenessCalibrator(min_samples=100)
    th = c.thresholds(
        regime="na",
        default_spread_shock_bps=20.0,
        default_spread_shock_bps_hard=40.0,
        default_book_stale_soft_ms=300.0,
        default_book_stale_hard_ms=600.0,
    )
    assert th.spread_shock_bps == 20.0
    assert th.spread_shock_bps_hard == 40.0
    assert th.book_stale_soft_ms == 300.0
    assert th.book_stale_hard_ms == 600.0
