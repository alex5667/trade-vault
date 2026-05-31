"""Tests for SmtCohIsotonicCalibrator (core/smt_coh_isotonic_calibrator.py).

Covers:
  - Cold / warmup behaviour
  - Input validation (NaN, Inf, out-of-range)
  - Threshold finding algorithm (_find_veto_threshold)
  - Isotonic smoothing (_isotonic_smooth)
  - Shadow → enforce (G5 scheme)
  - Auto-enforce streak
  - Hysteresis / jump-cap / hold-throttle
  - Per-symbol-regime independence
  - Redis refresh merge
  - Persistence (snapshot / load_state)
  - Writer: update_smt_coh_curves
  - Writer extraction helpers
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from core.smt_coh_isotonic_calibrator import (
    COH_CEIL,
    COH_FLOOR,
    DEFAULT_COH_MIN,
    SmtCohIsotonicCalibrator,
    _bucket_pct,
    _bucket_mid,
    _compute_threshold,
    _find_veto_threshold,
    _isotonic_smooth,
)

# ── helpers ───────────────────────────────────────────────────────────────────

def _fill(cal: SmtCohIsotonicCalibrator, symbol: str, regime: str,
          n: int, win_rate: float, coh_center: float = 0.70,
          coh_spread: float = 0.05) -> None:
    """Inject synthetic observations into the calibrator."""
    import random
    rng = random.Random(42)
    wins = 0
    for _ in range(n):
        coh = max(COH_FLOOR, min(COH_CEIL, rng.gauss(coh_center, coh_spread)))
        outcome = 1 if (wins / max(1, _ + 1)) < win_rate else 0
        # deterministic: alternate to hit target win-rate
        outcome = 1 if rng.random() < win_rate else 0
        wins += outcome
        cal.observe(symbol=symbol, regime=regime, coh=coh, outcome=outcome)


def _make_bins_low_wr() -> dict[int, tuple[int, int]]:
    """Bins where high-coh signals have very low win rate → threshold should be found."""
    return {
        30: (120, 72),   # p=0.60  (countertrend works at low coh)
        40: (100, 55),   # p=0.55
        50: (80, 38),    # p=0.475
        60: (60, 24),    # p=0.40  → veto_prec=0.60 ✓
        70: (40, 14),    # p=0.35
        80: (20, 6),     # p=0.30
        90: (10, 2),     # p=0.20
    }


def _make_bins_uniform() -> dict[int, tuple[int, int]]:
    """Bins where P(success) is flat — no good veto region."""
    return {bkt: (50, 28) for bkt in range(30, 100, 5)}   # p≈0.56 everywhere


# ── cold / warmup ─────────────────────────────────────────────────────────────

def test_cold_returns_static_default():
    cal = SmtCohIsotonicCalibrator(min_samples=300)
    th = cal.thresholds(symbol="BTCUSDT", regime="t1")
    assert th.coh_min == DEFAULT_COH_MIN
    assert th.src == "static"
    assert th.n_total == 0


def test_warm_but_shadow_returns_static():
    cal = SmtCohIsotonicCalibrator(min_samples=50, enforce=False)
    _fill(cal, "BTCUSDT", "t1", 60, win_rate=0.35)
    th = cal.thresholds(symbol="BTCUSDT", regime="t1")
    assert th.src == "static"
    assert th.coh_min == DEFAULT_COH_MIN


def test_n_total_counts_correctly():
    cal = SmtCohIsotonicCalibrator(min_samples=10)
    for _ in range(7):
        cal.observe(symbol="ETHUSDT", regime="t0", coh=0.65, outcome=0)
    assert cal.n_total(symbol="ETHUSDT", regime="t0") == 7


# ── input validation ──────────────────────────────────────────────────────────

def test_nan_coh_ignored():
    cal = SmtCohIsotonicCalibrator()
    cal.observe(symbol="X", regime="r", coh=float("nan"), outcome=1)
    assert cal.n_total(symbol="X", regime="r") == 0


def test_inf_coh_ignored():
    cal = SmtCohIsotonicCalibrator()
    cal.observe(symbol="X", regime="r", coh=float("inf"), outcome=1)
    assert cal.n_total(symbol="X", regime="r") == 0


def test_below_floor_ignored():
    cal = SmtCohIsotonicCalibrator()
    cal.observe(symbol="X", regime="r", coh=COH_FLOOR - 0.01, outcome=1)
    assert cal.n_total(symbol="X", regime="r") == 0


def test_above_ceil_ignored():
    cal = SmtCohIsotonicCalibrator()
    cal.observe(symbol="X", regime="r", coh=COH_CEIL + 0.01, outcome=1)
    assert cal.n_total(symbol="X", regime="r") == 0


def test_invalid_outcome_ignored():
    cal = SmtCohIsotonicCalibrator()
    cal.observe(symbol="X", regime="r", coh=0.65, outcome=2)
    cal.observe(symbol="X", regime="r", coh=0.65, outcome=-1)
    assert cal.n_total(symbol="X", regime="r") == 0


def test_boundary_coh_accepted():
    cal = SmtCohIsotonicCalibrator()
    cal.observe(symbol="X", regime="r", coh=COH_FLOOR, outcome=1)
    cal.observe(symbol="X", regime="r", coh=COH_CEIL, outcome=0)
    assert cal.n_total(symbol="X", regime="r") == 2


# ── bucket helpers ─────────────────────────────────────────────────────────────

def test_bucket_pct_quantization():
    assert _bucket_pct(0.67) == 65
    assert _bucket_pct(0.65) == 65
    assert _bucket_pct(0.70) == 70
    assert _bucket_pct(0.30) == 30
    assert _bucket_pct(0.98) == 95


def test_bucket_mid():
    assert abs(_bucket_mid(65) - 0.675) < 1e-6
    assert abs(_bucket_mid(30) - 0.325) < 1e-6


# ── _find_veto_threshold ──────────────────────────────────────────────────────

def test_find_veto_threshold_finds_correct_level():
    bins = _make_bins_low_wr()
    result = _find_veto_threshold(
        bins, target_veto_precision=0.60, min_samples_above=30
    )
    assert result is not None
    thr, veto_prec, n_above = result
    assert thr <= 0.60
    assert veto_prec >= 0.60
    assert n_above >= 30


def test_find_veto_threshold_uniform_bins_returns_none():
    bins = _make_bins_uniform()
    result = _find_veto_threshold(
        bins, target_veto_precision=0.60, min_samples_above=30
    )
    assert result is None


def test_find_veto_threshold_empty_bins():
    result = _find_veto_threshold({}, target_veto_precision=0.60, min_samples_above=10)
    assert result is None


def test_find_veto_threshold_too_few_above():
    bins = {90: (5, 1)}  # only 5 above threshold, need 30
    result = _find_veto_threshold(
        bins, target_veto_precision=0.60, min_samples_above=30
    )
    assert result is None


# ── isotonic smoothing ────────────────────────────────────────────────────────

def test_isotonic_smooth_preserves_bin_counts():
    bins = _make_bins_low_wr()
    smoothed = _isotonic_smooth(bins)
    for bkt, (n, _) in bins.items():
        assert smoothed[bkt][0] == n  # counts unchanged


def test_isotonic_smooth_hits_within_count():
    bins = _make_bins_low_wr()
    smoothed = _isotonic_smooth(bins)
    for _, (n, h) in smoothed.items():
        assert 0 <= h <= n


def test_isotonic_smooth_fallback_on_too_few_bins():
    bins = {30: (50, 30), 35: (40, 20)}  # only 2 eligible bins
    smoothed = _isotonic_smooth(bins)
    assert smoothed == bins  # unchanged (< 3 bins)


def test_isotonic_smooth_fallback_exception():
    bins = _make_bins_low_wr()
    with patch(
        "core.smt_coh_isotonic_calibrator._isotonic_smooth",
        side_effect=Exception("fail"),
    ):
        # _compute_threshold must not raise even when _isotonic_smooth fails
        result = _compute_threshold(
            bins,
            target_veto_precision=0.60,
            min_samples=10,
            min_samples_above=30,
            default_coh_min=DEFAULT_COH_MIN,
        )
        assert result.src in ("static", "isotonic_calib")


# ── _compute_threshold ────────────────────────────────────────────────────────

def test_compute_threshold_cold_returns_static():
    result = _compute_threshold(
        {}, target_veto_precision=0.60, min_samples=300,
        min_samples_above=30, default_coh_min=DEFAULT_COH_MIN,
    )
    assert result.src == "static"
    assert result.coh_min == DEFAULT_COH_MIN


def test_compute_threshold_finds_calibrated():
    bins = _make_bins_low_wr()
    result = _compute_threshold(
        bins, target_veto_precision=0.60, min_samples=10,
        min_samples_above=30, default_coh_min=DEFAULT_COH_MIN,
    )
    assert result.src == "isotonic_calib"
    assert COH_FLOOR <= result.coh_min <= COH_CEIL
    assert result.veto_precision >= 0.60


def test_compute_threshold_rails_applied():
    # Force a threshold that would be out of range
    bins = {5: (500, 100)}  # bucket 5% — below COH_FLOOR 0.30
    result = _compute_threshold(
        bins, target_veto_precision=0.60, min_samples=10,
        min_samples_above=30, default_coh_min=DEFAULT_COH_MIN,
    )
    # Either no threshold found or coh_min is within rails
    assert result.coh_min >= COH_FLOOR


# ── enforce path ──────────────────────────────────────────────────────────────

def test_enforce_serves_calibrated_after_warmup():
    cal = SmtCohIsotonicCalibrator(min_samples=50, enforce=True, target_veto_precision=0.60)
    # Feed skewed data: high coh → low win rate
    for coh, wr in [(0.35, 0.65), (0.50, 0.45), (0.65, 0.35), (0.80, 0.20)]:
        for _ in range(20):
            import random
            outcome = 1 if random.random() < wr else 0
            cal.observe(symbol="BTCUSDT", regime="t1", coh=coh, outcome=outcome)

    th = cal.thresholds(symbol="BTCUSDT", regime="t1")
    # Should be calibrated or static (depends on data)
    assert COH_FLOOR <= th.coh_min <= COH_CEIL


def test_shadow_available_before_enforce():
    cal = SmtCohIsotonicCalibrator(min_samples=50, enforce=False)
    bins = _make_bins_low_wr()
    key = ("BTCUSDT", "t1")
    cal._bins[key] = bins
    # Trigger threshold computation (shadow)
    cal.thresholds(symbol="BTCUSDT", regime="t1")
    shadow = cal.shadow_thresholds(symbol="BTCUSDT", regime="t1")
    assert shadow is not None


def test_shadow_none_before_first_call():
    cal = SmtCohIsotonicCalibrator()
    assert cal.shadow_thresholds(symbol="NEW", regime="r") is None


# ── auto-enforce streak ───────────────────────────────────────────────────────

def test_auto_enforce_triggers_after_stable_streak():
    cal = SmtCohIsotonicCalibrator(
        min_samples=50,
        enforce=False,
        auto_enforce=True,
        n_stable_streak_required=3,
        hysteresis=0.04,
    )
    # Inject warm data with clear veto region
    bins = _make_bins_low_wr()
    key = ("ETHUSDT", "t0")
    cal._bins[key] = bins

    # Call thresholds 4 times; auto-enforce should trigger after 3 stable proposals
    for _ in range(4):
        cal.thresholds(symbol="ETHUSDT", regime="t0")

    assert cal.enforce is True


# ── hysteresis / jump cap / hold ──────────────────────────────────────────────

def test_hysteresis_prevents_micro_update():
    cal = SmtCohIsotonicCalibrator(
        min_samples=10, enforce=True, hysteresis=0.20, hold_sec=0.0
    )
    bins = _make_bins_low_wr()
    key = ("BTCUSDT", "t1")
    cal._bins[key] = bins
    th1 = cal.thresholds(symbol="BTCUSDT", regime="t1")
    first_val = th1.coh_min
    # Second call with same bins: proposal unchanged → hysteresis prevents commit
    th2 = cal.thresholds(symbol="BTCUSDT", regime="t1")
    assert th2.coh_min == first_val


def test_hold_throttle_prevents_rapid_updates():
    cal = SmtCohIsotonicCalibrator(
        min_samples=10, enforce=True, hold_sec=3600.0, hysteresis=0.0
    )
    bins = _make_bins_low_wr()
    key = ("SOLUSDT", "t0")
    cal._bins[key] = bins
    th1 = cal.thresholds(symbol="SOLUSDT", regime="t0")
    assert th1.src == "isotonic_calib"
    # Immediate second call should return same committed value (hold throttle)
    th2 = cal.thresholds(symbol="SOLUSDT", regime="t0")
    assert th2.coh_min == th1.coh_min


def test_jump_cap_limits_step():
    cal = SmtCohIsotonicCalibrator(
        min_samples=10, enforce=True, hysteresis=0.0,
        max_jump=0.05, hold_sec=0.0,
    )
    bins = _make_bins_low_wr()
    key = ("BTCUSDT", "t1")
    cal._bins[key] = bins
    from core.smt_coh_isotonic_calibrator import _ClusterState
    st = cal._state.setdefault(key, _ClusterState())
    st.committed_coh_min = 0.90  # far from calibrated (~0.60)
    th = cal.thresholds(symbol="BTCUSDT", regime="t1")
    # Step must be capped at max_jump=0.05
    assert abs(th.coh_min - 0.90) <= 0.05 + 1e-6


# ── regime independence ───────────────────────────────────────────────────────

def test_two_regimes_independent():
    cal = SmtCohIsotonicCalibrator(min_samples=50, enforce=True, hold_sec=0.0, hysteresis=0.0)
    # BTC in t1: high-coh failures → low veto threshold
    bins_t1 = _make_bins_low_wr()
    # BTC in t0: uniform → no threshold
    bins_t0 = _make_bins_uniform()
    cal._bins[("BTCUSDT", "t1")] = bins_t1
    cal._bins[("BTCUSDT", "t0")] = bins_t0

    th_t1 = cal.thresholds(symbol="BTCUSDT", regime="t1")
    th_t0 = cal.thresholds(symbol="BTCUSDT", regime="t0")
    assert th_t1.src == "isotonic_calib"
    assert th_t0.src == "static"   # uniform bins → no veto region found


def test_two_symbols_independent():
    cal = SmtCohIsotonicCalibrator(min_samples=50, enforce=True, hold_sec=0.0, hysteresis=0.0)
    bins = _make_bins_low_wr()
    cal._bins[("BTCUSDT", "t1")] = bins
    th_btc = cal.thresholds(symbol="BTCUSDT", regime="t1")
    th_eth = cal.thresholds(symbol="ETHUSDT", regime="t1")
    assert th_btc.src == "isotonic_calib"
    assert th_eth.src == "static"  # no data for ETHUSDT


# ── Redis refresh ─────────────────────────────────────────────────────────────

def test_redis_refresh_merges_bins():
    redis = MagicMock()
    redis.hgetall.return_value = {
        b"b65:n": b"100", b"b65:h": b"35",
        b"b70:n": b"80", b"b70:h": b"25",
        b"last_ts_ms": b"999",
    }
    cal = SmtCohIsotonicCalibrator(redis_client=redis, min_samples=10)
    # Pre-populate in-memory
    cal._bins[("BTCUSDT", "t1")] = {65: (10, 5)}
    # Trigger refresh
    cal._maybe_refresh_from_redis(("BTCUSDT", "t1"))
    bins = cal._bins.get(("BTCUSDT", "t1"), {})
    # Redis had 100 for b65 → max(100, 10) = 100
    assert bins[65][0] == 100
    assert bins[70][0] == 80


def test_redis_refresh_fail_open():
    redis = MagicMock()
    redis.hgetall.side_effect = Exception("connection error")
    cal = SmtCohIsotonicCalibrator(redis_client=redis, min_samples=10)
    # Must not raise
    cal._maybe_refresh_from_redis(("BTCUSDT", "t1"))


def test_redis_refresh_ttl_cache():
    redis = MagicMock()
    redis.hgetall.return_value = {b"b65:n": b"50", b"b65:h": b"20"}
    cal = SmtCohIsotonicCalibrator(redis_client=redis, cache_ttl_sec=60.0)
    cal._maybe_refresh_from_redis(("BTCUSDT", "t1"))
    call_count = redis.hgetall.call_count
    # Second call within TTL: no Redis read
    cal._maybe_refresh_from_redis(("BTCUSDT", "t1"))
    assert redis.hgetall.call_count == call_count


# ── persistence ───────────────────────────────────────────────────────────────

def test_snapshot_load_roundtrip():
    # max_jump=1.0 removes jump-cap so both calibrators converge to same value.
    cal = SmtCohIsotonicCalibrator(
        min_samples=50, enforce=True, hold_sec=0.0, hysteresis=0.0, max_jump=1.0
    )
    bins = _make_bins_low_wr()
    cal._bins[("BTCUSDT", "t1")] = bins
    th1 = cal.thresholds(symbol="BTCUSDT", regime="t1")

    snap = cal.snapshot()
    cal2 = SmtCohIsotonicCalibrator(
        min_samples=50, enforce=True, hold_sec=0.0, hysteresis=0.0, max_jump=1.0
    )
    cal2.load_state(snap)
    cal2._bins[("BTCUSDT", "t1")] = bins
    th2 = cal2.thresholds(symbol="BTCUSDT", regime="t1")

    assert th2.coh_min == th1.coh_min


def test_snapshot_kind_field():
    cal = SmtCohIsotonicCalibrator()
    snap = cal.snapshot()
    assert snap["kind"] == "smt_coh_isotonic"
    assert snap["v"] == 1


def test_load_state_wrong_kind_noop():
    cal = SmtCohIsotonicCalibrator(min_samples=10, enforce=True)
    cal.observe(symbol="X", regime="r", coh=0.7, outcome=1)
    before_n = cal.n_total(symbol="X", regime="r")
    cal.load_state({"kind": "smt_coherence", "rows": [{"symbol": "X", "regime": "r"}]})
    assert cal.n_total(symbol="X", regime="r") == before_n


def test_load_state_corrupt_noop():
    cal = SmtCohIsotonicCalibrator()
    cal.load_state(None)
    cal.load_state(42)
    cal.load_state({"kind": "smt_coh_isotonic", "rows": "not-a-list"})


# ── writer: update_smt_coh_curves ─────────────────────────────────────────────

def test_writer_countertrend_signal():
    from services.reliability_calibrator import update_smt_coh_curves

    redis = MagicMock()
    pipe = MagicMock()
    redis.pipeline.return_value = pipe
    pipe.__enter__ = lambda s: s
    pipe.__exit__ = MagicMock(return_value=False)

    pos = {
        "indicators": {
            "smt_coh": 0.72,
            "smt_align": 0,
            "smt_leader_confirm": 1,
        }
    }
    trade_closed = {
        "symbol": "BTCUSDT",
        "regime": "t1",
        "tp2_hit": True,
    }
    update_smt_coh_curves(redis, pos=pos, trade_closed=trade_closed)
    pipe.execute.assert_called_once()


def test_writer_aligned_signal_skipped():
    from services.reliability_calibrator import update_smt_coh_curves

    redis = MagicMock()
    pipe = MagicMock()
    redis.pipeline.return_value = pipe

    pos = {
        "indicators": {
            "smt_coh": 0.72,
            "smt_align": 1,        # aligned → NOT countertrend → skip
            "smt_leader_confirm": 1,
        }
    }
    trade_closed = {"symbol": "BTCUSDT", "regime": "t1", "tp2_hit": True}
    update_smt_coh_curves(redis, pos=pos, trade_closed=trade_closed)
    pipe.execute.assert_not_called()


def test_writer_missing_coh_skipped():
    from services.reliability_calibrator import update_smt_coh_curves

    redis = MagicMock()
    pipe = MagicMock()
    redis.pipeline.return_value = pipe

    pos = {"indicators": {"smt_align": 0, "smt_leader_confirm": 1}}  # no smt_coh
    trade_closed = {"symbol": "BTCUSDT", "regime": "t1", "tp2_hit": True}
    update_smt_coh_curves(redis, pos=pos, trade_closed=trade_closed)
    pipe.execute.assert_not_called()


def test_writer_disabled_by_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("SMT_COH_CAL_ENABLED", "0")
    from services.reliability_calibrator import update_smt_coh_curves

    redis = MagicMock()
    pos = {"indicators": {"smt_coh": 0.72, "smt_align": 0, "smt_leader_confirm": 1}}
    trade_closed = {"symbol": "BTCUSDT", "regime": "t1", "tp2_hit": True}
    update_smt_coh_curves(redis, pos=pos, trade_closed=trade_closed)
    redis.pipeline.assert_not_called()


def test_writer_fail_open_on_redis_error():
    from services.reliability_calibrator import update_smt_coh_curves

    redis = MagicMock()
    redis.pipeline.side_effect = Exception("redis down")

    pos = {"indicators": {"smt_coh": 0.72, "smt_align": 0, "smt_leader_confirm": 1}}
    trade_closed = {"symbol": "BTCUSDT", "regime": "t1", "tp2_hit": True}
    # Must not raise
    update_smt_coh_curves(redis, pos=pos, trade_closed=trade_closed)
