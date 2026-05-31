"""Unit tests for DqMicrostructureCalibrator."""
from __future__ import annotations

import json
import time

from core.dq_microstructure_calibrator import (
    DEFAULT_SPREAD_BPS,
    DEFAULT_STALE_MS,
    SPREAD_FLOOR_BPS,
    SPREAD_CEIL_BPS,
    STALE_CEIL_MS,
    STALE_FLOOR_MS,
    DqMicrostructureCalibrator,
)

_NOW_MS = int(time.time() * 1000)
_2H_AGO = _NOW_MS - 2 * 3_600_000
_10MIN_AGO = _NOW_MS - 10 * 60_000


# ── helpers ───────────────────────────────────────────────────────────────────

def _feed_stale(
    cal: DqMicrostructureCalibrator,
    symbol: str,
    values: list[float],
    now_ms: int | None = None,
) -> None:
    ts = now_ms
    for v in values:
        cal.observe(symbol=symbol, book_stale_ms=v, spread_bps=5.0, now_ms=ts)


def _feed_spread(cal: DqMicrostructureCalibrator, symbol: str, values: list[float]) -> None:
    for v in values:
        cal.observe(symbol=symbol, book_stale_ms=10.0, spread_bps=v)


# ── cold / warmup ─────────────────────────────────────────────────────────────

def test_cold_returns_defaults():
    cal = DqMicrostructureCalibrator(min_samples=200, enforce=True)
    th = cal.thresholds("BTCUSDT")
    assert th.src == "static"
    assert th.stale_flag_ms == DEFAULT_STALE_MS
    assert th.spread_wide_bps == DEFAULT_SPREAD_BPS
    assert th.n == 0
    assert th.promoted is False


def test_pure_shadow_never_enforces():
    """enforce=False, auto_promote=False: always returns static defaults."""
    cal = DqMicrostructureCalibrator(min_samples=5, enforce=False, auto_promote=False)
    _feed_stale(cal, "BTCUSDT", [20.0] * 100, now_ms=_2H_AGO)
    th = cal.thresholds("BTCUSDT", now_ms=_NOW_MS)
    assert th.src == "static"
    assert th.promoted is False


def test_shadow_proposal_populated_regardless_of_mode():
    """Shadow thresholds should reflect calibration even in pure shadow mode."""
    cal = DqMicrostructureCalibrator(min_samples=5, enforce=False, auto_promote=False)
    _feed_stale(cal, "BTCUSDT", [20.0] * 10)
    cal.thresholds("BTCUSDT")
    sh = cal.shadow_thresholds("BTCUSDT")
    assert sh is not None
    assert sh.src == "calib_p99p95"
    assert sh.stale_flag_ms < DEFAULT_STALE_MS  # BTC 20ms × 3 << 1500ms


def test_not_warm_below_min_samples():
    cal = DqMicrostructureCalibrator(min_samples=50, enforce=True)
    _feed_stale(cal, "ETHUSDT", [100.0] * 49)
    th = cal.thresholds("ETHUSDT")
    assert th.src == "static"
    assert th.n == 49


# ── auto-promote ──────────────────────────────────────────────────────────────

def test_auto_promote_fires_after_warmup():
    """auto_promote=True (default): after min_samples + time_ok → calib thresholds."""
    cal = DqMicrostructureCalibrator(
        min_samples=10, enforce=False,
        auto_promote=True, auto_promote_min_hours=0.0,
    )
    _feed_stale(cal, "BTCUSDT", [20.0] * 10, now_ms=_2H_AGO)
    th = cal.thresholds("BTCUSDT", now_ms=_NOW_MS)
    assert th.src == "calib_p99p95_auto"
    assert th.promoted is True
    assert th.stale_flag_ms < DEFAULT_STALE_MS


def test_auto_promote_blocked_by_time_guard():
    """Not enough time elapsed → still static despite n ≥ min_samples."""
    cal = DqMicrostructureCalibrator(
        min_samples=5, enforce=False,
        auto_promote=True, auto_promote_min_hours=1.0,
    )
    # first obs 10 minutes ago — not 1h yet
    _feed_stale(cal, "BTCUSDT", [20.0] * 10, now_ms=_10MIN_AGO)
    th = cal.thresholds("BTCUSDT", now_ms=_NOW_MS)
    assert th.src == "static"
    assert th.promoted is False


def test_auto_promote_blocked_below_min_samples():
    """n < min_samples → stays static even if time criterion met."""
    cal = DqMicrostructureCalibrator(
        min_samples=50, enforce=False,
        auto_promote=True, auto_promote_min_hours=0.0,
    )
    _feed_stale(cal, "BTCUSDT", [20.0] * 10, now_ms=_2H_AGO)
    th = cal.thresholds("BTCUSDT", now_ms=_NOW_MS)
    assert th.src == "static"


def test_auto_promote_is_sticky():
    """Once promoted, subsequent thresholds() calls return calib without re-checking time."""
    cal = DqMicrostructureCalibrator(
        min_samples=5, enforce=False,
        auto_promote=True, auto_promote_min_hours=0.0,
    )
    _feed_stale(cal, "BTCUSDT", [20.0] * 10, now_ms=_2H_AGO)
    # First call promotes
    th1 = cal.thresholds("BTCUSDT", now_ms=_NOW_MS)
    assert th1.promoted is True
    # Second call with now_ms=0 (would fail time guard if not sticky)
    th2 = cal.thresholds("BTCUSDT", now_ms=0)
    assert th2.promoted is True
    assert th2.src == "calib_p99p95_auto"


def test_is_promoted_api():
    cal = DqMicrostructureCalibrator(
        min_samples=5, enforce=False,
        auto_promote=True, auto_promote_min_hours=0.0,
    )
    assert cal.is_promoted("BTCUSDT") is False
    _feed_stale(cal, "BTCUSDT", [20.0] * 10, now_ms=_2H_AGO)
    cal.thresholds("BTCUSDT", now_ms=_NOW_MS)
    assert cal.is_promoted("BTCUSDT") is True


def test_auto_promote_per_symbol_independent():
    """Each symbol has its own promotion state."""
    cal = DqMicrostructureCalibrator(
        min_samples=5, enforce=False,
        auto_promote=True, auto_promote_min_hours=0.0,
    )
    _feed_stale(cal, "BTCUSDT", [20.0] * 10, now_ms=_2H_AGO)
    _feed_stale(cal, "ETHUSDT", [20.0] * 3, now_ms=_2H_AGO)  # cold

    btc = cal.thresholds("BTCUSDT", now_ms=_NOW_MS)
    eth = cal.thresholds("ETHUSDT", now_ms=_NOW_MS)
    assert btc.promoted is True
    assert eth.promoted is False
    assert eth.src == "static"


def test_force_enforce_ignores_time_guard():
    """enforce=True: promoted immediately after min_samples, no time guard."""
    cal = DqMicrostructureCalibrator(
        min_samples=5, enforce=True,
        auto_promote=False, auto_promote_min_hours=999.0,
    )
    _feed_stale(cal, "BTCUSDT", [20.0] * 10, now_ms=_10MIN_AGO)
    th = cal.thresholds("BTCUSDT", now_ms=_NOW_MS)
    assert th.src == "calib_p99p95"
    # force-enforce does NOT set promoted flag (it's a separate concept)
    assert th.promoted is False


# ── calibration math ──────────────────────────────────────────────────────────

def test_btc_like_thresholds_much_lower_than_default():
    """BTC: book updates every ~20ms → threshold << 1500ms."""
    cal = DqMicrostructureCalibrator(min_samples=10, enforce=True)
    import random
    random.seed(42)
    vals = [random.uniform(10, 50) for _ in range(90)] + [random.uniform(80, 120) for _ in range(10)]
    _feed_stale(cal, "BTCUSDT", vals)
    th = cal.thresholds("BTCUSDT")
    assert th.src == "calib_p99p95"
    assert th.stale_flag_ms < 500.0
    assert th.stale_flag_ms >= STALE_FLOOR_MS


def test_pepe_like_thresholds_higher_than_btc():
    """PEPE: slower book → higher threshold than BTC."""
    cal = DqMicrostructureCalibrator(min_samples=10, enforce=True)
    import random
    random.seed(7)
    _feed_stale(cal, "BTCUSDT", [random.uniform(10, 50) for _ in range(100)])
    _feed_stale(cal, "PEPEUSDT", [random.uniform(200, 500) for _ in range(90)] + [random.uniform(700, 900) for _ in range(10)])
    assert cal.thresholds("PEPEUSDT").stale_flag_ms > cal.thresholds("BTCUSDT").stale_flag_ms


def test_stale_threshold_floor_respected():
    cal = DqMicrostructureCalibrator(min_samples=5, enforce=True)
    _feed_stale(cal, "TESTUSDT", [1.0] * 20)
    assert cal.thresholds("TESTUSDT").stale_flag_ms >= STALE_FLOOR_MS


def test_stale_threshold_ceil_respected():
    cal = DqMicrostructureCalibrator(min_samples=5, enforce=True)
    _feed_stale(cal, "SLOWUSDT", [9_000.0] * 20)
    assert cal.thresholds("SLOWUSDT").stale_flag_ms <= STALE_CEIL_MS


def test_spread_threshold_scales_with_symbol():
    cal = DqMicrostructureCalibrator(min_samples=5, enforce=True)
    _feed_spread(cal, "BTCUSDT", [1.0, 1.2, 1.5, 1.3, 1.4, 1.1, 1.6, 1.2, 1.4, 1.5])
    _feed_spread(cal, "SHIBUSDT", [28, 30, 32, 29, 31, 30, 33, 28, 31, 30])
    assert cal.thresholds("SHIBUSDT").spread_wide_bps > cal.thresholds("BTCUSDT").spread_wide_bps


def test_spread_threshold_floor_respected():
    cal = DqMicrostructureCalibrator(min_samples=5, enforce=True)
    _feed_spread(cal, "TESTUSDT", [0.1] * 10)
    assert cal.thresholds("TESTUSDT").spread_wide_bps >= SPREAD_FLOOR_BPS


def test_spread_threshold_ceil_respected():
    cal = DqMicrostructureCalibrator(min_samples=5, enforce=True)
    _feed_spread(cal, "TESTUSDT", [400.0] * 10)
    assert cal.thresholds("TESTUSDT").spread_wide_bps <= SPREAD_CEIL_BPS


# ── observe input validation ──────────────────────────────────────────────────

def test_observe_ignores_zero_stale():
    cal = DqMicrostructureCalibrator(min_samples=5, enforce=True)
    for _ in range(100):
        cal.observe(symbol="BTCUSDT", book_stale_ms=0, spread_bps=2.0)
    assert cal._n.get("BTCUSDT", 0) > 0
    assert "BTCUSDT" not in cal._st99


def test_observe_ignores_nan_stale():
    cal = DqMicrostructureCalibrator(min_samples=5)
    cal.observe(symbol="BTCUSDT", book_stale_ms=float("nan"), spread_bps=2.0)
    assert "BTCUSDT" not in cal._st99


def test_observe_ignores_out_of_range_spread():
    cal = DqMicrostructureCalibrator(min_samples=5)
    cal.observe(symbol="BTCUSDT", book_stale_ms=20.0, spread_bps=0.0)
    cal.observe(symbol="BTCUSDT", book_stale_ms=20.0, spread_bps=600.0)
    assert "BTCUSDT" not in cal._sp95


def test_observe_empty_symbol_ignored():
    cal = DqMicrostructureCalibrator()
    cal.observe(symbol="", book_stale_ms=100.0, spread_bps=5.0)
    assert not cal._n


def test_observe_records_first_obs_ms():
    cal = DqMicrostructureCalibrator()
    cal.observe(symbol="BTCUSDT", book_stale_ms=50.0, spread_bps=2.0, now_ms=12345)
    assert cal._first_obs_ms.get("BTCUSDT") == 12345
    # Second observe does NOT overwrite
    cal.observe(symbol="BTCUSDT", book_stale_ms=50.0, spread_bps=2.0, now_ms=99999)
    assert cal._first_obs_ms.get("BTCUSDT") == 12345


# ── persistence ───────────────────────────────────────────────────────────────

def test_dump_load_roundtrip():
    cal1 = DqMicrostructureCalibrator(min_samples=5, enforce=True)
    _feed_stale(cal1, "BTCUSDT", [20.0] * 10)
    _feed_spread(cal1, "BTCUSDT", [1.5] * 10)
    th1 = cal1.thresholds("BTCUSDT")

    state = cal1.dump_symbol_state(symbol="BTCUSDT", updated_ts_ms=1_700_000_000_000)
    assert state["schema_version"] == 2
    assert state["kind"] == "dq_micro"
    assert state["n"] == 20  # _feed_stale(10) + _feed_spread(10) — оба observe валидны

    cal2 = DqMicrostructureCalibrator(min_samples=5, enforce=True)
    cal2.load_symbol_state(state)
    th2 = cal2.thresholds("BTCUSDT")
    assert th2.src == th1.src
    assert abs(th2.stale_flag_ms - th1.stale_flag_ms) < 0.1
    assert abs(th2.spread_wide_bps - th1.spread_wide_bps) < 0.1


def test_dump_load_preserves_promoted_flag():
    """Promoted state must survive dump/load (sticky across restarts)."""
    cal1 = DqMicrostructureCalibrator(
        min_samples=5, enforce=False,
        auto_promote=True, auto_promote_min_hours=0.0,
    )
    _feed_stale(cal1, "BTCUSDT", [20.0] * 10, now_ms=_2H_AGO)
    cal1.thresholds("BTCUSDT", now_ms=_NOW_MS)  # triggers promotion
    assert cal1.is_promoted("BTCUSDT")

    state = cal1.dump_symbol_state(symbol="BTCUSDT", updated_ts_ms=_NOW_MS)
    assert state["promoted"] is True

    cal2 = DqMicrostructureCalibrator(
        min_samples=5, enforce=False,
        auto_promote=True, auto_promote_min_hours=0.0,
    )
    cal2.load_symbol_state(state)
    assert cal2.is_promoted("BTCUSDT")
    # After reload, thresholds() should return calib immediately
    th = cal2.thresholds("BTCUSDT", now_ms=_NOW_MS)
    assert th.promoted is True


def test_dump_load_preserves_first_obs_ms():
    cal1 = DqMicrostructureCalibrator(min_samples=5)
    _feed_stale(cal1, "BTCUSDT", [20.0] * 5, now_ms=_2H_AGO)
    state = cal1.dump_symbol_state(symbol="BTCUSDT", updated_ts_ms=_NOW_MS)
    assert state["first_obs_ms"] == _2H_AGO

    cal2 = DqMicrostructureCalibrator(min_samples=5)
    cal2.load_symbol_state(state)
    assert cal2._first_obs_ms.get("BTCUSDT") == _2H_AGO


def test_load_schema_v1_compat():
    """v1 state (no promoted/first_obs_ms fields) loads without error."""
    state = {
        "schema_version": 1, "kind": "dq_micro", "symbol": "BTCUSDT",
        "updated_ms": 0, "min_samples": 5, "enforce": False, "n": 10,
        "st99": None, "sp95": None,
    }
    cal = DqMicrostructureCalibrator(min_samples=5)
    cal.load_symbol_state(state)
    assert cal._n.get("BTCUSDT") == 10
    assert cal.is_promoted("BTCUSDT") is False


def test_dump_load_json_roundtrip():
    cal1 = DqMicrostructureCalibrator(min_samples=5, enforce=True)
    _feed_stale(cal1, "ETHUSDT", [50.0] * 8)
    raw_json = json.dumps(cal1.dump_symbol_state(symbol="ETHUSDT", updated_ts_ms=123456))
    cal2 = DqMicrostructureCalibrator(min_samples=5, enforce=True)
    cal2.load_symbol_state(DqMicrostructureCalibrator.loads(raw_json))
    assert cal2._n.get("ETHUSDT", 0) == 8


def test_load_bad_state_fail_open():
    cal = DqMicrostructureCalibrator(min_samples=5)
    cal.load_symbol_state(None)
    cal.load_symbol_state("not a dict")
    cal.load_symbol_state({"schema_version": 99, "symbol": "X"})
    assert cal.thresholds("BTCUSDT").src == "static"


def test_load_wrong_schema_version_ignored():
    cal = DqMicrostructureCalibrator(min_samples=5, enforce=True)
    _feed_stale(cal, "BTCUSDT", [20.0] * 10)
    state = cal.dump_symbol_state(symbol="BTCUSDT", updated_ts_ms=0)
    state["schema_version"] = 99
    cal2 = DqMicrostructureCalibrator(min_samples=5, enforce=True)
    cal2.load_symbol_state(state)
    assert cal2._n.get("BTCUSDT", 0) == 0


# ── per-symbol isolation ──────────────────────────────────────────────────────

def test_symbols_isolated():
    cal = DqMicrostructureCalibrator(min_samples=5, enforce=True)
    _feed_stale(cal, "BTCUSDT", [20.0] * 10)
    _feed_stale(cal, "PEPEUSDT", [400.0] * 10)
    assert cal.thresholds("BTCUSDT").stale_flag_ms != cal.thresholds("PEPEUSDT").stale_flag_ms


def test_case_insensitive_symbol():
    cal = DqMicrostructureCalibrator(min_samples=5, enforce=True)
    _feed_stale(cal, "btcusdt", [20.0] * 10)
    assert cal.thresholds("BTCUSDT").n == 10


# ── shortcut methods ─────────────────────────────────────────────────────────

def test_stale_threshold_shortcut_matches_thresholds():
    cal = DqMicrostructureCalibrator(min_samples=5, enforce=True)
    _feed_stale(cal, "BTCUSDT", [20.0] * 10)
    assert cal.stale_threshold("BTCUSDT") == cal.thresholds("BTCUSDT").stale_flag_ms


def test_spread_threshold_shortcut_matches_thresholds():
    cal = DqMicrostructureCalibrator(min_samples=5, enforce=True)
    _feed_spread(cal, "BTCUSDT", [2.0] * 10)
    assert cal.spread_threshold("BTCUSDT") == cal.thresholds("BTCUSDT").spread_wide_bps


# ── n counter ────────────────────────────────────────────────────────────────

def test_n_counts_observations_with_any_valid_input():
    cal = DqMicrostructureCalibrator(min_samples=200)
    cal.observe(symbol="X", book_stale_ms=100.0, spread_bps=0.0)   # only stale valid
    cal.observe(symbol="X", book_stale_ms=0.0, spread_bps=5.0)     # only spread valid
    cal.observe(symbol="X", book_stale_ms=50.0, spread_bps=3.0)    # both valid
    cal.observe(symbol="X", book_stale_ms=0.0, spread_bps=0.0)     # neither valid
    assert cal._n.get("X", 0) == 3


# ── default overrides ─────────────────────────────────────────────────────────

def test_custom_defaults_used_when_cold():
    cal = DqMicrostructureCalibrator(
        default_stale_ms=3000.0, default_spread_bps=25.0, enforce=True,
    )
    th = cal.thresholds("BTCUSDT")
    assert th.stale_flag_ms == 3000.0
    assert th.spread_wide_bps == 25.0
