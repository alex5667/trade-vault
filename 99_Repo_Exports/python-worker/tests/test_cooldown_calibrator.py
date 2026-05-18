"""Tests for CooldownCalibrator (core/cooldown_calibrator.py)."""
from __future__ import annotations

from core.cooldown_calibrator import (
    COOLDOWN_CEIL_MS,
    COOLDOWN_FLOOR_MS,
    DEFAULT_COOLDOWN_MS,
    CooldownCalibrator,
)


# ── helpers ───────────────────────────────────────────────────────────────────

def _feed_intervals(cal: CooldownCalibrator, symbol: str, n_signals: int, interval_ms: float = 30_000.0) -> None:
    """Simulate n_signals with fixed interval_ms between them."""
    ts = 1_000_000.0
    for _ in range(n_signals):
        cal.observe(symbol=symbol, emit_ts_ms=ts)
        ts += interval_ms


def _warm(cal: CooldownCalibrator, symbol: str, n: int = 150, interval_ms: float = 30_000.0) -> None:
    _feed_intervals(cal, symbol, n, interval_ms)


# ── cold / warmup ─────────────────────────────────────────────────────────────

def test_cold_returns_static_default():
    cal = CooldownCalibrator(min_signals=100)
    th = cal.thresholds(symbol="BTCUSDT")
    assert th.cooldown_ms == DEFAULT_COOLDOWN_MS
    assert th.src == "static"
    assert th.n == 0


def test_static_default_is_zero():
    assert DEFAULT_COOLDOWN_MS == 0.0


def test_auto_enforce_false_never_calibrates():
    cal = CooldownCalibrator(min_signals=10, enforce=False, auto_enforce=False)
    _warm(cal, "BTCUSDT", n=50)
    th = cal.thresholds(symbol="BTCUSDT")
    assert th.src == "static"


def test_auto_enforce_activates_after_warmup():
    cal = CooldownCalibrator(min_signals=100, enforce=False, auto_enforce=True)
    _warm(cal, "BTCUSDT", n=150)
    th = cal.thresholds(symbol="BTCUSDT")
    assert th.src == "calib_q80"
    assert th.n >= 100


def test_enforce_true_before_warmup():
    cal = CooldownCalibrator(min_signals=1000, enforce=True, auto_enforce=False)
    _warm(cal, "BTCUSDT", n=50)
    th = cal.thresholds(symbol="BTCUSDT")
    assert th.src == "calib_q80"


# ── interval computation ──────────────────────────────────────────────────────

def test_first_emit_does_not_count_interval():
    cal = CooldownCalibrator()
    cal.observe(symbol="BTCUSDT", emit_ts_ms=1_000_000.0)
    assert cal.n("BTCUSDT") == 0


def test_second_emit_counts_one_interval():
    cal = CooldownCalibrator()
    cal.observe(symbol="BTCUSDT", emit_ts_ms=1_000_000.0)
    cal.observe(symbol="BTCUSDT", emit_ts_ms=1_060_000.0)  # +60s
    assert cal.n("BTCUSDT") == 1


def test_interval_below_floor_not_counted():
    cal = CooldownCalibrator()
    # < 1000ms interval → below COOLDOWN_FLOOR_MS
    cal.observe(symbol="BTCUSDT", emit_ts_ms=1_000_000.0)
    cal.observe(symbol="BTCUSDT", emit_ts_ms=1_000_500.0)  # only 500ms
    assert cal.n("BTCUSDT") == 0


def test_interval_above_ceil_not_counted():
    cal = CooldownCalibrator()
    # > 300_000ms interval → above COOLDOWN_CEIL_MS
    cal.observe(symbol="BTCUSDT", emit_ts_ms=1_000_000.0)
    cal.observe(symbol="BTCUSDT", emit_ts_ms=1_000_000.0 + COOLDOWN_CEIL_MS + 1)
    assert cal.n("BTCUSDT") == 0


# ── calibrated value accuracy ─────────────────────────────────────────────────

def test_q80_reflects_interval_distribution():
    cal = CooldownCalibrator(min_signals=100, enforce=True, auto_enforce=False)
    # Feed 150 signals with 30_000ms intervals
    _warm(cal, "BTCUSDT", n=150, interval_ms=30_000.0)
    th = cal.thresholds(symbol="BTCUSDT")
    # q80 of constant 30_000ms distribution should be close to 30_000ms
    assert 25_000 <= th.cooldown_ms <= 35_000, f"Expected ~30000ms, got {th.cooldown_ms}"


def test_short_interval_symbol_lower_cooldown():
    cal = CooldownCalibrator(min_signals=100, enforce=True)
    # Symbol A: 5s intervals
    _warm(cal, "A", n=150, interval_ms=5_000.0)
    # Symbol B: 120s intervals
    _warm(cal, "B", n=150, interval_ms=120_000.0)

    th_a = cal.thresholds(symbol="A")
    th_b = cal.thresholds(symbol="B")
    assert th_a.cooldown_ms < th_b.cooldown_ms


# ── rails ─────────────────────────────────────────────────────────────────────

def test_rails_floor_applied():
    # With update_band_ms=0 (no hysteresis), any observed value must be >= floor.
    cal = CooldownCalibrator(min_signals=5, enforce=True, update_band_ms=0.0)
    ts = 1_000_000.0
    for _ in range(20):
        cal.observe(symbol="X", emit_ts_ms=ts)
        ts += COOLDOWN_FLOOR_MS + 1  # just above floor
    th = cal.thresholds(symbol="X")
    assert th.cooldown_ms >= COOLDOWN_FLOOR_MS


def test_rails_ceil_applied():
    cal = CooldownCalibrator(min_signals=5, enforce=True)
    # Feed intervals just below ceil
    ts = 1_000_000.0
    for _ in range(20):
        cal.observe(symbol="X", emit_ts_ms=ts)
        ts += COOLDOWN_CEIL_MS - 1
    th = cal.thresholds(symbol="X")
    assert th.cooldown_ms <= COOLDOWN_CEIL_MS


# ── invalid observations ──────────────────────────────────────────────────────

def test_nan_ts_ignored():
    cal = CooldownCalibrator()
    cal.observe(symbol="BTCUSDT", emit_ts_ms=float("nan"))
    assert cal.n("BTCUSDT") == 0


def test_negative_ts_ignored():
    cal = CooldownCalibrator()
    cal.observe(symbol="BTCUSDT", emit_ts_ms=-1.0)
    assert cal.n("BTCUSDT") == 0


def test_zero_ts_ignored():
    cal = CooldownCalibrator()
    cal.observe(symbol="BTCUSDT", emit_ts_ms=0.0)
    assert cal.n("BTCUSDT") == 0


# ── hysteresis ────────────────────────────────────────────────────────────────

def test_hysteresis_prevents_small_drift():
    cal = CooldownCalibrator(
        min_signals=10, enforce=True,
        update_band_ms=5_000.0,  # 5s band
    )
    _warm(cal, "BTCUSDT", n=50, interval_ms=30_000.0)
    th1 = cal.thresholds(symbol="BTCUSDT")
    committed1 = th1.cooldown_ms

    # Tiny nudge (< 5s band)
    _warm(cal, "BTCUSDT", n=5, interval_ms=31_000.0)
    th2 = cal.thresholds(symbol="BTCUSDT")
    assert th2.cooldown_ms == committed1


# ── multi-symbol isolation ────────────────────────────────────────────────────

def test_symbols_are_independent():
    cal = CooldownCalibrator(min_signals=50, enforce=True)
    _warm(cal, "BTCUSDT", n=80, interval_ms=10_000.0)
    _warm(cal, "PEPEUSDT", n=80, interval_ms=60_000.0)

    th_btc = cal.thresholds(symbol="BTCUSDT")
    th_pepe = cal.thresholds(symbol="PEPEUSDT")
    assert th_btc.cooldown_ms < th_pepe.cooldown_ms


def test_symbol_case_normalized():
    cal = CooldownCalibrator(min_signals=10, enforce=True)
    _warm(cal, "BTCUSDT", n=50, interval_ms=20_000.0)
    th_upper = cal.thresholds(symbol="BTCUSDT")
    th_lower = cal.thresholds(symbol="btcusdt")
    assert th_upper.cooldown_ms == th_lower.cooldown_ms


# ── shadow ────────────────────────────────────────────────────────────────────

def test_shadow_none_before_thresholds_call():
    cal = CooldownCalibrator()
    assert cal.shadow_thresholds(symbol="BTCUSDT") is None


def test_shadow_populated_after_thresholds_call():
    cal = CooldownCalibrator(min_signals=10, enforce=False)
    _warm(cal, "BTCUSDT", n=50)
    cal.thresholds(symbol="BTCUSDT")
    shadow = cal.shadow_thresholds(symbol="BTCUSDT")
    assert shadow is not None
    assert shadow.src == "calib_q80"


def test_shadow_in_static_mode():
    cal = CooldownCalibrator(min_signals=10, enforce=False, auto_enforce=False)
    _warm(cal, "BTCUSDT", n=50)
    cal.thresholds(symbol="BTCUSDT")
    shadow = cal.shadow_thresholds(symbol="BTCUSDT")
    assert shadow is not None


# ── persistence ───────────────────────────────────────────────────────────────

def test_dump_load_roundtrip():
    cal1 = CooldownCalibrator(min_signals=50, enforce=False, auto_enforce=True)
    _warm(cal1, "BTCUSDT", n=80, interval_ms=25_000.0)
    state = cal1.dump_symbol_state(symbol="BTCUSDT", updated_ts_ms=9_999_999)

    cal2 = CooldownCalibrator(min_signals=50, enforce=False, auto_enforce=True)
    cal2.load_symbol_state(state)

    assert cal2.n("btcusdt") == cal1.n("btcusdt")
    th1 = cal1.thresholds(symbol="BTCUSDT")
    th2 = cal2.thresholds(symbol="BTCUSDT")
    assert abs(th1.cooldown_ms - th2.cooldown_ms) < 2_000


def test_load_wrong_kind_ignored():
    cal = CooldownCalibrator()
    cal.load_symbol_state({"kind": "other", "symbol": "btcusdt", "n": 999})
    assert cal.n("btcusdt") == 0


def test_load_malformed_state_silent():
    cal = CooldownCalibrator()
    cal.load_symbol_state(None)
    cal.load_symbol_state("garbage")
    cal.load_symbol_state({"kind": "cooldown"})  # missing fields


def test_state_version_and_kind():
    cal = CooldownCalibrator()
    _warm(cal, "BTCUSDT", n=10)
    state = cal.dump_symbol_state(symbol="BTCUSDT", updated_ts_ms=1)
    assert state["v"] == 1
    assert state["kind"] == "cooldown"
    assert state["symbol"] == "btcusdt"


# ── n() ───────────────────────────────────────────────────────────────────────

def test_n_counts_intervals_not_signals():
    cal = CooldownCalibrator()
    # 5 signals → 4 valid intervals
    ts = 1_000_000.0
    for _ in range(5):
        cal.observe(symbol="X", emit_ts_ms=ts)
        ts += 30_000.0
    assert cal.n("X") == 4


def test_n_unknown_symbol_is_zero():
    cal = CooldownCalibrator()
    assert cal.n("UNKNOWN") == 0
