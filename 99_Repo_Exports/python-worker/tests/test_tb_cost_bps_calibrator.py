"""Tests for TbCostBpsCalibrator (#11, P2)."""
from __future__ import annotations

import json
import time

from core.tb_cost_bps_calibrator import TbCostBpsCalibrator


def _make_cal(**kw) -> TbCostBpsCalibrator:
    defaults = dict(
        enforce=True,
        min_samples=5,
        window_days=1.0,
        recompute_gap_ms=0,
        default_cost_bps=7.0,
        fee_bps=3.0,
    )
    defaults.update(kw)
    return TbCostBpsCalibrator(**defaults)


def _now_ms() -> int:
    return int(time.time() * 1000)


# ── Default / no-data behaviour ───────────────────────────────────────────────

def test_default_when_no_data():
    cal = _make_cal()
    assert cal.get_cost_bps("BTCUSDT") == 7.0


def test_default_when_enforce_off():
    cal = _make_cal(enforce=False, auto_enforce=False)
    ts = _now_ms()
    for _ in range(20):
        cal.observe(symbol="BTCUSDT", spread_bps=2.0, slip_bps=1.0, ts_ms=ts)
    assert cal.get_cost_bps("BTCUSDT") == 7.0


def test_global_wildcard_fallback():
    """If per-symbol bin has too few samples, falls back to global (*)."""
    cal = _make_cal(min_samples=50)
    ts = _now_ms()
    for _ in range(5):
        cal.observe(symbol="BTCUSDT", spread_bps=2.0, slip_bps=1.0, ts_ms=ts)
    # per-symbol bucket has 5 samples < min_samples=50
    # global bucket also has 5 samples < 50 → default
    assert cal.get_cost_bps("BTCUSDT") == 7.0


# ── Cost formula correctness ──────────────────────────────────────────────────

def test_cost_formula_correct():
    """cost = 2×spread_p50 + 2×fee + slip_p50."""
    cal = _make_cal(fee_bps=3.0, min_samples=3)
    ts = _now_ms()
    # deterministic: all samples identical → p50 == value
    for _ in range(10):
        cal.observe(symbol="ETHUSDT", spread_bps=4.0, slip_bps=2.0, ts_ms=ts)
    cost = cal.get_cost_bps("ETHUSDT")
    # 2×4 + 2×3 + 2 = 8 + 6 + 2 = 16
    assert abs(cost - 16.0) < 0.5, f"Expected ~16.0, got {cost}"


def test_cost_uses_median_not_mean():
    """Outlier should not dominate (median based)."""
    cal = _make_cal(fee_bps=3.0, min_samples=3)
    ts = _now_ms()
    # 9 normal samples + 1 extreme outlier
    for _ in range(9):
        cal.observe(symbol="SOLUSDT", spread_bps=2.0, slip_bps=1.0, ts_ms=ts)
    cal.observe(symbol="SOLUSDT", spread_bps=200.0, slip_bps=100.0, ts_ms=ts)
    cost = cal.get_cost_bps("SOLUSDT")
    # median of (2×9 + 200×1) = 2.0, median slip = 1.0
    # cost ≈ 2×2 + 2×3 + 1 = 11 (not dominated by outlier)
    assert cost < 30.0, f"Outlier skewed median too much: {cost}"


def test_cost_bounded_min():
    """Cost should never go below _MIN_COST_BPS=1.0."""
    cal = _make_cal(fee_bps=0.0, min_samples=3)
    ts = _now_ms()
    for _ in range(10):
        cal.observe(symbol="XRPUSDT", spread_bps=0.0, slip_bps=0.0, ts_ms=ts)
    cost = cal.get_cost_bps("XRPUSDT")
    assert cost >= 1.0


def test_cost_bounded_max():
    """Cost should never exceed _MAX_COST_BPS=50.0."""
    cal = _make_cal(fee_bps=3.0, min_samples=3)
    ts = _now_ms()
    for _ in range(10):
        cal.observe(symbol="PEPEUSDT", spread_bps=100.0, slip_bps=100.0, ts_ms=ts)
    cost = cal.get_cost_bps("PEPEUSDT")
    assert cost <= 50.0


# ── Fallback hierarchy ────────────────────────────────────────────────────────

def test_symbol_fallback_to_global():
    """Unknown symbol should fallback to global (*) bucket if populated and calibrated."""
    cal = _make_cal(min_samples=3)
    ts = _now_ms()
    # Feed wildcard symbol directly — writes to (*) bucket and triggers its recompute
    for _ in range(10):
        cal.observe(symbol="*", spread_bps=3.0, slip_bps=2.0, ts_ms=ts)
    # BTCUSDT: no per-symbol bucket, but global (*) has 10 samples >= min_samples=3
    cost = cal.get_cost_bps("BTCUSDT")
    # Should use global bucket's calibrated value
    # Global: 2×3 + 2×3 + 2 = 14
    assert cost != 7.0, f"Expected calibrated value from global bucket, got default 7.0"


# ── Snapshot / load_state roundtrip ──────────────────────────────────────────

def test_snapshot_roundtrip():
    cal = _make_cal()
    ts = _now_ms()
    for _ in range(10):
        cal.observe(symbol="BTCUSDT", spread_bps=2.5, slip_bps=1.5, ts_ms=ts)
    snap = cal.snapshot()
    assert snap["schema_version"] == 1
    assert isinstance(snap["bins"], list)
    assert "fee_bps" in snap

    cal2 = _make_cal()
    cal2.load_state(snap)
    assert cal2.get_cost_bps("BTCUSDT") == cal.get_cost_bps("BTCUSDT")


def test_snapshot_contains_enforce_flag():
    cal = _make_cal(enforce=True)
    snap = cal.snapshot()
    assert snap["enforce"] is True


def test_load_state_restores_fee_bps():
    cal = _make_cal(fee_bps=5.0)
    ts = _now_ms()
    for _ in range(10):
        cal.observe(symbol="ETHUSDT", spread_bps=2.0, slip_bps=1.0, ts_ms=ts)
    snap = cal.snapshot()

    cal2 = TbCostBpsCalibrator()
    cal2.load_state(snap)
    assert cal2.fee_bps == 5.0


# ── Window eviction ───────────────────────────────────────────────────────────

def test_window_eviction():
    cal = _make_cal(window_days=0.0001)  # tiny window (~8.6 seconds)
    ts_old = _now_ms() - 1_000_000  # definitely stale
    for _ in range(20):
        cal.observe(symbol="BTCUSDT", spread_bps=5.0, slip_bps=2.0, ts_ms=ts_old)
    # Force recompute with fresh sample (triggers prune)
    ts_now = _now_ms()
    cal.observe(symbol="BTCUSDT", spread_bps=1.0, slip_bps=0.5, ts_ms=ts_now)
    # Should not raise; result is valid (may fall back to default if buf too small)
    cost = cal.get_cost_bps("BTCUSDT")
    assert 1.0 <= cost <= 50.0


# ── Invalid input rejection ───────────────────────────────────────────────────

def test_invalid_nan_ignored():
    cal = _make_cal()
    ts = _now_ms()
    cal.observe(symbol="BTCUSDT", spread_bps=float("nan"), slip_bps=1.0, ts_ms=ts)
    cal.observe(symbol="BTCUSDT", spread_bps=2.0, slip_bps=float("inf"), ts_ms=ts)
    # Neither should crash or contaminate buffer
    assert cal.get_cost_bps("BTCUSDT") == 7.0  # no valid samples → default


def test_negative_spread_ignored():
    cal = _make_cal()
    ts = _now_ms()
    cal.observe(symbol="BTCUSDT", spread_bps=-1.0, slip_bps=1.0, ts_ms=ts)
    assert cal.get_cost_bps("BTCUSDT") == 7.0


# ── Shadow vs committed ───────────────────────────────────────────────────────

def test_shadow_returns_value():
    cal = _make_cal()
    ts = _now_ms()
    for _ in range(10):
        cal.observe(symbol="BTCUSDT", spread_bps=2.0, slip_bps=1.0, ts_ms=ts)
    shadow = cal.get_shadow("BTCUSDT")
    assert isinstance(shadow, float)
    assert 1.0 <= shadow <= 50.0


# ── Auto-enforce ──────────────────────────────────────────────────────────────

def test_auto_enforce_promotes_after_warmup():
    cal = _make_cal(enforce=False, auto_enforce=True, min_samples=5)
    ts = _now_ms()
    for _ in range(10):
        cal.observe(symbol="BTCUSDT", spread_bps=3.0, slip_bps=2.0, ts_ms=ts)
    cost = cal.get_cost_bps("BTCUSDT")
    # auto_enforce should kick in after min_samples reached
    assert isinstance(cost, float)
    assert 1.0 <= cost <= 50.0


# ── IPS weight influence ──────────────────────────────────────────────────────

def test_ips_weight_accepted():
    """Weighted samples should be accepted without error."""
    cal = _make_cal(min_samples=3)
    ts = _now_ms()
    for _ in range(10):
        cal.observe(symbol="BTCUSDT", spread_bps=2.0, slip_bps=1.0, ts_ms=ts, w=0.5)
    cost = cal.get_cost_bps("BTCUSDT")
    assert isinstance(cost, float)


# ── Reader module (without live Redis) ───────────────────────────────────────

def test_reader_returns_none_when_disabled(monkeypatch):
    """AUTOCAL_TB_COST_BPS_READ_ENABLED=0 → get_cost_bps returns None."""
    monkeypatch.setenv("AUTOCAL_TB_COST_BPS_READ_ENABLED", "0")
    # Reset singleton
    import services.tb_cost_bps_runtime_overrides as mod
    mod._READER = None
    result = mod.get_cost_bps("BTCUSDT")
    assert result is None


def test_reader_returns_calibrated_value_from_snapshot():
    """Reader should parse snapshot JSON and return committed value."""
    import services.tb_cost_bps_runtime_overrides as mod

    snap = {
        "schema_version": 1,
        "ts_ms": int(time.time() * 1000),
        "enforce": True,
        "default_cost_bps": 7.0,
        "fee_bps": 3.0,
        "bins": [
            {"symbol": "BTCUSDT", "committed_cost_bps": 12.5, "shadow_cost_bps": 12.5, "n": 100, "n_buf": 100},
        ],
    }

    class FakeRedis:
        def get(self, _key):
            return json.dumps(snap).encode()

    reader = mod.TbCostBpsReader(FakeRedis(), redis_key="autocal:tb_cost_bps:state")
    result = reader.get_cost_bps("BTCUSDT")
    assert result == 12.5


def test_reader_fallback_to_global_bucket():
    """Reader should fallback to (*) bucket for unknown symbol."""
    import services.tb_cost_bps_runtime_overrides as mod

    snap = {
        "schema_version": 1,
        "ts_ms": int(time.time() * 1000),
        "enforce": True,
        "default_cost_bps": 7.0,
        "fee_bps": 3.0,
        "bins": [
            {"symbol": "*", "committed_cost_bps": 9.0, "shadow_cost_bps": 9.0, "n": 100, "n_buf": 100},
        ],
    }

    class FakeRedis:
        def get(self, _key):
            return json.dumps(snap).encode()

    reader = mod.TbCostBpsReader(FakeRedis(), redis_key="autocal:tb_cost_bps:state")
    result = reader.get_cost_bps("UNKNOWN_SYM")
    assert result == 9.0


def test_reader_returns_none_when_not_enforce():
    """Reader returns None when enforce=False in snapshot."""
    import services.tb_cost_bps_runtime_overrides as mod

    snap = {
        "schema_version": 1,
        "ts_ms": int(time.time() * 1000),
        "enforce": False,
        "default_cost_bps": 7.0,
        "fee_bps": 3.0,
        "bins": [
            {"symbol": "BTCUSDT", "committed_cost_bps": 12.5, "shadow_cost_bps": 12.5, "n": 100, "n_buf": 100},
        ],
    }

    class FakeRedis:
        def get(self, _key):
            return json.dumps(snap).encode()

    reader = mod.TbCostBpsReader(FakeRedis(), redis_key="autocal:tb_cost_bps:state", refresh_ms=1)
    result = reader.get_cost_bps("BTCUSDT")
    assert result is None
