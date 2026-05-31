"""Tests for DQSoftFlagCalibrator (W5)."""
import time
from core.dq_softflag_calibrator import DQSoftFlagCalibrator


def _ts() -> int:
    return int(time.time() * 1000)


def _feed_stale(cal: DQSoftFlagCalibrator, *, symbol="BTCUSDT",
                vals: list[float]) -> None:
    cal.recompute_gap_ms = 0
    ts = _ts()
    for i, v in enumerate(vals):
        cal.observe_book_dt(symbol=symbol, dt_ms=v, ts_ms=ts + i * 1000)


def _feed_spread(cal: DQSoftFlagCalibrator, *, symbol="BTCUSDT",
                 vals: list[float]) -> None:
    cal.recompute_gap_ms = 0
    ts = _ts()
    for i, v in enumerate(vals):
        cal.observe_spread(symbol=symbol, spread_bps=v, ts_ms=ts + i * 1000)


class TestDQSoftFlagCalibratorShadow:
    def test_defaults_when_enforce_off(self):
        cal = DQSoftFlagCalibrator(enforce=False)
        assert cal.get_stale_flag_ms("BTCUSDT") == cal.default_stale_ms
        assert cal.get_spread_flag_bps("BTCUSDT") == cal.default_spread_bps

    def test_shadow_no_effect_on_output(self):
        cal = DQSoftFlagCalibrator(enforce=False, auto_enforce=False, min_samples=5)
        _feed_stale(cal, vals=[300.0] * 50)
        assert cal.get_stale_flag_ms("BTCUSDT") == cal.default_stale_ms


class TestDQSoftFlagCalibratorEnforce:
    def test_stale_converges(self):
        cal = DQSoftFlagCalibrator(enforce=True, min_samples=20)
        # p95(500ms) * 2.0 = 1000ms, floored at 200ms, capped at 10000ms
        _feed_stale(cal, vals=[500.0] * 100)
        ms = cal.get_stale_flag_ms("BTCUSDT")
        assert 200 <= ms <= 10000

    def test_spread_converges(self):
        cal = DQSoftFlagCalibrator(enforce=True, min_samples=20)
        # p75(5.0bps) * 2.0 = 10.0bps
        _feed_spread(cal, vals=[5.0] * 100)
        bps = cal.get_spread_flag_bps("BTCUSDT")
        assert 2.0 <= bps <= 100.0

    def test_stale_min_floor(self):
        cal = DQSoftFlagCalibrator(enforce=True, min_samples=5)
        _feed_stale(cal, vals=[0.1] * 100)  # near-zero stale → floored
        ms = cal.get_stale_flag_ms("BTCUSDT")
        assert ms >= 200

    def test_stale_max_cap(self):
        cal = DQSoftFlagCalibrator(enforce=True, min_samples=5)
        _feed_stale(cal, vals=[9000.0] * 100)
        ms = cal.get_stale_flag_ms("BTCUSDT")
        assert ms <= 10000

    def test_spread_min_floor(self):
        cal = DQSoftFlagCalibrator(enforce=True, min_samples=5)
        _feed_spread(cal, vals=[0.001] * 100)
        bps = cal.get_spread_flag_bps("BTCUSDT")
        assert bps >= 2.0

    def test_spread_max_cap(self):
        cal = DQSoftFlagCalibrator(enforce=True, min_samples=5)
        _feed_spread(cal, vals=[200.0] * 100)
        bps = cal.get_spread_flag_bps("BTCUSDT")
        assert bps <= 100.0


class TestDQSoftFlagCalibratorFallback:
    def test_wildcard_fallback_for_unknown_symbol(self):
        cal = DQSoftFlagCalibrator(enforce=True, min_samples=5)
        _feed_stale(cal, symbol="*", vals=[800.0] * 50)
        ms = cal.get_stale_flag_ms("SOLUSDT")
        # Should get wildcard value since SOLUSDT not observed
        assert ms == cal.get_stale_flag_ms("*")


class TestDQSoftFlagCalibratorAutoEnforce:
    def test_auto_enforce_promotes_after_warmup(self):
        cal = DQSoftFlagCalibrator(enforce=False, auto_enforce=True, min_samples=5)
        _feed_stale(cal, vals=[300.0] * 50)
        ms = cal.get_stale_flag_ms("BTCUSDT")
        # After warmup: returns calibrated p95(300) * 2.0 = 600ms, floored at 200
        assert 200 <= ms <= 10000
        assert ms != cal.default_stale_ms  # no longer the default

    def test_auto_enforce_roundtrip(self):
        snap = DQSoftFlagCalibrator(auto_enforce=True).snapshot()
        assert snap["auto_enforce"] is True
        cal2 = DQSoftFlagCalibrator(auto_enforce=False)
        cal2.load_state(snap)
        assert cal2.auto_enforce is True


class TestDQSoftFlagCalibratorSnapshot:
    def test_roundtrip(self):
        cal = DQSoftFlagCalibrator(enforce=True, min_samples=5)
        _feed_stale(cal, vals=[400.0] * 50)
        _feed_spread(cal, vals=[7.0] * 50)
        snap = cal.snapshot()
        cal2 = DQSoftFlagCalibrator(enforce=False)
        cal2.load_state(snap)
        assert cal2.enforce is True
        assert len(cal2._bins) > 0

    def test_schema_version(self):
        cal = DQSoftFlagCalibrator()
        assert cal.snapshot()["schema_version"] == 1

    def test_invalid_observations_ignored(self):
        cal = DQSoftFlagCalibrator(enforce=True)
        cal.observe_book_dt(symbol="BTC", dt_ms=0.0, ts_ms=_ts())
        cal.observe_book_dt(symbol="BTC", dt_ms=-1.0, ts_ms=_ts())
        cal.observe_book_dt(symbol="BTC", dt_ms=float("nan"), ts_ms=_ts())
        assert all(b.n_stale == 0 for b in cal._bins.values())

    def test_min_samples_gate(self):
        cal = DQSoftFlagCalibrator(enforce=True, min_samples=50)
        _feed_stale(cal, vals=[500.0] * 10)
        assert cal.get_stale_flag_ms("BTCUSDT") == cal.default_stale_ms
