"""Tests for ExecLatencyTimeoutCalibrator (W3)."""
import time
from core.exec_latency_timeout_calibrator import ExecLatencyTimeoutCalibrator


def _ts() -> int:
    return int(time.time() * 1000)


def _feed(cal: ExecLatencyTimeoutCalibrator, latencies: list[float]) -> None:
    cal.recompute_gap_ms = 0
    ts = _ts()
    for i, lat in enumerate(latencies):
        cal.observe(arm_latency_ms=lat, ts_ms=ts + i * 1000)


class TestExecLatencyTimeoutCalibratorShadow:
    def test_default_when_enforce_off(self):
        cal = ExecLatencyTimeoutCalibrator(enforce=False)
        snap = cal.snapshot()
        assert snap["committed_executor_ms"] == 2500  # _Bin default
        assert snap["committed_router_ms"] == 5000

    def test_get_methods_return_none_when_enforce_off(self):
        cal = ExecLatencyTimeoutCalibrator(enforce=False)
        assert cal.get_executor_timeout_ms() is None
        assert cal.get_router_timeout_ms() is None

    def test_enforce_off_get_returns_none(self):
        cal = ExecLatencyTimeoutCalibrator(enforce=False, auto_enforce=False, min_samples=5)
        _feed(cal, [100.0] * 20)
        # committed updates internally but public read API returns None when auto_enforce disabled
        assert cal.get_executor_timeout_ms() is None
        assert cal.get_router_timeout_ms() is None


class TestExecLatencyTimeoutCalibratorEnforce:
    def test_converges_on_low_latency(self):
        cal = ExecLatencyTimeoutCalibrator(enforce=True, min_samples=10)
        _feed(cal, [50.0] * 50)
        snap = cal.snapshot()
        # p99(50ms) * 2.0 = 100ms, but min is 1500ms
        assert snap["committed_executor_ms"] >= 1500

    def test_converges_on_high_latency(self):
        cal = ExecLatencyTimeoutCalibrator(enforce=True, min_samples=10)
        _feed(cal, [2000.0] * 50)
        snap = cal.snapshot()
        # p99(2000ms) * 2.0 = 4000ms — executor floor is 1500, max 10000
        assert 1500 <= snap["committed_executor_ms"] <= 10000
        # router > executor always
        assert snap["committed_router_ms"] > snap["committed_executor_ms"]

    def test_router_always_greater_than_executor(self):
        cal = ExecLatencyTimeoutCalibrator(enforce=True, min_samples=5)
        _feed(cal, [500.0] * 30)
        snap = cal.snapshot()
        assert snap["committed_router_ms"] > snap["committed_executor_ms"]


class TestExecLatencyTimeoutCalibratorBounds:
    def test_executor_ms_floor(self):
        cal = ExecLatencyTimeoutCalibrator(enforce=True, min_samples=5)
        _feed(cal, [1.0] * 20)  # tiny latencies
        snap = cal.snapshot()
        assert snap["committed_executor_ms"] >= 1500

    def test_executor_ms_cap(self):
        cal = ExecLatencyTimeoutCalibrator(enforce=True, min_samples=5)
        _feed(cal, [9999.0] * 20)  # huge latencies
        snap = cal.snapshot()
        assert snap["committed_executor_ms"] <= 10000
        assert snap["committed_router_ms"] <= 20000


class TestExecLatencyTimeoutCalibratorAutoEnforce:
    def test_auto_enforce_promotes_after_warmup(self):
        cal = ExecLatencyTimeoutCalibrator(enforce=False, auto_enforce=True, min_samples=5)
        _feed(cal, [300.0] * 20)
        exec_ms = cal.get_executor_timeout_ms()
        # After warmup (20 >= min_samples=5), auto_enforce promotes → returns calibrated value
        assert exec_ms is not None
        assert exec_ms >= 1500

    def test_auto_enforce_roundtrip(self):
        snap = ExecLatencyTimeoutCalibrator(auto_enforce=True).snapshot()
        assert snap["auto_enforce"] is True
        cal2 = ExecLatencyTimeoutCalibrator(auto_enforce=False)
        cal2.load_state(snap)
        assert cal2.auto_enforce is True


class TestExecLatencyTimeoutCalibratorSnapshot:
    def test_snapshot_roundtrip(self):
        cal = ExecLatencyTimeoutCalibrator(enforce=True, min_samples=5)
        _feed(cal, [300.0] * 20)
        snap = cal.snapshot()
        cal2 = ExecLatencyTimeoutCalibrator(enforce=False)
        cal2.load_state(snap)
        assert cal2.enforce is True
        assert cal2.snapshot()["committed_executor_ms"] == snap["committed_executor_ms"]

    def test_invalid_observations_ignored(self):
        cal = ExecLatencyTimeoutCalibrator(enforce=True)
        cal.observe(arm_latency_ms=float("nan"), ts_ms=_ts())
        cal.observe(arm_latency_ms=-1.0, ts_ms=_ts())
        assert cal._global.n_observed == 0

    def test_min_samples_gate(self):
        cal = ExecLatencyTimeoutCalibrator(enforce=True, min_samples=20)
        _feed(cal, [300.0] * 5)  # below min_samples
        snap = cal.snapshot()
        assert snap["committed_executor_ms"] == 2500  # _Bin default, not yet calibrated
