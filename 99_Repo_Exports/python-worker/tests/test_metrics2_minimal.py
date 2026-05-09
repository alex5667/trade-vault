
from common.metrics2 import EventRateTracker, InMemoryMetrics, LagTracker, MissingRateTracker
from utils.time_utils import get_ny_time_millis


def test_lag_tracker_exports_p50_p95():
    m = InMemoryMetrics()
    lt = LagTracker(window=64, export_every_n=10)
    # 20 значений: 0..19
    for i in range(1, 21):
        lt.update(i - 1)
        lt.maybe_export(m)

    # Экспорт должен случиться на 10 и 20
    names = [g[0] for g in m.gauges]
    assert "tick_lag_ms_p50" in names
    assert "tick_lag_ms_p95" in names

    # Последний экспорт по распределению 0..19:
    # p50 ~ 10, p95 ~ 18-19 (nearest-rank)
    last_p50 = [g for g in m.gauges if g[0] == "tick_lag_ms_p50"][-1][1]
    last_p95 = [g for g in m.gauges if g[0] == "tick_lag_ms_p95"][-1][1]
    assert 8.0 <= last_p50 <= 12.0
    assert 17.0 <= last_p95 <= 19.0


def test_missing_rate_tracker_exports_rate():
    m = InMemoryMetrics()
    rt = MissingRateTracker(metric="l3_missing_rate", export_every_n=10)
    # 10 событий: 3 missing
    for i in range(10):
        rt.mark(miss=(i in {0, 1, 2}))
        rt.maybe_export(m)
    assert len(m.gauges) >= 1
    name, value, _tags = m.gauges[-1]
    assert name == "l3_missing_rate"
    assert abs(value - 0.3) < 1e-6


def test_event_rate_tracker_exports_rate():
    m = InMemoryMetrics()
    er = EventRateTracker(metric="l3_event_rate", export_every_ms=500, alpha=1.0)
    # старт
    t0 = get_ny_time_millis()
    er.maybe_export(m, now_ms=t0)
    # 50 событий за 1000мс => 50 eps
    for _ in range(50):
        er.mark_event()
    er.maybe_export(m, now_ms=t0 + 1000)
    assert len(m.gauges) == 1
    assert m.gauges[0][0] == "l3_event_rate"
    assert 45.0 <= m.gauges[0][1] <= 55.0
