from common.metrics2 import InMemoryMetrics, MissingRateTracker


def test_l3_missing_rate_exports():
    m = InMemoryMetrics()
    tr = MissingRateTracker(metric="l3_missing_rate", export_every_n=5, tags={"symbol": "BTCUSDT"})

    # 5 событий: 1 miss => 0.2
    tr.mark(miss=False)
    tr.maybe_export(m)
    tr.mark(miss=False)
    tr.maybe_export(m)
    tr.mark(miss=True)
    tr.maybe_export(m)
    tr.mark(miss=False)
    tr.maybe_export(m)
    tr.mark(miss=False)
    tr.maybe_export(m)

    name, value, tags = m.gauges[-1]
    assert name == "l3_missing_rate"
    assert abs(value - 0.2) < 1e-9
    assert (tags or {}).get("symbol") == "BTCUSDT"
