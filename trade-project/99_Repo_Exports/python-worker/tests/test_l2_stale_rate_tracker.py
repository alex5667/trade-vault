from common.metrics2 import InMemoryMetrics, MissingRateTracker


def test_l2_stale_rate_exports_ratio():
    m = InMemoryMetrics()
    tr = MissingRateTracker(metric="l2_stale_rate", export_every_n=5, tags={"symbol": "BTCUSDT"})

    # 5 use-cases: 2 stale (или missing), 3 ok => rate = 0.4
    tr.mark(miss=True)
    tr.maybe_export(m)
    tr.mark(miss=False)
    tr.maybe_export(m)
    tr.mark(miss=True)
    tr.maybe_export(m)
    tr.mark(miss=False)
    tr.maybe_export(m)
    tr.mark(miss=False)
    tr.maybe_export(m)  # экспорт здесь

    assert len(m.gauges) >= 1
    name, value, tags = m.gauges[-1]
    assert name == "l2_stale_rate"
    assert abs(value - 0.4) < 1e-9
    assert (tags or {}).get("symbol") == "BTCUSDT"
