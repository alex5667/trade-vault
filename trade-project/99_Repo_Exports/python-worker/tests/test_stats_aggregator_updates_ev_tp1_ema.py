import importlib


class FakeRedis:
    def __init__(self):
        self.h = {}

    def hmget(self, key, *fields):
        d = self.h.get(key, {})
        return [d.get(f) for f in fields]

    def hset(self, key, mapping=None, **kwargs):
        if mapping is None:
            mapping = {}
        d = self.h.get(key)
        if d is None:
            d = {}
            self.h[key] = d
        for k, v in mapping.items():
            d[str(k)] = v
        for k, v in kwargs.items():
            d[str(k)] = v

    def expire(self, key, ttl):
        # not needed for assertion
        return True


def test_stats_aggregator_calls_ev_tp1_update(monkeypatch):
    monkeypatch.setenv("EV_TP1_ENABLED", "1")
    monkeypatch.setenv("EV_TP1_ALPHA", "0.10")
    monkeypatch.setenv("EV_TP1_USE_REGIME_DIM", "1")

    import services.stats_aggregator as sa
    importlib.reload(sa)

    captured = {}

    def fake_script(*, keys, args):
        captured["keys"] = keys
        captured["args"] = args
        return 1

    def fake_get_script(redis_client):
        return fake_script

    monkeypatch.setattr(sa.StatsAggregator, "_get_script", staticmethod(fake_get_script))

    r = FakeRedis()
    trade_closed = {
        "strategy": "breakout",
        "symbol": "BTCUSDT",
        "tf": "1m",
        "source": "x",
        "order_id": "OID-1",
        "exit_ts_ms": 123456,
        "pnl_net": 1.0,
        "pnl_gross": 1.0,
        "fees": 0.0,
        "pnl_pct": 0.0,
        "tp1_hit": 1,
        "duration_ms": 10000,
        "mfe_pnl": 1.0,
        "mae_pnl": 0.4,
        "regime": "trend",
        "is_final_close": True,
    }

    sa.StatsAggregator.update_stats(r, {}, trade_closed)

    # EMA key must be created
    # Key format: ev:tp1:{kind}:{symbol}:{tf}:{regime}
    k = "ev:tp1:breakout:BTCUSDT:1m:trend"
    assert k in r.h
    assert "p_ema" in r.h[k]
    assert "n" in r.h[k]
    assert int(float(r.h[k]["n"])) >= 1
