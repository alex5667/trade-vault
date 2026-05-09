from services.ev_tp1_stats import EvTp1StatsConfig, RedisEvTp1StatsProvider, update_evstats_on_close


class FakeRedis:
    def __init__(self):
        self.store = {}
        self.ttl = {}

    def hgetall(self, key):
        return self.store.get(key, {}).copy()

    def hset(self, key, mapping=None, **kwargs):
        if mapping is None:
            mapping = {}
        d = self.store.setdefault(key, {})
        for k, v in mapping.items():
            d[str(k)] = str(v)

    def expire(self, key, ttl):
        self.ttl[key] = int(ttl)

    def register_script(self, _lua: str):
        # We don't execute Lua in unit tests; we emulate the same math atomically in Python.
        def _call(keys=None, args=None, **_kw):
            k = keys[0]
            hit = int(float(args[0]))
            a = float(args[1])
            now = int(float(args[2]))
            ttl = int(float(args[3]))
            d = self.store.setdefault(k, {})
            tot = int(float(d.get("total_trades", "0"))) + 1
            hits = int(float(d.get("tp1_hits", "0"))) + (1 if hit > 0 else 0)
            if "ema_tp1" in d and d["ema_tp1"] != "":
                ema_old = float(d["ema_tp1"])
                ema_new = ema_old + a * ((1 if hit > 0 else 0) - ema_old)
            else:
                ema_new = float(1 if hit > 0 else 0)
            d["total_trades"] = str(tot)
            d["tp1_hits"] = str(hits)
            d["ema_tp1"] = str(ema_new)
            d["updated_ms"] = str(now)
            if ttl > 0:
                self.ttl[k] = ttl
            return str(ema_new)
        return _call


def test_evstats_writer_updates_counts_and_ema(monkeypatch):
    monkeypatch.setenv("EV_GATE_ENABLED", "1")
    monkeypatch.setenv("EV_GATE_USE_REGIME_DIM", "1")
    monkeypatch.setenv("EV_GATE_EMA_ALPHA", "0.10")
    monkeypatch.setenv("EV_GATE_STATS_TTL_SEC", "3600")

    r = FakeRedis()
    cfg = EvTp1StatsConfig.from_env()

    # First trade: hit=1 => ema initializes to 1.0
    update_evstats_on_close(r, cfg=cfg, kind="breakout", symbol="BTCUSDT", tf="1m", regime="range", tp1_hit=1)
    k = RedisEvTp1StatsProvider(r, cfg).key(kind="breakout", symbol="BTCUSDT", tf="1m", regime="range")
    d = r.hgetall(k)
    assert int(float(d["total_trades"])) == 1
    assert int(float(d["tp1_hits"])) == 1
    assert abs(float(d["ema_tp1"]) - 1.0) < 1e-12
    assert r.ttl.get(k) == 3600

    # Second trade: hit=0 => ema = 1.0 + 0.1*(0-1.0) = 0.9
    update_evstats_on_close(r, cfg=cfg, kind="breakout", symbol="BTCUSDT", tf="1m", regime="range", tp1_hit=0)
    d2 = r.hgetall(k)
    assert int(float(d2["total_trades"])) == 2
    assert int(float(d2["tp1_hits"])) == 1
    assert abs(float(d2["ema_tp1"]) - 0.9) < 1e-12


def test_evstats_key_regime_dim_toggle(monkeypatch):
    monkeypatch.setenv("EV_GATE_ENABLED", "1")
    monkeypatch.setenv("EV_GATE_USE_REGIME_DIM", "0")
    r = FakeRedis()
    cfg = EvTp1StatsConfig.from_env()
    update_evstats_on_close(r, cfg=cfg, kind="absorption", symbol="ETHUSDT", tf="1m", regime="range", tp1_hit=1)
    # regime is forced to "na"
    k = RedisEvTp1StatsProvider(r, cfg).key(kind="absorption", symbol="ETHUSDT", tf="1m", regime="range")
    assert k.endswith(":na")
    assert "total_trades" in r.hgetall(k)
