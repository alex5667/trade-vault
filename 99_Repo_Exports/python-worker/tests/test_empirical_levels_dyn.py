import importlib


class FakeRedis:
    def __init__(self, data=None):
        self.data = data or {}

    def lrange(self, key, a, b):
        return self.data.get(key, [])


def test_percentile_linear(monkeypatch):
    import signals.empirical_levels_dyn as m
    importlib.reload(m)

    xs = [1.0, 2.0, 3.0, 4.0]
    assert abs(m._percentile(xs, 0.0) - 1.0) < 1e-9
    assert abs(m._percentile(xs, 1.0) - 4.0) < 1e-9
    # 60% position = 0.6*(n-1)=1.8 => interp between 2 and 3 => 2.8
    assert abs(m._percentile(xs, 0.6) - 2.8) < 1e-9


def test_provider_quantiles_with_fallback_to_na(monkeypatch):
    monkeypatch.setenv("EMP_LEVELS_ENABLED", "1")
    monkeypatch.setenv("EMP_LEVELS_MIN_N", "5")
    monkeypatch.setenv("EMP_LEVELS_TP1_Q", "0.60")
    monkeypatch.setenv("EMP_LEVELS_SL_Q", "0.80")
    monkeypatch.setenv("EMP_LEVELS_USE_REGIME_DIM", "1")
    monkeypatch.setenv("EMP_LEVELS_FALLBACK_TO_NA", "1")
    monkeypatch.setenv("EMP_LEVELS_CACHE_MS", "0")

    import signals.empirical_levels_dyn as m
    importlib.reload(m)

    cfg = m.EmpiricalLevelsConfig.from_env()
    # regime-specific lists are missing => fallback to na
    base = "statsbuf:breakout:BTCUSDT:1m:na"
    r = FakeRedis(
        {
            f"{base}:mfe_bps": [b"10", b"20", b"30", b"40", b"50", b"60", b"70", b"80", b"90", b"100"],
            f"{base}:mae_bps": [b"5", b"10", b"15", b"20", b"25", b"30", b"35", b"40", b"45", b"50"],
            f"{base}:ttd_ms": [b"1000", b"2000", b"3000", b"4000", b"5000", b"6000", b"7000", b"8000", b"9000", b"10000"],
        }
    )
    p = m.RedisEmpiricalLevelsProvider(r, cfg)
    res = p.get(kind="breakout", symbol="BTCUSDT", tf="1m", regime="range")
    assert res is not None
    assert res.regime_used == "na"
    assert res.n_mfe == 10 and res.n_mae == 10


def test_apply_empirical_levels_rr_rebuilds_tps(monkeypatch):
    import signals.empirical_levels_dyn as m
    importlib.reload(m)

    class Ctx:
        pass

    ctx = Ctx()
    emp = m.EmpiricalLevelsResult(tp1_bps=30.0, sl_bps=10.0, ttd_ms=2000, n_mfe=100, n_mae=100, n_ttd=80, regime_used="range")
    cfg = {"TP_MODE": "RR", "TP_RR": "1,2,3"}

    ok = m.apply_empirical_levels_to_ctx(
        ctx,
        side="LONG",
        entry_price=100.0,
        atr=0.0,
        risk_cfg=cfg,
        emp=emp,
        logger=None,
    )
    assert ok is True
    assert abs(ctx.sl_price - 99.90) < 1e-9      # 10 bps
    assert abs(ctx.tp1_price - 100.10) < 1e-9    # 1R with risk=0.10
    assert len(ctx.tp_levels) == 3
    assert abs(ctx.tp_levels[1] - 100.20) < 1e-9 # 2R
    assert abs(ctx.tp_levels[2] - 100.30) < 1e-9 # 3R
