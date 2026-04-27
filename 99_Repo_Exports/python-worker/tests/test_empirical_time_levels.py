from signals.empirical_time_levels import EmpiricalTimeLevelsConfig, RedisEmpiricalTimeLevelsProvider


class FakeRedis:
    def __init__(self):
        self.lists = {}

    def lrange(self, key, a, b):
        return list(self.lists.get(key, []))

    def llen(self, key):
        return len(self.lists.get(key, []))


def _k(kind, sym, tf, regime, suffix):
    return f"statsbuf:{kind}:{sym}:{tf}:{regime}:{suffix}"


def test_empirical_time_levels_selects_bucket_by_median_ttd(monkeypatch):
    monkeypatch.setenv("EMP_TIME_LEVELS_RUNTIME_ENABLED", "1")
    monkeypatch.setenv("EMP_LEVELS_USE_REGIME_DIM", "1")
    monkeypatch.setenv("EMP_TIME_LEVELS_MIN_N", "5")
    monkeypatch.setenv("EMP_TIME_BUCKETS_MINUTES", "1,2,3,5,8,13,21,34,45")
    monkeypatch.setenv("EMP_TIME_LEVELS_Q_MFE", "0.60")
    monkeypatch.setenv("EMP_TIME_LEVELS_Q_MFE", "0.60")
    monkeypatch.setenv("EMP_TIME_LEVELS_Q_MAE", "0.80")
    monkeypatch.setenv("EMP_TIME_LEVELS_MIN_N_TOTAL", "5")  # override default 120

    r = FakeRedis()
    cfg = EmpiricalTimeLevelsConfig.from_env()
    p = RedisEmpiricalTimeLevelsProvider(r, cfg)

    kind, sym, tf, regime = "breakout", "BTCUSDT", "1m", "range"
    # ttd median ~ 10 minutes => nearest bucket is 13 min (780000ms) or 8 min (480000ms)
    # Here: median=600000 => nearest is 8m (480000) or 13m (780000), nearest is 8m.
    r.lists[_k(kind, sym, tf, regime, "ttd_ms")] = [b"300000", b"600000", b"900000", b"600000", b"600000"]

    # survival denominator must exist now
    r.lists[_k(kind, sym, tf, regime, "trades")] = [b"1"] * 25
    bucket = 8 * 60_000
    r.lists[_k(kind, sym, tf, regime, f"alive_t{bucket}")] = [b"1"] * 25
    r.lists[_k(kind, sym, tf, regime, f"mfe_bps_t{bucket}")] = [b"50", b"100", b"150", b"200", b"250"] * 5
    r.lists[_k(kind, sym, tf, regime, f"mae_bps_t{bucket}")] = [b"40", b"60", b"80", b"120", b"160"] * 5

    res = p.get_levels(kind=kind, symbol=sym, tf=tf, regime=regime)
    assert res.ok is True
    assert res.bucket_ms == bucket
    assert res.n_alive >= 5
    # q60 for mfe on [50,100,150,200,250] is 170
    assert 165 <= res.tp1_bps <= 175
    # q80 for mae on [40,60,80,120,160] duplicated is 128
    assert 125 <= res.sl_bps <= 145


def test_double_t_fast_regime_uses_q25(monkeypatch):
    monkeypatch.setenv("EMP_TIME_LEVELS_RUNTIME_ENABLED", "1")
    monkeypatch.setenv("EMP_LEVELS_USE_REGIME_DIM", "1")
    monkeypatch.setenv("EMP_TIME_LEVELS_MIN_N", "5")
    monkeypatch.setenv("EMP_TIME_LEVELS_MIN_N_TOTAL", "5")
    monkeypatch.setenv("EMP_TIME_LEVELS_SURVIVE_MIN", "0.10")
    monkeypatch.setenv("EMP_TIME_BUCKETS_MINUTES", "5,8,13")
    monkeypatch.setenv("EMP_TIME_TTD_Q_FAST", "0.25")
    monkeypatch.setenv("EMP_TIME_TTD_Q_SLOW", "0.50")
    monkeypatch.setenv("EMP_TIME_TTD_FAST_REGIMES", "expansion,trending_bull,trending_bear,trend")

    r = FakeRedis()
    cfg = EmpiricalTimeLevelsConfig.from_env()
    p = RedisEmpiricalTimeLevelsProvider(r, cfg)

    kind, sym, tf, regime = "breakout", "BTCUSDT", "1m", "expansion"
    # ttd distribution: q25 should be close to 8m (480000ms)
    # [420000, 480000, 600000, 900000, 1200000] -> q25 (idx 1.0) = 480000
    r.lists[_k(kind, sym, tf, regime, "ttd_ms")] = [b"420000", b"480000", b"600000", b"900000", b"1200000"]
    bucket_fast = 8 * 60_000
    r.lists[_k(kind, sym, tf, regime, "trades")] = [b"1"] * 50
    r.lists[_k(kind, sym, tf, regime, f"alive_t{bucket_fast}")] = [b"1"] * 30
    r.lists[_k(kind, sym, tf, regime, f"mfe_bps_t{bucket_fast}")] = [b"100", b"120", b"140", b"160", b"180"]
    r.lists[_k(kind, sym, tf, regime, f"mae_bps_t{bucket_fast}")] = [b"80", b"90", b"100", b"110", b"120"]

    res = p.get_levels(kind=kind, symbol=sym, tf=tf, regime=regime)
    assert res.ok is True
    assert res.bucket_ms == bucket_fast


def test_survival_gate_blocks_when_p_survive_low(monkeypatch):
    monkeypatch.setenv("EMP_TIME_LEVELS_RUNTIME_ENABLED", "1")
    monkeypatch.setenv("EMP_LEVELS_USE_REGIME_DIM", "1")
    monkeypatch.setenv("EMP_TIME_LEVELS_MIN_N", "5")
    monkeypatch.setenv("EMP_TIME_LEVELS_MIN_N_TOTAL", "10")
    monkeypatch.setenv("EMP_TIME_LEVELS_SURVIVE_MIN", "0.50")  # require >=50%
    monkeypatch.setenv("EMP_TIME_BUCKETS_MINUTES", "8")

    r = FakeRedis()
    cfg = EmpiricalTimeLevelsConfig.from_env()
    p = RedisEmpiricalTimeLevelsProvider(r, cfg)

    kind, sym, tf, regime = "breakout", "BTCUSDT", "1m", "range"
    r.lists[_k(kind, sym, tf, regime, "ttd_ms")] = [b"480000"] * 10
    bucket = 8 * 60_000
    r.lists[_k(kind, sym, tf, regime, "trades")] = [b"1"] * 100   # denominator
    r.lists[_k(kind, sym, tf, regime, f"alive_t{bucket}")] = [b"1"] * 20  # 20% survive < 50%
    r.lists[_k(kind, sym, tf, regime, f"mfe_bps_t{bucket}")] = [b"100"] * 20
    r.lists[_k(kind, sym, tf, regime, f"mae_bps_t{bucket}")] = [b"80"] * 20

    res = p.get_levels(kind=kind, symbol=sym, tf=tf, regime=regime)
    assert res.ok is False
    assert "low_survival" in res.notes


def test_clamp_tp1_sl(monkeypatch):
    monkeypatch.setenv("EMP_TIME_LEVELS_RUNTIME_ENABLED", "1")
    monkeypatch.setenv("EMP_LEVELS_USE_REGIME_DIM", "1")
    monkeypatch.setenv("EMP_TIME_LEVELS_MIN_N", "5")
    monkeypatch.setenv("EMP_TIME_LEVELS_MIN_N_TOTAL", "5")
    monkeypatch.setenv("EMP_TIME_LEVELS_SURVIVE_MIN", "0.10")
    monkeypatch.setenv("EMP_TIME_BUCKETS_MINUTES", "8")
    monkeypatch.setenv("EMP_TIME_TP1_MIN_BPS", "50")
    monkeypatch.setenv("EMP_TIME_TP1_MAX_BPS", "120")
    monkeypatch.setenv("EMP_TIME_SL_MIN_BPS", "40")
    monkeypatch.setenv("EMP_TIME_SL_MAX_BPS", "90")

    r = FakeRedis()
    cfg = EmpiricalTimeLevelsConfig.from_env()
    p = RedisEmpiricalTimeLevelsProvider(r, cfg)

    kind, sym, tf, regime = "breakout", "BTCUSDT", "1m", "range"
    r.lists[_k(kind, sym, tf, regime, "ttd_ms")] = [b"480000"] * 10
    bucket = 8 * 60_000
    r.lists[_k(kind, sym, tf, regime, "trades")] = [b"1"] * 50
    r.lists[_k(kind, sym, tf, regime, f"alive_t{bucket}")] = [b"1"] * 25
    # raw quantiles would be huge -> must clamp
    r.lists[_k(kind, sym, tf, regime, f"mfe_bps_t{bucket}")] = [b"800", b"900", b"1000", b"1100", b"1200"]
    r.lists[_k(kind, sym, tf, regime, f"mae_bps_t{bucket}")] = [b"200", b"300", b"400", b"500", b"600"]

    res = p.get_levels(kind=kind, symbol=sym, tf=tf, regime=regime)
    assert res.ok is True
    assert 50.0 <= res.tp1_bps <= 120.0
    assert 40.0 <= res.sl_bps <= 90.0
